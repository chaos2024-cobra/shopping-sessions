//! Lookups: manifest, products, campaigns, the concurrent string interner and
//! the union-find that joins a person's IDs.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::util::SpinLock;
use xxhash_rust::xxh3::xxh3_128;

// ---------------------------------------------------------------- manifest

pub struct Manifest {
    pub window_start: i64,
    pub window_end: i64,
}

fn json_string_value(text: &[u8], key: &str) -> Option<String> {
    let keyb = key.as_bytes();
    let mut at = 0;
    while at + keyb.len() + 1 < text.len() {
        if &text[at..at + keyb.len()] == keyb {
            let mut rest = &text[at + keyb.len()..];
            // skip to the opening quote of the value
            let mut k = 0;
            while k < rest.len() && rest[k] != b'"' {
                k += 1;
            }
            if k >= rest.len() {
                return None;
            }
            rest = &rest[k + 1..];
            let end = rest.iter().position(|&b| b == b'"')?;
            return Some(String::from_utf8_lossy(&rest[..end]).into_owned());
        }
        at += 1;
    }
    None
}

pub fn read_manifest(data_dir: &str) -> Option<Manifest> {
    let text = std::fs::read(format!("{data_dir}/manifest.json")).ok()?;
    let ws = crate::ts::parse_f1(json_string_value(&text, "\"window_start\"")?.as_bytes())?;
    let we = crate::ts::parse_f1(json_string_value(&text, "\"window_end\"")?.as_bytes())?;
    Some(Manifest {
        window_start: ws,
        window_end: we,
    })
}

// ---------------------------------------------------------------- interner

const INTERN_SHARDS: usize = 1024;

#[derive(Clone, Copy)]
struct ISlot {
    h_hi: u64, // high 64 bits of the hash; 0 = empty
    h_lo: u32, // low 32 bits, to verify a match
    node: u32,
}

struct IShardData {
    slots: Vec<ISlot>,
    strings: Vec<u8>,
    /// (node, offset into `strings`, len) for every id interned here.
    log: Vec<(u32, u32, u32)>,
    count: usize,
}

struct IShard {
    lock: SpinLock,
    cell: std::cell::UnsafeCell<IShardData>,
}

// SAFETY: every access to the cell happens while holding the shard lock.
unsafe impl Sync for IShard {}

pub struct Interner {
    shards: Vec<IShard>,
    next_node: AtomicU32,
}

fn round_up_pow2(n: usize) -> usize {
    n.max(4).next_power_of_two()
}

impl Interner {
    pub fn new(expected_nodes: usize) -> Self {
        let per = round_up_pow2((expected_nodes / INTERN_SHARDS * 2).max(4));
        let shards = (0..INTERN_SHARDS)
            .map(|_| IShard {
                lock: SpinLock::new(),
                cell: std::cell::UnsafeCell::new(IShardData {
                    slots: vec![ISlot { h_hi: 0, h_lo: 0, node: 0 }; per],
                    strings: Vec::with_capacity(1 << 16),
                    log: Vec::new(),
                    count: 0,
                }),
            })
            .collect::<Vec<IShard>>();
        for shard in &shards {
            // SAFETY: read-only access before any parsing.
            let d: &IShardData = unsafe { &*shard.cell.get() };
            crate::parse::madvise_hugepage(&d.slots);
        }
        Interner {
            shards,
            next_node: AtomicU32::new(0),
        }
    }

    #[inline]
    pub fn intern(&self, s: &[u8]) -> u32 {
        let mut h = xxh3_128(s);
        if h == 0 {
            h = 1;
        }
        let mut h_hi = (h >> 64) as u64;
        if h_hi == 0 {
            h_hi = 1;
        }
        let h_lo = h as u32;
        let shard = &self.shards[((h >> 118) as usize) & (INTERN_SHARDS - 1)];
        let _g = shard.lock.lock();
        // SAFETY: we hold the shard lock.
        let d = unsafe { &mut *shard.cell.get() };
        let mut mask = d.slots.len() - 1;
        let mut idx = (h as usize) & mask;
        loop {
            let slot = d.slots[idx];
            if slot.h_hi == 0 {
                if (d.count + 1) * 10 >= d.slots.len() * 8 {
                    Self::grow(&mut d.slots);
                    mask = d.slots.len() - 1;
                    idx = (h as usize) & mask;
                    continue;
                }
                let node = self.next_node.fetch_add(1, Ordering::Relaxed);
                let off = d.strings.len() as u32;
                d.strings.extend_from_slice(s);
                d.log.push((node, off, s.len() as u32));
                d.slots[idx] = ISlot { h_hi, h_lo, node };
                d.count += 1;
                return node;
            }
            if slot.h_hi == h_hi && slot.h_lo == h_lo {
                return slot.node;
            }
            idx = (idx + 1) & mask;
        }
    }

    fn grow(slots: &mut Vec<ISlot>) {
        let newcap = slots.len() * 2;
        let mut fresh = vec![ISlot { h_hi: 0, h_lo: 0, node: 0 }; newcap];
        let mask = newcap - 1;
        for &s in slots.iter() {
            if s.h_hi != 0 {
                let mut j = (s.h_hi as usize) & mask;
                while fresh[j].h_hi != 0 {
                    j = (j + 1) & mask;
                }
                fresh[j] = s;
            }
        }
        *slots = fresh;
    }

    pub fn node_count(&self) -> u32 {
        self.next_node.load(Ordering::Relaxed)
    }

    /// Freezes all shards and returns, per node id, its string. The returned
    /// slices alias the shard arenas, which must not be touched afterwards.
    pub fn freeze(&mut self) -> Vec<&'static [u8]> {
        let n = self.node_count() as usize;
        let mut node_str: Vec<&'static [u8]> = vec![&[][..]; n];
        for shard in &self.shards {
            let d = shard.cell.get();
            // SAFETY: no parsing is running any more.
            let d = unsafe { &*d };
            let strings: &'static [u8] = unsafe { std::mem::transmute(&d.strings[..]) };
            for &(node, off, len) in &d.log {
                node_str[node as usize] = &strings[off as usize..off as usize + len as usize];
            }
        }
        node_str
    }
}



// ---------------------------------------------------------------- union-find

pub struct UnionFind {
    parent: Vec<u32>,
    size: Vec<u32>,
}

impl UnionFind {
    pub fn new(n: usize) -> Self {
        UnionFind {
            parent: (0..n as u32).collect(),
            size: vec![1; n],
        }
    }

    #[inline]
    fn find(&mut self, mut x: u32) -> u32 {
        while self.parent[x as usize] != x {
            let p = self.parent[x as usize];
            let g = self.parent[p as usize];
            self.parent[x as usize] = g;
            x = g;
        }
        x
    }

    pub fn union(&mut self, a: u32, b: u32) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        if self.size[ra as usize] < self.size[rb as usize] {
            self.parent[ra as usize] = rb;
            self.size[rb as usize] += self.size[ra as usize];
        } else {
            self.parent[rb as usize] = ra;
            self.size[ra as usize] += self.size[rb as usize];
        }
    }

    /// Fully flattened roots for every node.
    pub fn roots(&mut self) -> Vec<u32> {
        let n = self.parent.len();
        let mut root = vec![0u32; n];
        for i in 0..n {
            root[i] = self.find(i as u32);
        }
        root
    }
}

// ---------------------------------------------------------------- products

/// product_id -> category index, with a fast path for `p<N>` sequential ids.
pub struct Products {
    sequential: bool,
    cat: Vec<u16>,           // fast path: cat of p{1..=n}
    keys: Vec<u128>,         // fallback hash table (h, cat) in parallel arrays
    vals: Vec<u16>,
    mask: usize,
    pub unknown_cat: u16,
}

impl Products {
    pub fn load(text: &[u8], category_index: &dyn Fn(&[u8]) -> u16) -> Products {
        let mut pairs: Vec<(&[u8], u16)> = Vec::new();
        let mut lines = split_lines(text);
        lines.next(); // header
        for line in lines {
            if let Some((id, cat)) = split2(line) {
                pairs.push((id, category_index(cat)));
            }
        }
        // Fast path when every id is p<N>, N = 1..=n in order.
        let n = pairs.len();
        let mut sequential = !pairs.is_empty();
        for (i, (id, _)) in pairs.iter().enumerate() {
            if !is_p_number(id, i as u64 + 1) {
                sequential = false;
                break;
            }
        }
        let mut cat = vec![0u16; n];
        if sequential {
            for (i, (_, c)) in pairs.iter().enumerate() {
                cat[i] = *c;
            }
        }
        let cap = round_up_pow2(n * 2 + 16);
        let mut keys = vec![0u128; cap];
        let mut vals = vec![0u16; cap];
        if !sequential {
            let mask = cap - 1;
            for (id, c) in &pairs {
                let mut h = xxh3_128(id);
                if h == 0 {
                    h = 1;
                }
                let mut i = (h as usize) & mask;
                while keys[i] != 0 {
                    i = (i + 1) & mask;
                }
                keys[i] = h;
                vals[i] = *c;
            }
        }
        Products {
            sequential,
            cat,
            keys,
            vals,
            mask: if sequential { 0 } else { cap - 1 },
            unknown_cat: category_index(b"unknown"),
        }
    }

    /// Category of a product id, or `unknown_cat` when not in the file.
    #[inline]
    pub fn category(&self, id: &[u8]) -> u16 {
        if self.sequential {
            if let Some(n) = parse_p_number(id) {
                let n = n as usize;
                if n >= 1 && n <= self.cat.len() {
                    return self.cat[n - 1];
                }
            }
            return self.unknown_cat;
        }
        let mut h = xxh3_128(id);
        if h == 0 {
            h = 1;
        }
        let mut i = (h as usize) & self.mask;
        loop {
            let k = self.keys[i];
            if k == 0 {
                return self.unknown_cat;
            }
            if k == h {
                return self.vals[i];
            }
            i = (i + 1) & self.mask;
        }
    }
}

fn is_p_number(id: &[u8], n: u64) -> bool {
    match parse_p_number(id) {
        Some(v) => v == n,
        None => false,
    }
}

/// `p` followed by 1..=9 ASCII digits, no leading zero needed.
fn parse_p_number(id: &[u8]) -> Option<u64> {
    if id.len() < 2 || id.len() > 10 || id[0] != b'p' {
        return None;
    }
    let mut v: u64 = 0;
    for &b in &id[1..] {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u64;
    }
    Some(v)
}

// ---------------------------------------------------------------- campaigns

pub const ORGANIC: u16 = u16::MAX;

pub struct Campaigns {
    pub ids: Vec<String>,
    pub channels: Vec<String>,
    /// fast path: index of c{N} (N = 1..=n), or u16::MAX when absent
    fast: Vec<u16>,
    keys: Vec<u128>,
    vals: Vec<u16>,
    mask: usize,
}

impl Campaigns {
    pub fn load(text: &[u8]) -> Campaigns {
        let mut ids = Vec::new();
        let mut channels = Vec::new();
        let mut lines = split_lines(text);
        lines.next(); // header
        for line in lines {
            if let Some((id, ch)) = split2(line) {
                ids.push(String::from_utf8_lossy(id).into_owned());
                channels.push(String::from_utf8_lossy(ch).into_owned());
            }
        }
        let n = ids.len();
        let mut sequential = n > 0;
        for (i, id) in ids.iter().enumerate() {
            if !is_c_number(id.as_bytes(), (i + 1) as u64) {
                sequential = false;
                break;
            }
        }
        let mut fast = vec![u16::MAX; n];
        let cap = round_up_pow2(n * 2 + 16);
        let mut keys = vec![0u128; cap];
        let mut vals = vec![0u16; cap];
        if sequential {
            for i in 0..n {
                fast[i] = i as u16;
            }
        } else {
            let mask = cap - 1;
            for (i, id) in ids.iter().enumerate() {
                let mut h = xxh3_128(id.as_bytes());
                if h == 0 {
                    h = 1;
                }
                let mut j = (h as usize) & mask;
                while keys[j] != 0 {
                    j = (j + 1) & mask;
                }
                keys[j] = h;
                vals[j] = i as u16;
            }
        }
        Campaigns {
            ids,
            channels,
            fast,
            keys,
            vals,
            mask: if sequential { 0 } else { cap - 1 },
        }
    }

    /// Index of a campaign id, or None when not in campaigns.csv.
    #[inline]
    pub fn index(&self, id: &[u8]) -> Option<u16> {
        let n = self.ids.len();
        if n == 0 {
            return None;
        }
        if let Some(v) = parse_c_number(id) {
            let v = v as usize;
            if v >= 1 && v <= n && self.fast[v - 1] != u16::MAX {
                return Some(self.fast[v - 1]);
            }
        }
        if self.mask == 0 {
            return None;
        }
        let mut h = xxh3_128(id);
        if h == 0 {
            h = 1;
        }
        let mut i = (h as usize) & self.mask;
        loop {
            let k = self.keys[i];
            if k == 0 {
                return None;
            }
            if k == h {
                return Some(self.vals[i]);
            }
            i = (i + 1) & self.mask;
        }
    }
}

fn is_c_number(id: &[u8], n: u64) -> bool {
    match parse_c_number(id) {
        Some(v) => v == n,
        None => false,
    }
}

fn parse_c_number(id: &[u8]) -> Option<u64> {
    if id.len() < 3 || id.len() > 11 || id[0] != b'c' || id[1] != b'_' {
        return None;
    }
    let mut v: u64 = 0;
    for &b in &id[2..] {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u64;
    }
    Some(v)
}

// ---------------------------------------------------------------- csv helpers

pub fn split_lines(text: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut rest = text;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        match memchr::memchr(b'\n', rest) {
            Some(at) => {
                let line = &rest[..at];
                rest = &rest[at + 1..];
                Some(line)
            }
            None => {
                let line = rest;
                rest = &[];
                Some(line)
            }
        }
    })
}

/// Split a two-column CSV line into unquoted field values.
/// Returns None for wrong field count or malformed quoting.
pub fn split2(line: &[u8]) -> Option<(&[u8], &[u8])> {
    let comma = memchr::memchr(b',', line)?;
    if memchr::memchr(b',', &line[comma + 1..]).is_some() {
        return None;
    }
    let a = &line[..comma];
    let b = &line[comma + 1..];
    Some((unquote(a)?, unquote(b)?))
}

/// A field's value after CSV unquoting, or None when a quote appears anywhere
/// except wrapped around the whole field.
#[inline]
pub fn unquote(f: &[u8]) -> Option<&[u8]> {
    if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
        if memchr::memchr(b'"', &f[1..f.len() - 1]).is_some() {
            None
        } else {
            Some(&f[1..f.len() - 1])
        }
    } else if memchr::memchr(b'"', f).is_some() {
        None
    } else {
        Some(f)
    }
}
