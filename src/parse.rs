//! Chunked parallel CSV parsing for events, clicks and links, with the
//! sharded dedup tables the rules require (event_id: smallest ingest_ts wins;
//! click_id: any copy).

use std::sync::Mutex;

use memmap2::Mmap;
use xxhash_rust::xxh3::xxh3_128;

#[allow(non_snake_case)]
unsafe fn libc_madvise_willneed(ptr: *const u8, len: usize) {
    extern "C" {
        fn madvise(addr: *mut core::ffi::c_void, len: usize, advice: i32) -> i32;
    }
    if !ptr.is_null() && len > 0 {
        madvise(ptr as *mut core::ffi::c_void, len, 3 /* MADV_WILLNEED */);
    }
}

/// Ask for transparent huge pages on a large table allocation, so random
/// probes into multi-GB tables do not thrash the TLB.
#[allow(non_snake_case)]
pub fn madvise_hugepage<T>(v: &[T]) {
    extern "C" {
        fn madvise(addr: *mut core::ffi::c_void, len: usize, advice: i32) -> i32;
    }
    const MADV_HUGEPAGE: i32 = 14;
    let ptr = v.as_ptr() as usize;
    let aligned = ptr & !0x1F_FFFF; // round down to 2 MiB
    let end = ptr + v.len() * std::mem::size_of::<T>();
    let len = end - aligned;
    if len >= 0x20_0000 {
        unsafe {
            madvise(aligned as *mut core::ffi::c_void, len, MADV_HUGEPAGE);
        }
    }
}

use crate::lookup::{split2, unquote, Campaigns, Interner, Products, ORGANIC};
use crate::ts::{parse_f1, parse_price, parse_ts, DAY_MS};
use crate::util::SpinLock;

pub const EVENT_FIELDS: usize = 7;
pub const CLICK_FIELDS: usize = 4;

// ---------------------------------------------------------------- records

/// One deduped event copy (the fields of the line it came from).
#[derive(Clone, Copy)]
pub struct EventRec {
    pub ingest: i64,
    pub ts: i64,
    pub user: u32, // u32::MAX when the user_id was empty
    pub price: u32,
    pub cat: u16,
    pub kind: u8, // 0 view, 1 cart, 2 purchase, 3 bad type
    pub ts_ok: bool,
    pub user_ok: bool,
    pub prod_ok: bool,
    pub price_ok: bool,
}

impl EventRec {
    #[inline]
    pub fn keep(&self) -> bool {
        self.kind != 3
            && self.ts_ok
            && self.user_ok
            && self.prod_ok
            && (self.kind != 2 || self.price_ok)
            && (self.ingest - self.ts) <= 86_400_000
    }
}

/// One deduped click.
#[derive(Clone, Copy)]
pub struct ClickRec {
    pub ts: i64,
    pub user: u32,
    pub camp: u16, // ORGANIC sentinel when unknown campaign
    pub off: u32,  // click_id bytes in this shard's arena
    pub len: u32,
    pub ts_ok: bool,
    pub user_ok: bool,
}

// ---------------------------------------------------------------- dedup shards

const DEDUP_SHARDS: usize = 1024;

#[derive(Clone, Copy)]
struct ESlot {
    h_hi: u64, // high 64 bits of the event_id hash (0 = empty)
    ingest: u32, // ingest seconds; duplicates always differ by whole seconds
    idx: u32, // record index within this shard
}

struct EShardData {
    slots: Vec<ESlot>,
    records: Vec<EventRec>,
    rec_lo: Vec<u32>, // low 32 bits of the hash, to verify slot matches
    count: usize,
}

struct EShard {
    lock: SpinLock,
    cell: std::cell::UnsafeCell<EShardData>,
}

// SAFETY: every access to the cell happens while holding the shard lock.
unsafe impl Sync for EShard {}

pub struct EventTable {
    shards: Vec<EShard>,
}

impl EventTable {
    pub fn new(expected_lines: usize) -> Self {
        // generous sizing keeps the linear-probe chains short (load ~0.45)
        let per = (expected_lines / DEDUP_SHARDS * 2).max(1024).next_power_of_two();
        let table = EventTable {
            shards: (0..DEDUP_SHARDS)
                .map(|_| EShard {
                    lock: SpinLock::new(),
                    cell: std::cell::UnsafeCell::new(EShardData {
                        slots: vec![
                            ESlot { h_hi: 0, ingest: 0, idx: 0 };
                            per
                        ],
                        records: Vec::with_capacity(expected_lines / DEDUP_SHARDS + 16),
                        rec_lo: Vec::with_capacity(expected_lines / DEDUP_SHARDS + 16),
                        count: 0,
                    }),
                })
                .collect(),
        };
        for shard in &table.shards {
            // SAFETY: read-only access before any parsing.
            let d = unsafe { &*shard.cell.get() };
            madvise_hugepage(&d.slots);
        }
        table
    }

    /// Insert-or-keep-min-ingest.
    #[inline]
    pub fn insert(&self, h: u128, rec: EventRec) {
        let mut h_hi = (h >> 64) as u64;
        if h_hi == 0 {
            h_hi = 1;
        }
        let h_lo = h as u32;
        let sec = (rec.ingest / 1000) as u32;
        let shard = &self.shards[((h >> 118) as usize) & (DEDUP_SHARDS - 1)];
        let _g = shard.lock.lock();
        // SAFETY: we hold the shard lock.
        let d = unsafe { &mut *shard.cell.get() };
        let mut mask = d.slots.len() - 1;
        let mut idx = (h as usize) & mask;
        loop {
            let s = d.slots[idx];
            if s.h_hi == 0 {
                if (d.count + 1) * 10 >= d.slots.len() * 8 {
                    Self::grow(&mut d.slots);
                    mask = d.slots.len() - 1;
                    idx = (h as usize) & mask;
                    continue;
                }
                let ri = d.records.len() as u32;
                d.records.push(rec);
                d.rec_lo.push(h_lo);
                d.slots[idx] = ESlot { h_hi, ingest: sec, idx: ri };
                d.count += 1;
                return;
            }
            if s.h_hi == h_hi && d.rec_lo[s.idx as usize] == h_lo {
                if sec < s.ingest {
                    d.slots[idx].ingest = sec;
                    d.records[s.idx as usize] = rec;
                }
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    fn grow(slots: &mut Vec<ESlot>) {
        let newcap = slots.len() * 2;
        let mut fresh = vec![ESlot { h_hi: 0, ingest: 0, idx: 0 }; newcap];
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

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn records(&self, shard: usize) -> &[EventRec] {
        // SAFETY: parsing is finished; read-only access.
        unsafe { &(*self.shards[shard].cell.get()).records }
    }
}

#[derive(Clone, Copy)]
struct CSlot {
    h_hi: u64,
    lo: u32,
    _pad: u32, // keeps the slot at 16 bytes
}

struct CShardData {
    slots: Vec<CSlot>,
    records: Vec<ClickRec>,
    arena: Vec<u8>,
    count: usize,
}

struct CShard {
    lock: SpinLock,
    cell: std::cell::UnsafeCell<CShardData>,
}

// SAFETY: every access to the cell happens while holding the shard lock.
unsafe impl Sync for CShard {}

pub struct ClickTable {
    shards: Vec<CShard>,
}

impl ClickTable {
    pub fn new(expected_lines: usize) -> Self {
        let per = (expected_lines / DEDUP_SHARDS * 2).max(1024).next_power_of_two();
        let table = ClickTable {
            shards: (0..DEDUP_SHARDS)
                .map(|_| CShard {
                    lock: SpinLock::new(),
                    cell: std::cell::UnsafeCell::new(CShardData {
                        slots: vec![CSlot { h_hi: 0, lo: 0, _pad: 0 }; per],
                        records: Vec::with_capacity(expected_lines / DEDUP_SHARDS + 16),
                        arena: Vec::with_capacity(expected_lines / DEDUP_SHARDS * 10 + 16),
                        count: 0,
                    }),
                })
                .collect(),
        };
        for shard in &table.shards {
            // SAFETY: read-only access before any parsing.
            let d = unsafe { &*shard.cell.get() };
            madvise_hugepage(&d.slots);
        }
        table
    }

    /// Insert if new; copies are identical, so an existing key is skipped.
    #[inline]
    pub fn insert(&self, h: u128, rec: ClickRec, cid: &[u8]) {
        let mut h_hi = (h >> 64) as u64;
        if h_hi == 0 {
            h_hi = 1;
        }
        let lo = h as u32;
        let shard = &self.shards[((h >> 118) as usize) & (DEDUP_SHARDS - 1)];
        let _g = shard.lock.lock();
        // SAFETY: we hold the shard lock.
        let d = unsafe { &mut *shard.cell.get() };
        let mut mask = d.slots.len() - 1;
        let mut idx = (h as usize) & mask;
        loop {
            let s = d.slots[idx];
            if s.h_hi == 0 {
                if (d.count + 1) * 10 >= d.slots.len() * 8 {
                    Self::grow(&mut d.slots);
                    mask = d.slots.len() - 1;
                    idx = (h as usize) & mask;
                    continue;
                }
                let off = d.arena.len() as u32;
                d.arena.extend_from_slice(cid);
                let mut r = rec;
                r.off = off;
                r.len = cid.len() as u32;
                d.records.push(r);
                d.slots[idx] = CSlot { h_hi, lo, _pad: 0 };
                d.count += 1;
                return;
            }
            if s.h_hi == h_hi && s.lo == lo {
                return; // identical copy already present
            }
            idx = (idx + 1) & mask;
        }
    }

    fn grow(slots: &mut Vec<CSlot>) {
        let newcap = slots.len() * 2;
        let mut fresh = vec![CSlot { h_hi: 0, lo: 0, _pad: 0 }; newcap];
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

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn records(&self, shard: usize) -> &[ClickRec] {
        // SAFETY: parsing is finished; read-only access.
        unsafe { &(*self.shards[shard].cell.get()).records }
    }

    pub fn arena(&self, shard: usize) -> &[u8] {
        // SAFETY: parsing is finished; read-only access.
        unsafe { &(*self.shards[shard].cell.get()).arena }
    }
}

// ---------------------------------------------------------------- chunks

pub struct Chunk {
    pub file: usize,
    pub start: u64,
    pub end: u64, // == file length for the last chunk of a file
}

pub struct Input {
    pub maps: Vec<Mmap>,
    pub chunks: Vec<Chunk>,
}

#[derive(Clone, Copy, PartialEq)]
pub enum TaskKind {
    Events,
    Clicks,
    Links,
}

pub fn list_csv_files(data_dir: &str, folder: &str) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let dir = std::path::Path::new(data_dir).join(folder);
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().map(|e| e == "csv").unwrap_or(false) {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// Maps every file and splits it into byte-range chunks on line boundaries.
/// `budget` is the target chunk size in bytes.
pub fn build_input(files: &[std::path::PathBuf], _kind: TaskKind, budget: u64) -> Input {
    let mut maps = Vec::new();
    let mut chunks = Vec::new();
    for path in files {
        let len = match std::fs::metadata(path) {
            Ok(m) => m.len(),
            Err(_) => continue,
        };
        if len == 0 {
            continue;
        }
        let file = maps.len();
        let map = unsafe { Mmap::map(&std::fs::File::open(path).unwrap()) }.unwrap();
        // kick off readahead for the whole file so page faults hit the cache
        unsafe {
            libc_madvise_willneed(map.as_ptr(), map.len());
        }
        maps.push(map);
        if len <= budget {
            chunks.push(Chunk {
                file,
                start: 0,
                end: len,
            });
        } else {
            let mut start = 0u64;
            while start < len {
                let end = (start + budget).min(len);
                chunks.push(Chunk { file, start, end });
                start = end;
            }
        }
    }
    Input { maps, chunks }
}

/// First byte offset at or after `from` that starts a line (a line's start).
#[inline]
fn line_start(data: &[u8], from: u64, file_len: u64) -> u64 {
    if from == 0 {
        return 0;
    }
    let at = from as usize;
    if at >= data.len() {
        return file_len;
    }
    if data[at - 1] == b'\n' {
        return from;
    }
    match memchr::memchr(b'\n', &data[at..]) {
        Some(nl) => at as u64 + nl as u64 + 1,
        None => file_len,
    }
}

/// Parse one chunk of a file. The chunk that begins a file skips its header.
pub fn run_chunk(input: &Input, chunk: &Chunk, kind: TaskKind, ctx: &ParseCtx) {
    let data = &input.maps[chunk.file][..];
    let len = data.len() as u64;
    let first = chunk.start == 0;
    let mut pos = line_start(data, chunk.start, len);
    if first {
        // skip the header line
        match memchr::memchr(b'\n', &data[pos as usize..]) {
            Some(nl) => pos += nl as u64 + 1,
            None => return,
        }
    }
    let limit = chunk.end.min(len);
    while pos < limit {
        let from = pos as usize;
        let line = match memchr::memchr(b'\n', &data[from..]) {
            Some(nl) => {
                pos = from as u64 + nl as u64 + 1;
                &data[from..from + nl]
            }
            None => {
                pos = len;
                &data[from..]
            }
        };
        if line.is_empty() {
            continue; // an empty line is malformed
        }
        match kind {
            TaskKind::Events => handle_event_line(line, ctx),
            TaskKind::Clicks => {
                if SKIP.load(std::sync::atomic::Ordering::Relaxed) & 16 == 0 {
                    handle_click_line(line, ctx)
                }
            }
            TaskKind::Links => handle_link_line(line, ctx),
        }
    }
}

// ---------------------------------------------------------------- contexts

pub struct ParseCtx<'a> {
    pub interner: &'a Interner,
    pub products: &'a Products,
    pub campaigns: &'a Campaigns,
    pub events: &'a EventTable,
    pub clicks: &'a ClickTable,
    pub links: &'a Mutex<Vec<(u32, u32)>>,
    pub window_start: i64,
    pub window_end: i64,
    pub click_window_start: i64,
}

/// Split into exactly `want` comma-separated fields; returns None for the
/// wrong count or any malformed quoting. `out` receives the unquoted values.
#[inline(always)]
fn split_fields<'a>(line: &'a [u8], want: usize, out: &mut [&'a [u8]]) -> bool {
    let mut nc = 0usize;
    let mut has_quote = false;
    let mut last = 0usize;
    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if nc < want - 1 {
                out[nc] = &line[last..i];
            }
            nc += 1;
            if nc > want - 1 {
                return false;
            }
            last = i + 1;
        } else if b == b'"' {
            has_quote = true;
        }
    }
    if nc != want - 1 {
        return false;
    }
    out[want - 1] = &line[last..];
    if has_quote {
        for i in 0..want {
            match unquote(out[i]) {
                Some(v) => out[i] = v,
                None => return false,
            }
        }
    }
    true
}

pub static SKIP: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

#[inline(always)]
fn handle_event_line(line: &[u8], ctx: &ParseCtx) {
    let skip = SKIP.load(std::sync::atomic::Ordering::Relaxed);
    let mut f: [&[u8]; EVENT_FIELDS] = [&[]; EVENT_FIELDS];
    if !split_fields(line, EVENT_FIELDS, &mut f) {
        return;
    }
    // event_id must be non-empty
    if f[0].is_empty() {
        return;
    }
    // ingest_ts is always valid F1; anything else cannot be ranked for dedup.
    let ingest = match parse_f1(f[3]) {
        Some(v) => v,
        None => return,
    };
    let mut h = xxh3_128(f[0]);
    if h == 0 {
        h = 1;
    }
    if skip & 1 != 0 {
        return;
    }

    let ts = if skip & 2 != 0 { Some(0i64) } else { parse_ts(f[2]) };
    let (ts_val, ts_ok) = match ts {
        Some(v) => (v, v >= ctx.window_start && v < ctx.window_end),
        None => (0, false),
    };
    let (user, user_ok) = if f[1].is_empty() {
        (u32::MAX, false)
    } else if skip & 4 != 0 {
        (0, true)
    } else {
        (ctx.interner.intern(f[1]), true)
    };
    let kind: u8 = match f[4] {
        b"view" => 0,
        b"cart" => 1,
        b"purchase" => 2,
        _ => 3,
    };
    let (prod_ok, cat) = if f[5].is_empty() {
        (false, 0)
    } else {
        (true, ctx.products.category(f[5]))
    };
    let (price_ok, price) = if kind == 2 {
        match parse_price(f[6]) {
            Some(p) => (true, p),
            None => (false, 0),
        }
    } else {
        (true, 0)
    };

    if skip & 8 == 0 {
    ctx.events.insert(
        h,
        EventRec {
            ingest,
            ts: if ts_ok { ts_val } else { 0 },
            user,
            price,
            cat,
            kind,
            ts_ok,
            user_ok,
            prod_ok,
            price_ok,
        },
    );
    }
}

#[inline(always)]
fn handle_click_line(line: &[u8], ctx: &ParseCtx) {
    let mut f: [&[u8]; CLICK_FIELDS] = [&[]; CLICK_FIELDS];
    if !split_fields(line, CLICK_FIELDS, &mut f) {
        return;
    }
    if f[0].is_empty() {
        return;
    }
    let ts = parse_ts(f[3]);
    let (ts_val, ts_ok) = match ts {
        Some(v) => (
            v,
            v >= ctx.click_window_start && v < ctx.window_end,
        ),
        None => (0, false),
    };
    let (user, user_ok) = if f[1].is_empty() {
        (u32::MAX, false)
    } else {
        (ctx.interner.intern(f[1]), true)
    };
    let camp = match ctx.campaigns.index(f[2]) {
        Some(c) => c,
        None => ORGANIC, // unknown campaign: invalid, marked by camp sentinel
    };
    let mut h = xxh3_128(f[0]);
    if h == 0 {
        h = 1;
    }
    ctx.clicks.insert(
        h,
        ClickRec {
            ts: if ts_ok { ts_val } else { 0 },
            user,
            camp,
            off: 0,
            len: 0,
            ts_ok,
            user_ok,
        },
        f[0],
    );
}

#[inline(always)]
fn handle_link_line(line: &[u8], ctx: &ParseCtx) {
    let Some((a, b)) = split2(line) else {
        return;
    };
    if a.is_empty() || b.is_empty() {
        return;
    }
    let na = ctx.interner.intern(a);
    let nb = ctx.interner.intern(b);
    let mut pairs = ctx.links.lock().unwrap();
    pairs.push((na, nb));
}

// ---------------------------------------------------------------- misc

pub fn epoch_day(ts: i64) -> i32 {
    ts.div_euclid(DAY_MS) as i32
}
