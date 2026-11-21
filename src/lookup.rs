//! Manifest, products and campaigns lookups (small, loaded once).

use crate::common::Files;
use crate::swar::split2;
use crate::ts::parse_f1;
use xxhash_rust::xxh3::xxh3_128;

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
    let ws = parse_f1(json_string_value(&text, "\"window_start\"")?.as_bytes())?;
    let we = parse_f1(json_string_value(&text, "\"window_end\"")?.as_bytes())?;
    Some(Manifest {
        window_start: ws,
        window_end: we,
    })
}

fn split_lines(text: &[u8]) -> impl Iterator<Item = &[u8]> {
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

// ---------------------------------------------------------------- categories

/// Category names sorted byte-wise; an index into this list ranks categories
/// for the tie-break (first maximum = byte-wise smallest).
pub struct Categories {
    pub names: Vec<String>,
}

impl Categories {
    pub fn load(products_text: &[u8]) -> Categories {
        let mut cats: Vec<&[u8]> = Vec::new();
        let mut lines = split_lines(products_text);
        lines.next();
        for line in lines {
            if let Some((_, cat)) = split2(line) {
                cats.push(cat);
            }
        }
        cats.push(b"unknown");
        cats.sort_unstable();
        cats.dedup();
        Categories {
            names: cats.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect(),
        }
    }

    pub fn index(&self, name: &[u8]) -> u16 {
        match self.names.binary_search_by(|a| a.as_bytes().cmp(name)) {
            Ok(i) => i as u16,
            Err(_) => (self.names.len() - 1) as u16, // unknown
        }
    }
}

// ---------------------------------------------------------------- products

/// product_id -> category index, with a fast path for `p<N>` sequential ids.
pub struct Products {
    sequential: bool,
    cat: Vec<u16>,
    keys: Vec<u128>,
    vals: Vec<u16>,
    mask: usize,
    pub unknown_cat: u16,
}

impl Products {
    pub fn load(text: &[u8], cats: &Categories) -> Products {
        let mut pairs: Vec<(&[u8], u16)> = Vec::new();
        let mut lines = split_lines(text);
        lines.next();
        for line in lines {
            if let Some((id, cat)) = split2(line) {
                pairs.push((id, cats.index(cat)));
            }
        }
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
        let cap = (n * 2 + 16).max(4).next_power_of_two();
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
            unknown_cat: cats.index(b"unknown"),
        }
    }

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
    matches!(parse_p_number(id), Some(v) if v == n)
}

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
        lines.next();
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
        let cap = (n * 2 + 16).max(4).next_power_of_two();
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
    matches!(parse_c_number(id), Some(v) if v == n)
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

// ---------------------------------------------------------------- file listing

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

pub fn map_all(files: &[std::path::PathBuf], kind: u8) -> (Files, u64) {
    let mut maps = Vec::new();
    let mut kinds = Vec::new();
    let mut total = 0u64;
    for p in files {
        if let Ok(f) = std::fs::File::open(p) {
            if let Ok(m) = unsafe { memmap2::Mmap::map(&f) } {
                total += m.len() as u64;
                maps.push(unsafe { std::mem::transmute::<&[u8], &'static [u8]>(&m[..]) });
                kinds.push(kind);
                std::mem::forget(m);
            }
        }
    }
    (Files { maps, kind: kinds }, total)
}
