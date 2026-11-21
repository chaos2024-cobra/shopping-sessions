//! Phase A: chunked parallel CSV parsing. No locks, no shared hash tables:
//! every parsed line lands in the worker's own per-bucket segments.

use crate::common::*;
use crate::lookup::{Campaigns, Products};
use crate::swar::split_n;
use crate::ts::{parse_f1, parse_price, parse_ts};
use xxhash_rust::xxh3::xxh3_128;

pub const NBUCKET: usize = 1024; // event-id hash buckets
pub const NPART: usize = 64; // user-hash partitions

/// Per-worker output; owned exclusively by one worker while parsing.
pub struct WorkerOut {
    /// Event segments, one per event-id bucket.
    pub ev: Vec<Vec<EvRec>>,
    /// Click segments, one per user-hash partition.
    pub cl: Vec<Vec<ClRec>>,
    pub n_events: u64,
    pub n_clicks: u64,
}

impl WorkerOut {
    pub fn new(est_event_lines: usize, est_click_lines: usize, nworkers: usize) -> WorkerOut {
        let ev = (0..NBUCKET)
            .map(|_| {
                Vec::with_capacity(
                    (est_event_lines / NBUCKET / nworkers * 6 / 5).max(1024),
                )
            })
            .collect();
        let cl = (0..NPART)
            .map(|_| {
                Vec::with_capacity((est_click_lines / NPART / nworkers * 6 / 5).max(256))
            })
            .collect();
        WorkerOut {
            ev,
            cl,
            n_events: 0,
            n_clicks: 0,
        }
    }
}

pub struct ParseCtx<'a> {
    pub products: &'a Products,
    pub campaigns: &'a Campaigns,
    pub window_start: i64,
    pub window_end: i64,
    pub click_window_start: i64,
}

/// One chunk of one file: [start, end) byte range, aligned to line starts.
#[derive(Clone, Copy)]
pub struct Chunk {
    pub file: usize,
    pub start: u64,
    pub end: u64,
    pub kind: u8, // 0 events, 1 clicks, 2 links
    pub skip_header: bool,
}

/// First byte offset at or after `from` that starts a line.
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

pub fn run_chunk(
    data: &[u8],
    out: &mut WorkerOut,
    ctx: &ParseCtx,
    chunk: &Chunk,
    sides: &mut Vec<LinkSide>,
) {
    let len = data.len() as u64;
    let mut pos = line_start(data, chunk.start, len);
    if chunk.skip_header {
        match memchr::memchr(b'\n', &data[pos as usize..]) {
            Some(nl) => pos += nl as u64 + 1,
            None => return,
        }
    }
    let limit = chunk.end.min(len);
    let base = data.as_ptr() as usize;
    let file = chunk.file;
    match chunk.kind {
        0 => {
            let mut n = 0u64;
            while pos < limit {
                let from = pos as usize;
                let nl = match memchr::memchr(b'\n', &data[from..]) {
                    Some(n) => n,
                    None => {
                        handle_event(out, ctx, file, base, &data[from..]);
                        break;
                    }
                };
                handle_event(out, ctx, file, base, &data[from..from + nl]);
                pos = from as u64 + nl as u64 + 1;
                n += 1;
            }
            out.n_events += n;
        }
        1 => {
            let mut n = 0u64;
            while pos < limit {
                let from = pos as usize;
                let nl = match memchr::memchr(b'\n', &data[from..]) {
                    Some(n) => n,
                    None => {
                        handle_click(out, ctx, file, base, &data[from..]);
                        break;
                    }
                };
                handle_click(out, ctx, file, base, &data[from..from + nl]);
                pos = from as u64 + nl as u64 + 1;
                n += 1;
            }
            out.n_clicks += n;
        }
        _ => {
            while pos < limit {
                let from = pos as usize;
                let nl = match memchr::memchr(b'\n', &data[from..]) {
                    Some(n) => n,
                    None => {
                        handle_link(sides, file, base, &data[from..]);
                        break;
                    }
                };
                handle_link(sides, file, base, &data[from..from + nl]);
                pos = from as u64 + nl as u64 + 1;
            }
        }
    }
}

#[inline(always)]
fn loc_of(file: usize, base: usize, s: &[u8]) -> u64 {
    ((file as u64) << 48) | (((s.as_ptr() as usize - base) as u64) & 0xFFFF_FFFF_FFFF)
}

#[inline(always)]
fn handle_event(out: &mut WorkerOut, ctx: &ParseCtx, file: usize, base: usize, line: &[u8]) {
    if line.is_empty() {
        return;
    }
    let f = match split_n::<7>(line) {
        Some(f) => f,
        None => return,
    };
    if f[0].is_empty() {
        return;
    }
    let ingest = match parse_f1(f[3]) {
        Some(v) => v,
        None => return,
    };
    let eh = xxh3_128(f[0]);
    let eh_hi = (eh >> 64) as u64;
    let eh_lo = eh as u64 as u16;

    let mut flags: u8 = 0;
    let mut ts_delta: i32 = 0;
    if let Some(v) = parse_ts(f[2]) {
        if v >= ctx.window_start && v < ctx.window_end {
            ts_delta = (v - ctx.window_start) as i32;
            flags |= F_TS;
        }
    }

    let (uh_hi, uh_lo, loc, id_len): (u64, u16, u64, u16) = if f[1].is_empty() {
        (1, 0, 0, 0)
    } else {
        flags |= F_USER;
        let h = xxh3_128(f[1]);
        (
            (h >> 64) as u64,
            h as u64 as u16,
            loc_of(file, base, f[1]),
            f[1].len() as u16,
        )
    };

    let kind: u8 = match f[4] {
        b"view" => 0,
        b"cart" => 1,
        b"purchase" => 2,
        _ => 3,
    };

    let (cat, prod_ok) = if f[5].is_empty() {
        (0u16, false)
    } else {
        (ctx.products.category(f[5]), true)
    };
    if prod_ok {
        flags |= F_PROD;
    }

    let mut price: u32 = 0;
    if kind == 2 {
        if let Some(p) = parse_price(f[6]) {
            price = p;
            flags |= F_PRICE;
        }
    } else {
        flags |= F_PRICE;
    }

    if flags & F_TS != 0 {
        let ts = ctx.window_start + ts_delta as i64;
        if ingest - ts <= 86_400_000 {
            flags |= F_LATE;
        }
    }

    let rec = EvRec {
        eh_hi,
        uh_hi,
        loc,
        ingest: (ingest / 1000) as u32,
        ts_delta,
        price,
        cat,
        id_len,
        eh_lo,
        uh_lo,
        kind,
        flags,
    };
    let b = (eh_hi >> 54) as usize; // top 10 bits: NBUCKET == 1024
    out.ev[b].push(rec);
}

#[inline(always)]
fn handle_click(out: &mut WorkerOut, ctx: &ParseCtx, file: usize, base: usize, line: &[u8]) {
    if line.is_empty() {
        return;
    }
    let f = match split_n::<4>(line) {
        Some(f) => f,
        None => return,
    };
    if f[0].is_empty() {
        return;
    }
    // Copies of a click are identical, so keeping every copy changes no
    // output: attribution picks the same (ts, click_id) and assists count a
    // campaign once. Dedup is skipped.
    let Some(camp) = ctx.campaigns.index(f[2]) else {
        return;
    };
    let (ts_delta, ts_ok) = match parse_ts(f[3]) {
        Some(v) => (
            (v - ctx.click_window_start) as i32,
            v >= ctx.click_window_start && v < ctx.window_end,
        ),
        None => (0, false),
    };
    if !ts_ok || f[1].is_empty() {
        return;
    }
    let h = xxh3_128(f[1]);
    let rec = ClRec {
        uh_hi: (h >> 64) as u64,
        uh_lo: h as u64 as u16,
        user_loc: loc_of(file, base, f[1]),
        cid_loc: loc_of(file, base, f[0]),
        ts_delta,
        camp,
        user_len: f[1].len() as u16,
        cid_len: f[0].len() as u16,
    };
    let p = rec.uh_hi as usize & (NPART - 1);
    out.cl[p].push(rec);
}

#[inline(always)]
fn handle_link(out: &mut Vec<LinkSide>, file: usize, base: usize, line: &[u8]) {
    if line.is_empty() {
        return;
    }
    let f = match split_n::<2>(line) {
        Some(f) => f,
        None => return,
    };
    if f[0].is_empty() || f[1].is_empty() {
        return;
    }
    let ha = xxh3_128(f[0]);
    let hb = xxh3_128(f[1]);
    let pair = (out.len() / 2) as u64;
    out.push(LinkSide {
        h_hi: (ha >> 64) as u64,
        loc: loc_of(file, base, f[0]),
        pair,
        h_lo: ha as u64 as u16,
        len: f[0].len() as u16,
    });
    out.push(LinkSide {
        h_hi: (hb >> 64) as u64,
        loc: loc_of(file, base, f[1]),
        pair,
        h_lo: hb as u64 as u16,
        len: f[1].len() as u16,
    });
}
