//! Phase A3: dedup each event-id bucket (keep the smallest ingest), then emit
//! valid winners into user-hash partitions. One bucket at a time; per-bucket
//! tables are small enough to sit in cache.

use crate::common::*;
use crate::parse::NPART;

#[derive(Clone, Copy)]
struct ESlot {
    h_hi: u64,
    ingest: u32,
    eh_lo: u16,
    idx: u32,
}

/// Iterate (global index, record) over the per-worker segments of a bucket.
struct SegIter<'a> {
    segs: std::slice::Iter<'a, &'a [EvRec]>,
    cur: &'a [EvRec],
    at: usize,
    base: usize,
}

impl<'a> SegIter<'a> {
    fn new(bucket: &'a [&'a [EvRec]]) -> SegIter<'a> {
        let mut segs = bucket.iter();
        let cur = segs.next().copied().unwrap_or(&[]);
        SegIter {
            segs,
            cur,
            at: 0,
            base: 0,
        }
    }
    #[inline]
    fn next_rec(&mut self) -> Option<(usize, &'a EvRec)> {
        loop {
            if self.at < self.cur.len() {
                let r = &self.cur[self.at];
                let g = self.base + self.at;
                self.at += 1;
                return Some((g, r));
            }
            match self.segs.next() {
                Some(s) => {
                    self.base += self.cur.len();
                    self.cur = *s;
                    self.at = 0;
                }
                None => return None,
            }
        }
    }
}

/// Dedup one bucket and emit winners into `col` (the worker's per-partition
/// segments of EvRec2). `bucket` holds the per-worker segments for this
/// event-id bucket.
pub fn dedup_bucket(bucket: &[&[EvRec]], col: &mut Vec<Vec<EvRec2>>) -> usize {
    let n: usize = bucket.iter().map(|s| s.len()).sum();
    if n == 0 {
        return 0;
    }
    let cap = (n * 2).next_power_of_two();
    let mask = cap - 1;
    let mut table: Vec<ESlot> = vec![
        ESlot {
            h_hi: 0,
            ingest: 0,
            eh_lo: 0,
            idx: 0
        };
        cap
    ];

    // pass 1: for each event id keep the smallest ingest, remembering which
    // record index currently wins
    let mut it = SegIter::new(bucket);
    while let Some((g, rec)) = it.next_rec() {
        let mut h_hi = rec.eh_hi;
        if h_hi == 0 {
            h_hi = 1;
        }
        let mut at = h_hi as usize & mask;
        loop {
            let s = table[at];
            if s.h_hi == 0 {
                table[at] = ESlot {
                    h_hi,
                    ingest: rec.ingest,
                    eh_lo: rec.eh_lo,
                    idx: g as u32,
                };
                break;
            }
            if s.h_hi == h_hi && s.eh_lo == rec.eh_lo {
                if rec.ingest < s.ingest {
                    table[at] = ESlot {
                        h_hi,
                        ingest: rec.ingest,
                        eh_lo: rec.eh_lo,
                        idx: g as u32,
                    };
                }
                break; // larger or equal ingest never wins
            }
            at = (at + 1) & mask;
        }
    }

    // pass 2: iterate the table and emit each winner directly (the winning
    // record is fetched by index; the bucket is small enough to stay in cache)
    let mut emitted = 0usize;
    // flat view of all records for indexed access
    let mut flat: Vec<&[EvRec]> = Vec::with_capacity(bucket.len());
    let mut bounds: Vec<(usize, usize)> = Vec::with_capacity(bucket.len() + 1);
    let mut total = 0usize;
    for seg in bucket {
        flat.push(seg);
        bounds.push((total, total + seg.len()));
        total += seg.len();
    }
    bounds.push((total, total));
    let get = |g: usize| -> &EvRec {
        let mut lo = 0usize;
        let mut hi = flat.len();
        while lo + 1 < hi {
            let mid = (lo + hi) / 2;
            if bounds[mid].0 <= g {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        let (s, e) = bounds[lo];
        debug_assert!(g >= s && g < e);
        &flat[lo][g - s]
    };
    for slot in table.iter() {
        if slot.h_hi == 0 {
            continue;
        }
        let rec = get(slot.idx as usize);
        if !rec.valid() {
            continue;
        }
        let p = rec.uh_hi as usize & (NPART - 1);
        col[p].push(EvRec2 {
            uh_hi: rec.uh_hi,
            loc: rec.loc,
            ts_delta: rec.ts_delta,
            price: rec.price,
            cat: rec.cat,
            uh_lo: rec.uh_lo,
            id_len: rec.id_len,
            kind: rec.kind,
        });
        emitted += 1;
    }
    emitted
}
