//! Phase B: turn link rows into people. Runs on the coordinator thread while
//! the parse workers grind through the event files.
//!
//! Sides are sorted by (uh_hi, uh_lo) to find distinct ids, node ids are
//! assigned, a union-find joins the pairs, and the byte-wise smallest id
//! names each component.

use crate::common::*;
use crate::parse::NPART;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
pub struct NodeInfo {
    pub h_hi: u64,
    pub loc: u64,
    pub h_lo: u16,
    pub len: u16,
}

pub struct LinkTables {
    /// Per user-hash partition, sorted by (uh_hi, uh_lo).
    pub parts: Vec<Vec<LinkEnt>>,
    /// Per partition open-addressing hash: uh_hi -> index into parts[p].
    pub hashes: Vec<Vec<u32>>,
}

const MSB_BITS: u32 = 11;
const MSB_BUCKETS: usize = 1 << MSB_BITS;

/// Concatenate the per-worker side lists, numbering every side globally.
pub fn concat_sides(worker_sides: &[Vec<LinkSide>]) -> Vec<LinkSide> {
    let total: usize = worker_sides.iter().map(|v| v.len()).sum();
    let mut all: Vec<LinkSide> = Vec::with_capacity(total);
    for v in worker_sides {
        let base = all.len() as u64;
        for (i, s) in v.iter().enumerate() {
            let mut s = *s;
            s.pair = base + i as u64;
            all.push(s);
        }
    }
    all
}

/// Scatter `items` into MSB_BUCKETS buckets by the top bits of key(item),
/// preserving nothing else. Parallel, race-free (per-slice reservations).
fn msb_partition<T: Copy + Send + Sync + 'static>(
    items: &mut Vec<T>,
    key: impl Fn(&T) -> u64 + Sync,
    nthreads: usize,
) -> Vec<Vec<T>> {
    let n = items.len();
    if n == 0 {
        items.clear();
        return (0..MSB_BUCKETS).map(|_| Vec::new()).collect();
    }
    let nt = nthreads.min(n).max(1);
    // per-slice histograms
    let mut hist = vec![0u32; nt * MSB_BUCKETS];
    let hist_ptr = Ptr::new(hist.as_mut_slice());
    let items_ptr = Ptr::new(items.as_mut_slice());
    run_parallel(nt, nt, |_, t| {
        let start = n * t / nt;
        let end = n * (t + 1) / nt;
        let items = items_ptr.r();
        for i in start..end {
            let b = (key(&items[i]) >> (64 - MSB_BITS)) as usize;
            *hist_ptr.elem_mut(t * MSB_BUCKETS + b) += 1;
        }
    });
    // prefix: bucket offsets per slice
    let mut off = vec![0u32; nt * MSB_BUCKETS + 1];
    let mut run = 0u32;
    for b in 0..MSB_BUCKETS {
        for t in 0..nt {
            let idx = t * MSB_BUCKETS + b;
            let c = hist[idx];
            off[idx] = run;
            run += c;
        }
    }
    off[nt * MSB_BUCKETS] = run;
    // scatter
    let mut out: Vec<std::mem::MaybeUninit<T>> =
        (0..n).map(|_| std::mem::MaybeUninit::uninit()).collect();
    let out_ptr = Ptr::new(out.as_mut_slice());
    let off_ptr = Ptr::new(off.as_mut_slice());
    run_parallel(nt, nt, |_, t| {
        let start = n * t / nt;
        let end = n * (t + 1) / nt;
        let items = items_ptr.r();
        let offs = off_ptr.r();
        let mut local = vec![0u32; MSB_BUCKETS];
        for i in start..end {
            let b = (key(&items[i]) >> (64 - MSB_BITS)) as usize;
            local[b] += 1;
        }
        for b in 0..MSB_BUCKETS {
            local[b] = offs[t * MSB_BUCKETS + b];
        }
        for i in start..end {
            let b = (key(&items[i]) >> (64 - MSB_BITS)) as usize;
            unsafe {
                let dst: *mut T = out_ptr.at(local[b] as usize) as *mut T;
                std::ptr::copy_nonoverlapping(&items[i] as *const T, dst, 1);
                local[b] += 1;
            }
        }
    });
    items.clear();
    let cap = out.capacity();
    let raw = out.as_mut_ptr() as *mut T;
    std::mem::forget(out);
    let out: Vec<T> = unsafe { Vec::from_raw_parts(raw, n, cap) };
    if std::env::var("SHOPSTATS_DBG").is_ok() {
        eprintln!("msb_partition: n={n} nt={nt} run={run} out.len={}", out.len());
    }
    let _ = &out;
    // bucket b occupies [off[b], off[b+1]) — slice-0 offsets, laid out bucket-major
    let mut starts = vec![0usize; MSB_BUCKETS + 1];
    for b in 0..MSB_BUCKETS {
        starts[b] = off[b] as usize;
    }
    starts[MSB_BUCKETS] = run as usize;
    let mut buckets: Vec<Vec<T>> = Vec::with_capacity(MSB_BUCKETS);
    for b in 0..MSB_BUCKETS {
        buckets.push(out[starts[b]..starts[b + 1]].to_vec());
    }
    buckets
}

pub struct Built {
    pub tables: LinkTables,
    pub n_nodes: usize,
}

pub fn build(files: &Files, worker_sides: &[Vec<LinkSide>], nthreads: usize) -> Built {
    let empty = Built {
        tables: LinkTables {
            parts: (0..NPART).map(|_| Vec::new()).collect(),
            hashes: (0..NPART).map(|_| Vec::new()).collect(),
        },
        n_nodes: 0,
    };
    let mut sides = concat_sides(worker_sides);
    let n_sides = sides.len();
    if n_sides == 0 {
        return empty;
    }

    // ---- sort sides by (h_hi, h_lo): MSB partition then per-bucket sort
    let mut buckets = msb_partition(&mut sides, |s| s.h_hi, nthreads);
    let buckets_ptr = Ptr::new(buckets.as_mut_slice());
    run_parallel(nthreads, MSB_BUCKETS, |_t, b| {
        let bk = unsafe { &mut *buckets_ptr.at(b) };
        bk.sort_unstable_by(|a, b| a.h_hi.cmp(&b.h_hi).then(a.h_lo.cmp(&b.h_lo)));
    });
    // ---- distinct counts per bucket, then prefix bases
    let mut bucket_base = vec![0u32; MSB_BUCKETS + 1];
    for b in 0..MSB_BUCKETS {
        let bk = &buckets[b];
        let mut distinct = 0u32;
        let mut i = 0;
        while i < bk.len() {
            let mut j = i + 1;
            while j < bk.len() && bk[j].h_hi == bk[i].h_hi && bk[j].h_lo == bk[i].h_lo {
                j += 1;
            }
            distinct += 1;
            i = j;
        }
        bucket_base[b + 1] = distinct;
    }
    let mut base = 0u32;
    for b in 0..MSB_BUCKETS {
        bucket_base[b] = base;
        base += bucket_base[b + 1];
    }
    bucket_base[MSB_BUCKETS] = base;
    let n_nodes = base as usize;
    if n_nodes == 0 {
        return empty;
    }

    let mut nodes: Vec<NodeInfo> = vec![
        NodeInfo {
            h_hi: 0,
            loc: 0,
            h_lo: 0,
            len: 0
        };
        n_nodes
    ];
    let mut side_node: Vec<u32> = vec![0u32; n_sides];
    let nodes_ptr = Ptr::new(nodes.as_mut_slice());
    let side_node_ptr = Ptr::new(side_node.as_mut_slice());
    let bucket_base_ptr = Ptr::new(bucket_base.as_mut_slice());
    run_parallel(nthreads, MSB_BUCKETS, |_t, b| {
        let bk = unsafe { &*buckets_ptr.at(b) };
        let mut nid = *bucket_base_ptr.elem(b);
        let mut i = 0;
        while i < bk.len() {
            let mut j = i + 1;
            while j < bk.len() && bk[j].h_hi == bk[i].h_hi && bk[j].h_lo == bk[i].h_lo {
                j += 1;
            }
            unsafe {
                *nodes_ptr.at(nid as usize) = NodeInfo {
                    h_hi: bk[i].h_hi,
                    loc: bk[i].loc,
                    h_lo: bk[i].h_lo,
                    len: bk[i].len,
                };
                for k in i..j {
                    *side_node_ptr.at(bk[k].pair as usize) = nid;
                }
            }
            nid += 1;
            i = j;
        }
    });
    drop(buckets);

    // ---- union-find over pairs (sequential; small and overlaps parsing)
    let mut parent: Vec<u32> = (0..n_nodes as u32).collect();
    for r in 0..(n_sides / 2) {
        let mut ra = side_node[2 * r] as usize;
        while parent[ra] as usize != ra {
            ra = parent[ra] as usize;
        }
        let mut rb = side_node[2 * r + 1] as usize;
        while parent[rb] as usize != rb {
            rb = parent[rb] as usize;
        }
        if ra != rb {
            if ra < rb {
                parent[rb] = ra as u32;
            } else {
                parent[ra] = rb as u32;
            }
        }
    }
    // flatten roots
    for i in 0..n_nodes {
        let mut r = i;
        while parent[r] as usize != r {
            r = parent[r] as usize;
        }
        let mut x = i;
        while x != r {
            let nx = parent[x] as usize;
            parent[x] = r as u32;
            x = nx;
        }
    }

    // ---- per-root min name: bucket nodes by root, min string per root
    #[derive(Clone, Copy)]
    struct NameEnt {
        root: u32,
        loc: u64,
        len: u16,
    }
    let mut ents: Vec<NameEnt> = (0..n_nodes)
        .map(|i| NameEnt {
            root: parent[i],
            loc: nodes[i].loc,
            len: nodes[i].len,
        })
        .collect();
    let mut nbuckets = msb_partition(&mut ents, |e| e.root as u64, nthreads);
    drop(ents);
    let nbuckets_ptr = Ptr::new(nbuckets.as_mut_slice());
    run_parallel(nthreads, MSB_BUCKETS, |_t, b| {
        let bk = unsafe { &mut *nbuckets_ptr.at(b) };
        bk.sort_unstable_by_key(|e| e.root);
    });
    let mut name_of_root: Vec<(u64, u16)> = vec![(0, 0); n_nodes];
    let name_ptr = Ptr::new(name_of_root.as_mut_slice());
    run_parallel(nthreads, MSB_BUCKETS, |_t, b| {
        let bk = unsafe { &*nbuckets_ptr.at(b) };
        let mut i = 0;
        while i < bk.len() {
            let mut j = i + 1;
            while j < bk.len() && bk[j].root == bk[i].root {
                j += 1;
            }
            let mut best = (bk[i].loc, bk[i].len);
            for e in &bk[i + 1..j] {
                let a = files.str_at(best.0, best.1);
                let bb = files.str_at(e.loc, e.len);
                if bb < a {
                    best = (e.loc, e.len);
                }
            }
            unsafe {
                *name_ptr.at(bk[i].root as usize) = best;
            }
            i = j;
        }
    });
    drop(nbuckets);

    // ---- per-partition link tables (nodes are in key order already)
    let mut parts: Vec<Vec<LinkEnt>> = (0..NPART).map(|_| Vec::new()).collect();
    for i in 0..n_nodes {
        let n = &nodes[i];
        let r = parent[i] as usize;
        let rn = &nodes[r];
        let p = n.h_hi as usize & (NPART - 1);
        parts[p].push(LinkEnt {
            uh_hi: n.h_hi,
            root_hi: rn.h_hi,
            name_loc: name_of_root[r].0,
            root_lo: rn.h_lo,
            name_len: name_of_root[r].1,
            uh_lo: n.h_lo,
        });
    }
    // ---- per-partition hashes: uh_hi -> index
    let parts_ptr = Ptr::new(parts.as_mut_slice());
    let hashes: Vec<Vec<u32>> = (0..NPART)
        .map(|p| {
            let part = parts_ptr.elem(p);
            let cap = (part.len() * 2 + 16).max(4).next_power_of_two();
            let mut h = vec![u32::MAX; cap];
            let mask = cap - 1;
            for (i, e) in part.iter().enumerate() {
                let mut at = e.uh_hi as usize & mask;
                while h[at] != u32::MAX {
                    at = (at + 1) & mask;
                }
                h[at] = i as u32;
            }
            h
        })
        .collect();
    Built {
        tables: LinkTables { parts, hashes },
        n_nodes,
    }
}

impl LinkTables {
    /// Look up a uh; returns the LinkEnt index in parts[p] or None.
    #[inline]
    pub fn find(&self, p: usize, h_hi: u64, h_lo: u16) -> Option<u32> {
        let part = &self.parts[p];
        if part.is_empty() {
            return None;
        }
        let h = &self.hashes[p];
        let mask = h.len() - 1;
        let mut at = h_hi as usize & mask;
        loop {
            let idx = h[at];
            if idx == u32::MAX {
                return None;
            }
            let e = &part[idx as usize];
            if e.uh_hi == h_hi {
                if e.uh_lo == h_lo {
                    return Some(idx);
                }
                // same uh_hi, different uh_lo: keep probing
            }
            at = (at + 1) & mask;
        }
    }
}

/// Count of finished link chunks, so the coordinator knows when to build.
pub struct LinkProgress(AtomicUsize);
impl LinkProgress {
    pub const fn new() -> Self {
        LinkProgress(AtomicUsize::new(0))
    }
    pub fn done_one(&self) {
        self.0.fetch_add(1, Ordering::Release);
    }
    pub fn wait(&self, expect: usize) {
        while self.0.load(Ordering::Acquire) < expect {
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xorshift(s: &mut u64) -> u64 {
        *s ^= *s << 13;
        *s ^= *s >> 7;
        *s ^= *s << 17;
        *s
    }

    #[test]
    fn msb_partition_random() {
        for trial in 0..8usize {
            let nth = 1 + (trial % 4);
            let mut s = 0x9E3779B97F4A7C15 ^ (trial as u64);
            let n = 1000 + (xorshift(&mut s) % 200_000) as usize;
            let mut items: Vec<LinkSide> = (0..n)
                .map(|i| LinkSide {
                    h_hi: xorshift(&mut s),
                    loc: i as u64,
                    pair: i as u64,
                    h_lo: (xorshift(&mut s) & 0xFFFF) as u16,
                    len: (xorshift(&mut s) & 0xFF) as u16,
                })
                .collect();
            let mut v = items.clone();
            let buckets = msb_partition(&mut v, |x| x.h_hi, nth);
            let mut total = 0usize;
            let mut bad = 0usize;
            for (bi, bk) in buckets.iter().enumerate() {
                total += bk.len();
                for x in bk {
                    let want = (x.h_hi >> (64 - MSB_BITS)) as usize;
                    if want != bi {
                        bad += 1;
                    }
                }
            }
            eprintln!("trial {trial} n {n} nth {nth} total {total} bad {bad}");
            assert_eq!(total, n, "trial {trial}");
            assert_eq!(bad, 0, "trial {trial}: {bad} misplaced");
        }
    }
}
