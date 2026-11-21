//! Phase C: within each user-hash partition, group events and clicks by user
//! id, resolve each user to a person through the link tables, and emit
//! per-person payload runs plus group entries into person-hash partitions.

use crate::common::*;
use crate::links::LinkTables;
use crate::parse::NPART;

pub const SEG_BITS: u32 = 4; // segment id bits in run_off (up to 16 workers)
pub const OFF_BITS: u32 = 32 - SEG_BITS;

/// One worker's emissions, per person-hash partition.
pub struct PersonCol {
    pub ev: Vec<Vec<EvPay>>, // [partition]
    pub cl: Vec<Vec<ClPay>>, // [partition]
    pub ents: Vec<Vec<GroupEnt>>, // [partition]
}

impl PersonCol {
    pub fn new(est_events: usize, est_clicks: usize, nworkers: usize) -> PersonCol {
        PersonCol {
            ev: (0..NPART)
                .map(|_| {
                    Vec::with_capacity((est_events / NPART / nworkers * 6 / 5).max(1024))
                })
                .collect(),
            cl: (0..NPART)
                .map(|_| {
                    Vec::with_capacity((est_clicks / NPART / nworkers * 6 / 5).max(256))
                })
                .collect(),
            ents: (0..NPART).map(|_| Vec::new()).collect(),
        }
    }
}

#[derive(Clone, Copy)]
struct GSlot {
    uh_hi: u64,
    uh_lo: u16,
    ev_count: u32,
    cl_count: u32,
    ev_base: u32, // running cursor during placement
    cl_base: u32,
    loc: u64,
    id_len: u16,
}

impl GSlot {
    const EMPTY: GSlot = GSlot {
        uh_hi: 0,
        uh_lo: 0,
        ev_count: 0,
        cl_count: 0,
        ev_base: 0,
        cl_base: 0,
        loc: 0,
        id_len: 0,
    };
}

struct GroupTable {
    slots: Vec<GSlot>,
    mask: usize,
    count: usize,
}

impl GroupTable {
    fn with_capacity_hint(records: usize) -> GroupTable {
        // distinct users are far fewer than records; this is just an upper
        // bound that avoids rehashing entirely
        let cap = records.max(1024).next_power_of_two();
        GroupTable {
            slots: vec![GSlot::EMPTY; cap],
            mask: cap - 1,
            count: 0,
        }
    }

    #[inline]
    fn find_slot(&self, uh_hi: u64, uh_lo: u16) -> usize {
        let mut h = uh_hi;
        if h == 0 {
            h = 1;
        }
        let mut at = h as usize & self.mask;
        loop {
            let s = self.slots[at];
            if s.uh_hi == h && s.uh_lo == uh_lo {
                return at;
            }
            at = (at + 1) & self.mask;
        }
    }

    #[inline]
    fn count_one(&mut self, uh_hi: u64, uh_lo: u16, loc: u64, id_len: u16, is_click: bool) {
        let mut h = uh_hi;
        if h == 0 {
            h = 1;
        }
        let mut at = h as usize & self.mask;
        loop {
            let s = self.slots[at];
            if s.uh_hi == 0 {
                if (self.count + 1) * 4 >= self.slots.len() * 3 {
                    self.grow();
                    self.count_one(uh_hi, uh_lo, loc, id_len, is_click);
                    return;
                }
                self.slots[at] = GSlot {
                    uh_hi: h,
                    uh_lo,
                    ev_count: 0,
                    cl_count: 0,
                    ev_base: 0,
                    cl_base: 0,
                    loc,
                    id_len,
                };
                self.count += 1;
                let e = &mut self.slots[at];
                if is_click {
                    e.cl_count += 1;
                } else {
                    e.ev_count += 1;
                }
                return;
            }
            if s.uh_hi == h && s.uh_lo == uh_lo {
                let e = &mut self.slots[at];
                if is_click {
                    e.cl_count += 1;
                } else {
                    e.ev_count += 1;
                }
                return;
            }
            at = (at + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let newcap = self.slots.len() * 2;
        let old = std::mem::replace(&mut self.slots, vec![GSlot::EMPTY; newcap]);
        self.mask = self.slots.len() - 1;
        for s in old {
            if s.uh_hi != 0 {
                let mut at = s.uh_hi as usize & self.mask;
                while self.slots[at].uh_hi != 0 {
                    at = (at + 1) & self.mask;
                }
                self.slots[at] = s;
            }
        }
    }
}

/// Process user-hash partition `p`: `ev_segs` and `cl_segs` are the
/// per-worker segments; emit into `out` under worker slot `tid`.
pub fn group_partition(
    p: usize,
    ev_segs: &[&[EvRec2]],
    cl_segs: &[&[ClRec]],
    links: &LinkTables,
    out: &mut PersonCol,
    tid: usize,
) {
    // ---- pass 1: count per distinct user
    let n_records: usize = ev_segs.iter().map(|s| s.len()).sum::<usize>()
        + cl_segs.iter().map(|s| s.len()).sum::<usize>();
    let mut table = GroupTable::with_capacity_hint(n_records);
    for seg in ev_segs {
        for r in *seg {
            table.count_one(r.uh_hi, r.uh_lo, r.loc, r.id_len, false);
        }
    }
    for seg in cl_segs {
        for r in *seg {
            table.count_one(r.uh_hi, r.uh_lo, r.user_loc, r.user_len, true);
        }
    }

    // ---- assign payload bases and snapshot the groups
    struct G {
        uh_hi: u64,
        uh_lo: u16,
        ev_base: u32,
        ev_count: u32,
        cl_base: u32,
        cl_count: u32,
        loc: u64,
        id_len: u16,
    }
    let mut groups: Vec<G> = Vec::with_capacity(table.count);
    let mut ev_run = 0u32;
    let mut cl_run = 0u32;
    for s in table.slots.iter_mut() {
        if s.uh_hi == 0 {
            continue;
        }
        let ev_base = ev_run;
        let cl_base = cl_run;
        ev_run += s.ev_count;
        cl_run += s.cl_count;
        s.ev_base = ev_base;
        s.cl_base = cl_base;
        groups.push(G {
            uh_hi: s.uh_hi,
            uh_lo: s.uh_lo,
            ev_base,
            ev_count: s.ev_count,
            cl_base,
            cl_count: s.cl_count,
            loc: s.loc,
            id_len: s.id_len,
        });
    }
    let mut ev_arena: Vec<EvPay> = vec![
        EvPay {
            ts_delta: 0,
            price: 0,
            cat_kind: 0
        };
        ev_run as usize
    ];
    let mut cl_arena: Vec<ClPay> = vec![
        ClPay {
            ts_delta: 0,
            camp: 0,
            cid_len: 0,
            cid_loc: 0
        };
        cl_run as usize
    ];

    // ---- pass 2: place payloads
    for seg in ev_segs {
        for r in *seg {
            let at = table.find_slot(r.uh_hi, r.uh_lo);
            let s = &mut table.slots[at];
            ev_arena[s.ev_base as usize] = EvPay {
                ts_delta: r.ts_delta,
                price: r.price,
                cat_kind: (r.cat << 2) | r.kind as u16,
            };
            s.ev_base += 1;
        }
    }
    for seg in cl_segs {
        for r in *seg {
            let at = table.find_slot(r.uh_hi, r.uh_lo);
            let s = &mut table.slots[at];
            cl_arena[s.cl_base as usize] = ClPay {
                ts_delta: r.ts_delta,
                camp: r.camp,
                cid_len: r.cid_len,
                cid_loc: r.cid_loc,
            };
            s.cl_base += 1;
        }
    }

    // ---- emit groups to person partitions
    for g in groups {
        let (root_hi, root_lo) = match links.find(p, g.uh_hi, g.uh_lo) {
            Some(i) => {
                let e = &links.parts[p][i as usize];
                (e.root_hi, e.root_lo)
            }
            None => (g.uh_hi, g.uh_lo),
        };
        let q = root_hi as usize & (NPART - 1);
        if g.ev_count > 0 {
            let seg = &mut out.ev[q];
            let run_off = ((tid as u32) << OFF_BITS) | seg.len() as u32;
            seg.extend_from_slice(&ev_arena[g.ev_base as usize..(g.ev_base + g.ev_count) as usize]);
            out.ents[q].push(GroupEnt {
                root_hi,
                loc: g.loc,
                run_off,
                len: g.ev_count,
                root_lo,
                id_len: g.id_len,
                is_click: 0,
            });
        }
        if g.cl_count > 0 {
            let seg = &mut out.cl[q];
            let run_off = ((tid as u32) << OFF_BITS) | seg.len() as u32;
            seg.extend_from_slice(&cl_arena[g.cl_base as usize..(g.cl_base + g.cl_count) as usize]);
            out.ents[q].push(GroupEnt {
                root_hi,
                loc: g.loc,
                run_off,
                len: g.cl_count,
                root_lo,
                id_len: g.id_len,
                is_click: 1,
            });
        }
    }
}
