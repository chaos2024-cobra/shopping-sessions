//! Phase D: per person-hash partition, regroup entries by person, then
//! sessionize each person's events, credit sessions to the latest click,
//! count assisting campaigns, and accumulate both daily tables.

use crate::common::*;
use crate::group::{PersonCol, OFF_BITS};
use crate::links::LinkTables;
use crate::lookup::ORGANIC;
use crate::ts::{DAY_MS, WEEK_MS};

pub const SESSION_GAP_MS: i32 = 1_800_000;

/// One output row of user_daily; the name lives in the worker's arena.
#[derive(Clone, Copy)]
pub struct UserRow {
    pub name_off: u32,
    pub name_len: u16,
    pub day: i32, // epoch days
    pub sessions: u32,
    pub events: u32,
    pub revenue: u64,
    pub top_cat: i16, // -1 null
}

/// Per (campaign, day) counters for one worker. Layout: campaigns 0..nc,
/// organic at index nc; `nd` day columns.
pub struct CampAcc {
    pub nc: usize,
    pub nd: usize,
    pub sess: Vec<u32>,
    pub purch: Vec<u32>,
    pub rev: Vec<u64>,
    pub assist: Vec<u32>,
}

impl CampAcc {
    pub fn new(nc: usize, nd: usize) -> CampAcc {
        let nd = nd.max(1);
        let cells = (nc + 1) * nd;
        CampAcc {
            nc,
            nd,
            sess: vec![0; cells],
            purch: vec![0; cells],
            rev: vec![0; cells],
            assist: vec![0; cells],
        }
    }

    #[inline]
    fn cell(&self, camp: u16, day: usize) -> usize {
        let c = if camp == ORGANIC {
            self.nc
        } else {
            camp as usize
        };
        c * self.nd + day
    }
}

pub struct WalkOut {
    pub rows: Vec<UserRow>,
    pub acc: CampAcc,
    /// Person names, concatenated; rows reference (name_off, name_len).
    pub names: Vec<u8>,
}

pub fn make_walk_out(nc: usize, nd: usize) -> WalkOut {
    WalkOut {
        rows: Vec::new(),
        acc: CampAcc::new(nc, nd),
        names: Vec::new(),
    }
}

impl CampAcc {
    pub fn merge_into(&mut self, other: &CampAcc) {
        for i in 0..self.sess.len() {
            self.sess[i] += other.sess[i];
            self.purch[i] += other.purch[i];
            self.rev[i] += other.rev[i];
            self.assist[i] += other.assist[i];
        }
    }
}

pub struct Scratch {
    ev: Vec<EvPay>,
    cl: Vec<ClPay>,
    starts: Vec<i64>, // session start, absolute ms
    sday: Vec<u32>,   // session day index within the window
    sev: Vec<u32>,    // events per session
    srev: Vec<u64>,   // revenue per session
    spurch: Vec<bool>,
    cat_rev: Vec<u64>, // per (day, category) revenue: day * ncat + cat
    cl_by_camp: Vec<ClPay>,
    credited: Vec<u16>,
    scat: Vec<u16>, // per-session top tracking is via cat_rev; scratch for names
    win_a: Vec<i64>,
    win_b: Vec<i64>,
    day_mask: u32,
}

impl Scratch {
    pub fn new(ncat: usize, nd: usize) -> Scratch {
        Scratch {
            ev: Vec::new(),
            cl: Vec::new(),
            starts: Vec::new(),
            sday: Vec::new(),
            sev: Vec::new(),
            srev: Vec::new(),
            spurch: Vec::new(),
            cat_rev: vec![0; ncat.saturating_mul(nd.max(1))],
            cl_by_camp: Vec::new(),
            credited: Vec::new(),
            scat: Vec::new(),
            win_a: Vec::new(),
            win_b: Vec::new(),
            day_mask: 0,
        }
    }
}

#[derive(Default)]
pub struct WalkTimers {
    pub on: bool,
    pub gather: u128,
    pub sort: u128,
    pub sess: u128,
    pub credit: u128,
    pub assist: u128,
    pub rows: u128,
}

#[derive(Clone, Copy)]
struct PSlot {
    root_hi: u64,
    root_lo: u16,
    count: u32,
    base: u32,
}

/// Walk one person-hash partition. `cols` holds every worker's PersonCol.
pub fn walk_partition(
    q: usize,
    cols: &[PersonCol],
    links: &LinkTables,
    files: &Files,
    ws: i64,
    ws_days: i32,
    nd: usize,
    ncat: usize,
    out: &mut WalkOut,
    scratch: &mut Scratch,
) {
    let dbg = std::env::var("SHOPSTATS_DBG").is_ok();
    let t_start = crate::common::now_ms();
    let mut timers = WalkTimers {
        on: dbg,
        ..Default::default()
    };
    let mut t_gather: u128 = 0;
    let mut t_sort: u128 = 0;
    let n_ents: usize = cols.iter().map(|c| c.ents[q].len()).sum();
    if n_ents == 0 {
        return;
    }
    let mut ents: Vec<GroupEnt> = Vec::with_capacity(n_ents);
    for c in cols.iter() {
        ents.extend_from_slice(&c.ents[q]);
    }

    // ---- group entries by person (root)
    let cap = (n_ents * 2).next_power_of_two().max(16);
    let mut slots: Vec<PSlot> = vec![
        PSlot {
            root_hi: 0,
            root_lo: 0,
            count: 0,
            base: 0
        };
        cap
    ];
    let mask = cap - 1;
    let mut n_persons = 0usize;
    for e in ents.iter() {
        let mut h = e.root_hi;
        if h == 0 {
            h = 1;
        }
        let mut at = h as usize & mask;
        loop {
            let s = slots[at];
            if s.root_hi == 0 {
                slots[at] = PSlot {
                    root_hi: h,
                    root_lo: e.root_lo,
                    count: 1,
                    base: 0,
                };
                n_persons += 1;
                break;
            }
            if s.root_hi == h && s.root_lo == e.root_lo {
                slots[at].count += 1;
                break;
            }
            at = (at + 1) & mask;
        }
    }
    let mut persons: Vec<(u32, u32)> = Vec::with_capacity(n_persons); // (base, count)
    let mut base = 0u32;
    for s in slots.iter_mut() {
        if s.root_hi == 0 {
            continue;
        }
        s.base = base;
        base += s.count;
        persons.push((s.base, s.count));
    }
    let mut grouped: Vec<&GroupEnt> = vec![&ents[0]; n_ents];
    {
        let mut cursor: Vec<u32> = vec![0; cap];
        for e in ents.iter() {
            let mut h = e.root_hi;
            if h == 0 {
                h = 1;
            }
            let mut at = h as usize & mask;
            loop {
                let s = slots[at];
                if s.root_hi == h && s.root_lo == e.root_lo {
                    let g = s.base + cursor[at];
                    cursor[at] += 1;
                    grouped[g as usize] = e;
                    break;
                }
                at = (at + 1) & mask;
            }
        }
    }

    if dbg {
        eprintln!("part {q}: ents {n_ents} persons {} group_ms {}", persons.len(), crate::common::now_ms() - t_start);
    }
    let cw = ws - WEEK_MS; // click window start
    // Resolve every person's name location first, then walk persons in
    // name-loc order so the (cold) input reads are sequential sweeps.
    struct PWalk {
        pbase: u32,
        pcount: u32,
        name_loc: u64,
        name_len: u16,
    }
    let mut walk_list: Vec<PWalk> = Vec::with_capacity(persons.len());
    for &(pbase, pcount) in persons.iter() {
        let ent_list = &grouped[pbase as usize..(pbase + pcount) as usize];
        // name: linked persons take the component name; unlinked their own id
        let (name_loc, name_len) = match links.find(q, ent_list[0].root_hi, ent_list[0].root_lo) {
            Some(i) => {
                let le = &links.parts[q][i as usize];
                (le.name_loc, le.name_len)
            }
            None => {
                let mut fe: Option<&GroupEnt> = None;
                for e in ent_list {
                    if e.is_click == 0 {
                        fe = Some(e);
                        break;
                    }
                }
                match fe {
                    Some(e) => (e.loc, e.id_len),
                    None => continue, // no events: no sessions, no rows
                }
            }
        };
        walk_list.push(PWalk {
            pbase,
            pcount,
            name_loc,
            name_len,
        });
    }
    walk_list.sort_unstable_by_key(|w| w.name_loc);

    for pw in walk_list.iter() {
        let tg0 = if dbg { crate::common::now_ms() } else { 0 };
        let ent_list = &grouped[pw.pbase as usize..(pw.pbase + pw.pcount) as usize];
        let (name_loc, name_len) = (pw.name_loc, pw.name_len);

        // gather events
        scratch.ev.clear();
        for e in ent_list {
            if e.is_click != 0 {
                continue;
            }
            let tid = (e.run_off >> OFF_BITS) as usize;
            let off = (e.run_off & ((1 << OFF_BITS) - 1)) as usize;
            scratch
                .ev
                .extend_from_slice(&cols[tid].ev[q][off..off + e.len as usize]);
        }
        if scratch.ev.is_empty() {
            continue; // no events: no sessions, no rows
        }
        if dbg {
            t_gather += crate::common::now_ms() - tg0;
        }
        let ts0 = if dbg { crate::common::now_ms() } else { 0 };
        scratch.ev.sort_unstable_by_key(|e| e.ts_delta);

        // gather clicks
        scratch.cl.clear();
        for e in ent_list {
            if e.is_click == 0 {
                continue;
            }
            let tid = (e.run_off >> OFF_BITS) as usize;
            let off = (e.run_off & ((1 << OFF_BITS) - 1)) as usize;
            scratch
                .cl
                .extend_from_slice(&cols[tid].cl[q][off..off + e.len as usize]);
        }
        scratch.cl.sort_unstable_by_key(|c| c.ts_delta);
        if dbg {
            t_sort += crate::common::now_ms() - ts0;
        }

        let ev = std::mem::take(&mut scratch.ev);
        let cl = std::mem::take(&mut scratch.cl);
        let t_p = if dbg { crate::common::now_ms() } else { 0 };
        let n_ev_p = ev.len();
        let n_cl_p = cl.len();
        walk_person(
            &ev,
            &cl,
            name_loc,
            name_len,
            files,
            ws,
            ws_days,
            cw,
            nd,
            ncat,
            out,
            scratch,
            &mut timers,
        );
        scratch.ev = ev;
        scratch.cl = cl;
        if dbg && n_ev_p > 50000 {
            eprintln!("  big person: ev {n_ev_p} cl {n_cl_p} walk_ms {}", crate::common::now_ms() - t_p);
        }
        let _ = t_p;
    }
    if dbg {
        let t_end = crate::common::now_ms();
        eprintln!(
            "part {q}: walk_total_ms {} (gather {t_gather} sort {t_sort} sess {} credit {} assist {} rows {})",
            t_end - t_start, timers.sess, timers.credit, timers.assist, timers.rows
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn walk_person(
    ev: &[EvPay],
    cl: &[ClPay],
    name_loc: u64,
    name_len: u16,
    files: &Files,
    ws: i64,
    ws_days: i32,
    cw: i64,
    nd: usize,
    ncat: usize,
    out: &mut WalkOut,
    scratch: &mut Scratch,
    timers: &mut WalkTimers,
) {
    let tw0 = if timers.on { crate::common::now_ms() } else { 0 };
    // (ev and cl are passed in as slices; see walk_partition)
    // ---- sessionize (single pass: starts, days, events, revenue, category revenue)
    scratch.starts.clear();
    scratch.sday.clear();
    scratch.sev.clear();
    scratch.srev.clear();
    scratch.spurch.clear();
    // clear only the day rows the previous person touched
    let mut dm = scratch.day_mask;
    while dm != 0 {
        let d = dm.trailing_zeros() as usize;
        dm &= dm - 1;
        let row = d * ncat;
        for v in scratch.cat_rev[row..row + ncat].iter_mut() {
            *v = 0;
        }
    }
    scratch.day_mask = 0;
    let mut i = 0usize;
    while i < ev.len() {
        let start = ev[i].ts_delta;
        let mut j = i + 1;
        while j < ev.len() && (ev[j].ts_delta - ev[j - 1].ts_delta) <= SESSION_GAP_MS {
            j += 1;
        }
        let abs = ws + start as i64;
        let day_idx = (abs.div_euclid(DAY_MS) as i32 - ws_days) as usize;
        if day_idx < nd {
            scratch.day_mask |= 1 << day_idx;
        }
        let mut n_e = 0u32;
        let mut rev = 0u64;
        let mut purch = false;
        for e in &ev[i..j] {
            n_e += 1;
            if (e.cat_kind & 3) == 2 {
                purch = true;
                rev += e.price as u64;
                if day_idx < nd {
                    scratch.cat_rev[day_idx * ncat + (e.cat_kind >> 2) as usize] +=
                        e.price as u64;
                }
            }
        }
        scratch.starts.push(abs);
        scratch.sday.push(day_idx as u32);
        scratch.sev.push(n_e);
        scratch.srev.push(rev);
        scratch.spurch.push(purch);
        i = j;
    }
    let n_sess = scratch.starts.len();
    if timers.on {
        timers.sess += crate::common::now_ms() - tw0;
    }

    // ---- crediting: latest click at or before each session start; ties on
    // ts go to the byte-wise smallest click_id
    let t1 = if timers.on { crate::common::now_ms() } else { 0 };
    scratch.credited.clear();
    scratch.credited.resize(n_sess, ORGANIC);
    {
        let credited = &mut scratch.credited;
        let mut cp = 0usize;
        let mut cand: Option<&ClPay> = None;
        let mut cand_ts = i64::MIN;
        for s in 0..n_sess {
            let st = scratch.starts[s];
            while cp < cl.len() && cw + cl[cp].ts_delta as i64 <= st {
                let c = &cl[cp];
                let cts = cw + c.ts_delta as i64;
                let take = match cand {
                    None => true,
                    Some(k) => {
                        if cts > cand_ts {
                            true
                        } else if cts == cand_ts {
                            let a = files.str_at(c.cid_loc, c.cid_len);
                            let b = files.str_at(k.cid_loc, k.cid_len);
                            a < b
                        } else {
                            false
                        }
                    }
                };
                if take {
                    cand = Some(c);
                    cand_ts = cts;
                }
                cp += 1;
            }
            if let Some(k) = cand {
                if st - cand_ts <= WEEK_MS {
                    credited[s] = k.camp;
                }
            }
        }
    }

    if timers.on {
        timers.credit += crate::common::now_ms() - t1;
    }
    // ---- account credited sessions into campaign_daily
    {
        let acc = &mut out.acc;
        let credited = &scratch.credited;
        for s in 0..n_sess {
            let d = scratch.sday[s] as usize;
            if d >= nd {
                continue;
            }
            let cell = acc.cell(credited[s], d);
            acc.sess[cell] += 1;
            if scratch.spurch[s] {
                acc.purch[cell] += 1;
            }
            acc.rev[cell] += scratch.srev[s];
        }
    }

    let t2 = if timers.on { crate::common::now_ms() } else { 0 };
    // ---- assists: one pass over the ts-sorted clicks, keeping an open
    // [t, t+7d] window per campaign (direct-indexed by campaign id) and
    // merging as clicks arrive (each campaign's clicks arrive in ts order
    // because the whole list is ts-sorted)
    if !cl.is_empty() {
        let ncamp = out.acc.nc + 1;
        // window state per campaign: (a, b) with a == i64::MIN meaning closed
        if scratch.win_a.len() < ncamp {
            scratch.win_a.resize(ncamp, i64::MIN);
            scratch.win_b.resize(ncamp, 0);
        }
        let mut touched: Vec<u16> = Vec::new();
        let starts = std::mem::take(&mut scratch.starts);
        let win_a = &mut scratch.win_a;
        let win_b = &mut scratch.win_b;
        for c in cl {
            let t = cw + c.ts_delta as i64;
            let k = c.camp as usize;
            if win_a[k] == i64::MIN {
                win_a[k] = t;
                win_b[k] = t + WEEK_MS;
                touched.push(c.camp);
            } else if t <= win_b[k] + 1 {
                let e2 = t + WEEK_MS;
                if e2 > win_b[k] {
                    win_b[k] = e2;
                }
            } else {
                let (a, b) = (win_a[k], win_b[k]);
                assist_window(&mut out.acc, c.camp, a, b, nd, ws_days, &starts);
                win_a[k] = t;
                win_b[k] = t + WEEK_MS;
            }
        }
        for &camp in touched.iter() {
            let (a, b) = (win_a[camp as usize], win_b[camp as usize]);
            assist_window(&mut out.acc, camp, a, b, nd, ws_days, &starts);
            win_a[camp as usize] = i64::MIN;
        }
        scratch.starts = starts;
    }

    if timers.on {
        timers.assist += crate::common::now_ms() - t2;
    }
    let t3 = if timers.on { crate::common::now_ms() } else { 0 };
    // ---- user rows per day (sessions are in ascending day order)
    {
        // materialize the name once per person into the arena
        let name = files.str_at(name_loc, name_len);
        let name_off = out.names.len() as u32;
        out.names.extend_from_slice(name);
        let rows = &mut out.rows;
        let mut s = 0usize;
        while s < n_sess {
            let d = scratch.sday[s] as usize;
            let mut e = s + 1;
            while e < n_sess && scratch.sday[e] as usize == d {
                e += 1;
            }
            if d < nd {
                let mut sessions = 0u32;
                let mut events = 0u32;
                let mut revenue = 0u64;
                for t in s..e {
                    sessions += 1;
                    events += scratch.sev[t];
                    revenue += scratch.srev[t];
                }
                let mut top: i16 = -1;
                if revenue > 0 {
                    let row = d * ncat;
                    let mut best_v: u64 = 0;
                    for c in 0..ncat {
                        let v = scratch.cat_rev[row + c];
                        if v > best_v {
                            best_v = v;
                            top = c as i16;
                        }
                    }
                }
                rows.push(UserRow {
                    name_off,
                    name_len,
                    day: ws_days + d as i32,
                    sessions,
                    events,
                    revenue,
                    top_cat: top,
                });
            }
            s = e;
        }
    }
    if timers.on {
        timers.rows += crate::common::now_ms() - t3;
    }
}

/// Add to `camp`'s assist counters every session whose start lies in [a, b].
fn assist_window(
    acc: &mut CampAcc,
    camp: u16,
    a: i64,
    b: i64,
    nd: usize,
    ws_days: i32,
    starts: &[i64],
) {
    let i0 = starts.partition_point(|&s| s < a);
    let i1 = starts.partition_point(|&s| s <= b);
    if i0 >= i1 {
        return;
    }
    let d0 = ((a.div_euclid(DAY_MS) as i32 - ws_days).max(0)) as usize;
    let d1 = (((b.div_euclid(DAY_MS) as i32 - ws_days).min(nd as i32 - 1)).max(0)) as usize;
    for d in d0..=d1 {
        if d >= nd {
            break;
        }
        let day_start = (ws_days + d as i32) as i64 * DAY_MS;
        let day_end = day_start + DAY_MS;
        let first = starts.partition_point(|&s| s < day_start);
        let last = starts.partition_point(|&s| s < day_end);
        let _ = &starts;
        let lo = i0.max(first);
        let hi = i1.min(last);
        if hi > lo {
            let cell = acc.cell(camp, d);
            acc.assist[cell] += (hi - lo) as u32;
        }
    }
}
