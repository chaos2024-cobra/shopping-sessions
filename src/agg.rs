//! Grouping survivors by person, sessionizing, crediting sessions to clicks,
//! counting assists, and accumulating both daily tables.

use crate::parse::{epoch_day, ClickTable, EventTable};
use crate::ts::WEEK_MS;

pub const SESSION_GAP_MS: i64 = 1_800_000;

/// A surviving event, grouped by person (the slice itself identifies the
/// person, so records only carry what aggregation needs).
#[derive(Clone, Copy, Default)]
pub struct SurvRec {
    pub ts: i64,
    pub price: u32,
    pub cat: u16,
    pub kind: u8, // 0 view, 1 cart, 2 purchase
}

/// A surviving click, grouped by person.
#[derive(Clone, Copy)]
pub struct ClickSurv {
    pub ts: i64,
    pub cid: &'static [u8],
    pub camp: u16,
}

impl Default for ClickSurv {
    fn default() -> Self {
        ClickSurv {
            ts: 0,
            cid: &[],
            camp: 0,
        }
    }
}

/// One output row of user_daily.
pub struct UserRow {
    pub person: u32,
    pub day: i32, // epoch days
    pub sessions: u32,
    pub events: u32,
    pub revenue: u64,
    pub top_cat: i16, // -1 for null
}

/// Per (campaign, day) counters for one worker thread. Layout: real campaigns
/// 0..nc, organic at index nc; `nd` day columns.
pub struct CampAcc {
    pub nc: usize,
    pub nd: usize,
    pub sess: Vec<u32>,
    pub purch: Vec<u32>,
    pub rev: Vec<u64>,
    pub assist: Vec<u32>,
}

impl CampAcc {
    pub fn new(nc: usize, nd: usize) -> Self {
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
        let c = if camp == crate::lookup::ORGANIC {
            self.nc
        } else {
            camp as usize
        };
        c * self.nd + day
    }
}

// ------------------------------------------------------------ grouping

/// Person-grouped survivors with prefix ranges (`len n_nodes + 1`).
pub struct Grouped {
    pub events: Vec<SurvRec>,
    pub event_base: Vec<u32>,
    pub clicks: Vec<ClickSurv>,
    pub click_base: Vec<u32>,
}

pub fn group_by_person(
    n_nodes: usize,
    nthreads: usize,
    events: &EventTable,
    clicks: &ClickTable,
    root: &[u32],
) -> Grouped {
    // ---- events: pass A (count), prefix, pass B (place)
    let (grouped, event_base) = group_generic::<SurvRec>(n_nodes, nthreads, |t, hist, out| {
        let nshards = events.shard_count();
        let mut s = t;
        while s < nshards {
            for rec in events.records(s) {
                if rec.keep() {
                    let p = root[rec.user as usize] as usize;
                    match out {
                        None => hist[p] += 1,
                        Some((base, _)) => unsafe {
                            let at = hist[p] as usize;
                            *base.add(at) = SurvRec {
                                ts: rec.ts,
                                price: rec.price,
                                cat: rec.cat,
                                kind: rec.kind,
                            };
                            hist[p] += 1;
                        },
                    }
                }
            }
            s += nthreads;
        }
    });

    // ---- clicks (same pattern)
    let (grouped_clicks, click_base) = group_generic::<ClickSurv>(n_nodes, nthreads, |t, hist, out| {
        let nshards = clicks.shard_count();
        let mut s = t;
        while s < nshards {
            let arena: &'static [u8] = unsafe { std::mem::transmute(clicks.arena(s)) };
            for rec in clicks.records(s) {
                if rec.ts_ok && rec.user_ok && rec.camp != crate::lookup::ORGANIC {
                    let p = root[rec.user as usize] as usize;
                    match out {
                        None => hist[p] += 1,
                        Some((base, _)) => unsafe {
                            let at = hist[p] as usize;
                            *base.add(at) = ClickSurv {
                                ts: rec.ts,
                                cid: &arena[rec.off as usize..rec.off as usize + rec.len as usize],
                                camp: rec.camp,
                            };
                            hist[p] += 1;
                        },
                    }
                }
            }
            s += nthreads;
        }
    });

    Grouped {
        events: grouped,
        event_base,
        clicks: grouped_clicks,
        click_base,
    }
}

/// Two-pass counting sort by person. The closure runs once per thread per pass:
/// pass 1 with `out = None` counts into `hist`, pass 2 with `out = Some((base, len))`
/// places records through `base` (disjoint offsets per thread via the cursors
/// prepared between passes).
fn group_generic<T: Copy + Default>(
    n_nodes: usize,
    nthreads: usize,
    work: impl Fn(usize, &mut [u32], Option<(*mut T, usize)>) + Sync + Send,
) -> (Vec<T>, Vec<u32>) {
    let mut hist: Vec<Vec<u32>> = (0..nthreads).map(|_| vec![0u32; n_nodes]).collect();
    let returned: Vec<Vec<u32>> = std::thread::scope(|scope| {
        let work = &work;
        let handles: Vec<_> = hist
            .into_iter()
            .enumerate()
            .map(|(t, mut h)| scope.spawn(move || {
                work(t, &mut h, None);
                h
            }))
            .collect();
        handles.into_iter().map(|j| j.join().unwrap()).collect()
    });
    hist = returned;
    // exclusive prefix sums; base has n_nodes + 1 entries
    let mut base = vec![0u32; n_nodes + 1];
    let mut total = 0u32;
    for p in 0..n_nodes {
        base[p] = total;
        let mut c = 0u32;
        for t in 0..nthreads {
            c += hist[t][p];
        }
        total += c;
    }
    base[n_nodes] = total;
    // per-thread write cursors
    for p in 0..n_nodes {
        let mut run = base[p];
        for t in 0..nthreads {
            let c = hist[t][p];
            hist[t][p] = run;
            run += c;
        }
    }
    let mut grouped: Vec<T> = vec![T::default(); total as usize];
    let base_ptr = SendPtr(grouped.as_mut_ptr());
    let out_len = total as usize;
    std::thread::scope(|scope| {
        let work = &work;
        let handles: Vec<_> = hist
            .into_iter()
            .enumerate()
            .map(|(t, mut h)| {
                scope.spawn(move || {
                    let ptr = base_ptr.as_ptr();
                    work(t, &mut h, Some((ptr, out_len)))
                })
            })
            .collect();
        handles.into_iter().for_each(|j| {
            j.join().unwrap();
        });
    });
    (grouped, base)
}

#[derive(Clone, Copy)]
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
impl<T> SendPtr<T> {
    fn as_ptr(&self) -> *mut T {
        self.0
    }
}

// ------------------------------------------------------------ per-person walk

pub struct WalkCtx<'a> {
    pub grouped: &'a Grouped,
    pub ws_days: i32,
    pub trace: bool,
    pub name_of: &'a (dyn Fn(u32) -> &'static [u8] + Sync),
}

pub struct Sweep {
    last_ts: Vec<i64>,
    in_active: Vec<bool>,
    pos: Vec<u32>,
    active: Vec<u16>,
    heap: std::collections::BinaryHeap<std::cmp::Reverse<(i64, u16)>>,
}

impl Sweep {
    pub fn new(nc: usize) -> Self {
        Sweep {
            last_ts: vec![0; nc],
            in_active: vec![false; nc],
            pos: vec![0; nc],
            active: Vec::new(),
            heap: std::collections::BinaryHeap::new(),
        }
    }

    fn reset(&mut self) {
        for &c in &self.active {
            self.in_active[c as usize] = false;
        }
        self.active.clear();
        self.heap.clear();
    }
}

#[inline]
fn flush_run(sweep: &Sweep, acc: &mut CampAcc, run_day: usize, run_count: u32) {
    if run_count > 0 {
        for &c in &sweep.active {
            let cell = acc.cell(c, run_day);
            acc.assist[cell] += run_count;
        }
    }
}

/// Process one person: sort their events and clicks, walk the sessions.
/// `events` and `clicks` are this person's disjoint ranges of the grouped
/// arrays (mutable so they can be sorted in place).
pub fn walk_person(
    person: u32,
    ctx: &WalkCtx,
    acc: &mut CampAcc,
    rows: &mut Vec<UserRow>,
    sweep: &mut Sweep,
    cat_rev: &mut [u64],
    events: &mut [SurvRec],
    clicks: &mut [ClickSurv],
) {
    if events.is_empty() {
        return;
    }
    events.sort_unstable_by_key(|e| e.ts);
    clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.cid.cmp(b.cid)));

    sweep.reset();

    let mut cur_day: i32 = i32::MIN;
    let mut day_sessions: u32 = 0;
    let mut day_events: u32 = 0;
    let mut day_revenue: u64 = 0;
    for v in cat_rev.iter_mut() {
        *v = 0;
    }

    let mut run_day: usize = 0;
    let mut run_count: u32 = 0;
    let mut have_run = false;

    let mut cp = 0usize;
    let mut cand: Option<(i64, u16, &'static [u8])> = None;

    let mut i = 0usize;
    while i < events.len() {
        let session_start = events[i].ts;
        let mut j = i + 1;
        while j < events.len() && events[j].ts - events[j - 1].ts <= SESSION_GAP_MS {
            j += 1;
        }
        let day_epoch = epoch_day(session_start);
        let day_idx = (day_epoch - ctx.ws_days) as usize;

        // --- attribution: flush the old run before mutating the active set
        let will_change = (cp < clicks.len() && clicks[cp].ts <= session_start)
            || sweep
                .heap
                .peek()
                .is_some_and(|r| r.0 .0 < session_start - WEEK_MS);
        if have_run && (will_change || run_day != day_idx) {
            flush_run(sweep, acc, run_day, run_count);
            run_count = 0;
        }
        while cp < clicks.len() && clicks[cp].ts <= session_start {
            let c = &clicks[cp];
            if cand.is_none() || c.ts > cand.unwrap().0 {
                cand = Some((c.ts, c.camp, c.cid));
            }
            let cu = c.camp as usize;
            if !sweep.in_active[cu] {
                sweep.in_active[cu] = true;
                sweep.pos[cu] = sweep.active.len() as u32;
                sweep.active.push(c.camp);
            }
            sweep.last_ts[cu] = c.ts;
            sweep.heap.push(std::cmp::Reverse((c.ts, c.camp)));
            cp += 1;
        }
        while let Some(&std::cmp::Reverse((t, c))) = sweep.heap.peek() {
            if t < session_start - WEEK_MS {
                sweep.heap.pop();
                let cu = c as usize;
                if sweep.in_active[cu] && sweep.last_ts[cu] == t {
                    let at = sweep.pos[cu] as usize;
                    let last = sweep.active.len() - 1;
                    sweep.active.swap(at, last);
                    sweep.active.pop();
                    if at < sweep.active.len() {
                        let moved = sweep.active[at];
                        sweep.pos[moved as usize] = at as u32;
                    }
                    sweep.in_active[cu] = false;
                }
            } else {
                break;
            }
        }
        run_day = day_idx;
        run_count += 1;
        have_run = true;

        // --- session aggregates
        let mut s_events: u32 = 0;
        let mut s_revenue: u64 = 0;
        let mut has_purchase = false;
        for e in &events[i..j] {
            s_events += 1;
            if e.kind == 2 {
                has_purchase = true;
                s_revenue += e.price as u64;
            }
        }

        let credited: u16 = match cand {
            Some((t, c, _)) if session_start - t <= WEEK_MS => c,
            _ => crate::lookup::ORGANIC,
        };
        if ctx.trace {
            let mut ties = 0usize;
            if let Some((t, _, _)) = cand {
                let mut j = cp;
                while j > 0 && clicks[j - 1].ts == t {
                    j -= 1;
                    ties += 1;
                }
            }
            let name = (ctx.name_of)(person);
            let camp_name = if credited == crate::lookup::ORGANIC {
                "organic".to_string()
            } else {
                format!("c_{}", credited as u64 + 1)
            };
            eprintln!(
                "T {} {} {} {} {} {}",
                String::from_utf8_lossy(name),
                session_start,
                camp_name,
                cand.map(|c| c.0).unwrap_or(0),
                cand.map(|c| String::from_utf8_lossy(c.2).into_owned()).unwrap_or_default(),
                ties
            );
        }
        let cell = acc.cell(credited, day_idx);
        acc.sess[cell] += 1;
        if has_purchase {
            acc.purch[cell] += 1;
        }
        acc.rev[cell] += s_revenue;

        if day_epoch != cur_day {
            if cur_day != i32::MIN {
                emit_row(
                    person,
                    cur_day,
                    day_sessions,
                    day_events,
                    day_revenue,
                    cat_rev,
                    rows,
                );
            }
            cur_day = day_epoch;
            day_sessions = 0;
            day_events = 0;
            day_revenue = 0;
            for v in cat_rev.iter_mut() {
                *v = 0;
            }
        }
        day_sessions += 1;
        day_events += s_events;
        day_revenue += s_revenue;
        for e in &events[i..j] {
            if e.kind == 2 {
                cat_rev[e.cat as usize] += e.price as u64;
            }
        }

        i = j;
    }
    flush_run(sweep, acc, run_day, run_count);
    if cur_day != i32::MIN {
        emit_row(
            person,
            cur_day,
            day_sessions,
            day_events,
            day_revenue,
            cat_rev,
            rows,
        );
    }
}

#[inline]
fn emit_row(
    person: u32,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    cat_rev: &[u64],
    rows: &mut Vec<UserRow>,
) {
    // Category indices are ranks in byte-sorted name order, so the first
    // maximum is the byte-wise smallest name on a tie.
    let mut top: i16 = -1;
    if revenue > 0 {
        let mut best_v: u64 = 0;
        for (c, &v) in cat_rev.iter().enumerate() {
            if v > best_v {
                best_v = v;
                top = c as i16;
            }
        }
    }
    rows.push(UserRow {
        person,
        day,
        sessions,
        events,
        revenue,
        top_cat: top,
    });
}
