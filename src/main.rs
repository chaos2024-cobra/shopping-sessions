//! Shopping Sessions: a single-pass Rust pipeline.
//!
//! Reads the dirty CSV logs under $DATA_DIR, joins linked IDs into people,
//! sessionizes each person's events, credits sessions to the latest click in
//! the 7 days before the session, counts assisting campaigns, and writes the
//! two daily Parquet tables under $OUTPUT_DIR.

mod agg;
mod lookup;
mod parquet;
mod parse;
mod ts;
mod util;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::sync::Mutex;

use agg::{CampAcc, Sweep, UserRow, WalkCtx};
use lookup::{Campaigns, Interner, Products, UnionFind};
use parquet::{ColData, Field, PLogical, PType};
use parse::{
    build_input, list_csv_files, run_chunk, ClickTable, EventTable, Input, ParseCtx, TaskKind,
};
use ts::{DAY_MS, WEEK_MS};
use util::now_ms;

fn main() {
    let t0 = now_ms();
    let mut args = std::env::args().skip(1);
    let arg_data = args.next();
    let arg_out = args.next();

    let data_dir = arg_data
        .clone()
        .or_else(|| std::env::var("DATA_DIR").ok())
        .unwrap_or_else(|| "/data".into());
    let out_dir = arg_out
        .clone()
        .or_else(|| std::env::var("OUTPUT_DIR").ok())
        .unwrap_or_else(|| "/output".into());
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(2, usize::from))
        .max(1);

    rayon::ThreadPoolBuilder::new()
        .num_threads(ncpu)
        .build_global()
        .ok();
    if let Ok(v) = std::env::var("SHOPSTATS_SKIP") {
        parse::SKIP.store(v.parse().unwrap_or(0), std::sync::atomic::Ordering::Relaxed);
    }

    let Some(manifest) = lookup::read_manifest(&data_dir) else {
        eprintln!("shopstats: cannot read {data_dir}/manifest.json");
        std::process::exit(1);
    };
    let ws = manifest.window_start;
    let we = manifest.window_end;
    let click_ws = ws - WEEK_MS;
    let ws_days = ws.div_euclid(DAY_MS) as i32;
    let nd = ((we - 1).div_euclid(DAY_MS) as i32 - ws_days + 1).max(1) as usize;

    // ---- lookups
    let products_text = std::fs::read(format!("{data_dir}/products.csv")).unwrap_or_default();
    let campaigns_text = std::fs::read(format!("{data_dir}/campaigns.csv")).unwrap_or_default();
    let cat_names: Vec<&[u8]> = {
        let mut lines = lookup::split_lines(&products_text);
        lines.next();
        let mut cats: Vec<&[u8]> = Vec::new();
        for line in lines {
            if let Some((_, cat)) = lookup::split2(line) {
                cats.push(cat);
            }
        }
        cats.push(b"unknown");
        cats.sort_unstable();
        cats.dedup();
        cats
    };
    let cat_index = |name: &[u8]| -> u16 {
        cat_names
            .binary_search(&name)
            .unwrap_or(cat_names.len() - 1) as u16
    };
    let products = Products::load(&products_text, &cat_index);
    let campaigns = Campaigns::load(&campaigns_text);
    let nc = campaigns.ids.len();
    let t1 = now_ms();

    // ---- inputs
    let event_files = list_csv_files(&data_dir, "events");
    let click_files = list_csv_files(&data_dir, "clicks");
    let link_files = list_csv_files(&data_dir, "links");
    let event_bytes: u64 = event_files
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
        .sum();
    let click_bytes: u64 = click_files
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
        .sum();
    let link_bytes: u64 = link_files
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
        .sum();
    let total_bytes = event_bytes + click_bytes + link_bytes;

    // Size hints (slightly generous; the tables grow if needed).
    let est_event_lines = (event_bytes / 70) as usize;
    let est_click_lines = (click_bytes / 45) as usize;
    let est_nodes = (est_event_lines + est_click_lines) / 12 + 1024;
    let est_links = (link_bytes / 16) as usize + 1024;

    let budget = (total_bytes / (ncpu as u64 * 6)).clamp(8 << 20, 64 << 20);
    let event_input = build_input(&event_files, TaskKind::Events, budget);
    let click_input = build_input(&click_files, TaskKind::Clicks, budget);
    let link_input = build_input(&link_files, TaskKind::Links, budget);

    let mut interner = Interner::new(est_nodes);
    let event_table = EventTable::new(est_event_lines);
    let click_table = ClickTable::new(est_click_lines);
    let links: Mutex<Vec<(u32, u32)>> = Mutex::new(Vec::with_capacity(est_links));
    let t2 = now_ms();

    // ---- parse everything in one parallel sweep
    {
        let ctx = ParseCtx {
            interner: &interner,
            products: &products,
            campaigns: &campaigns,
            events: &event_table,
            clicks: &click_table,
            links: &links,
            window_start: ws,
            window_end: we,
            click_window_start: click_ws,
        };
        let jobs: Vec<(&Input, TaskKind, usize)> = {
            let mut all: Vec<(&Input, TaskKind, usize)> = Vec::new();
            for (input, kind) in [
                (&link_input, TaskKind::Links),
                (&event_input, TaskKind::Events),
                (&click_input, TaskKind::Clicks),
            ] {
                for i in 0..input.chunks.len() {
                    all.push((input, kind, i));
                }
            }
            all
        };
        use rayon::prelude::*;
        jobs.into_par_iter().for_each(|(input, kind, i)| {
            run_chunk(input, &input.chunks[i], kind, &ctx);
        });
    }
    let t3 = now_ms();

    // ---- people: union-find, roots, names
    let pairs = links.into_inner().unwrap();
    let n_nodes = interner.node_count() as usize;
    let mut uf = UnionFind::new(n_nodes);
    for &(a, b) in &pairs {
        uf.union(a, b);
    }
    let root = uf.roots();
    drop(uf);
    let node_str = interner.freeze();
    let mut name_node = vec![u32::MAX; n_nodes];
    for n in 0..n_nodes {
        let r = root[n] as usize;
        if name_node[r] == u32::MAX || node_str[n] < node_str[name_node[r] as usize] {
            name_node[r] = n as u32;
        }
    }
    drop(pairs);
    let t4 = now_ms();

    // ---- group survivors by person
    let grouped = agg::group_by_person(n_nodes, ncpu, &event_table, &click_table, &root);
    drop(event_table);
    drop(root);
    let t5 = now_ms();

    // ---- bin persons across threads (largest first for balance)
    let g = &grouped;
    let mut persons: Vec<(u32, u32)> = Vec::new();
    for p in 0..n_nodes {
        let cnt = g.event_base[p + 1] - g.event_base[p];
        if cnt > 0 {
            persons.push((cnt, p as u32));
        }
    }
    persons.sort_unstable_by(|a, b| b.cmp(a));
    let mut bins: Vec<Vec<u32>> = vec![Vec::new(); ncpu];
    let mut loads = vec![0u64; ncpu];
    for &(cnt, p) in &persons {
        let (mut best, mut best_load) = (0usize, u64::MAX);
        for t in 0..ncpu {
            if loads[t] < best_load {
                best = t;
                best_load = loads[t];
            }
        }
        bins[best].push(p);
        loads[best] += cnt as u64;
    }
    let n_persons = persons.len();
    drop(persons);

    // ---- per-thread walk + user_daily files
    std::fs::create_dir_all(format!("{out_dir}/user_daily")).unwrap();
    std::fs::create_dir_all(format!("{out_dir}/campaign_daily")).unwrap();
    #[derive(Clone, Copy)]
    struct RawPtr<T>(*mut T);
    unsafe impl<T> Send for RawPtr<T> {}
    impl<T> RawPtr<T> {
        fn as_ptr(&self) -> *mut T {
            self.0
        }
    }
    let ev_ptr = RawPtr(grouped.events.as_ptr() as *mut agg::SurvRec);
    let cl_ptr = RawPtr(grouped.clicks.as_ptr() as *mut agg::ClickSurv);
    let trace = std::env::var("SHOPSTATS_TRACE").is_ok();
    let name_of = |p: u32| -> &'static [u8] { node_str[name_node[p as usize] as usize] };
    let walk_ctx = WalkCtx {
        grouped: &grouped,
        ws_days,
        trace,
        name_of: &name_of,
    };
    let out_ref: &str = &out_dir;
    let node_str_ref = &node_str;
    let name_node_ref = &name_node;
    let cat_ref = &cat_names;
    let results: Vec<(CampAcc, usize)> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for t in 0..ncpu {
            let ctx = &walk_ctx;
            let bin = &bins[t];
            handles.push(scope.spawn(move || {
                let mut acc_local = CampAcc::new(nc, nd);
                let mut rows: Vec<UserRow> = Vec::new();
                let mut sweep = Sweep::new(nc.max(1));
                let mut cat_rev = vec![0u64; cat_ref.len().max(1)];
                for &person in bin {
                    let es = ctx.grouped.event_base[person as usize] as usize;
                    let ee = ctx.grouped.event_base[person as usize + 1] as usize;
                    let cs = ctx.grouped.click_base[person as usize] as usize;
                    let ce = ctx.grouped.click_base[person as usize + 1] as usize;
                    // SAFETY: person ranges are disjoint across all threads.
                    let events = unsafe {
                        std::slice::from_raw_parts_mut(ev_ptr.as_ptr().add(es), ee - es)
                    };
                    let clicks = unsafe {
                        std::slice::from_raw_parts_mut(cl_ptr.as_ptr().add(cs), ce - cs)
                    };
                    agg::walk_person(
                        person, ctx, &mut acc_local, &mut rows, &mut sweep, &mut cat_rev,
                        events, clicks,
                    );
                }
                let count = rows.len();
                if count > 0 {
                    let path = format!("{out_ref}/user_daily/part-{t}.parquet");
                    write_user_daily(&path, &rows, node_str_ref, name_node_ref, cat_ref);
                }
                (acc_local, count)
            }));
        }
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    let accs = results;
    let user_files_written = accs.iter().filter(|(_, c)| *c > 0).count();
    if user_files_written == 0 {
        let rows: Vec<UserRow> = Vec::new();
        write_user_daily(
            &format!("{out_dir}/user_daily/part-0.parquet"),
            &rows,
            &node_str,
            &name_node,
            &cat_names,
        );
    }
    let t6 = now_ms();

    // ---- campaign_daily
    write_campaign_daily(
        &format!("{out_dir}/campaign_daily/part-0.parquet"),
        &accs,
        nc,
        nd,
        ws_days,
        &campaigns,
    );
    let t7 = now_ms();

    let total_events = grouped.event_base[n_nodes] as u64;
    let total_clicks = grouped.click_base[n_nodes] as u64;
    eprintln!(
        "shopstats: nodes {n_nodes} persons {n_persons} events {total_events} clicks {total_clicks} | \
         setup {}ms io {}ms parse {}ms people {}ms group {}ms walk {}ms output {}ms | total {}ms (threads {ncpu})",
        t1 - t0, t2 - t1, t3 - t2, t4 - t3, t5 - t4, t6 - t5, t7 - t6, t7 - t0
    );
}

fn write_user_daily(
    path: &str,
    rows: &[UserRow],
    node_str: &[&'static [u8]],
    name_node: &[u32],
    cat_names: &[&[u8]],
) {
    let fields = [
        Field { name: "user_id", ptype: PType::ByteArray, logical: PLogical::Str, optional: false },
        Field { name: "day", ptype: PType::Int32, logical: PLogical::Date, optional: false },
        Field { name: "sessions", ptype: PType::Int64, logical: PLogical::None, optional: false },
        Field { name: "events", ptype: PType::Int64, logical: PLogical::None, optional: false },
        Field { name: "revenue_cents", ptype: PType::Int64, logical: PLogical::None, optional: false },
        Field { name: "top_category", ptype: PType::ByteArray, logical: PLogical::Str, optional: true },
    ];
    let n = rows.len();
    let mut uid: Vec<&[u8]> = Vec::with_capacity(n);
    let mut day: Vec<i32> = Vec::with_capacity(n);
    let mut sess: Vec<i64> = Vec::with_capacity(n);
    let mut ev: Vec<i64> = Vec::with_capacity(n);
    let mut rev: Vec<i64> = Vec::with_capacity(n);
    let mut top: Vec<&[u8]> = Vec::with_capacity(n);
    let mut defs: Vec<bool> = Vec::with_capacity(n);
    for r in rows {
        let name = node_str[name_node[r.person as usize] as usize];
        uid.push(name);
        day.push(r.day);
        sess.push(r.sessions as i64);
        ev.push(r.events as i64);
        rev.push(r.revenue as i64);
        if r.top_cat >= 0 {
            defs.push(true);
            top.push(cat_names[r.top_cat as usize]);
        } else {
            defs.push(false);
            top.push(b"");
        }
    }
    let cols = [
        ColData::Str { vals: uid, defs: vec![true; n] },
        ColData::Int32(day),
        ColData::Int64(sess),
        ColData::Int64(ev),
        ColData::Int64(rev),
        ColData::Str { vals: top, defs },
    ];
    parquet::write_parquet(path, &fields, &cols, n);
}

fn write_campaign_daily(
    path: &str,
    accs: &[(CampAcc, usize)],
    nc: usize,
    nd: usize,
    ws_days: i32,
    campaigns: &Campaigns,
) {
    let fields = [
        Field { name: "campaign_id", ptype: PType::ByteArray, logical: PLogical::Str, optional: false },
        Field { name: "channel", ptype: PType::ByteArray, logical: PLogical::Str, optional: false },
        Field { name: "day", ptype: PType::Int32, logical: PLogical::Date, optional: false },
        Field { name: "sessions", ptype: PType::Int64, logical: PLogical::None, optional: false },
        Field { name: "purchasing_sessions", ptype: PType::Int64, logical: PLogical::None, optional: false },
        Field { name: "revenue_cents", ptype: PType::Int64, logical: PLogical::None, optional: false },
        Field { name: "assisted_sessions", ptype: PType::Int64, logical: PLogical::None, optional: false },
    ];
    let cells = (nc + 1) * nd;
    let mut cid: Vec<&[u8]> = Vec::new();
    let mut ch: Vec<&[u8]> = Vec::new();
    let mut day: Vec<i32> = Vec::new();
    let mut sess: Vec<i64> = Vec::new();
    let mut purch: Vec<i64> = Vec::new();
    let mut rev: Vec<i64> = Vec::new();
    let mut assist: Vec<i64> = Vec::new();
    for cell in 0..cells {
        let c = cell / nd;
        let d = cell % nd;
        let mut s = 0u32;
        let mut p = 0u32;
        let mut r = 0u64;
        let mut a = 0u32;
        for (acc, _) in accs {
            s += acc.sess[cell];
            p += acc.purch[cell];
            r += acc.rev[cell];
            a += acc.assist[cell];
        }
        if s == 0 && a == 0 {
            continue;
        }
        if c == nc {
            cid.push(b"organic");
            ch.push(b"organic");
        } else {
            cid.push(campaigns.ids[c].as_bytes());
            ch.push(campaigns.channels[c].as_bytes());
        }
        day.push(ws_days + d as i32);
        sess.push(s as i64);
        purch.push(p as i64);
        rev.push(r as i64);
        assist.push(a as i64);
    }
    let n = cid.len();
    let cols = [
        ColData::Str { vals: cid, defs: vec![true; n] },
        ColData::Str { vals: ch, defs: vec![true; n] },
        ColData::Int32(day),
        ColData::Int64(sess),
        ColData::Int64(purch),
        ColData::Int64(rev),
        ColData::Int64(assist),
    ];
    parquet::write_parquet(path, &fields, &cols, n);
}
