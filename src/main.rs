//! Shopping Sessions v2: a partition-first, lock-free pipeline.
//!
//! Parse (parallel, no shared tables) -> link union-find (overlapped with
//! parsing) -> event-id dedup (per-bucket, cache-resident tables) -> person
//! grouping -> sessionization/attribution -> two Parquet tables.

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod common;
mod dedup;
mod group;
mod links;
mod lookup;
mod parse;
mod parquet;
mod swar;
mod ts;
mod walk;

use common::*;
use walk::WalkOut;
use parse::{Chunk, NPART, NBUCKET, ParseCtx, WorkerOut};
use ts::{DAY_MS, WEEK_MS};

fn main() {
    let t0 = now_ms();
    let mut args = std::env::args().skip(1);
    let arg_data = args.next();
    let arg_out = args.next();

    let data_dir = arg_data
        .or_else(|| std::env::var("DATA_DIR").ok())
        .unwrap_or_else(|| "/data".into());
    let out_dir = arg_out
        .or_else(|| std::env::var("OUTPUT_DIR").ok())
        .unwrap_or_else(|| "/output".into());
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(2, usize::from))
        .clamp(1, 16);
    if ncpu >= (1 << group::SEG_BITS) {
        eprintln!("shopstats: too many workers");
        std::process::exit(1);
    }

    // ---- lookups
    let Some(manifest) = lookup::read_manifest(&data_dir) else {
        eprintln!("shopstats: cannot read {data_dir}/manifest.json");
        std::process::exit(1);
    };
    let ws = manifest.window_start;
    let we = manifest.window_end;
    let cw = ws - WEEK_MS;
    let ws_days = ws.div_euclid(DAY_MS) as i32;
    let nd = (((we - 1).div_euclid(DAY_MS) as i32 - ws_days + 1).max(1)) as usize;

    let products_text = std::fs::read(format!("{data_dir}/products.csv")).unwrap_or_default();
    let campaigns_text = std::fs::read(format!("{data_dir}/campaigns.csv")).unwrap_or_default();
    let cats = lookup::Categories::load(&products_text);
    let ncat = cats.names.len();
    let products = lookup::Products::load(&products_text, &cats);
    let campaigns = lookup::Campaigns::load(&campaigns_text);
    let nc = campaigns.ids.len();
    let t1 = now_ms();

    // ---- inputs: one mapping table for every input file
    let ev_files = lookup::list_csv_files(&data_dir, "events");
    let cl_files = lookup::list_csv_files(&data_dir, "clicks");
    let lk_files = lookup::list_csv_files(&data_dir, "links");
    let mut files = Files {
        maps: Vec::new(),
        kind: Vec::new(),
    };
    let mut file_of_kind: Vec<(u8, u64, u64)> = Vec::new(); // kind, file idx, len
    for (kind, list) in [(2u8, &lk_files), (0u8, &ev_files), (1u8, &cl_files)] {
        for p in list.iter() {
            if let Ok(f) = std::fs::File::open(p) {
                if let Ok(m) = unsafe { memmap2::Mmap::map(&f) } {
                    file_of_kind.push((kind, files.maps.len() as u64, m.len() as u64));
                    files.maps.push(unsafe {
                        std::mem::transmute::<&[u8], &'static [u8]>(&m[..])
                    });
                    files.kind.push(kind);
                    std::mem::forget(m);
                }
            }
        }
    }
    let est_event_lines = {
        let b: u64 = file_of_kind
            .iter()
            .filter(|(k, _, _)| *k == 0)
            .map(|(_, _, l)| *l)
            .sum();
        (b / 70) as usize
    };
    let est_click_lines = {
        let b: u64 = file_of_kind
            .iter()
            .filter(|(k, _, _)| *k == 1)
            .map(|(_, _, l)| *l)
            .sum();
        (b / 44) as usize
    };
    let t2 = now_ms();

    // ---- task list: links first, then events and clicks interleaved
    const CHUNK_BUDGET: u64 = 3 << 20;
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut n_link_chunks = 0usize;
    for &(kind, fidx, len) in file_of_kind.iter() {
        if len == 0 {
            continue;
        }
        if kind == 2 {
            // links: whole file per chunk (they are small)
            chunks.push(Chunk {
                file: fidx as usize,
                start: 0,
                end: len,
                kind,
                skip_header: true,
            });
            n_link_chunks += 1;
        }
    }
    // interleave event and click files
    let mut order: Vec<(u8, u64, u64)> = file_of_kind
        .iter()
        .filter(|(k, _, _)| *k != 2)
        .copied()
        .collect();
    // round-robin: events, clicks, events, ...
    let mut evs: Vec<(u8, u64, u64)> = order.iter().filter(|(k, _, _)| *k == 0).copied().collect();
    let mut cls: Vec<(u8, u64, u64)> = order.iter().filter(|(k, _, _)| *k == 1).copied().collect();
    // largest first within each kind for a gentle tail
    evs.sort_by_key(|(_, _, l)| std::cmp::Reverse(*l));
    cls.sort_by_key(|(_, _, l)| std::cmp::Reverse(*l));
    let mut merged: Vec<(u8, u64, u64)> = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < evs.len() || j < cls.len() {
        if i < evs.len() {
            merged.push(evs[i]);
            i += 1;
        }
        if j < cls.len() {
            merged.push(cls[j]);
            j += 1;
        }
    }
    for (kind, fidx, len) in merged {
        let mut start = 0u64;
        while start < len {
            let end = (start + CHUNK_BUDGET).min(len);
            chunks.push(Chunk {
                file: fidx as usize,
                start,
                end,
                kind,
                skip_header: start == 0,
            });
            start = end;
        }
    }
    let ntasks = chunks.len();
    let t3 = now_ms();

    // ---- Phase A: parse everything; Phase B runs on the main thread as soon
    // as the link chunks are done.
    let ctx = ParseCtx {
        products: &products,
        campaigns: &campaigns,
        window_start: ws,
        window_end: we,
        click_window_start: cw,
    };
    let mut worker_outs: Vec<WorkerOut> = (0..ncpu)
        .map(|_| WorkerOut::new(est_event_lines, est_click_lines, ncpu))
        .collect();
    let mut side_outs: Vec<Vec<LinkSide>> = (0..ncpu).map(|_| Vec::new()).collect();
    let link_progress = links::LinkProgress::new();
    let counter = std::sync::atomic::AtomicUsize::new(0);
    let built = std::thread::scope(|scope| {
        let outs_ptr = Ptr::new(worker_outs.as_mut_slice());
        let sides_ptr = Ptr::new(side_outs.as_mut_slice());
        let counter_ref = &counter;
        let lp = &link_progress;
        let ctx = &ctx;
        let files_ref = &files;
        let chunks_ref = &chunks;
        for t in 0..ncpu {
            let outs_ptr = outs_ptr;
            let sides_ptr = sides_ptr;
            scope.spawn(move || {
                let out = outs_ptr.elem_mut(t);
                let sides = sides_ptr.elem_mut(t);
                loop {
                    let i = counter_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= ntasks {
                        return;
                    }
                    let c = &chunks_ref[i];
                    let data = files_ref.maps[c.file];
                    parse::run_chunk(data, out, ctx, c, sides);
                    if c.kind == 2 {
                        lp.done_one();
                    }
                }
            });
        }
        // main thread: wait for links, then build the link tables while the
        // workers keep parsing events and clicks
        link_progress.wait(n_link_chunks);
        let tb = now_ms();
        let sides: &[Vec<LinkSide>] = sides_ptr.r();
        let built = links::build(files_ref, sides, ncpu);
        if std::env::var("SHOPSTATS_DBG").is_ok() {
            eprintln!("  links-build {}ms", now_ms() - tb);
        }
        built
    });
    let n_links = side_outs.iter().map(|v| v.len()).sum::<usize>() / 2;
    let t4 = now_ms();
    if std::env::var("SHOPSTATS_DBG").is_ok() {
        eprintln!("  parse-only approx: workers saw {} event lines", {
            let n: u64 = worker_outs.iter().map(|w| w.n_events).sum();
            n
        });
    }

    // ---- Phase A3: dedup buckets, emit into user partitions (per worker col)
    let mut user_cols: Vec<Vec<Vec<EvRec2>>> = (0..ncpu)
        .map(|_| {
            (0..NPART)
                .map(|_| Vec::with_capacity((est_event_lines / NPART / ncpu * 6 / 5).max(1024)))
                .collect()
        })
        .collect();
    {
        let outs_ptr = Ptr::new(worker_outs.as_mut_slice());
        let cols_ptr = Ptr::new(user_cols.as_mut_slice());
        run_parallel(ncpu, NBUCKET, |t, b| {
            let n = outs_ptr.r().len();
            let mut bucket: Vec<&[EvRec]> = Vec::with_capacity(n);
            for w in 0..n {
                bucket.push(&outs_ptr.elem(w).ev[b]);
            }
            let col = cols_ptr.elem_mut(t);
            dedup::dedup_bucket(&bucket, col);
            drop(bucket);
            // free this bucket's segments right away: only the worker that
            // processes bucket b touches them
            for w in 0..n {
                let seg = outs_ptr.elem_mut(w).ev.get_mut(b).unwrap();
                if seg.capacity() > 4096 {
                    seg.clear();
                    seg.shrink_to_fit();
                }
            }
        });
    }
    let t5 = now_ms();

    // ---- Phase C: group by user, resolve persons, emit person payloads
    let mut person_cols: Vec<group::PersonCol> = (0..ncpu)
        .map(|_| group::PersonCol::new(est_event_lines, est_click_lines, ncpu))
        .collect();
    {
        let ev_cols_ref = &user_cols;
        let cl_ref = &worker_outs;
        let out_ptr = Ptr::new(person_cols.as_mut_slice());
        let links_ref = &built.tables;
        run_parallel(ncpu, NPART, |t, p| {
            let mut ev_segs: Vec<&[EvRec2]> = Vec::with_capacity(ncpu);
            let mut cl_segs: Vec<&[ClRec]> = Vec::with_capacity(ncpu);
            for w in 0..ncpu {
                ev_segs.push(&ev_cols_ref[w][p]);
                cl_segs.push(&cl_ref[w].cl[p]);
            }
            let col = out_ptr.elem_mut(t);
            group::group_partition(p, &ev_segs, &cl_segs, links_ref, col, t);
        });
    }
    drop(user_cols);
    for w in worker_outs.iter_mut() {
        for p in w.cl.iter_mut() {
            let cap = p.capacity();
            p.clear();
            if cap > (1 << 14) {
                p.shrink_to_fit();
            }
        }
    }
    let t6 = now_ms();

    // ---- Phase D: per-person walk
    let mut walk_outs: Vec<WalkOut> = (0..ncpu)
        .map(|_| walk::make_walk_out(nc, nd))
        .collect();
    {
        let cols_ref = &person_cols;
        let out_ptr = Ptr::new(walk_outs.as_mut_slice());
        let links_ref = &built.tables;
        let files_ref = &files;
        let mut scratches: Vec<walk::Scratch> = (0..ncpu)
            .map(|_| walk::Scratch::new(ncat, nd))
            .collect();
        let scratch_ptr = Ptr::new(scratches.as_mut_slice());
        run_parallel(ncpu, NPART, |t, q| {
            let out = out_ptr.elem_mut(t);
            let sc = scratch_ptr.elem_mut(t);
            walk::walk_partition(q, cols_ref, links_ref, files_ref, ws, ws_days, nd, ncat, out, sc);
        });
    }
    let t7 = now_ms();

    // ---- Phase E: outputs
    std::fs::create_dir_all(format!("{out_dir}/user_daily")).unwrap();
    std::fs::create_dir_all(format!("{out_dir}/campaign_daily")).unwrap();
    let n_users = walk_outs.iter().map(|w| w.rows.len()).sum::<usize>();
    for (t, w) in walk_outs.iter().enumerate() {
        if !w.rows.is_empty() {
            write_user_daily(
                &format!("{out_dir}/user_daily/part-{t}.parquet"),
                &w.rows,
                &w.names,
                &cats,
            );
        }
    }
    if n_users == 0 {
        write_user_daily(
            &format!("{out_dir}/user_daily/part-0.parquet"),
            &[],
            &[],
            &cats,
        );
    }
    {
        let mut acc = walk_outs.remove(0).acc;
        for w in walk_outs.iter() {
            acc.merge_into(&w.acc);
        }
        write_campaign_daily(
            &format!("{out_dir}/campaign_daily/part-0.parquet"),
            &acc,
            nc,
            nd,
            ws_days,
            &campaigns,
        );
    }
    let t8 = now_ms();

    let n_events: u64 = worker_outs.iter().map(|w| w.n_events).sum();
    let n_clicks: u64 = worker_outs.iter().map(|w| w.n_clicks).sum();
    eprintln!(
        "shopstats: links {n_links} nodes {} events {n_events} clicks {n_clicks} users {n_users} | \
         setup {}ms io {}ms chunk {}ms parse+links {}ms dedup {}ms group {}ms walk {}ms output {}ms | \
         total {}ms (workers {ncpu})",
        built.n_nodes,
        t1 - t0,
        t2 - t1,
        t3 - t2,
        t4 - t3,
        t5 - t4,
        t6 - t5,
        t7 - t6,
        t8 - t7,
        t8 - t0,
    );
}

fn write_user_daily(
    path: &str,
    rows: &[walk::UserRow],
    names: &[u8],
    cats: &lookup::Categories,
) {
    use parquet::{ColData, Field, PLogical, PType};
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
        let end = (r.name_off as usize + r.name_len as usize).min(names.len());
        uid.push(&names[r.name_off as usize..end]);
        day.push(r.day);
        sess.push(r.sessions as i64);
        ev.push(r.events as i64);
        rev.push(r.revenue as i64);
        if r.top_cat >= 0 {
            defs.push(true);
            top.push(cats.names[r.top_cat as usize].as_bytes());
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
    acc: &walk::CampAcc,
    nc: usize,
    nd: usize,
    ws_days: i32,
    campaigns: &lookup::Campaigns,
) {
    use parquet::{ColData, Field, PLogical, PType};
    let fields = [
        Field { name: "campaign_id", ptype: PType::ByteArray, logical: PLogical::Str, optional: false },
        Field { name: "channel", ptype: PType::ByteArray, logical: PLogical::Str, optional: false },
        Field { name: "day", ptype: PType::Int32, logical: PLogical::Date, optional: false },
        Field { name: "sessions", ptype: PType::Int64, logical: PLogical::None, optional: false },
        Field { name: "purchasing_sessions", ptype: PType::Int64, logical: PLogical::None, optional: false },
        Field { name: "revenue_cents", ptype: PType::Int64, logical: PLogical::None, optional: false },
        Field { name: "assisted_sessions", ptype: PType::Int64, logical: PLogical::None, optional: false },
    ];
    let cells = (nc + 1) * acc.nd;
    let mut cid: Vec<&[u8]> = Vec::new();
    let mut ch: Vec<&[u8]> = Vec::new();
    let mut day: Vec<i32> = Vec::new();
    let mut sess: Vec<i64> = Vec::new();
    let mut purch: Vec<i64> = Vec::new();
    let mut rev: Vec<i64> = Vec::new();
    let mut assist: Vec<i64> = Vec::new();
    for cell in 0..cells {
        let c = cell / acc.nd;
        let d = cell % acc.nd;
        let s = acc.sess[cell];
        let a = acc.assist[cell];
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
        purch.push(acc.purch[cell] as i64);
        rev.push(acc.rev[cell] as i64);
        assist.push(a as i64);
    }
    let n = cid.len();
    let _ = nd;
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
