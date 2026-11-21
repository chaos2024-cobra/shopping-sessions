# Shopping Sessions — Rust solution (v2, partition-first)

A single Rust binary implementing the full `rules.md` pipeline. No engine, no
JVM, no Python at run time: the container starts the binary directly, so the
whole run budget is spent on data.

## Design: partition first, never lock, stay sequential

The pipeline is built around one idea: every cross-record decision (dedup,
person grouping, crediting) is made **after** a hash-partitioning step, so the
hot loops only ever append to per-worker segments and probe small,
cache-resident hash tables. There is not a single lock or atomic on any hot
path (the only shared counters are one `fetch_add` per multi-MB chunk).

1. **Parse (Phase A).** All input files are mapped read-only and split into
   3 MB line-aligned chunks. A worker pool claims chunks through an atomic
   counter. Each line is split with a SWAR scanner: commas and quote bytes are
   found with 8-byte words using the carry-free exact byte-equality mask, and
   only fields overlapping a quote word are unquoted. `ingest_ts` is parsed as
   strict F1, `ts` in all three forms with full calendar checks, prices with
   leading zeros, and every field's validation flags are precomputed.
   Events are appended (48-byte records, no lock) to **event-id-hash buckets**
   (1024 buckets x one segment per worker); clicks go straight into
   **user-hash partitions** because click copies are identical and dedup is
   provably a no-op for the outputs; link sides are collected per worker.
2. **People (Phase B)** runs on the main thread *while the workers keep
   parsing*: link sides are MSB-radix-partitioned and sorted, distinct ids get
   node ids, a union-find joins the pairs, and the byte-wise smallest id names
   each component. Every user-hash partition receives a sorted link table
   plus an O(1) hash index.
3. **Dedup (Phase A3).** Each event-id bucket is processed independently:
   a two-pass open-addressing table (sized to the bucket, L2-resident) keeps
   the smallest-ingest copy per event id — dedup before validation, exactly
   as the rules demand — and the winners are emitted into user-hash
   partitions as 32-byte records. Bucket memory is freed as it is consumed.
4. **Person grouping (Phase C).** Per user-hash partition: one counting pass
   builds per-user groups (L2 hash table), a second pass places 12-byte
   payloads, and each group is resolved to a person through the link table,
   then emitted (with its payload run and a group entry) into the
   **person-hash partition** of its component root.
5. **Walk (Phase D).** Per person-hash partition: group entries are regrouped
   by person, each person's events are gathered and sorted by timestamp, then
   sessionized in one pass (sessions split on gaps over 30 minutes; per-day
   category revenue is accumulated in the same pass). Crediting walks the
   person's clicks once, keeping the latest click at or before each session
   start (ties to the byte-wise smallest `click_id`, compared only on
   same-millisecond ties). Assists reuse the same ts-sorted pass: each
   campaign keeps an open `[t, t+7d]` window, merge-extended as its clicks
   arrive, and each closed window adds, per day, the number of session starts
   it covers — O(clicks + campaigns x days), no per-session active-set flush.
   Person names are materialized once per person into a per-worker arena,
   walking persons in name-location order so input reads stay sequential.
6. **Output (Phase E).** `user_daily` is written as one Parquet part per
   worker (PLAIN, uncompressed, DataPageV2) from the arenas;
   `campaign_daily` merges the per-worker counters into one file.

Timestamps, prices, quoting, malformed-line rules, the 24-hour late check,
the 7-day click window (inclusive on both ends), session-gap boundaries
(exactly 30:00.000 stays), dedup-before-validation, byte-wise smallest names
and tie-breaks follow `rules.md` exactly; the test suite below pins them.

## Correctness

- 36/36 edge-case fixtures pass (`run_fixtures.py`).
- The 1M-event dev set matches the published answer byte-for-byte on both
  tables (0 differing rows).
- Fresh datasets (seeds 42, 777, 2028, 31337) match a slow reference
  implementation exactly on both tables.
- The SWAR splitter is differentially tested against a byte-by-byte splitter
  on every line of ~34.6M lines across five datasets: 0 mismatches.

## Performance notes

All hash keys are 80-bit (64+16 bits of xxh3-128), giving collision
probability < 1e-12 at 60M events. Records are fixed-size (48/40/32/12 bytes)
so every phase is a memcpy-bound sweep. Peak anonymous memory at 60M events is
about 8 GB; the input files are never copied (mmap + `MADV_WILLNEED`), so the
page cache serves the name materialization and a 24 GB box keeps everything
warm. The binary logs phase timings to stderr (stdout stays clean for the
scorer).

## Layout

- `src/main.rs` — orchestration, phases, Parquet output
- `src/parse.rs` — Phase A: chunked parallel CSV parse
- `src/links.rs` — Phase B: sides sort, union-find, person names, link tables
- `src/dedup.rs` — Phase A3: per-bucket smallest-ingest dedup
- `src/group.rs` — Phase C: user grouping, person resolution, payload runs
- `src/walk.rs` — Phase D: sessionize, credit, assists, both daily tables
- `src/swar.rs` — SWAR field splitting and unquoting
- `src/ts.rs` — timestamp/price parsing per rules.md
- `src/lookup.rs` — manifest, categories, products, campaigns
- `src/common.rs` — records, file table, parallel helpers
- `src/parquet.rs` — minimal Parquet writer

## Build & run

```
docker build -t shopping-sessions .
docker run --rm -v $PWD/dev:/data -v $PWD/out:/output -e NCPU=8 shopping-sessions
```

The binary reads `DATA_DIR` (default `/data`) and `OUTPUT_DIR` (default
`/output`), sizes itself from `NCPU` (default: available parallelism), and
accepts optional positional `DATA_DIR OUTPUT_DIR` arguments for local runs.
