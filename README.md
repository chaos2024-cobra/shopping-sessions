# Shopping Sessions — Rust solution

A single Rust binary that implements the full `rules.md` pipeline and writes
both Parquet output tables. No engine, no JVM, no Python at run time: the
container starts the binary directly, so the whole run budget is spent on data.

## How it works

One pass over the input files, memory-mapped and split into ~64 MB
line-aligned chunks that a small thread pool parses in parallel:

1. **CSV parsing per rules.md.** A byte scanner splits each line on commas and
   flags quotes in the same pass; fields are unquoted only when a quote is
   present anywhere in the line. `\r` is an ordinary character, only `\n`
   ends a line, the first line of every file is the header, and a wrong field
   count or a stray quote drops the line. Timestamps are parsed in the three
   allowed forms with strict ASCII-digit and calendar checks (February 29
   only in leap years, offsets 00:00..23:59).
2. **Linked IDs.** Every `user_id` is interned in a sharded lock-protected
   table (xxh3-128 keys, 16-byte slots). Link lines build a union-find, each
   person's name is the byte-wise smallest of their IDs, and the flattened
   root of every node is looked up once afterwards.
3. **Dedup before validation, per the rules.** Events go into a sharded
   open-addressing table keyed by a 128-bit hash of `event_id`; the slot
   carries the ingest second so the smallest-ingest copy wins without
   touching the record, and a stored hash-low word verifies identity. The
   winner's validation flags and the 24-hour late check run later, so an
   invalid earliest copy poisons its event exactly as required. Clicks
   dedup by `click_id` (copies are identical, any one is kept).
4. **Grouping.** Surviving events and clicks are counting-sorted by person
   (two passes over compact 24/32-byte records, per-thread cursors, no
   atomics on hot paths), then each person's slice is sorted by timestamp
   (clicks by timestamp then click_id bytes).
5. **Sessions, credit, assists.** Each thread walks its persons: sessions
   split on gaps over 30 minutes; a merge sweep over the person's clicks
   keeps the credit candidate (latest click at or before the session start,
   ties to the smallest click_id) and an expiry-heap set of campaigns whose
   latest click is within 7 days. Runs of sessions with an unchanged active
   set are flushed together, so assists cost one counter bump per campaign.
6. **Output.** Each worker writes its own Parquet file directly
   (hand-rolled writer: PLAIN encoding, DataPageV2, DATE as int32, nulls
   only in `top_category`), plus one merged campaign table. Polars, DuckDB
   and pyarrow all read the result.

Performance notes: the big tables are sized generously from the input byte
counts to keep linear-probe chains short, `madvise(WILLNEED)` starts readahead
on every input file before parsing, and the large slot arrays ask for
transparent huge pages to spare the TLB. Everything is byte-exact: no string
normalisation anywhere.

## Testing

- `make dev` (or `generate --seed 1 --events 1000000 --clicks 250000`) builds
  the dev set; the output matches `dev-answer.zip` exactly (117,038 and
  27,400 rows).
- All 36 edge-case fixtures from the starters pass.
- A slow Python reference implementation (in `ref/`, not shipped) agrees
  byte-for-byte on additional seeds, including a window holding February 29,
  2028.

## Layout

- `src/main.rs` — orchestration, sizing, person bins, output assembly
- `src/parse.rs` — chunked CSV parsing, sharded dedup tables
- `src/lookup.rs` — manifest, products, campaigns, interner, union-find
- `src/ts.rs` — the three timestamp forms, prices
- `src/agg.rs` — grouping, sessionization, credit and assists
- `src/parquet.rs` — minimal Parquet writer (Thrift compact, DataPageV2)

Build the image the way the scorer does:

```sh
docker build -t shopping-sessions:latest .
```
