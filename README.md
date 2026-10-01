# Upserting 1 million JSON rows into a 1-billion-row Vertica table, three ways

A mainframe ADABAS system sends its changes (CDC) as **hierarchical JSON**. Each batch holds
**1 million** changed records, half of them **updates** of existing rows and half **inserts**
of new rows. They must reach a **1-billion-row** fact table in Vertica as fast as possible.

This demo measures three ways of doing it. The timer always covers the **whole job**: parsing
the JSON files, loading them, and applying the changes.

| # | Method | In one line |
|---|---|---|
| 1 | **Insert-only upsert: journal + Top-K LAP** | never UPDATE: `COPY` every change into a journal; a `LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)` Live Aggregate Projection serves the newest version |
| 2 | **Staging table + partition COPY/SWAP** | `COPY` into a staging table, rebuild only the touched partitions, publish them with one atomic `SWAP_PARTITIONS_BETWEEN_TABLES` |
| 3 | **Optimized MERGE** | `COPY` into a delta table shaped like the target, then one `MERGE` that meets the optimization rules |

A second phase scales out: **10 tables** (the 1B-row table and 9 more ADABAS files of 60M rows
each) receive **1 million multi-level JSON changes each**, loaded and merged **in parallel**.

It's bash scripts built on `vsql`, with the SQL inside. Every step prints **what** runs,
**why**, the **SQL** and **how long** it took, so it can be presented and recorded.

## Quick start

On a Vertica node (the JSON files must be readable by the server), as a user that can
create a schema:

```bash
git clone https://github.com/mogomo/vertica-json-load-demo.git && cd vertica-json-load-demo
cp vload.env.example vload.env     # optional: connection and settings

./generate.sh                      # once: 1B-row table (SQL) + 1M JSON changes   (~12 min)
./apply.sh                         # the three methods, timed and checked          (~1 min)
./apply.sh --runs 3                # as often as you like: every run starts from the same data
```

Then the multi-table phase:

```bash
./generate_multi.sh                # once: 9 tables × 60M rows + 9 × 1M JSON changes  (~2.5 min)
./apply_multi.sh                   # 10 tables × 1M changes, all in parallel   (< 1 min)
./apply_multi.sh --parallel 10,5,1 --runs 3   # compare 10 / 5 / 1 tables at a time
```

Try it small first: `./generate.sh --rows 10M --changes 100K`, then `./apply.sh`.

## What the scripts do

```
generate.sh
  ├─ vload.txn_base       1,000,000,000 rows, generated in Vertica with INSERT … SELECT
  │                       (36 monthly partitions, 11 parallel sessions, one partition per statement)
  ├─ vload.txn_jrn_base   the same rows as an insert-only journal + Top-K LAP
  └─ demo/changes/*.json  1,000,000 CDC records: 500,000 updates (newest 5 % of the ISNs)
                          + 500,000 inserts (new ISNs), 22 JSON Lines files

apply.sh  (per run, per method)
  ├─ reset    COPY_TABLE from the pristine table: catalog only, milliseconds   (not timed)
  ├─ ⏱ parse + load   COPY … FROM 'demo/changes/*.json' PARSER FJSONPARSER(flatten_arrays=true)
  ├─ ⏱ apply          upsert: nothing more │ swap: parallel rebuild + SWAP │ merge: MERGE
  └─ check    row count + checksum of the current data: identical in every method   (not timed)
```

- **The data:** ADABAS file TXN (account transactions) with a CDC header, a MERCHANT group and
  a TAG multiple-value field. See [docs/ADABAS_MAPPING.md](docs/ADABAS_MAPPING.md).
- **Repeatable:** `COPY_TABLE` resets each method's table in milliseconds, without copying
  data, so `apply.sh` can run again and again and every run measures the same work.
- **Correct:** after each method, the current rows of the touched partitions and everything
  newer are counted and checksummed. All three methods must match each other and the
  expected count. `--full-check` checks all billion rows.

## Phase 1 results

Measured on a single-node Vertica 26.2 (22 hardware threads, 61 GB RAM, one NVMe SSD).
The details and analysis are in [docs/RESULTS.md](docs/RESULTS.md).

<!-- results:begin -->
**1,000,000 JSON changes** (500,000 updates + 500,000 inserts) applied to a
**1,000,000,000-row** table, average of 3 runs:

| Method | Parse + load JSON | Apply | **Total** | Rows/s | Delete vectors |
|---|---:|---:|---:|---:|---:|
| 1 Upsert (journal + Top-K LAP) | 2.12 s | – | **2.12 s** | 471K | 0 |
| 2 Staging + partition SWAP | 1.23 s | 14.66 s | **15.89 s** | 63K | 0 |
| 3 Optimized MERGE | 1.31 s | 0.89 s | **2.21 s** | 453K | 500,000 |

The three runs differ by less than 0.3 s per method. All methods end with **identical data**:
a `--full-check` compared all 1,000,500,000 current rows (same count, same checksum).
Building the data took 12 min 19 s: the 1B-row table in 3 min 51 s (4.3M rows/s), the journal
with its LAP in 8 min 14 s.
<!-- results:end -->

## Phase 2: 10 tables in parallel

```
generate_multi.sh
  ├─ vload.<table>_base   9 ADABAS files from conf/tables.def, 60,000,000 rows each (SQL, 11 sessions)
  └─ demo/multi/<table>/  1,000,000 multi-level JSON changes per table (8 files each):
                          groups, multiple-value fields, periodic groups (arrays of objects)

apply_multi.sh  (per run, per --parallel setting)
  ├─ reset    COPY_TABLE × 10                                                     (not timed)
  ├─ ⏱ 10 pipelines, at most N at a time:  COPY JSON → <table>_delta  →  optimized MERGE
  └─ check    rows = base + inserts, and the changed rows equal the JSON rows (checksum of all columns)
```

The method is **optimized MERGE**: in phase 1 it was as fast as the upsert, with half the
storage and plain reads. Every table has its own JSON files and its own pipeline, so the 10
tables are fully independent.

<!-- multi:begin -->
**10 tables × 1,000,000 JSON changes = 10,000,000 changes** (500,000 updates + 500,000 inserts
per table) into the 1B-row table and 9 tables of 60M rows; average of 3 runs:

| Tables at a time | Wall-clock, all 10 tables | JSON rows/s | vs one at a time |
|---|---:|---:|---:|
| **10** (all in parallel) | **12.24 s** | 817K | 2.3x |
| 5 (a new table starts when one finishes) | 12.88 s | 777K | 2.2x |
| 1 (one after the other) | 28.20 s | 355K | 1.0x |

| Table | Rows | 1 at a time: parse + load / MERGE / total | 10 at a time: total |
|---|---:|---:|---:|
| txn | 1,000,000,000 | 1.36 / 0.92 / **2.28 s** | 8.78 s |
| customer | 60,000,000 | 2.31 / 2.03 / **4.34 s** | 12.19 s |
| account | 60,000,000 | 1.72 / 0.68 / **2.40 s** | 9.92 s |
| card | 60,000,000 | 1.79 / 0.71 / **2.51 s** | 10.26 s |
| loan | 60,000,000 | 1.85 / 0.69 / **2.54 s** | 10.23 s |
| payment | 60,000,000 | 1.88 / 1.83 / **3.71 s** | 11.99 s |
| policy | 60,000,000 | 1.84 / 0.71 / **2.55 s** | 10.45 s |
| claim | 60,000,000 | 1.74 / 0.66 / **2.40 s** | 10.21 s |
| employees | 60,000,000 | 2.08 / 0.98 / **3.06 s** | 11.02 s |
| vehicles | 60,000,000 | 1.67 / 0.69 / **2.36 s** | 10.15 s |

Running the 10 tables together cuts the wall-clock from 28.2 s to 12.2 s. Each table takes
longer when they share the machine (8.8–12.2 s instead of 2.3–4.3 s), but they all finish
within 12.2 s. Ten at a time and five at a time are within 0.7 s of each other: JSON parsing
saturates the 22 cores at ~800K rows/s either way, because every COPY already parses its files
with several threads. Building the 9 tables took 2 min 14 s (540M rows at 4.4M rows/s).
<!-- multi:end -->

## Which method, when?

| | Upsert (journal + Top-K LAP) | Swap partitions | Optimized MERGE |
|---|---|---|---|
| Best when | high change rates, MERGE can't be optimized, history is needed | changes cluster in few, small partitions; atomic publication | changes are spread out and small compared to the table |
| Apply cost grows with | number of changes | size of the touched partitions | number of changes |
| Delete vectors | none | none | one per updated row |
| Storage | about 2x (journal + LAP) + history | 1x | 1x + delete vectors |
| Version history | every version, with time | – | – |

The reasoning, SQL and pitfalls of each method are in [docs/METHODS.md](docs/METHODS.md).

## Commands

| Command | Does |
|---|---|
| `./generate.sh [--rows 1B] [--changes 1M] [--force]` | builds the fact table, the journal and the JSON change files. Keeps existing tables of the right size unless `--force` |
| `./apply.sh [--runs N] [--method upsert,swap,merge]` | resets, times and checks every method; writes `reports/summary.md` and appends to `reports/results.tsv` |
| `./apply.sh --pause` | presentation mode: waits for Enter before each step |
| `./apply.sh --full-check` / `--no-check` | check all rows / skip the check |
| `./generate_multi.sh [--rows 60M] [--changes 1M] [--force]` | builds the 9 other tables and their multi-level JSON changes (needs `generate.sh` first) |
| `./apply_multi.sh [--parallel 10\|5\|10,5,1] [--runs N]` | the 10-table phase: parallel COPY + optimized MERGE; writes `reports/multi_summary.md` |
| `./clean.sh [--all]` | drops the method tables (`--all`: the whole schema and the generated files) |

Settings (`vload.env` or environment): `CHANGE_MIX` (update:delete:insert, default `50:0:50`),
`HOT_PCT` (default 5), `JSON_FILES`, `GEN_SESSIONS`, `REBUILD_SESSIONS`, `MULTI_ROWS`,
`MULTI_JSON_FILES`, `PARALLEL`, `SCHEMA`, `DEMO_DIR`. See
[vload.env.example](vload.env.example). Every SQL statement, with its output, is kept in
`logs/`.

## Repository layout

```
generate.sh            phase 1, step 1: the 1B-row table and its JSON changes
apply.sh               phase 1, step 2: the three methods, timed and checked
generate_multi.sh      phase 2, step 1: 9 more tables and their multi-level JSON changes
apply_multi.sh         phase 2, step 2: 10 tables in parallel, timed and checked
clean.sh               remove the demo
conf/tables.def        the 10 ADABAS files of phase 2: JSON paths, columns, types, generators
lib/common.sh          settings, console output, timers, vsql wrapper, job pool
lib/sql.sh             phase 1 SQL: DDL, generator, COPY, swap, MERGE, LAP
lib/multi.sh           phase 2 SQL, generated from tables.def
lib/gen_changes.awk    phase 1 JSON change records (portable awk)
lib/gen_json.awk       phase 2 multi-level JSON change records for any table of tables.def
examples/              standalone SQL (Top-K LAP basics)
docs/                  methods, ADABAS mapping, results, video script
```

## Requirements

- Vertica 12.x or newer (tested on 26.2) with the flex table package (`FJSONPARSER`)
- bash ≥ 4.3, awk (mawk is fastest)
- Disk: about 37 GB for the 1B-row table and 75 GB for the journal with its LAP, plus a few
  GB while the methods run. Phase 2 adds about 27 GB for the 9 tables. The JSON change files
  take about 320 MB (phase 1) and 3.3 GB (phase 2).

## Disclaimer

This repository is a demo. It is not a product of, and is not endorsed or supported by,
Rocket Software, Vertica or any other company. The software is provided "as is", without
warranty of any kind, as the MIT license says. Please try it on your own systems and data
before you rely on it.

Vertica, Rocket Software and all other product and company names are trademarks or registered
trademarks of their respective owners.

## License

[MIT](LICENSE)
