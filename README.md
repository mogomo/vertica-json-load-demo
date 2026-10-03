# JSON upserts into a 1-billion-row Vertica table: three methods, three phases

A mainframe ADABAS system sends its changes (CDC) as **hierarchical JSON**. Each batch holds
**1 million** changed records per file: half **updates** of existing rows, half **inserts** of
new rows. They must reach Vertica, where the biggest table has **1 billion rows**, as fast as
possible. The timer always covers the **whole job**: parsing the JSON, loading it and
applying the changes.

| Phase | Data | What is measured |
|---|---|---|
| **1** | 1M JSON changes → one 1B-row table | the three upsert methods side by side |
| **2** | 10M JSON changes → 10 tables (1B + 9 × 60M); **one record per line, the 10 tables mixed in every file** | one COPY into a flat staging table, then 10 parallel MERGEs |
| **3** | the same 10M changes; **one ADABAS transaction per line: a nested document with an array of records for each file** | one COPY into a document staging table, then 10 parallel MERGEs |

The three upsert methods of phase 1:

| # | Method | In one line |
|---|---|---|
| 1 | **Insert-only upsert: journal + Top-K LAP** | never UPDATE: `COPY` every change into a journal; a `LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)` Live Aggregate Projection serves the newest version |
| 2 | **Staging table + partition COPY/SWAP** | `COPY` into a staging table, rebuild only the touched partitions, publish them with one atomic `SWAP_PARTITIONS_BETWEEN_TABLES` |
| 3 | **Optimized MERGE** | `COPY` into a delta table shaped like the target, then one `MERGE` that meets the optimization rules |

It's bash scripts built on `vsql`, with the SQL inside. Every step prints **what** runs,
**why**, the **SQL** and **how long** it took, so it can be presented and recorded. Every
phase is independent, has its own timings and can be run again and again: each run starts
from the same data.

## Quick start

On a Vertica node (the JSON files must be readable by the server), as a user that can
create a schema:

```bash
git clone https://github.com/mogomo/vertica-json-load-demo.git && cd vertica-json-load-demo
cp vload.env.example vload.env     # optional: connection and settings

./generate.sh                      # once: the 1B-row table (SQL) + 1M JSON changes        (~12 min)
./phase1.sh                        # phase 1: three methods, timed and checked               (~1 min)

./generate_multi.sh                # once: 9 tables × 60M rows + 10M JSON changes, 2 shapes   (~2.5 min)
./phase2.sh                        # phase 2: mixed records → flat staging → 10 MERGEs       (~1 min)
./phase3.sh                        # phase 3: nested documents → staging → 10 MERGEs         (~1 min)

./phase1.sh --runs 3                          # repeat and average
./phase2.sh --parallel 10,5,1 --runs 3        # 10, 5 or 1 MERGEs at a time
```

Try it small first: `./generate.sh --rows 10M --changes 100K`, `./phase1.sh`,
`./generate_multi.sh --rows 1M --changes 100K`, `./phase2.sh`, `./phase3.sh`.

## Results

Measured on a single-node Vertica 26.2 (22 hardware threads, 61 GB RAM, one NVMe SSD),
averages of 3 runs. Details and analysis: [docs/RESULTS.md](docs/RESULTS.md).

<!-- results:begin -->
### Phase 1: 1,000,000 JSON changes → 1,000,000,000-row table

500,000 updates + 500,000 inserts:

| Method | Parse + load JSON | Apply | **Total** | Rows/s | Delete vectors |
|---|---:|---:|---:|---:|---:|
| 1 Upsert (journal + Top-K LAP) | 2.12 s | – | **2.12 s** | 471K | 0 |
| 2 Staging + partition SWAP | 1.23 s | 14.66 s | **15.89 s** | 63K | 0 |
| 3 Optimized MERGE | 1.31 s | 0.89 s | **2.21 s** | 453K | 500,000 |

All three methods end with **identical data**: a `--full-check` compared all 1,000,500,000
current rows (same count, same checksum).

### Phases 2 and 3: 10,000,000 JSON changes → 10 tables

1,000,000 changes per table (500,000 updates + 500,000 inserts) into the 1B-row table and 9
tables of 60M rows. The JSON is parsed **once**, then 10 optimized MERGEs run in parallel:

| | JSON shape | COPY (one parse) | 10 MERGEs, 10 at a time | **Total** | Changes/s |
|---|---|---:|---:|---:|---:|
| Phase 2 | 10M records, one per line, 10 tables mixed | 30.10 s | 2.46 s | **32.56 s** | 307K |
| Phase 3 | 1M transactions, nested arrays of records | 20.90 s | 5.62 s | **26.51 s** | 377K |

| MERGEs at a time | Phase 2 total | Phase 3 total |
|---|---:|---:|
| 10 | **32.56 s** | **26.51 s** |
| 5 | 33.14 s | 26.79 s |
| 1 (one after the other) | 38.60 s | 35.45 s |

Every table passes its check in every run: base rows + 500,000 inserts, and the 1,000,000
changed rows equal the JSON rows (count and checksum of all columns).
<!-- results:end -->

**In short:** for a small batch on a big table, the optimized MERGE and the insert-only upsert
are equally fast, and parsing the JSON is most of the work. When the files mix many tables,
parse them **once** and fan out inside the database: ten MERGEs running in parallel take
2.5–5.6 s for 10 million changes. Nested documents parse faster than the same records as
flat lines (20.9 s vs 30.1 s), because the parser handles one tenth of the rows.

## What the scripts do

```
generate.sh
  ├─ vload.txn_base       1,000,000,000 rows, generated in Vertica with INSERT … SELECT
  │                       (36 monthly partitions, 11 parallel sessions, one partition per statement)
  ├─ vload.txn_jrn_base   the same rows as an insert-only journal + Top-K LAP
  └─ demo/changes/*.json  1,000,000 CDC records: 500,000 updates + 500,000 inserts

phase1.sh  (per run, per method)
  ├─ reset    COPY_TABLE from the pristine table: catalog only, milliseconds   (not timed)
  ├─ ⏱ parse + load   COPY … FROM 'demo/changes/*.json' PARSER FJSONPARSER(flatten_arrays=true)
  ├─ ⏱ apply          upsert: nothing more │ swap: parallel rebuild + SWAP │ merge: MERGE
  └─ check    row count + checksum: identical in every method                  (not timed)

generate_multi.sh
  ├─ vload.<table>_base   9 more ADABAS files (conf/tables.def), 60,000,000 rows each (SQL)
  ├─ demo/phase2/*.json   10 × 1,000,000 records, one per line, the 10 tables mixed
  └─ demo/phase3/*.json   the same records as 1,000,000 ADABAS transactions (nested documents)

phase2.sh / phase3.sh  (per run, per --parallel setting)
  ├─ reset    COPY_TABLE × 10, staging table recreated                         (not timed)
  ├─ ⏱ COPY   all JSON files, parsed once, into stg_flat (phase 2) / stg_doc (phase 3)
  ├─ ⏱ MERGE  10 optimized MERGEs, one per table, reading the staging table, N at a time
  └─ check    per table: rows + inserts; changed rows = JSON rows (checksum)    (not timed)
```

- **The data:** ADABAS files with a CDC header, groups, multiple-value fields and periodic
  groups, and the two JSON shapes of phases 2 and 3: [docs/ADABAS_MAPPING.md](docs/ADABAS_MAPPING.md).
- **Repeatable:** `COPY_TABLE` resets a table in milliseconds without copying data, so every
  run of every phase starts from exactly the same rows.
- **The methods:** reasoning, SQL and pitfalls in [docs/METHODS.md](docs/METHODS.md).
- **The video:** *MERGE 1 Million Changes into a 1-Billion-Row Table in Under a Second: Three
  ways to update data in Vertica* walks through the three methods and the three phases in
  13 minutes; its transcript: [docs/VIDEO_TRANSCRIPT.md](docs/VIDEO_TRANSCRIPT.md).

## Which method, when?

| | Upsert (journal + Top-K LAP) | Swap partitions | Optimized MERGE |
|---|---|---|---|
| Best when | high change rates, MERGE can't be optimized, history is needed | changes cluster in few, small partitions; atomic publication | changes are spread out and small compared to the table |
| Apply cost grows with | number of changes | size of the touched partitions | number of changes |
| Delete vectors | none | none | one per updated row |
| Storage | about 2x (journal + LAP) + history | 1x | 1x + delete vectors |
| Version history | every version, with time | – | – |

When one file feeds many tables: parse it once into a staging table, then run one MERGE per
table, in parallel. Don't run one COPY per table over the same files: each would parse
everything.

## Commands

| Command | Does |
|---|---|
| `./generate.sh [--rows 1B] [--changes 1M] [--force]` | the 1B-row table, its journal + LAP, and the phase 1 JSON. Keeps existing tables of the right size unless `--force` |
| `./phase1.sh [--runs N] [--method upsert,swap,merge] [--full-check \| --no-check]` | phase 1; writes `reports/phase1_summary.md` |
| `./generate_multi.sh [--rows 60M] [--changes 1M] [--force]` | the 9 other tables and the phase 2 and 3 JSON (needs `generate.sh` first) |
| `./phase2.sh [--parallel 10\|5\|10,5,1] [--runs N] [--no-check]` | phase 2; writes `reports/phase2_summary.md` |
| `./phase3.sh [--parallel 10\|5\|10,5,1] [--runs N] [--no-check]` | phase 3; writes `reports/phase3_summary.md` |
| `--pause` (every phase) | presentation mode: waits for Enter before each step |
| `./clean.sh [--all]` | drops the working tables (`--all`: the whole schema and the generated files) |

Settings (`vload.env` or environment): `CHANGE_MIX` (update:delete:insert, default `50:0:50`),
`HOT_PCT` (default 5), `JSON_FILES`, `GEN_SESSIONS`, `REBUILD_SESSIONS`, `MULTI_ROWS`,
`PARALLEL`, `SCHEMA`, `DEMO_DIR`. See [vload.env.example](vload.env.example). Every SQL
statement, with its output, is kept in `logs/`.

## Repository layout

```
generate.sh            the 1B-row table and the phase 1 JSON
phase1.sh              phase 1: three methods, timed and checked
generate_multi.sh      the 9 other tables and the phase 2 and 3 JSON
phase2.sh, phase3.sh   phases 2 and 3: one parse, 10 parallel MERGEs, timed and checked
clean.sh               remove the demo
conf/tables.def        the 10 ADABAS files: JSON paths, columns, types, generators
lib/common.sh          settings, console output, timers, vsql wrapper, job pool
lib/sql.sh             phase 1 SQL: DDL, generator, COPY, swap, MERGE, LAP
lib/multi.sh           phase 2 and 3 SQL, generated from tables.def
lib/phase23.sh         the runner of phases 2 and 3
lib/gen_changes.awk    phase 1 JSON records (portable awk)
lib/gen_json.awk       multi-level JSON records for any table of tables.def
lib/gen_docs.awk       the phase 2 and phase 3 shapes of the same records
examples/              standalone SQL (Top-K LAP basics)
docs/                  methods, ADABAS mapping, results, video transcript
```

## Requirements

- Vertica 12.x or newer (tested on 26.2) with the flex table package (`FJSONPARSER`)
- bash ≥ 4.3, awk (mawk is fastest)
- Disk: about 38 GB for the 1B-row table and 78 GB for its journal with the LAP (phase 1),
  27 GB for the 9 other tables, a few GB while the phases run, and 0.3 GB + 7.5 GB of JSON.

## Disclaimer

This repository is a demo. It is not a product of, and is not endorsed or supported by,
Rocket Software, Vertica or any other company. The software is provided "as is", without
warranty of any kind, as the MIT license says. Please try it on your own systems and data
before you rely on it.

Vertica, Rocket Software and all other product and company names are trademarks or registered
trademarks of their respective owners.

## License

[MIT](LICENSE)
