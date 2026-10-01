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

It's two bash scripts built on `vsql`, with the SQL inside. Every step prints **what** runs,
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

## Results

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
| `./clean.sh [--all]` | drops the method tables (`--all`: the whole schema and the generated files) |

Settings (`vload.env` or environment): `CHANGE_MIX` (update:delete:insert, default `50:0:50`),
`HOT_PCT` (default 5), `JSON_FILES`, `GEN_SESSIONS`, `REBUILD_SESSIONS`, `SCHEMA`, `DEMO_DIR`. See
[vload.env.example](vload.env.example). Every SQL statement, with its output, is kept in
`logs/`.

## Repository layout

```
generate.sh            step 1: the data
apply.sh               step 2: the three methods, timed and checked
clean.sh               remove the demo
lib/common.sh          settings, console output, timers, vsql wrapper
lib/sql.sh             every SQL statement: DDL, generator, COPY, swap, MERGE, LAP
lib/gen_changes.awk    the JSON change records (portable awk)
examples/              standalone SQL (Top-K LAP basics)
docs/                  methods, ADABAS mapping, results, video script
```

## Requirements

- Vertica 12.x or newer (tested on 26.2) with the flex table package (`FJSONPARSER`)
- bash ≥ 4.3, awk (mawk is fastest)
- Disk: about 37 GB for the 1B-row table and 75 GB for the journal with its LAP, plus a few
  GB while the methods run. The JSON change files take about 350 MB.

## Disclaimer

This repository is a demo. It is not a product of, and is not endorsed or supported by,
Rocket Software, Vertica or any other company. The software is provided "as is", without
warranty of any kind, as the MIT license says. Please try it on your own systems and data
before you rely on it.

Vertica, Rocket Software and all other product and company names are trademarks or registered
trademarks of their respective owners.

## License

[MIT](LICENSE)
