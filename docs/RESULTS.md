# Results

Phase 1: one 1-billion-row table, three methods. Phases 2 and 3: ten tables, the JSON parsed once,
10 parallel MERGEs.

## Test system

- Vertica 26.2 Community Edition, **single node**
- 22 hardware threads, 61 GB RAM, one NVMe SSD (Vertica data and temp on the same disk)
- Ubuntu 26.04, power profile "performance", background update timers disabled
- Default settings: 22 JSON files, 11 parallel sessions for generation and for the swap rebuild

## The data

| | |
|---|---|
| Fact table `vload.txn_base` | 1,000,000,000 rows, 19 columns, 36 monthly partitions (~28 M rows each), 38 GB |
| Journal `vload.txn_jrn_base` + Top-K LAP | the same 1,000,000,000 rows twice (anchor + LAP), 78 GB |
| Change files `demo/changes/*.json` | 1,000,000 JSON records in 22 files, 317 MB |
| Change mix | 500,000 updates of existing rows (in the newest 5 % of the ISNs) + 500,000 inserts of new rows |

`./generate.sh` built all of it in **12 min 19 s**:

| Step | Time | Rate |
|---|---:|---:|
| `txn_base`: 1B rows with `INSERT … SELECT`, 11 sessions, one partition per statement | 3 min 51 s | 4.33 M rows/s |
| `txn_jrn_base`: the same rows into the journal, maintaining the Top-K LAP | 8 min 14 s | 2.02 M rows/s |
| `ANALYZE_STATISTICS` on both tables | 14 s | |
| 1M JSON change records (22 awk processes) | 0.3 s | |

## Phase 1: 1 million changes into 1 billion rows

`./phase1.sh --runs 3`. Each method starts from the same pristine table (reset with
`COPY_TABLE`, not timed). The timer covers **parsing the JSON + loading + applying**.

<!-- results:begin -->
| Method | Parse + load JSON | Apply | **Total** | Rows/s | Runs | Delete vectors |
|---|---:|---:|---:|---:|---|---:|
| 1 Upsert (journal + Top-K LAP) | 2.12 s | – | **2.12 s** | 471K | 2.06 / 2.13 / 2.17 s | 0 |
| 2 Staging + partition SWAP | 1.23 s | 14.66 s | **15.89 s** | 63K | 15.74 / 15.92 / 16.01 s | 0 |
| 3 Optimized MERGE | 1.31 s | 0.89 s | **2.21 s** | 453K | 2.17 / 2.24 / 2.21 s | 500,000 |
<!-- results:end -->

All three methods produced **identical data** in every run: 56,156,934 current rows in the
touched partitions and newer, with the same checksum. A separate `./phase1.sh --full-check`
run compared all **1,000,500,000** current rows of the three methods (1,000,000,000 + 500,000
inserted) and found the same count and checksum everywhere.

### Where the time goes

**Upsert (journal + Top-K LAP), 2.1 s.** The whole job is one COPY. It is ~0.9 s slower than
the plain COPY of the other methods because the journal is sorted by `(isn, change_ts)` and the
COPY also maintains the Top-K projection. There is no apply step at all.

**Optimized MERGE, 2.2 s.** COPY into the delta table 1.3 s, MERGE 0.9 s. `EXPLAIN` shows the
optimized plan (`DML DELETE` + `DML INSERT` over a presorted merge join). The target has a
billion rows, yet the MERGE takes under a second: its cost follows the 1 million changes,
not the size of the table. It leaves 500,000 delete vectors behind (one per
updated row) for the Tuple Mover to purge.

**Staging + partition SWAP, 15.9 s.** A typical run:

| Step | Time |
|---|---:|
| COPY into the staging table | 1.20 s |
| find the touched partitions (202511, 202512 and the new 202601) | 0.03 s |
| create the stage table + ISN range of the rows to rebuild | 1.31 s |
| rebuild the touched partitions: 56.2 M rows, 11 parallel sessions | 12.84 s |
| `SWAP_PARTITIONS_BETWEEN_TABLES` | 0.04 s |

The method rewrites every row of the touched partitions, ~56 times more rows than it
changes. With one session the rebuild took 44 s (total 45.6 s); 11 sessions bring it to 13 s.
In exchange, the fact table gets no delete vectors and the new data appears atomically.

### What it means

- When the changes are a small slice of a big table, **optimized MERGE and the insert-only
  upsert are equally fast**: about 2 seconds for a million JSON rows, of which more than half
  is parsing the JSON.
- The **upsert** wins when MERGE can't be optimized, when there are duplicate keys in a batch,
  or when the history of every row is needed. It pays with twice the storage and with Top-K
  work at query time: reading *all* current rows through the LAP took 324 s for 1 billion
  rows (3.1 M rows/s), while queries on a key or a key range stay fast.
- **Swap** costs the size of the touched partitions, not the size of the change. It suits
  batches that rewrite most of a few small partitions, and readers that must see a whole
  batch at once. Smaller partitions (daily instead of monthly) would make it much cheaper here.

### Repeatability

The three runs differ by less than 0.3 s per method. `COPY_TABLE` resets a 1-billion-row
table in 0.04–0.11 s and uses no extra disk: the method tables share the storage of
`txn_base` / `txn_jrn_base`, and only the partitions a method rewrites take new space (a few
GB). Note that `v_monitor.projection_storage` counts shared containers once per table, so it
reports ~38 GB for each copy.

## Phases 2 and 3: one parse, 10 parallel MERGEs

Ten tables: the 1-billion-row `txn` table plus 9 more ADABAS files of 60,000,000 rows each
(customer, account, card, loan, payment, policy, claim, employees, vehicles; groups,
multiple-value fields and periodic groups, see [ADABAS_MAPPING.md](ADABAS_MAPPING.md)). Each
table receives 1,000,000 multi-level JSON changes (500,000 updates + 500,000 inserts):
10,000,000 changes in all. `generate_multi.sh` writes the **same** records in two shapes:

| | Shape | Files |
|---|---|---|
| Phase 2 | one record per line, the table named in the header, the 10 tables mixed in every file: `{"hdr":{"file":"customer","isn":…},"customer":{…}}` | 22 files, 3.8 GB, 10,000,000 lines |
| Phase 3 | one ADABAS transaction per line, an array of changed records for each file (0, 1 or 2 records each): `{"et_id":…,"customer":[{…},{…}],"account":[],…}` | 22 files, 3.7 GB, 1,000,000 lines |

`generate_multi.sh` built the 9 tables (540M rows, 27 GB) in 2 min 4 s at 4.35M rows/s and
both JSON shapes in 11 s.

Both phases do the same two timed steps:

1. **One COPY parses every JSON file once** into a staging table.
   - Phase 2, `stg_flat`: one row per record, with the columns of all 10 tables side by side
     (`customer__first_name`, …), partitioned and sorted by table. A record fills only its own
     table's columns.
   - Phase 3, `stg_doc`: one row per transaction. `FJSONPARSER(flatten_arrays=true)` flattens
     the arrays too, so the 2nd customer record of a document arrives as
     `"customer.1.rec.name.first"`, and `stg_doc` has 2 slots of columns per table named
     exactly like those keys.
2. **10 optimized MERGEs, one per table**, read their rows straight from the staging table
   (phase 2: `WHERE file = '<table>'`; phase 3: `UNION ALL` of the table's slots), at most
   `--parallel` at a time.

`./phase2.sh --parallel 10,5,1 --runs 3` and `./phase3.sh --parallel 10,5,1 --runs 3`:

<!-- phases23:begin -->
| | MERGEs at a time | COPY (one parse) | 10 MERGEs | **Total** | Changes/s | Runs (total) |
|---|---:|---:|---:|---:|---:|---|
| **Phase 2** (flat records) | 10 | 30.10 s | 2.46 s | **32.56 s** | 307K | 32.73 / 32.13 / 32.81 s |
| | 5 | 30.16 s | 2.98 s | 33.14 s | 302K | 33.33 / 33.13 / 32.97 s |
| | 1 | 29.95 s | 8.65 s | 38.60 s | 259K | 39.09 / 38.17 / 38.55 s |
| **Phase 3** (nested documents) | 10 | 20.90 s | 5.62 s | **26.51 s** | 377K | 27.07 / 26.50 / 25.96 s |
| | 5 | 20.50 s | 6.30 s | 26.79 s | 373K | 26.82 / 26.85 / 26.71 s |
| | 1 | 20.66 s | 14.80 s | 35.45 s | 282K | 35.35 / 35.30 / 35.70 s |

MERGE time per table, average of 3 runs (seconds):

| Table | Rows | Phase 2: 10 / 5 / 1 at a time | Phase 3: 10 / 5 / 1 at a time |
|---|---:|---:|---:|
| txn | 1,000,000,000 | 1.92 / 1.32 / 0.93 | 5.28 / 3.62 / 2.51 |
| customer | 60,000,000 | 2.46 / 2.17 / 1.40 | 5.61 / 4.77 / 1.89 |
| account | 60,000,000 | 1.74 / 1.14 / 0.70 | 4.86 / 3.52 / 1.24 |
| card | 60,000,000 | 1.78 / 1.22 / 0.76 | 5.03 / 3.53 / 1.31 |
| loan | 60,000,000 | 1.69 / 1.15 / 0.71 | 4.89 / 3.50 / 1.26 |
| payment | 60,000,000 | 2.06 / 1.41 / 0.98 | 5.30 / 2.32 / 1.41 |
| policy | 60,000,000 | 1.79 / 1.12 / 0.75 | 5.09 / 2.14 / 1.28 |
| claim | 60,000,000 | 1.73 / 1.06 / 0.70 | 4.58 / 2.09 / 1.20 |
| employees | 60,000,000 | 2.02 / 1.39 / 0.98 | 5.25 / 2.32 / 1.44 |
| vehicles | 60,000,000 | 1.72 / 0.81 / 0.69 | 4.86 / 1.52 / 1.22 |
<!-- phases23:end -->

Every one of the 18 runs passed the check on every table: base rows + 500,000 inserts,
exactly 1,000,000 rows of the new batch, and their checksum over all columns equal to the
checksum of the source rows in the staging table.

### What it means

- **Parsing is the job.** The single COPY takes 79–92 % of the time; the 10 MERGEs into 1.54
  billion rows in all take 2.5 s (phase 2) and 5.6 s (phase 3) when they run in parallel.
- **Parse once.** One COPY over all the files, then fan out inside the database. Ten COPYs over
  the same mixed files would each parse all 10M records.
- **Nested documents parse faster than flat lines:** 20.9 s vs 30.1 s for the same 10M
  records. Phase 3 hands the parser 1M rows instead of 10M; the per-row cost (above all the
  ~160 mostly empty columns of the flat staging table) is paid ten times less often. For
  comparison, 1M records into one narrow table parse in 1.2 s (phase 1).
- **The phase 2 MERGEs are faster** (2.5 s vs 5.6 s): `stg_flat` is sorted by `(file, isn)`, so
  each MERGE reads a presorted slice and Vertica uses a merge join. Phase 3's `UNION ALL` of
  slots is not sorted on `isn`, so it uses a hash join. Both are the optimized
  `DML DELETE + DML INSERT` plan.
- **10 or 5 at a time hardly differ** (32.6 vs 33.1 s, 26.5 vs 26.8 s); both beat one after the
  other by 6–9 s. Ten parallel MERGEs are not "too heavy" for this machine.
- **Fixed slots vs maps.** Phase 3 needs a maximum number of records per file in one
  transaction (2 here, like the maximum occurrences of an ADABAS periodic group). Unbounded
  arrays can be kept as VMaps instead (`FJSONPARSER(flatten_arrays=false, flatten_maps=false)`,
  one `LONG VARBINARY` column per table) and exploded with `MAPITEMS` + `MAPLOOKUP`. We measured
  that alternative: the COPY was 3x slower, reading the fields ~15x slower, and a MERGE that
  reads a `MAPITEMS` query directly gets the generic, non-optimized plan, so the rows must be
  materialized first.

## Reproduce

```bash
./generate.sh              # ~12 min, ~116 GB in Vertica
./phase1.sh --runs 3       # ~2.5 min including the checks
./phase1.sh --full-check   # optional: compare all 1 billion current rows (~6 min)
./generate_multi.sh        # ~2.5 min, ~27 GB in Vertica + 7.5 GB of JSON
./phase2.sh --parallel 10,5,1 --runs 3
./phase3.sh --parallel 10,5,1 --runs 3
```
