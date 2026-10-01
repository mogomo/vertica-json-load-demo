# Results

Phase 1: one 1-billion-row table, three methods. Phase 2: ten tables in parallel.

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

## Applying 1 million changes to 1 billion rows

`./apply.sh --runs 3`. Each method starts from the same pristine table (reset with
`COPY_TABLE`, not timed). The timer covers **parsing the JSON + loading + applying**.

<!-- results:begin -->
| Method | Parse + load JSON | Apply | **Total** | Rows/s | Runs | Delete vectors |
|---|---:|---:|---:|---:|---|---:|
| 1 Upsert (journal + Top-K LAP) | 2.12 s | – | **2.12 s** | 471K | 2.06 / 2.13 / 2.17 s | 0 |
| 2 Staging + partition SWAP | 1.23 s | 14.66 s | **15.89 s** | 63K | 15.74 / 15.92 / 16.01 s | 0 |
| 3 Optimized MERGE | 1.31 s | 0.89 s | **2.21 s** | 453K | 2.17 / 2.24 / 2.21 s | 500,000 |
<!-- results:end -->

All three methods produced **identical data** in every run: 56,156,934 current rows in the
touched partitions and newer, with the same checksum. A separate `./apply.sh --full-check`
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

## Phase 2: 10 tables in parallel

The 1-billion-row `txn` table plus 9 more ADABAS files of 60,000,000 rows each (customer,
account, card, loan, payment, policy, claim, employees, vehicles: groups, multiple-value fields
and periodic groups, see [ADABAS_MAPPING.md](ADABAS_MAPPING.md)). Every table receives
1,000,000 multi-level JSON changes (500,000 updates + 500,000 inserts): 10,000,000 changes in
all. Each table runs its own pipeline, COPY of its JSON files into a delta table, then an
optimized MERGE, and `--parallel` sets how many pipelines run at once.

`./generate_multi.sh` built the 9 tables (540M rows, 27 GB) in 2 min 14 s at 4.4M rows/s,
and the 9M JSON records (72 files, 3.3 GB) in 8 s.

`./apply_multi.sh --parallel 10,5,1 --runs 3`. The wall-clock runs from the start of the first
COPY to the commit of the last MERGE:

<!-- multi:begin -->
| Tables at a time | Wall-clock (avg) | Runs | JSON rows/s | vs one at a time |
|---|---:|---|---:|---:|
| **10** (all in parallel) | **12.24 s** | 12.50 / 12.42 / 11.81 s | 817K | 2.3x |
| 5 (a new table starts when one finishes) | 12.88 s | 13.18 / 12.98 / 12.47 s | 777K | 2.2x |
| 1 (one after the other) | 28.20 s | 28.30 / 28.27 / 28.04 s | 355K | 1.0x |

Per table, average of 3 runs (parse + load / MERGE / total, in seconds):

| Table | Rows | 1 at a time | 5 at a time | 10 at a time |
|---|---:|---:|---:|---:|
| txn | 1,000,000,000 | 1.36 / 0.92 / **2.28** | 3.78 / 1.44 / **5.22** | 6.32 / 2.46 / **8.78** |
| customer | 60,000,000 | 2.31 / 2.03 / **4.34** | 5.45 / 4.52 / **9.97** | 9.52 / 2.67 / **12.19** |
| account | 60,000,000 | 1.72 / 0.68 / **2.40** | 4.59 / 1.14 / **5.73** | 8.25 / 1.67 / **9.92** |
| card | 60,000,000 | 1.79 / 0.71 / **2.51** | 4.66 / 1.48 / **6.14** | 8.63 / 1.63 / **10.26** |
| loan | 60,000,000 | 1.85 / 0.69 / **2.54** | 4.49 / 1.18 / **5.67** | 8.50 / 1.73 / **10.23** |
| payment | 60,000,000 | 1.88 / 1.83 / **3.71** | 4.35 / 2.96 / **7.31** | 9.42 / 2.57 / **11.99** |
| policy | 60,000,000 | 1.84 / 0.71 / **2.55** | 4.07 / 1.26 / **5.33** | 8.91 / 1.54 / **10.45** |
| claim | 60,000,000 | 1.74 / 0.66 / **2.40** | 3.97 / 1.20 / **5.16** | 8.72 / 1.49 / **10.21** |
| employees | 60,000,000 | 2.08 / 0.98 / **3.06** | 4.32 / 1.42 / **5.74** | 9.50 / 1.52 / **11.02** |
| vehicles | 60,000,000 | 1.67 / 0.69 / **2.36** | 2.14 / 0.76 / **2.90** | 8.51 / 1.65 / **10.15** |
<!-- multi:end -->

Every run of every setting passed the check: each table holds its base rows plus the 500,000
inserts, exactly 1,000,000 rows carry the new batch, and their checksum over all columns equals
the checksum of the JSON rows.

### What it means

- **Parallel pays: 28.2 s → 12.2 s** for 10 million JSON changes in 10 tables, 2.3 times
  faster than one table after the other.
- **10 at a time vs 5 at a time makes little difference** (12.2 s vs 12.9 s). The bottleneck is
  JSON parsing: one COPY already parses its files with several threads (8 files per table,
  22 for txn), so with five tables at once the 22 cores are already busy. Beyond that,
  extra concurrency only stretches each table's time (2.3 → 8.8 s for txn) while the total
  stays the same. On this machine the ceiling is about **800,000 parsed and merged JSON rows
  per second**.
- So 10 in parallel is not "too heavy": it is safe and slightly faster, with every MERGE
  still on the optimized plan. Five at a time is a good choice when other work shares the
  database, since it gives the same throughput with half the sessions and memory.
- The size of the target hardly matters: the MERGE of 1M changes into the 1B-row table took
  0.92 s, against 0.66–2.03 s for the 60M-row tables. The cost follows the changes and the
  width of the records (customer and payment, with the most columns and nested arrays, are the
  slowest).

## Reproduce

```bash
./generate.sh              # ~12 min, ~116 GB in Vertica
./apply.sh --runs 3        # ~2.5 min including the checks
./apply.sh --full-check    # optional: compare all 1 billion current rows (~6 min)
./generate_multi.sh        # ~2.5 min, ~27 GB in Vertica + 3.3 GB of JSON
./apply_multi.sh --parallel 10,5,1 --runs 3
```
