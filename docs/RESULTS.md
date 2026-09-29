# Measured results

**Test system:** single-node Vertica 26.2 Community Edition on one workstation (22 hardware
threads, 61 GB RAM, one NVMe SSD) running Ubuntu 26.04. The data generator and the database
share the same machine and disk.

**Workload per scale:** 10 tables load in parallel. Each table gets a base load of scale/10
rows, then 3 doses of 1% of its rows each. A dose is 60% updates, 10% deletes and 30% inserts;
updates and deletes hit the newest 5% of the ISNs. Files are zstd-compressed JSON Lines, and
each table and stream is split into 6 files.

Reproduce with `./vload.sh demo --scale <S>` (add `--drop-after` at 1B).
`./vload.sh report --scale <S>` prints the same tables from `reports/results.tsv`.

<!-- 1B:begin -->
## 1B rows (10 × 100M)

`./vload.sh demo --scale 1B --drop-after`: each method's schema is fingerprinted, measured and
dropped before the next method runs, so only one method occupies disk at a time.

| Method | Base load | Base rows/s | Avg dose apply (10 × 1M changes) | Total incl. doses | Storage | Delete-vector rows |
|---|---:|---:|---:|---:|---:|---:|
| Swap partitions | 1,045.8 s | 956K | 28.3 s | 1,133.8 s | 52,436 MB | 0 |
| Optimized MERGE | 1,051.2 s | 951K | 13.5 s | 1,093.7 s | 53,342 MB | 17,381,271 |
| Journal + Top-K LAP ¹ | 1,458.0 s | 686K | 13.1 s | 1,498.7 s | 107,074 MB | 0 |
| LAP journal purge ² | 1,703.3 s | – | – | 1,703.6 s | 104,917 MB | 0 |

Dose apply time broken into steps (10 tables in parallel, per dose):

| Method | COPY dose → delta/journal | Apply | Publish |
|---|---:|---:|---:|
| Swap partitions | 9.3–9.9 s | rebuild of 57.7–63.7M rows (touched partitions): 17.4–19.7 s | swap: 0.12–0.13 s |
| Optimized MERGE | 9.3–9.7 s | MERGE: 3.6–4.4 s | – |
| Journal + Top-K LAP | 12.6–13.6 s | – (the COPY is the apply) | – |

- **Correctness:** all four result sets have 100,800,000 live rows per table with identical
  checksums.
- **Publishing** 1B rows with `MOVE_PARTITIONS_TO_TABLE` took 0.53 s.
- **Generation:** 1,030,000,000 JSON records, 57.7 GB zstd (about 470 GB as plain JSON), in
  1,013 s (1.02M rows/s).
- **Delete vectors:** MERGE created 21M of them. The Tuple Mover had already purged about
  3.6M while the run was still going, which left the 17.4M in the table.

¹ The first attempt, with one COPY per table and stream, failed with `ERROR 2927 … insufficient
space`. The DATA and TEMP locations share one ~200 GB disk with the 58 GB of JSON, and 10
concurrent 100M-row COPYs spill their sort runs to TEMP before writing the final containers.
The LAP method has twice the data (anchor + Top-K projection), so the peak didn't fit. Loading
with `COPY_BATCH_FILES=2` (3 consecutive COPYs of 2 files per table) bounded the peak. It
completed at 686K rows/s.

² The purge ran with `PURGE_PARALLEL=2`: 2 tables at a time, because a purge briefly holds the
old and the new copy of a table. Before the purge, the already-loaded base JSON files were
deleted to free space; `generate` recreates them identically from the seed.
<!-- 1B:end -->

## 100M rows (10 × 10M)

| Method | Base load | Base rows/s | Avg dose apply (10 × 1M changes) | Storage | Delete-vector rows |
|---|---:|---:|---:|---:|---:|
| Swap partitions | 98.1 s | 1.02M | 2.63 s | 5,225 MB | 0 |
| Optimized MERGE | 102.3 s | 0.98M | 1.53 s | 5,332 MB | 2,100,000 |
| Journal + Top-K LAP | 141.1 s | 0.71M | 1.43 s | 10,669 MB | 0 |
| LAP journal purge | 65.6 s | – | – | 10,455 MB | 0 |

Dose apply time broken into steps (10 tables in parallel, per dose):

| Method | COPY dose → delta/journal | Apply | Publish |
|---|---:|---:|---:|
| Swap partitions | 1.05 s | rebuild of ~6M rows (touched partitions): 1.37–1.58 s | swap: 0.07 s |
| Optimized MERGE | 1.06 s | MERGE: 0.43–0.48 s | – |
| Journal + Top-K LAP | 1.43 s | – (the COPY is the apply) | – |

All four result sets are identical: 10,080,000 live rows per table with the same checksum.
Generating the data took 96 s at 1.08M rows/s, 5.7 GB.

## 10M rows (10 × 1M)

| Method | Base load | Base rows/s | Avg dose apply (10 × 100K changes) | Storage | Delete-vector rows |
|---|---:|---:|---:|---:|---:|
| Swap partitions | 8.4 s | 1.19M | 0.51 s | 514 MB | 0 |
| Optimized MERGE | 8.4 s | 1.20M | 0.26 s | 525 MB | 210,000 |
| Journal + Top-K LAP | 12.2 s | 0.82M | 0.21 s | 1,050 MB | 0 |
| LAP journal purge | 7.1 s | – | – | 1,029 MB | 0 |

## Reading the numbers

- **COPY throughput stays flat as volume grows**, at about 1M JSON rows/s on this box from
  10M to 1B rows. Parsing JSON is CPU-bound: the 10 parallel COPYs keep all cores busy (the
  runner reports a speed-up of 9–10x over running the tables one by one). On a cluster,
  `ON ANY NODE` spreads the parsing across the nodes.
- **Publishing is free.** `MOVE_PARTITIONS_TO_TABLE` / `SWAP_PARTITIONS_BETWEEN_TABLES` take
  0.1–0.5 s for any volume (1B rows included), because they're catalog operations.
- **Swap partitions** pays for the size of the **touched partitions** (about 6% of each table
  here, because updates are recent), not for the size of the dose. It leaves no delete vectors
  and switches atomically. If `HOT_PCT` grows until every dose touches every partition, the
  rebuild becomes a full table rewrite. That's the point where MERGE wins. At 1B, a dose
  rebuilt about 60M rows to apply 10M changes, and that still took only 18 s.
- **Optimized MERGE** pays for the **dose**. With the delta table created
  `LIKE … INCLUDING PROJECTIONS`, the join is local and presorted. 1M changes against 100M rows
  merge in under half a second, and 10M changes against 1B rows in about 4 s. Its cost shows up
  later as delete vectors (2.1M at 100M, 21M at 1B), which the Tuple Mover has to purge.
- **Journal + LAP** makes applying a dose as cheap as loading data, and keeps the full version
  history. It pays at load time (about 30% slower base load, because the Top-K projection is
  maintained) and in storage (about 2x), until the journal is purged.
