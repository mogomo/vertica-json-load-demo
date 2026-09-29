# Video script: loading ADABAS JSON into Vertica, three ways

Target length is 15–20 minutes. Record in a terminal at least 120 columns wide, with a dark
theme and a large font. `--pause` stops before every step, so you can talk over the WHAT / WHY
/ SQL panel and press Enter when ready.

Preparation (off camera):

```bash
./vload.sh check
./vload.sh generate --scale 10K
./vload.sh generate --scale 1B        # ~15 min, ~57 GB (zstd)
./vload.sh clean                      # start with empty schemas
```

---

## Scene 1 — The problem (1 min)

*Slide or README top.* A mainframe ADABAS system is being offloaded to Vertica. Ten ADABAS
files arrive as **hierarchical JSON**: groups, multiple-value fields and periodic groups.
First comes a full unload of **one billion records**, then CDC doses with inserts, updates and
deletes. We need to:

1. load 10 tables **in parallel**, as fast as Vertica allows,
2. apply the changes efficiently,
3. prove that the result is correct.

We'll compare three Vertica techniques.

## Scene 2 — The data (2 min)

```bash
less conf/tables.def
zstd -dc data/10K/customer/base/part_0001.json.zst | head -1 | python3 -m json.tool
```

- Point at `hdr` (ISN, op, ts) and at `rec`: the `name` group, the `phone` MU array and the
  `address` PE array of objects.
- One definition file drives everything: generator, DDL, COPY mapping and MERGE.

## Scene 3 — The COPY, on 10K rows (3 min)

```bash
./vload.sh run --scale 10K --method merge --pause
```

- **Step 1.1:** the DDL, with `PRIMARY KEY … DISABLED`, `ORDER BY isn`, `SEGMENTED BY HASH(isn)`
  and monthly partitions.
- **Step 2.1:** the COPY. Explain `FJSONPARSER(flatten_arrays=true)`, then how the `FILLER`
  columns turn `"rec.address.1.city"` into `addr2_city`. Explain `ZSTD`, the glob with many
  files, `ON ANY NODE`, `REJECTED DATA AS TABLE` and `STREAM NAME`.
- Point at the per-table timing and the **parallel speed-up** line.

## Scene 4 — Method 2, optimized MERGE (2 min)

In the same run, continue to the doses.

- Show the MERGE and the three rules: declared key, all columns, same values.
- Show the **EXPLAIN** that the runner prints: `DML DELETE` + `DML INSERT` over a presorted
  `MERGEJOIN [Semi]`, and no `DML MERGE`.
- At the end, show the **delete vectors** count. That's the cost of updating in place.

## Scene 5 — Method 1, partition swap (3 min)

```bash
./vload.sh run --scale 10K --method swap --pause
```

- Base load goes into `_stage`, then `MOVE_PARTITIONS_TO_TABLE`: publishing is metadata only.
- Dose: delta COPY, then the rebuild of **only the touched partitions** (show the partition
  list in the SQL), then `SWAP_PARTITIONS_BETWEEN_TABLES`.
- Key message: no delete vectors and an atomic switch, but the cost follows the size of the
  touched partitions. It suits recent-data CDC.

## Scene 6 — Method 3, journal + Top-K LAP (3 min)

```bash
./vload.sh run --scale 10K --method lap --pause
```

- The `LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)` projection.
- Dose = COPY, and nothing else.
- **History** panel: one ISN with versions I → U → U → U, and the view showing only the last.
- `EXPLAIN` shows `Rewritten TOPK`: the view is answered from the LAP.
- Purge:

```bash
./vload.sh purge --scale 10K --pause
```

## Scene 7 — Correctness (1 min)

```bash
./vload.sh validate --scale 10K
```

Same live rows and checksum for every table in every method.

## Scene 8 — One billion rows (4 min, speed up the recording)

```bash
./vload.sh demo --scale 1B --drop-after
```

- The live progress line shows rows loaded and rows/s, read from `v_monitor.load_streams`.
- In a second terminal, you can show:

```sql
SELECT table_name, stream_name, accepted_row_count, rejected_row_count, read_bytes, unsorted_row_count
  FROM v_monitor.load_streams WHERE is_executing ORDER BY 1;
```

- `--drop-after` keeps disk usage to one method at a time. The fingerprints recorded for each
  method still allow validation.

## Scene 9 — Results and the decision (2 min)

```bash
./vload.sh report --scale 1B
```

Walk through the table in [`RESULTS.md`](RESULTS.md) and the decision matrix in
[`METHODS.md`](METHODS.md#choosing-a-method):

- **Swap partitions** when changes concentrate in few partitions and publication must be atomic.
- **Optimized MERGE** when changes are spread out and small compared to the table.
- **Journal + LAP** when MERGE can't be optimized, or when you need the full history. Purge it
  periodically.
