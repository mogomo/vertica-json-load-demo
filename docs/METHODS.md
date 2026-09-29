# The three load / update methods

All three methods load the **same JSON files** into the **same table shape** and must end
with **identical current data**; `./vload.sh validate` checks this with a row count and a
checksum per table. They differ in how a CDC dose (inserts, updates and deletes) is applied.

- [Common ground: the COPY](#common-ground-the-copy)
- [Method 1: staging table + partition COPY/SWAP](#method-1--staging-table--partition-copyswap)
- [Method 2: optimized MERGE](#method-2--optimized-merge)
- [Method 3: insert-only journal + Top-K LAP](#method-3--insert-only-journal--top-k-live-aggregate-projection)
- [Choosing a method](#choosing-a-method)

---

## Common ground: the COPY

Every method starts with the same statement: a bulk `COPY` of JSON files.

```sql
COPY vload_merge.customer (
    "hdr.isn"               FILLER BIGINT,       isn          AS "hdr.isn",
    "hdr.op"                FILLER CHAR(1),      op_code      AS "hdr.op",
    ...
    "rec.name.first"        FILLER VARCHAR(30),  first_name   AS "rec.name.first",
    "rec.phone.0"           FILLER VARCHAR(20),  phone_1      AS "rec.phone.0",
    "rec.address.1.city"    FILLER VARCHAR(30),  addr2_city   AS "rec.address.1.city"
)
FROM '/data/1B/customer/base/*.json.zst' ON ANY NODE ZSTD
PARSER FJSONPARSER(flatten_arrays = true)
STREAM NAME 'vl_..._base_customer'
REJECTED DATA AS TABLE vload_merge.customer_rejects;
```

Why this is the fastest path into Vertica:

| Choice | Reason |
|---|---|
| `COPY`, not `INSERT` | COPY writes sorted, encoded, compressed ROS containers directly to disk in one transaction. There's no per-row overhead, and since Vertica 10 there's no WOS staging. |
| Many files per table | Each file is parsed by its own thread (`FILES_PER_TABLE`), so one COPY uses many cores. |
| 10 COPYs at once | The 10 tables load concurrently from 10 sessions and share the cluster's resources. The runner prints the parallel speed-up for every step. |
| `ON ANY NODE` | On a cluster with shared storage, every node takes part in parsing. |
| `ZSTD` | Files are ~8x smaller on disk, and decompression costs little CPU next to JSON parsing. |
| `FJSONPARSER(flatten_arrays=true)` + `FILLER` | Maps the nested JSON (groups, MU and PE arrays) onto plain relational columns in the same pass. There's no landing table and no second `INSERT … SELECT`. |
| `REJECTED DATA AS TABLE` | Bad records go to a table you can query instead of failing the load. |
| `STREAM NAME` | Progress shows live in `v_monitor.load_streams` (the runner's progress line uses it). |

The target is the same for every method. Tables are segmented by `HASH(isn)`, sorted by `isn`
and partitioned by month of the immutable `created_date`.

---

## Method 1 — staging table + partition COPY/SWAP

**Idea:** never modify the fact table in place. Build the new version of the affected
partitions next to it, then exchange partitions in a single catalog operation.

```
dose ─COPY─► delta ──┐
                     ├─ INSERT…SELECT (unchanged rows of touched partitions + new images) ─► stage
fact ────────────────┘                                                                      │
fact ◄──────────── SWAP_PARTITIONS_BETWEEN_TABLES(stage, pmin, pmax, fact) ◄───────────────┘
```

Base load:

```sql
CREATE TABLE s.customer_stage LIKE s.customer INCLUDING PROJECTIONS;
COPY s.customer_stage ( … ) FROM '…/base/*.json.zst' …;
SELECT MOVE_PARTITIONS_TO_TABLE('s.customer_stage', '190001', '299912', 's.customer');  -- publish
```

Every dose:

```sql
-- 1. load the dose
CREATE TABLE s.customer_delta LIKE s.customer INCLUDING PROJECTIONS;
COPY s.customer_delta ( … ) FROM '…/dose_01/*.json.zst' …;

-- 2. rebuild only the partitions the dose touches (here 2025-11 … 2026-01)
CREATE TABLE s.customer_stage LIKE s.customer INCLUDING PROJECTIONS;
--    (if untouched partitions lie inside the range: link them in, metadata only)
--    SELECT COPY_PARTITIONS_TO_TABLE('s.customer', '202511', '202601', 's.customer_stage');
--    SELECT DROP_PARTITIONS('s.customer_stage', '202512', '202512');
INSERT INTO s.customer_stage
SELECT f.* FROM s.customer f
 WHERE ((created_date >= '2025-11-01' AND created_date < ADD_MONTHS('2025-11-01'::DATE, 1)) OR …)
   AND NOT EXISTS (SELECT 1 FROM s.customer_delta d WHERE d.isn = f.isn)
UNION ALL
SELECT * FROM s.customer_delta WHERE op_code <> 'D';

-- 3. publish atomically
SELECT SWAP_PARTITIONS_BETWEEN_TABLES('s.customer_stage', '202511', '202601', 's.customer');
DROP TABLE s.customer_stage, s.customer_delta;
```

**Why it's fast and clean**

- Partition functions (`MOVE_`, `COPY_`, `SWAP_PARTITIONS…`) change only the catalog. They
  take milliseconds for any volume.
- Updates and deletes become one sequential `INSERT … SELECT` with a local merge anti-join.
  The fact table never gets delete vectors, so it never needs a purge.
- Readers see the old or the new partitions, never a half-applied dose.
- Deletes are real deletes: the rows are simply not rewritten.

**What it costs:** the rebuild rewrites every row of the touched partitions, not just the
changed ones. That's cheap when changes cluster in recent partitions, which is typical for CDC
on transactional data (`HOT_PCT` controls this in the demo). It becomes expensive when every
dose touches every partition.

**Rules**

- The partition key must be **immutable**, like a creation date or an ISN range. Otherwise an
  update can move a row to another partition and leave the old image behind.
- Stage and fact tables need identical columns, partitioning and projections. Use
  `LIKE … INCLUDING PROJECTIONS`.
- `SWAP_PARTITIONS_BETWEEN_TABLES` swaps the **whole key range**. Every partition inside
  `[pmin, pmax]` must therefore be present in the stage table, which is why untouched ones are
  linked in with `COPY_PARTITIONS_TO_TABLE`.

---

## Method 2 — optimized MERGE

**Idea:** load each dose into a delta table shaped exactly like the target, then apply it with
one `MERGE` that Vertica can run through its optimized plan.

```sql
CREATE TABLE s.customer_delta LIKE s.customer INCLUDING PROJECTIONS;
COPY s.customer_delta ( … ) FROM '…/dose_01/*.json.zst' …;

MERGE INTO s.customer t
USING s.customer_delta s
   ON t.isn = s.isn
 WHEN MATCHED THEN UPDATE SET
    isn = s.isn, op_code = s.op_code, change_ts = s.change_ts, … every column …
 WHEN NOT MATCHED THEN INSERT ( … every column … ) VALUES ( s.… every column … );
COMMIT;
```

**The optimization rules** (all three must hold):

1. The target's join column has a `PRIMARY KEY` or `UNIQUE` constraint. A declared,
   **not enforced** (`DISABLED`) key is enough and adds no load cost.
2. `UPDATE SET` and `INSERT` list **every** column of the target.
3. Both use the **same** source values.

How to prove it: the runner prints the `EXPLAIN` of each MERGE.

| Plan | What you see |
|---|---|
| optimized | `DML DELETE` + `DML INSERT`, `JOIN MERGEJOIN(inputs presorted) [Semi]` |
| not optimized | `DML MERGE` over a `JOIN … [RightOuter]` |

Leave one column out of `UPDATE SET` and the plan falls back to the generic MERGE.

**Why it's fast**

- The delta table is created `LIKE … INCLUDING PROJECTIONS`, so it has the same segmentation
  (`HASH(isn)`) and sort order (`isn`) as the target. The join is local and presorted: no
  network shuffle, no hash table, no sort.
- The cost is proportional to the dose, not to the table.

**What it costs**

- An update is a delete plus an insert. Every updated row leaves a **delete vector** in the
  target (the demo reports the count). The Tuple Mover's mergeout purges them over time, or
  you can run `PURGE_TABLE()`. Many delete vectors slow scans until they're purged.
- Duplicate keys in one dose make MERGE fail. Compact the dose to the last image per key
  first, for example with `LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)`.
- To keep one statement for inserts, updates and deletes, deletes are **soft**
  (`op_code = 'D'`) and a view (`customer_current`) hides them.

---

## Method 3 — insert-only journal + Top-K Live Aggregate Projection

**When an optimized MERGE is not possible, don't UPDATE or DELETE at all.** Every CDC
image, including deletes (as tombstones), is appended to the base (anchor) table with its
change timestamp. A **Top-K Live Aggregate Projection** keeps the newest version per key:

```sql
CREATE TABLE s.customer ( isn BIGINT NOT NULL, op_code CHAR(1), change_ts TIMESTAMP, … )
ORDER BY isn, change_ts
SEGMENTED BY HASH(isn) ALL NODES
PARTITION BY ((YEAR(created_date) * 100) + MONTH(created_date));

-- select list: PARTITION BY column, ORDER BY column, then the rest
CREATE PROJECTION s.customer_topk ( isn, change_ts, op_code, … ) AS
SELECT isn, change_ts, op_code, …
  FROM s.customer
 LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC);

CREATE VIEW s.customer_current AS
SELECT … FROM (SELECT isn, change_ts, … FROM s.customer
               LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)) last_version
 WHERE op_code <> 'D';
```

Applying a dose is just:

```sql
COPY s.customer ( … ) FROM '…/dose_01/*.json.zst' …;
```

The optimizer answers the view from the LAP. `EXPLAIN` shows
`STORAGE ACCESS for s.customer_topk (Rewritten TOPK)` and `TopK Optimized: K=1`.

A minimal walk-through is in [`examples/lap_topk_basic.sql`](../examples/lap_topk_basic.sql).

**Why it's useful**

- Applying a dose is as fast as loading new data. There's no delta table, no join, no delete
  vectors and no locks against readers.
- **Data versioning comes free.** The anchor table holds the full insert, update and delete
  history of every key, with times, for audit and debugging:
  ```sql
  SELECT isn, op_code, change_ts, batch_id FROM s.customer WHERE isn = 951 ORDER BY change_ts;
  ```
- It works even when MERGE's rules can't be met, for example with no usable key, duplicate
  images per key in one dose, or a source that sends partial rows.

**What it costs**

- The LAP is a second, pre-aggregated copy of the data, so storage is about 2x at first and
  the load does a little more work.
- The anchor table only grows. Hidden old versions still take space until you purge.
- The anchor table can't be UPDATEd or DELETEd while the LAP exists. That's by design.

**Purging the journal (every few months)**

```sql
-- Step 1: new table (+ its LAP, empty) filled with the latest live version of every key
CREATE TABLE s.customer__new ( … same DDL … );
CREATE PROJECTION s.customer__new_topk … LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC);
INSERT INTO s.customer__new SELECT * FROM s.customer_current;   -- read from the LAP
COMMIT;
-- Step 2: atomic swap of the names (instead of DROP + RENAME: no moment without a table)
ALTER TABLE s.customer, s.customer__new RENAME TO customer__old, customer;
-- Step 3: drop the old journal with its projections and LAP
DROP TABLE s.customer__old CASCADE;
-- Step 4: the LAP already exists: it was created empty in step 1 and filled by the INSERT
```

Run it with `./vload.sh purge --scale <S>`. Then carry on INSERTing into the base table as
usual. Vertica renames `<table>_super` and `<table>_topk` together with the table.

---

## Choosing a method

| | Swap partitions | Optimized MERGE | Journal + Top-K LAP |
|---|---|---|---|
| Best when | changes cluster in few (recent) partitions; readers need atomic, all-or-nothing publication | changes are spread across the table; dose is small compared to the table | MERGE can't be optimized, or history/audit of every version is needed |
| Apply cost grows with | size of the **touched partitions** | size of the **dose** | size of the **dose** (append only) |
| Delete vectors | none | one per updated or deleted row | none |
| Deletes | physical | soft (`op_code='D'`) | tombstone version |
| Storage | 1x | 1x (+ delete vectors until purged) | about 2x (anchor + LAP) + history |
| History of changes | no | no | yes, every version |
| Housekeeping | none | Tuple Mover / `PURGE_TABLE` | periodic journal purge |
| Readers during apply | never blocked; switch atomically at the swap | never blocked (snapshot reads); other writers wait for MERGE's X lock | never blocked |

Measured results are in [`RESULTS.md`](RESULTS.md).
