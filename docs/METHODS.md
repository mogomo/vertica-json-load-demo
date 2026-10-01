# The three methods

All three methods load the **same JSON files** into the **same table** and must end with
**identical current data**. `phase1.sh` checks this after every method with a row count and a
checksum. The methods differ in how the changes (updates of existing rows and inserts of new
ones) reach the 1-billion-row fact table.

- [Common ground: the COPY](#common-ground-the-copy)
- [Method 1: insert-only upsert (journal + Top-K LAP)](#method-1--insert-only-upsert-journal--top-k-lap)
- [Method 2: staging table + partition COPY/SWAP](#method-2--staging-table--partition-copyswap)
- [Method 3: optimized MERGE](#method-3--optimized-merge)
- [One file, many tables: parse once, fan out](#one-file-many-tables-parse-once-fan-out-phases-2-and-3)
- [Repeatable runs: COPY_TABLE](#repeatable-runs-copy_table)
- [Choosing a method](#choosing-a-method)

---

## Common ground: the COPY

Every method starts with the same statement: a bulk `COPY` of the JSON files. It is part of
the measured time, because parsing JSON is a real part of the job.

```sql
COPY vload.txn_merge_delta (
    "hdr.isn"              FILLER BIGINT,        isn              AS "hdr.isn",
    "hdr.op"               FILLER CHAR(1),       op_code          AS "hdr.op",
    "hdr.ts"               FILLER TIMESTAMP,     change_ts        AS "hdr.ts",
    …
    "rec.merchant.city"    FILLER VARCHAR(30),   merchant_city    AS "rec.merchant.city",
    "rec.tag.0"            FILLER VARCHAR(12),   tag_1            AS "rec.tag.0",
    "rec.tag.1"            FILLER VARCHAR(12),   tag_2            AS "rec.tag.1",
    "rec.reversal"         FILLER BOOLEAN,       is_reversal      AS "rec.reversal"
)
FROM '/…/demo/changes/*.json' ON ANY NODE
PARSER FJSONPARSER(flatten_arrays = true)
STREAM NAME 'vload_…_merge'
REJECTED DATA AS TABLE vload.txn_rejects_merge;
```

| Choice | Reason |
|---|---|
| `COPY`, not `INSERT` | COPY writes sorted, encoded, compressed ROS containers straight to disk in one transaction, with no per-row overhead. |
| Many files | Each file is parsed by its own thread, so one COPY uses many cores (`JSON_FILES`, default = number of CPUs, at most 24). |
| `FJSONPARSER(flatten_arrays=true)` + `FILLER` | Maps the nested JSON (the MERCHANT group, the TAG multiple-value field) onto plain columns in the same pass: no landing table, no second `INSERT … SELECT`. See [ADABAS_MAPPING.md](ADABAS_MAPPING.md). |
| `ON ANY NODE` | On a cluster with shared storage, every node takes part in parsing. |
| `REJECTED DATA AS TABLE` | Bad records go to a table you can query; `phase1.sh` stops if there are any. |
| `STREAM NAME` | The load shows in `v_monitor.load_streams` while it runs. |

The fact table is segmented by `HASH(isn)`, sorted by `isn` and partitioned by month of the
immutable `created_date`, so an update never moves a row to another partition.

---

## Method 1 — insert-only upsert (journal + Top-K LAP)

**Don't UPDATE at all.** Every change is appended to the journal with its change timestamp.
A **Top-K Live Aggregate Projection** keeps the newest version of every key, and readers use
a view on it.

```sql
CREATE TABLE vload.txn_jrn_base ( isn BIGINT NOT NULL, op_code CHAR(1) NOT NULL, change_ts TIMESTAMP NOT NULL, … )
ORDER BY isn, change_ts
SEGMENTED BY HASH(isn) ALL NODES
PARTITION BY ((YEAR(created_date) * 100) + MONTH(created_date));

-- select list: PARTITION BY column, ORDER BY column, then the rest
CREATE PROJECTION vload.txn_jrn_base_topk (isn, change_ts, op_code, …) AS
SELECT isn, change_ts, op_code, …
  FROM vload.txn_jrn_base
 LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC);

CREATE VIEW vload.txn_upsert_current AS
SELECT … FROM (SELECT isn, change_ts, … FROM vload.txn_upsert
               LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)) last_version
 WHERE op_code <> 'D';
```

Applying the changes is one statement:

```sql
COPY vload.txn_upsert ( … ) FROM '/…/demo/changes/*.json' … ;
```

The optimizer answers the view from the LAP. `EXPLAIN` shows
`STORAGE ACCESS for vload.txn_upsert_topk (Rewritten TOPK)` and `TopK Optimized: K=1`.
A predicate on the key (`WHERE isn = …` or `isn >= …`) is pushed into the projection scan.

**Why it's fast:** an upsert costs as much as a load. There's no delta table, no join, no
delete vectors and no locks against readers.

**Data versioning comes free.** The journal holds every version of every key, with times:

```sql
SELECT isn, op_code, change_ts, batch_id, amount FROM vload.txn_upsert WHERE isn = 950000001 ORDER BY change_ts;
```

**What it costs**

- The LAP is a second, pre-aggregated copy of the data: about twice the storage, and the
  COPY does a little more work to maintain it.
- Reading **all** current rows runs the Top-K operator over the whole LAP: in this demo about
  3 M rows/s on one node, so a full scan of 1 billion rows takes minutes, while point and
  range queries on the key stay fast.
- The journal only grows. Compact it from time to time (every few months):

```sql
CREATE TABLE vload.txn__new ( … same DDL … );
CREATE PROJECTION vload.txn__new_topk … LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC);
INSERT INTO vload.txn__new SELECT * FROM vload.txn_upsert_current;   -- read from the LAP
COMMIT;
ALTER TABLE vload.txn_upsert, vload.txn__new RENAME TO txn__old, txn_upsert;   -- atomic
DROP TABLE vload.txn__old CASCADE;
```

A minimal walk-through is in [`examples/lap_topk_basic.sql`](../examples/lap_topk_basic.sql).

---

## Method 2 — staging table + partition COPY/SWAP

**Never modify the fact table in place.** Build the new version of the affected partitions
next to it, then exchange partitions in one catalog operation.

```
changes ─COPY─► delta ──┐
                        ├─ INSERT…SELECT (unchanged rows of touched partitions + new images) ─► stage
fact ───────────────────┘                                                                      │
fact ◄─────────────── SWAP_PARTITIONS_BETWEEN_TABLES(stage, pmin, pmax, fact) ◄────────────────┘
```

```sql
-- 1. load the changes
CREATE TABLE vload.txn_swap_delta LIKE vload.txn_swap INCLUDING PROJECTIONS;
COPY vload.txn_swap_delta ( … ) FROM '/…/demo/changes/*.json' … ;

-- 2. which partitions are touched? (here 202511, 202512 and the new 202601)
SELECT DISTINCT ((YEAR(created_date) * 100) + MONTH(created_date)) FROM vload.txn_swap_delta;

-- 3. rebuild only those partitions, in 11 parallel sessions (one ISN slice each)
CREATE TABLE vload.txn_swap_stage LIKE vload.txn_swap INCLUDING PROJECTIONS;
SELECT MIN(isn), MAX(isn) FROM ( … rows of the touched partitions + the delta … ) x;
INSERT /*+DIRECT*/ INTO vload.txn_swap_stage                -- slice 1 of 11
SELECT f.* FROM vload.txn_swap f
 WHERE ((f.created_date >= '2025-11-01' AND f.created_date < ADD_MONTHS('2025-11-01'::DATE, 1)) OR …)
   AND f.isn BETWEEN 944343067 AND 949448241
   AND NOT EXISTS (SELECT 1 FROM vload.txn_swap_delta d WHERE d.isn = f.isn)
UNION ALL
SELECT * FROM vload.txn_swap_delta WHERE op_code <> 'D' AND isn BETWEEN 944343067 AND 949448241;
COMMIT;

-- 4. publish atomically
SELECT SWAP_PARTITIONS_BETWEEN_TABLES('vload.txn_swap_stage', '202511', '202601', 'vload.txn_swap');
DROP TABLE vload.txn_swap_stage, vload.txn_swap_delta;
```

**Why it's clean:** partition functions change only the catalog, the fact table never gets
delete vectors, and readers see the old or the new partitions, never a half-applied change.

**What it costs:** the rebuild rewrites **every row of the touched partitions**, not just the
changed ones. In this demo the updates hit the newest 5 % of the table (`HOT_PCT`), which
spans two monthly partitions of ~28 M rows each, so ~56 M rows are rewritten to apply 1 M
changes. Several sessions can insert into the same table at once, so `phase1.sh` splits the
rebuild into ISN slices (`REBUILD_SESSIONS`, default = half the CPUs): one session needed
44 s for the rebuild, 11 sessions need 13 s. The method shines when changes are concentrated
and partitions are small, and it loses when changes are spread over the whole table.

**Rules**

- The partition key must be **immutable** (a creation date, an ISN range). Otherwise an
  update could move a row to another partition and leave the old image behind.
- Stage and fact tables need identical columns, partitioning and projections:
  `LIKE … INCLUDING PROJECTIONS`.
- `SWAP_PARTITIONS_BETWEEN_TABLES` swaps the **whole key range**. If untouched partitions lie
  inside `[pmin, pmax]`, link them into the stage table first with
  `COPY_PARTITIONS_TO_TABLE` (metadata only); `phase1.sh` does this automatically.

---

## Method 3 — optimized MERGE

**Load the changes into a delta table shaped like the target, then apply them with one MERGE
that Vertica can run through its optimized plan.**

```sql
CREATE TABLE vload.txn_merge_delta LIKE vload.txn_merge INCLUDING PROJECTIONS;
COPY vload.txn_merge_delta ( … ) FROM '/…/demo/changes/*.json' … ;

MERGE INTO vload.txn_merge t
USING vload.txn_merge_delta s
   ON t.isn = s.isn
 WHEN MATCHED THEN UPDATE SET
    isn = s.isn, op_code = s.op_code, change_ts = s.change_ts, … every column …
 WHEN NOT MATCHED THEN INSERT ( … every column … ) VALUES ( s.… every column … );
COMMIT;
```

**The optimization rules** (all three must hold):

1. The target's join column has a `PRIMARY KEY` or `UNIQUE` constraint. A declared,
   **not enforced** (`DISABLED`) key is enough and costs nothing at load time.
2. `UPDATE SET` and `INSERT` list **every** column of the target.
3. Both use the **same** source values.

`phase1.sh` prints the `EXPLAIN` of the MERGE on the first run:

| Plan | What you see |
|---|---|
| optimized | `DML DELETE` + `DML INSERT`, `JOIN MERGEJOIN(inputs presorted) [Semi]` |
| not optimized | `DML MERGE` over a `JOIN … [RightOuter]` |

Leave one column out of `UPDATE SET` and the plan falls back to the generic MERGE.

**Why it's fast:** the delta has the target's segmentation and sort order, so the join is a
local, presorted merge join. The cost follows the number of changes, not the table size.

**What it costs**

- An update is a delete plus an insert: every updated row leaves a **delete vector** in the
  target (`phase1.sh` reports the count). The Tuple Mover purges them over time, or run
  `PURGE_TABLE()`.
- Duplicate keys in one batch make MERGE fail: compact the batch to the last image per key
  first, for example with `LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)`.
- Deletes, if the change files carry any (`CHANGE_MIX`), are kept as `op_code = 'D'` rows
  that the current view hides, so one MERGE handles everything.

---

## One file, many tables: parse once, fan out (phases 2 and 3)

When every JSON file mixes the records of many tables, the expensive part, parsing, must
happen **once**:

| Approach | Parses | Verdict |
|---|---|---|
| one COPY per table over all files, unwanted records rejected | every record 10 times, plus ~9M rejected rows written per COPY | avoid |
| one COPY per table with a `CASE` in the column list | still 10 parses, and COPY can only append: no update | avoid |
| **one COPY into a staging table, then one MERGE per table** | **once** | used here |

COPY only appends to one table, so it parses into a staging table, and the upserts are
`MERGE INTO` statements that read their rows from it. A MERGE source can be a query: Vertica
still uses the optimized plan as long as the three rules hold.

**Phase 2: flat records** (`stg_flat`, columns of all tables side by side, partitioned by file):

```sql
COPY vload.stg_flat (
    "hdr.file"            FILLER VARCHAR(12),  file                 AS "hdr.file",
    "hdr.isn"             FILLER BIGINT,       isn                  AS "hdr.isn",
    …
    "customer.name.first" FILLER VARCHAR(30),  customer__first_name AS "customer.name.first",
    …)
FROM '/…/demo/phase2/*.json' PARSER FJSONPARSER(flatten_arrays = true);

MERGE INTO vload.customer t
USING (SELECT isn, op_code, change_ts, batch_id, customer__created_date AS created_date,
              customer__cust_no AS cust_no, customer__first_name AS first_name, …
         FROM vload.stg_flat
        WHERE file = 'customer') s          -- partition pruning: reads one partition
   ON t.isn = s.isn
 WHEN MATCHED THEN UPDATE SET … every column …
 WHEN NOT MATCHED THEN INSERT … every column …;
```

**Phase 3: nested documents** (`stg_doc`, one row per transaction, 2 slots of columns per table):

```sql
COPY vload.stg_doc FROM '/…/demo/phase3/*.json' PARSER FJSONPARSER(flatten_arrays = true);
-- no column list: the columns are named like the keys, e.g. "customer.1.rec.name.first"

MERGE INTO vload.customer t
USING (SELECT "customer.0.hdr.isn" AS isn, …, "customer.0.rec.name.first" AS first_name, …
         FROM vload.stg_doc WHERE "customer.0.hdr.isn" IS NOT NULL
       UNION ALL
       SELECT "customer.1.hdr.isn" AS isn, …, "customer.1.rec.name.first" AS first_name, …
         FROM vload.stg_doc WHERE "customer.1.hdr.isn" IS NOT NULL) s
   ON t.isn = s.isn
 WHEN MATCHED THEN UPDATE SET …
 WHEN NOT MATCHED THEN INSERT …;
```

The 10 MERGEs are independent and run in parallel sessions (`--parallel`). For arrays
without a known maximum length, keep them as VMaps (`flatten_arrays=false,
flatten_maps=false`, one `LONG VARBINARY` column per table) and explode them with
`MAPITEMS(...) OVER (PARTITION BEST)` + `MAPLOOKUP`. It works, but it is much slower, and the
exploded rows must go into a delta table first, because a MERGE that reads `MAPITEMS`
directly does not get the optimized plan. See [RESULTS.md](RESULTS.md#phases-2-and-3-one-parse-10-parallel-merges).

---

## Repeatable runs: COPY_TABLE

`phase1.sh` can run any number of times and every run starts from the same data. Before each
method it resets the method's table from the pristine copy made by `generate.sh`:

```sql
DROP TABLE IF EXISTS vload.txn_merge, vload.txn_merge_delta, vload.txn_rejects_merge CASCADE;
SELECT COPY_TABLE('vload.txn_base', 'vload.txn_merge');
```

`COPY_TABLE` copies the definition, the projections (including a Top-K LAP), the constraints
and the statistics, and **shares** the storage containers of the source instead of copying
data. It takes milliseconds for a billion rows and no disk space until the copy is changed.
This reset is not part of the measured time.

---

## Choosing a method

| | Upsert (journal + Top-K LAP) | Swap partitions | Optimized MERGE |
|---|---|---|---|
| Best when | the change rate is high, MERGE can't be optimized, or history is needed | changes cluster in few, small partitions; readers need all-or-nothing publication | changes are spread out and small compared to the table |
| Apply cost grows with | number of changes (append only) | size of the **touched partitions** | number of changes |
| Delete vectors | none | none | one per updated row |
| Storage | about 2x (journal + LAP) + history | 1x | 1x (+ delete vectors until purged) |
| Reading all current rows | Top-K at query time (slower) | plain scan | plain scan (+ delete vectors) |
| History of changes | yes, every version | no | no |
| Housekeeping | periodic journal compaction | none | Tuple Mover / `PURGE_TABLE` |

Measured results are in [`RESULTS.md`](RESULTS.md).
