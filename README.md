# vload — parallel JSON loading into Vertica, three ways

Loads **hierarchical JSON** (CDC records of 10 ADABAS files) into **10 Vertica tables in
parallel**, from a 10K sample up to **one billion rows**, then applies insert/update/delete
doses with the three techniques Vertica offers for this job and compares them:

| # | Method | In one line |
|---|---|---|
| 1 | **Staging table + partition COPY/SWAP** | load into a side table, rebuild only the touched partitions, publish with one atomic `SWAP_PARTITIONS_BETWEEN_TABLES` |
| 2 | **Optimized MERGE** | COPY each dose into a delta table shaped like the target and apply it with a `MERGE` that meets the optimization rules |
| 3 | **Insert-only journal + Top-K LAP** | never UPDATE or DELETE: append every version, and a `LIMIT 1 OVER (PARTITION BY key ORDER BY ts DESC)` Live Aggregate Projection serves the latest |

It's a single bash tool built on `vsql`, with the SQL inside. It needs no Python, Java or
extra drivers. Every step prints **what** runs, **why** it's the right technique, the **SQL**,
and **how long** it took per table and in total. It's built to be presented and recorded.

```
▶ STEP 4.2  Optimized MERGE delta → target
  WHAT  A single MERGE applies inserts, updates and soft deletes …
  WHY   This meets all three optimization rules (declared key, all columns, identical values) …
  SQL   MERGE INTO vload_merge.customer t USING vload_merge.customer_delta s ON t.isn = s.isn …
  EXPLAIN (access path):
        +-DML DELETE …  JOIN MERGEJOIN(inputs presorted) [Semi] …
✔ optimized MERGE: DELETE + INSERT with a presorted merge join (no 'DML MERGE' operator, no outer join)
  table                   rows    seconds       rows/s
  customer           1,000,000       0.93        1.08M
  …
✔ dose 1 MERGE: 10,000,000 rows in 1.02 s wall-clock (9.8M rows/s; 8.9 s of work done in parallel → 8.7x)
```

## Quick start

On a Vertica node (the JSON files must be readable by the server), as a user that can
create schemas:

```bash
git clone <this repo> && cd vertica_vload_demo
cp vload.env.example vload.env        # optional: connection, tuning, dose mix
./vload.sh check                      # vsql, database, CPUs, disk
./vload.sh demo --scale 10K           # generate + 3 methods + purge + validate + report  (~15 s)
```

Then scale up:

```bash
./vload.sh demo --scale 100M
./vload.sh demo --scale 1B --drop-after     # ~57 GB of JSON; one method's schema on disk at a time
```

Presentation mode (waits for Enter before every step):

```bash
./vload.sh run --scale 10K --method merge --pause
```

## What happens

```
conf/tables.def ──► generate ──► data/<scale>/<table>/{base,dose_01..03}/part_NNNN.json.zst
   (10 ADABAS files:           (parallel awk → zstd; ISN-ordered created dates; doses = U/D/I
    groups, MU, PE)             after-images hitting the newest HOT_PCT % of the ISNs)
                                    │
             ┌──────────────────────┼──────────────────────┐   10 tables in parallel,
             ▼                      ▼                      ▼   every COPY parses many files
     vload_swap.*            vload_merge.*             vload_lap.*
   stage → MOVE/SWAP       delta → optimized MERGE   COPY into journal, Top-K LAP
             │                      │                      │
             └──────── <table>_current views ──────────────┘
                                    │
                      validate: rows + checksum per table must be identical
                      report:   timings, rows/s, storage, delete vectors
```

- **Base load:** one full unload per table (`--scale` rows in total). Every COPY uses
  `FJSONPARSER(flatten_arrays=true)` with `FILLER` columns that map the nested JSON onto
  relational columns in the same pass. See [ADABAS mapping](docs/ADABAS_MAPPING.md).
- **Doses:** `DOSES` × `DOSE_PCT`% of the rows per table, mixed `60:10:30`
  (update:delete:insert). Updates and deletes hit the newest `HOT_PCT`% of the records; inserts
  get new ISNs and land in new partitions.
- **Validation:** after each method, the live rows and a checksum of `(isn, change_ts)` are
  recorded per table. `validate` requires every method to match.

## Commands

| Command | Does |
|---|---|
| `check` | vsql connectivity, Vertica version, nodes, CPUs, disk, FJSONPARSER |
| `generate --scale S` | writes the data set (base + doses) and `manifest.env` |
| `run --scale S --method swap,merge,lap` | runs the methods: schema, base load, every dose, fingerprint |
| `purge --scale S` | compacts the LAP journals, keeping only the latest live version |
| `validate --scale S` | compares the fingerprints of all methods |
| `report --scale S` | comparison table (also written to `reports/summary_<S>.md`) |
| `demo --scale S` | all of the above |
| `sql --table T` | prints every statement used for table `T` in every method |
| `clean [--data]` | drops the demo schemas (and the generated data) |

Options: `--pause`, `--drop-after`, `--dry-run`, `--no-color`. Every SQL statement a run
executes, with its output, is kept in `logs/<scale>/<run>/`.

## Results

Measured on a single-node Vertica 26.2 (22 threads, 64 GB RAM, NVMe), 10 tables in parallel.
The full numbers and analysis are in [docs/RESULTS.md](docs/RESULTS.md).

<!-- results:begin -->
<!-- results:end -->

## Which method, when?

| | Swap partitions | Optimized MERGE | Journal + Top-K LAP |
|---|---|---|---|
| Best when | changes cluster in few partitions; atomic publication | changes are spread out; dose ≪ table | MERGE can't be optimized; history or audit is needed |
| Apply cost grows with | touched partitions | dose size | dose size (append only) |
| Delete vectors | none | one per updated or deleted row | none |
| Storage | 1x | 1x + delete vectors | about 2x + history, until purged |
| Version history | – | – | every version, with time |

The reasoning, SQL and pitfalls of each method are in [docs/METHODS.md](docs/METHODS.md).

## Repository layout

```
vload.sh                 CLI entry point
vload.env.example        configuration template (copy to vload.env)
conf/tables.def          the 10 ADABAS files: JSON paths, columns, types, generators
lib/common.sh            config, step banners, vsql wrapper, parallel runner, live progress
lib/schema.sh            DDL / COPY mapping / MERGE / Top-K SQL generated from tables.def
lib/generate.sh          parallel data set generation (awk → zstd)
lib/gen_json.awk         hierarchical JSON record generator (portable awk)
lib/method_swap.sh       method 1
lib/method_merge.sh      method 2
lib/method_lap.sh        method 3 + journal purge
examples/                standalone SQL (Top-K LAP basics)
docs/                    methods, ADABAS mapping, results, video script
```

## Requirements

- Vertica 12.x or newer (tested on 26.2) with the flex table package (`FJSONPARSER`)
- bash ≥ 4.3, awk (mawk is fastest), zstd (or `COMPRESSION=gzip`)
- For 1B rows: about 60 GB for the JSON, plus about 55 GB per method in Vertica (about 110 GB
  for the LAP method). Use `--drop-after` to keep only one method's schema at a time.

## License

[MIT](LICENSE)
