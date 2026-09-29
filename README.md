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
▶ STEP 3.2  Optimized MERGE delta → target                        (1B run, dose 1)
  WHAT  A single MERGE applies inserts, updates and soft deletes (op_code='D'). …
  WHY   This meets all three optimization rules (declared key, all columns, identical values), …
  SQL   MERGE INTO vload_merge.customer t USING vload_merge.customer_delta s ON t.isn = s.isn …
  EXPLAIN (access path):
        +-DML INSERT [Cost: 0, Rows: 0]
        +-DML DELETE [Cost: 0, Rows: 0]
        |  Target Projection: vload_merge.customer_super (DELETE ON CONTAINER)
        | +---> JOIN MERGEJOIN(inputs presorted) [Semi] …
✔ optimized MERGE: DELETE + INSERT with a presorted merge join (no 'DML MERGE' operator, no outer join)
  table                   rows    seconds       rows/s
  customer           1,000,000       3.57       280.0K
  account            1,000,000       1.82       547.9K
  …
✔ dose 1 MERGE: 10,000,000 rows in 3.57 s wall-clock  (2.80M rows/s; 23.26 s of work done in parallel → 6.5x)
```

## Quick start

On a Vertica node (the JSON files must be readable by the server), as a user that can
create schemas:

```bash
git clone https://github.com/mogomo/vertica-json-load-demo.git && cd vertica-json-load-demo
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

Measured on a single-node Vertica 26.2 (22 hardware threads, 61 GB RAM, one NVMe SSD), with
the 10 tables loading in parallel.
The full numbers and analysis are in [docs/RESULTS.md](docs/RESULTS.md).

<!-- results:begin -->
**1 billion rows** (10 tables × 100M), then 3 doses of 10M changes each (60% update, 10%
delete, 30% insert):

| Method | Base load (1B rows) | Base rows/s | Avg dose apply (10M changes) | Storage | Delete-vector rows |
|---|---:|---:|---:|---:|---:|
| Swap partitions | 17m 26s | 956K | 28.3 s | 52 GB | 0 |
| Optimized MERGE | 17m 31s | 951K | 13.5 s | 53 GB | 17.4M |
| Journal + Top-K LAP | 24m 18s ¹ | 686K | 13.1 s | 107 GB | 0 |
| LAP journal purge | 28m 23s | – | – | 105 GB | 0 |

All methods end with **identical data**: 100,800,000 live rows per table, same checksum.
Generating the 1.03B JSON records (57.7 GB zstd) took 17 minutes.
¹ loaded with `COPY_BATCH_FILES=2` to fit the disk.
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
- Disk for 1B rows: about 58 GB of JSON, plus about 52 GB per method in Vertica (about 100 GB
  for the LAP method), plus temp space while COPY sorts. Use `--drop-after` to keep only one
  method's schema at a time. On a disk under 250 GB, set `COPY_BATCH_FILES=2` and
  `PURGE_PARALLEL=2` for the LAP method (see `vload.env.example`).

## Disclaimer

This repository is a demo. It is not a product of, and is not endorsed or supported by,
Rocket Software, Vertica or any other company. The software is provided "as is", without
warranty of any kind, as the MIT license says. Please try it on your own systems and data
before you rely on it.

Vertica, Rocket Software and all other product and company names are trademarks or registered
trademarks of their respective owners.

## License

[MIT](LICENSE)
