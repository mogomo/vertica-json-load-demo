# From ADABAS records to Vertica columns

## ADABAS in one paragraph

An ADABAS database is made of numbered **files**. Every record in a file is identified by its
**ISN** (Internal Sequence Number), which never changes for the life of the record. Fields can
be plain elementary fields, **groups** (a named set of fields), **MU** fields (multiple-value
fields: one field holding up to N values) and **PE** groups (periodic groups: a group that
repeats up to N times, like an array of structs). A DDM gives the fields long names. CDC tools
(ADABAS Event Replicator, or log-based replication products) publish every change as a record
carrying the ISN, the operation and the after-image (or the before-image for a delete).

## The JSON change records

The demo uses one ADABAS file, **TXN** (file 16, account transactions), as the fact table.
`generate.sh` writes the change records as JSON Lines (one change per line) into
`demo/changes/`, with a CDC header and the record:

```json
{"hdr":{"isn":950000001,"op":"U","ts":"2026-01-01 00:00:00.000000","batch":1},
 "rec":{"created":"2025-11-07","txn_ref":"TX0000000950000001","acct_isn":31613501,
        "card_isn":null,"type":"FEE","amount":4414.00,"currency":"CHF","booking_date":"2025-11-08",
        "merchant":{"mcc":7747,"name":"Smith","city":"PARIS","country":"ES"},   <- group (optional)
        "tag":["FOREIGN"],                                                       <- MU field (0-2 values)
        "reversal":false}}
```

| Header key | Meaning |
|---|---|
| `hdr.isn` | ADABAS ISN, the key of the record |
| `hdr.op` | `I` insert, `U` update (after-image), `D` delete |
| `hdr.ts` | commit time of the change; orders the versions of an ISN |
| `hdr.batch` | 0 = the rows of the base table, 1 = the change files |
| `rec.created` | creation date of the record, which never changes. It's the partition key. |

The columns of the table, with their JSON keys, are listed once in `TXN_COLUMNS` at the top
of [`lib/sql.sh`](../lib/sql.sh). The DDL, the COPY mapping and the MERGE are all built from
that list.

| JSON key | Column | Type |
|---|---|---|
| `hdr.isn` | `isn` | `BIGINT NOT NULL` (primary key, declared) |
| `hdr.op`, `hdr.ts`, `hdr.batch` | `op_code`, `change_ts`, `batch_id` | `CHAR(1)`, `TIMESTAMP`, `INT` |
| `rec.created` | `created_date` | `DATE` (partition key) |
| `rec.txn_ref`, `rec.acct_isn`, `rec.card_isn`, `rec.type` | `txn_ref`, `acct_isn`, `card_isn`, `txn_type` | |
| `rec.amount`, `rec.currency`, `rec.booking_date` | `amount`, `currency`, `booking_date` | |
| `rec.merchant.mcc` … `rec.merchant.country` | `mcc`, `merchant_name`, `merchant_city`, `merchant_country` | group MERCHANT |
| `rec.tag.0`, `rec.tag.1` | `tag_1`, `tag_2` | MU field TAG |
| `rec.reversal` | `is_reversal` | `BOOLEAN` |

The base table itself is not loaded from JSON: its billion rows are generated inside Vertica
with SQL, as the same columns. Only the changes travel as JSON, and parsing them is part of
the measured time.

## Flattening in the COPY

The hierarchy is flattened **during** the COPY, with no landing table:

1. `FJSONPARSER(flatten_arrays = true)` turns every leaf into a key built from its path:
   `rec.merchant.city`, `rec.tag.0`, `rec.tag.1`. Maps are flattened by default;
   `flatten_arrays` also flattens the MU/PE arrays.
2. A `FILLER` column, named exactly like the key, receives each value.
3. The real column is computed from the filler: `tag_2 AS "rec.tag.1"`.

```sql
COPY vload.txn_upsert (
    "rec.merchant.city"  FILLER VARCHAR(30),  merchant_city AS "rec.merchant.city",
    "rec.tag.0"          FILLER VARCHAR(12),  tag_1         AS "rec.tag.0",
    "rec.tag.1"          FILLER VARCHAR(12),  tag_2         AS "rec.tag.1",
    …)
FROM '/…/demo/changes/*.json' PARSER FJSONPARSER(flatten_arrays = true);
```

Missing keys and JSON `null` become SQL NULL, so a missing group (no `merchant`) or an MU field
with fewer values than columns loads fine. Values are cast to the column type while loading; a value that can't be
cast sends the record to the rejects table.

This is the **denormalized** mapping: a fixed number of MU/PE occurrences become numbered
columns. It keeps one row per ISN, which is what MERGE keys and Top-K projections need.

## Alternatives considered

| Alternative | Why not used here |
|---|---|
| `JSONPARSER` into `ROW` / `ARRAY` columns | Loads the hierarchy as native complex types, but the columns can't be used as sort or segmentation keys. The flattening would then need a second `INSERT … SELECT`, and complex-type `FILLER` columns aren't supported (ERROR 9848). |
| Flex table (`__raw__` VMap) + `MAPLOOKUP` views | Very flexible when the schema is unknown, but every query pays for map lookups. Better used as a landing zone than as the fact table. |
| Normalizing PE groups into child tables | The right model when occurrences are unbounded, such as 0..191 PE occurrences. It doubles the number of load streams and needs keys on the children. With the small fixed occurrence counts here, flat columns are simpler and faster. |

Unmatched keys: the parser also reports the parent objects (`hdr`, `rec`) as keys with no
matching column, which raises WARNING 10596 once per COPY. It's harmless: every leaf value is
mapped. The scripts keep it in the log files and leave it off the screen.
