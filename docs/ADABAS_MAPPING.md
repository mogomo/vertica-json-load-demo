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

## The 10 files of phases 2 and 3

Phases 2 and 3 (`generate_multi.sh`, `phase2.sh`, `phase3.sh`) add nine banking, insurance and
classic ADABAS demo files next to TXN. They're defined in [`conf/tables.def`](../conf/tables.def), which
drives the JSON generator, the DDL, the SQL row generator, the COPY mapping and the MERGE:

| # | File | Table | Rows | Hierarchy |
|---|---|---|---:|---|
| 11 | Customer | `customer` | 60M | group NAME, MU PHONE (3), PE ADDRESS (2) |
| 12 | Account | `account` | 60M | group BALANCE, MU SIGNATORY (3) |
| 13 | Card | `card` | 60M | group LIMITS, PE TOKEN (2) |
| 14 | Loan | `loan` | 60M | group TERMS, PE COLLATERAL (2) |
| 15 | Payment | `payment` | 60M | groups DEBTOR / CREDITOR, MU REMITTANCE (2) |
| 16 | Transaction | `txn` | 1B | group MERCHANT, MU TAG (2): the fact table of phase 1 |
| 17 | Policy | `policy` | 60M | group PREMIUM, PE COVERAGE (2), MU BENEFICIARY (2) |
| 18 | Claim | `claim` | 60M | group INCIDENT, PE PAYOUT (2) |
| 19 | Employees | `employees` | 60M | groups FULL-NAME / FULL-ADDRESS, MU LANG (2), PE INCOME (2) |
| 20 | Vehicles | `vehicles` | 60M | group MAKE-MODEL, MU SERVICE-DATE (3) |

A customer change record nests three levels deep (`rec` → `address` array → object):

```json
{"hdr":{"isn":57007920,"op":"U","ts":"2026-01-01 00:00:00.000000","batch":1},
 "rec":{"created":"2025-11-07","cust_no":"CU0057007920",
        "name":{"first":"Thomas","last":"Wilson"},                                  <- group
        "birth_date":"1967-03-24","segment":"P","risk_score":20,
        "email":"sarah.57007920@mail.example",
        "phone":["+95-057-8793394",null,null],                                      <- MU field
        "address":[{"type":"W","street":"5 Ivanov ST","city":"BOSTON","country":"AT"},
                   {"type":"H","street":"13 Schneider BLVD","city":"VIENNA","country":"IE"}]}}   <- PE group
```

A line of `tables.def` looks like this:

```
F|customer|address.1.city|addr2_city|VARCHAR(30)|city?50
   table   JSON path      column     type        generator (50 % NULL)
```

## The two shapes of phases 2 and 3

`generate_multi.sh` writes the same 10,000,000 records (1,000,000 per file) twice.

**Phase 2: one record per line, the files mixed.** The header names the file; the record
sits under a key with the file's name, so every field has a unique flattened key:

```json
{"hdr":{"file":"customer","isn":57007920,"op":"U","ts":"…","batch":1},"customer":{"created":"2025-11-07","cust_no":"CU0057007920","name":{…},"phone":[…],"address":[…]}}
{"hdr":{"file":"account","isn":57007920,"op":"U","ts":"…","batch":1},"account":{"created":"2025-11-07","acct_no":"AC…","balance":{…},"signatory":[…]}}
```

The COPY maps `"customer.address.1.city"` onto the staging column `customer__addr2_city` with a
FILLER column, and `"hdr.file"` onto `file`, the partition key of `stg_flat`.

**Phase 3: one ADABAS transaction (ET) per line.** A document carries an array of changed
records for each file: 0, 1 or 2 records, about 10 per document.

```json
{"et_id":1,"et_ts":"2026-01-01 00:00:00",
 "txn":[],
 "customer":[{"hdr":{"isn":57007920,"op":"U",…},"rec":{"created":"2025-11-07","name":{…},"address":[{…},{…}]}}],
 "account":[{"hdr":{…},"rec":{…}}],
 "card":[{"hdr":{…},"rec":{…}},{"hdr":{…},"rec":{…}}],
 …}
```

Up to five levels deep: document → file array → record → periodic group → field. With
`FJSONPARSER(flatten_arrays=true)` the key of a field carries every level:
`"card.1.rec.token.0.wallet"` is the wallet of the 1st token of the 2nd card record of the
transaction. `stg_doc` has 2 slots of columns per file named exactly like those keys, so the
COPY needs no column list, and a file's records are the `UNION ALL` of its slots.

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
