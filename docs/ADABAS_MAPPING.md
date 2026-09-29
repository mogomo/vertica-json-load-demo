# From ADABAS records to Vertica columns

## ADABAS in one paragraph

An ADABAS database is made of numbered **files**. Every record in a file is identified by its
**ISN** (Internal Sequence Number), which never changes for the life of the record. Fields can
be plain elementary fields, **groups** (a named set of fields), **MU** fields (multiple-value
fields: one field holding up to N values) and **PE** groups (periodic groups: a group that
repeats up to N times, like an array of structs). A DDM gives the fields long names. CDC tools
(ADABAS Event Replicator, or log-based replication products) publish every change as a record
carrying the ISN, the operation and the after-image (or the before-image for a delete).

## The JSON produced by the generator

One line per change (JSON Lines), with a CDC header and the record:

```json
{"hdr":{"isn":951,"op":"U","ts":"2026-01-01 00:00:00.000001","batch":1},
 "rec":{"created":"2025-11-07",
        "cust_no":"CU0000000951",
        "name":{"first":"Tamar","last":"Jackson"},                   <- group
        "phone":["+36-765-3178602",null,null],                      <- MU field
        "address":[{"type":"H","street":"191 Brown RD","city":"PARIS","country":"FR"},
                   {"type":"W","street":"114 Levi RD","city":"ROME","country":"IT"}]}}   <- PE group
```

| Header key | Meaning |
|---|---|
| `hdr.isn` | ADABAS ISN, the business key of the record |
| `hdr.op` | `I` insert, `U` update (after-image), `D` delete |
| `hdr.ts` | commit time of the change; orders the versions of an ISN |
| `hdr.batch` | 0 = initial unload (base), n = CDC dose n |
| `rec.created` | creation date of the record, which never changes. It's the partition key. |

The 10 files are defined in [`conf/tables.def`](../conf/tables.def). They're banking and
insurance style files, plus the two classic ADABAS demo files, EMPLOYEES and VEHICLES:

| # | File | Table | Hierarchy |
|---|---|---|---|
| 11 | Customer | `customer` | group NAME, MU PHONE (3), PE ADDRESS (2) |
| 12 | Account | `account` | group BALANCE, MU SIGNATORY (3) |
| 13 | Card | `card` | group LIMITS, PE TOKEN (2) |
| 14 | Loan | `loan` | group TERMS, PE COLLATERAL (2) |
| 15 | Payment | `payment` | groups DEBTOR / CREDITOR, MU REMITTANCE (2) |
| 16 | Transaction | `txn` | group MERCHANT |
| 17 | Policy | `policy` | group PREMIUM, PE COVERAGE (2), MU BENEFICIARY (2) |
| 18 | Claim | `claim` | group INCIDENT, PE PAYOUT (2) |
| 19 | Employees | `employees` | groups FULL-NAME / FULL-ADDRESS, MU LANG (2), PE INCOME (2) |
| 20 | Vehicles | `vehicles` | group MAKE-MODEL, MU SERVICE-DATE (3) |

To add a field, a table or a PE occurrence, edit `tables.def`. The generator, DDL, COPY
mapping and MERGE statements all follow from it. A line looks like this:

```
F|customer|address.1.city|addr2_city|VARCHAR(30)|city?50
   table   JSON path      column     type        generator (50 % NULL)
```

## Flattening in the COPY

The hierarchy is flattened **during** the COPY, with no landing table:

1. `FJSONPARSER(flatten_arrays = true)` turns every leaf into a key built from its path:
   `rec.name.first`, `rec.phone.0`, `rec.address.1.city`. Maps are flattened by default;
   `flatten_arrays` also flattens the MU/PE arrays.
2. A `FILLER` column, named exactly like the key, receives each value.
3. The real column is computed from the filler: `addr2_city AS "rec.address.1.city"`.

```sql
COPY s.customer (
    "rec.name.first"      FILLER VARCHAR(30),  first_name AS "rec.name.first",
    "rec.phone.0"         FILLER VARCHAR(20),  phone_1    AS "rec.phone.0",
    "rec.address.1.city"  FILLER VARCHAR(30),  addr2_city AS "rec.address.1.city",
    …)
FROM '…/*.json.zst' ZSTD PARSER FJSONPARSER(flatten_arrays = true);
```

Missing keys and JSON `null` become SQL NULL, so a PE group with fewer occurrences than
columns loads fine. Values are cast to the column type while loading; a value that can't be
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
mapped. The runner keeps it in the log files and leaves it off the screen.
