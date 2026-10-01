# shellcheck shell=bash
# =============================================================================
#  sql.sh — every SQL statement of the demo
# =============================================================================
#  Tables (schema $SCHEMA, default "vload"):
#
#    txn_base         the pristine fact table: BASE_ROWS rows, never modified
#    txn_jrn_base     the same rows as an insert-only journal with a Top-K LAP
#
#    txn_upsert       method 1 · insert-only upsert (journal + Top-K LAP)
#    txn_swap         method 2 · staging table + partition SWAP
#    txn_merge        method 3 · optimized MERGE
#
#  Before every run, each method table is reset from its pristine copy with
#  COPY_TABLE: a catalog-only copy that shares the storage of the source
#  (milliseconds, no extra disk), so every run starts from identical data.
# =============================================================================

# The fact table: ADABAS file 16 "TXN" (account transactions) as CDC records.
#   json key | column | type
TXN_COLUMNS=(
    "hdr.isn|isn|BIGINT NOT NULL"                       # ADABAS ISN = the key
    "hdr.op|op_code|CHAR(1) NOT NULL"                   # I / U / D
    "hdr.ts|change_ts|TIMESTAMP NOT NULL"               # CDC commit time = version order
    "hdr.batch|batch_id|INT NOT NULL"                   # 0 = base, 1 = the change files
    "rec.created|created_date|DATE NOT NULL"            # immutable -> partition key
    "rec.txn_ref|txn_ref|VARCHAR(18)"
    "rec.acct_isn|acct_isn|BIGINT"
    "rec.card_isn|card_isn|BIGINT"
    "rec.type|txn_type|VARCHAR(8)"
    "rec.amount|amount|NUMERIC(15,2)"
    "rec.currency|currency|CHAR(3)"
    "rec.booking_date|booking_date|DATE"
    "rec.merchant.mcc|mcc|INT"                          # group MERCHANT
    "rec.merchant.name|merchant_name|VARCHAR(40)"
    "rec.merchant.city|merchant_city|VARCHAR(30)"
    "rec.merchant.country|merchant_country|CHAR(2)"
    "rec.tag.0|tag_1|VARCHAR(12)"                       # MU field TAG (0-2 values)
    "rec.tag.1|tag_2|VARCHAR(12)"
    "rec.reversal|is_reversal|BOOLEAN"
)

# Monthly partitions on the immutable created date: an update never moves a
# row to another partition, which is what makes partition swapping safe.
PART_EXPR="((YEAR(created_date) * 100) + MONTH(created_date))"

col_names() {   # "isn, op_code, …" [prefix]
    local c out="" p=${1:-}
    for c in "${TXN_COLUMNS[@]}"; do IFS='|' read -r _ name _ <<< "$c"; out+="${out:+, }$p$name"; done
    printf '%s' "$out"
}

# select list of the Top-K projection: PARTITION BY column, ORDER BY column, the rest
topk_col_names() {
    local c out="isn, change_ts"
    for c in "${TXN_COLUMNS[@]}"; do
        IFS='|' read -r _ name _ <<< "$c"
        [[ $name == isn || $name == change_ts ]] || out+=", $name"
    done
    printf '%s' "$out"
}

col_defs() {
    local c key name type
    for c in "${TXN_COLUMNS[@]}"; do
        IFS='|' read -r key name type <<< "$c"
        printf '    %-18s %s,\n' "$name" "$type"
    done
}

# --------------------------------------------------------------------- DDL
sql_create_fact() {   # <table>
    cat <<SQL
CREATE TABLE ${SCHEMA}.$1 (
$(col_defs)
    CONSTRAINT $1_pk PRIMARY KEY (isn) DISABLED    -- declared, not enforced
)
ORDER BY isn
SEGMENTED BY HASH(isn) ALL NODES
PARTITION BY ${PART_EXPR};
SQL
}

sql_create_journal() {   # <table>
    local cols
    cols=$(topk_col_names)
    cat <<SQL
CREATE TABLE ${SCHEMA}.$1 (
$(col_defs | sed '$ s/,$//')
)
ORDER BY isn, change_ts
SEGMENTED BY HASH(isn) ALL NODES
PARTITION BY ${PART_EXPR};

-- Top-K Live Aggregate Projection: the newest version of every ISN
CREATE PROJECTION ${SCHEMA}.$1_topk ($cols) AS
SELECT $cols
  FROM ${SCHEMA}.$1
 LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC);
SQL
}

# --------------------------------------------------------------------- generator
# 0 … 999,999: the row source of the generator (1,000 × 1M cross join = 1B)
sql_create_seq() {
    cat <<SQL
DROP TABLE IF EXISTS ${SCHEMA}.seq_1m;
CREATE TABLE ${SCHEMA}.seq_1m (n INT NOT NULL) ORDER BY n UNSEGMENTED ALL NODES;
INSERT /*+DIRECT*/ INTO ${SCHEMA}.seq_1m
SELECT (EXTRACT(EPOCH FROM ts) - EXTRACT(EPOCH FROM TIMESTAMP '2000-01-01'))::INT
  FROM (SELECT TIMESTAMP '2000-01-01' AS t UNION ALL
        SELECT TIMESTAMP '2000-01-01' + INTERVAL '999999 seconds') x
TIMESERIES ts AS '1 second' OVER (ORDER BY t);
COMMIT;
SQL
}

# sql_generate <first_isn> <last_isn> <partition label>
#   Writes the rows of one monthly partition: every value is a deterministic
#   function of the ISN (HASH), so the table is identical every time.
sql_generate() {
    local lo=$1 hi=$2 label=$3
    cat <<SQL
-- partition ${label}: ISN ${lo} … ${hi}
INSERT /*+DIRECT*/ INTO ${SCHEMA}.txn_base
SELECT isn, 'I',
       created_date + (HASH(isn, 1) % 86400) * INTERVAL '1 second',      -- change_ts
       0, created_date,
       'TX' || LPAD(isn::VARCHAR, 16, '0'),                              -- txn_ref
       1 + HASH(isn, 2) % $(( BASE_ROWS / 20 + 1 )),                     -- acct_isn
       CASE WHEN HASH(isn, 3) % 100 < 40 THEN NULL ELSE 1 + HASH(isn, 4) % $(( BASE_ROWS / 10 + 1 )) END,
       DECODE(HASH(isn, 5) % 6, 0, 'POS', 1, 'ATM', 2, 'XFER', 3, 'FEE', 4, 'INT', 'DD'),
       ((HASH(isn, 6) % 1600001) - 800000) / 100,                        -- amount
       DECODE(HASH(isn, 7) % 5, 0, 'USD', 1, 'EUR', 2, 'GBP', 3, 'ILS', 'CHF'),
       created_date + (HASH(isn, 8) % 3)::INT,                           -- booking_date
       CASE WHEN m THEN NULL ELSE 1000 + HASH(isn, 10) % 9000 END,       -- group MERCHANT
       CASE WHEN m THEN NULL ELSE DECODE(HASH(isn, 11) % 8, 0, 'Cohen', 1, 'Smith', 2, 'Rossi', 3, 'Dubois',
                                         4, 'Mueller', 5, 'Levi', 6, 'Garcia', 'Novak') END,
       CASE WHEN m THEN NULL ELSE DECODE(HASH(isn, 12) % 8, 0, 'TEL_AVIV', 1, 'LONDON', 2, 'ROME', 3, 'PARIS',
                                         4, 'BERLIN', 5, 'HAIFA', 6, 'MADRID', 'PRAGUE') END,
       CASE WHEN m THEN NULL ELSE DECODE(HASH(isn, 13) % 8, 0, 'IL', 1, 'GB', 2, 'IT', 3, 'FR',
                                         4, 'DE', 5, 'IL', 6, 'ES', 'CZ') END,
       CASE WHEN t < 50 THEN NULL ELSE DECODE(HASH(isn, 15) % 5, 0, 'ONLINE', 1, 'CONTACTLESS', 2, 'RECURRING', 3, 'FOREIGN', 'PROMO') END,
       CASE WHEN t < 85 THEN NULL ELSE DECODE(HASH(isn, 16) % 5, 0, 'ONLINE', 1, 'CONTACTLESS', 2, 'RECURRING', 3, 'FOREIGN', 'PROMO') END,
       HASH(isn, 17) % 100 < 3                                            -- is_reversal
  FROM (SELECT isn,
               DATE '${START_DATE}' + ((isn - 1) * ${SPAN_DAYS} // ${BASE_ROWS})::INT AS created_date,
               HASH(isn, 9) % 100 < 30 AS m,                              -- 30% without merchant
               HASH(isn, 14) % 100 AS t
          FROM (SELECT k.n * 1000000 + s.n + 1 AS isn
                  FROM ${SCHEMA}.seq_1m k CROSS JOIN ${SCHEMA}.seq_1m s
                 WHERE k.n BETWEEN $(( (lo - 1) / 1000000 )) AND $(( (hi - 1) / 1000000 ))) g
         WHERE isn BETWEEN ${lo} AND ${hi}) r;
COMMIT;
SQL
}

# the same rows, copied into the journal (its Top-K LAP is maintained by the INSERT)
sql_fill_journal() {   # <from date> <to date> <label>
    cat <<SQL
-- partition $3
INSERT /*+DIRECT*/ INTO ${SCHEMA}.txn_jrn_base
SELECT * FROM ${SCHEMA}.txn_base
 WHERE created_date >= '$1' AND created_date < '$2';
COMMIT;
SQL
}

# --------------------------------------------------------------------- reset
sql_reset() {   # <method>
    case $1 in
        upsert) cat <<SQL
DROP TABLE IF EXISTS ${SCHEMA}.txn_upsert, ${SCHEMA}.txn_rejects_upsert CASCADE;
SELECT COPY_TABLE('${SCHEMA}.txn_jrn_base', '${SCHEMA}.txn_upsert');
CREATE OR REPLACE VIEW ${SCHEMA}.txn_upsert_current AS
SELECT $(col_names)
  FROM (SELECT $(topk_col_names)
          FROM ${SCHEMA}.txn_upsert
         LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)) last_version
 WHERE op_code <> 'D';
SQL
        ;;
        swap) cat <<SQL
DROP TABLE IF EXISTS ${SCHEMA}.txn_swap, ${SCHEMA}.txn_swap_delta, ${SCHEMA}.txn_swap_stage, ${SCHEMA}.txn_rejects_swap CASCADE;
SELECT COPY_TABLE('${SCHEMA}.txn_base', '${SCHEMA}.txn_swap');
CREATE OR REPLACE VIEW ${SCHEMA}.txn_swap_current AS SELECT * FROM ${SCHEMA}.txn_swap;
SQL
        ;;
        merge) cat <<SQL
DROP TABLE IF EXISTS ${SCHEMA}.txn_merge, ${SCHEMA}.txn_merge_delta, ${SCHEMA}.txn_rejects_merge CASCADE;
SELECT COPY_TABLE('${SCHEMA}.txn_base', '${SCHEMA}.txn_merge');
CREATE OR REPLACE VIEW ${SCHEMA}.txn_merge_current AS SELECT * FROM ${SCHEMA}.txn_merge WHERE op_code <> 'D';
SQL
        ;;
    esac
}

# --------------------------------------------------------------------- COPY
# FILLER columns receive the flattened JSON keys ("rec.merchant.city",
# "rec.tag.0"); the real columns are computed from them in the same pass.
sql_copy() {   # <target table> <stream name> <method>
    local c key name type out=""
    for c in "${TXN_COLUMNS[@]}"; do
        IFS='|' read -r key name type <<< "$c"
        type=${type% NOT NULL}
        out+=$(printf '    %-24s FILLER %-14s %-17s AS %s' "\"$key\"" "$type," "$name" "\"$key\"")$',\n'
    done
    cat <<SQL
COPY ${SCHEMA}.$1 (
${out%$',\n'}
)
FROM '${CHANGES_DIR}/*.json' ${COPY_NODE_CLAUSE}
PARSER FJSONPARSER(flatten_arrays = true)
STREAM NAME '$2'
REJECTED DATA AS TABLE ${SCHEMA}.txn_rejects_$3;
COMMIT;
SQL
}

# --------------------------------------------------------------------- method 2: swap
sql_swap_delta() {
    cat <<SQL
CREATE TABLE ${SCHEMA}.txn_swap_delta LIKE ${SCHEMA}.txn_swap INCLUDING PROJECTIONS;
$(sql_copy txn_swap_delta "$1" swap)
SQL
}

sql_swap_touched() {
    echo "SELECT DISTINCT ${PART_EXPR} FROM ${SCHEMA}.txn_swap_delta ORDER BY 1;"
}

months_between() {   # 202511 202602 -> 4 (inclusive)
    echo $(( (${2:0:4} * 12 + 10#${2:4:2}) - (${1:0:4} * 12 + 10#${1:4:2}) + 1 ))
}

part_predicate() {   # yyyymm … -> created_date ranges (partition pruning)
    local p out=""
    for p in "$@"; do
        out+="${out:+$'\n    OR '}(f.created_date >= '${p:0:4}-${p:4:2}-01' AND f.created_date < ADD_MONTHS('${p:0:4}-${p:4:2}-01'::DATE, 1))"
    done
    printf '%s' "$out"
}

# stage table + the ISN range of the rows to rebuild (printed as "min|max")
sql_swap_prepare() {   # <touched partitions…>
    local p pmin=$1 pmax=${!#}
    echo "CREATE TABLE ${SCHEMA}.txn_swap_stage LIKE ${SCHEMA}.txn_swap INCLUDING PROJECTIONS;"
    if (( $(months_between "$pmin" "$pmax") > $# )); then
        # untouched partitions inside [pmin, pmax] must survive the range swap:
        # link them into the stage table (metadata only), drop the touched ones
        echo "SELECT COPY_PARTITIONS_TO_TABLE('${SCHEMA}.txn_swap', '${pmin}', '${pmax}', '${SCHEMA}.txn_swap_stage');"
        for p in "$@"; do echo "SELECT DROP_PARTITIONS('${SCHEMA}.txn_swap_stage', '${p}', '${p}');"; done
    fi
    cat <<SQL
SELECT MIN(isn), MAX(isn)
  FROM (SELECT f.isn FROM ${SCHEMA}.txn_swap f
         WHERE $(part_predicate "$@" | sed '2,$s/^/  /')
        UNION ALL
        SELECT isn FROM ${SCHEMA}.txn_swap_delta) x;
SQL
}

# one slice of the rebuild: the rows of the touched partitions with an ISN in
# [lo, hi]; the slices run in parallel sessions
sql_swap_rebuild() {   # <lo> <hi> <touched partitions…>
    local lo=$1 hi=$2; shift 2
    cat <<SQL
INSERT /*+DIRECT*/ INTO ${SCHEMA}.txn_swap_stage
SELECT f.*                                    -- unchanged rows of the touched partitions
  FROM ${SCHEMA}.txn_swap f
 WHERE ($(part_predicate "$@"))
   AND f.isn BETWEEN ${lo} AND ${hi}
   AND NOT EXISTS (SELECT 1 FROM ${SCHEMA}.txn_swap_delta d WHERE d.isn = f.isn)
UNION ALL
SELECT *                                      -- new images (deleted rows are left out)
  FROM ${SCHEMA}.txn_swap_delta
 WHERE op_code <> 'D'
   AND isn BETWEEN ${lo} AND ${hi};
COMMIT;
SQL
}

sql_swap_swap() {   # <touched partitions…>
    cat <<SQL
SELECT SWAP_PARTITIONS_BETWEEN_TABLES('${SCHEMA}.txn_swap_stage', '$1', '${!#}', '${SCHEMA}.txn_swap');
DROP TABLE ${SCHEMA}.txn_swap_stage, ${SCHEMA}.txn_swap_delta;
SQL
}

# --------------------------------------------------------------------- method 3: merge
sql_merge_delta() {
    cat <<SQL
CREATE TABLE ${SCHEMA}.txn_merge_delta LIKE ${SCHEMA}.txn_merge INCLUDING PROJECTIONS;
$(sql_copy txn_merge_delta "$1" merge)
SQL
}

# Optimized MERGE requirements (all met here):
#   * the target join column has a PRIMARY KEY / UNIQUE constraint
#   * UPDATE SET and INSERT list every column of the target table
#   * UPDATE and INSERT use the same source values
sql_merge_stmt() {
    local c name set=""
    for c in "${TXN_COLUMNS[@]}"; do IFS='|' read -r _ name _ <<< "$c"; set+="${set:+,$'\n'}    ${name} = s.${name}"; done
    cat <<SQL
MERGE INTO ${SCHEMA}.txn_merge t
USING ${SCHEMA}.txn_merge_delta s
   ON t.isn = s.isn
 WHEN MATCHED THEN UPDATE SET
${set}
 WHEN NOT MATCHED THEN INSERT ($(col_names))
      VALUES ($(col_names s.));
SQL
}

sql_merge_apply() {
    cat <<SQL
$(sql_merge_stmt)
COMMIT;
DROP TABLE ${SCHEMA}.txn_merge_delta;
SQL
}

# --------------------------------------------------------------------- checks
# live rows + checksum of the current data: must be identical in every method
sql_fingerprint() {   # <view> [first isn]
    echo "SELECT COUNT(*), SUM(HASH(isn, op_code, change_ts, amount, merchant_city, tag_1) % 1000000007) FROM ${SCHEMA}.$1${2:+ WHERE isn >= $2};"
}

# first ISN of the oldest partition the changes can touch: every row from there
# on is checked; older partitions are the untouched, shared storage of txn_base
sql_check_from() {
    local hot_lo=$(( BASE_ROWS - BASE_ROWS * HOT_PCT / 100 + 1 ))
    echo "SELECT MIN(isn) FROM ${SCHEMA}.txn_base WHERE created_date >= (SELECT TRUNC(created_date, 'MM') FROM ${SCHEMA}.txn_base WHERE isn = ${hot_lo});"
}
