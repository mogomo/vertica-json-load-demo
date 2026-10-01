# shellcheck shell=bash
# =============================================================================
#  multi.sh — SQL of phases 2 and 3, generated from conf/tables.def
# =============================================================================
#  Tables (schema $SCHEMA):
#    <table>_base     pristine data, never modified (txn_base = the 1B-row fact
#                     table of phase 1, the others MULTI_ROWS rows each)
#    <table>          working copy, reset from <table>_base with COPY_TABLE
#    stg_flat         phase 2 staging: one row per JSON record, the columns of
#                     all 10 tables side by side, partitioned by table
#    stg_doc          phase 3 staging: one row per JSON document (ADABAS
#                     transaction), MAX_OCCURS slots of columns per table for
#                     the records of that table's array
#
#  Every table gets the CDC envelope columns followed by its own columns:
#    hdr.isn → isn BIGINT · hdr.op → op_code · hdr.ts → change_ts
#    hdr.batch → batch_id · rec.created → created_date (partition key)
# =============================================================================

: "${DEFS_FILE:=$ROOT_DIR/conf/tables.def}"

ENVELOPE_DEF="hdr.isn|isn|BIGINT NOT NULL
hdr.op|op_code|CHAR(1) NOT NULL
hdr.ts|change_ts|TIMESTAMP NOT NULL
hdr.batch|batch_id|INT NOT NULL
rec.created|created_date|DATE NOT NULL"

M_PART_EXPR="((YEAR(created_date) * 100) + MONTH(created_date))"

# --------------------------------------------------------------------- lookups
defs_tables()      { awk -F'|' '$1=="T"{print $2}' "$DEFS_FILE"; }
defs_new_tables()  { awk -F'|' '$1=="T" && $5!="fact"{print $2}' "$DEFS_FILE"; }
defs_fact_table()  { awk -F'|' '$1=="T" && $5=="fact"{print $2; exit}' "$DEFS_FILE"; }
defs_file_no()     { awk -F'|' -v t="$1" '$1=="T" && $2==t {print $3}' "$DEFS_FILE"; }
defs_description() { awk -F'|' -v t="$1" '$1=="T" && $2==t {print $4}' "$DEFS_FILE"; }

# all columns of a table as "json_key|column|type" (envelope first)
defs_columns() {
    printf '%s\n' "$ENVELOPE_DEF"
    awk -F'|' -v t="$1" '$1=="F" && $2==t {print "rec." $3 "|" $4 "|" $5}' "$DEFS_FILE"
}

m_col_list() {   # <table> [prefix] -> "isn, op_code, …"
    defs_columns "$1" | awk -F'|' -v p="${2:-}" '{printf "%s%s%s", (NR>1?", ":""), p, $2} END{print ""}'
}

# rows of a table in this phase
table_rows() { if [[ $1 == "$FACT_TABLE" ]]; then echo "$BASE_ROWS"; else echo "$MULTI_ROWS"; fi; }

# --------------------------------------------------------------------- DDL
m_create_table() {   # <table>
    local t=$1
    cat <<SQL
CREATE TABLE ${SCHEMA}.${t}_base (
$(defs_columns "$t" | awk -F'|' '{printf "    %-20s %s,\n", $2, $3}')
    CONSTRAINT ${t}_base_pk PRIMARY KEY (isn) DISABLED
)
ORDER BY isn
SEGMENTED BY HASH(isn) ALL NODES
PARTITION BY ${M_PART_EXPR};
COMMENT ON TABLE ${SCHEMA}.${t}_base IS 'ADABAS file $(defs_file_no "$t") - $(defs_description "$t")';
SQL
}

# --------------------------------------------------------------------- generator
# SQL expressions for the fields of a table: deterministic functions of the
# ISN (HASH), following the generators of tables.def.
m_gen_exprs() {   # <table>
    awk -F'|' -v Q="'" -v t="$1" -v rows="$MULTI_ROWS" -v fact="$FACT_TABLE" -v fact_rows="$BASE_ROWS" '
    function pick(seed, list,    a, n, i, s) {
        n = split(list, a, ";")
        s = "DECODE(HASH(isn, " seed ") % " n
        for (i = 1; i < n; i++) s = s ", " (i - 1) ", " Q a[i] Q
        return s ", " Q a[n] Q ")"
    }
    BEGIN {
        FIRST = "James;Mary;Robert;Patricia;Noam;Maya;Hans;Giulia"
        LAST  = "Smith;Cohen;Levi;Garcia;Mueller;Rossi;Dubois;Novak"
        CITY  = "NEW_YORK;LONDON;PARIS;BERLIN;TEL_AVIV;HAIFA;ZURICH;ROME"
        CTRY  = "US;GB;FR;DE;IL;CH;IT;ES"
        SFX   = "ST;AVE;RD;BLVD"
        DOM   = "mail.example;example.org;corp.example;bank.example"
        nc = 0
    }
    $1 == "F" && $2 == t {
        i++; s = 100 + i * 4; spec = $6; nul = 0
        q = index(spec, "?"); if (q) { nul = substr(spec, q + 1) + 0; spec = substr(spec, 1, q - 1) }
        split(spec, g, ":"); k = g[1]
        if      (k == "first")   e = pick(s, FIRST)
        else if (k == "last")    e = pick(s, LAST)
        else if (k == "city")    e = pick(s, CITY)
        else if (k == "country") e = pick(s, CTRY)
        else if (k == "street")  e = "(1 + HASH(isn, " s ") % 250) || " Q " " Q " || " pick(s + 1, LAST) " || " Q " " Q " || " pick(s + 2, SFX)
        else if (k == "phone")   e = Q "+" Q " || (1 + HASH(isn, " s ") % 98) || " Q "-" Q " || LPAD((HASH(isn, " s + 1 ") % 1000)::VARCHAR, 3, " Q "0" Q ") || " Q "-" Q " || LPAD((HASH(isn, " s + 2 ") % 10000000)::VARCHAR, 7, " Q "0" Q ")"
        else if (k == "email")   e = "LOWER(" pick(s, FIRST) ") || " Q "." Q " || isn || " Q "@" Q " || " pick(s + 1, DOM)
        else if (k == "code")    e = "LEFT(UPPER(TO_HEX(HASH(isn, " s ")) || TO_HEX(HASH(isn, " s + 1 "))), " g[2] ")"
        else if (k == "bool")    e = "HASH(isn, " s ") % 100 < 3"
        else if (k == "pick")    e = pick(s, g[2])
        else if (k == "int")     e = g[2] " + HASH(isn, " s ") % " (g[3] - g[2] + 1)
        else if (k == "num")     e = "(" (g[2] * 100) " + HASH(isn, " s ") % " ((g[3] - g[2]) * 100 + 1) ") / 100"
        else if (k == "date")    e = "DATE " Q g[2] Q " + (HASH(isn, " s ") % " g[3] ")::INT"
        else if (k == "id")      e = Q g[2] Q " || LPAD(isn::VARCHAR, " g[3] ", " Q "0" Q ")"
        else if (k == "ref")     e = "1 + HASH(isn, " s ") % " (g[2] == fact ? fact_rows : rows)
        else { print "unknown generator " k > "/dev/stderr"; exit 2 }
        if (nul) {
            # optional fields of the same group / PE occurrence are NULL together
            cont = $3; sub(/\.[^.]*$/, "", cont); if (cont == $3) cont = $3
            if (!(cont in cid)) cid[cont] = ++nc
            e = "CASE WHEN HASH(isn, " (5000 + cid[cont]) ") % 100 < " nul " THEN NULL ELSE " e " END"
        }
        printf ",\n       %s", e
    }' "$DEFS_FILE"
}

m_generate() {   # <table> <first isn> <last isn> <partition label>
    local t=$1 lo=$2 hi=$3
    cat <<SQL
-- ${t}, partition $4: ISN ${lo} … ${hi}
INSERT /*+DIRECT*/ INTO ${SCHEMA}.${t}_base
SELECT isn, 'I', created_date + (HASH(isn, 1) % 86400) * INTERVAL '1 second', 0, created_date$(m_gen_exprs "$t")
  FROM (SELECT isn, DATE '${START_DATE}' + ((isn - 1) * ${SPAN_DAYS} // ${MULTI_ROWS})::INT AS created_date
          FROM (SELECT k.n * 1000000 + s.n + 1 AS isn
                  FROM ${SCHEMA}.seq_1m k CROSS JOIN ${SCHEMA}.seq_1m s
                 WHERE k.n BETWEEN $(( (lo - 1) / 1000000 )) AND $(( (hi - 1) / 1000000 ))) g
         WHERE isn BETWEEN ${lo} AND ${hi}) r;
COMMIT;
SQL
}

# --------------------------------------------------------------------- reset
m_reset() {   # <table>
    cat <<SQL
DROP TABLE IF EXISTS ${SCHEMA}.$1, ${SCHEMA}.$1_delta, ${SCHEMA}.$1_rejects CASCADE;
SELECT COPY_TABLE('${SCHEMA}.$1_base', '${SCHEMA}.$1');
SQL
}

# --------------------------------------------------------------------- phase 2
# One JSON record per line; the table is named in the header and the record
# sits under a key with the table's name:
#   {"hdr":{"file":"customer","isn":…,"op":"U","ts":…,"batch":1},"customer":{…}}
# Columns of stg_flat: file + the envelope + <table>__<column> for every table.
sql_flat_create() {
    local t
    echo "DROP TABLE IF EXISTS ${SCHEMA}.stg_flat CASCADE;"
    echo "CREATE TABLE ${SCHEMA}.stg_flat ("
    echo "    file               VARCHAR(12) NOT NULL,"
    defs_columns "$FACT_TABLE" | awk -F'|' '$1 ~ /^hdr\./ { t = $3; sub(/ NOT NULL$/, "", t); printf "    %-18s %s,\n", $2, t }'
    for t in "${TABLES[@]}"; do
        defs_columns "$t" | awk -F'|' -v t="$t" '$1 !~ /^hdr\./ { ty = $3; sub(/ NOT NULL$/, "", ty); printf "    %-40s %s,\n", t "__" $2, ty }'
    done | sed '$ s/,$//'
    cat <<SQL
)
ORDER BY file, isn
SEGMENTED BY HASH(isn) ALL NODES
PARTITION BY file;
SQL
}

sql_flat_copy() {   # <stream name>
    local t
    echo "COPY ${SCHEMA}.stg_flat ("
    {
        printf '    %-34s FILLER %-15s %-40s AS %s' '"hdr.file"' 'VARCHAR(12),' 'file' '"hdr.file"'
        defs_columns "$FACT_TABLE" | awk -F'|' '$1 ~ /^hdr\./ { ty = $3; sub(/ NOT NULL$/, "", ty)
            printf ",\n    %-34s FILLER %-15s %-40s AS %s", "\"" $1 "\"", ty ",", $2, "\"" $1 "\"" }'
        for t in "${TABLES[@]}"; do
            defs_columns "$t" | awk -F'|' -v t="$t" '$1 !~ /^hdr\./ { ty = $3; sub(/ NOT NULL$/, "", ty); k = $1; sub(/^rec\./, t ".", k)
                printf ",\n    %-34s FILLER %-15s %-40s AS %s", "\"" k "\"", ty ",", t "__" $2, "\"" k "\"" }'
        done
        echo
    }
    cat <<SQL
)
FROM '${PHASE2_DIR}/*.json' ${COPY_NODE_CLAUSE}
PARSER FJSONPARSER(flatten_arrays = true)
STREAM NAME '$1'
REJECTED DATA AS TABLE ${SCHEMA}.stg_flat_rejects;
SQL
}

sql_flat_source() {   # <table> : the table's rows, with its own column names
    local t=$1
    printf 'SELECT %s\n  FROM %s.stg_flat\n WHERE file = '"'"'%s'"'" \
        "$(defs_columns "$t" | awk -F'|' -v t="$t" '{ c = ($1 ~ /^hdr\./) ? $2 : t "__" $2 " AS " $2; printf "%s%s", (NR > 1 ? ", " : ""), c }')" "$SCHEMA" "$t"
}

# --------------------------------------------------------------------- phase 3
# One JSON document per line: an ADABAS transaction (ET) with the changed
# records of several files, as one array per file:
#   {"et_id":…,"et_ts":…,"customer":[{"hdr":{…},"rec":{…}},…],"account":[…],…}
# FJSONPARSER(flatten_arrays=true) flattens the arrays too: the 2nd customer
# record of a document arrives as "customer.1.hdr.isn", "customer.1.rec.name.first"…
# stg_doc has MAX_OCCURS slots of columns per table, named exactly like those
# keys, so no column list is needed. A table's rows are the UNION ALL of its
# slots. (A record beyond MAX_OCCURS would be an unmatched key; the check after
# the MERGEs would then find missing rows.)
doc_slot_cols() {   # <table> <slot> -> "json_key|column|type" lines for that slot
    defs_columns "$1" | awk -F'|' -v t="$1" -v o="$2" '{ ty = $3; sub(/ NOT NULL$/, "", ty); print t "." o "." $1 "|" $2 "|" ty }'
}

sql_doc_create() {
    local t o
    echo "DROP TABLE IF EXISTS ${SCHEMA}.stg_doc CASCADE;"
    echo "CREATE TABLE ${SCHEMA}.stg_doc ("
    echo "    et_id INT NOT NULL,"
    echo "    et_ts TIMESTAMP,"
    for t in "${TABLES[@]}"; do
        for (( o = 0; o < MAX_OCCURS; o++ )); do
            doc_slot_cols "$t" "$o" | awk -F'|' '{ printf "    %-44s %s,\n", "\"" $1 "\"", $3 }'
        done
    done | sed '$ s/,$//'
    cat <<SQL
)
ORDER BY et_id
SEGMENTED BY HASH(et_id) ALL NODES;
SQL
}

sql_doc_copy() {   # <stream name>
    cat <<SQL
COPY ${SCHEMA}.stg_doc
FROM '${PHASE3_DIR}/*.json' ${COPY_NODE_CLAUSE}
PARSER FJSONPARSER(flatten_arrays = true)
STREAM NAME '$1'
REJECTED DATA AS TABLE ${SCHEMA}.stg_doc_rejects;
SQL
}

sql_doc_source() {   # <table> : the table's records = UNION ALL of its slots
    local t=$1 o out=""
    for (( o = 0; o < MAX_OCCURS; o++ )); do
        out+="${out:+$'\nUNION ALL\n'}SELECT $(doc_slot_cols "$t" "$o" | awk -F'|' '{ printf "%s\"%s\" AS %s", (NR > 1 ? ", " : ""), $1, $2 }')
  FROM ${SCHEMA}.stg_doc
 WHERE \"${t}.${o}.hdr.isn\" IS NOT NULL"
    done
    printf '%s' "$out"
}

# --------------------------------------------------------------------- MERGE
# Optimized MERGE: declared key, every column in UPDATE SET and INSERT, same
# values. The source is a query on the staging table.
m_merge() {   # <table> <source query | table>
    local t=$1 src
    if [[ $2 == SELECT* ]]; then src="(
$(printf '%s\n' "$2" | sed 's/^/    /')
)"; else src=$2; fi
    cat <<SQL
MERGE INTO ${SCHEMA}.${t} t
USING ${src} s
   ON t.isn = s.isn
 WHEN MATCHED THEN UPDATE SET
$(defs_columns "$t" | awk -F'|' '{printf "%s    %s = s.%s", (NR>1?",\n":""), $2, $2} END{print ""}')
 WHEN NOT MATCHED THEN INSERT ($(m_col_list "$t"))
      VALUES ($(m_col_list "$t" s.));
COMMIT;
SQL
}

# --------------------------------------------------------------------- check
# rows in the table · rows of this batch · checksum of the batch rows in the
# table · checksum of the source rows → the last two must be equal
m_check() {   # <table> <source query>
    local t=$1 h
    h="SUM(HASH($(m_col_list "$t")) % 1000000007)"
    cat <<SQL
SELECT (SELECT COUNT(*) FROM ${SCHEMA}.${t}),
       (SELECT COUNT(*) FROM ${SCHEMA}.${t} WHERE batch_id = 1),
       (SELECT ${h} FROM ${SCHEMA}.${t} WHERE batch_id = 1),
       (SELECT ${h} FROM $(if [[ $2 == SELECT* ]]; then printf '(\n%s\n       ) src' "$(printf '%s\n' "$2" | sed 's/^/            /')"; else echo "$2"; fi));
SQL
}
