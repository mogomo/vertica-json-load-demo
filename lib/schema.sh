# shellcheck shell=bash
# =============================================================================
#  schema.sh — SQL text generated from conf/tables.def
# =============================================================================
#  Every table gets the same CDC envelope columns, followed by the payload
#  columns declared in tables.def:
#
#    JSON key         column         type
#    hdr.isn          isn            BIGINT     ADABAS Internal Sequence Number
#    hdr.op           op_code        CHAR(1)    I=insert U=update D=delete
#    hdr.ts           change_ts      TIMESTAMP  CDC commit time (version order)
#    hdr.batch        batch_id       INT        0 = base load, n = dose n
#    rec.created      created_date   DATE       immutable -> partition key
# =============================================================================

ENVELOPE_DEF="hdr.isn|isn|BIGINT NOT NULL
hdr.op|op_code|CHAR(1) NOT NULL
hdr.ts|change_ts|TIMESTAMP NOT NULL
hdr.batch|batch_id|INT NOT NULL
rec.created|created_date|DATE NOT NULL"

# Monthly partitions on the immutable created date. A row never moves to
# another partition when it is updated, which is what makes partition
# swapping safe.
PART_EXPR="((YEAR(created_date) * 100) + MONTH(created_date))"

# --------------------------------------------------------------------- lookups
defs_tables() { awk -F'|' '$1=="T"{print $2}' "$DEFS_FILE"; }
defs_adabas_file() { awk -F'|' -v t="$1" '$1=="T" && $2==t {print $3}' "$DEFS_FILE"; }
defs_description() { awk -F'|' -v t="$1" '$1=="T" && $2==t {print $4}' "$DEFS_FILE"; }

# all columns of a table as "json_key|column|type" (envelope first)
defs_columns() {
    printf '%s\n' "$ENVELOPE_DEF"
    awk -F'|' -v t="$1" '$1=="F" && $2==t {print "rec." $3 "|" $4 "|" $5}' "$DEFS_FILE"
}

# "isn, op_code, change_ts, ..."
sql_column_list() {
    local prefix=${2:-}
    defs_columns "$1" | awk -F'|' -v p="$prefix" '{printf "%s%s%s", (NR>1?", ":""), p, $2} END{print ""}'
}

# --------------------------------------------------------------------- DDL
# sql_create_table <schema> <table> <method> [<physical_name>]
sql_create_table() {
    local schema=$1 tbl=$2 method=$3 name=${4:-$2} pk sort
    case $method in
        lap) pk=""                                     # many versions per ISN
             sort="isn, change_ts" ;;
        *)   pk=$',\n    CONSTRAINT '"${name}"'_pk PRIMARY KEY (isn) DISABLED'
             sort="isn" ;;
    esac
    cat <<SQL
CREATE TABLE ${schema}.${name} (
$(defs_columns "$tbl" | awk -F'|' '{printf "%s    %-22s %s", (NR>1?",\n":""), $2, $3} END{printf ""}')${pk}
)
ORDER BY ${sort}
SEGMENTED BY HASH(isn) ALL NODES
PARTITION BY ${PART_EXPR};
COMMENT ON TABLE ${schema}.${name} IS 'ADABAS file $(defs_adabas_file "$tbl") - $(defs_description "$tbl")';
SQL
}

# Column order required by a Top-K projection: PARTITION BY column(s) first,
# then the ORDER BY column(s), then everything else.
sql_topk_column_list() {
    local prefix=${2:-}
    { echo "isn"; echo "change_ts"; defs_columns "$1" | cut -d'|' -f2 | grep -vx -e isn -e change_ts; } \
        | awk -v p="$prefix" '{printf "%s%s%s", (NR>1?", ":""), p, $1} END{print ""}'
}

# Top-K Live Aggregate Projection: keeps only the newest version per ISN.
sql_create_topk() {
    local schema=$1 tbl=$2 name=${3:-$2}
    cat <<SQL
CREATE PROJECTION ${schema}.${name}_topk (
    $(sql_topk_column_list "$tbl")
) AS
SELECT $(sql_topk_column_list "$tbl")
  FROM ${schema}.${name}
 LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC);
SQL
}

# --------------------------------------------------------------------- COPY
# FILLER columns carry the flattened JSON key ("rec.name.first"); the real
# column is computed from it. FJSONPARSER(flatten_arrays=true) flattens nested
# objects AND arrays, so PE/MU occurrences arrive as "rec.address.0.city".
sql_copy_column_map() {
    defs_columns "$1" | awk -F'|' '{
        type = $3; sub(/ NOT NULL$/, "", type)
        printf "%s    %-32s FILLER %-15s %s AS %s", (NR>1?",\n":""), "\"" $1 "\"", type ",", $2, "\"" $1 "\""
    } END { print "" }'
}

# sql_copy <schema> <target_table> <source_table_def> <data_dir> <stream_name>
#   One COPY reads every file of <data_dir> (one parse thread per file).
#   With COPY_BATCH_FILES=N the files are loaded by several consecutive COPY
#   statements of N files each: every batch is sorted and committed on its own,
#   which bounds the temp space a huge load needs (at the cost of more, smaller
#   ROS containers for the Tuple Mover to merge).
sql_copy() {
    local schema=$1 target=$2 tbl=$3 dir=$4 stream=$5 files=() i b=0 from
    if (( ${COPY_BATCH_FILES:-0} > 0 )) && [[ -d $dir ]]; then
        files=("$dir"/*."$FILE_EXT")
    fi
    if (( ${#files[@]} <= ${COPY_BATCH_FILES:-0} || ${#files[@]} == 0 )); then
        sql_copy_stmt "$schema" "$target" "$tbl" "'${dir}/*.${FILE_EXT}' ${COPY_NODE_CLAUSE} ${COPY_FILTER}" "$stream"
        return
    fi
    for (( i = 0; i < ${#files[@]}; i += COPY_BATCH_FILES )); do
        b=$((b + 1))
        from=$(printf "'%s' ${COPY_NODE_CLAUSE} ${COPY_FILTER},\n     " "${files[@]:i:COPY_BATCH_FILES}")
        printf -- '-- batch %d: files %d-%d of %d\n' "$b" $((i + 1)) $(( i + COPY_BATCH_FILES < ${#files[@]} ? i + COPY_BATCH_FILES : ${#files[@]} )) ${#files[@]}
        sql_copy_stmt "$schema" "$target" "$tbl" "${from%,*}" "${stream}_b${b}"
    done
}

sql_copy_stmt() {
    local schema=$1 target=$2 tbl=$3 from=$4 stream=$5
    cat <<SQL
COPY ${schema}.${target} (
$(sql_copy_column_map "$tbl")
)
FROM ${from}
PARSER FJSONPARSER(flatten_arrays = true)
STREAM NAME '${stream}'
REJECTED DATA AS TABLE ${schema}.${tbl}_rejects;
SQL
}

# --------------------------------------------------------------------- MERGE
# Optimized MERGE requirements (all met here):
#   * the target join column has a PRIMARY KEY / UNIQUE constraint
#   * UPDATE SET and INSERT list every column of the target table
#   * UPDATE and INSERT use the same source values
sql_merge() {
    local schema=$1 tbl=$2 src=$3
    cat <<SQL
MERGE INTO ${schema}.${tbl} t
USING ${schema}.${src} s
   ON t.isn = s.isn
 WHEN MATCHED THEN UPDATE SET
$(defs_columns "$tbl" | awk -F'|' '{printf "%s    %s = s.%s", (NR>1?",\n":""), $2, $2} END{print ""}')
 WHEN NOT MATCHED THEN INSERT (
    $(sql_column_list "$tbl")
 ) VALUES (
    $(sql_column_list "$tbl" s.)
 );
SQL
}

# --------------------------------------------------------------------- misc
# partition key <-> date range, so partition pruning works on the fact scan
part_date_predicate() {   # <yyyymm list>
    local p out=""
    for p in "$@"; do
        out+="${out:+ OR }(created_date >= '${p:0:4}-${p:4:2}-01' AND created_date < ADD_MONTHS('${p:0:4}-${p:4:2}-01'::DATE, 1))"
    done
    printf '%s' "$out"
}
