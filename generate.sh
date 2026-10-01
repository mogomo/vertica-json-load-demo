#!/usr/bin/env bash
# =============================================================================
#  generate.sh — build the demo data
# =============================================================================
#    1. the fact table vload.txn_base: 1 billion rows, generated inside Vertica
#       with SQL (parallel INSERT … SELECT, one monthly partition per statement)
#    2. vload.txn_jrn_base: the same rows as an insert-only journal with a
#       Top-K Live Aggregate Projection (the starting point of the upsert method)
#    3. demo/changes/*.json: 1 million CDC change records (50% updates of
#       existing rows, 50% inserts of new rows) as hierarchical ADABAS-style JSON
#
#  Run once; ./phase1.sh can then be run as many times as you like.
#  Usage: ./generate.sh [--rows 1B] [--changes 1M] [--force] [--pause] [--no-color]
# =============================================================================
set -o errexit -o nounset -o pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=/dev/null
[[ -f $ROOT_DIR/vload.env ]] && . "$ROOT_DIR/vload.env"
# shellcheck source=lib/common.sh
. "$ROOT_DIR/lib/common.sh"
# shellcheck source=lib/sql.sh
. "$ROOT_DIR/lib/sql.sh"

FORCE=0
while (( $# )); do
    case $1 in
        --rows)     BASE_ROWS=${2:?--rows needs a value}; shift ;;
        --changes)  CHANGE_ROWS=${2:?--changes needs a value}; shift ;;
        --force)    FORCE=1 ;;
        --pause)    PAUSE=1 ;;
        --no-color) no_color ;;
        -h|--help)  sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "unknown option '$1' (see ./generate.sh --help)" ;;
    esac
    shift
done
load_config
split_changes
RUN_LOG="$LOG_DIR/generate_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN_LOG" "$DEMO_DIR"
DEMO_DIR=$(cd "$DEMO_DIR" && pwd); CHANGES_DIR="$DEMO_DIR/changes"   # COPY needs absolute paths

gen_partition()  { sql_generate "$4" "$5" "$1"; }
fill_partition() { sql_fill_journal "$2" "$3" "$1"; }

# ---------------------------------------------------------------- steps
step_check() {
    chapter "GENERATE  $(count_label "$BASE_ROWS")-row fact table + $(count_label "$CHANGE_ROWS") JSON change records" \
            "$(fmt_num "$N_UPD") updates · $(fmt_num "$N_DEL") deletes · $(fmt_num "$N_INS") inserts   (CHANGE_MIX=$CHANGE_MIX, HOT_PCT=$HOT_PCT)"
    command -v awk >/dev/null || die "awk is required"
    vsql_check
    info "$(vsql_query "SELECT version()") · $(vsql_query "SELECT COUNT(*) FROM nodes WHERE node_state = 'UP'") node(s) up · $NCPU CPUs · $(awk '/MemTotal/{printf "%d GB RAM", $2/1024/1024}' /proc/meminfo 2>/dev/null)"
    vsql_query "SELECT COUNT(*) FROM user_functions WHERE function_name ILIKE 'FJSONParser'" | grep -qv '^0$' \
        || die "FJSONPARSER not found — install the flex table package"
    local free need have
    free=$(vsql_query "SELECT COALESCE(SUM(disk_space_free_mb), 0) FROM disk_storage WHERE storage_usage ILIKE '%DATA%'")
    # space of an existing data set counts as available: a rebuild replaces it
    have=$(vsql_query "SELECT COALESCE(SUM(used_bytes), 0) // 1048576 FROM v_monitor.projection_storage
                        WHERE anchor_table_schema = '${SCHEMA}' AND anchor_table_name IN ('txn_base', 'txn_jrn_base')")
    need=$(( BASE_ROWS / 1000000 * 110 ))   # ~37 bytes/row × (table + journal + LAP)
    info "Vertica data storage: $(fmt_num "$free") MB free$( (( have > 0 )) && echo " + $(fmt_num "$have") MB of the current data set"), the demo needs about $(fmt_num "$need") MB"
    (( free + have > need )) || die "not enough space for the Vertica tables"
    (( N_UPD + N_DEL <= BASE_ROWS * HOT_PCT / 100 )) || die "updates + deletes exceed the hot window: raise HOT_PCT or lower --changes"
}

step_base() {
    local existing
    if table_exists txn_base && table_exists txn_jrn_base && (( FORCE == 0 )); then
        existing=$(vsql_query "SELECT COUNT(*) FROM ${SCHEMA}.txn_base")
        if [[ $existing == "$BASE_ROWS" && $(vsql_query "SELECT COUNT(*) FROM ${SCHEMA}.txn_jrn_base") == "$BASE_ROWS" ]]; then
            ok "${SCHEMA}.txn_base and ${SCHEMA}.txn_jrn_base already hold $(fmt_num "$BASE_ROWS") rows — kept (use --force to rebuild)"
            return 0
        fi
        info "${SCHEMA}.txn_base holds $(fmt_num "$existing") rows, $(fmt_num "$BASE_ROWS") wanted: rebuilding"
    fi

    explain_step 1 "Schema and a 1-million-row number table" \
        "Creates the schema and seq_1m (0 … 999,999) with TIMESERIES. Cross-joined with itself it yields up to 10^12 row numbers." \
        "The billion rows are created inside Vertica: no files to write, read or parse, and every row is a deterministic function of its ISN." \
        "CREATE SCHEMA IF NOT EXISTS ${SCHEMA};
$(sql_create_seq)"
    { echo "DROP TABLE IF EXISTS ${SCHEMA}.txn_base, ${SCHEMA}.txn_jrn_base, ${SCHEMA}.txn_upsert, ${SCHEMA}.txn_swap, ${SCHEMA}.txn_merge CASCADE;"
      echo "CREATE SCHEMA IF NOT EXISTS ${SCHEMA};"; sql_create_seq; } | timed "number table" "$RUN_LOG/seq"

    local p1
    p1=$(partition_plan "$BASE_ROWS" | head -1)
    explain_step 2 "The fact table: $(fmt_num "$BASE_ROWS") rows, one monthly partition per INSERT" \
        "Creates txn_base (ADABAS file TXN: account transactions) and fills its $(partition_plan "$BASE_ROWS" | wc -l | tr -d ' ') monthly partitions with $GEN_SESSIONS parallel INSERT … SELECT sessions. The ISN is the primary key (declared, not enforced), the sort key and the segmentation key." \
        "Writing one whole partition per statement gives one sorted ROS container per partition, so the Tuple Mover has nothing to merge afterwards and the timings that follow are stable. Partitions are months of the immutable created_date: an update never moves a row to another partition." \
        "$(sql_create_fact txn_base)

$(set -- $p1; sql_generate "$4" "$5" "$1")"
    sql_create_fact txn_base | timed "create txn_base" "$RUN_LOG/create_base"
    mapfile -t JOBS < <(partition_plan "$BASE_ROWS")
    run_jobs "generate txn_base" gen_partition "$BASE_ROWS" "$RUN_LOG/gen" "$GEN_SESSIONS" "partitions"

    explain_step 3 "The same rows as an insert-only journal with a Top-K LAP" \
        "Creates txn_jrn_base, sorted by (isn, change_ts), with a Top-K Live Aggregate Projection that keeps the newest version of every ISN, and copies txn_base into it, one partition per INSERT." \
        "This is the starting point of the upsert method. The LAP is maintained while the rows are inserted, so it is ready when the changes arrive." \
        "$(sql_create_journal txn_jrn_base)

$(set -- $p1; sql_fill_journal "$2" "$3" "$1")"
    sql_create_journal txn_jrn_base | timed "create txn_jrn_base + Top-K LAP" "$RUN_LOG/create_jrn"
    run_jobs "fill txn_jrn_base (+ LAP)" fill_partition "$BASE_ROWS" "$RUN_LOG/fill" "$GEN_SESSIONS" "partitions"

    explain_step 4 "Optimizer statistics" \
        "ANALYZE_STATISTICS on both tables." \
        "Accurate row counts and value ranges give the optimizer the right join plans for the MERGE and the partition rebuild." \
        "SELECT ANALYZE_STATISTICS('${SCHEMA}.txn_base');
SELECT ANALYZE_STATISTICS('${SCHEMA}.txn_jrn_base');"
    printf "SELECT ANALYZE_STATISTICS('%s.txn_base');\nSELECT ANALYZE_STATISTICS('%s.txn_jrn_base');\n" "$SCHEMA" "$SCHEMA" \
        | timed "analyze statistics" "$RUN_LOG/analyze"
}

step_json() {
    local files=$JSON_FILES f k0 k1 start awk_bin running=0
    (( files > CHANGE_ROWS )) && files=$CHANGE_ROWS
    awk_bin=$(command -v mawk || command -v gawk || command -v awk)
    explain_step 5 "$(fmt_num "$CHANGE_ROWS") change records as JSON files in ${CHANGES_DIR#"$ROOT_DIR"/}/" \
        "Writes $files JSON Lines files with $(fmt_num "$N_UPD") updates and $(fmt_num "$N_DEL") deletes of existing ISNs (in the newest ${HOT_PCT}% of the table) and $(fmt_num "$N_INS") inserts of new ISNs. Each record has a CDC header (isn, op, ts, batch) and the ADABAS record: fields, the MERCHANT group and the TAG multiple-value field." \
        "Several files let one COPY statement parse in parallel: one parse thread per file. The files are written once and loaded by every method on every run, so all methods do exactly the same work."
    rm -rf "$CHANGES_DIR"; mkdir -p "$CHANGES_DIR"
    start=$(now_ms)
    for (( f = 0; f < files; f++ )); do
        k0=$(( CHANGE_ROWS * f / files )); k1=$(( CHANGE_ROWS * (f + 1) / files ))
        "$awk_bin" -v base_rows="$BASE_ROWS" -v n_upd="$N_UPD" -v n_del="$N_DEL" -v n_ins="$N_INS" \
            -v k0="$k0" -v k1="$k1" -v hot_pct="$HOT_PCT" -v start_date="$START_DATE" -v span_days="$SPAN_DAYS" \
            -v seed=$(( SEED + f * 7919 )) -f "$ROOT_DIR/lib/gen_changes.awk" > "$CHANGES_DIR/$(printf 'changes_%03d.json' $((f + 1)))" &
        running=$((running + 1))
        if (( running >= NCPU )); then wait -n || die "generator failed"; running=$((running - 1)); fi
    done
    while (( running > 0 )); do wait -n || die "generator failed"; running=$((running - 1)); done
    LAST_MS=$(( $(now_ms) - start ))
    printf '  %s⏱%s  %-44s %s%8s s%s  (%d files, %s MB)\n' "$C_GREEN" "$C_RESET" "write JSON change files" "$C_BOLD" "$(secs "$LAST_MS")" "$C_RESET" \
        "$files" "$(du -sm "$CHANGES_DIR" | awk '{print $1}')"

    cat > "$DEMO_DIR/manifest.env" <<EOF
# written by generate.sh on $(date '+%F %T')
BASE_ROWS=$BASE_ROWS
CHANGE_ROWS=$CHANGE_ROWS
CHANGE_MIX=$CHANGE_MIX
N_UPD=$N_UPD
N_DEL=$N_DEL
N_INS=$N_INS
HOT_PCT=$HOT_PCT
START_DATE=$START_DATE
SPAN_DAYS=$SPAN_DAYS
SEED=$SEED
JSON_FILES=$files
EOF
    info "one update record, pretty-printed:"
    if command -v python3 >/dev/null; then
        head -n 1 "$CHANGES_DIR/changes_001.json" | python3 -m json.tool | sed 's/^/        /'
    else
        head -n 1 "$CHANGES_DIR/changes_001.json" | sed 's/^/        /'
    fi
}

step_summary() {
    chapter "READY"
    vsql_query "SELECT anchor_table_name, SUM(row_count), SUM(used_bytes) // 1048576
                  FROM v_monitor.projection_storage
                 WHERE anchor_table_schema = '${SCHEMA}' AND anchor_table_name IN ('txn_base', 'txn_jrn_base')
                 GROUP BY 1 ORDER BY 1" \
        | while IFS='|' read -r t rows mb; do
              info "$(printf '%-14s %16s rows in all projections  %10s MB' "$t" "$(fmt_num "$rows")" "$(fmt_num "$mb")")"
          done
    info "$(printf '%-14s %16s records in %s files' "JSON changes" "$(fmt_num "$CHANGE_ROWS")" "$(find "$CHANGES_DIR" -name '*.json' | wc -l | tr -d ' ')")"
    ok "done — now run ./phase1.sh"
}

START_ALL=$(now_ms)
step_check
step_base
step_json
step_summary
info "total time: $(hms $(( $(now_ms) - START_ALL )))   log: ${RUN_LOG#"$ROOT_DIR"/}"
