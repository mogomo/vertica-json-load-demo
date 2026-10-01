#!/usr/bin/env bash
# =============================================================================
#  generate_multi.sh — data for the multi-table phase
# =============================================================================
#  Adds 9 more ADABAS files (conf/tables.def) next to the 1-billion-row fact
#  table of generate.sh:
#    1. vload.<table>_base: 60 million rows each, generated inside Vertica
#       with SQL (parallel INSERT … SELECT, one monthly partition per statement)
#    2. demo/multi/<table>/*.json: 1 million multi-level JSON change records
#       per table (50% updates, 50% inserts), with groups, multiple-value
#       fields and periodic groups (arrays of objects)
#  The fact table (txn) uses vload.txn_base and demo/changes from generate.sh.
#
#  Run ./generate.sh first. Then ./apply_multi.sh, as often as you like.
#  Usage: ./generate_multi.sh [--rows 60M] [--changes 1M] [--force] [--pause] [--no-color]
# =============================================================================
set -o errexit -o nounset -o pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=/dev/null
[[ -f $ROOT_DIR/vload.env ]] && . "$ROOT_DIR/vload.env"
# shellcheck source=lib/common.sh
. "$ROOT_DIR/lib/common.sh"
# shellcheck source=lib/multi.sh
. "$ROOT_DIR/lib/multi.sh"

FORCE=0
while (( $# )); do
    case $1 in
        --rows)     MULTI_ROWS=${2:?--rows needs a value}; shift ;;
        --changes)  CHANGE_ROWS=${2:?--changes needs a value}; shift ;;
        --force)    FORCE=1 ;;
        --pause)    PAUSE=1 ;;
        --no-color) no_color ;;
        -h|--help)  sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "unknown option '$1' (see ./generate_multi.sh --help)" ;;
    esac
    shift
done
load_config
DEMO_DIR=$(cd "$DEMO_DIR" 2>/dev/null && pwd) || die "run ./generate.sh first"
CHANGES_DIR="$DEMO_DIR/changes"; MULTI_DIR="$DEMO_DIR/multi"
[[ -f $DEMO_DIR/manifest.env ]] || die "run ./generate.sh first (the fact table and its JSON changes)"
# the fact table's size comes from the first phase
BASE_ROWS=$(. "$DEMO_DIR/manifest.env"; echo "$BASE_ROWS")
split_changes
FACT_TABLE=$(defs_fact_table)
mapfile -t NEW_TABLES < <(defs_new_tables)
RUN_LOG="$LOG_DIR/generate_multi_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN_LOG"

gen_job() { m_generate "$1" "$5" "$6" "$2"; }   # <table> <label> <from> <to> <lo> <hi>

step_check() {
    chapter "GENERATE MULTI  ${#NEW_TABLES[@]} tables × $(count_label "$MULTI_ROWS") rows + $(count_label "$CHANGE_ROWS") multi-level JSON changes each" \
            "$(fmt_num "$N_UPD") updates · $(fmt_num "$N_INS") inserts per table   ·   the fact table ${FACT_TABLE} ($(count_label "$BASE_ROWS") rows) comes from generate.sh"
    vsql_check
    table_exists "${FACT_TABLE}_base" && table_exists seq_1m || die "${SCHEMA}.${FACT_TABLE}_base missing — run ./generate.sh first"
    local free need have
    free=$(vsql_query "SELECT COALESCE(SUM(disk_space_free_mb), 0) FROM disk_storage WHERE storage_usage ILIKE '%DATA%'")
    have=$(vsql_query "SELECT COALESCE(SUM(used_bytes), 0) // 1048576 FROM v_monitor.projection_storage
                        WHERE anchor_table_schema = '${SCHEMA}' AND anchor_table_name IN ($(printf "'%s_base'," "${NEW_TABLES[@]}" | sed 's/,$//'))")
    need=$(( ${#NEW_TABLES[@]} * MULTI_ROWS / 1000000 * 50 + 5000 ))   # ~50 bytes/row + JSON
    info "Vertica data storage: $(fmt_num "$free") MB free$( (( have > 0 )) && echo " + $(fmt_num "$have") MB of the current tables"), needed about $(fmt_num "$need") MB"
    (( free + have > need )) || die "not enough space"
    (( N_UPD + N_DEL <= MULTI_ROWS * HOT_PCT / 100 )) || die "updates exceed the hot window: raise HOT_PCT or lower --changes"
}

step_tables() {
    local t todo=() n
    for t in "${NEW_TABLES[@]}"; do
        if (( FORCE == 0 )) && table_exists "${t}_base" && [[ $(vsql_query "SELECT COUNT(*) FROM ${SCHEMA}.${t}_base") == "$MULTI_ROWS" ]]; then
            continue
        fi
        todo+=("$t")
    done
    if (( ${#todo[@]} == 0 )); then
        ok "all ${#NEW_TABLES[@]} tables already hold $(fmt_num "$MULTI_ROWS") rows — kept (use --force to rebuild)"
        return 0
    fi

    explain_step M1 "Create ${#todo[@]} tables: ${todo[*]}" \
        "One table per ADABAS file, built from conf/tables.def. Same layout as the fact table: CDC envelope, ISN as declared primary key, sort key and segmentation key, monthly partitions on the immutable created_date." \
        "The declared key (not enforced, so free at load time) is what lets every MERGE run the optimized plan." \
        "$(m_create_table "${todo[0]}")"
    for t in "${todo[@]}"; do echo "DROP TABLE IF EXISTS ${SCHEMA}.${t}_base, ${SCHEMA}.${t}, ${SCHEMA}.${t}_delta CASCADE;"; m_create_table "$t"; done \
        | timed "create ${#todo[@]} tables" "$RUN_LOG/create"

    JOBS=()
    for t in "${todo[@]}"; do
        while read -r line; do JOBS+=("$t $line"); done < <(partition_plan "$MULTI_ROWS")
    done
    n=$(partition_plan "$MULTI_ROWS" | wc -l | tr -d ' ')
    explain_step M2 "Generate $(fmt_num $(( ${#todo[@]} * MULTI_ROWS ))) rows: ${#todo[@]} tables × $n partitions, $GEN_SESSIONS sessions" \
        "Each INSERT … SELECT writes one monthly partition of one table. Every value is a deterministic function of the ISN, following the generator of each field in tables.def; optional fields of the same group or periodic-group occurrence are NULL together." \
        "One partition per statement gives one sorted ROS container per partition: nothing for the Tuple Mover to merge afterwards, so the timings that follow are stable." \
        "$(set -- ${JOBS[0]}; gen_job "$@")"
    run_jobs "generate ${#todo[@]} tables" gen_job $(( ${#todo[@]} * MULTI_ROWS )) "$RUN_LOG/gen" "$GEN_SESSIONS" "partitions"

    for t in "${todo[@]}"; do echo "SELECT ANALYZE_STATISTICS('${SCHEMA}.${t}_base');"; done \
        | timed "analyze statistics" "$RUN_LOG/analyze"
}

step_json() {
    local t ti=0 f files=$MULTI_JSON_FILES k0 k1 start running=0 awk_bin
    (( files > CHANGE_ROWS )) && files=$CHANGE_ROWS
    awk_bin=$(command -v mawk || command -v gawk || command -v awk)
    explain_step M3 "$(fmt_num "$CHANGE_ROWS") multi-level JSON changes per table → ${MULTI_DIR#"$ROOT_DIR"/}/<table>/" \
        "Writes $files JSON Lines files per table: $(fmt_num "$N_UPD") updates of existing ISNs (newest ${HOT_PCT}% of the table) and $(fmt_num "$N_INS") inserts of new ISNs. The records nest up to three levels: groups (objects), multiple-value fields (arrays) and periodic groups (arrays of objects)." \
        "Each table has its own files, so the ten tables can be parsed and loaded by ten independent COPY statements at the same time."
    rm -rf "$MULTI_DIR"; mkdir -p "$MULTI_DIR"
    start=$(now_ms)
    for t in "${NEW_TABLES[@]}"; do
        ti=$((ti + 1)); mkdir -p "$MULTI_DIR/$t"
        for (( f = 0; f < files; f++ )); do
            k0=$(( CHANGE_ROWS * f / files )); k1=$(( CHANGE_ROWS * (f + 1) / files ))
            "$awk_bin" -v defs="$DEFS_FILE" -v tbl="$t" -v mode=dose -v dose=1 -v base_rows="$MULTI_ROWS" \
                -v k0="$k0" -v k1="$k1" -v n_upd="$N_UPD" -v n_del="$N_DEL" -v n_ins="$N_INS" -v hot_pct="$HOT_PCT" \
                -v start_date="$START_DATE" -v span_days="$SPAN_DAYS" -v seed=$(( SEED + ti * 1000003 + f * 104729 )) \
                -f "$ROOT_DIR/lib/gen_json.awk" > "$MULTI_DIR/$t/$(printf 'part_%03d.json' $((f + 1)))" &
            running=$((running + 1))
            if (( running >= NCPU )); then wait -n || die "generator failed"; running=$((running - 1)); fi
        done
    done
    while (( running > 0 )); do wait -n || die "generator failed"; running=$((running - 1)); done
    LAST_MS=$(( $(now_ms) - start ))
    printf '  %s⏱%s  %-44s %s%8s s%s  (%d files, %s MB)\n' "$C_GREEN" "$C_RESET" "write JSON change files" "$C_BOLD" "$(secs "$LAST_MS")" "$C_RESET" \
        $(( files * ${#NEW_TABLES[@]} )) "$(du -sm "$MULTI_DIR" | awk '{print $1}')"
    cat > "$MULTI_DIR/manifest.env" <<EOF
# written by generate_multi.sh on $(date '+%F %T')
MULTI_ROWS=$MULTI_ROWS
MULTI_CHANGE_ROWS=$CHANGE_ROWS
MULTI_N_UPD=$N_UPD
MULTI_N_DEL=$N_DEL
MULTI_N_INS=$N_INS
MULTI_TABLES="${NEW_TABLES[*]}"
EOF
    info "one customer update, pretty-printed (group NAME, MU PHONE, PE ADDRESS):"
    if command -v python3 >/dev/null; then
        head -n 1 "$MULTI_DIR/customer/part_001.json" | python3 -m json.tool | sed 's/^/        /'
    else
        head -n 1 "$MULTI_DIR/customer/part_001.json" | sed 's/^/        /'
    fi
}

step_summary() {
    chapter "READY"
    vsql_query "SELECT anchor_table_name, SUM(row_count), SUM(used_bytes) // 1048576
                  FROM v_monitor.projection_storage
                 WHERE anchor_table_schema = '${SCHEMA}'
                   AND anchor_table_name IN ($(printf "'%s_base'," "$FACT_TABLE" "${NEW_TABLES[@]}" | sed 's/,$//'))
                 GROUP BY 1 ORDER BY 2 DESC, 1" \
        | while IFS='|' read -r t rows mb; do
              info "$(printf '%-16s %16s rows  %10s MB' "$t" "$(fmt_num "$rows")" "$(fmt_num "$mb")")"
          done
    ok "done — now run ./apply_multi.sh"
}

START_ALL=$(now_ms)
step_check
step_tables
step_json
step_summary
info "total time: $(hms $(( $(now_ms) - START_ALL )))   log: ${RUN_LOG#"$ROOT_DIR"/}"
