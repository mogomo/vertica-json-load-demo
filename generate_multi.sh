#!/usr/bin/env bash
# =============================================================================
#  generate_multi.sh — data for phases 2 and 3
# =============================================================================
#  Adds 9 more ADABAS files (conf/tables.def) next to the 1-billion-row fact
#  table of generate.sh:
#    1. vload.<table>_base: 60 million rows each, generated inside Vertica
#       with SQL (parallel INSERT … SELECT, one monthly partition per statement)
#    2. 1 million multi-level JSON change records per table, for all 10 tables
#       (50% updates, 50% inserts), written twice, in two shapes:
#         demo/phase2/*.json  one record per line, the 10 tables mixed in
#                             every file
#         demo/phase3/*.json  one ADABAS transaction per line: a document with
#                             an array of changed records for each file
#
#  Run ./generate.sh first. Then ./phase2.sh and ./phase3.sh, as often as you like.
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
PHASE2_DIR="$DEMO_DIR/phase2"; PHASE3_DIR="$DEMO_DIR/phase3"
[[ -f $DEMO_DIR/manifest.env ]] || die "run ./generate.sh first (the fact table and its JSON changes)"
# the fact table's size comes from the first phase
BASE_ROWS=$(. "$DEMO_DIR/manifest.env"; echo "$BASE_ROWS")
split_changes
FACT_TABLE=$(defs_fact_table)
mapfile -t NEW_TABLES < <(defs_new_tables)
TABLES=("$FACT_TABLE" "${NEW_TABLES[@]}")
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

# one output file pair: documents [d0, d1) of the change stream
json_file() {   # <file#> <d0> <d1>
    local f=$1 d0=$2 d1=$3 t ti=0 tmp="$DEMO_DIR/.tmp_$1" slices=()
    mkdir -p "$tmp"
    for t in "${TABLES[@]}"; do
        ti=$((ti + 1))
        "$AWK_BIN" -v defs="$DEFS_FILE" -v tbl="$t" -v mode=dose -v dose=1 -v base_rows="$(table_rows "$t")" \
            -v k0="$d0" -v k1="$d1" -v n_upd="$N_UPD" -v n_del="$N_DEL" -v n_ins="$N_INS" -v hot_pct="$HOT_PCT" \
            -v start_date="$START_DATE" -v span_days="$SPAN_DAYS" -v seed=$(( SEED + ti * 1000003 + f * 104729 )) \
            -f "$ROOT_DIR/lib/gen_json.awk" > "$tmp/$t.json"
        slices+=("$tmp/$t.json")
    done
    "$AWK_BIN" -v tables="${TABLES[*]}" -v d0="$d0" -v d1="$d1" -v et_day="$ET_DAY" \
        -v phase2="$PHASE2_DIR/$(printf 'part_%03d.json' "$f")" -v phase3="$PHASE3_DIR/$(printf 'part_%03d.json' "$f")" \
        -f "$ROOT_DIR/lib/gen_docs.awk" "${slices[@]}"
    rm -rf "$tmp"
}

step_json() {
    local f files=$JSON_FILES quads d0 d1 start running=0
    AWK_BIN=$(command -v mawk || command -v gawk || command -v awk)
    ET_DAY=$(date -d "$START_DATE + $SPAN_DAYS days" +%F 2>/dev/null || echo 2026-01-01)
    (( CHANGE_ROWS % 4 == 0 )) || die "--changes must be a multiple of 4"
    quads=$(( CHANGE_ROWS / 4 ))
    (( files > quads )) && files=$quads
    explain_step M3 "$(fmt_num "$CHANGE_ROWS") JSON changes for each of the ${#TABLES[@]} tables, in two shapes" \
        "Generates $(fmt_num "$N_UPD") updates (newest ${HOT_PCT}% of each table) and $(fmt_num "$N_INS") inserts per table, with groups, multiple-value fields and periodic groups. The same $(fmt_num $(( CHANGE_ROWS * ${#TABLES[@]} ))) records are written twice, in $files files each: ${PHASE2_DIR#"$ROOT_DIR"/}/ holds one record per line, the 10 tables mixed in every file; ${PHASE3_DIR#"$ROOT_DIR"/}/ holds one ADABAS transaction per line ($(fmt_num "$CHANGE_ROWS") documents), each with an array of changed records for every file (0, 1 or 2 records)." \
        "Real CDC files mix the records of many files. Phases 2 and 3 load exactly the same changes, so their timings compare directly; only the shape of the JSON differs."
    rm -rf "$PHASE2_DIR" "$PHASE3_DIR" "$DEMO_DIR/multi"; mkdir -p "$PHASE2_DIR" "$PHASE3_DIR"
    start=$(now_ms)
    for (( f = 0; f < files; f++ )); do
        d0=$(( 4 * (quads * f / files) )); d1=$(( 4 * (quads * (f + 1) / files) ))
        json_file $((f + 1)) "$d0" "$d1" &
        running=$((running + 1))
        if (( running >= NCPU )); then wait -n || die "generator failed"; running=$((running - 1)); fi
    done
    while (( running > 0 )); do wait -n || die "generator failed"; running=$((running - 1)); done
    LAST_MS=$(( $(now_ms) - start ))
    printf '  %s⏱%s  %-44s %s%8s s%s  (2 × %d files, %s MB + %s MB)\n' "$C_GREEN" "$C_RESET" "write JSON change files" "$C_BOLD" "$(secs "$LAST_MS")" "$C_RESET" \
        "$files" "$(du -sm "$PHASE2_DIR" | awk '{print $1}')" "$(du -sm "$PHASE3_DIR" | awk '{print $1}')"
    cat > "$DEMO_DIR/multi_manifest.env" <<EOF
# written by generate_multi.sh on $(date '+%F %T')
MULTI_ROWS=$MULTI_ROWS
MULTI_CHANGE_ROWS=$CHANGE_ROWS
MULTI_N_UPD=$N_UPD
MULTI_N_DEL=$N_DEL
MULTI_N_INS=$N_INS
MULTI_TABLES="${TABLES[*]}"
MULTI_MAX_OCCURS=2
EOF
    info "phase 2 shape: one record per line (first 3 lines of a file, cut):"
    head -n 3 "$PHASE2_DIR/part_001.json" | cut -c1-140 | sed 's/$/ …/; s/^/        /'
    info "phase 3 shape: one ADABAS transaction per line (first document, first two files):"
    if command -v python3 >/dev/null; then
        head -n 1 "$PHASE3_DIR/part_001.json" | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print(json.dumps({x: d[x] for x in list(d)[:4]}, indent=2))' | sed 's/^/        /'
    else
        head -n 1 "$PHASE3_DIR/part_001.json" | cut -c1-400 | sed 's/^/        /'
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
    ok "done — now run ./phase2.sh and ./phase3.sh"
}

START_ALL=$(now_ms)
step_check
step_tables
step_json
step_summary
info "total time: $(hms $(( $(now_ms) - START_ALL )))   log: ${RUN_LOG#"$ROOT_DIR"/}"
