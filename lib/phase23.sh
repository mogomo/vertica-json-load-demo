# shellcheck shell=bash
# =============================================================================
#  phase23.sh — the runner of phases 2 and 3 (sourced by phase2.sh / phase3.sh
#  with PHASE=2 or PHASE=3)
# =============================================================================
#  One timed job per run:
#    ⏱ 1. one COPY parses every JSON file once into the staging table
#    ⏱ 2. 10 optimized MERGEs, one per table, read their rows from the staging
#          table; at most --parallel run at the same time
#  Before: the 10 tables are reset with COPY_TABLE and the staging table is
#  recreated (not timed). After: every table is checked (not timed).
# =============================================================================
set -o errexit -o nounset -o pipefail

# shellcheck source=/dev/null
[[ -f $ROOT_DIR/vload.env ]] && . "$ROOT_DIR/vload.env"
# shellcheck source=lib/common.sh
. "$ROOT_DIR/lib/common.sh"
# shellcheck source=lib/multi.sh
. "$ROOT_DIR/lib/multi.sh"

RUNS=1 VALIDATE=1 PARALLEL_LIST=""
while (( $# )); do
    case $1 in
        --parallel) PARALLEL_LIST=${2:?--parallel needs a value}; shift ;;
        --runs)     RUNS=${2:?--runs needs a value}; shift ;;
        --pause)    PAUSE=1 ;;
        --no-check) VALIDATE=0 ;;
        --no-color) no_color ;;
        -h|--help)  sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "unknown option '$1' (see $0 --help)" ;;
    esac
    shift
done
[[ $RUNS =~ ^[1-9][0-9]*$ ]] || die "--runs must be a positive number"

load_config
IFS=, read -r -a PLIST <<< "${PARALLEL_LIST:-$PARALLEL}"
for p in "${PLIST[@]}"; do [[ $p =~ ^[1-9][0-9]*$ ]] || die "--parallel takes numbers, e.g. 10 or 10,5,1"; done
DEMO_DIR=$(cd "$DEMO_DIR" 2>/dev/null && pwd) || die "run ./generate.sh and ./generate_multi.sh first"
PHASE2_DIR="$DEMO_DIR/phase2"; PHASE3_DIR="$DEMO_DIR/phase3"
[[ -f $DEMO_DIR/manifest.env && -f $DEMO_DIR/multi_manifest.env ]] || die "run ./generate.sh and ./generate_multi.sh first"
BASE_ROWS=$(. "$DEMO_DIR/manifest.env"; echo "$BASE_ROWS")
# shellcheck source=/dev/null
. "$DEMO_DIR/multi_manifest.env"     # MULTI_ROWS, MULTI_CHANGE_ROWS, MULTI_N_INS, MULTI_TABLES, MULTI_MAX_OCCURS
MAX_OCCURS=${MULTI_MAX_OCCURS:-2}
FACT_TABLE=$(defs_fact_table)
read -r -a TABLES <<< "$MULTI_TABLES"
TOTAL_CHANGES=$(( MULTI_CHANGE_ROWS * ${#TABLES[@]} ))

if [[ $PHASE == 2 ]]; then
    JSON_DIR=$PHASE2_DIR STAGE=stg_flat
    PHASE_TITLE="PHASE 2 · mixed JSON records → one flat staging table → 10 parallel MERGEs"
    SHAPE="one JSON record per line, the ${#TABLES[@]} tables mixed in every file"
    stage_create() { sql_flat_create; }
    stage_copy()   { sql_flat_copy "$1"; }
    source_sql()   { sql_flat_source "$1"; }
else
    JSON_DIR=$PHASE3_DIR STAGE=stg_doc
    SHAPE="one ADABAS transaction per line, an array of changed records for each of the ${#TABLES[@]} files"
    PHASE_TITLE="PHASE 3 · nested JSON documents → one staging row per document → 10 parallel MERGEs"
    stage_create() { sql_doc_create; }
    stage_copy()   { sql_doc_copy "$1"; }
    source_sql()   { sql_doc_source "$1"; }
fi
compgen -G "$JSON_DIR/*.json" >/dev/null || die "no JSON files in $JSON_DIR — run ./generate_multi.sh"
N_FILES=$(find "$JSON_DIR" -name '*.json' | wc -l | tr -d ' ')

SESSION=$(date +%Y%m%d-%H%M%S)
RUN_LOG="$LOG_DIR/phase${PHASE}_$SESSION"
mkdir -p "$RUN_LOG" "$REPORT_DIR"
RESULTS_TSV="$REPORT_DIR/phase${PHASE}_results.tsv"
[[ -f $RESULTS_TSV ]] || printf 'session\trun\tparallel\tstep\ttable\trows\tms\n' > "$RESULTS_TSV"
tsv() { printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$SESSION" "$RUN" "$P" "$@" >> "$RESULTS_TSV"; }

preflight() {
    vsql_check
    local t n
    for t in "${TABLES[@]}"; do
        table_exists "${t}_base" || die "${SCHEMA}.${t}_base missing — run ./generate.sh and ./generate_multi.sh"
        n=$(vsql_query "SELECT COUNT(*) FROM ${SCHEMA}.${t}_base")
        [[ $n == "$(table_rows "$t")" ]] || die "${SCHEMA}.${t}_base has $(fmt_num "$n") rows, $(fmt_num "$(table_rows "$t")") expected"
    done
}

# ---------------------------------------------------------------- MERGE pool
# One table's MERGE, in the background. Writes "ms end_ms rc" to $TMP_DIR/<table>.
merge_one() {
    local t=$1 s0 rc=0
    s0=$(now_ms)
    m_merge "$t" "$(source_sql "$t")" | vsql_exec "$RUN_LOG/r${RUN}_p${P}_${t}_merge" >/dev/null || rc=1
    printf '%s %s %s\n' $(( $(now_ms) - s0 )) "$(now_ms)" "$rc" > "$TMP_DIR/$t"
}

merge_all() {
    local t start dispatcher spin='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏' i=0 done_n end fails=0 ms e rc
    TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/vload.XXXXXX")
    start=$(now_ms)
    (
        for t in "${TABLES[@]}"; do
            merge_one "$t" &
            while (( $(jobs -rp | wc -l) >= P )); do wait -n || true; done
        done
        wait
    ) &
    dispatcher=$!
    while kill -0 "$dispatcher" 2>/dev/null; do
        if (( IS_TTY )); then
            done_n=$(find "$TMP_DIR" -type f | wc -l)
            printf '\r  %s%s %-44s %8s s  %d/%d tables merged%s' "$C_CYAN" "${spin:i++%10:1}" "$P MERGEs at a time" "$(secs $(( $(now_ms) - start )))" "$done_n" "${#TABLES[@]}" "$C_RESET"
        fi
        sleep 0.1
    done
    wait "$dispatcher" || true
    (( IS_TTY )) && printf '\r\033[K'
    end=$start
    printf '  %s%-12s %15s %12s%s\n' "$C_DIM" "table" "table rows" "MERGE" "$C_RESET"
    for t in "${TABLES[@]}"; do
        ms=0 e=$start rc=1
        read -r ms e rc < "$TMP_DIR/$t" 2>/dev/null || true
        (( e > end )) && end=$e
        if (( rc == 0 )); then
            printf '  %-12s %15s %10s s\n' "$t" "$(fmt_num "$(table_rows "$t")")" "$(secs "$ms")"
        else
            printf '  %s%-12s FAILED — see %s%s\n' "$C_RED" "$t" "${RUN_LOG#"$ROOT_DIR"/}/r${RUN}_p${P}_${t}_*.err" "$C_RESET"
            fails=$((fails + 1))
        fi
        tsv merge "$t" "$MULTI_CHANGE_ROWS" "$ms"
    done
    rm -rf "$TMP_DIR"
    (( fails == 0 )) || die "$fails MERGE(s) failed"
    LAST_MS=$(( end - start ))
    printf '  %s⏱%s  %-44s %s%8s s%s\n' "$C_GREEN" "$C_RESET" "${#TABLES[@]} MERGEs, $P at a time" "$C_BOLD" "$(secs "$LAST_MS")" "$C_RESET"
}

check_tables() {
    local t cnt b1 h1 h2 bad=0
    TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/vload.XXXXXX")
    for t in "${TABLES[@]}"; do
        ( m_check "$t" "$(source_sql "$t")" | vsql_exec "$RUN_LOG/r${RUN}_p${P}_${t}_check" > "$TMP_DIR/$t" ) &
    done
    wait
    for t in "${TABLES[@]}"; do
        IFS='|' read -r cnt b1 h1 h2 < "$TMP_DIR/$t"
        if [[ $cnt != $(( $(table_rows "$t") + MULTI_N_INS )) || $b1 != "$MULTI_CHANGE_ROWS" || $h1 != "$h2" ]]; then
            warn "$t: rows $cnt, changed rows $b1, checksum $h1 vs JSON $h2"; bad=1
        fi
    done
    rm -rf "$TMP_DIR"
    (( bad == 0 )) || die "check failed"
    ok "check (not timed): every table holds its rows + $(fmt_num "$MULTI_N_INS") inserts, and its $(fmt_num "$MULTI_CHANGE_ROWS") changed rows equal the JSON rows (count + checksum of all columns)"
}

show_plan() {   # EXPLAIN of one MERGE: must be the optimized plan
    local plan
    plan=$(m_merge customer "$(source_sql customer)" | sed '/^COMMIT;$/d; s/^MERGE/EXPLAIN MERGE/' | "$VSQL" -X -A -t -q 2>/dev/null || true)
    printf '%s  EXPLAIN (customer):%s\n' "$C_BOLD" "$C_RESET"
    { printf '%s\n' "$plan" | grep -E '^ ?(\+-|\| ?\+)' | head -4 | sed 's/\[Cost.*//; s/^/        /'; } || true
    if printf '%s\n' "$plan" | grep -q 'DML MERGE'; then
        warn "the plan contains a 'DML MERGE' operator: this MERGE is NOT optimized"
    else
        ok "optimized MERGE: DML DELETE + DML INSERT, no 'DML MERGE' operator"
    fi
}

# ---------------------------------------------------------------- one run
run_once() {
    local t copy_ms rows rej
    chapter "PHASE $PHASE · run $RUN of $RUNS · $P MERGEs at a time"
    explain_step "$PHASE.0" "Reset the ${#TABLES[@]} tables, recreate the staging table  (not timed)" \
        "COPY_TABLE makes each table a catalog-only copy of <table>_base (milliseconds, no extra disk); ${STAGE} is dropped and created empty." \
        "Every run and every parallel setting starts from the same data." \
        "$(m_reset customer)
$(stage_create)"
    { for t in "${TABLES[@]}"; do m_reset "$t"; done; stage_create; } | timed "reset + staging table — not timed" "$RUN_LOG/r${RUN}_p${P}_reset"

    if [[ $PHASE == 2 ]]; then
        explain_step 2.1 "ONE COPY: parse all $N_FILES files once into the flat staging table" \
            "FJSONPARSER flattens every record; FILLER columns put each table's fields into that table's columns of stg_flat (customer.name.first → customer__first_name) and the header's file name into the column file, the partition key. A record fills only its own table's columns; the others stay NULL, which costs almost nothing in columnar storage." \
            "The JSON is parsed exactly once, by one COPY that reads all files in parallel. Ten COPYs over the same mixed files would parse everything ten times." \
            "$(stage_copy "vload_…_phase2")"
    else
        explain_step 3.1 "ONE COPY: parse all $N_FILES files once into the document staging table" \
            "Each document (one ADABAS transaction) becomes one row of stg_doc. FJSONPARSER(flatten_arrays=true) flattens the nested arrays: the 2nd customer record of a document arrives as \"customer.1.rec.name.first\". stg_doc has $MAX_OCCURS slots of columns per table, named exactly like those keys, so the COPY needs no column list." \
            "The JSON is parsed exactly once, into plain columns. A document carries up to $MAX_OCCURS records per file (like the maximum occurrences of an ADABAS periodic group); the check after the MERGEs would catch any record beyond that." \
            "$(stage_copy "vload_…_phase3")"
    fi
    stage_copy "vload_${SESSION//-/_}_r${RUN}_p${P}" | timed "COPY: parse all JSON into ${STAGE}" "$RUN_LOG/r${RUN}_p${P}_copy"
    copy_ms=$LAST_MS rows=$(tail -n 1 <<< "$LAST_OUT")
    rej=$(vsql_query "SELECT COUNT(*) FROM ${SCHEMA}.${STAGE}_rejects" 2>/dev/null || echo 0)
    (( rej == 0 )) || die "$(fmt_num "$rej") JSON lines rejected — see ${SCHEMA}.${STAGE}_rejects"
    info "$(fmt_num "$rows") JSON $( [[ $PHASE == 2 ]] && echo "records" || echo "documents") parsed, $(rate "$rows" "$copy_ms") per second"
    tsv copy "_all" "$rows" "$copy_ms"

    if [[ $PHASE == 2 ]]; then
        explain_step 2.2 "${#TABLES[@]} optimized MERGEs straight from the staging table, $P at a time" \
            "Each MERGE reads its table's rows with SELECT … FROM stg_flat WHERE file = '<table>' (partition pruning: it only reads its own partition) and applies the updates and inserts." \
            "No intermediate tables. The tables are independent, so the MERGEs run side by side. The source is a query, so Vertica joins with a hash join instead of a presorted merge join, and the plan stays the optimized DELETE + INSERT." \
            "$(m_merge customer "$(source_sql customer)")"
    else
        explain_step 3.2 "${#TABLES[@]} optimized MERGEs straight from the staging table, $P at a time" \
            "Each MERGE reads its table's records as the UNION ALL of the table's slots (\"customer.0.*\", \"customer.1.*\"), skipping empty slots, and applies the updates and inserts." \
            "The document's arrays become rows without a single extra parse or intermediate table. All the work is columnar: each MERGE reads only its own table's columns of stg_doc." \
            "$(m_merge customer "$(source_sql customer)")"
    fi
    merge_all
    [[ ${QUIET:-0} == 1 ]] || show_plan
    tsv merge_wall "_all" "$TOTAL_CHANGES" "$LAST_MS"
    tsv total "_all" "$TOTAL_CHANGES" $(( copy_ms + LAST_MS ))
    echo
    printf '  %s%s⏱  PHASE %s · %s JSON changes into %d tables:  %s s%s   = COPY %s s + MERGEs %s s   (%s changes/s)\n' \
        "$C_BOLD" "$C_GREEN" "$PHASE" "$(fmt_num "$TOTAL_CHANGES")" "${#TABLES[@]}" "$(secs $(( copy_ms + LAST_MS )))" "$C_RESET" \
        "$(secs "$copy_ms")" "$(secs "$LAST_MS")" "$(rate "$TOTAL_CHANGES" $(( copy_ms + LAST_MS )))"
    if (( VALIDATE )); then check_tables; fi
}

summary() {
    local out="$REPORT_DIR/phase${PHASE}_summary.md"
    chapter "PHASE $PHASE RESULTS  $(fmt_num "$TOTAL_CHANGES") JSON changes → ${#TABLES[@]} tables" \
            "$SHAPE · $RUNS run(s) per setting · averages"
    awk -F'\t' -v s="$SESSION" -v tot="$TOTAL_CHANGES" -v B="$C_BOLD" -v Z="$C_RESET" -v md="$out.tmp" '
        $1 != s { next }
        $5 == "_all" { v[$3, $4] += $7; n[$3, $4]++; if (!($3 in seen)) { seen[$3] = 1; order[++np] = $3 } next }
        $4 == "merge" { m[$3, $5] += $7; nm[$3, $5]++; if (!($5 in ts)) { ts[$5] = 1; tab[++nt] = $5 } }
        function avg(p, k) { return v[p, k] / n[p, k] / 1000 }
        END {
            seq = (n["1", "total"] ? avg("1", "total") : 0)
            printf "  %s%-22s %12s %16s %12s %14s %18s%s\n", B, "MERGEs at a time", "COPY", "10 MERGEs", "TOTAL", "changes/s", (seq ? "vs one at a time" : ""), Z
            print "| MERGEs at a time | COPY (s) | 10 MERGEs (s) | **Total (s)** | Changes/s |" (seq ? " vs one at a time |" : "") > md
            print "|---|---:|---:|---:|---:|" (seq ? "---:|" : "") > md
            for (i = 1; i <= np; i++) {
                p = order[i]; c = avg(p, "copy"); w = avg(p, "merge_wall"); t = avg(p, "total")
                printf "  %-22s %10.2f s %14.2f s %s%10.2f s%s %14s %18s\n", p, c, w, B, t, Z, sprintf("%.0fK", tot / t / 1000), (seq ? sprintf("%.1fx", seq / t) : "")
                printf "| %s | %.2f | %.2f | **%.2f** | %.0fK |%s\n", p, c, w, t, tot / t / 1000, (seq ? sprintf(" %.1fx |", seq / t) : "") > md
            }
            printf "\n  %sMERGE time per table (s)%s\n  %-12s", B, Z, "table"
            hdr = "| Table |"; sep = "|---|"
            for (i = 1; i <= np; i++) { printf " %12s", order[i] " at a time"; hdr = hdr " " order[i] " at a time |"; sep = sep "---:|" }
            print ""; print "" > md; print hdr > md; print sep > md
            for (j = 1; j <= nt; j++) {
                t = tab[j]; printf "  %-12s", t; row = "| " t " |"
                for (i = 1; i <= np; i++) { p = order[i]; x = m[p, t] / nm[p, t] / 1000; printf " %12.2f", x; row = row sprintf(" %.2f |", x) }
                print ""; print row > md
            }
        }' "$RESULTS_TSV"
    {
        printf '# Phase %s results\n\n%s JSON changes (%s per table) into %d tables: %s.\nSession %s, %s run(s) per setting, averages.\n\n' \
            "$PHASE" "$(fmt_num "$TOTAL_CHANGES")" "$(fmt_num "$MULTI_CHANGE_ROWS")" "${#TABLES[@]}" "$SHAPE" "$SESSION" "$RUNS"
        cat "$out.tmp"
    } > "$out"
    rm -f "$out.tmp"
    echo
    info "report: ${out#"$ROOT_DIR"/}   all runs: ${RESULTS_TSV#"$ROOT_DIR"/}   SQL logs: ${RUN_LOG#"$ROOT_DIR"/}"
}

# ---------------------------------------------------------------- main
chapter "$PHASE_TITLE" \
        "$(fmt_num "$TOTAL_CHANGES") changes ($(fmt_num "$MULTI_CHANGE_ROWS") per table, updates + inserts) · ${FACT_TABLE} $(count_label "$BASE_ROWS") rows + $(( ${#TABLES[@]} - 1 )) tables × $(count_label "$MULTI_ROWS") · $SHAPE"
preflight
FIRST=1
for (( RUN = 1; RUN <= RUNS; RUN++ )); do
    for P in "${PLIST[@]}"; do
        QUIET=$(( FIRST == 1 ? 0 : 1 ))
        run_once
        FIRST=0
    done
done
summary
