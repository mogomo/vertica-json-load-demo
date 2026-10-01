#!/usr/bin/env bash
# =============================================================================
#  apply_multi.sh — 10 tables, 1 million JSON changes each, in parallel
# =============================================================================
#  The 1-billion-row fact table and the 9 tables of generate_multi.sh each get
#  1 million multi-level JSON changes (updates + inserts). Every table runs its
#  own pipeline:
#      COPY its JSON files into a delta table  →  optimized MERGE into the table
#  and the pipelines run side by side, at most --parallel at a time.
#
#  Optimized MERGE is used because it was the fastest method of the first phase
#  (with the insert-only upsert) and needs half the storage of the upsert.
#
#  Before each run the 10 tables are reset with COPY_TABLE (not timed). After
#  it, each table is checked: row count, and the checksum of the changed rows
#  must equal the checksum of the JSON rows (not timed).
#
#  --parallel takes a list: 10,5,1 runs every setting (1 = one table after the
#  other, the baseline for the speed-up).
#
#  Usage: ./apply_multi.sh [--parallel 10] [--parallel 10,5,1] [--runs N]
#                          [--pause] [--no-check] [--no-color]
# =============================================================================
set -o errexit -o nounset -o pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
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
        -h|--help)  sed -n '2,23p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "unknown option '$1' (see ./apply_multi.sh --help)" ;;
    esac
    shift
done
[[ $RUNS =~ ^[1-9][0-9]*$ ]] || die "--runs must be a positive number"

load_config
IFS=, read -r -a PLIST <<< "${PARALLEL_LIST:-$PARALLEL}"
for p in "${PLIST[@]}"; do [[ $p =~ ^[1-9][0-9]*$ ]] || die "--parallel takes numbers, e.g. 10 or 10,5"; done
DEMO_DIR=$(cd "$DEMO_DIR" 2>/dev/null && pwd) || die "run ./generate.sh and ./generate_multi.sh first"
CHANGES_DIR="$DEMO_DIR/changes"; MULTI_DIR="$DEMO_DIR/multi"
[[ -f $DEMO_DIR/manifest.env && -f $MULTI_DIR/manifest.env ]] || die "run ./generate.sh and ./generate_multi.sh first"
# shellcheck source=/dev/null
. "$DEMO_DIR/manifest.env"      # BASE_ROWS, CHANGE_ROWS, N_INS … of the fact table
FACT_CHANGES=$CHANGE_ROWS FACT_INS=$N_INS
# shellcheck source=/dev/null
. "$MULTI_DIR/manifest.env"     # MULTI_ROWS, MULTI_CHANGE_ROWS, MULTI_N_INS, MULTI_TABLES
FACT_TABLE=$(defs_fact_table)
TABLES=("$FACT_TABLE" $MULTI_TABLES)

changes_of() { if [[ $1 == "$FACT_TABLE" ]]; then echo "$FACT_CHANGES"; else echo "$MULTI_CHANGE_ROWS"; fi; }
inserts_of() { if [[ $1 == "$FACT_TABLE" ]]; then echo "$FACT_INS"; else echo "$MULTI_N_INS"; fi; }
TOTAL_CHANGES=0
for t in "${TABLES[@]}"; do TOTAL_CHANGES=$(( TOTAL_CHANGES + $(changes_of "$t") )); done

SESSION=$(date +%Y%m%d-%H%M%S)
RUN_LOG="$LOG_DIR/apply_multi_$SESSION"
mkdir -p "$RUN_LOG" "$REPORT_DIR"
RESULTS_TSV="$REPORT_DIR/multi_results.tsv"
[[ -f $RESULTS_TSV ]] || printf 'session\trun\tparallel\ttable\ttable_rows\tchanges\tparse_load_ms\tmerge_ms\ttotal_ms\tok\n' > "$RESULTS_TSV"

preflight() {
    vsql_check
    local t n
    for t in "${TABLES[@]}"; do
        table_exists "${t}_base" || die "${SCHEMA}.${t}_base missing — run ./generate.sh and ./generate_multi.sh"
        n=$(vsql_query "SELECT COUNT(*) FROM ${SCHEMA}.${t}_base")
        [[ $n == "$(table_rows "$t")" ]] || die "${SCHEMA}.${t}_base has $(fmt_num "$n") rows, $(fmt_num "$(table_rows "$t")") expected"
        compgen -G "$(table_json_dir "$t")/*.json" >/dev/null || die "no JSON files for $t"
    done
}

# ---------------------------------------------------------------- one table
# Runs in the background: COPY, then MERGE, each timed. Writes
# "rows copy_ms merge_ms end_ms rc" to $TMP_DIR/<table>.
pipeline() {
    local t=$1 s0 s1 s2 rows rc=0
    s0=$(now_ms)
    if rows=$(m_copy "$t" "vload_${SESSION//-/_}_r${RUN}_p${P}_$t" | vsql_exec "$RUN_LOG/r${RUN}_p${P}_${t}_copy" | tail -n 1); then
        s1=$(now_ms)
        m_merge "$t" | vsql_exec "$RUN_LOG/r${RUN}_p${P}_${t}_merge" >/dev/null || rc=2
    else
        s1=$(now_ms); rc=1
    fi
    s2=$(now_ms)
    printf '%s %s %s %s %s\n' "${rows:-0}" $((s1 - s0)) $((s2 - s1)) "$s2" "$rc" > "$TMP_DIR/$t"
}

run_parallel_set() {
    local t start dispatcher spin='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏' i=0 done_n rows="" el end wall sum=0 fails=0
    TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/vload.XXXXXX")
    start=$(now_ms)
    (
        for t in "${TABLES[@]}"; do
            pipeline "$t" &
            while (( $(jobs -rp | wc -l) >= P )); do wait -n || true; done
        done
        wait
    ) &
    dispatcher=$!
    while kill -0 "$dispatcher" 2>/dev/null; do
        if (( IS_TTY )); then
            el=$(( $(now_ms) - start ))
            done_n=$(find "$TMP_DIR" -type f | wc -l)
            if (( i % 4 == 0 )); then
                rows=$(vsql_query "SELECT COALESCE(SUM(accepted_row_count), 0) FROM v_monitor.load_streams WHERE stream_name LIKE 'vload_${SESSION//-/_}_r${RUN}_p${P}_%'" 2>/dev/null || echo "")
            fi
            printf '\r  %s%s %6ss  %d/%d tables done  |  JSON rows parsed + loaded: %s%s' "$C_CYAN" "${spin:i%10:1}" "$(secs "$el")" "$done_n" "${#TABLES[@]}" "$(fmt_num "${rows:-0}")" "$C_RESET"
            i=$((i + 1))
        fi
        sleep 0.25
    done
    wait "$dispatcher" || true
    (( IS_TTY )) && printf '\r\033[K'

    end=$start
    printf '  %s%-12s %15s %10s %12s %10s %10s%s\n' "$C_DIM" "table" "table rows" "changes" "parse+load" "MERGE" "total" "$C_RESET"
    for t in "${TABLES[@]}"; do
        local r=0 c=0 m=0 e=$start rc=1
        read -r r c m e rc < "$TMP_DIR/$t" 2>/dev/null || true
        (( e > end )) && end=$e
        sum=$(( sum + c + m ))
        if (( rc == 0 )) && [[ $r == "$(changes_of "$t")" ]]; then
            printf '  %-12s %15s %10s %10s s %8s s %8s s\n' "$t" "$(fmt_num "$(table_rows "$t")")" "$(fmt_num "$r")" "$(secs "$c")" "$(secs "$m")" "$(secs $(( c + m )))"
        else
            printf '  %s%-12s FAILED (rows loaded: %s) — see %s%s\n' "$C_RED" "$t" "$r" "${RUN_LOG#"$ROOT_DIR"/}/r${RUN}_p${P}_${t}_*.err" "$C_RESET"
            fails=$((fails + 1)); rc=1
        fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$SESSION" "$RUN" "$P" "$t" "$(table_rows "$t")" "$r" "$c" "$m" $(( c + m )) $(( rc == 0 )) >> "$RESULTS_TSV"
    done
    wall=$(( end - start ))
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$SESSION" "$RUN" "$P" "_wall" "" "$TOTAL_CHANGES" "" "" "$wall" $(( fails == 0 )) >> "$RESULTS_TSV"
    rm -rf "$TMP_DIR"
    (( fails == 0 )) || die "$fails table(s) failed"
    echo
    printf '  %s%s⏱  %d tables · %s JSON changes · %d at a time:  %s s wall-clock%s   (%s rows/s; the per-table times add up to %s s)\n' \
        "$C_BOLD" "$C_GREEN" "${#TABLES[@]}" "$(fmt_num "$TOTAL_CHANGES")" "$P" "$(secs "$wall")" "$C_RESET" "$(rate "$TOTAL_CHANGES" "$wall")" "$(secs "$sum")"
}

check_tables() {
    local t out cnt b1 h1 h2 bad=0
    JOBS=()
    TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/vload.XXXXXX")
    for t in "${TABLES[@]}"; do
        ( m_check "$t" | vsql_exec "$RUN_LOG/r${RUN}_p${P}_${t}_check" > "$TMP_DIR/$t" ) &
    done
    wait
    for t in "${TABLES[@]}"; do
        IFS='|' read -r cnt b1 h1 h2 < "$TMP_DIR/$t"
        if [[ $cnt != $(( $(table_rows "$t") + $(inserts_of "$t") )) || $b1 != "$(changes_of "$t")" || $h1 != "$h2" ]]; then
            warn "$t: rows $cnt, changed rows $b1, checksum $h1 vs JSON $h2"; bad=1
        fi
    done
    rm -rf "$TMP_DIR"
    (( bad == 0 )) || die "check failed"
    ok "check: every table has its rows + the inserts, and its changed rows match the JSON rows exactly (count + checksum of all columns)"
}

drop_deltas() {
    local t
    for t in "${TABLES[@]}"; do echo "DROP TABLE IF EXISTS ${SCHEMA}.${t}_delta;"; done | vsql_exec "$RUN_LOG/r${RUN}_p${P}_drop" >/dev/null
}

summary() {
    local out="$REPORT_DIR/multi_summary.md"
    chapter "RESULTS  ${#TABLES[@]} tables · $(fmt_num "$TOTAL_CHANGES") JSON changes · parse + load + optimized MERGE" \
            "$RUNS run(s) per setting · wall-clock = first COPY starts → last MERGE commits"
    awk -F'\t' -v s="$SESSION" -v tot="$TOTAL_CHANGES" -v B="$C_BOLD" -v Z="$C_RESET" -v md="$out.tmp" '
        function fmt(x,   r) { x = sprintf("%d", x); r = ""; while (length(x) > 3) { r = "," substr(x, length(x) - 2) r; x = substr(x, 1, length(x) - 3) } return x r }
        $1 != s { next }
        $4 == "_wall" { w[$3] += $9; nw[$3]++; if (!($3 in seen)) { seen[$3] = 1; order[++np] = $3 } next }
        { c[$3, $4] += $7; m[$3, $4] += $8; n[$3, $4]++; if (!(($4) in tseen)) { tseen[$4] = 1; tab[++nt] = $4; rows[$4] = fmt($5); ch[$4] = fmt($6) } }
        END {
            seq = ("1" in nw) ? w["1"] / nw["1"] / 1000 : 0
            printf "  %s%-26s %14s %12s %22s%s\n", B, "setting", "wall-clock", "rows/s", (seq ? "vs one at a time" : ""), Z
            print "| Setting | Wall-clock (s) | JSON rows/s |" (seq ? " vs one at a time |" : "") > md
            print "|---|---:|---:|" (seq ? "---:|" : "") > md
            for (i = 1; i <= np; i++) {
                p = order[i]; a = w[p] / nw[p] / 1000
                printf "  %-26s %12.2f s %12s %21s\n", p " at a time", a, sprintf("%.0fK", tot / a / 1000), (seq ? sprintf("%.1fx", seq / a) : "")
                printf "| %s at a time | **%.2f** | %.0fK |%s\n", p, a, tot / a / 1000, (seq ? sprintf(" %.1fx |", seq / a) : "") > md
            }
            print "" > md
            for (i = 1; i <= np; i++) {
                p = order[i]
                printf "\n  %s%d at a time — per table (average)%s\n", B, p, Z
                printf "  %-12s %15s %10s %12s %10s %10s\n", "table", "table rows", "changes", "parse+load", "MERGE", "total"
                print "| " p " at a time: table | Table rows | Changes | Parse + load (s) | MERGE (s) | Total (s) |" > md
                print "|---|---:|---:|---:|---:|---:|" > md
                for (j = 1; j <= nt; j++) {
                    t = tab[j]; k = n[p, t]
                    printf "  %-12s %15s %10s %10.2f s %8.2f s %8.2f s\n", t, rows[t], ch[t], c[p, t] / k / 1000, m[p, t] / k / 1000, (c[p, t] + m[p, t]) / k / 1000
                    printf "| %s | %s | %s | %.2f | %.2f | %.2f |\n", t, rows[t], ch[t], c[p, t] / k / 1000, m[p, t] / k / 1000, (c[p, t] + m[p, t]) / k / 1000 > md
                }
                print "" > md
            }
        }' "$RESULTS_TSV"
    {
        printf '# Multi-table results\n\n%s JSON changes into %d tables, session %s, %s run(s) per setting (averages).\n\n' "$(fmt_num "$TOTAL_CHANGES")" "${#TABLES[@]}" "$SESSION" "$RUNS"
        cat "$out.tmp"
    } > "$out"
    rm -f "$out.tmp"
    echo
    info "report: ${out#"$ROOT_DIR"/}   all runs: ${RESULTS_TSV#"$ROOT_DIR"/}   SQL logs: ${RUN_LOG#"$ROOT_DIR"/}"
}

# ---------------------------------------------------------------- main
chapter "MULTI-TABLE  ${#TABLES[@]} tables · $(fmt_num "$TOTAL_CHANGES") multi-level JSON changes · parallel parse + optimized MERGE" \
        "${FACT_TABLE} ($(fmt_num "$BASE_ROWS") rows) + $(( ${#TABLES[@]} - 1 )) tables of $(fmt_num "$MULTI_ROWS") rows · $(fmt_num "$MULTI_CHANGE_ROWS") changes per table (updates + inserts)"
preflight
FIRST=1
for (( RUN = 1; RUN <= RUNS; RUN++ )); do
    for P in "${PLIST[@]}"; do
        QUIET=$(( FIRST == 1 ? 0 : 1 ))
        chapter "${#TABLES[@]} tables, $P at a time   [run $RUN of $RUNS]"
        explain_step "R" "Reset the ${#TABLES[@]} tables from their pristine copies  (not timed)" \
            "COPY_TABLE makes each table a catalog-only copy of <table>_base: milliseconds, no extra disk." \
            "Every run, and every parallel setting, starts from the same data." \
            "$(m_reset customer)"
        for t in "${TABLES[@]}"; do m_reset "$t"; done | timed "reset ${#TABLES[@]} tables (COPY_TABLE) — not timed" "$RUN_LOG/r${RUN}_p${P}_reset"
        explain_step "P" "Parse + load + MERGE: ${#TABLES[@]} pipelines, $P at a time" \
            "Each table has its own pipeline: one COPY parses the table's multi-level JSON files into a delta table (FJSONPARSER flattens groups, multiple-value fields and periodic groups: \"rec.address.1.city\" → addr2_city), then one optimized MERGE applies the updates and inserts. Up to $P pipelines run at the same time." \
            "The tables are independent, so their loads can overlap: while one pipeline waits for its MERGE, others parse JSON. The wall-clock shows how much of the summed work the machine absorbs in parallel." \
            "$(m_copy customer "vload_…_customer")

$(m_merge customer)"
        run_parallel_set
        if (( VALIDATE )); then check_tables; fi
        drop_deltas
        FIRST=0
    done
done
summary
