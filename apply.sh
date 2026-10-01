#!/usr/bin/env bash
# =============================================================================
#  apply.sh — apply the JSON change records to the fact table, three ways
# =============================================================================
#  Every method starts from the same 1-billion-row table (reset with
#  COPY_TABLE, not timed) and loads the same JSON files. The timer covers the
#  whole job: parsing the JSON (COPY) plus applying the changes.
#
#    1 upsert   insert-only upsert: COPY into a journal; a Top-K Live Aggregate
#               Projection serves the newest version of every row
#    2 swap     COPY into a staging table, rebuild only the touched partitions,
#               SWAP_PARTITIONS_BETWEEN_TABLES into the fact table
#    3 merge    COPY into a delta table, then one optimized MERGE
#
#  After each method the current data is checked (row count + checksum): all
#  methods must produce exactly the same table. By default the check covers
#  every row from the oldest partition the changes can touch (the older
#  partitions are the untouched storage shared with txn_base); --full-check
#  checks all rows (slower: the Top-K view then reads the whole LAP).
#  Safe to run again and again.
#
#  Usage: ./apply.sh [--runs N] [--method upsert,swap,merge] [--pause]
#                    [--full-check | --no-check] [--no-color]
# =============================================================================
set -o errexit -o nounset -o pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=/dev/null
[[ -f $ROOT_DIR/vload.env ]] && . "$ROOT_DIR/vload.env"
# shellcheck source=lib/common.sh
. "$ROOT_DIR/lib/common.sh"
# shellcheck source=lib/sql.sh
. "$ROOT_DIR/lib/sql.sh"

ALL_METHODS=(upsert swap merge)
METHODS=("${ALL_METHODS[@]}") RUNS=1 VALIDATE=1 FULL_CHECK=0
while (( $# )); do
    case $1 in
        --runs)        RUNS=${2:?--runs needs a value}; shift ;;
        --method|--methods) IFS=, read -r -a METHODS <<< "${2:?--method needs a value}"; shift ;;
        --pause)       PAUSE=1 ;;
        --full-check)  FULL_CHECK=1 ;;
        --no-check)    VALIDATE=0 ;;
        --no-color)    no_color ;;
        -h|--help)     sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "unknown option '$1' (see ./apply.sh --help)" ;;
    esac
    shift
done
[[ $RUNS =~ ^[1-9][0-9]*$ ]] || die "--runs must be a positive number"
for m in "${METHODS[@]}"; do [[ " ${ALL_METHODS[*]} " == *" $m "* ]] || die "unknown method '$m' (upsert, swap, merge)"; done

load_config
DEMO_DIR=$(cd "$DEMO_DIR" 2>/dev/null && pwd) || die "no demo data — run ./generate.sh first"
CHANGES_DIR="$DEMO_DIR/changes"
[[ -f $DEMO_DIR/manifest.env ]] || die "no demo data — run ./generate.sh first"
# shellcheck source=/dev/null
. "$DEMO_DIR/manifest.env"          # BASE_ROWS, CHANGE_ROWS, N_UPD, N_DEL, N_INS … of the data set
compgen -G "$CHANGES_DIR/*.json" >/dev/null || die "no JSON files in $CHANGES_DIR — run ./generate.sh"

SESSION=$(date +%Y%m%d-%H%M%S)
RUN_LOG="$LOG_DIR/apply_$SESSION"
mkdir -p "$RUN_LOG" "$REPORT_DIR"
RESULTS_TSV="$REPORT_DIR/results.tsv"
[[ -f $RESULTS_TSV ]] || printf 'session\trun\tmethod\tbase_rows\tchange_rows\tparse_load_ms\tapply_ms\ttotal_ms\tlive_rows\tchecksum\tdelete_vector_rows\n' > "$RESULTS_TSV"

method_title() {
    case $1 in
        upsert) echo "METHOD 1 · Insert-only upsert: journal + Top-K Live Aggregate Projection" ;;
        swap)   echo "METHOD 2 · Staging table + partition COPY / SWAP" ;;
        merge)  echo "METHOD 3 · Optimized MERGE" ;;
    esac
}
method_short() {
    case $1 in
        upsert) echo "1 Upsert (journal + Top-K LAP)" ;;
        swap)   echo "2 Staging + partition SWAP" ;;
        merge)  echo "3 Optimized MERGE" ;;
    esac
}

# ---------------------------------------------------------------- checks
preflight() {
    vsql_check
    table_exists txn_base && table_exists txn_jrn_base || die "${SCHEMA}.txn_base / txn_jrn_base missing — run ./generate.sh"
    local n
    n=$(vsql_query "SELECT COUNT(*) FROM ${SCHEMA}.txn_base")
    [[ $n == "$BASE_ROWS" ]] || die "${SCHEMA}.txn_base has $(fmt_num "$n") rows but the JSON files were made for $(fmt_num "$BASE_ROWS") — run ./generate.sh"
    CHECK_FROM=""
    (( FULL_CHECK )) || CHECK_FROM=$(vsql_query "$(sql_check_from)")
}

check_load() {   # <rows loaded> <method>
    local loaded=$1 rej=0
    if table_exists "txn_rejects_$2"; then rej=$(vsql_query "SELECT COUNT(*) FROM ${SCHEMA}.txn_rejects_$2"); fi
    (( rej == 0 )) || die "$(fmt_num "$rej") JSON records were rejected — see ${SCHEMA}.txn_rejects_$2"
    [[ $loaded == "$CHANGE_ROWS" ]] || die "COPY loaded $(fmt_num "$loaded") rows, $(fmt_num "$CHANGE_ROWS") expected"
}

reset_method() {   # untimed: start from the pristine table
    local src=txn_base
    [[ $1 == upsert ]] && src=txn_jrn_base
    explain_step "$2.0" "Reset txn_$1 from $src  (not timed)" \
        "COPY_TABLE creates txn_$1 as a copy of the pristine $(fmt_num "$BASE_ROWS")-row table. It is a catalog operation: the copy shares the source's storage, so it takes milliseconds and no disk space." \
        "Every method, on every run, starts from exactly the same data. That makes the timings comparable and the demo repeatable." \
        "$(sql_reset "$1")"
    sql_reset "$1" | timed "reset (COPY_TABLE) — not timed" "$RUN_LOG/r${RUN}_$1_reset"
}

# ---------------------------------------------------------------- method 1
method_upsert() {
    explain_step 1.1 "COPY the JSON changes straight into the journal — that is the whole upsert" \
        "FJSONPARSER parses the $(find "$CHANGES_DIR" -name '*.json' | wc -l | tr -d ' ') files in parallel; FILLER columns map the flattened JSON keys onto the columns. Updates arrive as new versions of an ISN, inserts as new ISNs: both are simply appended. The Top-K LAP is maintained during the load." \
        "No UPDATE, no DELETE, no join, no delete vectors: the cost of an upsert is the cost of a load. Readers use txn_upsert_current, which reads the LAP: LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC) returns the newest version of each row. The older versions stay as history." \
        "$(sql_copy txn_upsert "vload_${SESSION//-/_}_r${RUN}_upsert" upsert)"
    sql_copy txn_upsert "vload_${SESSION//-/_}_r${RUN}_upsert" upsert | timed "parse JSON + COPY into journal (+ LAP)" "$RUN_LOG/r${RUN}_upsert_copy"
    T_LOAD=$LAST_MS; T_APPLY=0
    check_load "$LAST_OUT" upsert
}

upsert_showcase() {   # run 1 only: version history + the LAP rewrite
    local isn=$(( BASE_ROWS - BASE_ROWS * HOT_PCT / 100 + 1 ))   # the first updated ISN
    explain_step 1.2 "Version history of ISN $isn  (not timed)" \
        "The journal keeps every version; the current view returns only the newest one, and EXPLAIN shows that it is answered from the Top-K projection (Rewritten TOPK)." \
        "Data versioning for free: when and how every row changed, for audit and debugging." \
        "SELECT isn, op_code, change_ts, batch_id, amount FROM ${SCHEMA}.txn_upsert WHERE isn = $isn ORDER BY change_ts;
SELECT isn, op_code, change_ts, batch_id, amount FROM ${SCHEMA}.txn_upsert_current WHERE isn = $isn;"
    "$VSQL" -X -c "SELECT isn, op_code, change_ts, batch_id, amount FROM ${SCHEMA}.txn_upsert WHERE isn = $isn ORDER BY change_ts;" | sed 's/^/        /'
    "$VSQL" -X -c "SELECT isn, op_code, change_ts, batch_id, amount FROM ${SCHEMA}.txn_upsert_current WHERE isn = $isn;" | sed 's/^/        /'
    vsql_query "EXPLAIN SELECT * FROM ${SCHEMA}.txn_upsert_current WHERE isn = $isn" | grep -E 'TopK Optimized|Rewritten TOPK' | sed -E 's/^[ |+>-]*/        /' | head -2
}

# ---------------------------------------------------------------- method 2
swap_slice() {   # <i>: the i-th of REBUILD_SESSIONS equal ISN slices
    local n=$(( SLICE_HI - SLICE_LO + 1 )) i=$1
    sql_swap_rebuild $(( SLICE_LO + n * (i - 1) / REBUILD_SESSIONS )) $(( SLICE_LO + n * i / REBUILD_SESSIONS - 1 )) "${TOUCHED[@]}"
}

method_swap() {
    local stream="vload_${SESSION//-/_}_r${RUN}_swap" touched
    explain_step 2.1 "COPY the JSON changes into a staging (delta) table" \
        "Parses the JSON files into txn_swap_delta, created LIKE the fact table INCLUDING PROJECTIONS." \
        "The fact table is not touched yet. Same segmentation and sort order as the fact table, so the next step joins them locally without reshuffling data." \
        "$(sql_swap_delta "$stream")"
    sql_swap_delta "$stream" | timed "parse JSON + COPY into staging table" "$RUN_LOG/r${RUN}_swap_copy"
    T_LOAD=$LAST_MS
    check_load "$LAST_OUT" swap

    explain_step 2.2 "Which partitions do the changes touch?" \
        "One query over the staging table returns the monthly partitions that receive changes." \
        "Only these partitions are rebuilt; all others stay as they are." \
        "$(sql_swap_touched)"
    sql_swap_touched | timed "find the touched partitions" "$RUN_LOG/r${RUN}_swap_touched"
    T_APPLY=$LAST_MS
    mapfile -t touched <<< "$LAST_OUT"
    [[ -n ${touched[0]:-} ]] || die "the staging table is empty"
    info "touched partitions: ${touched[*]}"

    explain_step 2.3 "Prepare the stage table" \
        "Creates txn_swap_stage LIKE the fact table and finds the ISN range of the rows to rebuild (the touched partitions plus the new rows), to split the rebuild into $REBUILD_SESSIONS slices." \
        "Several sessions can INSERT into the same table at the same time, so the rebuild can use the whole machine." \
        "$(sql_swap_prepare "${touched[@]}")"
    sql_swap_prepare "${touched[@]}" | timed "prepare stage table + ISN range" "$RUN_LOG/r${RUN}_swap_prepare"
    T_APPLY=$(( T_APPLY + LAST_MS ))
    IFS='|' read -r SLICE_LO SLICE_HI <<< "$(tail -n 1 <<< "$LAST_OUT")"
    TOUCHED=("${touched[@]}")

    explain_step 2.4 "Rebuild only the touched partitions: $REBUILD_SESSIONS slices in parallel" \
        "For the ${#touched[@]} touched partitions, each session writes one ISN slice of the unchanged fact rows plus the new row images into txn_swap_stage. Deleted rows (if any) are left out. The fact table is still untouched." \
        "Updates become sequential INSERT … SELECTs: no UPDATE, no delete vectors, nothing to purge later. The cost follows the size of the touched partitions, not the size of the table, which suits changes that concentrate in recent data." \
        "$(sql_swap_rebuild "$SLICE_LO" "$(( SLICE_LO + (SLICE_HI - SLICE_LO + 1) / REBUILD_SESSIONS - 1 ))" "${touched[@]}")
-- … and $(( REBUILD_SESSIONS - 1 )) more slices up to ISN $SLICE_HI, in parallel sessions"
    timed_parallel "rebuild ${#touched[@]} partitions" "$RUN_LOG/r${RUN}_swap_rebuild" "$REBUILD_SESSIONS" swap_slice
    T_APPLY=$(( T_APPLY + LAST_MS ))

    explain_step 2.5 "Atomic SWAP_PARTITIONS_BETWEEN_TABLES stage ⇄ fact" \
        "Exchanges the rebuilt partitions with the fact table's partitions in one transaction, then drops the stage and delta tables." \
        "A catalog operation: instant and atomic. Readers see the old partitions or the new ones, never a mix." \
        "$(sql_swap_swap "${touched[@]}")"
    sql_swap_swap "${touched[@]}" | timed "swap partitions" "$RUN_LOG/r${RUN}_swap_swap"
    T_APPLY=$(( T_APPLY + LAST_MS ))
}

# ---------------------------------------------------------------- method 3
method_merge() {
    local stream="vload_${SESSION//-/_}_r${RUN}_merge"
    explain_step 3.1 "COPY the JSON changes into a delta table" \
        "Parses the JSON files into txn_merge_delta, created LIKE the target INCLUDING PROJECTIONS." \
        "Same segmentation (HASH(isn)) and sort order (isn) as the target, so the MERGE join is a presorted merge join: no sort, no hash table, no network shuffle." \
        "$(sql_merge_delta "$stream")"
    sql_merge_delta "$stream" | timed "parse JSON + COPY into delta table" "$RUN_LOG/r${RUN}_merge_copy"
    T_LOAD=$LAST_MS
    check_load "$LAST_OUT" merge

    explain_step 3.2 "Optimized MERGE delta → target" \
        "One MERGE updates the existing ISNs and inserts the new ones. UPDATE SET and INSERT list every column with the same source values; the ISN is a declared primary key. (Deletes, if any, are kept as op_code 'D' rows that the current view hides.)" \
        "These three rules let Vertica run the MERGE as a DELETE + INSERT over a merge join instead of the generic MERGE operator. Trade-off: each updated row leaves a delete vector behind, which the Tuple Mover purges later." \
        "$(sql_merge_stmt)"
    if [[ ${QUIET:-0} != 1 ]]; then
        local plan
        plan=$(sql_merge_stmt | sed 's/^MERGE/EXPLAIN MERGE/' | "$VSQL" -X -A -t -q 2>/dev/null || true)
        printf '%s  EXPLAIN (access path):%s\n' "$C_BOLD" "$C_RESET"
        printf '%s\n' "$plan" | grep -E '^ ?(\+-|\| ?\+)' | grep -v -- '-->.*STORAGE ACCESS for s ' | head -8 | sed 's/^/        /'
        if printf '%s\n' "$plan" | grep -q 'DML MERGE'; then
            warn "the plan contains a 'DML MERGE' operator: this MERGE is NOT optimized"
        else
            ok "optimized MERGE: DML DELETE + DML INSERT, no 'DML MERGE' operator"
        fi
    fi
    sql_merge_apply | timed "MERGE (update + insert)" "$RUN_LOG/r${RUN}_merge_merge"
    T_APPLY=$LAST_MS
}

# ---------------------------------------------------------------- one method
run_method() {   # <method> <number>
    local m=$1 fp="" dv=0
    chapter "$(method_title "$m")   [run $RUN of $RUNS]"
    reset_method "$m" "$2"
    "method_$m"
    local total=$(( T_LOAD + T_APPLY ))
    printf '\n  %s%s%-30s total %8s s%s   = parse+load JSON %s s + apply %s s   →  %s rows/s\n' "$C_BOLD" "$C_GREEN" "$(method_short "$m")" "$(secs "$total")" "$C_RESET" \
        "$(secs "$T_LOAD")" "$(secs "$T_APPLY")" "$(rate "$CHANGE_ROWS" "$total")"
    [[ $m == upsert && $RUN == 1 && ${QUIET:-0} != 1 ]] && upsert_showcase
    dv=$(vsql_query "SELECT COALESCE(SUM(deleted_row_count), 0) FROM v_monitor.delete_vectors WHERE schema_name = '${SCHEMA}' AND projection_name ILIKE 'txn\_${m}\_%'")
    if (( VALIDATE )); then
        sql_fingerprint "txn_${m}_current" "$CHECK_FROM" | timed "check: count + checksum (not timed)" "$RUN_LOG/r${RUN}_${m}_check"
        fp=$LAST_OUT
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$SESSION" "$RUN" "$m" "$BASE_ROWS" "$CHANGE_ROWS" "$T_LOAD" "$T_APPLY" "$total" \
        "${fp%%|*}" "${fp#*|}" "$dv" >> "$RESULTS_TSV"
}

# ---------------------------------------------------------------- results
validate_run() {
    local expected=$(( BASE_ROWS - ${CHECK_FROM:-1} + 1 - N_DEL + N_INS )) ref="" bad=0 m line rows sum scope="all rows"
    [[ -n $CHECK_FROM ]] && scope="rows with isn >= $(fmt_num "$CHECK_FROM") (the touched partitions and everything newer)"
    echo
    printf '%s  check of run %s: current data of every method, %s%s\n' "$C_BOLD" "$RUN" "$scope" "$C_RESET"
    printf '  %-34s %16s %14s\n' "method" "live rows" "checksum"
    for m in "${METHODS[@]}"; do
        line=$(awk -F'\t' -v s="$SESSION" -v r="$RUN" -v m="$m" '$1==s && $2==r && $3==m {print $9 "|" $10}' "$RESULTS_TSV" | tail -1)
        rows=${line%%|*}; sum=${line#*|}
        printf '  %-34s %16s %14s\n' "$(method_short "$m")" "$(fmt_num "$rows")" "$sum"
        [[ -z $ref ]] && ref=$line
        [[ $line == "$ref" && $rows == "$expected" ]] || bad=1
    done
    (( bad == 0 )) || die "the methods disagree, or the row count is not the expected $(fmt_num "$expected")"
    if [[ -n $CHECK_FROM ]]; then
        ok "identical in every method: $(fmt_num "$expected") live rows, as expected, and the same checksum"
    else
        ok "identical in every method: $(fmt_num "$expected") live rows (= $(fmt_num "$BASE_ROWS") − $(fmt_num "$N_DEL") deleted + $(fmt_num "$N_INS") inserted), same checksum"
    fi
}

summary() {
    local out="$REPORT_DIR/summary.md"
    chapter "RESULTS  $(fmt_num "$CHANGE_ROWS") JSON changes → $(fmt_num "$BASE_ROWS")-row fact table" \
            "$(fmt_num "$N_UPD") updates + $(fmt_num "$N_INS") inserts$( (( N_DEL > 0 )) && echo " + $(fmt_num "$N_DEL") deletes")   ·   $RUNS run(s)   ·   timer = parse JSON + load + apply"
    awk -F'\t' -v s="$SESSION" -v runs="$RUNS" -v order="${METHODS[*]}" -v B="$C_BOLD" -v Z="$C_RESET" -v G="$C_GREEN" -v md="$out.tmp" '
        function sec(ms) { return sprintf("%.2f", ms / 1000) }
        function name(m) { return m == "upsert" ? "1 Upsert (journal + Top-K LAP)" : m == "swap" ? "2 Staging + partition SWAP" : "3 Optimized MERGE" }
        $1 == s { n[$3]++; load[$3] += $6; app[$3] += $7; tot[$3] += $8; t[$3, $2] = $8; dv[$3] = $11; rows = $5 }
        END {
            k = split(order, M, " ")
            best = -1; for (i = 1; i <= k; i++) { a = tot[M[i]] / n[M[i]]; if (best < 0 || a < best) best = a }
            hdr = sprintf("%-32s %12s %10s %10s %10s", "method", "parse+load", "apply", "TOTAL", "rows/s")
            if (runs > 1) for (r = 1; r <= runs; r++) hdr = hdr sprintf(" %8s", "run " r)
            print "  " B hdr sprintf("  %s", "vs best") Z
            print "| Method | Parse + load JSON (s) | Apply (s) | **Total (s)** | Rows/s |" (runs > 1 ? " Runs (s) |" : "") " Delete vectors |" > md
            print "|---|---:|---:|---:|---:|" (runs > 1 ? "---|" : "") "---:|" > md
            for (i = 1; i <= k; i++) {
                m = M[i]; c = n[m]; avg = tot[m] / c
                line = sprintf("%-32s %12s %10s %s%10s%s %10s", name(m), sec(load[m] / c), sec(app[m] / c), B, sec(avg), Z, sprintf("%.0fK", rows / (avg / 1000) / 1000))
                list = ""
                if (runs > 1) for (r = 1; r <= runs; r++) { line = line sprintf(" %8s", sec(t[m, r])); list = list (r > 1 ? " / " : "") sec(t[m, r]) }
                line = line sprintf("  %s%5.1fx%s", (avg == best ? G : ""), avg / best, Z)
                print "  " line
                printf "| %s | %s | %s | **%s** | %.0fK |%s %d |\n", name(m), sec(load[m] / c), sec(app[m] / c), sec(avg), rows / (avg / 1000) / 1000, (runs > 1 ? " " list " |" : ""), dv[m] > md
            }
            if (runs > 1) print "  (parse+load, apply and TOTAL are averages over the runs)"
        }' "$RESULTS_TSV"
    {
        printf '# Results\n\n%s JSON change records (%s updates, %s inserts%s) applied to a %s-row fact table.\n' \
            "$(fmt_num "$CHANGE_ROWS")" "$(fmt_num "$N_UPD")" "$(fmt_num "$N_INS")" "$( (( N_DEL > 0 )) && echo ", $(fmt_num "$N_DEL") deletes")" "$(fmt_num "$BASE_ROWS")"
        printf 'Session %s, %s run(s); times are averages. The timer covers parsing the JSON, loading and applying.\n\n' "$SESSION" "$RUNS"
        cat "$out.tmp"
    } > "$out"
    rm -f "$out.tmp"
    echo
    info "report: ${out#"$ROOT_DIR"/}   all runs: ${RESULTS_TSV#"$ROOT_DIR"/}   SQL logs: ${RUN_LOG#"$ROOT_DIR"/}"
}

# ---------------------------------------------------------------- main
chapter "APPLY  $(fmt_num "$CHANGE_ROWS") JSON change records to a $(fmt_num "$BASE_ROWS")-row fact table, three ways" \
        "$(fmt_num "$N_UPD") updates of existing rows + $(fmt_num "$N_INS") inserts of new rows$( (( N_DEL > 0 )) && echo " + $(fmt_num "$N_DEL") deletes")   ·   files: ${CHANGES_DIR#"$ROOT_DIR"/}/"
preflight
for (( RUN = 1; RUN <= RUNS; RUN++ )); do
    QUIET=0; (( RUN > 1 )) && QUIET=1
    i=0
    for m in "${ALL_METHODS[@]}"; do
        i=$((i + 1))
        [[ " ${METHODS[*]} " == *" $m "* ]] || continue
        run_method "$m" "$i"
    done
    if (( VALIDATE )); then validate_run; fi
done
summary
