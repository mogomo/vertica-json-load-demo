#!/usr/bin/env bash
# =============================================================================
#  vload.sh — parallel JSON (ADABAS CDC) loading into Vertica, three ways
# =============================================================================
#  Generates hierarchical JSON for 10 ADABAS files and loads it into 10
#  Vertica tables in parallel, then applies CDC doses (insert/update/delete)
#  with three techniques and compares them:
#
#    swap   staging table + COPY/SWAP partitions into the fact table
#    merge  optimized MERGE from a delta table
#    lap    insert-only journal + Top-K Live Aggregate Projection
#
#  Run ./vload.sh help for usage.
# =============================================================================
set -o errexit -o nounset -o pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LIB_DIR=$ROOT_DIR/lib
VERSION=1.0.0

if (( BASH_VERSINFO[0] < 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] < 3) )); then
    echo "vload.sh needs bash >= 4.3 (found $BASH_VERSION)" >&2; exit 1
fi

# shellcheck source=/dev/null
[[ -f $ROOT_DIR/vload.env ]] && . "$ROOT_DIR/vload.env"
# shellcheck source=lib/common.sh
. "$LIB_DIR/common.sh"
# shellcheck source=lib/schema.sh
. "$LIB_DIR/schema.sh"
# shellcheck source=lib/generate.sh
. "$LIB_DIR/generate.sh"
# shellcheck source=lib/method_swap.sh
. "$LIB_DIR/method_swap.sh"
# shellcheck source=lib/method_merge.sh
. "$LIB_DIR/method_merge.sh"
# shellcheck source=lib/method_lap.sh
. "$LIB_DIR/method_lap.sh"

ALL_METHODS=(swap merge lap)

usage() {
    cat <<EOF
vload.sh $VERSION — parallel JSON loading into Vertica: partition swap vs optimized MERGE vs Top-K LAP

Usage: ./vload.sh <command> [options]

Commands
  check                      verify vsql connection, database and host resources
  generate  --scale S        generate the JSON data set (base load + CDC doses)
  run       --scale S --method M[,M…]
                             load the base data and apply every dose with method M
  purge     --scale S        compact the LAP journals (keep only the latest version)
  validate  --scale S        check that all methods produced identical current data
  report    --scale S        timing / storage comparison of the methods
  demo      --scale S        generate (if needed) + run all methods + validate + purge + report
  sql       --table T        print every SQL statement used for table T
  clean     [--data]         drop the demo schemas (and delete generated data)

Options
  --scale S       10K, 1M, 100M, 1B … (total rows over the 10 tables)
  --method M      swap | merge | lap | all  (comma separated list allowed)
  --pause         wait for Enter before each step (presentation / video mode)
  --drop-after    drop each method's schema after it is measured (saves disk at 1B)
  --dry-run       print the steps and SQL without executing anything
  --no-color      plain output

Configuration: copy vload.env.example to vload.env (connection, tuning, dose mix).
EOF
}

# ------------------------------------------------------------------ helpers
method_title() {
    case $1 in
        swap)  echo "METHOD 1 · Staging table + partition COPY / SWAP" ;;
        merge) echo "METHOD 2 · Optimized MERGE" ;;
        lap)   echo "METHOD 3 · Insert-only journal + Top-K Live Aggregate Projection" ;;
    esac
}
method_subtitle() {
    case $1 in
        swap)  echo "load into a side table, rebuild only touched partitions, publish with one atomic swap" ;;
        merge) echo "COPY each dose into a delta table and apply it with a MERGE that meets the optimization rules" ;;
        lap)   echo "never UPDATE or DELETE: append every version, let the LAP serve the latest one" ;;
    esac
}

init_run() {  # <method>
    METHOD=$1
    SCHEMA="${SCHEMA_PREFIX}_${METHOD}"
    RUN_ID=$(date +%Y%m%d-%H%M%S)
    RUN_TAG="${RUN_ID//-/_}_${METHOD}"
    RUN_LOG_DIR="$LOG_DIR/${SCALE_LABEL}/${RUN_ID}_${METHOD}"
    RESULTS_FILE="$REPORT_DIR/results.tsv"
    mkdir -p "$RUN_LOG_DIR" "$REPORT_DIR"
    if [[ $DRY_RUN == 1 ]]; then RESULTS_FILE=/dev/null; return 0; fi
    [[ -f $RESULTS_FILE ]] || printf 'run_id\tscale\tmethod\tstep\ttable\trows\tms\tfailed\n' > "$RESULTS_FILE"
}

# rows + checksum of the current (live) data of every table -> fingerprints.tsv
record_fingerprint() {  # <label>
    local label=$1 t fp f="$REPORT_DIR/fingerprints.tsv" storage dv
    [[ $DRY_RUN == 1 ]] && return
    [[ -f $f ]] || printf 'scale\tmethod\ttable\tlive_rows\tchecksum\trun_id\n' > "$f"
    for t in "${TABLES[@]}"; do
        fp=$(vsql_query "SELECT COUNT(*), COALESCE(SUM(HASH(isn, change_ts) % 1000000007), 0) FROM ${SCHEMA}.${t}_current" | tr '|' '\t')
        printf '%s\t%s\t%s\t%s\t%s\n' "$SCALE_LABEL" "$label" "$t" "$fp" "$RUN_ID" >> "$f"
    done
    storage=$(vsql_query "SELECT COALESCE(SUM(used_bytes),0) FROM v_monitor.projection_storage WHERE anchor_table_schema = '${SCHEMA}'")
    dv=$(vsql_query "SELECT COALESCE(SUM(deleted_row_count),0) FROM v_monitor.delete_vectors WHERE schema_name = '${SCHEMA}'")
    [[ -f $REPORT_DIR/stats.tsv ]] || printf 'scale\tmethod\tstorage_bytes\tdelete_vector_rows\trun_id\n' > "$REPORT_DIR/stats.tsv"
    printf '%s\t%s\t%s\t%s\t%s\n' "$SCALE_LABEL" "$label" "$storage" "$dv" "$RUN_ID" >> "$REPORT_DIR/stats.tsv"
    info "storage of ${SCHEMA}: $(fmt_num $((storage / 1024 / 1024))) MB   delete-vector rows: $(fmt_num "$dv")"
}

# ------------------------------------------------------------------ commands
cmd_check() {
    chapter "CHECK  environment"
    info "bash $BASH_VERSION, $(pick_awk), compression=$COMPRESSION, $NCPU CPUs, $(awk '/MemTotal/{printf "%d GB RAM", $2/1024/1024}' /proc/meminfo 2>/dev/null)"
    mkdir -p "$DATA_DIR"
    info "data dir $DATA_DIR: $(df -Ph "$DATA_DIR" | awk 'NR==2{print $4 " free"}')"
    [[ $COMPRESSION == zstd ]] && { command -v zstd >/dev/null || die "zstd missing (apt install zstd) or set COMPRESSION=gzip"; }
    vsql_check
    info "$(vsql_query "SELECT version()")"
    info "nodes: $(vsql_query "SELECT COUNT(*) || ' (' || SUM(CASE WHEN node_state='UP' THEN 1 ELSE 0 END) || ' up)' FROM nodes")"
    info "database: $(vsql_query "SELECT database_name FROM databases LIMIT 1"), user: $(vsql_query "SELECT current_user()")"
    vsql_query "SELECT COUNT(*) FROM user_functions WHERE function_name ILIKE 'FJSONParser'" | grep -qv '^0$' \
        || warn "FJSONPARSER not found — install the flex table package"
    ok "ready"
}

cmd_run() {  # <method>...
    local m t0 start
    load_manifest "$SCALE_LABEL"
    read -r -a TABLES <<< "$TABLE_LIST"
    [[ $DRY_RUN == 1 ]] || vsql_check
    for m in "$@"; do
        init_run "$m"
        chapter "$(method_title "$m")   [$SCALE_LABEL rows → schema $SCHEMA]" "$(method_subtitle "$m")"
        info "logs: ${RUN_LOG_DIR#"$ROOT_DIR"/}"
        printf 'DROP SCHEMA IF EXISTS %s CASCADE;\nCREATE SCHEMA %s;\n' "$SCHEMA" "$SCHEMA" | vsql_exec "$RUN_LOG_DIR/_schema" >/dev/null
        start=$(now_ms)
        "method_$m"
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$RUN_ID" "$SCALE_LABEL" "$METHOD" "_total" "_wall" 0 $(( $(now_ms) - start )) 0 >> "$RESULTS_FILE"
        record_fingerprint "$m"
        ok "$(method_title "$m") finished in $(secs $(( $(now_ms) - start ))) s"
        drop_schema_if_requested
    done
}

drop_schema_if_requested() {
    if [[ ${DROP_AFTER:-0} == 1 && $DRY_RUN != 1 ]]; then
        vsql_query "DROP SCHEMA ${SCHEMA} CASCADE" >/dev/null && info "dropped schema ${SCHEMA} (--drop-after)"
    fi
    return 0
}

cmd_purge() {
    load_manifest "$SCALE_LABEL"
    read -r -a TABLES <<< "$TABLE_LIST"
    [[ $DRY_RUN == 1 ]] || vsql_check
    init_run lap
    METHOD=lap_purge
    [[ $DRY_RUN == 1 ]] || [[ $(vsql_query "SELECT COUNT(*) FROM schemata WHERE schema_name = '${SCHEMA}'") == 1 ]] \
        || die "schema ${SCHEMA} does not exist — run: ./vload.sh run --scale $SCALE_LABEL --method lap"
    local before=0 after=0 start
    [[ $DRY_RUN == 1 ]] || before=$(vsql_query "SELECT COALESCE(SUM(row_count),0) FROM v_monitor.projection_storage WHERE anchor_table_schema = '${SCHEMA}' AND projection_name NOT LIKE '%topk%'")
    start=$(now_ms)
    method_lap_purge
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$RUN_ID" "$SCALE_LABEL" "lap_purge" "_total" "_wall" 0 $(( $(now_ms) - start )) 0 >> "$RESULTS_FILE"
    [[ $DRY_RUN == 1 ]] || after=$(vsql_query "SELECT COALESCE(SUM(row_count),0) FROM v_monitor.projection_storage WHERE anchor_table_schema = '${SCHEMA}' AND projection_name NOT LIKE '%topk%'")
    info "journal rows before purge: $(fmt_num "$before")   after: $(fmt_num "$after")"
    record_fingerprint lap_purge
}

cmd_validate() {
    local f="$REPORT_DIR/fingerprints.tsv"
    [[ -f $f ]] || die "no fingerprints yet — run some methods first"
    chapter "VALIDATE  current data of every method must be identical  [$SCALE_LABEL]"
    awk -F'\t' -v s="$SCALE_LABEL" -v G="$C_GREEN" -v R="$C_RED" -v Z="$C_RESET" '
        NR > 1 && $1 == s { key = $2 SUBSEP $3; last[key] = $4 "/" $5; m[$2] = 1; t[$3] = 1; if (!($3 in tord)) tord[$3] = ++nt; }
        END {
            if (nt == 0) { print "  no fingerprints for scale " s; exit 1 }
            nm = 0; for (x in m) ml[++nm] = x
            for (i = 1; i <= nm; i++) for (j = i + 1; j <= nm; j++) if (ml[j] < ml[i]) { tmp = ml[i]; ml[i] = ml[j]; ml[j] = tmp }
            printf "  %-12s", "table"; for (i = 1; i <= nm; i++) printf " %16s", ml[i]; printf "   result\n"
            bad = 0
            for (tt in tord) order[tord[tt]] = tt
            for (k = 1; k <= nt; k++) {
                tt = order[k]; printf "  %-12s", tt; ref = ""; same = 1
                for (i = 1; i <= nm; i++) {
                    v = last[ml[i], tt]; split(v, p, "/"); printf " %16s", (v == "" ? "-" : p[1])
                    if (v != "") { if (ref == "") ref = v; else if (v != ref) same = 0 }
                }
                if (same) printf "   %s✔ identical%s\n", G, Z; else { printf "   %s✘ DIFFERENT%s\n", R, Z; bad++ }
            }
            exit bad > 0
        }' "$f" && ok "all methods agree: same live row count and checksum per table" || die "methods disagree — see $f"
}

cmd_report() {
    local r="$REPORT_DIR/results.tsv" s="$REPORT_DIR/stats.tsv" out="$REPORT_DIR/summary_${SCALE_LABEL}.md"
    [[ -f $r ]] || die "no results yet"
    chapter "REPORT  $SCALE_LABEL"
    awk -F'\t' -v scale="$SCALE_LABEL" -v statsf="$s" '
        BEGIN {
            while ((getline line < statsf) > 0) { split(line, a, "\t"); if (a[1] == scale) { sto[a[2]] = a[3]; dv[a[2]] = a[4] } }
        }
        NR > 1 && $2 == scale {
            # keep the latest run of each method
            if ($1 != run[$3]) { if ($1 < run[$3]) next; run[$3] = $1; delete_method($3) }
            if ($5 != "_wall") next
            if ($4 == "_total") { tot[$3] = $7; next }
            if ($4 == "base_copy" || $4 == "publish") { base[$3] += $7; if ($4 == "base_copy") brows[$3] = $6 }
            else if ($4 ~ /^dose[0-9]+_/) { d = $4; sub(/_.*/, "", d); dose[$3, d] += $7; if ($4 ~ /_copy$/) drows[$3] += $6; nd[$3, d] = 1; dms[$3] += $7 }
            else if ($4 == "purge") { base[$3] += $7 }
        }
        function delete_method(m,   k) { base[m] = 0; brows[m] = 0; drows[m] = 0; dms[m] = 0; tot[m] = 0; for (k in nd) { split(k, q, SUBSEP); if (q[1] == m) delete nd[k] } }
        function fmt(n,   s, r) { s = sprintf("%d", n); r = ""; while (length(s) > 3) { r = "," substr(s, length(s) - 2) r; s = substr(s, 1, length(s) - 3) } return s r }
        function rate(r, ms) { if (ms <= 0) return "-"; v = r * 1000 / ms; return (v >= 1e6) ? sprintf("%.2fM", v / 1e6) : sprintf("%.1fK", v / 1e3) }
        END {
            printf "| Method | Base load | Base rows/s | Doses | Avg dose apply | Dose rows/s | Total | Storage MB | Delete-vector rows |\n"
            printf "|---|---:|---:|---:|---:|---:|---:|---:|---:|\n"
            n = split("swap merge lap lap_purge", order, " ")
            for (i = 1; i <= n; i++) {
                m = order[i]; if (!(m in run)) continue
                k = 0; for (x in nd) { split(x, q, SUBSEP); if (q[1] == m) k++ }
                sm = m
                printf "| %s | %s | %s | %d | %s | %s | %s | %s | %s |\n", m,
                    (base[m] ? sprintf("%.2f s", base[m] / 1000) : "-"),
                    (m == "lap_purge" ? "-" : rate(brows[m], base[m])), k,
                    (k ? sprintf("%.2f s", dms[m] / k / 1000) : "-"),
                    (k ? rate(drows[m], dms[m]) : "-"),
                    sprintf("%.2f s", tot[m] / 1000),
                    (sm in sto ? fmt(sto[sm] / 1048576) : "-"), (sm in dv ? fmt(dv[sm]) : "-")
            }
        }' "$r" | tee "$out.tmp"
    { printf '# vload results — %s rows\n\n_Generated %s_\n\n' "$SCALE_LABEL" "$(date '+%F %T')"; cat "$out.tmp"; } > "$out"
    rm -f "$out.tmp"
    echo
    info "per-step timings: ${r#"$ROOT_DIR"/}    markdown summary: ${out#"$ROOT_DIR"/}"
}

cmd_sql() {  # --table T
    local t=${TABLE:-customer}
    SCALE_LABEL=${SCALE_LABEL:-10K}; DATASET_DIR="$DATA_DIR/$SCALE_LABEL"; RUN_TAG=example; DOSE_DIR=dose_01; DRY_RUN=1
    grep -q "^T|$t|" "$DEFS_FILE" || die "unknown table '$t'"
    for m in "${ALL_METHODS[@]}"; do
        SCHEMA="${SCHEMA_PREFIX}_$m"; STREAM="vl_example"
        printf '\n-- ======================= %s =======================\n' "$(method_title "$m")"
        "${m}_setup_sql" "$t"
        case $m in
            swap)  printf '\n-- base load\n'; swap_base_copy_sql "$t"; swap_publish_sql "$t"
                   printf '\n-- each dose\n'; swap_delta_copy_sql "$t"; swap_rebuild_sql "$t"; swap_swap_sql "$t" ;;
            merge) printf '\n-- base load\n'; merge_base_copy_sql "$t"
                   printf '\n-- each dose\n'; merge_delta_copy_sql "$t"; merge_apply_sql "$t" ;;
            lap)   printf '\n-- base load\n'; lap_copy_sql "$t" base
                   printf '\n-- each dose\n'; lap_copy_sql "$t" dose_01
                   printf '\n-- periodic purge\n'; lap_purge_sql "$t" ;;
        esac
    done
}

cmd_clean() {
    local m
    vsql_check
    for m in "${ALL_METHODS[@]}"; do
        vsql_query "DROP SCHEMA IF EXISTS ${SCHEMA_PREFIX}_${m} CASCADE" >/dev/null && info "dropped schema ${SCHEMA_PREFIX}_${m}"
    done
    if [[ ${CLEAN_DATA:-0} == 1 ]]; then
        rm -rf "$DATA_DIR" && info "deleted $DATA_DIR"
    fi
    ok "clean"
}

cmd_demo() {
    local m drop=${DROP_AFTER:-0}
    [[ -f $DATA_DIR/$SCALE_LABEL/manifest.env ]] || cmd_generate "$SCALE_ROWS"
    for m in "${METHODS[@]}"; do
        if [[ $m == lap ]]; then
            DROP_AFTER=0; cmd_run lap; cmd_purge; DROP_AFTER=$drop
            drop_schema_if_requested
        else
            cmd_run "$m"
        fi
    done
    [[ $DRY_RUN == 1 ]] || cmd_validate
    [[ $DRY_RUN == 1 ]] || cmd_report
}

# ------------------------------------------------------------------ main
main() {
    local cmd=${1:-help}; shift || true
    SCALE_ROWS="" METHODS=() TABLE=""
    while (( $# )); do
        case $1 in
            --scale)      SCALE_ROWS=$(parse_scale "${2:?--scale needs a value}"); shift ;;
            --method|--methods)
                          IFS=, read -r -a METHODS <<< "${2:?--method needs a value}"; shift ;;
            --table)      TABLE=${2:?--table needs a value}; shift ;;
            --pause)      PAUSE=1 ;;
            --dry-run)    DRY_RUN=1 ;;
            --drop-after) DROP_AFTER=1 ;;
            --data)       CLEAN_DATA=1 ;;
            --no-color)   NO_COLOR=1; C_RESET='' C_BOLD='' C_DIM='' C_RED='' C_GREEN='' C_YELLOW='' C_BLUE='' C_MAGENTA='' C_CYAN=''; IS_TTY=0 ;;
            -h|--help)    usage; exit 0 ;;
            *) die "unknown option '$1' (see ./vload.sh help)" ;;
        esac
        shift
    done
    load_config
    DATA_DIR=$(mkdir -p "$DATA_DIR" && cd "$DATA_DIR" && pwd)   # COPY needs absolute paths
    mapfile -t TABLES < <(defs_tables)
    [[ ${#METHODS[@]} -eq 0 || ${METHODS[0]} == all ]] && METHODS=("${ALL_METHODS[@]}")
    local m; for m in "${METHODS[@]}"; do [[ " ${ALL_METHODS[*]} " == *" $m "* ]] || die "unknown method '$m'"; done
    if [[ -n $SCALE_ROWS ]]; then SCALE_LABEL=$(scale_label "$SCALE_ROWS"); fi
    need_scale() { [[ -n $SCALE_ROWS ]] || die "--scale is required (e.g. --scale 10K)"; }

    case $cmd in
        check)    cmd_check ;;
        generate) need_scale; cmd_generate "$SCALE_ROWS" ;;
        run)      need_scale; cmd_run "${METHODS[@]}" ;;
        purge)    need_scale; cmd_purge ;;
        validate) need_scale; cmd_validate ;;
        report)   need_scale; cmd_report ;;
        demo)     need_scale; cmd_demo ;;
        sql)      cmd_sql ;;
        clean)    cmd_clean ;;
        help|-h|--help) usage ;;
        *) die "unknown command '$cmd' (see ./vload.sh help)" ;;
    esac
}

main "$@"
