# shellcheck shell=bash
# =============================================================================
#  common.sh — configuration, presentation, vsql wrapper, parallel runner
# =============================================================================

# ----------------------------------------------------------------- config
load_config() {
    # defaults (override in vload.env or in the environment)
    : "${VSQL:=/opt/vertica/bin/vsql}"
    : "${SCHEMA_PREFIX:=vload}"
    : "${DATA_DIR:=$ROOT_DIR/data}"
    : "${LOG_DIR:=$ROOT_DIR/logs}"
    : "${REPORT_DIR:=$ROOT_DIR/reports}"
    : "${DEFS_FILE:=$ROOT_DIR/conf/tables.def}"
    : "${COMPRESSION:=zstd}"            # zstd | gzip | none
    : "${FILES_PER_TABLE:=auto}"        # JSON files per table and stream
    : "${GEN_JOBS:=auto}"               # parallel generator processes
    : "${DOSES:=3}"                     # CDC doses applied after the base load
    : "${DOSE_PCT:=1}"                  # dose size, % of the base rows
    : "${DOSE_MIX:=60:10:30}"           # update:delete:insert share of a dose
    : "${HOT_PCT:=5}"                   # updates/deletes hit the newest HOT_PCT % of rows
    : "${START_DATE:=2023-01-01}"       # created_date of ISN 1
    : "${SPAN_DAYS:=1096}"              # base data covers 3 years = 36 partitions
    : "${SEED:=20260101}"
    : "${COPY_NODE_CLAUSE:=ON ANY NODE}"
    : "${RESOURCE_POOL:=}"
    : "${SQL_PREVIEW_LINES:=60}"        # 0 = always print the full statement
    : "${PAUSE:=0}"
    : "${DRY_RUN:=0}"
    case $COMPRESSION in
        zstd) COPY_FILTER=ZSTD; FILE_EXT=json.zst ;;
        gzip) COPY_FILTER=GZIP; FILE_EXT=json.gz ;;
        none) COPY_FILTER="";   FILE_EXT=json ;;
        *) die "COMPRESSION must be zstd, gzip or none" ;;
    esac
    NCPU=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN)
    [[ $GEN_JOBS == auto ]] && GEN_JOBS=$NCPU
    export VSQL_HOST VSQL_PORT VSQL_USER VSQL_PASSWORD VSQL_DATABASE
}

# 10K, 1M, 250M, 1B, 12345 -> number
parse_scale() {
    local s=${1^^} n m=1
    case $s in
        *K) m=1000; s=${s%K} ;;
        *M) m=1000000; s=${s%M} ;;
        *B) m=1000000000; s=${s%B} ;;
    esac
    [[ $s =~ ^[0-9]+$ ]] || die "invalid scale '$1' (use e.g. 10K, 1M, 1B)"
    n=$((s * m))
    (( n >= 10 * 10 )) || die "scale must be at least 100 rows"
    echo "$n"
}

scale_label() { # 1000000000 -> 1B
    local n=$1
    if   (( n % 1000000000 == 0 )); then echo "$((n / 1000000000))B"
    elif (( n % 1000000 == 0 ));    then echo "$((n / 1000000))M"
    elif (( n % 1000 == 0 ));       then echo "$((n / 1000))K"
    else echo "$n"; fi
}

# ----------------------------------------------------------------- output
if [[ -t 1 && -z ${NO_COLOR:-} ]]; then
    C_RESET=$'\e[0m' C_BOLD=$'\e[1m' C_DIM=$'\e[2m' C_RED=$'\e[31m' C_GREEN=$'\e[32m'
    C_YELLOW=$'\e[33m' C_BLUE=$'\e[34m' C_MAGENTA=$'\e[35m' C_CYAN=$'\e[36m'
    IS_TTY=1
else
    C_RESET='' C_BOLD='' C_DIM='' C_RED='' C_GREEN='' C_YELLOW='' C_BLUE='' C_MAGENTA='' C_CYAN=''
    IS_TTY=0
fi

log()  { printf '%s\n' "$*"; }
info() { printf '%s•%s %s\n' "$C_CYAN" "$C_RESET" "$*"; }
ok()   { printf '%s✔%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn() { printf '%s⚠ %s%s\n' "$C_YELLOW" "$*" "$C_RESET" >&2; }
die()  { printf '%s✘ %s%s\n' "$C_RED" "$*" "$C_RESET" >&2; exit 1; }

hr() { printf '%s%s%s\n' "$C_DIM" "────────────────────────────────────────────────────────────────────────────────" "$C_RESET"; }

# A chapter of the demo (method, phase)
chapter() {
    echo
    printf '%s%s════════════════════════════════════════════════════════════════════════════════%s\n' "$C_BOLD" "$C_MAGENTA" "$C_RESET"
    printf '%s%s  %s%s\n' "$C_BOLD" "$C_MAGENTA" "$1" "$C_RESET"
    [[ -n ${2:-} ]] && printf '%s  %s%s\n' "$C_MAGENTA" "$2" "$C_RESET"
    printf '%s%s════════════════════════════════════════════════════════════════════════════════%s\n' "$C_BOLD" "$C_MAGENTA" "$C_RESET"
}

# Wraps text to the terminal width with a hanging indent.
wrap() { local indent=$1; shift; printf '%s\n' "$*" | fold -s -w $(( ${COLUMNS:-100} > 120 ? 110 : ${COLUMNS:-100} - 4 - ${#indent} )) | sed "2,\$s/^/${indent}/"; }

# explain_step <id> <title> <what> <why> <sql>
#   Prints the step banner, the explanation and the SQL that will run.
explain_step() {
    local id=$1 title=$2 what=$3 why=$4 sql=$5 n
    echo
    hr
    printf '%s%s▶ STEP %s  %s%s\n' "$C_BOLD" "$C_BLUE" "$id" "$title" "$C_RESET"
    printf '%s  WHAT%s  ' "$C_BOLD" "$C_RESET"; wrap "        " "$what"
    printf '%s  WHY %s  ' "$C_BOLD" "$C_RESET"; wrap "        " "$why"
    if [[ -n $sql ]]; then
        printf '%s  SQL %s  %s(shown for one table; the same statement runs for every table)%s\n' "$C_BOLD" "$C_RESET" "$C_DIM" "$C_RESET"
        n=$(printf '%s\n' "$sql" | wc -l)
        if (( SQL_PREVIEW_LINES > 0 && n > SQL_PREVIEW_LINES )); then
            printf '%s\n' "$sql" | head -n "$SQL_PREVIEW_LINES" | highlight_sql
            printf '        %s… %d more lines (full SQL in the run log)%s\n' "$C_DIM" $((n - SQL_PREVIEW_LINES)) "$C_RESET"
        else
            printf '%s\n' "$sql" | highlight_sql
        fi
    fi
    pause
}

highlight_sql() {
    if (( IS_TTY )); then
        sed -E "s/^/        /; s/\<(CREATE|TABLE|PROJECTION|SCHEMA|VIEW|DROP|IF|EXISTS|CASCADE|COPY|FROM|PARSER|STREAM|NAME|REJECTED|DATA|AS|FILLER|SELECT|INSERT|INTO|VALUES|MERGE|USING|ON|WHEN|MATCHED|NOT|THEN|UPDATE|SET|WHERE|AND|OR|UNION|ALL|ORDER|BY|SEGMENTED|HASH|NODES|PARTITION|LIMIT|OVER|DESC|LIKE|INCLUDING|PROJECTIONS|COMMIT|ALTER|RENAME|TO|PRIMARY|KEY|CONSTRAINT|DISABLED|COMMENT|IS|ANY|NODE|GROUP|COUNT|NULL)\>/${C_YELLOW}\1${C_RESET}/g"
    else
        sed 's/^/        /'
    fi
}

pause() {
    if [[ $PAUSE == 1 && -t 0 ]]; then
        printf '%s  ⏎ press Enter to run…%s' "$C_DIM" "$C_RESET"
        read -r _
    fi
}

fmt_num() { awk -v n="$1" 'BEGIN{ s=sprintf("%d", n); r=""; while (length(s) > 3) { r="," substr(s, length(s)-2) r; s=substr(s, 1, length(s)-3) } print s r }'; }
# epoch milliseconds (bash 5 EPOCHREALTIME; GNU/uutils date otherwise)
now_ms() {
    if [[ -n ${EPOCHREALTIME:-} ]]; then
        local t=${EPOCHREALTIME//[!0-9]/}
        echo $(( t / 1000 ))
    else
        echo $(( $(date +%s%N) / 1000000 ))
    fi
}
secs()    { awk -v ms="$1" 'BEGIN{printf "%.2f", ms/1000}'; }
rate()    { awk -v r="$1" -v ms="$2" 'BEGIN{ if (ms<=0) {print "-"; exit} v=r*1000/ms; if (v>=1e6) printf "%.2fM", v/1e6; else if (v>=1e3) printf "%.1fK", v/1e3; else printf "%.0f", v }'; }

# ----------------------------------------------------------------- vsql
# vsql_exec <logfile> : runs SQL from stdin, output appended to <logfile>,
# stdout = result tuples (unaligned, no headers).
vsql_exec() {
    local logf=$1 sql rc
    sql=$(cat)
    {
        printf -- '-- %s\n' "$(date '+%F %T')"
        printf '%s\n' "$sql"
    } >> "$logf.sql"
    if [[ $DRY_RUN == 1 ]]; then return 0; fi
    if {
        printf '\\set ON_ERROR_STOP on\n'
        [[ -n $RESOURCE_POOL ]] && printf 'SET SESSION RESOURCE_POOL = %s;\n' "$RESOURCE_POOL"
        printf '%s\n' "$sql"
    } | "$VSQL" -X -A -t -q -v ON_ERROR_STOP=1 2>>"$logf.err" | tee -a "$logf.out"; then
        return 0
    else
        rc=$?
    fi
    grep -v -e 'WARNING 10596' -e '^HINT:.*UNMATCHED_KEY' "$logf.err" | tail -5 >&2
    return "$rc"
}

# single query, returns tuples on stdout
vsql_query() { "$VSQL" -X -A -t -q -v ON_ERROR_STOP=1 -c "$1"; }

vsql_check() {
    [[ -x $VSQL ]] || die "vsql not found at $VSQL (set VSQL in vload.env)"
    vsql_query "SELECT 1" >/dev/null 2>&1 || die "cannot connect with vsql — check VSQL_HOST/VSQL_USER/VSQL_PASSWORD/VSQL_DATABASE in vload.env"
}

# ----------------------------------------------------------------- parallel
# par_tables <step_id> <label> <worker_fn> [stream_prefix]
#   Runs "<worker_fn> <table>" for every table concurrently. Each worker
#   prints the number of rows it processed as the last line of stdout.
#   Records per-table and wall-clock timing in $RESULTS_FILE.
par_tables() {
    local step=$1 label=$2 fn=$3 stream=${4:-} t start end e rc rows ms fails=0 total=0 sum_ms=0
    local -A pids=()
    local tmp="$RUN_LOG_DIR/.par.$step"
    mkdir -p "$tmp"
    start=$(now_ms)
    for t in "${TABLES[@]}"; do
        (
            local s e rows
            s=$(now_ms)
            if rows=$("$fn" "$t" | tail -n 1); then
                e=$(now_ms); printf '%s %s %s %s\n' "${rows:-0}" $((e - s)) 0 "$e" > "$tmp/$t"
            else
                e=$(now_ms); printf '%s %s %s %s\n' 0 $((e - s)) 1 "$e" > "$tmp/$t"; exit 1
            fi
        ) &
        pids[$t]=$!
    done
    progress_monitor "$start" "$stream" "${pids[@]}"
    for t in "${TABLES[@]}"; do wait "${pids[$t]}" || true; done
    end=$start     # wall-clock = last worker finished (not the monitor's poll)
    for t in "${TABLES[@]}"; do
        read -r _ _ _ e < "$tmp/$t" 2>/dev/null && (( e > end )) && end=$e
    done

    local lines=""
    for t in "${TABLES[@]}"; do
        read -r rows ms rc _ < "$tmp/$t" || { rows=0 ms=0 rc=1; }
        [[ $rows =~ ^[0-9]+$ ]] || rows=0
        if (( rc == 0 )); then
            lines+=$(printf '  %-12s %15s %10s %12s' "$t" "$(fmt_num "$rows")" "$(secs "$ms")" "$(rate "$rows" "$ms")")$'\n'
        else
            lines+=$(printf '  %s%-12s %15s %10s %12s  FAILED — see %s%s' "$C_RED" "$t" "-" "$(secs "$ms")" "-" "${RUN_LOG_DIR#"$ROOT_DIR"/}/$t.*.err" "$C_RESET")$'\n'
            fails=$((fails + 1))
        fi
        total=$((total + rows)); sum_ms=$((sum_ms + ms))
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$RUN_ID" "$SCALE_LABEL" "$METHOD" "$step" "$t" "$rows" "$ms" "$rc" >> "$RESULTS_FILE"
    done
    if (( total > 0 || fails > 0 )); then
        printf '  %s%-12s %15s %10s %12s%s\n' "$C_DIM" "table" "rows" "seconds" "rows/s" "$C_RESET"
        printf '%s' "$lines"
    fi
    local wall=$((end - start))
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$RUN_ID" "$SCALE_LABEL" "$METHOD" "$step" "_wall" "$total" "$wall" "$fails" >> "$RESULTS_FILE"
    rm -rf "$tmp"
    if (( fails > 0 )); then
        die "$label: $fails table(s) failed"
    fi
    if (( total == 0 )); then
        ok "$(printf '%s%s%s: %d tables in %s s wall-clock' "$C_BOLD" "$label" "$C_RESET" "${#TABLES[@]}" "$(secs "$wall")")"
        return 0
    fi
    ok "$(printf '%s%s%s: %s rows in %s s wall-clock  (%s rows/s; %s s of work done in parallel → %sx)' \
        "$C_BOLD" "$label" "$C_RESET" "$(fmt_num "$total")" "$(secs "$wall")" "$(rate "$total" "$wall")" "$(secs "$sum_ms")" \
        "$(awk -v a="$sum_ms" -v b="$wall" 'BEGIN{printf "%.1f", (b>0? a/b : 0)}')")"
}

# Live progress line while workers run. For COPY steps the rows loaded so far
# are read from v_monitor.load_streams (by STREAM NAME prefix).
progress_monitor() {
    local start=$1 stream=$2; shift 2
    local pids=("$@") alive p rows="" spin='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏' i=0 el
    while :; do
        alive=0
        for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && alive=$((alive + 1)); done
        (( alive == 0 )) && break
        if (( IS_TTY )); then
            el=$(( $(now_ms) - start ))
            if [[ -n $stream && $DRY_RUN != 1 && $((i % 4)) == 0 ]]; then
                rows=$(vsql_query "SELECT COALESCE(SUM(accepted_row_count),0) FROM v_monitor.load_streams WHERE stream_name LIKE '${stream}%' AND is_executing" 2>/dev/null || echo "")
            fi
            printf '\r  %s%s %6ss  %d/%d tables running%s%s' "$C_CYAN" "${spin:i%10:1}" "$(secs "$el")" "$alive" "${#pids[@]}" \
                "${rows:+  |  rows loaded: $(fmt_num "$rows")  ($(rate "$rows" "$el") rows/s)}" "$C_RESET"
            i=$((i + 1))
        fi
        sleep 0.25
    done
    (( IS_TTY )) && printf '\r\033[K'
    return 0
}
