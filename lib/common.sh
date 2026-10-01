# shellcheck shell=bash
# =============================================================================
#  common.sh — configuration, console output, timers and the vsql wrapper
# =============================================================================

# "sql_function | timed …" must run timed in this shell (it sets LAST_MS)
shopt -s lastpipe

# ----------------------------------------------------------------- config
load_config() {
    # defaults (override in vload.env or in the environment)
    : "${VSQL:=/opt/vertica/bin/vsql}"
    : "${SCHEMA:=vload}"
    : "${DEMO_DIR:=$ROOT_DIR/demo}"        # the JSON change files go to $DEMO_DIR/changes
    : "${LOG_DIR:=$ROOT_DIR/logs}"
    : "${REPORT_DIR:=$ROOT_DIR/reports}"
    : "${BASE_ROWS:=1B}"                   # rows of the fact table
    : "${CHANGE_ROWS:=1M}"                 # rows in the JSON change files
    : "${CHANGE_MIX:=50:0:50}"             # update : delete : insert (percent)
    : "${HOT_PCT:=5}"                      # updates/deletes hit the newest HOT_PCT % of the rows
    : "${START_DATE:=2023-01-01}"          # created_date of ISN 1
    : "${SPAN_DAYS:=1096}"                 # 3 years of data = 36 monthly partitions
    : "${SEED:=20260101}"
    : "${JSON_FILES:=auto}"                # change files = COPY parse threads
    : "${GEN_SESSIONS:=auto}"              # parallel INSERT sessions while generating
    : "${REBUILD_SESSIONS:=auto}"          # parallel sessions of the swap rebuild
    : "${MULTI_ROWS:=60M}"                 # phases 2-3: rows of each of the 9 other tables
    : "${PARALLEL:=10}"                    # phases 2-3: MERGEs running at the same time
    : "${COPY_NODE_CLAUSE:=ON ANY NODE}"
    : "${RESOURCE_POOL:=}"
    : "${SQL_PREVIEW_LINES:=40}"           # 0 = always print the full statement
    : "${PAUSE:=0}"
    NCPU=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN)
    BASE_ROWS=$(parse_count "$BASE_ROWS")
    CHANGE_ROWS=$(parse_count "$CHANGE_ROWS")
    MULTI_ROWS=$(parse_count "$MULTI_ROWS")
    [[ $JSON_FILES == auto ]] && JSON_FILES=$(( NCPU < 24 ? NCPU : 24 ))
    [[ $GEN_SESSIONS == auto ]] && GEN_SESSIONS=$(( NCPU / 2 > 1 ? NCPU / 2 : 1 ))
    [[ $REBUILD_SESSIONS == auto ]] && REBUILD_SESSIONS=$GEN_SESSIONS
    CHANGES_DIR="$DEMO_DIR/changes"
    PHASE2_DIR="$DEMO_DIR/phase2"
    PHASE3_DIR="$DEMO_DIR/phase3"
    export VSQL_HOST VSQL_PORT VSQL_USER VSQL_PASSWORD VSQL_DATABASE
}

# 10K, 1M, 1B, 12345 -> number
parse_count() {
    local s=${1^^} m=1
    case $s in
        *K) m=1000; s=${s%K} ;;
        *M) m=1000000; s=${s%M} ;;
        *B) m=1000000000; s=${s%B} ;;
    esac
    [[ $s =~ ^[0-9]+$ ]] || die "invalid number '$1' (use e.g. 10M, 1M, 1B)"
    echo $(( s * m ))
}

# 1000000000 -> 1B
count_label() {
    local n=$1
    if   (( n % 1000000000 == 0 )); then echo "$((n / 1000000000))B"
    elif (( n % 1000000 == 0 ));    then echo "$((n / 1000000))M"
    elif (( n % 1000 == 0 ));       then echo "$((n / 1000))K"
    else echo "$n"; fi
}

# the change mix as row counts: sets N_UPD N_DEL N_INS
split_changes() {
    local u d i
    IFS=: read -r u d i <<< "$CHANGE_MIX"
    (( u + d + i == 100 )) || die "CHANGE_MIX must add up to 100 (got $CHANGE_MIX)"
    N_UPD=$(( CHANGE_ROWS * u / 100 ))
    N_DEL=$(( CHANGE_ROWS * d / 100 ))
    N_INS=$(( CHANGE_ROWS - N_UPD - N_DEL ))
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
no_color() { C_RESET='' C_BOLD='' C_DIM='' C_RED='' C_GREEN='' C_YELLOW='' C_BLUE='' C_MAGENTA='' C_CYAN=''; IS_TTY=0; }

info() { printf '%s•%s %s\n' "$C_CYAN" "$C_RESET" "$*"; }
ok()   { printf '%s✔%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn() { printf '%s⚠ %s%s\n' "$C_YELLOW" "$*" "$C_RESET" >&2; }
die()  { printf '%s✘ %s%s\n' "$C_RED" "$*" "$C_RESET" >&2; exit 1; }

hr() { printf '%s%s%s\n' "$C_DIM" "────────────────────────────────────────────────────────────────────────────────" "$C_RESET"; }

chapter() {
    echo
    printf '%s%s════════════════════════════════════════════════════════════════════════════════%s\n' "$C_BOLD" "$C_MAGENTA" "$C_RESET"
    printf '%s%s  %s%s\n' "$C_BOLD" "$C_MAGENTA" "$1" "$C_RESET"
    [[ -n ${2:-} ]] && printf '%s  %s%s\n' "$C_MAGENTA" "$2" "$C_RESET"
    printf '%s%s════════════════════════════════════════════════════════════════════════════════%s\n' "$C_BOLD" "$C_MAGENTA" "$C_RESET"
}

# Wraps text to the terminal width with a hanging indent.
wrap() { local indent=$1; shift; printf '%s\n' "$*" | fold -s -w $(( ${COLUMNS:-100} > 120 ? 110 : ${COLUMNS:-100} - 4 - ${#indent} )) | sed "2,\$s/^/${indent}/"; }

# explain_step <id> <title> <what> <why> [sql]
#   Prints the step banner, the explanation and the SQL that is about to run.
#   QUIET=1 (repeated runs) prints the title only.
explain_step() {
    local id=$1 title=$2 what=$3 why=$4 sql=${5:-} n
    echo
    printf '%s%s▶ STEP %s  %s%s\n' "$C_BOLD" "$C_BLUE" "$id" "$title" "$C_RESET"
    [[ ${QUIET:-0} == 1 ]] && return 0
    printf '%s  WHAT%s  ' "$C_BOLD" "$C_RESET"; wrap "        " "$what"
    printf '%s  WHY %s  ' "$C_BOLD" "$C_RESET"; wrap "        " "$why"
    if [[ -n $sql ]]; then
        printf '%s  SQL %s\n' "$C_BOLD" "$C_RESET"
        n=$(printf '%s\n' "$sql" | wc -l)
        if (( SQL_PREVIEW_LINES > 0 && n > SQL_PREVIEW_LINES )); then
            printf '%s\n' "$sql" | head -n "$SQL_PREVIEW_LINES" | highlight_sql
            printf '        %s… %d more lines (full SQL in the log)%s\n' "$C_DIM" $((n - SQL_PREVIEW_LINES)) "$C_RESET"
        else
            printf '%s\n' "$sql" | highlight_sql
        fi
    fi
    pause
}

highlight_sql() {
    if (( IS_TTY )); then
        sed -E "s/^/        /; s/\<(CREATE|TABLE|PROJECTION|SCHEMA|VIEW|DROP|IF|EXISTS|CASCADE|COPY|FROM|PARSER|STREAM|NAME|REJECTED|DATA|AS|FILLER|SELECT|INSERT|INTO|VALUES|MERGE|USING|ON|WHEN|MATCHED|NOT|THEN|UPDATE|SET|WHERE|AND|OR|UNION|ALL|ORDER|BY|SEGMENTED|HASH|NODES|PARTITION|LIMIT|OVER|DESC|LIKE|INCLUDING|PROJECTIONS|COMMIT|PRIMARY|KEY|CONSTRAINT|DISABLED|ANY|NODE|CROSS|JOIN|CASE|ELSE|END|NULL|REPLACE|BETWEEN|DISTINCT)\>/${C_YELLOW}\1${C_RESET}/g; s/(--.*)$/${C_DIM}\1${C_RESET}/"
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
# epoch milliseconds (bash 5 EPOCHREALTIME; date otherwise)
now_ms() {
    if [[ -n ${EPOCHREALTIME:-} ]]; then
        local t=${EPOCHREALTIME//[!0-9]/}
        echo $(( t / 1000 ))
    else
        echo $(( $(date +%s%N) / 1000000 ))
    fi
}
secs() { awk -v ms="$1" 'BEGIN{printf "%.2f", ms/1000}'; }
hms()  { awk -v ms="$1" 'BEGIN{s=ms/1000; if (s<60) printf "%.1f s", s; else printf "%dm %02ds", int(s/60), int(s)%60}'; }
rate() { awk -v r="$1" -v ms="$2" 'BEGIN{ if (ms<=0) {print "-"; exit} v=r*1000/ms; if (v>=1e6) printf "%.2fM", v/1e6; else if (v>=1e3) printf "%.0fK", v/1e3; else printf "%.0f", v }'; }

# ----------------------------------------------------------------- vsql
# vsql_exec <logfile> : runs the SQL read from stdin and stops at the first
# error. The SQL goes to <logfile>.sql, the output to <logfile>.out/.err;
# stdout = result tuples (unaligned, no headers).
vsql_exec() {
    local logf=$1 sql rc
    sql=$(cat)
    { printf -- '-- %s\n' "$(date '+%F %T')"; printf '%s\n' "$sql"; } >> "$logf.sql"
    if {
        [[ -n $RESOURCE_POOL ]] && printf 'SET SESSION RESOURCE_POOL = %s;\n' "$RESOURCE_POOL"
        printf '%s\n' "$sql"
    } | "$VSQL" -X -A -t -q -v ON_ERROR_STOP=1 2>>"$logf.err" | tee -a "$logf.out"; then
        return 0
    else
        rc=$?
    fi
    # WARNING 10596 = JSON parent keys (hdr, rec, …) with no matching column: expected
    grep -v -e 'WARNING 10596' -e '^HINT:.*UNMATCHED_KEY' "$logf.err" | tail -5 >&2
    return "$rc"
}

# single query, tuples on stdout
vsql_query() { "$VSQL" -X -A -t -q -v ON_ERROR_STOP=1 -c "$1"; }

vsql_check() {
    [[ -x $VSQL ]] || die "vsql not found at $VSQL (set VSQL in vload.env)"
    vsql_query "SELECT 1" >/dev/null 2>&1 \
        || die "cannot connect with vsql: is the database up? (admintools -t start_db -d <db>) Check VSQL_HOST/VSQL_USER/VSQL_PASSWORD/VSQL_DATABASE in vload.env"
}

table_exists() { [[ $(vsql_query "SELECT COUNT(*) FROM tables WHERE table_schema = '$SCHEMA' AND table_name = '$1'") == 1 ]]; }

# timed <label> <logfile> : runs the SQL read from stdin with a live elapsed
# time, then prints "label … seconds". Sets LAST_MS (wall-clock of the SQL,
# vsql start to exit) and LAST_OUT (the result tuples).
timed() {
    local label=$1 logf=$2 sql tmp pid start end rc=0 spin='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏' i=0
    sql=$(cat)
    tmp=$(mktemp "${TMPDIR:-/tmp}/vload.XXXXXX")
    start=$(now_ms)
    (
        if printf '%s\n' "$sql" | vsql_exec "$logf" > "$tmp.out"; then r=0; else r=$?; fi
        now_ms > "$tmp.end"; exit "$r"
    ) &
    pid=$!
    while kill -0 "$pid" 2>/dev/null; do
        (( IS_TTY )) && printf '\r  %s%s %-44s %8s s%s' "$C_CYAN" "${spin:i++%10:1}" "$label" "$(secs $(( $(now_ms) - start )))" "$C_RESET"
        sleep 0.1
    done
    wait "$pid" || rc=$?
    (( IS_TTY )) && printf '\r\033[K'
    end=$(cat "$tmp.end" 2>/dev/null || now_ms)
    LAST_MS=$(( end - start ))
    LAST_OUT=$(cat "$tmp.out" 2>/dev/null)
    rm -f "$tmp" "$tmp.out" "$tmp.end"
    (( rc == 0 )) || die "$label failed — see ${logf#"$ROOT_DIR"/}.err"
    printf '  %s⏱%s  %-44s %s%8s s%s\n' "$C_GREEN" "$C_RESET" "$label" "$C_BOLD" "$(secs "$LAST_MS")" "$C_RESET"
}

# timed_parallel <label> <logfile prefix> <n> <fn> : runs "<fn> <i> | vsql"
# for i = 1 … n in n concurrent sessions. LAST_MS = wall-clock from the start
# to the end of the last session.
timed_parallel() {
    local label=$1 logp=$2 n=$3 fn=$4 i tmpd start end e rc=0 spin='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏' k=0 alive
    local -a pids=()
    tmpd=$(mktemp -d "${TMPDIR:-/tmp}/vload.XXXXXX")
    start=$(now_ms)
    for (( i = 1; i <= n; i++ )); do
        (
            if "$fn" "$i" | vsql_exec "${logp}_$i" >/dev/null; then r=0; else r=$?; fi
            now_ms > "$tmpd/$i.end"; exit "$r"
        ) &
        pids+=($!)
    done
    while :; do
        alive=0
        for e in "${pids[@]}"; do kill -0 "$e" 2>/dev/null && alive=$((alive + 1)); done
        (( alive == 0 )) && break
        (( IS_TTY )) && printf '\r  %s%s %-44s %8s s  (%d/%d sessions running)%s' "$C_CYAN" "${spin:k++%10:1}" "$label" "$(secs $(( $(now_ms) - start )))" "$alive" "$n" "$C_RESET"
        sleep 0.1
    done
    for e in "${pids[@]}"; do wait "$e" || rc=$?; done
    (( IS_TTY )) && printf '\r\033[K'
    end=$start
    for (( i = 1; i <= n; i++ )); do
        e=$(cat "$tmpd/$i.end" 2>/dev/null || echo "$start"); (( e > end )) && end=$e
    done
    rm -rf "$tmpd"
    LAST_MS=$(( end - start ))
    (( rc == 0 )) || die "$label failed — see ${logp#"$ROOT_DIR"/}_*.err"
    printf '  %s⏱%s  %-44s %s%8s s%s  (%d parallel sessions)\n' "$C_GREEN" "$C_RESET" "$label" "$C_BOLD" "$(secs "$LAST_MS")" "$C_RESET" "$n"
}

# ----------------------------------------------------------------- generation
# partition_plan <rows> : one line per monthly partition of a table of <rows>
# rows whose created_date = START_DATE + (isn - 1) * SPAN_DAYS / rows:
#   label first_day next_month_first_day first_isn last_isn
partition_plan() {
    awk -v start="$START_DATE" -v span="$SPAN_DAYS" -v n="$1" -f /dev/stdin <<'AWK'
function dfc(y, m, d,    era, yoe, doy, doe) {            # days since 1970-01-01
    if (m <= 2) y--
    era = int((y >= 0 ? y : y - 399) / 400); yoe = y - era * 400
    doy = int((153 * (m + (m > 2 ? -3 : 9)) + 2) / 5) + d - 1
    doe = yoe * 365 + int(yoe / 4) - int(yoe / 100) + doy
    return era * 146097 + doe - 719468
}
function ceil_div(a, b) { return int((a + b - 1) / b) }
BEGIN {
    y = substr(start, 1, 4) + 0; m = substr(start, 6, 2) + 0; s0 = dfc(y, m, substr(start, 9, 2) + 0)
    while (1) {
        ny = y + (m == 12); nm = (m == 12) ? 1 : m + 1
        d0 = dfc(y, m, 1) - s0; d1 = dfc(ny, nm, 1) - s0
        if (d0 >= span) break
        if (d0 < 0) d0 = 0
        if (d1 > span) d1 = span
        lo = ceil_div(d0 * n, span) + 1; hi = ceil_div(d1 * n, span)
        if (lo <= hi) printf "%04d%02d %04d-%02d-01 %04d-%02d-01 %d %d\n", y, m, y, m, ny, nm, lo, hi
        y = ny; m = nm
    }
}
AWK
}

# run_jobs <label> <sql function> <rows> <log prefix> <sessions> <unit>
#   Runs "<fn> <job line words> | vsql" for every line of the JOBS array, at
#   most <sessions> at a time, with a live progress line. <rows> is only used
#   for the rows/s figure.
run_jobs() {
    local label=$1 fn=$2 rows=$3 logp=$4 sessions=$5 unit=$6 line running=0 done_n=0 total start fails=0 j=0
    total=${#JOBS[@]}
    start=$(now_ms)
    _progress() { (( IS_TTY )) && printf '\r  %s%-44s %3d/%d %s  %8s s%s' "$C_CYAN" "$label" "$done_n" "$total" "$unit" "$(secs $(( $(now_ms) - start )))" "$C_RESET"; return 0; }
    for line in "${JOBS[@]}"; do
        j=$((j + 1))
        # shellcheck disable=SC2086
        ( set -- $line; "$fn" "$@" | vsql_exec "${logp}_$(printf '%03d' "$j")" >/dev/null ) &
        running=$((running + 1))
        while (( running >= sessions )); do
            wait -n || fails=$((fails + 1)); running=$((running - 1)); done_n=$((done_n + 1)); _progress
        done
    done
    while (( running > 0 )); do
        wait -n || fails=$((fails + 1)); running=$((running - 1)); done_n=$((done_n + 1)); _progress
    done
    (( IS_TTY )) && printf '\r\033[K'
    LAST_MS=$(( $(now_ms) - start ))
    (( fails == 0 )) || die "$label: $fails job(s) failed — see ${logp#"$ROOT_DIR"/}_*.err"
    printf '  %s⏱%s  %-44s %s%8s s%s  (%s %s, %s sessions, %s rows/s)\n' "$C_GREEN" "$C_RESET" "$label" "$C_BOLD" "$(secs "$LAST_MS")" "$C_RESET" \
        "$total" "$unit" "$sessions" "$(rate "$rows" "$LAST_MS")"
}
