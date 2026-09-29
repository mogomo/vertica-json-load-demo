# shellcheck shell=bash
# =============================================================================
#  generate.sh — build the JSON data set: 1 base load + N CDC doses per table
# =============================================================================
#  Layout:  $DATA_DIR/<scale>/<table>/base/part_0001.json.zst
#           $DATA_DIR/<scale>/<table>/dose_01/part_0001.json.zst
#           $DATA_DIR/<scale>/manifest.env
#
#  Several files per table and stream let one COPY statement parse files in
#  parallel (one file = one parse thread). Generation itself runs one awk
#  process per file, GEN_JOBS at a time.
# =============================================================================

pick_awk() {
    local a
    for a in mawk gawk awk; do command -v "$a" >/dev/null && { echo "$a"; return; }; done
    die "no awk found"
}

compress_cmd() {
    case $COMPRESSION in
        zstd) command -v zstd >/dev/null || die "zstd not installed (apt install zstd) or set COMPRESSION=gzip"
              echo "zstd -q -1 -T1 -c" ;;
        gzip) if command -v pigz >/dev/null; then echo "pigz -1 -p 1 -c"; else echo "gzip -1 -c"; fi ;;
        none) echo "cat" ;;
    esac
}

decompress() {
    case $COMPRESSION in
        zstd) zstd -dc "$1" ;;
        gzip) gzip -dc "$1" ;;
        none) cat "$1" ;;
    esac
}

# rows per table for one file stream -> number of files
files_for() {
    local rows=$1 f
    if [[ $FILES_PER_TABLE == auto ]]; then
        # enough files that 10 concurrent COPYs keep every core busy
        f=$(( (NCPU + 9) / 10 * 2 ))
    else
        f=$FILES_PER_TABLE
    fi
    local max=$(( (rows + 999) / 1000 ))          # at least ~1000 rows per file
    (( f > max )) && f=$max
    (( f < 1 )) && f=1
    echo "$f"
}

cmd_generate() {
    local total=$1 label per_table dir awk_bin comp
    label=$(scale_label "$total")
    per_table=$(( total / ${#TABLES[@]} ))
    dir="$DATA_DIR/$label"
    awk_bin=$(pick_awk)
    comp=$(compress_cmd)

    local mix_u mix_d mix_i dose_rows n_upd n_del n_ins hot_size
    IFS=: read -r mix_u mix_d mix_i <<< "$DOSE_MIX"
    dose_rows=$(( per_table * DOSE_PCT / 100 )); (( dose_rows < 1 )) && dose_rows=1
    n_upd=$(( dose_rows * mix_u / 100 ))
    n_del=$(( dose_rows * mix_d / 100 ))
    n_ins=$(( dose_rows - n_upd - n_del ))
    hot_size=$(( per_table * HOT_PCT / 100 ))
    (( n_upd + n_del <= hot_size )) || die "dose updates+deletes ($((n_upd + n_del))) exceed the hot window ($hot_size rows) — raise HOT_PCT or lower DOSE_PCT"

    chapter "GENERATE  $label rows of hierarchical ADABAS-style JSON" \
            "${#TABLES[@]} tables × $(fmt_num "$per_table") base rows + $DOSES doses × $(fmt_num "$dose_rows") changes (U:D:I = $DOSE_MIX)"

    # disk estimate: ~470 bytes/row raw; zstd ~6x, gzip ~5x
    local est_rows=$(( total + DOSES * dose_rows * ${#TABLES[@]} )) ratio=1 need free
    case $COMPRESSION in zstd) ratio=6 ;; gzip) ratio=5 ;; esac
    need=$(( est_rows * 470 / ratio / 1024 / 1024 ))
    mkdir -p "$DATA_DIR"
    free=$(df -Pm "$DATA_DIR" | awk 'NR==2{print $4}')
    info "estimated size: ~$(fmt_num "$need") MB ($COMPRESSION), free in $DATA_DIR: $(fmt_num "$free") MB"
    (( need < free )) || die "not enough disk space in $DATA_DIR"

    if [[ -f $dir/manifest.env ]]; then
        warn "data set $label already exists in $dir — regenerating"
    fi
    rm -rf "$dir"; mkdir -p "$dir"

    # ---- build the job list: table stream file k0 k1
    local jobs=() t ti=0 stream nfiles rows f k0 k1 d
    for t in "${TABLES[@]}"; do
        ti=$((ti + 1))
        for (( d = 0; d <= DOSES; d++ )); do
            if (( d == 0 )); then stream=base; rows=$per_table; else stream=$(printf 'dose_%02d' "$d"); rows=$dose_rows; fi
            nfiles=$(files_for "$rows")
            mkdir -p "$dir/$t/$stream"
            for (( f = 0; f < nfiles; f++ )); do
                k0=$(( rows * f / nfiles )); k1=$(( rows * (f + 1) / nfiles ))
                jobs+=("$t $ti $stream $d $((f + 1)) $k0 $k1")
            done
        done
    done

    info "writing ${#jobs[@]} files with $GEN_JOBS parallel generators ($awk_bin → ${comp%% *})"
    local start end done_n=0 j running=0
    start=$(now_ms)
    for j in "${jobs[@]}"; do
        # shellcheck disable=SC2086
        set -- $j
        (
            local mode=base
            [[ $4 != 0 ]] && mode=dose
            "$awk_bin" -v defs="$DEFS_FILE" -v tbl="$1" -v mode="$mode" -v dose="$4" \
                -v base_rows="$per_table" -v k0="$6" -v k1="$7" \
                -v n_upd="$n_upd" -v n_del="$n_del" -v n_ins="$n_ins" -v hot_pct="$HOT_PCT" \
                -v start_date="$START_DATE" -v span_days="$SPAN_DAYS" \
                -v seed="$(( SEED + $2 * 1000003 + $4 * 7919 + $5 * 104729 ))" \
                -f "$LIB_DIR/gen_json.awk" | $comp > "$dir/$1/$3/$(printf 'part_%04d' "$5").$FILE_EXT"
        ) &
        running=$((running + 1))
        if (( running >= GEN_JOBS )); then
            wait -n || die "generator failed"
            running=$((running - 1)); done_n=$((done_n + 1))
            (( IS_TTY )) && printf '\r  %s%d/%d files  %ss%s' "$C_CYAN" "$done_n" "${#jobs[@]}" "$(secs $(( $(now_ms) - start )))" "$C_RESET"
        fi
    done
    while (( running > 0 )); do
        wait -n || die "generator failed"
        running=$((running - 1)); done_n=$((done_n + 1))
        (( IS_TTY )) && printf '\r  %s%d/%d files  %ss%s' "$C_CYAN" "$done_n" "${#jobs[@]}" "$(secs $(( $(now_ms) - start )))" "$C_RESET"
    done
    (( IS_TTY )) && printf '\r\033[K'
    end=$(now_ms)

    cat > "$dir/manifest.env" <<EOF
# generated $(date '+%F %T') by vload.sh generate
SCALE_ROWS=$total
SCALE_LABEL=$label
ROWS_PER_TABLE=$per_table
TABLE_LIST="${TABLES[*]}"
DOSES=$DOSES
DOSE_PCT=$DOSE_PCT
DOSE_MIX=$DOSE_MIX
DOSE_ROWS=$dose_rows
N_UPD=$n_upd
N_DEL=$n_del
N_INS=$n_ins
HOT_PCT=$HOT_PCT
START_DATE=$START_DATE
SPAN_DAYS=$SPAN_DAYS
SEED=$SEED
COMPRESSION=$COMPRESSION
FILE_EXT=$FILE_EXT
EOF
    local size
    size=$(du -sm "$dir" | awk '{print $1}')
    ok "generated $(fmt_num "$est_rows") records in ${#jobs[@]} files, $(fmt_num "$size") MB, in $(secs $((end - start))) s ($(rate "$est_rows" $((end - start))) rows/s)"
    info "sample record (${TABLES[0]}, base):"
    local f="$dir/${TABLES[0]}/base/part_0001.$FILE_EXT" sample
    # head closes the pipe early: the decompressor's SIGPIPE is expected
    sample=$(set +o pipefail; decompress "$f" 2>/dev/null | head -n 1)
    if command -v python3 >/dev/null; then
        printf '%s\n' "$sample" | python3 -m json.tool | sed 's/^/    /'
    else
        printf '    %s\n' "$sample"
    fi
}

# Loads $DATA_DIR/<label>/manifest.env into the environment
load_manifest() {
    local label=$1 m="$DATA_DIR/$1/manifest.env"
    [[ -f $m ]] || die "no data set '$label' in $DATA_DIR — run: ./vload.sh generate --scale $label"
    # shellcheck disable=SC1090
    . "$m"
    DATASET_DIR="$DATA_DIR/$label"
}
