# shellcheck shell=bash
# =============================================================================
#  Method 1 — staging table + partition COPY / SWAP
# =============================================================================
#  Every dose is loaded into a side table. Only the partitions touched by the
#  dose are rebuilt there (untouched ones are linked in, metadata only), then
#  published into the fact table with one atomic SWAP_PARTITIONS_BETWEEN_TABLES.
#  The fact table never receives an UPDATE or DELETE, so it has no delete
#  vectors, and readers see the old or the new partitions, never a mix.
# =============================================================================

swap_setup_sql() {
    local t=$1
    sql_create_table "$SCHEMA" "$t" swap
    cat <<SQL
CREATE VIEW ${SCHEMA}.${t}_current AS SELECT * FROM ${SCHEMA}.${t};
SQL
}

# ---------------------------------------------------------------- base load
swap_base_copy_sql() {
    local t=$1
    cat <<SQL
CREATE TABLE ${SCHEMA}.${t}_stage LIKE ${SCHEMA}.${t} INCLUDING PROJECTIONS;
$(sql_copy "$SCHEMA" "${t}_stage" "$t" "$DATASET_DIR/$t/base/*.$FILE_EXT" "${STREAM}_${t}")
SQL
}

swap_publish_sql() {
    local t=$1
    cat <<SQL
SELECT MOVE_PARTITIONS_TO_TABLE('${SCHEMA}.${t}_stage', '190001', '299912', '${SCHEMA}.${t}');
DROP TABLE ${SCHEMA}.${t}_stage;
SELECT COUNT(*) FROM ${SCHEMA}.${t};
SQL
}

# ---------------------------------------------------------------- doses
swap_delta_copy_sql() {
    local t=$1
    cat <<SQL
DROP TABLE IF EXISTS ${SCHEMA}.${t}_delta;
CREATE TABLE ${SCHEMA}.${t}_delta LIKE ${SCHEMA}.${t} INCLUDING PROJECTIONS;
$(sql_copy "$SCHEMA" "${t}_delta" "$t" "$DATASET_DIR/$t/$DOSE_DIR/*.$FILE_EXT" "${STREAM}_${t}")
SQL
}

# partition keys touched by the delta, one per line
swap_touched() {
    if [[ $DRY_RUN == 1 ]]; then printf '%s\n' 202512 202601; return; fi
    vsql_query "SELECT DISTINCT ${PART_EXPR} FROM ${SCHEMA}.${1}_delta ORDER BY 1"
}

months_between() { # 202511 202602 -> 4 (inclusive)
    echo $(( (${2:0:4} * 12 + 10#${2:4:2}) - (${1:0:4} * 12 + 10#${1:4:2}) + 1 ))
}

swap_rebuild_sql() {
    local t=$1 touched pmin pmax p
    mapfile -t touched < <(swap_touched "$t")
    (( ${#touched[@]} > 0 )) || { echo "SELECT 0;"; return; }
    pmin=${touched[0]}; pmax=${touched[-1]}
    echo "CREATE TABLE ${SCHEMA}.${t}_stage LIKE ${SCHEMA}.${t} INCLUDING PROJECTIONS;"
    if (( $(months_between "$pmin" "$pmax") > ${#touched[@]} )); then
        # untouched partitions inside [pmin, pmax] must survive the range swap:
        # link them into the stage table (metadata only), drop the touched ones
        echo "SELECT COPY_PARTITIONS_TO_TABLE('${SCHEMA}.${t}', '${pmin}', '${pmax}', '${SCHEMA}.${t}_stage');"
        for p in "${touched[@]}"; do
            echo "SELECT DROP_PARTITIONS('${SCHEMA}.${t}_stage', '${p}', '${p}');"
        done
    fi
    cat <<SQL
INSERT INTO ${SCHEMA}.${t}_stage
SELECT f.*
  FROM ${SCHEMA}.${t} f
 WHERE ($(part_date_predicate "${touched[@]}"))
   AND NOT EXISTS (SELECT 1 FROM ${SCHEMA}.${t}_delta d WHERE d.isn = f.isn)
UNION ALL
SELECT *
  FROM ${SCHEMA}.${t}_delta
 WHERE op_code <> 'D';
COMMIT;
SQL
}

swap_swap_sql() {
    local t=$1 touched
    mapfile -t touched < <(swap_touched "$t")
    (( ${#touched[@]} > 0 )) || { echo "SELECT 0;"; return; }
    cat <<SQL
SELECT SWAP_PARTITIONS_BETWEEN_TABLES('${SCHEMA}.${t}_stage', '${touched[0]}', '${touched[-1]}', '${SCHEMA}.${t}');
DROP TABLE ${SCHEMA}.${t}_stage, ${SCHEMA}.${t}_delta;
SQL
}

# ---------------------------------------------------------------- flow
method_swap() {
    local t0=${TABLES[0]}

    STEP=1.1; STREAM=""
    explain_step 1.1 "Create the fact tables" \
        "One partitioned fact table per ADABAS file. Partitions are months of the immutable created_date. The ISN is declared as the primary key (not enforced) and is the sort and segmentation key." \
        "Partitions are the unit of work for this method: loading, rebuilding and publishing all happen per partition, and partition operations are metadata-only. Because the partition key never changes, an update always stays in its partition." \
        "$(swap_setup_sql "$t0")"
    w_setup() { swap_setup_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.setup" >/dev/null && echo 0; }
    par_tables setup "create tables" w_setup

    STEP=2.1; STREAM="vl_${RUN_TAG}_base"
    explain_step 2.1 "Base load: COPY the JSON files into staging tables (10 tables in parallel)" \
        "COPY with FJSONPARSER reads all JSON files of a table in parallel (one parse thread per file). FILLER columns receive the flattened JSON keys (rec.address.0.city) and are mapped onto the relational columns." \
        "COPY is the fastest way to bring data into Vertica: it writes sorted, compressed ROS containers directly to disk, with no per-row transaction cost. Loading into a stage table keeps the fact table untouched and consistent until the data is validated and published." \
        "$(swap_base_copy_sql "$t0")"
    w_base() { swap_base_copy_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.base_copy"; }
    par_tables base_copy "base COPY" w_base "$STREAM"

    STEP=2.2; STREAM=""
    explain_step 2.2 "Publish: MOVE_PARTITIONS_TO_TABLE stage → fact" \
        "Moves every partition of the stage table into the fact table. This is a catalog operation: no data is read or rewritten." \
        "Publishing is atomic and takes milliseconds whatever the volume, so queries never see a half-loaded table." \
        "$(swap_publish_sql "$t0")"
    w_publish() { swap_publish_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.publish"; }
    par_tables publish "publish" w_publish

    local d
    for (( d = 1; d <= DOSES; d++ )); do
        DOSE=$d; DOSE_DIR=$(printf 'dose_%02d' "$d")
        chapter "SWAP · dose $d of $DOSES" "$(fmt_num "$DOSE_ROWS") changes per table: $(fmt_num "$N_UPD") updates, $(fmt_num "$N_DEL") deletes, $(fmt_num "$N_INS") inserts"

        STEP=$((d + 2)).1; STREAM="vl_${RUN_TAG}_d${d}"
        explain_step "$STEP" "COPY dose $d into delta tables" \
            "Loads this dose's CDC records (I/U/D after-images) into a delta table with the same projections as the fact table." \
            "Identical segmentation and sort order (HASH(isn) / ORDER BY isn) mean the anti-join in the next step is a local merge join with no data shuffling between nodes." \
            "$(swap_delta_copy_sql "$t0")"
        w_delta() { swap_delta_copy_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.dose${DOSE}_copy"; }
        par_tables "dose${d}_copy" "dose $d COPY" w_delta "$STREAM"

        STEP=$((d + 2)).2; STREAM=""
        explain_step "$STEP" "Rebuild only the touched partitions in a stage table" \
            "Finds the partitions the dose touches. For those partitions only, writes the fact rows that did not change plus the new images; deleted rows are left out. Untouched partitions inside the swap range are linked in with COPY_PARTITIONS_TO_TABLE (metadata only)." \
            "Updates and deletes become a sequential INSERT…SELECT, so there are no delete vectors and no later purge. The cost is proportional to the touched partitions, not to the whole table. The method shines when changes concentrate in recent partitions (HOT_PCT=${HOT_PCT}%)." \
            "$(swap_rebuild_sql "$t0")"
        w_rebuild() { swap_rebuild_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.dose${DOSE}_rebuild"; }
        par_tables "dose${d}_rebuild" "dose $d rebuild" w_rebuild

        STEP=$((d + 2)).3
        explain_step "$STEP" "Atomic SWAP_PARTITIONS_BETWEEN_TABLES stage ⇄ fact" \
            "Exchanges the rebuilt partitions with the fact table's partitions in a single transaction, then drops the stage table (which now holds the old partitions) and the delta table." \
            "The swap is a catalog operation, so it is instant and atomic. It only needs a short lock on the fact table, so readers keep running." \
            "$(swap_swap_sql "$t0")"
        w_swap() { swap_swap_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.dose${DOSE}_swap" >/dev/null && echo "$DOSE_ROWS"; }
        par_tables "dose${d}_swap" "dose $d swap" w_swap
    done
}
