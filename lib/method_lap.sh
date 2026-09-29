# shellcheck shell=bash
# =============================================================================
#  Method 3 — insert-only journal + Top-K Live Aggregate Projection (LAP)
# =============================================================================
#  The anchor table is append-only: every CDC image (I, U and D) is INSERTed
#  with its change timestamp. A Top-K LAP
#      LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)
#  keeps the newest version per ISN, pre-aggregated as data is loaded.
#  Queries read the latest version from the LAP; the anchor keeps the full
#  version history (who/what/when) for audit and debugging.
#  From time to time the journal is compacted with lap_purge (see below).
# =============================================================================

lap_topk_select() {   # the Top-K query; Vertica rewrites it to read the LAP
    local t=$1 name=${2:-$1}
    printf 'SELECT %s\n  FROM %s.%s\n LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)' \
        "$(sql_topk_column_list "$t")" "$SCHEMA" "$name"
}

lap_view_sql() {
    local t=$1
    cat <<SQL
CREATE OR REPLACE VIEW ${SCHEMA}.${t}_current AS
SELECT $(sql_column_list "$t")
  FROM (
$(lap_topk_select "$t" | sed 's/^/    /')
) last_version
WHERE op_code <> 'D';
SQL
}

lap_setup_sql() {
    local t=$1
    sql_create_table "$SCHEMA" "$t" lap
    sql_create_topk "$SCHEMA" "$t"
    lap_view_sql "$t"
}

lap_copy_sql() {
    local t=$1 dir=$2
    sql_copy "$SCHEMA" "$t" "$t" "$DATASET_DIR/$t/$dir/*.$FILE_EXT" "${STREAM}_${t}"
}

method_lap() {
    local t0=${TABLES[0]}

    STEP=1.1; STREAM=""
    explain_step 1.1 "Create the journal tables, their Top-K LAPs and the 'current' views" \
        "The anchor (journal) table is sorted by (isn, change_ts) and only ever receives INSERTs. The Top-K projection keeps one row per ISN: the one with the newest change_ts. The view reads the LAP and hides tombstones (op_code='D')." \
        "When an optimized MERGE is not possible, or the version history must be kept, this replaces every UPDATE/DELETE with a cheap INSERT. Vertica maintains the LAP incrementally at load time, so reading the latest version needs no GROUP BY or window function over the whole history." \
        "$(lap_setup_sql "$t0")"
    w_setup() { lap_setup_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.setup" >/dev/null && echo 0; }
    par_tables setup "create tables" w_setup

    STEP=2.1; STREAM="vl_${RUN_TAG}_base"
    explain_step 2.1 "Base load: COPY the JSON files into the journal tables (10 tables in parallel)" \
        "A plain COPY into the anchor table. While loading, Vertica also computes the Top-K rows for the LAP." \
        "The same COPY speed as the other methods, plus the cost of maintaining the LAP (a second, pre-aggregated copy of the data). You pay that at load time instead of at every query." \
        "$(lap_copy_sql "$t0" base)"
    w_base() { lap_copy_sql "$1" base | vsql_exec "$RUN_LOG_DIR/$1.base_copy"; }
    par_tables base_copy "base COPY" w_base "$STREAM"

    local d
    for (( d = 1; d <= DOSES; d++ )); do
        DOSE=$d; DOSE_DIR=$(printf 'dose_%02d' "$d")
        chapter "LAP · dose $d of $DOSES" "$(fmt_num "$DOSE_ROWS") changes per table: $(fmt_num "$N_UPD") updates, $(fmt_num "$N_DEL") deletes, $(fmt_num "$N_INS") inserts"

        STEP=$((d + 2)).1; STREAM="vl_${RUN_TAG}_d${d}"
        explain_step "$STEP" "COPY dose $d straight into the journal (updates and deletes are INSERTs)" \
            "Updates arrive as new versions and deletes as tombstone versions (op_code='D'). They are appended with a plain COPY. That's the whole apply step." \
            "There's no delta table, join, DELETE or delete vectors. Applying a dose costs the same as loading new data, and the old versions stay available as history. The LAP picks the newest version per ISN." \
            "$(lap_copy_sql "$t0" "$DOSE_DIR")"
        w_dose() { lap_copy_sql "$1" "$DOSE_DIR" | vsql_exec "$RUN_LOG_DIR/$1.dose${DOSE}_copy"; }
        par_tables "dose${d}_copy" "dose $d COPY" w_dose "$STREAM"
    done

    [[ $DRY_RUN == 1 ]] && return
    local isn
    isn=$(vsql_query "SELECT isn FROM ${SCHEMA}.${t0} WHERE batch_id = 1 AND op_code = 'U' ORDER BY isn LIMIT 1")
    if [[ -n $isn ]]; then
        explain_step "H" "Version history of one ${t0} record (ISN ${isn})" \
            "The anchor table keeps every version; the view returns only the newest one." \
            "This gives data versioning for free: the insert, update and delete history of every ISN, with its time, for audit and debugging." \
            "SELECT isn, op_code, change_ts, batch_id FROM ${SCHEMA}.${t0} WHERE isn = ${isn} ORDER BY change_ts;"
        "$VSQL" -X -c "SELECT isn, op_code, change_ts, batch_id FROM ${SCHEMA}.${t0} WHERE isn = ${isn} ORDER BY change_ts;" | sed 's/^/        /'
        "$VSQL" -X -c "SELECT isn, op_code, change_ts, batch_id FROM ${SCHEMA}.${t0}_current WHERE isn = ${isn};" | sed 's/^/        /'
        printf '%s  EXPLAIN SELECT … FROM %s_current (the view is answered from the LAP):%s\n' "$C_BOLD" "$t0" "$C_RESET"
        vsql_query "EXPLAIN SELECT * FROM ${SCHEMA}.${t0}_current" | grep -E 'TopK Optimized|Rewritten TOPK|Projection:' | sed 's/^ *[|+-]* */        /'
    fi
}

# =============================================================================
#  Journal purge (compaction) — run every few months
# =============================================================================
#  Step 1  create a new table + LAP and fill it with the latest live version
#          of every ISN (tombstones are dropped for good)
#  Step 2  atomically swap the table names (ALTER TABLE a, b RENAME TO …)
#          and re-point the view
#  Step 3  drop the old journal with CASCADE (its projections and LAP go too)
#  Step 4  (re-create the LAP) is already done: the LAP of the new table was
#          created empty in step 1 and maintained by the INSERT
#  Steps 2-3 replace "DROP base / RENAME new": the rename is atomic, so readers
#  never find the table missing. Vertica renames the table's projections
#  (<table>_super, <table>_topk) together with the table.
# =============================================================================
lap_purge_sql() {
    local t=$1
    cat <<SQL
-- Step 1: new journal = last live version of every ISN
$(sql_create_table "$SCHEMA" "$t" lap "${t}__new")
$(sql_create_topk "$SCHEMA" "$t" "${t}__new")
INSERT INTO ${SCHEMA}.${t}__new
SELECT * FROM ${SCHEMA}.${t}_current;
COMMIT;
-- Step 2: atomic name swap, re-point the view
ALTER TABLE ${SCHEMA}.${t}, ${SCHEMA}.${t}__new RENAME TO ${t}__old, ${t};
$(lap_view_sql "$t")
-- Step 3: drop the old journal, its projections and its LAP
DROP TABLE ${SCHEMA}.${t}__old CASCADE;
-- Step 4: nothing to re-create: the new LAP was built empty in step 1 and
-- filled by the INSERT, and the projections were renamed with their table
SELECT COUNT(*) FROM ${SCHEMA}.${t};
SQL
}

method_lap_purge() {
    local t0=${TABLES[0]}
    chapter "LAP · journal purge (compaction)" "Keep only the latest live version per ISN; continue INSERTing afterwards as usual"
    explain_step P.1 "Compact the journals: CTAS from the LAP, atomic rename, drop old" \
        "Builds a new journal holding only the newest live version of each ISN, read from the LAP. The new table is swapped in by name atomically, then the old journal and its LAP are dropped." \
        "The journal grows with every dose, because old versions are hidden, not removed. Compacting every few months keeps storage and LAP maintenance small, and removes tombstones. Since the rename is atomic, queries keep working throughout." \
        "$(lap_purge_sql "$t0")"
    w_purge() { lap_purge_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.purge"; }
    par_tables purge "journal purge" w_purge
}
