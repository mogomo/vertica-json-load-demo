# shellcheck shell=bash
# =============================================================================
#  Method 2 — optimized MERGE
# =============================================================================
#  The base load is a plain COPY straight into the target (a MERGE of a billion
#  new rows would only add a join). Every dose is COPYed into a delta table
#  and applied with one MERGE statement that meets the optimization rules, so
#  Vertica runs it as a DELETE + INSERT over a presorted merge join instead of
#  the generic outer-join MERGE plan.
#  Deletes are soft (op_code = 'D') so that a single MERGE handles I/U/D.
# =============================================================================

merge_setup_sql() {
    local t=$1
    sql_create_table "$SCHEMA" "$t" merge
    cat <<SQL
CREATE VIEW ${SCHEMA}.${t}_current AS SELECT * FROM ${SCHEMA}.${t} WHERE op_code <> 'D';
SQL
}

merge_base_copy_sql() {
    local t=$1
    sql_copy "$SCHEMA" "$t" "$t" "$DATASET_DIR/$t/base" "${STREAM}_${t}"
}

merge_delta_copy_sql() {
    local t=$1
    cat <<SQL
DROP TABLE IF EXISTS ${SCHEMA}.${t}_delta;
CREATE TABLE ${SCHEMA}.${t}_delta LIKE ${SCHEMA}.${t} INCLUDING PROJECTIONS;
$(sql_copy "$SCHEMA" "${t}_delta" "$t" "$DATASET_DIR/$t/$DOSE_DIR" "${STREAM}_${t}")
SQL
}

merge_apply_sql() {
    local t=$1
    sql_merge "$SCHEMA" "$t" "${t}_delta"
    cat <<SQL
COMMIT;
DROP TABLE ${SCHEMA}.${t}_delta;
SQL
}

# Prints the access path of the MERGE and whether it is the optimized plan.
merge_show_plan() {
    local t=$1 plan
    [[ $DRY_RUN == 1 ]] && return
    plan=$(sql_merge "$SCHEMA" "$t" "${t}_delta" | sed 's/^MERGE/EXPLAIN MERGE/' | "$VSQL" -X -A -t -q 2>/dev/null)
    printf '%s  EXPLAIN (access path):%s\n' "$C_BOLD" "$C_RESET"
    printf '%s\n' "$plan" | grep -E '^ ?(\+-|\| ?\+|\|  ?(Target Projection|Join Cond))' | grep -v -- '-->.*STORAGE ACCESS for s ' \
        | head -12 | sed "s/^/        /"
    if printf '%s\n' "$plan" | grep -q 'DML MERGE'; then
        warn "the plan contains a 'DML MERGE' operator: this MERGE is NOT optimized"
    else
        ok "optimized MERGE: DELETE + INSERT with a presorted merge join (no 'DML MERGE' operator, no outer join)"
    fi
}

method_merge() {
    local t0=${TABLES[0]}

    STEP=1.1; STREAM=""
    explain_step 1.1 "Create the target tables" \
        "Target tables have the same shape as in the other methods, with a PRIMARY KEY on the ISN. The key is declared but not enforced, so loads pay nothing for it. A view hides soft-deleted rows." \
        "The declared key is the first condition for an optimized MERGE: it tells the optimizer that each source row matches at most one target row." \
        "$(merge_setup_sql "$t0")"
    w_setup() { merge_setup_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.setup" >/dev/null && echo 0; }
    par_tables setup "create tables" w_setup

    STEP=2.1; STREAM="vl_${RUN_TAG}_base"
    explain_step 2.1 "Base load: COPY the JSON files directly into the targets (10 tables in parallel)" \
        "The initial load is a plain COPY into the target table. FILLER columns map the flattened JSON hierarchy onto the columns." \
        "There is nothing to match yet, so a MERGE would only add a join. COPY writes sorted, compressed containers directly: this is the fastest possible path." \
        "$(merge_base_copy_sql "$t0")"
    w_base() { merge_base_copy_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.base_copy" | sum_rows; }
    par_tables base_copy "base COPY" w_base "$STREAM"

    local d
    for (( d = 1; d <= DOSES; d++ )); do
        DOSE=$d; DOSE_DIR=$(printf 'dose_%02d' "$d")
        chapter "MERGE · dose $d of $DOSES" "$(fmt_num "$DOSE_ROWS") changes per table: $(fmt_num "$N_UPD") updates, $(fmt_num "$N_DEL") deletes, $(fmt_num "$N_INS") inserts"

        STEP=$((d + 2)).1; STREAM="vl_${RUN_TAG}_d${d}"
        explain_step "$STEP" "COPY dose $d into delta tables" \
            "Loads this dose's CDC after-images into a delta table created LIKE the target, INCLUDING PROJECTIONS." \
            "Same segmentation (HASH(isn)) and sort order (isn) as the target, so the MERGE join is a local, presorted merge join: no sort, no hash table, no network shuffle." \
            "$(merge_delta_copy_sql "$t0")"
        w_delta() { merge_delta_copy_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.dose${DOSE}_copy" | sum_rows; }
        par_tables "dose${d}_copy" "dose $d COPY" w_delta "$STREAM"

        STEP=$((d + 2)).2
        explain_step "$STEP" "Optimized MERGE delta → target" \
            "A single MERGE applies inserts, updates and soft deletes (op_code='D'). UPDATE SET and INSERT list every target column with the same source values." \
            "This meets all three optimization rules (declared key, all columns, identical values), so Vertica plans a DELETE + INSERT over a merge join instead of the generic MERGE operator: typically several times faster. Trade-off: updated rows leave delete vectors behind, which the Tuple Mover purges later." \
            "$(sql_merge "$SCHEMA" "$t0" "${t0}_delta")"
        merge_show_plan "$t0"
        w_merge() { merge_apply_sql "$1" | vsql_exec "$RUN_LOG_DIR/$1.dose${DOSE}_merge"; }
        par_tables "dose${d}_merge" "dose $d MERGE" w_merge
    done

    if [[ $DRY_RUN != 1 ]]; then
        local dv
        dv=$(vsql_query "SELECT COALESCE(SUM(deleted_row_count),0) FROM v_monitor.delete_vectors WHERE schema_name = '${SCHEMA}'")
        info "delete vectors left by MERGE in ${SCHEMA}: $(fmt_num "${dv:-0}") rows (purged later by the Tuple Mover, or with PURGE_TABLE())"
    fi
}
