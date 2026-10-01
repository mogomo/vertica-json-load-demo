#!/usr/bin/env bash
# =============================================================================
#  clean.sh — remove the demo from the database (and, with --all, the files)
# =============================================================================
#  Usage: ./clean.sh          drop the method tables, keep the generated data
#         ./clean.sh --all    drop the whole schema and delete demo/, logs/, reports/
# =============================================================================
set -o errexit -o nounset -o pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=/dev/null
[[ -f $ROOT_DIR/vload.env ]] && . "$ROOT_DIR/vload.env"
# shellcheck source=lib/common.sh
. "$ROOT_DIR/lib/common.sh"

ALL=0
case ${1:-} in
    --all) ALL=1 ;;
    "") ;;
    -h|--help) sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown option '$1'" ;;
esac
load_config
vsql_check
if (( ALL )); then
    vsql_query "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE" >/dev/null && info "dropped schema ${SCHEMA}"
    rm -rf "$DEMO_DIR" "$LOG_DIR" "$REPORT_DIR" && info "deleted ${DEMO_DIR#"$ROOT_DIR"/}/, ${LOG_DIR#"$ROOT_DIR"/}/, ${REPORT_DIR#"$ROOT_DIR"/}/"
else
    vsql_query "DROP TABLE IF EXISTS ${SCHEMA}.txn_upsert, ${SCHEMA}.txn_swap, ${SCHEMA}.txn_swap_delta, ${SCHEMA}.txn_swap_stage,
                ${SCHEMA}.txn_merge, ${SCHEMA}.txn_merge_delta, ${SCHEMA}.txn_rejects_upsert, ${SCHEMA}.txn_rejects_swap,
                ${SCHEMA}.txn_rejects_merge CASCADE" >/dev/null
    info "dropped the method tables; txn_base, txn_jrn_base and the JSON files are kept"
fi
ok "clean"
