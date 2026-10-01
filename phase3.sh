#!/usr/bin/env bash
# =============================================================================
#  phase3.sh — nested JSON documents (one ADABAS transaction per line, with an
#  array of changed records for each file) → 10 parallel optimized MERGEs
# =============================================================================
#  The 1-billion-row fact table and 9 tables of 60 million rows each receive
#  1 million JSON changes per table (50% updates, 50% inserts). The timer
#  covers the whole job: ONE COPY that parses every JSON file once into a
#  staging table, then one optimized MERGE per table reading its rows from it.
#  --parallel sets how many MERGEs run at the same time; a list (10,5,1) runs
#  every setting, 1 = one table after the other.
#
#  Run ./generate.sh and ./generate_multi.sh first. Safe to run again and again.
#  Usage: ./phase3.sh [--parallel 10|5|10,5,1] [--runs N] [--pause] [--no-check] [--no-color]
# =============================================================================
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PHASE=3
# shellcheck source=lib/phase23.sh
. "$ROOT_DIR/lib/phase23.sh"
