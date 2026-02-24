#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="${OUT_DIR:-target/helixc_alloc_profile}"
mkdir -p "$OUT_DIR"

STAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$OUT_DIR/alloc_profile_${STAMP}.log"

if [[ "$(uname -s)" == "Darwin" ]]; then
  TIME_ARGS=("-l")
else
  TIME_ARGS=("-v")
fi

run_profile() {
  local label="$1"
  shift

  {
    echo
    echo "=== ${label} ==="
    echo "Command: $*"
  } | tee -a "$LOG_FILE"

  /usr/bin/time "${TIME_ARGS[@]}" "$@" 2>&1 | tee -a "$LOG_FILE"
}

run_profile \
  "parser_fixtures_parse" \
  cargo test --features compiler helixc::parser::tests::benchmark_fixtures_parse_cleanly -- --exact --nocapture

run_profile \
  "analyzer_query_validation" \
  cargo test --features compiler helixc::analyzer::methods::query_validation::tests::test_query_with_traversal_and_filtering -- --exact --nocapture

run_profile \
  "compiler_bench_quick" \
  cargo bench --features compiler --bench helixc_compiler_benches -- --quick

echo
echo "Allocation profile log written to: ${LOG_FILE}"
