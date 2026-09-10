#!/usr/bin/env bash
# Local coverage and conformance gate. Produces no dated or published artifacts.
set -euo pipefail
export HELIX_TELEMETRY_LEVEL=off
export HELIX_NO_UPDATE_CHECK=1

CYPHER_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$CYPHER_REPO_ROOT"
mkdir -p target
case "${1:-}" in
    "") CYPHER_COVERAGE_ARGS=(-p helix-cypher -p helix-opencypher-tck); CYPHER_FULL_COVERAGE=false ;;
    --workspace) CYPHER_COVERAGE_ARGS=(--workspace --all-targets); CYPHER_FULL_COVERAGE=true ;;
    *) echo "usage: scripts/cypher-coverage.sh [--workspace]" >&2; exit 2 ;;
esac
cargo llvm-cov clean --workspace
cargo llvm-cov --no-report --locked "${CYPHER_COVERAGE_ARGS[@]}"
if "$CYPHER_FULL_COVERAGE"; then
    # Exercise real production-linked interpreter paths, including storage
    # failures and snapshot contracts that differ from the unit-test build.
    # Keep the existing workspace coverage denominator on its default features.
    # The additional production feature exposes other benchmark/test support;
    # verify it separately instead of mixing those modules into that denominator.
    cargo test --locked -p db --features production-coverage \
        --test production_internal_contracts
    # Exercise the existing ignored topology conflict/preparation test at a
    # bounded local size; its source is included in the repository's denominator.
    HELIX_SWEEP_BENCH_EXPIRED=1000 HELIX_SWEEP_BENCH_INSERTED=100 \
    HELIX_SWEEP_BENCH_SWEEP_CHUNK=100 HELIX_SWEEP_BENCH_WRITER_CHUNK=25 \
    HELIX_SWEEP_BENCH_SAMPLES=3 cargo llvm-cov --no-report --locked -p db --lib -- \
        benchmark_exclusive_vs_disjoint_linked_event_sweep --ignored
fi
cargo llvm-cov run --no-report --locked -p helix-opencypher-tck -- --gate --output target/cypher-tck-coverage.json
cargo llvm-cov report --json --output-path target/cypher-coverage.json \
    --ignore-filename-regex '(^|/)(tests|benches|examples)/|/test_support.rs$|/(registry|rustc)/'
jq --argjson workspace "$CYPHER_FULL_COVERAGE" '
  .data[0].files as $files |
  [.data[0].files[]
   | select(.filename | test("/(crates/cypher|tools/opencypher-tck)/src/"))
   | {module: .filename, lines: .summary.lines}]
  | . as $modules
  | (if $workspace then [
        {scope:"planner",needle:"/crates/planner/src/",minimum:95},
        {scope:"db",needle:"/crates/db/src/",minimum:94},
        {scope:"interpreter",needle:"/crates/db/src/execution/interpreter/",minimum:98},
        {scope:"index_lifecycle",needle:"/crates/db/src/index_lifecycle/",minimum:93},
        {scope:"search",needle:"/crates/db/src/search/",minimum:93}
      ] | map(. as $scope | [$files[] | select(.filename | contains($scope.needle))] as $selected
          | ($selected | map(.summary.lines.count) | add // 0) as $count
          | ($selected | map(.summary.lines.covered) | add // 0) as $covered
          | {scope:$scope.scope,minimum:$scope.minimum,count:$count,covered:$covered,
             percent:(if $count == 0 then 0 else $covered*100/$count end)})
     else [] end) as $scopes
  | {modules:$modules,scopes:$scopes,passed:(($modules | length >= 12 and all(.[]; .lines.count > 0 and .lines.percent >= 95))
      and ($scopes | all(.[];.count > 0 and .percent >= .minimum)))}
' target/cypher-coverage.json > target/cypher-coverage-summary.json
jq -e '.passed' target/cypher-coverage-summary.json
