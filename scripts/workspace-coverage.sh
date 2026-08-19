#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHARD="${1:-}"
case "$SHARD" in
    planner)
        COVERAGE_ARGS=(-p helix-planner --all-targets)
        SCOPE_CONFIG='{
            "planner": {
                "needle": "/crates/planner/src/",
                "minimum_percent": 95
            }
        }'
        ;;
    db)
        COVERAGE_ARGS=(-p db --all-targets)
        SCOPE_CONFIG='{
            "db": {
                "needle": "/crates/db/src/",
                "minimum_percent": 94
            },
            "interpreter": {
                "needle": "/crates/db/src/execution/interpreter/",
                "minimum_percent": 98
            },
            "index_lifecycle": {
                "needle": "/crates/db/src/index_lifecycle/",
                "minimum_percent": 93
            },
            "search": {
                "needle": "/crates/db/src/search/",
                "minimum_percent": 93
            }
        }'
        ;;
    *)
        echo "usage: scripts/workspace-coverage.sh <planner|db>" >&2
        exit 2
        ;;
esac
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/helix-proper-${SHARD}-coverage.XXXXXX")"
REPORT_PATH="$TEMP_ROOT/coverage.json"
SUMMARY_PATH="$TEMP_ROOT/summary.json"
FULL_REPORT_PATH="${WORKSPACE_COVERAGE_FULL_REPORT_PATH:-}"

cleanup() {
    rm -rf -- "$TEMP_ROOT"
}
trap cleanup EXIT

command -v jq >/dev/null 2>&1 || {
    echo "workspace coverage requires jq" >&2
    exit 1
}
cargo llvm-cov --version >/dev/null

(
    cd "$ROOT"
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$TEMP_ROOT/target" cargo llvm-cov \
        --quiet \
        "${COVERAGE_ARGS[@]}" \
        --json \
        --output-path "$REPORT_PATH" \
        --ignore-filename-regex '(^|/)(tests|benches|examples)/|crates/server/src/transport_contracts.rs|/(registry|rustc)/'
)

if [[ -n "$FULL_REPORT_PATH" ]]; then
    cp "$REPORT_PATH" "$FULL_REPORT_PATH"
fi

jq --arg shard "$SHARD" --argjson scope_config "$SCOPE_CONFIG" '
    def file_line_metric($files):
        ($files | map(.summary.lines.count) | add // 0) as $count
        | ($files | map(.summary.lines.covered) | add // 0) as $covered
        | {
            count: $count,
            covered: $covered,
            percent: (if $count == 0 then 0 else ($covered * 100 / $count) end)
        };

    .data[0].files as $files
    | (reduce ($scope_config | to_entries[]) as $scope (
        {};
        .[$scope.key] = file_line_metric([
            $files[] | select(.filename | contains($scope.value.needle))
        ])
    )) as $scopes
    | ($scope_config | map_values(.minimum_percent)) as $minimum_percent
    | ([
        $minimum_percent
        | to_entries[]
        | select($scopes[.key].percent < .value)
        | {
            scope: .key,
            actual: $scopes[.key],
            minimum_percent: .value
        }
    ]) as $regressions
    | {
        schema_version: 1,
        coverage_kind: "workspace-all-targets",
        shard: $shard,
        scopes: $scopes,
        minimum_percent: $minimum_percent,
        regressions: $regressions,
        passed: (($regressions | length) == 0)
    }
    ' "$REPORT_PATH" >"$SUMMARY_PATH"

cat "$SUMMARY_PATH"
jq -e '.passed' "$SUMMARY_PATH" >/dev/null || {
    echo "workspace all-target source coverage thresholds were not met" >&2
    exit 1
}
