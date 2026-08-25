#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/helix-proper-server-production-coverage.XXXXXX")"
REPORT_PATH="$TEMP_ROOT/coverage.json"
SUMMARY_PATH="$TEMP_ROOT/summary.json"
BASELINES_PATH="$ROOT/scripts/server-production-coverage-baselines.json"
FULL_REPORT_PATH="${SERVER_COVERAGE_FULL_REPORT_PATH:-}"

cleanup() {
    rm -rf -- "$TEMP_ROOT"
}
trap cleanup EXIT

command -v jq >/dev/null 2>&1 || {
    echo "server production coverage requires jq" >&2
    exit 1
}
cargo llvm-cov --version >/dev/null
jq -e '
    .schema_version == 1
    and (.uncovered_source_lines.count | type == "number" and . >= 0 and floor == .)
    and (.uncovered_source_lines.sha256 | test("^[0-9a-f]{64}$"))
    and .uncovered_source_lines.classification == "test-required"
    and (.uncovered_source_lines.reason | type == "string" and length > 0)
    and (.scopes | keys == ["query_service", "server"])
    and all(
        .scopes[];
        (.minimum_covered_lines | type == "number" and . >= 0 and floor == .)
        and (.minimum_percent | type == "number" and . >= 0 and . <= 100)
    )
' "$BASELINES_PATH" >/dev/null

EXCLUDE_ARGS=()
while IFS= read -r package; do
    if [[ "$package" != "server" ]]; then
        EXCLUDE_ARGS+=(--exclude-from-test "$package")
    fi
done < <(
    cd "$ROOT"
    cargo metadata --no-deps --format-version 1 | jq -r '.packages[].name'
)

(
    cd "$ROOT"
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$TEMP_ROOT/target" cargo llvm-cov \
        --quiet \
        --workspace \
        --lib \
        "${EXCLUDE_ARGS[@]}" \
        --json \
        --output-path "$REPORT_PATH" \
        --ignore-filename-regex 'crates/server/src/(tests|transport_contracts).rs|/(registry|rustc)/' \
        -- \
        --test-threads=1
)

if [[ -n "$FULL_REPORT_PATH" ]]; then
    cp "$REPORT_PATH" "$FULL_REPORT_PATH"
fi

UNCOVERED_LINES="$(jq -r '
    [
        .data[0].files[]
        | select(
            (.filename | contains("/crates/server/src/"))
            or (.filename | endswith("/crates/db/src/query_service.rs"))
        )
        | .filename as $filename
        | .segments[]
        | select(.[3] and (.[5] | not))
        | {
            path: (
                if ($filename | contains("/crates/server/src/")) then
                    "crates/server/src/" + ($filename | split("/crates/server/src/") | last)
                else
                    "crates/db/src/query_service.rs"
                end
            ),
            line: .[0],
            covered: (.[2] > 0)
        }
    ]
    | group_by([.path, .line])
    | map({
        path: .[0].path,
        line: .[0].line,
        covered: any(.[]; .covered)
    })
    | map(select(.covered | not))
    | sort_by(.path, .line)
    | .[]
    | "\(.path):\(.line)"
' "$REPORT_PATH")"
UNCOVERED_COUNT="$(wc -l <<<"$UNCOVERED_LINES" | tr -d ' ')"
if command -v sha256sum >/dev/null 2>&1; then
    UNCOVERED_SHA256="$(printf '%s\n' "$UNCOVERED_LINES" | sha256sum | awk '{print $1}')"
else
    UNCOVERED_SHA256="$(printf '%s\n' "$UNCOVERED_LINES" | shasum -a 256 | awk '{print $1}')"
fi

jq \
    --argjson uncovered_count "$UNCOVERED_COUNT" \
    --arg uncovered_sha256 "$UNCOVERED_SHA256" \
    --slurpfile baselines "$BASELINES_PATH" \
    '
    def file_line_metric($files):
        ($files | map(.summary.lines.count) | add // 0) as $count
        | ($files | map(.summary.lines.covered) | add // 0) as $covered
        | {
            count: $count,
            covered: $covered,
            percent: (if $count == 0 then 0 else ($covered * 100 / $count) end)
        };

    .data[0].files as $files
    | {
        server: file_line_metric([
            $files[] | select(.filename | contains("/crates/server/src/"))
        ]),
        query_service: file_line_metric([
            $files[] | select(.filename | endswith("/crates/db/src/query_service.rs"))
        ])
    } as $scopes
    | ($baselines[0].scopes) as $scope_baselines
    | ([
        $scope_baselines
        | to_entries[]
        | select(
            $scopes[.key].covered < .value.minimum_covered_lines
            or $scopes[.key].percent < .value.minimum_percent
        )
        | {
            scope: .key,
            actual: $scopes[.key],
            required: .value
        }
    ]) as $scope_regressions
    | ($uncovered_count == $baselines[0].uncovered_source_lines.count
        and $uncovered_sha256 == $baselines[0].uncovered_source_lines.sha256
      ) as $classification_current
    | {
        schema_version: 1,
        coverage_kind: "production-transport-and-query-service",
        scopes: $scopes,
        baselines: $scope_baselines,
        regressions: $scope_regressions,
        uncovered_source_lines: {
            count: $uncovered_count,
            sha256: $uncovered_sha256,
            classification: $baselines[0].uncovered_source_lines.classification,
            classification_current: $classification_current
        },
        passed: (($scope_regressions | length) == 0 and $classification_current)
    }
    ' "$REPORT_PATH" >"$SUMMARY_PATH"

cat "$SUMMARY_PATH"
jq -e '.passed' "$SUMMARY_PATH" >/dev/null || {
    echo "server production coverage thresholds or uncovered-line classifications were not met" >&2
    exit 1
}
