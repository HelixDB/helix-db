#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/private/tmp}/helix-proper-db-production-coverage.XXXXXX")"
REPORT_PATH="$TEMP_ROOT/coverage.json"
EXCLUSIONS_PATH="$ROOT/scripts/db-production-coverage-exclusions.json"
DISPOSITIONS_PATH="$ROOT/scripts/db-production-coverage-dispositions.json"
SUMMARY_PATH="$TEMP_ROOT/summary.json"

# Callers performing the required source-level gap review may preserve the
# complete LLVM JSON outside this temporary directory. The runner always asks
# LLVM for regions because the enforced line metric merges generic/async
# instantiations back to unique source lines. The default path still leaves no
# report artifact after the owned temporary directory is removed.
FULL_REPORT_PATH="${DB_COVERAGE_FULL_REPORT_PATH:-}"

cleanup() {
    rm -rf -- "$TEMP_ROOT"
}
trap cleanup EXIT

command -v jq >/dev/null 2>&1 || {
    echo "db production coverage requires jq" >&2
    exit 1
}
cargo llvm-cov --version >/dev/null
jq -e '
    type == "array"
    and all(
        .[];
        (.path | startswith("crates/db/src/search/vector/"))
        and (.line | type == "number" and . > 0 and floor == .)
        and (.reason | type == "string" and length > 0)
        and (.evidence | type == "string" and length > 0)
    )
' "$EXCLUSIONS_PATH" >/dev/null
jq -e '
    type == "array"
    and all(
        .[];
        (.path | startswith("crates/db/src/search/vector/"))
        and (.lines | type == "array" and length > 0)
        and (.lines | all(.[]; type == "number" and . > 0 and floor == .))
        and (.classification == "named-test" or .classification == "architecture-test")
        and (.reason | type == "string" and length > 0)
        and (.evidence | type == "string" and length > 0)
    )
' "$DISPOSITIONS_PATH" >/dev/null

TARGETS_JSON="$({
    cd "$ROOT"
    cargo metadata --no-deps --format-version 1
} | jq -c '[
    .packages[]
    | select(.name == "db")
    | .targets[]
    | select(.kind | index("test"))
    | select(.name | startswith("production_"))
    | select(((."required-features" // []) | index("production-scale")) | not)
    | .name
] | sort')"

TARGETS=()
while IFS= read -r target; do
    TARGETS+=("$target")
done < <(jq -r '.[]' <<<"$TARGETS_JSON")
if [[ "${#TARGETS[@]}" -eq 0 ]]; then
    echo "db has no Cargo-discovered integration-test targets" >&2
    exit 1
fi

(
    cd "$ROOT"
    COVERAGE_ARGS=(
        --quiet
        -p db
        --features production-coverage
        --json
        --output-path "$REPORT_PATH"
        --ignore-filename-regex '(^|/)(tests|benches|examples)/|/(registry|rustc)/'
    )
    for target in "${TARGETS[@]}"; do
        COVERAGE_ARGS+=(--test "$target")
    done
    # Run each libtest binary serially so randomized graph tests and async
    # continuation mapping cannot race one another while collecting the
    # source-line disposition evidence.
    RUST_TEST_THREADS=1 CARGO_TARGET_DIR="$TEMP_ROOT/target" cargo llvm-cov "${COVERAGE_ARGS[@]}"
)

if [[ "${DB_COVERAGE_DEBUG_FILES:-0}" == "1" ]]; then
    jq -r '.data[0].files[].filename | select(contains("vector"))' "$REPORT_PATH" >&2
fi

if [[ -n "$FULL_REPORT_PATH" ]]; then
    cp "$REPORT_PATH" "$FULL_REPORT_PATH"
fi

jq \
    --arg root "$ROOT" \
    --argjson targets "$TARGETS_JSON" \
    --slurpfile exclusions "$EXCLUSIONS_PATH" \
    --slurpfile dispositions "$DISPOSITIONS_PATH" \
    '
    def metric($summaries; $name):
        ($summaries | map(.[$name].count) | add // 0) as $count
        | ($summaries | map(.[$name].covered) | add // 0) as $covered
        | {
            count: $count,
            covered: $covered,
            percent: (if $count == 0 then 0 else ($covered * 100 / $count) end)
        };

    def vector_path($filename):
        "crates/db/src/search/vector/" +
        ($filename | split("/crates/db/src/search/vector/") | last);

    def source_line_metric($lines):
        ($lines | length) as $count
        | ($lines | map(select(.covered)) | length) as $covered
        | {
            count: $count,
            covered: $covered,
            percent: (if $count == 0 then 0 else ($covered * 100 / $count) end)
        };

    .data[0] as $data
    | [
        $data.files[]
        | select(
            (.filename | startswith($root + "/crates/db/src/search/vector/"))
            or (.filename | contains("/crates/db/src/search/vector/"))
            or (.filename | startswith("crates/db/src/search/vector/"))
        )
    ] as $vector_files
    | ($exclusions[0]) as $line_exclusions
    | ($line_exclusions | map(.path + ":" + (.line | tostring))) as $excluded_keys
    | ($dispositions[0]) as $line_dispositions
    | ([
        $line_dispositions[]
        | .path as $path
        | .lines[]
        | $path + ":" + (tostring)
    ]) as $disposition_keys
    | if ($excluded_keys | unique | length) != ($excluded_keys | length) then
        error("duplicate db production coverage line exclusion")
      else . end
    | if ($disposition_keys | unique | length) != ($disposition_keys | length) then
        error("duplicate db production coverage line disposition")
      else . end
    | [
        $vector_files[]
        | .filename as $filename
        | .segments[]
        | select(.[3] and (.[5] | not))
        | {
            path: vector_path($filename),
            line: .[0],
            covered: (.[2] > 0)
        }
    ]
    | group_by([.path, .line])
    | map({
        path: .[0].path,
        line: .[0].line,
        covered: any(.[]; .covered)
    }) as $source_lines
    | [
        $line_exclusions[] as $exclusion
        | select(
            $source_lines
            | any(
                .[];
                .path == $exclusion.path
                and .line == $exclusion.line
                and (.covered | not)
            )
            | not
        )
        | $exclusion
    ] as $invalid_exclusions
    | if ($invalid_exclusions | length) != 0 then
        error("stale or covered db production coverage exclusions: \($invalid_exclusions)")
      else . end
    | [
        $source_lines[]
        | select(.covered | not)
        | (.path + ":" + (.line | tostring)) as $key
        | select(($excluded_keys | index($key) | not) and ($disposition_keys | index($key) | not))
    ] as $undisposed_lines
    | if ($undisposed_lines | length) != 0 then
        error("undisposed db production coverage source lines: \($undisposed_lines)")
      else . end
    | [
        $disposition_keys[] as $key
        | select(
            $source_lines
            | any(
                .[];
                (.path + ":" + (.line | tostring)) == $key
                and (.covered | not)
            )
            | not
        )
        | $key
    ] as $invalid_dispositions
    | if ($invalid_dispositions | length) != 0 then
        error("stale or covered db production coverage dispositions: \($invalid_dispositions)")
      else . end
    | [
        $source_lines[]
        | select((.path + ":" + (.line | tostring)) as $key | $excluded_keys | index($key) | not)
    ] as $adjusted_source_lines
    | ($vector_files | map(.summary)) as $vector
    | source_line_metric($adjusted_source_lines) as $line_metric
    | metric($vector; "functions") as $function_metric
    | metric($vector; "regions") as $region_metric
    | ($function_metric.percent >= 98
        and $line_metric.percent >= 98
        and $region_metric.percent >= 95) as $passed
    | {
        schema_version: 2,
        package: "db",
        coverage_kind: "production-only-integration-targets",
        integration_targets: $targets,
        db: {
            functions: $data.totals.functions,
            lines: $data.totals.lines,
            regions: $data.totals.regions
        },
        search_vector: {
            functions: $function_metric,
            lines: $line_metric,
            llvm_instantiated_lines: metric($vector; "lines"),
            source_lines_before_exclusions: source_line_metric($source_lines),
            deliberate_unreachable_line_exclusions: ($line_exclusions | length),
            uncovered_source_line_dispositions: ($disposition_keys | length),
            regions: $region_metric
        },
        thresholds: {
            functions_percent: 98,
            lines_percent: 98,
            regions_percent: 95,
            passed: $passed
        }
    }
    ' "$REPORT_PATH" >"$SUMMARY_PATH"

cat "$SUMMARY_PATH"
jq -e '.thresholds.passed' "$SUMMARY_PATH" >/dev/null || {
    echo "db search/vector production coverage thresholds were not met" >&2
    exit 1
}
