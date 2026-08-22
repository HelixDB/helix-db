#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/helix-proper-db-production-coverage.XXXXXX")"
REPORT_PATH="$TEMP_ROOT/coverage.json"
EXCLUSIONS_PATH="$ROOT/scripts/db-production-coverage-exclusions.json"
DISPOSITIONS_PATH="$ROOT/scripts/db-production-coverage-dispositions.json"
BASELINES_PATH="$ROOT/scripts/db-production-coverage-baselines.json"
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
jq -e '
    .schema_version == 1
    and (.uncovered_non_vector_source_lines.count | type == "number" and . >= 0 and floor == .)
    and (.uncovered_non_vector_source_lines.sha256 | test("^[0-9a-f]{64}$"))
    and .uncovered_non_vector_source_lines.classification == "test-required"
    and (.uncovered_non_vector_source_lines.reason | type == "string" and length > 0)
    and (.scopes | keys == [
        "index_lifecycle",
        "interpreter",
        "runtime_dependencies",
        "secondary_lifecycle",
        "text_lifecycle",
        "text_search",
        "vector_lifecycle",
        "whole_db"
    ])
    and all(
        .scopes[];
        (.minimum_covered_lines | type == "number" and . >= 0 and floor == .)
        and (.minimum_percent | type == "number" and . >= 0 and . <= 100)
    )
' "$BASELINES_PATH" >/dev/null

TARGETS_JSON="$({
    cd "$ROOT"
    cargo metadata --no-deps --format-version 1
} | jq -c '[
    .packages[]
    | select(.name == "db")
    | .targets[]
    | select(.kind | index("test"))
    | select(
        (.name | startswith("production_"))
        or .name == "index_lifecycle_contracts"
    )
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
if ! jq -e 'index("index_lifecycle_contracts") != null' <<<"$TARGETS_JSON" >/dev/null; then
    echo "db production coverage omitted index_lifecycle_contracts" >&2
    exit 1
fi

(
    cd "$ROOT"
    COVERAGE_ARGS=(
        --quiet
        -p db
        --features production-coverage,migration-parity,index-lifecycle-testing
        --json
        --output-path "$REPORT_PATH"
        --ignore-filename-regex '(^|/)(tests|benches|examples)/|/(registry|rustc)/|/crates/db/src/index_lifecycle_testing(/|\.rs$)'
    )
    for target in "${TARGETS[@]}"; do
        COVERAGE_ARGS+=(--test "$target")
    done
    # Run each libtest binary serially so randomized graph tests and async
    # continuation mapping cannot race one another while collecting the
    # source-line disposition evidence.
    RUST_MIN_STACK="${RUST_MIN_STACK:-16777216}" \
        RUST_TEST_THREADS=1 \
        CARGO_TARGET_DIR="$TEMP_ROOT/target" \
        cargo llvm-cov "${COVERAGE_ARGS[@]}"
)

if [[ "${DB_COVERAGE_DEBUG_FILES:-0}" == "1" ]]; then
    jq -r '.data[0].files[].filename | select(contains("vector"))' "$REPORT_PATH" >&2
fi

if [[ -n "$FULL_REPORT_PATH" ]]; then
    cp "$REPORT_PATH" "$FULL_REPORT_PATH"
fi

UNCOVERED_NON_VECTOR_LINES="$(jq -r '
    [
        .data[0].files[]
        | select(
            (.filename | contains("/crates/db/src/"))
            and (
                (
                    (.filename | contains("/crates/db/src/search/vector/"))
                    or (.filename | contains("/crates/db/src/encoding/v2/legacy/vector/"))
                    or (.filename | contains("/crates/db/src/encoding/v2/keys/indexes/vector/"))
                    or (.filename | contains("/crates/db/src/encoding/v2/values/indexes/vector/"))
                )
                | not
            )
        )
        | .filename as $filename
        | .segments[]
        | select(.[3] and (.[5] | not))
        | {
            path: ("crates/db/src/" + ($filename | split("/crates/db/src/") | last)),
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
UNCOVERED_NON_VECTOR_COUNT="$(wc -l <<<"$UNCOVERED_NON_VECTOR_LINES" | tr -d ' ')"
if command -v sha256sum >/dev/null 2>&1; then
    UNCOVERED_NON_VECTOR_SHA256="$(printf '%s\n' "$UNCOVERED_NON_VECTOR_LINES" | sha256sum | awk '{print $1}')"
else
    UNCOVERED_NON_VECTOR_SHA256="$(printf '%s\n' "$UNCOVERED_NON_VECTOR_LINES" | shasum -a 256 | awk '{print $1}')"
fi

jq \
    --arg root "$ROOT" \
    --argjson targets "$TARGETS_JSON" \
    --arg uncovered_non_vector_sha256 "$UNCOVERED_NON_VECTOR_SHA256" \
    --argjson uncovered_non_vector_count "$UNCOVERED_NON_VECTOR_COUNT" \
    --slurpfile exclusions "$EXCLUSIONS_PATH" \
    --slurpfile dispositions "$DISPOSITIONS_PATH" \
    --slurpfile baselines "$BASELINES_PATH" \
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

    def file_line_metric($files):
        ($files | map(.summary.lines.count) | add // 0) as $count
        | ($files | map(.summary.lines.covered) | add // 0) as $covered
        | {
            count: $count,
            covered: $covered,
            percent: (if $count == 0 then 0 else ($covered * 100 / $count) end)
        };

    .data[0] as $data
    | [
        $data.files[]
        | select(.filename | contains("/crates/db/src/"))
    ] as $db_files
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
    | {
        whole_db: file_line_metric($db_files),
        interpreter: file_line_metric([
            $db_files[]
            | select(.filename | contains("/crates/db/src/execution/interpreter/"))
        ]),
        runtime_dependencies: file_line_metric([
            $db_files[]
            | select(.filename | endswith("/crates/db/src/runtime_dependencies.rs"))
        ]),
        index_lifecycle: file_line_metric([
            $db_files[]
            | select(.filename | contains("/crates/db/src/index_lifecycle/"))
        ]),
        secondary_lifecycle: file_line_metric([
            $db_files[]
            | select(
                (.filename | endswith("/crates/db/src/index_lifecycle/secondary.rs"))
                or (.filename | endswith("/crates/db/src/execution/interpreter/ddl/secondary.rs"))
            )
        ]),
        vector_lifecycle: file_line_metric([
            $db_files[]
            | select(
                (.filename | contains("/crates/db/src/index_lifecycle/vector/"))
                or (.filename | endswith("/crates/db/src/index_lifecycle/vector.rs"))
            )
        ]),
        text_lifecycle: file_line_metric([
            $db_files[]
            | select(.filename | contains("/crates/db/src/index_lifecycle/text/"))
        ]),
        text_search: file_line_metric([
            $db_files[]
            | select(.filename | contains("/crates/db/src/search/text/"))
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
    | ($uncovered_non_vector_count == $baselines[0].uncovered_non_vector_source_lines.count
        and $uncovered_non_vector_sha256 == $baselines[0].uncovered_non_vector_source_lines.sha256
      ) as $classification_current
    | ($function_metric.percent >= 96.5
        and $line_metric.percent >= 97
        and $region_metric.percent >= 93
        and ($scope_regressions | length) == 0
        and $classification_current) as $passed
    | {
        schema_version: 3,
        package: "db",
        coverage_kind: "production-linked-integration-targets",
        integration_targets: $targets,
        db: {
            functions: $data.totals.functions,
            lines: $data.totals.lines,
            regions: $data.totals.regions
        },
        production_scopes: $scopes,
        production_scope_baselines: $scope_baselines,
        production_scope_regressions: $scope_regressions,
        uncovered_non_vector_source_lines: {
            count: $uncovered_non_vector_count,
            sha256: $uncovered_non_vector_sha256,
            classification: $baselines[0].uncovered_non_vector_source_lines.classification,
            classification_current: $classification_current
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
            functions_percent: 96.5,
            lines_percent: 97,
            regions_percent: 93,
            passed: $passed
        }
    }
    ' "$REPORT_PATH" >"$SUMMARY_PATH"

cat "$SUMMARY_PATH"
jq -e '.thresholds.passed' "$SUMMARY_PATH" >/dev/null || {
    echo "db production coverage thresholds or uncovered-line classifications were not met" >&2
    exit 1
}
