#!/usr/bin/env bash

# Measures the production V2 source surface without counting coverage-only
# harness code in the denominator. With no arguments the script creates and
# removes its own LLVM target/report directory. Two report arguments may be
# supplied to audit already-generated broad and production-only JSON reports.
# Deliberate unreachable/platform/LLVM positions remain individually justified;
# all other gaps are protected by exact test-backlog digests and metric floors.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXCLUSIONS_PATH="$ROOT/scripts/index-lifecycle-production-coverage-exclusions.json"
BASELINES_PATH="$ROOT/scripts/index-lifecycle-production-coverage-baselines.json"
TEMP_ROOT=""

cleanup() {
    if [[ -n "$TEMP_ROOT" ]]; then
        rm -rf -- "$TEMP_ROOT"
    fi
}
trap cleanup EXIT

command -v jq >/dev/null 2>&1 || {
    echo "index V2 production coverage requires jq" >&2
    exit 1
}

jq -e '
    type == "array"
    and all(
        .[];
        (.path
            | startswith("crates/db/src/index_lifecycle/")
                or . == "crates/db/src/encoding/v2/keys/lifecycle.rs"
                or startswith("crates/db/src/encoding/v2/values/lifecycle/"))
        and (.lines | type == "array")
        and (.lines | all(.[]; type == "number" and . > 0 and floor == .))
        and (.functions | type == "array")
        and (.functions | all(.[]; type == "number" and . > 0 and floor == .))
        and (((.lines | length) + (.functions | length)) > 0)
        and (
            .classification == "structurally-unreachable"
            or .classification == "platform-gated"
            or .classification == "llvm-artifact"
        )
        and (.reason | type == "string" and length > 0)
        and (.evidence | type == "string" and length > 0)
    )
' "$EXCLUSIONS_PATH" >/dev/null

jq -e '
    .schema_version == 1
    and (.uncovered_test_required_positions.classification == "test-required")
    and (.uncovered_test_required_positions.reason | type == "string" and length > 0)
    and all(
        .uncovered_test_required_positions.lines,
        .uncovered_test_required_positions.functions;
        (.count | type == "number" and . >= 0 and floor == .)
        and (.sha256 | type == "string" and test("^[0-9a-f]{64}$"))
    )
    and all(
        .metrics.lines,
        .metrics.functions;
        (.minimum_covered_positions | type == "number" and . >= 0 and floor == .)
        and (.minimum_percent | type == "number" and . >= 0 and . <= 100)
    )
' "$BASELINES_PATH" >/dev/null

case "$#" in
    0)
        cargo llvm-cov --version >/dev/null
        TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/helix-proper-index-lifecycle-coverage.XXXXXX")"
        BROAD_REPORT="$TEMP_ROOT/broad.json"
        DENOMINATOR_REPORT="$TEMP_ROOT/denominator.json"

        (
            cd "$ROOT"
            CARGO_TARGET_DIR="$TEMP_ROOT/target" cargo llvm-cov \
                -p db \
                --features 'production-coverage migration-parity index-lifecycle-testing' \
                --lib \
                --test index_lifecycle_contracts \
                --test production_contracts \
                --test production_index_delete_regressions \
                --test production_index_lifecycle_contracts \
                --test production_internal_contracts \
                --test production_migration_contracts \
                --test production_text_correctness_regressions \
                --test production_text_lifecycle \
                --test production_vector_planner \
                --locked \
                --json \
                --output-path "$BROAD_REPORT" \
                --ignore-filename-regex '(^|/)(tests|benches|examples)/|/(registry|rustc)/' \
                -- \
                --test-threads=1

            CARGO_TARGET_DIR="$TEMP_ROOT/target" cargo llvm-cov \
                -p db \
                --test production_contracts \
                --locked \
                --json \
                --output-path "$DENOMINATOR_REPORT" \
                --ignore-filename-regex '(^|/)(tests|benches|examples)/|/(registry|rustc)/' \
                -- \
                --test-threads=1
        )
        ;;
    2)
        BROAD_REPORT="$1"
        DENOMINATOR_REPORT="$2"
        ;;
    *)
        echo "usage: $0 [BROAD_REPORT DENOMINATOR_REPORT]" >&2
        exit 2
        ;;
esac

for report in "$BROAD_REPORT" "$DENOMINATOR_REPORT"; do
    if [[ ! -f "$report" ]]; then
        echo "coverage report does not exist: $report" >&2
        exit 1
    fi
done

SUMMARY="$({
    jq -n \
        --arg root "$ROOT" \
        --slurpfile broad "$BROAD_REPORT" \
        --slurpfile denominator "$DENOMINATOR_REPORT" \
        --slurpfile exclusions "$EXCLUSIONS_PATH" \
        '
        def selected_path($filename):
            ($filename | startswith($root + "/crates/db/src/index_lifecycle/"))
            or ($filename | endswith("/crates/db/src/encoding/v2/keys/lifecycle.rs"))
            or ($filename | startswith($root + "/crates/db/src/encoding/v2/values/lifecycle/"));

        def relative_path($filename):
            $filename | sub("^" + $root + "/"; "");

        def position_key($position):
            relative_path($position.path) + ":" + ($position.line | tostring);

        def metric($positions):
            ($positions | length) as $count
            | ($positions | map(select(.covered)) | length) as $covered
            | {
                count: $count,
                covered: $covered,
                missing: ($count - $covered),
                percent: (if $count == 0 then 0 else $covered * 100 / $count end)
            };

        def line_positions($data):
            [
                $data.files[]
                | select(selected_path(.filename))
                | .filename as $filename
                | .segments[]
                | select(.[3] and (.[5] | not))
                | {
                    path: $filename,
                    line: .[0],
                    covered: (.[2] > 0)
                }
            ]
            | group_by([.path, .line])
            | map({
                path: .[0].path,
                line: .[0].line,
                covered: any(.[]; .covered)
            });

        def function_positions($data):
            [
                $data.functions[]
                | select(.filenames | length > 0)
                | select(selected_path(.filenames[0]))
                | {
                    path: .filenames[0],
                    line: (.regions[0][0] // 0),
                    covered: (.count > 0)
                }
            ]
            | group_by([.path, .line])
            | map({
                path: .[0].path,
                line: .[0].line,
                covered: any(.[]; .covered)
            });

        $broad[0].data[0] as $broad_data
        | $denominator[0].data[0] as $denominator_data
        | (line_positions($broad_data) | INDEX(.path + ":" + (.line | tostring))) as $broad_lines
        | (function_positions($broad_data) | INDEX(.path + ":" + (.line | tostring))) as $broad_functions
        | (line_positions($denominator_data)
            | map(
                (.path + ":" + (.line | tostring)) as $key
                | .covered = ($broad_lines[$key].covered // false)
            )) as $lines
        | (function_positions($denominator_data)
            | map(
                (.path + ":" + (.line | tostring)) as $key
                | .covered = ($broad_functions[$key].covered // false)
            )) as $functions
        | ([$exclusions[0][] as $exclusion
            | $exclusion.lines[]
            | $exclusion.path + ":" + (tostring)]) as $excluded_line_keys
        | ([$exclusions[0][] as $exclusion
            | $exclusion.functions[]
            | $exclusion.path + ":" + (tostring)]) as $excluded_function_keys
        | if ($excluded_line_keys | unique | length) != ($excluded_line_keys | length) then
            error("duplicate index V2 production line exclusion")
          else . end
        | if ($excluded_function_keys | unique | length) != ($excluded_function_keys | length) then
            error("duplicate index V2 production function exclusion")
          else . end
        | ([$excluded_line_keys[] as $key
            | select($lines | any(.[]; position_key(.) == $key and (.covered | not)) | not)
            | $key]) as $invalid_line_exclusions
        | if ($invalid_line_exclusions | length) != 0 then
            error("stale, covered, or absent index V2 line exclusions: \($invalid_line_exclusions)")
          else . end
        | ([$excluded_function_keys[] as $key
            | select($functions | any(.[]; position_key(.) == $key and (.covered | not)) | not)
            | $key]) as $invalid_function_exclusions
        | if ($invalid_function_exclusions | length) != 0 then
            error("stale, covered, or absent index V2 function exclusions: \($invalid_function_exclusions)")
          else . end
        | ($lines
            | map(select(position_key(.) as $key | $excluded_line_keys | index($key) | not))) as $adjusted_lines
        | ($functions
            | map(select(position_key(.) as $key | $excluded_function_keys | index($key) | not))) as $adjusted_functions
        | {
            schema_version: 1,
            package: "db",
            coverage_kind: "index-lifecycle-production-source-positions",
            lines: metric($adjusted_lines),
            functions: metric($adjusted_functions),
            raw_lines: metric($lines),
            raw_functions: metric($functions),
            deliberate_exclusions: {
                entries: ($exclusions[0] | length),
                lines: ($excluded_line_keys | length),
                functions: ($excluded_function_keys | length)
            },
            missing_lines_by_file: (
                $adjusted_lines
                | map(select(.covered | not))
                | group_by(.path)
                | map({
                    path: relative_path(.[0].path),
                    lines: (map(.line) | unique)
                })
            ),
            missing_functions_by_file: (
                $adjusted_functions
                | map(select(.covered | not))
                | group_by(.path)
                | map({
                    path: relative_path(.[0].path),
                    lines: (map(.line) | unique)
                })
            )
        }
        '
})"

MISSING_LINE_POSITIONS="$(jq -r '
    .missing_lines_by_file[]
    | .path as $path
    | .lines[]
    | $path + ":" + tostring
' <<<"$SUMMARY")"
MISSING_FUNCTION_POSITIONS="$(jq -r '
    .missing_functions_by_file[]
    | .path as $path
    | .lines[]
    | $path + ":" + tostring
' <<<"$SUMMARY")"
MISSING_LINE_COUNT="$(jq '[.missing_lines_by_file[].lines[]] | length' <<<"$SUMMARY")"
MISSING_FUNCTION_COUNT="$(jq '[.missing_functions_by_file[].lines[]] | length' <<<"$SUMMARY")"
if command -v sha256sum >/dev/null 2>&1; then
    MISSING_LINE_SHA256="$(printf '%s\n' "$MISSING_LINE_POSITIONS" | sha256sum | awk '{print $1}')"
    MISSING_FUNCTION_SHA256="$(printf '%s\n' "$MISSING_FUNCTION_POSITIONS" | sha256sum | awk '{print $1}')"
else
    MISSING_LINE_SHA256="$(printf '%s\n' "$MISSING_LINE_POSITIONS" | shasum -a 256 | awk '{print $1}')"
    MISSING_FUNCTION_SHA256="$(printf '%s\n' "$MISSING_FUNCTION_POSITIONS" | shasum -a 256 | awk '{print $1}')"
fi

SUMMARY="$({
    jq \
        --argjson missing_line_count "$MISSING_LINE_COUNT" \
        --arg missing_line_sha256 "$MISSING_LINE_SHA256" \
        --argjson missing_function_count "$MISSING_FUNCTION_COUNT" \
        --arg missing_function_sha256 "$MISSING_FUNCTION_SHA256" \
        --slurpfile baselines "$BASELINES_PATH" \
        '
        ($baselines[0]) as $baseline
        | ([
            (
                {scope: "lines", actual: .lines, required: $baseline.metrics.lines},
                {scope: "functions", actual: .functions, required: $baseline.metrics.functions}
            )
            | select(
                .actual.covered < .required.minimum_covered_positions
                or .actual.percent < .required.minimum_percent
            )
        ]) as $regressions
        | ($missing_line_count == $baseline.uncovered_test_required_positions.lines.count
            and $missing_line_sha256 == $baseline.uncovered_test_required_positions.lines.sha256
            and $missing_function_count == $baseline.uncovered_test_required_positions.functions.count
            and $missing_function_sha256 == $baseline.uncovered_test_required_positions.functions.sha256
        ) as $classification_current
        | .coverage_baselines = $baseline.metrics
        | .coverage_regressions = $regressions
        | .uncovered_test_required_positions = {
            lines: {count: $missing_line_count, sha256: $missing_line_sha256},
            functions: {count: $missing_function_count, sha256: $missing_function_sha256},
            classification: $baseline.uncovered_test_required_positions.classification,
            classification_current: $classification_current
        }
        | .passed = (($regressions | length) == 0 and $classification_current)
        ' <<<"$SUMMARY"
})"

if [[ -z "$SUMMARY" ]]; then
    echo "index V2 production coverage summary is empty" >&2
    exit 1
fi
jq . <<<"$SUMMARY"
jq -e 'type == "object" and .passed == true' <<<"$SUMMARY" >/dev/null
