#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHARD="${1:-}"
BASE_REF="${CARDINALITY_COVERAGE_BASE_REF:-planner/update-for-new-encodings}"

case "$SHARD" in
    planner)
        COVERAGE_ARGS=(-p helix-planner --lib)
        TARGET_FILES=(
            crates/planner/src/exec/access/edge.rs
            crates/planner/src/exec/access/node.rs
            crates/planner/src/exec/count.rs
            crates/planner/src/exec/error.rs
            crates/planner/src/exec/lowering/secondary_set.rs
            crates/planner/src/exec/selected/count.rs
            crates/planner/src/exec/selected/lowering/access/leaf.rs
            crates/planner/src/exec/selected/lowering/count.rs
            crates/planner/src/exec/selected/lowering/contracts/matching/access/source/edge.rs
            crates/planner/src/exec/selected/lowering/contracts/matching/access/source/node.rs
            crates/planner/src/exec/validation/contracts.rs
            crates/planner/src/logical/root/terminal.rs
            crates/planner/src/optimizer/config.rs
            crates/planner/src/physical/access.rs
            crates/planner/src/physical/cardinality.rs
            crates/planner/src/planning/selected/lowering/case/classify.rs
            crates/planner/src/planning/selected/lowering/dispatch.rs
            crates/planner/src/planning/selected/lowering/root.rs
            crates/planner/src/planning/selected/native/equality_bindings.rs
            crates/planner/src/planning/selected/native/pipeline/ops/filter.rs
            crates/planner/src/planning/selected/native/source/lowering/stream.rs
            crates/planner/src/planning/selected/native/stream/accumulator/ops.rs
            crates/planner/src/rules/cardinality.rs
            crates/planner/src/rules/physical_contracts/access/contract.rs
            crates/planner/src/rules/physical_contracts/access/source/edge.rs
            crates/planner/src/rules/physical_contracts/access/source/node.rs
            crates/planner/src/rules/physical_contracts/access/source/shared/dispatch.rs
            crates/planner/src/rules/physical_contracts/access/source/shared/leaf.rs
        )
        ;;
    db)
        COVERAGE_ARGS=(-p db --lib)
        TARGET_FILES=(
            crates/db/src/execution/interpreter/access/dispatch.rs
            crates/db/src/execution/interpreter/access/indexes.rs
            crates/db/src/execution/interpreter/access/range.rs
            crates/db/src/execution/interpreter/access/secondary_set.rs
            crates/db/src/execution/interpreter/count.rs
            crates/db/src/execution/interpreter/dispatch.rs
            crates/db/src/index_lifecycle/secondary/exact.rs
        )
        ;;
    *)
        echo "usage: scripts/cardinality-boundary-coverage.sh <planner|db>" >&2
        exit 2
        ;;
esac

TEMP_ROOT="$(mktemp -d "${TMPDIR:-/private/tmp}/helix-cardinality-${SHARD}-coverage.XXXXXX")"
REPORT_PATH="$TEMP_ROOT/coverage.json"
FUNCTION_REPORT_PATH="$TEMP_ROOT/function-coverage.json"
CHANGED_LINES_PATH="$TEMP_ROOT/changed-lines.tsv"
PRODUCTION_CHANGED_LINES_PATH="$TEMP_ROOT/production-changed-lines.tsv"
TARGET_FILES_PATH="$TEMP_ROOT/target-files.json"
SUMMARY_PATH="$TEMP_ROOT/summary.json"
FULL_REPORT_PATH="${CARDINALITY_COVERAGE_FULL_REPORT_PATH:-}"

cleanup() {
    rm -rf -- "$TEMP_ROOT"
}
trap cleanup EXIT

command -v jq >/dev/null 2>&1 || {
    echo "cardinality boundary coverage requires jq" >&2
    exit 1
}
command -v perl >/dev/null 2>&1 || {
    echo "cardinality boundary coverage requires perl" >&2
    exit 1
}
cargo llvm-cov --version >/dev/null
git -C "$ROOT" rev-parse --verify "$BASE_REF^{commit}" >/dev/null

printf '%s\n' "${TARGET_FILES[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))' \
    >"$TARGET_FILES_PATH"

git -C "$ROOT" diff --unified=0 --no-color "$BASE_REF" -- "${TARGET_FILES[@]}" \
    | perl -ne '
        if (/^\+\+\+ b\/(.+)$/) {
            $file = $1;
            next;
        }
        if (/^@@ .*\+(\d+)(?:,(\d+))? @@/) {
            $start = $1;
            $count = defined($2) ? $2 : 1;
            for ($offset = 0; $offset < $count; $offset += 1) {
                print "$file\t", $start + $offset, "\n";
            }
        }
    ' >"$CHANGED_LINES_PATH"

for target_file in "${TARGET_FILES[@]}"; do
    if ! git -C "$ROOT" ls-files --error-unmatch "$target_file" >/dev/null 2>&1; then
        awk -v path="$target_file" '{ print path "\t" NR }' "$ROOT/$target_file" \
            >>"$CHANGED_LINES_PATH"
    fi
done

for target_file in "${TARGET_FILES[@]}"; do
    if ! grep -Fq "$target_file" "$CHANGED_LINES_PATH"; then
        echo "targeted cardinality file has no changed lines against $BASE_REF: $target_file" >&2
        exit 1
    fi
done

# Unit-test assertion failure paths are not production contract branches. Keep
# the gate focused on the changed source preceding each file's inline test
# module; integration tests and doctests still exercise that production code.
: >"$PRODUCTION_CHANGED_LINES_PATH"
for target_file in "${TARGET_FILES[@]}"; do
    test_start="$(awk '
        /^#\[cfg\(.*test/ { candidate = NR; next }
        candidate && /^#\[/ { next }
        candidate && /^(pub(\([^)]*\))? )?mod tests/ { print candidate; exit }
        candidate { candidate = 0 }
    ' "$ROOT/$target_file")"
    if [[ -z "$test_start" ]]; then
        test_start=2147483647
    fi
    awk -F '\t' -v path="$target_file" -v test_start="$test_start" \
        '$1 == path && $2 < test_start { print }' "$CHANGED_LINES_PATH" \
        >>"$PRODUCTION_CHANGED_LINES_PATH"
done

if [[ "$SHARD" == db ]]; then
    # LLVM 22's exporter currently crashes while traversing the complete DB
    # source map. Run the entire branch-instrumented library suite, merge its
    # complete profile, then export each targeted production file separately.
    # Every test execution and every metric remains present; only unrelated
    # source records are kept out of the crashing exporter invocation.
    (
        cd "$ROOT"
        env -u RUSTC_WRAPPER \
            CARGO_INCREMENTAL=0 \
            CARGO_TARGET_DIR="$TEMP_ROOT/branch-target" \
            cargo llvm-cov \
                --quiet \
                "${COVERAGE_ARGS[@]}" \
                --branch \
                --no-report
    )

    RUST_SYSROOT="$(rustc --print sysroot)"
    RUST_HOST="$(rustc -vV | sed -n 's/^host: //p')"
    LLVM_TOOLS_ROOT="$RUST_SYSROOT/lib/rustlib/$RUST_HOST/bin"
    DB_OBJECT="$(find "$TEMP_ROOT/branch-target/llvm-cov-target/debug/deps" \
        -maxdepth 1 -type f -perm -111 -name 'db-*' -print -quit)"
    if [[ -z "$DB_OBJECT" ]]; then
        echo "database coverage binary was not produced" >&2
        exit 1
    fi
    find "$TEMP_ROOT/branch-target/llvm-cov-target" -maxdepth 1 -type f -name '*.profraw' \
        -print0 \
        | xargs -0 "$LLVM_TOOLS_ROOT/llvm-profdata" merge -sparse \
            -o "$TEMP_ROOT/db.profdata"

    FILTERED_REPORTS=()
    for target_file in "${TARGET_FILES[@]}"; do
        report_name="$(printf '%s' "$target_file" | tr '/.' '__')"
        raw_report="$TEMP_ROOT/$report_name.raw.json"
        filtered_report="$TEMP_ROOT/$report_name.json"
        (
            cd "$ROOT"
            "$LLVM_TOOLS_ROOT/llvm-cov" export \
                -format=text \
                -instr-profile="$TEMP_ROOT/db.profdata" \
                -object "$DB_OBJECT" \
                -sources "$target_file" >"$raw_report"
        )
        jq --arg suffix "/$target_file" '
            .data[0].files = [
                .data[0].files[] | select(.filename | endswith($suffix))
            ]
            | .data[0].functions = [
                .data[0].functions[]
                | select(any(.filenames[]; endswith($suffix)))
            ]
        ' "$raw_report" >"$filtered_report"
        FILTERED_REPORTS+=("$filtered_report")
    done
    jq -s '
        .[0] as $first
        | {
            data: [{
                files: [.[].data[0].files[]],
                functions: [.[].data[0].functions[]],
                totals: $first.data[0].totals
            }],
            type: $first.type,
            version: $first.version
        }
    ' "${FILTERED_REPORTS[@]}" >"$REPORT_PATH"
    cp "$REPORT_PATH" "$FUNCTION_REPORT_PATH"
else
    (
        cd "$ROOT"
        env -u RUSTC_WRAPPER \
            CARGO_INCREMENTAL=0 \
            CARGO_TARGET_DIR="$TEMP_ROOT/target" \
            cargo llvm-cov \
                --quiet \
                "${COVERAGE_ARGS[@]}" \
                --branch \
                --json \
                --output-path "$REPORT_PATH" \
                --ignore-filename-regex '(^|/)(tests|benches|examples)/|/(registry|rustc)/'
    )
    cp "$REPORT_PATH" "$FUNCTION_REPORT_PATH"
fi

if [[ -n "$FULL_REPORT_PATH" ]]; then
    cp "$REPORT_PATH" "$FULL_REPORT_PATH"
fi

jq \
    --arg shard "$SHARD" \
    --arg base_ref "$BASE_REF" \
    --rawfile changed_lines "$PRODUCTION_CHANGED_LINES_PATH" \
    --slurpfile target_files "$TARGET_FILES_PATH" \
    --slurpfile function_report "$FUNCTION_REPORT_PATH" '
    def metric($covered):
        ($covered | length) as $count
        | ($covered | map(select(.)) | length) as $covered_count
        | {
            count: $count,
            covered: $covered_count,
            percent: (if $count == 0 then 100 else ($covered_count * 100 / $count) end)
        };

    def overlaps_changed($region; $lines):
        any($lines[]; . >= $region[0] and . <= $region[2]);

    ($changed_lines
        | split("\n")
        | map(select(length > 0) | split("\t"))
        | map({path: .[0], line: (.[1] | tonumber)})) as $changed
    | .data[0] as $data
    | $function_report[0].data[0] as $function_data
    | [
        $target_files[0][] as $path
        | [$changed[] | select(.path == $path) | .line] as $lines
        | ($data.files[] | select(.filename | endswith("/" + $path))) as $file
        | ([
            $file.segments[]
            | select(.[3] and (.[5] | not))
            | {line: .[0], covered: (.[2] > 0)}
            | select(.line as $line | $lines | index($line))
        ]
            | group_by(.line)
            | map(any(.[]; .covered))) as $line_coverage
        | ([
            $function_data.functions[]
            | . as $function
            | ([
                $function.regions[]
                | select(
                    (($function.filenames[.[5]] // "") | endswith("/" + $path))
                    and overlaps_changed(.; $lines)
                )
            ]) as $regions
            | select($regions | length > 0)
            | {
                key: [
                    ($regions | map(.[0:4]) | sort | first),
                    ($regions | map(.[0:4]) | sort | last)
                ],
                covered: ($function.count > 0)
            }
        ]
            | group_by(.key)
            | map(any(.[]; .covered))) as $function_coverage
        | ([
            $function_data.functions[]
            | . as $function
            | $function.regions[]
            | select(
                (($function.filenames[.[5]] // "") | endswith("/" + $path))
                and overlaps_changed(.; $lines)
            )
            | {key: .[0:4], covered: (.[4] > 0)}
        ]
            | group_by(.key)
            | map(any(.[]; .covered))) as $region_coverage
        | ([
            $file.branches[]
            | select(overlaps_changed(.; $lines))
            | {key: (.[0:4] + [0]), covered: (.[4] > 0)},
              {key: (.[0:4] + [1]), covered: (.[5] > 0)}
        ]
            | group_by(.key)
            | map(any(.[]; .covered))) as $branch_coverage
        | {
            path: $path,
            changed_lines: ($lines | length),
            metrics: {
                lines: metric($line_coverage),
                functions: metric($function_coverage),
                regions: metric($region_coverage),
                branches: metric($branch_coverage)
            }
        }
    ] as $files
    | ([
        $files[]
        | .path as $path
        | .metrics
        | to_entries[]
        | select(.value.percent != 100)
        | {path: $path, metric: .key, actual: .value}
    ]) as $failures
    | {
        schema_version: 1,
        coverage_kind: "cardinality-boundary-changed-code",
        shard: $shard,
        base_ref: $base_ref,
        required_percent: 100,
        files: $files,
        failures: $failures,
        passed: (($failures | length) == 0)
    }
    ' "$REPORT_PATH" >"$SUMMARY_PATH"

cat "$SUMMARY_PATH"
jq -e '.passed' "$SUMMARY_PATH" >/dev/null || {
    echo "cardinality boundary changed-code coverage is below 100%" >&2
    exit 1
}
