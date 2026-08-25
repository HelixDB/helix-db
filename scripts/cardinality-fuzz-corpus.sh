#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FUZZ_ROOT="$ROOT/crates/db-testkit/fuzz"
RUNS="${FUZZ_RUNS:-128}"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/private/tmp}/helix-cardinality-fuzz.XXXXXX")"

cleanup() {
    rm -rf -- "$TEMP_ROOT"
}
trap cleanup EXIT

case "$RUNS" in
    ''|*[!0-9]*)
        echo "FUZZ_RUNS must be a positive integer" >&2
        exit 2
        ;;
    *)
        if [[ "$RUNS" -eq 0 ]]; then
            echo "FUZZ_RUNS must be a positive integer" >&2
            exit 2
        fi
        ;;
esac

for target in planner_context_ast planner_interpreter; do
    corpus="$TEMP_ROOT/$target"
    mkdir "$corpus"
    cp -R "$FUZZ_ROOT/corpus/$target/." "$corpus/"
    (
        cd "$ROOT"
        env -u RUSTC_WRAPPER cargo fuzz run \
            --fuzz-dir "$FUZZ_ROOT" \
            "$target" \
            "$corpus" \
            -- "-runs=$RUNS"
    )
done
