#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HYPERSCALE_REPO="${HELIX_HYPERSCALE_REPO:-$ROOT/../helix-hyperscale}"
PINNED_WORKTREE="${HELIX_HYPERSCALE_PARITY_WORKTREE:-$ROOT/../helix-hyperscale-migration-parity-source}"
PINNED_REVISION="e5bac15b020c9acac1649c44b58a2cf16dd1f874"
TARGET_REVISION="$(git -C "$ROOT" rev-parse HEAD)"
REPORT_DIR="${MIGRATION_PARITY_REPORT_DIR:-$ROOT/target/migration-parity-reports}"
TOOL_MANIFEST="$ROOT/tools/hyperscale-migration-parity/Cargo.toml"
MAX_SCALE_REPORT_BYTES=$((16 * 1024 * 1024))
SCALE_MAX_NODES="${MIGRATION_PARITY_SCALE_MAX_NODES:-2000000}"

case "$SCALE_MAX_NODES" in
    5000|20000|100000|500000|2000000) ;;
    *)
        echo "MIGRATION_PARITY_SCALE_MAX_NODES must name a configured scale rung" >&2
        exit 2
        ;;
esac

usage() {
    echo "usage: scripts/run-migration-parity.sh contracts|dev|full-correctness|scale-local|scale-minio|full" >&2
    exit 2
}

[[ $# -eq 1 ]] || usage
PROFILE="$1"
case "$PROFILE" in
    contracts|dev|full-correctness|scale-local|scale-minio|full) ;;
    *) usage ;;
esac

contracts() {
    cargo test --locked -p db \
        --features production-coverage,migration-parity,index-lifecycle-testing \
        --test production_migration_contracts
    cargo test --locked -p db --features production-coverage --test production_index_lifecycle_contracts
}

if [[ "$PROFILE" == contracts ]]; then
    cd "$ROOT"
    contracts
    exit 0
fi

command -v jq >/dev/null 2>&1 || {
    echo "$PROFILE migration parity requires jq" >&2
    exit 1
}
if [[ "$PROFILE" == full-correctness || "$PROFILE" == scale-minio || "$PROFILE" == full ]]; then
    [[ -n "${MINIO_ENDPOINT:-}" ]] || {
        echo "$PROFILE requires MINIO_ENDPOINT" >&2
        exit 1
    }
fi
[[ "$(git -C "$ROOT" rev-parse HEAD)" == "$TARGET_REVISION" ]] || {
    echo "target checkout must remain pinned to $TARGET_REVISION" >&2
    exit 1
}

TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/helix-migration-parity.XXXXXX")"
BUILD_TARGET="$TEMP_ROOT/cargo-target"
TOOL_BINARY="$TEMP_ROOT/hyperscale-migration-parity"
worktree_added=0
cleanup() {
    if [[ "$worktree_added" == 1 ]]; then
        git -C "$HYPERSCALE_REPO" worktree remove --force "$PINNED_WORKTREE" >/dev/null 2>&1 || true
    fi
    rm -rf -- "$TEMP_ROOT"
}
trap cleanup EXIT

if ! git -C "$PINNED_WORKTREE" rev-parse HEAD >/dev/null 2>&1 \
    || [[ "$(git -C "$PINNED_WORKTREE" rev-parse HEAD)" != "$PINNED_REVISION" ]]; then
    git -C "$HYPERSCALE_REPO" cat-file -e "$PINNED_REVISION^{commit}"
    if [[ -e "$PINNED_WORKTREE" ]]; then
        git -C "$HYPERSCALE_REPO" worktree remove --force "$PINNED_WORKTREE"
    fi
    git -C "$HYPERSCALE_REPO" worktree add --detach "$PINNED_WORKTREE" "$PINNED_REVISION"
    worktree_added=1
fi
[[ "$(git -C "$PINNED_WORKTREE" rev-parse HEAD)" == "$PINNED_REVISION" ]]
[[ -z "$(git -C "$PINNED_WORKTREE" status --porcelain)" ]] || {
    echo "pinned legacy checkout is dirty: $PINNED_WORKTREE" >&2
    exit 1
}

TARGET_SLATEDB_REVISION="$({
    cargo metadata --locked --manifest-path "$TOOL_MANIFEST" --format-version 1
} | jq -er '
    [.packages[] | select(.name == "slatedb" and .version == "0.15.0")] as $packages
    | if ($packages | length) != 1 then
        error("expected exactly one resolved target SlateDB package")
      else
        $packages[0].source as $source
        | if ($source | type) != "string"
            or ($source | startswith("git+https://github.com/HelixDB/slatedb.git?rev=") | not)
            or ($source | test("#[0-9a-fA-F]{40}$") | not)
          then error("target SlateDB did not resolve from the pinned Git source")
          else $source | capture("#(?<revision>[0-9a-fA-F]{40})$").revision
          end
      end
')"

mkdir -p "$REPORT_DIR"
cd "$ROOT"
if [[ "$PROFILE" == dev ]]; then
    CARGO_TARGET_DIR="$BUILD_TARGET" cargo test --locked --manifest-path "$TOOL_MANIFEST"
fi
CARGO_TARGET_DIR="$BUILD_TARGET" cargo build --manifest-path "$TOOL_MANIFEST" --locked
cp "$BUILD_TARGET/debug/hyperscale-migration-parity" "$TOOL_BINARY"
rm -rf -- "$BUILD_TARGET"

validate_report() {
    local report="$1"
    jq --arg target_revision "$TARGET_REVISION" \
        --arg target_slatedb_revision "$TARGET_SLATEDB_REVISION" -e '
        . as $report
        | .schema_version >= 3
        and .status == "passed"
        and .hash_contract.passed == true
        and .hash_contract.migrated_descending_outputs == 24
        and .revisions.target_helix == $target_revision
        and .revisions.target_slatedb == $target_slatedb_revision
        and .revisions.source_hyperscale == "e5bac15b020c9acac1649c44b58a2cf16dd1f874"
        and (.release_blockers | length == 0)
        and (.scenarios | length > 0)
        and all(
            .scenarios[];
            . as $scenario
            | all($scenario.comparison[]; .differences == 0)
            and all($scenario.migration_jobs[]; .state.state == "completed")
            and $scenario.text_query.passed == true
            and $scenario.vector_query.passed == true
            and $scenario.source_garbage_collection.cold_reopen_passed == true
            and all($scenario.source_gc_durability[]; .differences == 0)
            and $scenario.compaction_drain.passed == true
            and $scenario.compaction_errors.count == 0
            and $scenario.migration_snapshot.v2.legacy_definition_rows == 0
            and $scenario.migration_snapshot.v2.pending_operation_pointers == 0
            and all($scenario.migration_snapshot.v2.canonical_records[]; .state == "active")
            and $scenario.failed_compactions == 0
            and $scenario.source_oracle.source_physical_rows > 0
            and $scenario.source_oracle.source_physical_bytes > 0
            and $scenario.source_oracle.legacy_vector_rows > 0
            and $scenario.source_oracle.materialized_node_vector_properties > 0
            and $scenario.source_oracle.materialized_edge_vector_properties > 0
            and $scenario.source_oracle.unmatched_legacy_vector_rows == 0
            and $scenario.source_oracle.preserved_unmanaged_legacy_vector_rows > 0
            and $scenario.source_oracle.legacy_vector_rows == (
                $scenario.source_oracle.materialized_node_vector_properties
                + $scenario.source_oracle.materialized_edge_vector_properties
                + $scenario.source_oracle.preserved_unmanaged_legacy_vector_rows
            )
            and ($scenario.source_oracle.raw_hash_key_manifest | length) > 0
            and all([
                $scenario.source_oracle.nodes,
                $scenario.source_oracle.current_edges,
                $scenario.source_oracle.legacy_edges,
                $scenario.source_oracle.expected_edges,
                $scenario.source_oracle.edges_by_id,
                $scenario.source_oracle.exact_keys,
                $scenario.target_oracle.nodes,
                $scenario.target_oracle.current_edges,
                $scenario.target_oracle.edges_by_id,
                $scenario.target_oracle.exact_keys,
                $scenario.target_oracle.expected_indexes,
                $scenario.target_oracle.actual_indexes,
                $scenario.target_oracle.expected_graph_state,
                $scenario.target_oracle.actual_graph_state
            ][]; (.sha256 | length) == 64)
        )
        and (.scale_analysis == null or (
            .scale_analysis.passed == true
            and (.scale_analysis.exponent_passed | type) == "boolean"
        ))
    ' "$report" >/dev/null
    if jq -e '.config.scale_nodes > 0' "$report" >/dev/null \
        && [[ "$(wc -c < "$report")" -gt "$MAX_SCALE_REPORT_BYTES" ]]; then
        echo "scale report exceeds ${MAX_SCALE_REPORT_BYTES} bytes: $report" >&2
        return 1
    fi
}

run_report() {
    local name="$1"
    shift
    local report="$REPORT_DIR/$name.json"
    local store="$TEMP_ROOT/$name"
    "$TOOL_BINARY" \
        --profile "$PROFILE" \
        --hyperscale "$PINNED_WORKTREE" \
        --store-root "$store" \
        --report "$report" \
        "$@"
    validate_report "$report"
}

run_dev() {
    run_report dev \
        --batch-rows 1024 \
        --distribution power-law \
        --scenario all \
        --scale-nodes 1000 \
        --scale-edges 4000 \
        --seed-batch-rows 1000 \
        --maximum-scenario-seconds 300 \
        --maximum-suite-seconds 300 \
        --compaction-drain-seconds 5
}

run_crash_matrix() {
    local report="$REPORT_DIR/crash-recovery-matrix.json"
    "$TOOL_BINARY" \
        --profile full-correctness \
        --hyperscale "$PINNED_WORKTREE" \
        --store-root "$TEMP_ROOT/crash-recovery" \
        --report "$report" \
        --batch-rows 1 \
        --scenario all \
        --maximum-open-attempts 10 \
        --maximum-scenario-seconds 300 \
        --crash-recovery-matrix
    jq -e '
        .status == "crash_recovery_matrix_passed"
        and (.entries | length == 66)
        and ([.entries[] | select(.kind == "graph_migration")] | length == 42)
        and ([.entries[] | select(.kind == "index_v2_outbox")] | length == 24)
        and all(.entries[]; .recovery_report.status == "resumed_verification_passed")
    ' "$report" >/dev/null
}

run_migration_failpoint_matrix() {
    local migration_failpoints=(
        job_creation_before_commit
        job_creation_after_commit
        allocator_reservation_before
        allocator_reservation_after
        batch_read_before
        batch_read_after
        batch_write_before
        batch_write_after
        batch_commit_before
        batch_commit_after
        stage_transition_before
        stage_transition_after
        rewrite_completion_before
        rewrite_completion_after
        cleanup_enqueue_before
        cleanup_enqueue_after
        cleanup_delete_before
        cleanup_delete_after
        legacy_vector_reservation_before
        legacy_vector_reservation_after
        legacy_definition_enqueue_before
        legacy_definition_enqueue_after
        legacy_vector_validation_checkpoint_before
        legacy_vector_validation_checkpoint_after
        legacy_vector_metadata_publication_before
        legacy_vector_metadata_publication_after
        legacy_vector_reservation_transition_before
        legacy_vector_reservation_transition_after
        legacy_definition_retirement_before
        legacy_definition_retirement_after
        migration_ready_publication_before
        migration_ready_publication_after
        storage_schema_completion_before
        storage_schema_completion_after
        vector_directory_preflight_commit_before
        vector_directory_preflight_commit_after
        vector_directory_backfill_commit_before
        vector_directory_backfill_commit_after
        vector_directory_verification_commit_before
        vector_directory_verification_commit_after
        vector_directory_publication_commit_before
        vector_directory_publication_commit_after
    )
    for failpoint in "${migration_failpoints[@]}"; do
        run_report "recoverable-${failpoint}" \
            --batch-rows 1 \
            --scenario all \
            --migration-failpoint "$failpoint" \
            --maximum-suite-seconds 86400 \
            --compaction-drain-seconds 60
    done
}

run_full_correctness() {
    local distributions=(uniform power-law star dense self-loop hot-pair)
    for batch_rows in 1 1024; do
        for distribution in "${distributions[@]}"; do
            run_report "shape-local-batch-${batch_rows}-${distribution}" \
                --batch-rows "$batch_rows" \
                --distribution "$distribution" \
                --scenario all \
                --maximum-suite-seconds 86400 \
                --compaction-drain-seconds 60
        done
    done
    run_migration_failpoint_matrix
    run_crash_matrix
    for kind in transient timeout throttled connection-loss; do
        for operation in get head put multipart list delete copy; do
            run_report "fault-${kind}-${operation}" \
                --minio-endpoint "$MINIO_ENDPOINT" \
                --minio-bucket "${MINIO_BUCKET:-helix-migration-parity}" \
                --minio-run-prefix "fault-${kind}-${operation}" \
                --target-fault "${kind}:${operation}:2" \
                --maximum-open-attempts 10 \
                --scenario all \
                --maximum-suite-seconds 86400 \
                --compaction-drain-seconds 60
        done
    done
}

scale_storage() {
    local storage="$1"
    local storage_args=()
    if [[ "$storage" == minio ]]; then
        storage_args=(
            --minio-endpoint "$MINIO_ENDPOINT"
            --minio-bucket "${MINIO_BUCKET:-helix-migration-parity}"
        )
    fi
    local modes=(eager lazy adaptive)
    for mode in "${modes[@]}"; do
        run_report "scale-${storage}-${mode}-5k-20k" \
            "${storage_args[@]}" \
            --minio-run-prefix "scale-${storage}-${mode}-5k-20k" \
            --distribution power-law --scenario "$mode" --batch-rows 1024 \
            --scale-nodes 5000 --scale-edges 20000 --seed-batch-rows 10000 \
            --compaction-drain-seconds 300 --project-next-rows 100000
    done
    if [[ "$SCALE_MAX_NODES" == 5000 ]]; then
        return
    fi
    for mode in "${modes[@]}"; do
        run_report "scale-${storage}-${mode}-20k-80k" \
            "${storage_args[@]}" \
            --minio-run-prefix "scale-${storage}-${mode}-20k-80k" \
            --distribution power-law --scenario "$mode" --batch-rows 1024 \
            --scale-nodes 20000 --scale-edges 80000 --seed-batch-rows 10000 \
            --compaction-drain-seconds 300 --project-next-rows 500000 \
            --scale-baseline-report "$REPORT_DIR/scale-${storage}-${mode}-5k-20k.json"
    done
    if [[ "$SCALE_MAX_NODES" == 20000 ]]; then
        return
    fi
    for mode in "${modes[@]}"; do
        run_report "scale-${storage}-${mode}-100k-400k" \
            "${storage_args[@]}" \
            --minio-run-prefix "scale-${storage}-${mode}-100k-400k" \
            --distribution power-law --scenario "$mode" --batch-rows 1024 \
            --scale-nodes 100000 --scale-edges 400000 --seed-batch-rows 10000 \
            --compaction-drain-seconds 300 --project-next-rows 2500000 \
            --scale-baseline-report "$REPORT_DIR/scale-${storage}-${mode}-5k-20k.json" \
            --scale-baseline-report "$REPORT_DIR/scale-${storage}-${mode}-20k-80k.json"
    done
    if [[ "$SCALE_MAX_NODES" == 100000 ]]; then
        return
    fi

    local slowest
    slowest="$({
        for mode in "${modes[@]}"; do
            jq -r --arg mode "$mode" '[.scenarios[].timings_millis.total] | add | "\(.)\t\($mode)"' \
                "$REPORT_DIR/scale-${storage}-${mode}-100k-400k.json"
        done
    } | sort -k1,1n -k2,2 | tail -1 | cut -f2)"
    [[ -n "$slowest" ]] || {
        echo "failed to select the slowest 100k/400k mode" >&2
        exit 1
    }
    printf '%s\n' "$slowest" > "$REPORT_DIR/scale-${storage}-selected-mode.txt"

    run_report "scale-${storage}-${slowest}-500k-2m" \
        "${storage_args[@]}" \
        --minio-run-prefix "scale-${storage}-${slowest}-500k-2m" \
        --distribution power-law --scenario "$slowest" --batch-rows 1024 \
        --scale-nodes 500000 --scale-edges 2000000 --seed-batch-rows 10000 \
        --compaction-drain-seconds 600 --project-next-rows 10000000 \
        --scale-baseline-report "$REPORT_DIR/scale-${storage}-${slowest}-20k-80k.json" \
        --scale-baseline-report "$REPORT_DIR/scale-${storage}-${slowest}-100k-400k.json"
    if [[ "$SCALE_MAX_NODES" == 500000 ]]; then
        return
    fi
    run_report "scale-${storage}-${slowest}-2m-8m" \
        "${storage_args[@]}" \
        --minio-run-prefix "scale-${storage}-${slowest}-2m-8m" \
        --distribution power-law --scenario "$slowest" --batch-rows 1024 \
        --scale-nodes 2000000 --scale-edges 8000000 --seed-batch-rows 10000 \
        --compaction-drain-seconds 1200 \
        --scale-baseline-report "$REPORT_DIR/scale-${storage}-${slowest}-100k-400k.json" \
        --scale-baseline-report "$REPORT_DIR/scale-${storage}-${slowest}-500k-2m.json"
}

case "$PROFILE" in
    dev) run_dev ;;
    full-correctness) run_full_correctness ;;
    scale-local) scale_storage local ;;
    scale-minio) scale_storage minio ;;
    full)
        run_full_correctness
        scale_storage local
        scale_storage minio
        ;;
esac

echo "migration parity $PROFILE passed; reports: $REPORT_DIR"
