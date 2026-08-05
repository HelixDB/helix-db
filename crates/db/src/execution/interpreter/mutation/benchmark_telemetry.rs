//! Task-local observations for foreground deletion benchmarks.
//!
//! These counters are compiled only for `production-coverage`. Logical storage
//! counts are explicitly instrumented observations rather than a view into
//! SlateDB's private transaction write batch.

#![allow(
    dead_code,
    reason = "the stacked deletion PRs activate family-specific benchmark hooks incrementally"
)]

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Commit-boundary durations observed inside one foreground mutation request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MutationPhaseTimings {
    /// Time spent preparing every deferred index family.
    pub preparation_ns: u64,
    /// Time spent awaiting the atomic SlateDB commit.
    pub commit_ns: u64,
    /// Time spent publishing cache and worker effects after commit.
    pub post_commit_ns: u64,
}

/// Instrumented logical operations attributed to one deletion request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstrumentedMutationOperations {
    /// Point reads whose call sites opt into the observer.
    pub point_gets: u64,
    /// Multi-get invocations whose call sites opt into the observer.
    pub multi_get_calls: u64,
    /// Keys requested by instrumented multi-get invocations.
    pub multi_get_keys: u64,
    /// Range scans whose call sites opt into the observer.
    pub scans: u64,
    /// Logical put calls whose call sites opt into the observer.
    pub puts: u64,
    /// Logical delete calls whose call sites opt into the observer.
    pub deletes: u64,
    /// Logical merge calls whose call sites opt into the observer.
    pub merges: u64,
    /// Encoded key bytes passed through instrumented writes.
    pub staged_key_bytes: u64,
    /// Encoded value bytes passed through instrumented writes.
    pub staged_value_bytes: u64,
    /// Topology-runtime flush invocations.
    pub topology_flushes: u64,
    /// Nodes observed by deletion-closure construction.
    pub cascade_nodes: u64,
    /// Edges observed by deletion-closure construction.
    pub cascade_edges: u64,
    /// Directed pairs observed by deletion-closure construction.
    pub cascade_pairs: u64,
    /// Terminal secondary-index deletion observations.
    pub secondary_deletions: u64,
    /// Text epochs drained by the request.
    pub text_epochs: u64,
    /// Coalesced entities contained in drained text epochs.
    pub text_entities: u64,
    /// Foreground text object uploads.
    pub text_uploads: u64,
    /// Active vector entities deleted by the request.
    pub vector_deletions: u64,
    /// Surviving HNSW rows considered for deletion repair.
    pub vector_repair_sources: u64,
}

/// Complete task-local benchmark observation for one request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MutationBenchmarkTelemetry {
    /// Durations measured inside the atomic commit boundary.
    pub phases: MutationPhaseTimings,
    /// Call-site instrumentation, never exact SlateDB transaction totals.
    pub instrumented_logical_operations: InstrumentedMutationOperations,
}

#[derive(Default)]
struct MutableTelemetry {
    snapshot: MutationBenchmarkTelemetry,
}

tokio::task_local! {
    static ACTIVE_TELEMETRY: Arc<Mutex<MutableTelemetry>>;
}

/// Runs one future with an isolated mutation observer and returns its snapshot.
///
/// Nested observers are intentionally unsupported: every benchmark sample owns
/// exactly one request and therefore one unambiguous set of counters.
pub async fn observe<F>(future: F) -> (F::Output, MutationBenchmarkTelemetry)
where
    F: Future,
{
    let telemetry = Arc::new(Mutex::new(MutableTelemetry::default()));
    let output = ACTIVE_TELEMETRY.scope(Arc::clone(&telemetry), future).await;
    let snapshot = telemetry
        .lock()
        .expect("mutation benchmark telemetry lock is not poisoned")
        .snapshot;
    (output, snapshot)
}

/// Records one complete internal commit phase without affecting its ordering.
pub(crate) fn record_phase(phase: MutationBenchmarkPhase, duration: Duration) {
    let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
    with_snapshot(|snapshot| match phase {
        MutationBenchmarkPhase::Preparation => snapshot.phases.preparation_ns = nanos,
        MutationBenchmarkPhase::Commit => snapshot.phases.commit_ns = nanos,
        MutationBenchmarkPhase::PostCommit => snapshot.phases.post_commit_ns = nanos,
    });
}

/// Closed set of internally timed commit phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationBenchmarkPhase {
    Preparation,
    Commit,
    PostCommit,
}

/// Records one instrumented point read.
pub fn record_point_get() {
    with_operations(|operations| operations.point_gets += 1);
}

/// Records one instrumented multi-get and its requested key count.
pub fn record_multi_get(keys: usize) {
    with_operations(|operations| {
        operations.multi_get_calls += 1;
        operations.multi_get_keys = operations
            .multi_get_keys
            .saturating_add(u64::try_from(keys).unwrap_or(u64::MAX));
    });
}

/// Records one instrumented scan.
pub fn record_scan() {
    with_operations(|operations| operations.scans += 1);
}

/// Records one instrumented put payload.
pub fn record_put(key_bytes: usize, value_bytes: usize) {
    record_write(key_bytes, value_bytes, |operations| operations.puts += 1);
}

/// Records one instrumented delete payload.
pub fn record_delete(key_bytes: usize) {
    record_write(key_bytes, 0, |operations| operations.deletes += 1);
}

/// Records one instrumented merge payload.
pub fn record_merge(key_bytes: usize, value_bytes: usize) {
    record_write(key_bytes, value_bytes, |operations| operations.merges += 1);
}

/// Records one topology runtime flush invocation.
pub fn record_topology_flush() {
    with_operations(|operations| operations.topology_flushes += 1);
}

/// Records graph entities observed by one deletion closure.
pub fn record_cascade(nodes: usize, edges: usize, pairs: usize) {
    with_operations(|operations| {
        operations.cascade_nodes = operations
            .cascade_nodes
            .saturating_add(u64::try_from(nodes).unwrap_or(u64::MAX));
        operations.cascade_edges = operations
            .cascade_edges
            .saturating_add(u64::try_from(edges).unwrap_or(u64::MAX));
        operations.cascade_pairs = operations
            .cascade_pairs
            .saturating_add(u64::try_from(pairs).unwrap_or(u64::MAX));
    });
}

/// Records terminal secondary-index deletions.
pub fn record_secondary_deletions(count: usize) {
    with_operations(|operations| {
        operations.secondary_deletions = operations
            .secondary_deletions
            .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
    });
}

/// Records one drained text epoch and its coalesced entity count.
pub fn record_text_epoch(entities: usize) {
    with_operations(|operations| {
        operations.text_epochs += 1;
        operations.text_entities = operations
            .text_entities
            .saturating_add(u64::try_from(entities).unwrap_or(u64::MAX));
    });
}

/// Records object uploads attributable to foreground text publication.
pub fn record_text_uploads(count: usize) {
    with_operations(|operations| {
        operations.text_uploads = operations
            .text_uploads
            .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
    });
}

/// Records cohort deletions and surviving rows selected for HNSW repair.
pub fn record_vector_deletion(deletions: usize, repair_sources: usize) {
    with_operations(|operations| {
        operations.vector_deletions = operations
            .vector_deletions
            .saturating_add(u64::try_from(deletions).unwrap_or(u64::MAX));
        operations.vector_repair_sources = operations
            .vector_repair_sources
            .saturating_add(u64::try_from(repair_sources).unwrap_or(u64::MAX));
    });
}

fn record_write(
    key_bytes: usize,
    value_bytes: usize,
    update: impl FnOnce(&mut InstrumentedMutationOperations),
) {
    with_operations(|operations| {
        update(operations);
        operations.staged_key_bytes = operations
            .staged_key_bytes
            .saturating_add(u64::try_from(key_bytes).unwrap_or(u64::MAX));
        operations.staged_value_bytes = operations
            .staged_value_bytes
            .saturating_add(u64::try_from(value_bytes).unwrap_or(u64::MAX));
    });
}

fn with_operations(update: impl FnOnce(&mut InstrumentedMutationOperations)) {
    with_snapshot(|snapshot| update(&mut snapshot.instrumented_logical_operations));
}

fn with_snapshot(update: impl FnOnce(&mut MutationBenchmarkTelemetry)) {
    let _ = ACTIVE_TELEMETRY.try_with(|telemetry| {
        update(
            &mut telemetry
                .lock()
                .expect("mutation benchmark telemetry lock is not poisoned")
                .snapshot,
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn observers_are_task_local_and_accumulate_saturating_counts() {
        let (_, snapshot) = observe(async {
            record_point_get();
            record_multi_get(3);
            record_put(4, 5);
            record_delete(6);
            record_merge(7, 8);
            record_cascade(2, 3, 4);
            record_text_epoch(9);
            record_vector_deletion(10, 11);
        })
        .await;

        assert_eq!(snapshot.instrumented_logical_operations.point_gets, 1);
        assert_eq!(snapshot.instrumented_logical_operations.multi_get_keys, 3);
        assert_eq!(snapshot.instrumented_logical_operations.puts, 1);
        assert_eq!(snapshot.instrumented_logical_operations.deletes, 1);
        assert_eq!(snapshot.instrumented_logical_operations.merges, 1);
        assert_eq!(
            snapshot.instrumented_logical_operations.staged_key_bytes,
            17
        );
        assert_eq!(
            snapshot.instrumented_logical_operations.staged_value_bytes,
            13
        );
        assert_eq!(snapshot.instrumented_logical_operations.cascade_edges, 3);
        assert_eq!(snapshot.instrumented_logical_operations.text_entities, 9);
        assert_eq!(
            snapshot
                .instrumented_logical_operations
                .vector_repair_sources,
            11
        );
    }

    #[test]
    fn recording_without_an_observer_is_a_no_op() {
        record_point_get();
        record_topology_flush();
    }
}
