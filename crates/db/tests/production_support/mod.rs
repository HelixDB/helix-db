//! Entry points for feature-gated production-codec coverage contracts.
//!
//! The `production-coverage` feature compiles this module into the database
//! crate so its children can reach private implementation boundaries through
//! their owning modules. Keeping the orchestration source under `tests/` lets
//! the coverage report exclude harness code while measuring the unchanged
//! production implementations and codecs that the contracts exercise.
//!
//! Integration tests should call the public runners in this module instead of
//! depending on private vector modules directly.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

mod index_v2;
#[cfg(feature = "production-scale")]
mod index_v2_scale;

/// Subscriber that enables production log fields without retaining events.
///
/// Coverage contracts use this process-global sink so diagnostics evaluate
/// their structured fields while keeping test output quiet and deterministic.
#[derive(Default)]
struct CoverageSubscriber {
    next_span_id: AtomicU64,
}

impl Subscriber for CoverageSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(
            self.next_span_id
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1),
        )
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

/// Installs the quiet all-level subscriber once for diagnostic coverage.
fn enable_vector_tracing() {
    static ENABLE: Once = Once::new();
    ENABLE.call_once(|| {
        let _ = tracing::subscriber::set_global_default(CoverageSubscriber::default());
    });
}

/// Exercises the descriptor-bound vector memory registry through production code.
///
/// The production-only integration test calls this runner to cover hydration,
/// refresh, lease, retirement, and commit fencing transitions that cannot be
/// reached through the crate's public API. The runner persists no alternate
/// representation and is unavailable unless `production-coverage` is enabled.
pub async fn vector_memory_registry_contracts() {
    crate::search::vector::run_memory_registry_contracts().await;
}

/// Exercises vector-memory identity, fencing, hydration, and corruption contracts.
///
/// The runner uses tenant-scoped production keys and current row codecs to
/// validate every cache capability, bounded admission, shutdown, read-through
/// repair, and descriptor-bound fail-closed behavior.
pub async fn vector_memory_store_contracts() {
    crate::search::vector::run_memory_store_contracts().await;
}

/// Exercises every active layer-zero filtering and sampling policy branch.
///
/// The runner projects deployed settings into the pure policy ADTs, then checks
/// metric compatibility, bypass behavior, activation thresholds, and adaptive
/// quality boundaries without performing storage or random-number operations.
pub fn vector_policy_contracts() {
    crate::search::vector::run_policy_contracts();
}

/// Exercises bounded deterministic SimHasher registry admission and reuse.
///
/// The runner checks typed limits, descriptor algorithm identity, exact byte
/// boundaries, LRU eviction, deterministic recreation, and concurrent
/// single-flight publication using the production projection constructor.
pub fn vector_simhash_registry_contracts() {
    crate::search::vector::run_simhash_registry_contracts();
}

/// Exercises the deployed transactional SimHash-row cache contract.
///
/// The runner verifies legacy and tenant-scoped constructors, descriptor-bound
/// registry admission, missing/present/corrupt reads, current f32 hashing,
/// measured writes, dimension rejection, and deletion through the unchanged
/// dedicated SimHash key and value codecs.
pub async fn vector_simhash_contracts() {
    crate::search::vector::run_simhash_contracts().await;
}

/// Exercises measured vector-transaction replacement and failure contracts.
///
/// The runner verifies final-write accounting, checkpoint ownership, shared
/// recorder identity, read delegation, and pre-write failure injection without
/// committing or changing any current vector row representation.
pub async fn vector_write_transaction_contracts() {
    crate::search::vector::run_write_transaction_contracts().await;
}

/// Exercises the typed storage boundary for every current vector row family.
///
/// The runner verifies scoped key ownership, deployed value codecs, opaque
/// scan tokens, cross-keyspace rejection, and exhaustive lane cleanup through
/// measured transactions without introducing a new physical representation.
pub async fn vector_storage_contracts() {
    crate::search::vector::run_storage_contracts().await;
}

/// Exercises the single production vector-search session and helper policies.
///
/// The runner covers validation, empty and populated sessions, observer
/// publication, deterministic prefetch selection, visited-state admission, and
/// typed layer reads without adding an alternate search implementation.
pub async fn vector_search_contracts() {
    enable_vector_tracing();
    crate::search::vector::run_search_contracts().await;
}

/// Exercises operation-local mutation state and typed graph-write repair.
///
/// The runner verifies row identity, clean/dirty transitions, first-original
/// retention, fresh-row proof, bounded eviction, entry-candidate cleanup,
/// stale-root recovery, and current neighbor-row writes. It uses only deployed
/// vector encodings in isolated databases.
pub async fn vector_mutation_cache_contracts() {
    enable_vector_tracing();
    crate::search::vector::run_mutation_contracts().await;
}

/// Exercises active f32 views, typed candidates, result units, and RNG isolation.
///
/// The runner covers only the currently descriptor-bindable f32 codec and
/// process-local runtime values. Reserved f16, binary, and binary-quantized
/// formats remain disabled, and no persisted vector row is read or rewritten.
pub fn vector_primitive_contracts() {
    crate::search::vector::run_primitive_contracts();
}

/// Exercises active distance semantics and canonical neighbor-set invariants.
///
/// The runner compares scalar and architecture-dispatched f32 arithmetic,
/// rejects mismatched dimensions, and validates bounded neighbor differences
/// against the unchanged deployed row encoders. Reserved codecs remain absent.
pub fn vector_distance_neighbor_contracts() {
    crate::search::vector::run_distance_neighbor_contracts();
}

/// Exercises request-owned vector read views and generation-bound readers.
///
/// The runner delegates every `DbReadOps` operation through both a write
/// transaction and a stable snapshot, then verifies exact-generation cache
/// leasing and fail-closed metric/visibility fallbacks.
pub async fn vector_read_boundary_contracts() {
    crate::search::vector::run_read_boundary_contracts().await;
}

/// Exercises the vector-index facade's identity, DDL, and row-safety contracts.
///
/// The runner covers descriptor-bound SimHash construction, write-once
/// dimensions, creation validation, typed current-row lookup, operation-local
/// item caching, missing/corrupt row rejection, search diagnostics, and drop.
/// It writes no new row family and changes none of the deployed encodings.
pub async fn vector_index_contracts() {
    crate::search::vector::run_index_contracts().await;
}

/// Exercises every stable V2 operation and upload failpoint against durable rows.
///
/// Each boundary is injected twice from a clean database. The contract then
/// proves that the durable state is either unchanged, recoverably claimed, or
/// already terminal; no test-only persistence representation is involved.
pub async fn index_v2_outbox_failpoint_contracts() {
    index_v2::run_outbox_failpoint_contracts().await;
}

/// Runs the V2 secondary lifecycle against a deterministic reference model.
///
/// The state machine covers public lifecycle semantics, graph-source mutation,
/// physical lookup, reopen, typed blocking/retry, abort cleanup, drop, and
/// generation recreation through production repositories and drivers.
pub async fn index_v2_secondary_state_machine_contracts() {
    index_v2::run_secondary_state_machine_contracts().await;
}

/// Exercises tenant isolation and global outbox discovery across 16 scopes.
pub async fn index_v2_multi_scope_discovery_contracts() {
    index_v2::run_multi_scope_discovery_contracts().await;
}

/// Runs the non-ignored 100k production-entry lifecycle scale contract.
///
/// This runner seeds authoritative graph rows through current typed codecs,
/// then routes every index through the public DDL interpreter, supervised
/// worker, refreshed catalog, and public search path. It is kept behind the
/// explicit `production-scale` feature because it is a release gate rather
/// than a unit-test workload.
#[cfg(feature = "production-scale")]
pub async fn index_v2_secondary_text_scale_contracts() {
    index_v2_scale::run_secondary_text_tenant().await;
}

/// Reproduces text CREATE/search/DROP without the full release-scale fixture.
#[cfg(feature = "production-scale")]
pub async fn index_v2_text_drop_smoke() {
    index_v2_scale::run_text_drop_smoke().await;
}

/// Reproduces text CREATE/search/DROP after multi-split compaction.
#[cfg(feature = "production-scale")]
pub async fn index_v2_text_drop_multi_split_smoke() {
    index_v2_scale::run_text_drop_multi_split_smoke().await;
}

/// Runs the non-ignored 100k 128D f32 vector lifecycle scale contract.
#[cfg(feature = "production-scale")]
pub async fn index_v2_vector_scale_contracts() {
    index_v2_scale::run_vector().await;
}
