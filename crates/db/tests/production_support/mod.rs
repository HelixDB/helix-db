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

#[doc(hidden)]
pub use crate::execution::interpreter::mutation::benchmark_telemetry::{
    InstrumentedMutationOperations, MutationBenchmarkTelemetry, MutationPhaseTimings,
};

pub use crate::search::vector::{
    VectorBatchBenchmarkCacheLimits, VectorBatchBenchmarkCase, VectorBatchBenchmarkFixture,
    VectorBatchBenchmarkMetric, VectorBatchBenchmarkSample, VectorBatchBenchmarkWorkload,
};

/// Current managed-index storage version exposed to production fixtures.
pub const CURRENT_INDEX_STORAGE_VERSION: u16 =
    crate::index_lifecycle::CURRENT_INDEX_STORAGE_VERSION;

mod deletion_batch;
pub use deletion_batch::{
    DeletionBatchSize, DeletionBenchmarkCachePolicy, DeletionBenchmarkCase,
    DeletionBenchmarkFixture, DeletionBenchmarkSample, DeletionBenchmarkWorkload,
    PhysicalObjectStoreOperations,
};

mod graph_mutation_representation;
mod index_lifecycle;
#[cfg(feature = "production-scale")]
mod index_lifecycle_scale;
mod index_lifecycle_text_rows;
mod index_lifecycle_typed_boundaries;
mod migration_text_rebuild;
mod secondary_equality_hot_path;
mod v1_migration;

pub use graph_mutation_representation::graph_mutation_representation_contracts;
pub use secondary_equality_hot_path::{
    benchmark_million_sequential_id_bitmap, SecondaryEqualityHotPathFixture,
    SecondaryEqualityInsertMode, SecondaryEqualityInsertSample, SecondaryEqualityInspection,
    SecondaryEqualityLookupInspection, SecondaryEqualityMillionBitmapSample,
    SecondaryEqualityReadMode, SecondaryEqualityReadSample,
};

pub use crate::search::text::{
    FtsPrefilterBenchmarkCase, FtsPrefilterBenchmarkFixture, FtsPrefilterBenchmarkLayout,
    FtsPrefilterBenchmarkSample, FtsPrefilterBenchmarkStrategy,
};
pub use migration_text_rebuild::{
    inspect_legacy_text_physical_rows, inspect_legacy_text_source, repair_legacy_text_source,
    seed_legacy_text_source_fixture, seed_populated_legacy_text_fixture,
    seed_recovery_legacy_text_fixture, LegacyTextFixtureCase, LegacyTextFixtureDocument,
    LegacyTextPhysicalEvidence, LegacyTextSourceEvidence, LegacyTextSourceFixture,
    LegacyTextSourceFixtureKind, PopulatedLegacyTextFixture,
};
pub use v1_migration::{
    populated_v1_current_index_migration_contract, v1_equality_semantics_migration_contract,
    v1_graph_crash_recovery_contract, v1_malformed_catalog_failure_preservation_contract,
    v1_prefix_successor_contract, v1_property_hash_collision_migration_contract,
    v1_range_failure_preservation_contract, v1_range_semantics_migration_contract,
    v1_secondary_retirement_failpoint_contract, v1_unique_failure_preservation_contract,
    V1ActiveIndexObservation, V1CollisionIndexObservation, V1CollisionMigrationObservation,
    V1CrashRecoveryObservation, V1ElementKind, V1EqualityMigrationObservation,
    V1EqualityQueryObservation, V1MalformedCatalogObservation, V1OracleValue,
    V1PopulatedMigrationObservation, V1PrefixSuccessorObservation, V1RangeAccess, V1RangeBound,
    V1RangeCaseObservation, V1RangeDirection, V1RangeFailureCaseObservation,
    V1RangeFailureMigrationObservation, V1RangeMigrationObservation,
    V1RetirementFailpointObservation, V1SemanticRow, V1UniqueMigrationObservation,
};

/// Runs graph-first legacy-definition migration contracts with one-row batches.
pub async fn migration_definition_contracts() {
    crate::migrations::production_contracts::run_migration_contracts().await;
}

/// Covers typed reader rejection for version two and every partial schema stage.
pub async fn writer_migration_requirement_contracts() {
    crate::migrations::production_contracts::run_writer_migration_requirement_contracts().await;
}

/// Runs the exhaustive exact cardinality interpreter contract matrix.
pub async fn interpreter_cardinality_program_contracts() {
    crate::execution::interpreter::run_cardinality_production_contracts().await;
}

/// Exercises literal secondary reads and verified ranges through exact storage primitives.
pub async fn secondary_exact_storage_contracts() {
    crate::index_lifecycle::secondary::run_exact_production_contracts().await;
}

/// Rejects or completes legacy secondary work when automatic driving is disabled.
pub async fn migration_disabled_secondary_worker_open_contract() {
    crate::migrations::production_contracts::run_disabled_secondary_worker_open_contract().await;
}

/// Proves populated legacy HNSW adoption, cold reopen, DROP, and recreate.
pub async fn migration_vector_adoption_contract() {
    crate::migrations::production_contracts::run_vector_adoption_contract().await;
}

/// Exercises the red adoption validation-checkpoint timeout regression.
pub async fn migration_vector_adoption_failpoint_recovery_contracts() -> Vec<&'static str> {
    crate::migrations::production_contracts::run_vector_adoption_failpoint_recovery_contracts()
        .await
}

/// Proves corrupt legacy physical lanes fail closed without source mutation.
pub async fn migration_vector_corruption_contracts() {
    crate::migrations::production_contracts::run_vector_corruption_contracts().await;
}

/// Proves ineligible legacy vector definitions retain the rebuild path.
pub async fn migration_vector_ineligible_contracts() {
    crate::migrations::production_contracts::run_vector_ineligible_contracts().await;
}

/// Proves mismatched names and reservations fail before source mutation.
pub async fn migration_vector_ownership_conflict_contracts() {
    crate::migrations::production_contracts::run_vector_ownership_conflict_contracts().await;
}

/// Runs malformed, partial, older, and future V2 bootstrap rejection contracts.
pub async fn migration_bootstrap_rejection_contracts() {
    crate::migrations::production_contracts::run_bootstrap_rejection_contracts().await;
}

/// Proves recoverable migration failures preserve source definitions and resume.
pub async fn migration_failure_preservation_contract() {
    crate::migrations::production_contracts::run_failure_preservation_contract().await;
}

/// Proves vector materialization and retirement recover at every batch boundary.
pub async fn migration_vector_failpoint_recovery_contracts() {
    crate::migrations::production_contracts::run_vector_failpoint_recovery_contracts().await;
}

/// Proves a repaired zero-cosine payload resumes from its exact failed cursor.
pub async fn migration_vector_zero_cosine_recovery_contract() {
    crate::migrations::production_contracts::run_vector_zero_cosine_recovery_contract().await;
}

/// Proves exact Active definitions retire and conflicting definitions fail closed.
pub async fn migration_existing_active_and_conflict_contracts() {
    crate::migrations::production_contracts::run_existing_active_and_conflict_contracts().await;
}

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

/// Exercises descriptor-bound startup hydration through Active V2 handles.
///
/// The runner covers empty, unpartitioned, partitioned, refreshed, bounded,
/// corrupt, duplicate, cross-scope, and cancelled loads. It invokes the
/// unchanged production loader and current V1 key/value codecs.
pub async fn vector_hydration_contracts() {
    crate::search::vector::run_hydration_contracts().await;
}

/// Exercises descriptor-bound vector generation authority and rejection paths.
///
/// The runner projects building, active, aborting, and dropping capabilities
/// through canonical V2 records, verifies all current metrics, and proves
/// mismatched operations, layouts, descriptors, and kernels fail closed.
pub fn vector_generation_contracts() {
    crate::search::vector::run_generation_contracts();
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
    crate::search::vector::run_write_cache_contracts();
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

/// Characterizes the independent finite-score magnitude oracle and active kernels.
pub fn vector_magnitude_oracle_and_kernel_contracts() {
    crate::search::vector::run_magnitude_oracle_and_kernel_contracts();
}

/// Freezes accepted vector bytes/scores and cosine's existing finite domain.
pub async fn vector_magnitude_golden_and_cosine_contracts() {
    crate::search::vector::run_magnitude_golden_and_cosine_contracts().await;
}

/// Requires current-format row decoding to reject out-of-domain finite values.
pub fn vector_magnitude_current_row_decode_contracts() {
    crate::search::vector::run_magnitude_current_row_decode_contracts();
}

/// Requires insert and upsert magnitude rejection before writes or cache effects.
pub async fn vector_magnitude_mutation_contracts() {
    crate::search::vector::run_magnitude_mutation_contracts().await;
}

/// Requires unrestricted vector search to reject out-of-domain finite queries.
pub async fn vector_magnitude_search_contracts() {
    crate::search::vector::run_magnitude_search_contracts().await;
}

/// Requires restricted vector search to enforce the same numeric domain.
pub async fn vector_magnitude_restricted_search_contracts() {
    crate::search::vector::run_magnitude_restricted_search_contracts().await;
}

/// Requires legacy physical validation to reject out-of-domain finite rows.
pub async fn vector_magnitude_legacy_validation_contracts() {
    crate::search::vector::run_magnitude_legacy_validation_contracts().await;
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

/// Exercises restricted vector search through the deployed row representation.
///
/// The runner covers admission, empty and oversized requests, every active
/// metric, corruption rejection, SimHash-directory lifecycle and seeding,
/// bounded bridge traversal, termination reasons, membership, and recall.
pub async fn vector_restricted_search_contracts() {
    crate::search::vector::run_restricted_contracts().await;
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
pub async fn index_lifecycle_outbox_failpoint_contracts() {
    index_lifecycle::run_outbox_failpoint_contracts().await;
}

/// Enters the configured process-abort failpoint for a subprocess contract.
pub fn index_lifecycle_failpoint_abort_probe() -> ! {
    crate::index_lifecycle::failpoints::production_contracts::abort_probe()
}

/// Runs the V2 secondary lifecycle against a deterministic reference model.
///
/// The state machine covers public lifecycle semantics, graph-source mutation,
/// physical lookup, reopen, typed blocking/retry, abort cleanup, drop, and
/// generation recreation through production repositories and drivers.
pub async fn index_lifecycle_secondary_state_machine_contracts() {
    index_lifecycle::run_secondary_state_machine_contracts().await;
}

/// Exercises tenant isolation and global outbox discovery across 16 scopes.
pub async fn index_lifecycle_multi_scope_discovery_contracts() {
    index_lifecycle::run_multi_scope_discovery_contracts().await;
}

/// Exercises compact typed model, catalog, and resource-admission boundaries.
///
/// These synchronous contracts cover storage-version rejection, all three
/// Active catalog-handle projections, duplicate protection, and every stable
/// Active text mutation resource ceiling through production constructors.
pub fn index_lifecycle_typed_boundary_contracts() {
    index_lifecycle_typed_boundaries::run();
    crate::index_lifecycle::text::run_active_preflight_contracts();
}

/// Exercises every Active text serving read and corruption boundary.
///
/// The owning-module child harness uses current typed roots, pages, and entity
/// states to prove family refinement, partition shape, presence, value kind,
/// ownership, and revision checks before Tantivy or object-store access.
pub async fn index_lifecycle_text_serving_contracts() {
    crate::index_lifecycle::text::run_serving_contracts().await;
}

/// Exercises Active text state-only retirement and serialized preflight.
///
/// The owning-module child harness proves family/entity authority, root/state
/// integrity, exact resource admission, input revalidation, and atomic dead
/// state staging through current typed rows.
pub async fn index_lifecycle_active_text_retirement_contracts() {
    crate::index_lifecycle::text::run_active_retirement_contracts().await;
}

/// Proves a real graph conflict leaves one unattached blob and no V2 row changes.
pub async fn interpreter_active_text_graph_conflict_contracts() {
    crate::execution::interpreter::production_contracts::run_active_text_graph_conflict().await;
}

/// Proves Active text epochs publish once per destination and skip delete-only uploads.
#[cfg(feature = "index-lifecycle-testing")]
pub async fn interpreter_active_text_transaction_batching_contracts() {
    crate::execution::interpreter::production_contracts::run_text_transaction_batching_contracts()
        .await;
}

/// Proves production interpreter reads cannot bypass their request snapshot.
pub async fn interpreter_request_read_view_guard_contracts() {
    crate::execution::interpreter::production_contracts::run_request_read_view_guards().await;
}

/// Proves planning catalog authority transfers through write-open and refresh.
pub async fn interpreter_catalog_write_open_authority_contracts() {
    crate::execution::interpreter::production_contracts::run_catalog_write_open_authority_contracts()
        .await;
}

/// Proves request-mode composition and direct operation-owned mutation commits.
pub async fn interpreter_request_mode_and_isolated_mutation_contracts() {
    crate::execution::interpreter::production_contracts::run_request_mode_and_isolated_mutation_contracts()
        .await;
}

/// Proves coalesced topology rows preserve transactional graph semantics.
pub async fn interpreter_topology_mutation_contracts() {
    crate::execution::interpreter::production_contracts::run_topology_mutation_contracts().await;
}

/// Exercises scheduler transfer and row-projection branches through production code.
pub async fn interpreter_scheduler_and_projection_contracts() {
    crate::execution::interpreter::production_contracts::run_scheduler_and_projection_contracts()
        .await;
}

#[cfg(feature = "index-lifecycle-testing")]
pub use crate::execution::interpreter::production_contracts::{
    run_text_transaction_batch_benchmark_sample, TextTransactionBatchBenchmarkCase,
    TextTransactionBatchBenchmarkSample,
};

/// Verifies one Active text generation's complete durable row graph.
///
/// The observer waits for request-independent finalization work to drain, then
/// cross-checks manifest roots, pages, entity live state, and every split slot.
pub async fn index_lifecycle_text_steady_state_contracts(
    db: &crate::HelixDB,
    expected_live_entities: usize,
) {
    index_lifecycle_text_rows::verify_steady_state(db, expected_live_entities).await;
}

/// Verifies terminal text cleanup removed every physical and transient row.
///
/// Canonical index and terminal operation history rows are intentionally
/// retained by the lifecycle contract; generation-owned text rows must
/// converge to absence.
pub async fn index_lifecycle_text_dropped_row_contracts(db: &crate::HelixDB) {
    index_lifecycle_text_rows::verify_dropped(db).await;
}

/// Runs the non-ignored 100k production-entry lifecycle scale contract.
///
/// This runner seeds authoritative graph rows through current typed codecs,
/// then routes every index through the public DDL interpreter, supervised
/// worker, refreshed catalog, and public search path. It is kept behind the
/// explicit `production-scale` feature because it is a release gate rather
/// than a unit-test workload.
#[cfg(feature = "production-scale")]
pub async fn index_lifecycle_secondary_text_scale_contracts() {
    index_lifecycle_scale::run_secondary_text_tenant().await;
}

/// Reproduces text CREATE/search/DROP without the full release-scale fixture.
#[cfg(feature = "production-scale")]
pub async fn index_lifecycle_text_drop_smoke() {
    index_lifecycle_scale::run_text_drop_smoke().await;
}

/// Reproduces text CREATE/search/DROP after multi-split compaction.
#[cfg(feature = "production-scale")]
pub async fn index_lifecycle_text_drop_multi_split_smoke() {
    index_lifecycle_scale::run_text_drop_multi_split_smoke().await;
}

/// Runs the non-ignored 100k 128D f32 vector lifecycle scale contract.
#[cfg(feature = "production-scale")]
pub async fn index_lifecycle_vector_scale_contracts() {
    index_lifecycle_scale::run_vector().await;
}

/// Runs the fixed 8k public vector lifecycle contract used by bounded CI.
#[cfg(feature = "production-scale")]
pub async fn index_lifecycle_vector_ci_contracts() {
    index_lifecycle_scale::run_vector_ci().await;
}

/// Runs the 50k 1536D indexed-prefilter, one-hop, restricted-vector benchmark.
#[cfg(feature = "production-scale")]
pub async fn traversal_vector_prefilter_scale_contract() {
    index_lifecycle_scale::run_traversal_vector_prefilter().await;
}

/// Runs the disk-backed 1M 1536D traversal benchmark at four selectivities.
#[cfg(feature = "production-scale")]
pub async fn traversal_vector_prefilter_1m_scale_contract() {
    index_lifecycle_scale::run_traversal_vector_prefilter_1m().await;
}

/// Runs one full configured-batch resource blocker for every V2 index family.
///
/// Each family proves exact-checkpoint retry after a higher-limit reopen,
/// successful activation and drop, then a second blocked build's abort and
/// complete cleanup.
#[cfg(feature = "production-scale")]
pub async fn index_lifecycle_blocked_limit_scale_contracts() {
    index_lifecycle_scale::run_blocked_limits().await;
}

/// Runs vector property materialization and physical retirement for 100k rows.
#[cfg(feature = "production-scale")]
pub async fn vector_migration_scale_100k() {
    crate::migrations::run_vector_migration_scale_contract(100_000).await;
}

/// Runs vector property materialization and physical retirement for one million rows.
#[cfg(feature = "production-scale")]
pub async fn vector_migration_scale_1m() {
    crate::migrations::run_vector_migration_scale_contract(1_000_000).await;
}

/// Runs the opt-in ten-million-row vector migration release soak.
#[cfg(feature = "production-scale")]
pub async fn vector_migration_scale_10m() {
    crate::migrations::run_vector_migration_scale_contract(10_000_000).await;
}
