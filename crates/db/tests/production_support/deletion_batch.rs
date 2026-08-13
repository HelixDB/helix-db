//! Deterministic foreground-deletion benchmark fixtures.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::stream::BoxStream;
use helix_ast::prelude::*;
use helix_ast::query::QueryRequest;
use helix_planner::{context::ParamBindings, exec::ExecutablePlan, planning};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};

use crate::execution::interpreter::mutation::benchmark_telemetry::{
    self, MutationBenchmarkTelemetry,
};
use crate::{config, error::HelixDbError, search, HelixDB};

/// Supported deletion sizes or the explicit non-blocking 100k stress boundary.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DeletionBatchSize {
    /// A merge-blocking foreground case within the supported 10k boundary.
    Supported(NonZeroUsize),
    /// The explicit 100k manual stress/failure-boundary case.
    Stress100k,
}

impl DeletionBatchSize {
    /// Validates the supported foreground batch range through 10,000 entities.
    ///
    /// ```
    /// # #[cfg(feature = "production-coverage")] {
    /// use db::production_coverage::DeletionBatchSize;
    ///
    /// assert_eq!(
    ///     DeletionBatchSize::try_supported(10_000)
    ///         .expect("10k is supported")
    ///         .get(),
    ///     10_000,
    /// );
    /// assert!(DeletionBatchSize::try_supported(10_001).is_err());
    /// # }
    /// ```
    pub fn try_supported(value: usize) -> Result<Self, HelixDbError> {
        let value = NonZeroUsize::new(value).ok_or_else(|| {
            HelixDbError::Config("deletion benchmark batch size must be positive".to_string())
        })?;
        if value.get() > 10_000 {
            return Err(HelixDbError::Config(
                "supported deletion benchmark batches contain at most 10,000 entities".to_string(),
            ));
        }
        Ok(Self::Supported(value))
    }

    /// Selects the explicit non-blocking 100k stress/failure boundary.
    pub const fn stress_100k() -> Self {
        Self::Stress100k
    }

    /// Returns the concrete number of requested entities or relationships.
    pub const fn get(self) -> usize {
        match self {
            Self::Supported(value) => value.get(),
            Self::Stress100k => 100_000,
        }
    }
}

/// Stable graph/deletion shapes exercised by the benchmark harness.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DeletionBenchmarkWorkload {
    /// Deletes nodes without incident edges.
    IsolatedNodes,
    /// Deletes every node in a directed chain.
    ChainNodes,
    /// Deletes the center of an outbound star.
    HighDegreeNode,
    /// Deletes parallel edges through the edge-ID API.
    ParallelEdgesById,
    /// Deletes independent relationships through the pair API.
    EdgePairs,
    /// Deletes independent relationships through the labeled-pair API.
    LabeledEdgePairs,
}

/// Index families maintained by a measured deletion transaction.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DeletionBenchmarkIndexes {
    /// Measures graph storage without configured V2 index targets.
    None,
    /// Maintains node and edge equality indexes.
    Secondary,
    /// Maintains node and edge vector indexes.
    Vector,
    /// Maintains node and edge text indexes.
    Text,
    /// Maintains every foreground index family together.
    All,
}

/// Lifecycle state whose foreground mutation behavior is measured.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DeletionBenchmarkLifecycle {
    /// Published generations receive eager physical maintenance.
    Active,
    /// Hidden generations receive coalesced Building deltas.
    Building,
}

/// Entity kind selected by a canonical deletion workload.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DeletionBenchmarkEntityKind {
    /// A node-drop API, including incident-edge cascades.
    Node,
    /// A direct edge deletion API.
    Edge,
}

/// Public deletion API shape selected by the workload.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DeletionBenchmarkApi {
    /// Node drop with incident-edge closure.
    NodeDrop,
    /// Edge deletion by exact edge IDs.
    EdgeId,
    /// Unlabeled source/target pair deletion.
    EdgePair,
    /// Labeled source/target pair deletion.
    LabeledEdgePair,
}

/// Explicit cache state applied before measurement.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DeletionBenchmarkCachePolicy {
    /// Performs representative reads before resetting counters.
    Warm,
    /// Closes and reopens the fixture without representative reads.
    Cold,
}

/// One validated deletion benchmark fixture selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct DeletionBenchmarkCase {
    /// The graph shape and deletion API under measurement.
    workload: DeletionBenchmarkWorkload,
    /// The supported or manual stress batch size.
    batch_size: DeletionBatchSize,
    /// The explicitly prepared cache state.
    cache_policy: DeletionBenchmarkCachePolicy,
    /// Configured foreground index families.
    indexes: DeletionBenchmarkIndexes,
    /// Active or Building generation behavior.
    lifecycle: DeletionBenchmarkLifecycle,
    /// Entity kind implied by the selected workload.
    entity_kind: DeletionBenchmarkEntityKind,
    /// Public deletion API implied by the selected workload.
    api: DeletionBenchmarkApi,
}

impl DeletionBenchmarkCase {
    /// Binds one graph shape, supported size, and cache policy.
    pub fn try_supported(
        workload: DeletionBenchmarkWorkload,
        batch_size: usize,
        cache_policy: DeletionBenchmarkCachePolicy,
    ) -> Result<Self, HelixDbError> {
        Self::try_indexed(
            workload,
            batch_size,
            cache_policy,
            DeletionBenchmarkIndexes::None,
            DeletionBenchmarkLifecycle::Active,
        )
    }

    /// Binds a canonical workload to exact family and lifecycle behavior.
    ///
    /// ```
    /// # #[cfg(feature = "production-coverage")] {
    /// use db::production_coverage::{
    ///     DeletionBenchmarkCachePolicy, DeletionBenchmarkCase, DeletionBenchmarkIndexes,
    ///     DeletionBenchmarkLifecycle, DeletionBenchmarkWorkload,
    /// };
    ///
    /// let case = DeletionBenchmarkCase::try_indexed(
    ///     DeletionBenchmarkWorkload::IsolatedNodes,
    ///     10_000,
    ///     DeletionBenchmarkCachePolicy::Warm,
    ///     DeletionBenchmarkIndexes::Secondary,
    ///     DeletionBenchmarkLifecycle::Active,
    /// )
    /// .expect("indexed 10k case validates");
    /// assert_eq!(case.entity_count(), 10_000);
    /// # }
    /// ```
    pub fn try_indexed(
        workload: DeletionBenchmarkWorkload,
        batch_size: usize,
        cache_policy: DeletionBenchmarkCachePolicy,
        indexes: DeletionBenchmarkIndexes,
        lifecycle: DeletionBenchmarkLifecycle,
    ) -> Result<Self, HelixDbError> {
        let (entity_kind, api) = workload_contract(workload);
        Ok(Self {
            workload,
            batch_size: DeletionBatchSize::try_supported(batch_size)?,
            cache_policy,
            indexes,
            lifecycle,
            entity_kind,
            api,
        })
    }

    /// Binds the manual 100k stress boundary to one graph shape.
    pub const fn stress_100k(
        workload: DeletionBenchmarkWorkload,
        cache_policy: DeletionBenchmarkCachePolicy,
    ) -> Self {
        let (entity_kind, api) = workload_contract(workload);
        Self {
            workload,
            batch_size: DeletionBatchSize::stress_100k(),
            cache_policy,
            indexes: DeletionBenchmarkIndexes::None,
            lifecycle: DeletionBenchmarkLifecycle::Active,
            entity_kind,
            api,
        }
    }

    /// Returns the concrete entity count used for throughput calculations.
    pub const fn entity_count(self) -> usize {
        self.batch_size.get()
    }
}

#[derive(serde::Deserialize)]
struct DeletionBenchmarkCaseWire {
    workload: DeletionBenchmarkWorkload,
    batch_size: DeletionBatchSize,
    cache_policy: DeletionBenchmarkCachePolicy,
    indexes: DeletionBenchmarkIndexes,
    lifecycle: DeletionBenchmarkLifecycle,
    entity_kind: DeletionBenchmarkEntityKind,
    api: DeletionBenchmarkApi,
}

impl<'de> serde::Deserialize<'de> for DeletionBenchmarkCase {
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let wire = <DeletionBenchmarkCaseWire as serde::Deserialize>::deserialize(deserializer)?;
        let (entity_kind, api) = workload_contract(wire.workload);
        if (wire.entity_kind, wire.api) != (entity_kind, api) {
            return Err(serde::de::Error::custom(
                "deletion benchmark workload disagrees with its entity/API contract",
            ));
        }
        Ok(Self {
            workload: wire.workload,
            batch_size: wire.batch_size,
            cache_policy: wire.cache_policy,
            indexes: wire.indexes,
            lifecycle: wire.lifecycle,
            entity_kind,
            api,
        })
    }
}

const fn workload_contract(
    workload: DeletionBenchmarkWorkload,
) -> (DeletionBenchmarkEntityKind, DeletionBenchmarkApi) {
    match workload {
        DeletionBenchmarkWorkload::IsolatedNodes
        | DeletionBenchmarkWorkload::ChainNodes
        | DeletionBenchmarkWorkload::HighDegreeNode => (
            DeletionBenchmarkEntityKind::Node,
            DeletionBenchmarkApi::NodeDrop,
        ),
        DeletionBenchmarkWorkload::ParallelEdgesById => (
            DeletionBenchmarkEntityKind::Edge,
            DeletionBenchmarkApi::EdgeId,
        ),
        DeletionBenchmarkWorkload::EdgePairs => (
            DeletionBenchmarkEntityKind::Edge,
            DeletionBenchmarkApi::EdgePair,
        ),
        DeletionBenchmarkWorkload::LabeledEdgePairs => (
            DeletionBenchmarkEntityKind::Edge,
            DeletionBenchmarkApi::LabeledEdgePair,
        ),
    }
}

/// Exact object-store calls observed after fixture counters are reset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PhysicalObjectStoreOperations {
    /// Complete-object put requests.
    pub puts: u64,
    /// Bytes supplied to complete-object put requests.
    pub put_bytes: u64,
    /// Multipart-upload initializations.
    pub multipart_starts: u64,
    /// Object get requests.
    pub gets: u64,
    /// Bytes advertised by returned object metadata.
    pub get_bytes: u64,
    /// Bulk-delete streams.
    pub delete_streams: u64,
    /// Recursive list requests.
    pub lists: u64,
    /// Delimiter-aware list requests.
    pub delimiter_lists: u64,
    /// Server-side copy requests.
    pub copies: u64,
}

/// One setup-free foreground deletion measurement.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeletionBenchmarkSample {
    /// The validated fixture represented by this sample.
    pub case: DeletionBenchmarkCase,
    /// End-to-end request latency excluding fixture and correctness work.
    pub total_ns: u64,
    /// Interpreter work before commit-boundary preparation begins.
    pub staging_ns: u64,
    /// Internal phase durations and instrumented logical operations.
    pub telemetry: MutationBenchmarkTelemetry,
    /// Existing vector-specific mutation observations.
    pub vector_telemetry: crate::search::vector::VectorMutationBenchmarkTelemetry,
    /// Exact calls observed at the physical object-store boundary.
    pub physical_object_store_operations: PhysicalObjectStoreOperations,
    /// Expected surviving node count used by untimed verification.
    pub expected_remaining_nodes: u64,
    /// Expected surviving edge count used by untimed verification.
    pub expected_remaining_edges: u64,
    /// Process-global allocation calls during serialized measurement.
    pub allocation_calls: u64,
    /// Process-global allocated bytes during serialized measurement.
    pub allocated_bytes: u64,
    /// Resident bytes immediately before the measured request.
    pub baseline_rss_bytes: u64,
    /// Peak resident bytes sampled during the measured request.
    pub peak_rss_bytes: u64,
    /// Process CPU nanoseconds consumed during the measured request.
    pub process_cpu_ns: u64,
}

impl DeletionBenchmarkSample {
    /// Attaches process-global measurements collected by the serialized bench.
    pub const fn with_process_measurements(
        mut self,
        allocation_calls: u64,
        allocated_bytes: u64,
        baseline_rss_bytes: u64,
        peak_rss_bytes: u64,
        process_cpu_ns: u64,
    ) -> Self {
        self.allocation_calls = allocation_calls;
        self.allocated_bytes = allocated_bytes;
        self.baseline_rss_bytes = baseline_rss_bytes;
        self.peak_rss_bytes = peak_rss_bytes;
        self.process_cpu_ns = process_cpu_ns;
        self
    }
}

/// Prepared graph and executable deletion plan for one measured sample.
pub struct DeletionBenchmarkFixture {
    case: DeletionBenchmarkCase,
    db: Arc<HelixDB>,
    deletion: ExecutablePlan,
    store: Arc<CountingObjectStore>,
    expected_remaining_nodes: u64,
    expected_remaining_edges: u64,
}

impl DeletionBenchmarkFixture {
    /// Builds, optionally reopens, and warms one fixture outside timed phases.
    pub async fn prepare(case: DeletionBenchmarkCase) -> Result<Self, HelixDbError> {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(CountingObjectStore::new(inner));
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let database = format!(
            "deletion-batch-bench-{:?}-{}",
            case.workload,
            case.batch_size.get()
        );
        let mut db = Arc::new(open_fixture_db(&database, Arc::clone(&object_store), case).await?);
        db.wait_for_startup_cache_warm().await;
        if matches!(case.lifecycle, DeletionBenchmarkLifecycle::Building) {
            install_building_indexes(&db, case).await?;
        }
        let prepared = prepare_graph(&db, case).await?;
        if matches!(case.lifecycle, DeletionBenchmarkLifecycle::Active) {
            install_active_indexes(&db, case).await?;
        }

        match case.cache_policy {
            DeletionBenchmarkCachePolicy::Warm => {
                verify_counts(
                    &db,
                    prepared.initial_nodes,
                    prepared.initial_edges,
                    "warming deletion fixture",
                )
                .await?;
            }
            DeletionBenchmarkCachePolicy::Cold => {
                db.close().await?;
                db = Arc::new(open_fixture_db(&database, Arc::clone(&object_store), case).await?);
                db.wait_for_startup_cache_warm().await;
            }
        }
        store.reset();

        Ok(Self {
            case,
            db,
            deletion: prepared.deletion,
            store,
            expected_remaining_nodes: prepared.remaining_nodes,
            expected_remaining_edges: prepared.remaining_edges,
        })
    }

    /// Executes only the planned foreground deletion and records internal phases.
    pub async fn run_sample(&self) -> Result<DeletionBenchmarkSample, HelixDbError> {
        crate::search::vector::reset_benchmark_telemetry();
        self.store.reset();
        let total_started = Instant::now();
        let (result, telemetry) =
            benchmark_telemetry::observe(self.db.execute(&self.deletion, ParamBindings::default()))
                .await;
        result?;
        let total_ns = u64::try_from(total_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let committed_ns = telemetry
            .phases
            .preparation_ns
            .saturating_add(telemetry.phases.commit_ns)
            .saturating_add(telemetry.phases.post_commit_ns);
        Ok(DeletionBenchmarkSample {
            case: self.case,
            total_ns,
            staging_ns: total_ns.saturating_sub(committed_ns),
            telemetry,
            vector_telemetry: crate::search::vector::benchmark_telemetry_snapshot(),
            physical_object_store_operations: self.store.snapshot(),
            expected_remaining_nodes: self.expected_remaining_nodes,
            expected_remaining_edges: self.expected_remaining_edges,
            allocation_calls: 0,
            allocated_bytes: 0,
            baseline_rss_bytes: 0,
            peak_rss_bytes: 0,
            process_cpu_ns: 0,
        })
    }

    /// Verifies final graph visibility after measurement and closes the fixture.
    pub async fn verify_and_close(self) -> Result<(), HelixDbError> {
        verify_counts(
            &self.db,
            self.expected_remaining_nodes,
            self.expected_remaining_edges,
            "verifying deletion fixture",
        )
        .await?;
        self.db.close().await
    }
}

struct PreparedGraph {
    deletion: ExecutablePlan,
    initial_nodes: u64,
    initial_edges: u64,
    remaining_nodes: u64,
    remaining_edges: u64,
}

async fn prepare_graph(
    db: &HelixDB,
    case: DeletionBenchmarkCase,
) -> Result<PreparedGraph, HelixDbError> {
    let count = case.batch_size.get();
    let (node_count, edges, deletion_write, remaining_nodes) = match case.workload {
        DeletionBenchmarkWorkload::IsolatedNodes => (
            count,
            Vec::new(),
            write_batch().var_as("deleted", g().n(NodeRef::all()).drop()),
            0,
        ),
        DeletionBenchmarkWorkload::ChainNodes => {
            let edges = (1..count)
                .map(|to| (to as u64 - 1, to as u64))
                .collect::<Vec<_>>();
            (
                count,
                edges,
                write_batch().var_as("deleted", g().n(NodeRef::all()).drop()),
                0,
            )
        }
        DeletionBenchmarkWorkload::HighDegreeNode => {
            let edges = (1..=count).map(|to| (0, to as u64)).collect::<Vec<_>>();
            (
                count + 1,
                edges,
                write_batch().var_as("deleted", g().n(NodeRef::id(0)).drop()),
                count,
            )
        }
        DeletionBenchmarkWorkload::ParallelEdgesById => {
            let edges = (0..count).map(|_| (0, 1)).collect::<Vec<_>>();
            (
                2,
                edges,
                write_batch().var_as(
                    "deleted",
                    g().drop_edge_by_id(EdgeRef::ids(0..count as u64)),
                ),
                2,
            )
        }
        DeletionBenchmarkWorkload::EdgePairs => {
            let edges = (0..count)
                .map(|source| (source as u64, count as u64))
                .collect::<Vec<_>>();
            (
                count + 1,
                edges,
                write_batch().var_as(
                    "deleted",
                    g().n(NodeRef::ids(0..count as u64))
                        .drop_edge(NodeRef::id(count as u64)),
                ),
                count + 1,
            )
        }
        DeletionBenchmarkWorkload::LabeledEdgePairs => {
            let edges = (0..count)
                .map(|source| (source as u64, count as u64))
                .collect::<Vec<_>>();
            (
                count + 1,
                edges,
                write_batch().var_as(
                    "deleted",
                    g().n(NodeRef::ids(0..count as u64))
                        .drop_edge_labeled(NodeRef::id(count as u64), "DeletionBenchmarkEdge"),
                ),
                count + 1,
            )
        }
    };

    let mut create_nodes = write_batch();
    for node_id in 0..node_count {
        let variable = format!("node_{node_id}");
        create_nodes = create_nodes.var_as(
            &variable,
            g().add_n("DeletionBenchmarkNode", benchmark_properties(node_id)),
        );
    }
    let create_nodes = plan_write(db, &create_nodes, "creating benchmark nodes")?;
    db.execute(&create_nodes, ParamBindings::default()).await?;

    if !edges.is_empty() {
        let mut create_edges = write_batch();
        for (edge_index, (from, to)) in edges.iter().copied().enumerate() {
            let variable = format!("edge_{edge_index}");
            create_edges = create_edges.var_as(
                &variable,
                g().n(NodeRef::id(from)).add_e(
                    "DeletionBenchmarkEdge",
                    NodeRef::id(to),
                    benchmark_properties(edge_index),
                ),
            );
        }
        let create_edges = plan_write(db, &create_edges, "creating benchmark edges")?;
        db.execute(&create_edges, ParamBindings::default()).await?;
    }

    let deletion = plan_write(db, &deletion_write, "planning benchmark deletion")?;
    Ok(PreparedGraph {
        deletion,
        initial_nodes: u64::try_from(node_count).unwrap_or(u64::MAX),
        initial_edges: u64::try_from(edges.len()).unwrap_or(u64::MAX),
        remaining_nodes: u64::try_from(remaining_nodes).unwrap_or(u64::MAX),
        remaining_edges: 0,
    })
}

fn benchmark_properties(ordinal: usize) -> Vec<(&'static str, PropertyInput)> {
    vec![
        ("ordinal", PropertyInput::from(ordinal as i64)),
        ("body", PropertyInput::from("atomic deletion benchmark")),
        (
            "embedding",
            PropertyInput::from(vec![ordinal as f32 + 1.0, 1.0_f32]),
        ),
    ]
}

async fn open_fixture_db(
    database: &str,
    object_store: Arc<dyn ObjectStore>,
    case: DeletionBenchmarkCase,
) -> Result<HelixDB, HelixDbError> {
    match case.lifecycle {
        DeletionBenchmarkLifecycle::Active => {
            HelixDB::open_with_object_store(database.to_string(), object_store).await
        }
        DeletionBenchmarkLifecycle::Building => {
            #[cfg(feature = "index-lifecycle-testing")]
            {
                HelixDB::open_with_object_store_for_index_lifecycle_testing(
                    database.to_string(),
                    object_store,
                    crate::config::DbConfig::new(),
                    crate::index_lifecycle_testing::LifecycleTestScheduling::Explicit,
                )
                .await
            }
            #[cfg(not(feature = "index-lifecycle-testing"))]
            {
                let _ = (database, object_store);
                Err(HelixDbError::Config(
                    "Building deletion benchmarks require index-lifecycle-testing".to_string(),
                ))
            }
        }
    }
}

async fn install_active_indexes(
    db: &HelixDB,
    case: DeletionBenchmarkCase,
) -> Result<(), HelixDbError> {
    for definition in benchmark_index_definitions(case.indexes)? {
        db.install_index_for_tests(definition).await?;
    }
    Ok(())
}

async fn install_building_indexes(
    db: &HelixDB,
    case: DeletionBenchmarkCase,
) -> Result<(), HelixDbError> {
    #[cfg(feature = "index-lifecycle-testing")]
    {
        let controller = crate::index_lifecycle_testing::LifecycleTestController::new();
        for definition in benchmark_index_definitions(case.indexes)? {
            controller
                .create_index(
                    db,
                    crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    definition,
                    helix_planner::ir::IndexCreateMode::IfNotExists,
                )
                .await?;
        }
        db.refresh_runtime_catalog(crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped)
            .await
    }
    #[cfg(not(feature = "index-lifecycle-testing"))]
    {
        let _ = (db, case);
        Err(HelixDbError::Config(
            "Building deletion benchmarks require index-lifecycle-testing".to_string(),
        ))
    }
}

fn benchmark_index_definitions(
    indexes: DeletionBenchmarkIndexes,
) -> Result<Vec<crate::index_lifecycle::ValidatedDynamicIndexDefinition>, HelixDbError> {
    let includes_secondary = matches!(
        indexes,
        DeletionBenchmarkIndexes::Secondary | DeletionBenchmarkIndexes::All
    );
    let includes_vector = matches!(
        indexes,
        DeletionBenchmarkIndexes::Vector | DeletionBenchmarkIndexes::All
    );
    let includes_text = matches!(
        indexes,
        DeletionBenchmarkIndexes::Text | DeletionBenchmarkIndexes::All
    );
    let mut definitions = Vec::new();
    if includes_secondary {
        definitions.push(
            config::SecondaryIndexDefinition::node_equality("DeletionBenchmarkNode", "ordinal")?
                .try_into()?,
        );
        definitions.push(
            config::SecondaryIndexDefinition::edge_equality("DeletionBenchmarkEdge", "ordinal")?
                .try_into()?,
        );
    }
    if includes_vector {
        definitions.push(
            config::VectorIndexDefinition::new_node(
                "DeletionBenchmarkNode",
                "embedding",
                2,
                search::vector::VectorDistanceMetric::Euclidean,
            )?
            .try_into()?,
        );
        definitions.push(
            config::VectorIndexDefinition::new_edge(
                "DeletionBenchmarkEdge",
                "embedding",
                2,
                search::vector::VectorDistanceMetric::Euclidean,
            )?
            .try_into()?,
        );
    }
    if includes_text {
        definitions.push(
            config::TextIndexDefinition::new_node("DeletionBenchmarkNode", "body")?.try_into()?,
        );
        definitions.push(
            config::TextIndexDefinition::new_edge("DeletionBenchmarkEdge", "body")?.try_into()?,
        );
    }
    Ok(definitions)
}

fn plan_write(
    db: &HelixDB,
    batch: &helix_ast::batch::WriteBatch,
    context: &str,
) -> Result<ExecutablePlan, HelixDbError> {
    planning::plan_write_batch(batch, &db.planner_context(ParamBindings::default()))
        .map_err(|error| HelixDbError::Query(format!("{context}: {error}")))
}

async fn verify_counts(
    db: &HelixDB,
    expected_nodes: u64,
    expected_edges: u64,
    context: &str,
) -> Result<(), HelixDbError> {
    let query = read_batch()
        .var_as("nodes", g().n(NodeRef::all()).count())
        .var_as("edges", g().e(EdgeRef::all()).count())
        .returning(["nodes", "edges"]);
    let result = db.query(QueryRequest::read(query)).await?;
    let nodes = result["nodes"].as_u64().ok_or_else(|| {
        HelixDbError::InvariantViolation(format!("{context}: node count is not an integer"))
    })?;
    let edges = result["edges"].as_u64().ok_or_else(|| {
        HelixDbError::InvariantViolation(format!("{context}: edge count is not an integer"))
    })?;
    if (nodes, edges) != (expected_nodes, expected_edges) {
        return Err(HelixDbError::InvariantViolation(format!(
            "{context}: expected ({expected_nodes}, {expected_edges}) nodes/edges, observed ({nodes}, {edges})"
        )));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ObjectStoreCounters {
    puts: AtomicU64,
    put_bytes: AtomicU64,
    multipart_starts: AtomicU64,
    gets: AtomicU64,
    get_bytes: AtomicU64,
    delete_streams: AtomicU64,
    lists: AtomicU64,
    delimiter_lists: AtomicU64,
    copies: AtomicU64,
}

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: ObjectStoreCounters,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            counters: ObjectStoreCounters::default(),
        }
    }

    fn reset(&self) {
        for counter in [
            &self.counters.puts,
            &self.counters.put_bytes,
            &self.counters.multipart_starts,
            &self.counters.gets,
            &self.counters.get_bytes,
            &self.counters.delete_streams,
            &self.counters.lists,
            &self.counters.delimiter_lists,
            &self.counters.copies,
        ] {
            counter.store(0, Ordering::Release);
        }
    }

    fn snapshot(&self) -> PhysicalObjectStoreOperations {
        PhysicalObjectStoreOperations {
            puts: self.counters.puts.load(Ordering::Acquire),
            put_bytes: self.counters.put_bytes.load(Ordering::Acquire),
            multipart_starts: self.counters.multipart_starts.load(Ordering::Acquire),
            gets: self.counters.gets.load(Ordering::Acquire),
            get_bytes: self.counters.get_bytes.load(Ordering::Acquire),
            delete_streams: self.counters.delete_streams.load(Ordering::Acquire),
            lists: self.counters.lists.load(Ordering::Acquire),
            delimiter_lists: self.counters.delimiter_lists.load(Ordering::Acquire),
            copies: self.counters.copies.load(Ordering::Acquire),
        }
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("deletion-benchmark-counting-store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.counters.puts.fetch_add(1, Ordering::AcqRel);
        self.counters.put_bytes.fetch_add(
            u64::try_from(payload.content_length()).unwrap_or(u64::MAX),
            Ordering::AcqRel,
        );
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.counters
            .multipart_starts
            .fetch_add(1, Ordering::AcqRel);
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.counters.gets.fetch_add(1, Ordering::AcqRel);
        let result = self.inner.get_opts(location, options).await?;
        self.counters
            .get_bytes
            .fetch_add(result.meta.size, Ordering::AcqRel);
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.counters.delete_streams.fetch_add(1, Ordering::AcqRel);
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.counters.lists.fetch_add(1, Ordering::AcqRel);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.counters.delimiter_lists.fetch_add(1, Ordering::AcqRel);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.counters.copies.fetch_add(1, Ordering::AcqRel);
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_sizes_reject_zero_and_values_above_ten_thousand() {
        assert!(DeletionBatchSize::try_supported(0).is_err());
        assert!(DeletionBatchSize::try_supported(10_000).is_ok());
        assert!(DeletionBatchSize::try_supported(10_001).is_err());
        assert_eq!(DeletionBatchSize::stress_100k().get(), 100_000);
    }

    #[test]
    fn serialized_case_rejects_an_inconsistent_entity_api_contract() {
        let case = DeletionBenchmarkCase::try_indexed(
            DeletionBenchmarkWorkload::IsolatedNodes,
            10,
            DeletionBenchmarkCachePolicy::Warm,
            DeletionBenchmarkIndexes::Secondary,
            DeletionBenchmarkLifecycle::Active,
        )
        .unwrap();
        let encoded = serde_json::to_value(case).unwrap();
        assert_eq!(
            serde_json::from_value::<DeletionBenchmarkCase>(encoded.clone()).unwrap(),
            case
        );
        let mut invalid = encoded;
        invalid["entity_kind"] = serde_json::json!("edge");
        assert!(serde_json::from_value::<DeletionBenchmarkCase>(invalid).is_err());
    }

    #[tokio::test]
    async fn every_graph_workload_deletes_the_expected_rows() {
        for workload in [
            DeletionBenchmarkWorkload::IsolatedNodes,
            DeletionBenchmarkWorkload::ChainNodes,
            DeletionBenchmarkWorkload::HighDegreeNode,
            DeletionBenchmarkWorkload::ParallelEdgesById,
            DeletionBenchmarkWorkload::EdgePairs,
            DeletionBenchmarkWorkload::LabeledEdgePairs,
        ] {
            let case = DeletionBenchmarkCase::try_supported(
                workload,
                3,
                DeletionBenchmarkCachePolicy::Warm,
            )
            .expect("small benchmark case validates");
            let fixture = DeletionBenchmarkFixture::prepare(case)
                .await
                .expect("benchmark fixture prepares");
            let sample = fixture
                .run_sample()
                .await
                .expect("benchmark deletion succeeds");
            assert!(sample.total_ns > 0);
            fixture
                .verify_and_close()
                .await
                .expect("benchmark fixture verifies");
        }
    }

    #[tokio::test]
    async fn secondary_fixture_covers_active_and_building_deletion_paths() {
        for lifecycle in [
            DeletionBenchmarkLifecycle::Active,
            DeletionBenchmarkLifecycle::Building,
        ] {
            let case = DeletionBenchmarkCase::try_indexed(
                DeletionBenchmarkWorkload::IsolatedNodes,
                3,
                DeletionBenchmarkCachePolicy::Warm,
                DeletionBenchmarkIndexes::Secondary,
                lifecycle,
            )
            .unwrap();
            let fixture = DeletionBenchmarkFixture::prepare(case).await.unwrap();
            let sample = fixture.run_sample().await.unwrap();
            assert_eq!(
                sample
                    .telemetry
                    .instrumented_logical_operations
                    .secondary_deletions,
                3
            );
            fixture.verify_and_close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn node_cascade_stages_one_non_empty_topology_epoch() {
        let fixture = DeletionBenchmarkFixture::prepare(
            DeletionBenchmarkCase::try_supported(
                DeletionBenchmarkWorkload::ChainNodes,
                10,
                DeletionBenchmarkCachePolicy::Warm,
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let sample = fixture.run_sample().await.unwrap();
        let operations = sample.telemetry.instrumented_logical_operations;

        assert_eq!(operations.topology_flushes, 1);
        assert_eq!(operations.cascade_nodes, 10);
        assert_eq!(operations.cascade_edges, 9);
        assert_eq!(operations.cascade_pairs, 9);
        fixture.verify_and_close().await.unwrap();
    }
}
