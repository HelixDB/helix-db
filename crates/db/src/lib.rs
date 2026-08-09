#![recursion_limit = "256"]

//! Helix database runtime, storage, query execution, and index lifecycle API.

pub mod config;
pub mod encoding;
pub mod error;
pub mod execution;
pub mod execution_control;
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
pub mod id_allocator;
pub mod index_lifecycle;
#[cfg(feature = "index-lifecycle-testing")]
pub mod index_lifecycle_testing;
mod merge_operator;
#[cfg(feature = "migration-parity")]
pub mod migration_parity;
pub mod migrations;
pub mod query_service;
mod runtime_dependencies;
pub mod search;

pub use runtime_dependencies::{IndexRuntimeReadiness, ProcessLocalDatabaseToken};

#[cfg(feature = "production-coverage")]
#[path = "../tests/production_support/mod.rs"]
pub mod production_coverage;

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

pub use config::{DbConfig, HelixConfig};

#[cfg(any(test, feature = "production-coverage"))]
use config::ValidatedDynamicIndexDefinition;
use config::{db::EmbeddedStorageProfile, runtime_catalog, CacheMode, RuntimeIndexCatalog};
use error::{HelixDbError, Result};
use execution::interpreter::{ExecutionResult, Interpreter};
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
};
use futures::{stream, StreamExt};
use helix_ast::query::QueryRequest;
use helix_metrics::query::transport::OssQueryMetrics;
use helix_metrics::telemetry;
use helix_planner::{
    catalog::IndexCatalogSnapshot,
    context::{ParamBindings, PlannerContext},
    exec, ir,
};
use id_allocator::{EdgeIdAllocator, NodeIdAllocator};
use serde_json::Value as JsonValue;
use slatedb::db_cache::{
    foyer::{FoyerCache, FoyerCacheOptions},
    foyer_hybrid::{FoyerHybridCache, FoyerHybridCacheMetrics},
    CacheUsageSnapshot, CachedEntry, DbCache, SplitCache,
};
#[cfg(test)]
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};
use slatedb::{CacheTarget, Db, DbCacheManagerOps, DbMetadataOps, DbReader};
use tokio::sync::{oneshot, watch, Mutex};
use tokio::task::JoinHandle;

use crate::encoding::keys::tenant::DataScope;

/// Open mode for a [`HelixDB`] handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelixDbMode {
    /// Read-only handle.
    ReadOnly,
    /// Read/write handle.
    Writer,
}

/// Meaning of the byte accounting exposed for one cache tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheUsageSemantics {
    WeightedResident,
    EstimatedResident,
    PhysicalFiles,
    AllocatedBlocks,
}

/// Typed lifecycle and byte accounting for one cache tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheTierState {
    Disabled,
    Initializing {
        capacity_bytes: Option<u64>,
    },
    Ready {
        used_bytes: u64,
        capacity_bytes: Option<u64>,
    },
    Unavailable,
}

/// One point-in-time cache tier measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheTierSnapshot {
    pub semantics: CacheUsageSemantics,
    pub state: CacheTierState,
}

impl CacheTierSnapshot {
    const fn disabled(semantics: CacheUsageSemantics) -> Self {
        Self {
            semantics,
            state: CacheTierState::Disabled,
        }
    }

    const fn ready(
        semantics: CacheUsageSemantics,
        used_bytes: u64,
        capacity_bytes: Option<u64>,
    ) -> Self {
        Self {
            semantics,
            state: CacheTierState::Ready {
                used_bytes,
                capacity_bytes,
            },
        }
    }

    const fn from_slate(semantics: CacheUsageSemantics, snapshot: CacheUsageSnapshot) -> Self {
        let state = match snapshot {
            CacheUsageSnapshot::Disabled => CacheTierState::Disabled,
            CacheUsageSnapshot::Initializing { capacity_bytes } => {
                CacheTierState::Initializing { capacity_bytes }
            }
            CacheUsageSnapshot::Ready {
                used_bytes,
                capacity_bytes,
            } => CacheTierState::Ready {
                used_bytes,
                capacity_bytes,
            },
            CacheUsageSnapshot::Unavailable => CacheTierState::Unavailable,
        };
        Self { semantics, state }
    }

    pub const fn is_publishable(self) -> bool {
        !matches!(self.state, CacheTierState::Unavailable)
    }
}

/// Cheap aggregate cache snapshot for one live database handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseCacheStats {
    pub slate_memory: CacheTierSnapshot,
    pub slate_object_store_disk: CacheTierSnapshot,
    pub foyer_hybrid_disk: CacheTierSnapshot,
    pub fts_memory: CacheTierSnapshot,
    pub fts_disk: CacheTierSnapshot,
    pub vector_memory: CacheTierSnapshot,
}

impl DatabaseCacheStats {
    pub const fn is_publishable(self) -> bool {
        self.slate_memory.is_publishable()
            && self.slate_object_store_disk.is_publishable()
            && self.foyer_hybrid_disk.is_publishable()
            && self.fts_memory.is_publishable()
            && self.fts_disk.is_publishable()
            && self.vector_memory.is_publishable()
    }
}

/// Monotonic storage sequence visible to one writer or reader handle.
///
/// Sequence lag is measured in committed storage progress rather than elapsed
/// wall-clock time.
///
/// # Examples
///
/// ```
/// use db::DatabaseSequence;
///
/// let reader = DatabaseSequence::new(17);
/// let writer = DatabaseSequence::new(23);
/// assert_eq!(reader.lag_to(writer), 6);
/// assert_eq!(writer.lag_to(reader), 0);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatabaseSequence(u64);

impl DatabaseSequence {
    /// Returns the empty-database sequence.
    pub const fn initial() -> Self {
        Self(0)
    }

    /// Wraps a storage sequence captured by an external adapter or trace.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying monotonic storage sequence.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns commit lag from this visible sequence to a newer writer sequence.
    pub const fn lag_to(self, writer: Self) -> u64 {
        writer.0.saturating_sub(self.0)
    }
}

impl HelixDbMode {
    /// Stable lowercase name for diagnostics and errors.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "reader",
            Self::Writer => "writer",
        }
    }
}

/// Storage source used to open a [`HelixDB`] handle.
#[derive(Clone)]
pub enum HelixDbSource {
    /// In-memory object store scoped by logical database path.
    InMemory {
        /// Logical database path inside the in-memory store.
        database: String,
    },
    /// Reusable in-memory identity shared by every handle opened from the token.
    InMemoryToken {
        /// Reusable in-memory object-store identity.
        token: ProcessLocalDatabaseToken,
    },
    /// Local filesystem object store rooted at `root`.
    Disk {
        /// Filesystem directory used as the object-store root.
        root: PathBuf,
        /// Logical database path inside the local object store.
        database: String,
    },
    /// S3-compatible object storage.
    ObjectStorage {
        /// Logical database path inside the object store.
        database: String,
        /// Bucket name.
        bucket: String,
        /// Region.
        region: String,
        /// Optional endpoint for S3-compatible local storage.
        endpoint: Option<String>,
        /// Whether HTTP endpoints are allowed.
        allow_http: bool,
    },
}

impl HelixDbSource {
    /// Build the bounded defaults used by an embedded handle for this source.
    ///
    /// Local storage uses a shorter durability cadence and smaller cache/write-buffer
    /// bounds than remote object storage. Object storage retains the general runtime
    /// defaults because its latency and request-cost tradeoffs are different.
    pub fn embedded_default_config(&self) -> DbConfig {
        match self {
            Self::InMemory { .. } | Self::InMemoryToken { .. } => {
                DbConfig::embedded_default(EmbeddedStorageProfile::InMemory)
            }
            Self::Disk { .. } => DbConfig::embedded_default(EmbeddedStorageProfile::Disk),
            Self::ObjectStorage { .. } => DbConfig::new(),
        }
    }

    fn into_parts(self) -> Result<(String, Arc<dyn ObjectStore>)> {
        match self {
            Self::InMemory { database } => {
                let token = ProcessLocalDatabaseToken::new(database)?;
                Ok((token.database().to_string(), token.object_store()))
            }
            Self::InMemoryToken { token } => {
                Ok((token.database().to_string(), token.object_store()))
            }
            Self::Disk { root, database } => {
                let object_store: Arc<dyn ObjectStore> =
                    Arc::new(LocalFileSystem::new_with_prefix(root)?);
                Ok((database, object_store))
            }
            Self::ObjectStorage {
                database,
                bucket,
                region,
                endpoint,
                allow_http,
            } => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_allow_http(allow_http);
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                Ok((database, Arc::new(builder.build()?)))
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum HelixStorage {
    Reader(Arc<DbReader>),
    Writer(Arc<HelixWriter>),
}

struct HelixStorageParts {
    path: String,
    object_store: Arc<dyn ObjectStore>,
    handle: HelixStorage,
}

impl HelixStorageParts {
    fn new(path: String, object_store: Arc<dyn ObjectStore>, handle: HelixStorage) -> Self {
        Self {
            path,
            object_store,
            handle,
        }
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    fn handle(&self) -> &HelixStorage {
        &self.handle
    }

    fn cache_usage_snapshot(&self) -> slatedb::db_cache::SlateDbCacheUsageSnapshot {
        match &self.handle {
            HelixStorage::Reader(reader) => reader.cache_usage_snapshot(),
            HelixStorage::Writer(writer) => writer.db.cache_usage_snapshot(),
        }
    }
}

/// Writer-only state attached to a database handle.
pub(crate) struct HelixWriter {
    db: Arc<Db>,
    node_ids: Arc<NodeIdAllocator>,
    edge_ids: Arc<EdgeIdAllocator>,
}

impl HelixWriter {
    fn new(db: Arc<Db>, lease_size: u64) -> Self {
        Self {
            db: Arc::clone(&db),
            node_ids: Arc::new(NodeIdAllocator::new(Arc::clone(&db), lease_size)),
            edge_ids: Arc::new(EdgeIdAllocator::new(db, lease_size)),
        }
    }

    pub(crate) fn db(&self) -> &Db {
        self.db.as_ref()
    }

    pub(crate) fn node_ids(&self) -> &NodeIdAllocator {
        self.node_ids.as_ref()
    }

    pub(crate) fn edge_ids(&self) -> &EdgeIdAllocator {
        self.edge_ids.as_ref()
    }
}

impl std::ops::Deref for HelixWriter {
    type Target = Db;

    fn deref(&self) -> &Self::Target {
        self.db()
    }
}

struct VectorMemoryRefreshTask {
    shutdown: watch::Sender<bool>,
    initial_refresh: watch::Receiver<bool>,
    handle: JoinHandle<()>,
}

impl VectorMemoryRefreshTask {
    async fn stop(self) {
        let _ = self.shutdown.send(true);
        match self.handle.await {
            Ok(()) => {}
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                tracing::warn!(error = %err, "vector memory refresh task failed during shutdown");
            }
        }
    }
}

struct VectorMemoryCache {
    registry: Arc<search::vector::VectorCacheRegistry>,
    simhasher_registry: Arc<search::vector::SimHasherRegistry>,
    refresh_task: Mutex<Option<VectorMemoryRefreshTask>>,
}

impl VectorMemoryCache {
    /// Builds all vector runtime caches from one validated, non-persisted policy.
    fn new(settings: config::VectorMemorySettings) -> Self {
        Self {
            registry: Arc::new(search::vector::VectorCacheRegistry::default()),
            simhasher_registry: Arc::new(search::vector::SimHasherRegistry::new(
                search::vector::SimHasherRegistryLimits::from_config(settings.simhasher_cache()),
            )),
            refresh_task: Mutex::new(None),
        }
    }
}

struct CacheWarmTask {
    handle: JoinHandle<()>,
}

impl CacheWarmTask {
    async fn stop(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }

    async fn wait(self) {
        let _ = self.handle.await;
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlateWarmSummary {
    /// Physical SSTs selected from the current manifest.
    pub sst_count: usize,
    /// Selected SSTs whose metadata targets warmed successfully.
    pub warmed_ssts: usize,
    /// Selected SSTs whose metadata targets failed to warm.
    pub warm_errors: u64,
    /// End-to-end elapsed milliseconds for this warm pass.
    pub warm_elapsed_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlateCacheStateSnapshot {
    /// Whether SlateDB proactive metadata warming is configured.
    pub enabled: bool,
    /// Configured startup warm mode, if warming is enabled.
    pub warm_mode: Option<config::CacheWarmMode>,
    /// Most recently completed warm pass in this process.
    pub last_warm: Option<SlateWarmSummary>,
}

struct StartupCacheTasks {
    slate: Mutex<Option<CacheWarmTask>>,
    fts: Mutex<Option<CacheWarmTask>>,
}

impl StartupCacheTasks {
    fn new() -> Self {
        Self {
            slate: Mutex::new(None),
            fts: Mutex::new(None),
        }
    }
}

struct HelixCaches {
    slate_db: Option<Arc<dyn DbCache>>,
    fts: Option<Arc<search::text::FtsCache>>,
    vector_memory: VectorMemoryCache,
    slate_last_warm: Mutex<Option<SlateWarmSummary>>,
    startup_tasks: StartupCacheTasks,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CatalogGeneration(NonZeroU64);

impl CatalogGeneration {
    const INITIAL: Self = Self(NonZeroU64::MIN);

    fn next(self) -> Self {
        let next = self
            .0
            .get()
            .checked_add(1)
            .expect("runtime catalog generation must not overflow");
        Self(NonZeroU64::new(next).expect("incremented catalog generation remains non-zero"))
    }
}

struct LoadedRuntimeCatalog {
    catalog: Arc<index_lifecycle::LoadedV2ScopeCatalog>,
    generation: CatalogGeneration,
}

struct HelixRuntimeState {
    catalogs: HashMap<DataScope, LoadedRuntimeCatalog>,
}

impl HelixRuntimeState {
    fn new(scope: DataScope, catalog: index_lifecycle::LoadedV2ScopeCatalog) -> Self {
        assert_eq!(
            scope,
            catalog.scope(),
            "loaded catalog scope must match its map key"
        );
        let mut catalogs = HashMap::new();
        catalogs.insert(
            scope,
            LoadedRuntimeCatalog {
                catalog: Arc::new(catalog),
                generation: CatalogGeneration::INITIAL,
            },
        );
        Self { catalogs }
    }

    /// Replaces one scope with a fresh configured-plus-canonical projection.
    fn replace_catalog(
        &mut self,
        scope: DataScope,
        catalog: index_lifecycle::LoadedV2ScopeCatalog,
    ) {
        assert_eq!(
            scope,
            catalog.scope(),
            "loaded catalog scope must match its map key"
        );
        let generation = self
            .catalogs
            .get(&scope)
            .map_or(CatalogGeneration::INITIAL, |loaded| {
                loaded.generation.next()
            });
        self.catalogs.insert(
            scope,
            LoadedRuntimeCatalog {
                catalog: Arc::new(catalog),
                generation,
            },
        );
    }

    fn catalog(&self, scope: DataScope) -> RuntimeIndexCatalog {
        self.catalogs
            .get(&scope)
            .map(|loaded| loaded.catalog.runtime())
            .cloned()
            .expect("scoped catalog must be loaded before runtime access")
    }

    fn planner_snapshot(&self, scope: DataScope) -> IndexCatalogSnapshot {
        self.catalog(scope).planner_snapshot()
    }

    fn planner_snapshot_with_catalog(
        &self,
        scope: DataScope,
    ) -> (
        IndexCatalogSnapshot,
        Arc<index_lifecycle::LoadedV2ScopeCatalog>,
    ) {
        let loaded = self
            .catalogs
            .get(&scope)
            .expect("scoped catalog must be loaded before planner access");
        (
            loaded.catalog.runtime().planner_snapshot(),
            Arc::clone(&loaded.catalog),
        )
    }

    #[cfg(test)]
    fn generation(&self, scope: DataScope) -> CatalogGeneration {
        self.catalogs
            .get(&scope)
            .map(|loaded| loaded.generation)
            .expect("scoped catalog must be loaded before generation access")
    }

    fn active_handles(&self, scope: DataScope) -> Vec<index_lifecycle::ActiveIndexHandle> {
        let Some(catalog) = self.catalogs.get(&scope) else {
            return Vec::new();
        };
        catalog.catalog.active_handles().cloned().collect()
    }

    /// Returns every scope whose configured catalog is available to runtime work.
    fn loaded_scopes(&self) -> Vec<DataScope> {
        let mut scopes = self.catalogs.keys().copied().collect::<Vec<_>>();
        scopes.sort_unstable();
        scopes
    }
}

/// Main database entry point.
///
/// `HelixDB` owns the SlateDB handle and exposes the narrow execution boundary
/// used by the planner-backed interpreter. Open it explicitly as a writer or a
/// read-only reader; write plans cannot be represented as valid work on reader
/// handles.
///
/// Every request owns one stable read view. A successful write commits
/// atomically, and the following writer request observes it.
///
/// # Examples
///
/// ```
/// # #![recursion_limit = "256"]
/// # tokio_test::block_on(async {
/// use db::{HelixDB, HelixDbSource};
/// use helix_ast::{batch, graph::NodeRef, query::QueryRequest, traversal, value::PropertyInput};
///
/// let db = HelixDB::open(HelixDbSource::InMemory {
///     database: "helix-db-contract-example".to_string(),
/// }).await.unwrap();
/// db.query(QueryRequest::write(
///     batch::write_batch()
///         .var_as(
///             "created",
///             traversal::g().add_n(
///                 "Document",
///                 vec![("name", PropertyInput::from("example"))],
///             ),
///         )
///         .returning(["created"]),
/// )).await.unwrap();
/// let result = db.query(QueryRequest::read(
///     batch::read_batch()
///         .var_as("count", traversal::g().n(NodeRef::all()).count())
///         .returning(["count"]),
/// )).await.unwrap();
/// assert_eq!(result["count"], 1);
/// db.close().await.unwrap();
/// # });
/// ```
pub struct HelixDB {
    inner: Arc<HelixDBInner>,
}

/// Shared runtime identity used by request handles and owned background tasks.
///
/// Every clone points at the same storage handle, caches, and planner catalog.
/// Keeping these fields together prevents background cache work from observing
/// a detached in-memory view.
struct HelixDBInner {
    storage: HelixStorageParts,
    caches: HelixCaches,
    index_scope_gates: Arc<index_lifecycle::IndexScopeGates>,
    config: HelixConfig,
    runtime_state: RwLock<HelixRuntimeState>,
    index_capabilities: index_lifecycle::worker::IndexFamilyCapabilities,
    index_worker_wake: Option<index_lifecycle::worker::IndexWorkerWakeHandle>,
    index_worker: Mutex<Option<index_lifecycle::worker::IndexWorkerSupervisor>>,
    index_claim_sequences: Arc<index_lifecycle::worker::ClaimSequenceAllocator>,
    secondary_lifecycle_step: Mutex<()>,
    #[cfg(feature = "index-lifecycle-testing")]
    lifecycle_test_scheduling: IndexLifecycleScheduling,
    #[cfg(feature = "index-lifecycle-testing")]
    lifecycle_metrics: Arc<index_lifecycle_testing::AutomaticLifecycleMetrics>,
    query_metrics: RwLock<Option<OssQueryMetrics>>,
    query_metrics_runtime: Mutex<Option<telemetry::Runtime>>,
    close_state: Mutex<CloseState>,
}

/// Non-forgeable evidence that planning observed one exact runtime catalog.
///
/// The weak runtime identity prevents a proof from keeping a database alive or
/// being reused against a different handle. The scope prevents tenant mismatch.
/// The permit keeps the exact read snapshot and catalog authoritative until a
/// write opens its serializable snapshot and loads the mutation catalog.
pub(crate) struct CatalogRefreshProof {
    runtime: Weak<HelixDBInner>,
    scope: DataScope,
    catalog_permit: index_lifecycle::IndexScopeCatalogPermit,
    catalog: Arc<index_lifecycle::LoadedV2ScopeCatalog>,
    read_view: Box<execution::interpreter::read_view::StableRequestReadView>,
}

impl CatalogRefreshProof {
    fn into_read_parts(
        self,
    ) -> (
        Box<execution::interpreter::read_view::StableRequestReadView>,
        Arc<index_lifecycle::LoadedV2ScopeCatalog>,
        index_lifecycle::IndexScopeCatalogPermit,
    ) {
        (self.read_view, self.catalog, self.catalog_permit)
    }

    fn into_write_permit(self) -> index_lifecycle::IndexScopeCatalogPermit {
        (*self.read_view).close();
        self.catalog_permit
    }
}

/// Planner context coupled to the catalog observation that produced it.
pub(crate) struct PreparedPlannerContext {
    context: PlannerContext,
    proof: CatalogRefreshProof,
}

impl PreparedPlannerContext {
    pub(crate) const fn context(&self) -> &PlannerContext {
        &self.context
    }

    pub(crate) fn into_catalog_proof(self) -> CatalogRefreshProof {
        self.proof
    }
}

/// Runtime-only scheduling selection used while constructing family capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexLifecycleScheduling {
    /// Preserve the configured secondary mode and automatic vector/text scheduling.
    Configured,
    /// Install every driver while excluding every lane from background discovery.
    #[cfg(feature = "index-lifecycle-testing")]
    ExplicitOnly,
}

impl IndexLifecycleScheduling {
    const fn resolve(
        self,
        configured: index_lifecycle::worker::IndexDriverScheduling,
    ) -> index_lifecycle::worker::IndexDriverScheduling {
        match self {
            Self::Configured => configured,
            #[cfg(feature = "index-lifecycle-testing")]
            Self::ExplicitOnly => index_lifecycle::worker::IndexDriverScheduling::ExplicitOnly,
        }
    }
}

/// Serializes close without making an in-progress close look completed.
enum CloseState {
    Open,
    Closing { waiters: Vec<oneshot::Sender<()>> },
    Closed,
}

impl HelixDB {
    /// Open a read/write database handle.
    pub async fn open(source: HelixDbSource) -> Result<Self> {
        let config = source.embedded_default_config();
        Self::open_with_config(source, config).await
    }

    /// Open a read/write database handle backed by a caller-provided object store.
    pub async fn open_with_object_store(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let db = Self::open_writer_inner(database.into(), object_store, DbConfig::new()).await?;
        Ok(db.with_embedded_query_metrics().await)
    }

    /// Open a read/write database handle backed by a caller-provided object store and config.
    pub async fn open_with_object_store_and_config(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
    ) -> Result<Self> {
        let db = Self::open_writer_inner(database.into(), object_store, config).await?;
        Ok(db.with_embedded_query_metrics().await)
    }

    /// Opens one parity-harness process over a caller-provided store.
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    pub async fn open_with_object_store_for_migration_parity(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
    ) -> Result<Self> {
        Self::open_writer_inner(database.into(), object_store, config).await
    }

    /// Open a read/write database handle with explicit tuning config.
    pub async fn open_with_config(source: HelixDbSource, config: DbConfig) -> Result<Self> {
        let (path, object_store) = source.into_parts()?;
        let db = Self::open_writer_inner(path, object_store, config).await?;
        Ok(db.with_embedded_query_metrics().await)
    }

    /// Opens a database for a transport server that owns its metrics recorder.
    #[doc(hidden)]
    pub async fn open_for_server(source: HelixDbSource) -> Result<Self> {
        let (path, object_store) = source.into_parts()?;
        Self::open_writer_inner(path, object_store, DbConfig::new()).await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_object_store_for_tests(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_with_object_store_for_tests_inner(database, object_store).await
    }

    /// Opens a process-local test database from its non-forgeable identity.
    #[cfg(test)]
    pub(crate) async fn open_with_process_local_token_for_tests(
        token: ProcessLocalDatabaseToken,
    ) -> Result<Self> {
        Self::open_with_object_store_for_tests_inner(
            token.database().to_string(),
            token.object_store(),
        )
        .await
    }

    #[cfg(test)]
    async fn open_with_object_store_for_tests_inner(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let path = database.into();
        let config = DbConfig::new();
        let db = Arc::new(
            Db::builder(path.clone(), Arc::clone(&object_store))
                .with_merge_operator(Arc::new(merge_operator::HelixMergeOperator::new()))
                .with_settings(
                    config
                        .slate()
                        .to_writer_settings(config.cache().object_store_cache()),
                )
                .build()
                .await?,
        );
        index_lifecycle::repository::bootstrap_writer(&db).await?;
        migrations::preflight_legacy_vector_reservations(&db).await?;
        let writer = HelixWriter::new(Arc::clone(&db), config.id_lease_size());
        migrations::run_blocking_startup_migration(&writer, config.migrations()).await?;
        index_lifecycle::outbox::reconcile_legacy_reader_coordination_operations(
            &db,
            DataScope::LegacyUnscoped,
        )
        .await?;
        index_lifecycle::outbox::reconcile_operation_queue(&db).await?;
        let loaded_catalog =
            index_lifecycle::repository::load_scope_catalog(db.as_ref(), DataScope::LegacyUnscoped)
                .await?;
        let storage = HelixStorage::Writer(Arc::new(writer));
        let fts_cache = build_fts_cache(&path, &object_store, &config)?;
        let db = Self::from_storage(
            HelixStorageParts::new(path, object_store, storage),
            HelixConfig::new(config),
            loaded_catalog,
            None,
            fts_cache,
        );
        migrations::migrate_legacy_definitions(&db).await?;
        Box::pin(migrations::migrate_active_vector_simhash_directories(&db)).await?;
        Ok(db)
    }

    /// Opens writer storage and starts disposable cache warming.
    async fn open_writer_inner(
        path: String,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
    ) -> Result<Self> {
        Self::open_writer_inner_with_index_scheduling(
            path,
            object_store,
            config,
            IndexLifecycleScheduling::Configured,
        )
        .await
    }

    /// Opens writer storage with an explicit runtime-only lifecycle scheduling policy.
    async fn open_writer_inner_with_index_scheduling(
        path: String,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
        index_scheduling: IndexLifecycleScheduling,
    ) -> Result<Self> {
        let vector_memory_settings = *config.cache().vector_memory();
        let slate_db_cache = build_slate_db_cache(config.cache().mode()).await?;
        let mut builder = Db::builder(path.clone(), Arc::clone(&object_store))
            .with_merge_operator(Arc::new(merge_operator::HelixMergeOperator::new()))
            .with_settings(
                config
                    .slate()
                    .to_writer_settings(config.cache().object_store_cache()),
            );

        match config.cache().mode() {
            CacheMode::VectorMemoryOnly => builder = builder.with_db_cache_disabled(),
            CacheMode::Memory { .. } | CacheMode::Hybrid { .. } => {
                let Some(cache) = &slate_db_cache else {
                    return Err(HelixDbError::Config(
                        "configured SlateDB cache mode must build a cache".into(),
                    ));
                };
                builder = builder.with_db_cache(Arc::clone(cache));
            }
        }
        let db = Arc::new(builder.build().await?);
        index_lifecycle::repository::bootstrap_writer(&db).await?;
        migrations::preflight_legacy_vector_reservations(&db).await?;
        let writer = HelixWriter::new(Arc::clone(&db), config.id_lease_size());
        migrations::run_blocking_startup_migration(&writer, config.migrations()).await?;
        index_lifecycle::outbox::reconcile_legacy_reader_coordination_operations(
            &db,
            DataScope::LegacyUnscoped,
        )
        .await?;
        index_lifecycle::outbox::reconcile_operation_queue(&db).await?;
        let loaded_catalog =
            index_lifecycle::repository::load_scope_catalog(db.as_ref(), DataScope::LegacyUnscoped)
                .await?;
        let storage = HelixStorage::Writer(Arc::new(writer));
        let fts_cache = build_fts_cache(&path, &object_store, &config)?;
        let db = Self::from_storage_with_index_scheduling(
            HelixStorageParts::new(path, object_store, storage),
            HelixConfig::new(config),
            loaded_catalog,
            slate_db_cache,
            fts_cache,
            index_scheduling,
        );
        if let Err(error) = migrations::migrate_legacy_definitions(&db).await {
            let _ = db.close().await;
            return Err(error);
        }
        if let Err(error) =
            Box::pin(migrations::migrate_active_vector_simhash_directories(&db)).await
        {
            let _ = db.close().await;
            return Err(error);
        }
        db.run_configured_startup_cache_warm().await?;
        db.run_configured_vector_memory_warm(vector_memory_settings)
            .await?;
        Ok(db)
    }

    /// Open a read-only database handle.
    pub async fn open_reader(source: HelixDbSource) -> Result<Self> {
        let config = source.embedded_default_config();
        Self::open_reader_with_config(source, config).await
    }

    /// Open a read-only database handle with explicit tuning config.
    pub async fn open_reader_with_config(source: HelixDbSource, config: DbConfig) -> Result<Self> {
        let (path, object_store) = source.into_parts()?;
        let db = Self::open_reader_inner(path, object_store, config).await?;
        Ok(db.with_embedded_query_metrics().await)
    }

    /// Opens a read-only handle over a caller-provided object store.
    pub async fn open_reader_with_object_store(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let db = Self::open_reader_inner(database.into(), object_store, DbConfig::new()).await?;
        Ok(db.with_embedded_query_metrics().await)
    }

    /// Opens a configured read-only handle over a caller-provided object store.
    pub async fn open_reader_with_object_store_and_config(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
    ) -> Result<Self> {
        let db = Self::open_reader_inner(database.into(), object_store, config).await?;
        Ok(db.with_embedded_query_metrics().await)
    }

    #[cfg(test)]
    pub(crate) async fn open_reader_with_object_store_for_tests(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_reader_with_object_store_for_tests_inner(database, object_store).await
    }

    #[cfg(test)]
    async fn open_reader_with_object_store_for_tests_inner(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let path = database.into();
        let config = DbConfig::new();
        let reader = DbReader::builder(path.clone(), Arc::clone(&object_store))
            .with_merge_operator(Arc::new(merge_operator::HelixMergeOperator::new()))
            .with_options(
                config
                    .slate()
                    .to_reader_options(config.cache().object_store_cache()),
            )
            .build()
            .await?;
        index_lifecycle::repository::require_reader_bootstrap_or_legacy(&reader).await?;
        let loaded_catalog =
            index_lifecycle::repository::load_scope_catalog(&reader, DataScope::LegacyUnscoped)
                .await?;
        let storage = HelixStorage::Reader(Arc::new(reader));
        let fts_cache = build_fts_cache(&path, &object_store, &config)?;
        Ok(Self::from_storage(
            HelixStorageParts::new(path, object_store, storage),
            HelixConfig::new(config),
            loaded_catalog,
            None,
            fts_cache,
        ))
    }

    async fn open_reader_inner(
        path: String,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
    ) -> Result<Self> {
        let vector_memory_settings = *config.cache().vector_memory();
        let slate_db_cache = build_slate_db_cache(config.cache().mode()).await?;
        let mut builder = DbReader::builder(path.clone(), Arc::clone(&object_store))
            .with_merge_operator(Arc::new(merge_operator::HelixMergeOperator::new()))
            .with_options(
                config
                    .slate()
                    .to_reader_options(config.cache().object_store_cache()),
            );
        match config.cache().mode() {
            CacheMode::VectorMemoryOnly => builder = builder.with_db_cache_disabled(),
            CacheMode::Memory { .. } | CacheMode::Hybrid { .. } => {
                let Some(cache) = &slate_db_cache else {
                    return Err(HelixDbError::Config(
                        "configured SlateDB cache mode must build a cache".into(),
                    ));
                };
                builder = builder.with_db_cache(Arc::clone(cache));
            }
        }
        let reader = builder.build().await?;
        index_lifecycle::repository::require_reader_bootstrap_or_legacy(&reader).await?;
        let loaded_catalog =
            index_lifecycle::repository::load_scope_catalog(&reader, DataScope::LegacyUnscoped)
                .await?;
        let storage = HelixStorage::Reader(Arc::new(reader));
        let fts_cache = build_fts_cache(&path, &object_store, &config)?;
        let db = Self::from_storage(
            HelixStorageParts::new(path, object_store, storage),
            HelixConfig::new(config),
            loaded_catalog,
            slate_db_cache,
            fts_cache,
        );
        db.run_configured_startup_cache_warm().await?;
        db.run_configured_vector_memory_warm(vector_memory_settings)
            .await?;
        Ok(db)
    }

    fn from_storage(
        storage: HelixStorageParts,
        config: HelixConfig,
        indexes: index_lifecycle::LoadedV2ScopeCatalog,
        slate_db_cache: Option<Arc<dyn DbCache>>,
        fts_cache: Option<Arc<search::text::FtsCache>>,
    ) -> Self {
        Self::from_storage_with_index_scheduling(
            storage,
            config,
            indexes,
            slate_db_cache,
            fts_cache,
            IndexLifecycleScheduling::Configured,
        )
    }

    fn from_storage_with_index_scheduling(
        storage: HelixStorageParts,
        config: HelixConfig,
        indexes: index_lifecycle::LoadedV2ScopeCatalog,
        slate_db_cache: Option<Arc<dyn DbCache>>,
        fts_cache: Option<Arc<search::text::FtsCache>>,
        index_scheduling: IndexLifecycleScheduling,
    ) -> Self {
        let vector_memory = VectorMemoryCache::new(*config.db().cache().vector_memory());
        let index_scope_gates = Arc::new(index_lifecycle::IndexScopeGates::default());
        let secondary_tuning = config.db().secondary_index_lifecycle();
        let lifecycle_throughput = config.db().index_lifecycle_throughput();
        let secondary_limits = config::SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(secondary_tuning.batch_rows().get())
                .expect("secondary batch rows are positive"),
            secondary_tuning.max_input_bytes(),
            secondary_tuning.max_output_operations(),
            secondary_tuning.max_output_bytes(),
            NonZeroU64::new(secondary_tuning.max_output_bytes().get())
                .expect("secondary output bytes are positive"),
        )
        .expect("secondary output ceiling is a valid family batch limit");
        let secondary_driver: Arc<dyn index_lifecycle::outbox::IndexOperationDriver> = Arc::new(
            index_lifecycle::secondary::SecondaryIndexDriver::with_catch_up_delay(
                Arc::clone(&index_scope_gates),
                secondary_tuning.catch_up_tail_delay_millis(),
            )
            .with_scan_tuning(lifecycle_throughput.scan()),
        );
        let vector_driver: Arc<dyn index_lifecycle::outbox::IndexOperationDriver> = Arc::new(
            index_lifecycle::vector::VectorIndexDriver::new(
                Arc::clone(&index_scope_gates),
                Arc::clone(&vector_memory.registry),
                Arc::clone(&vector_memory.simhasher_registry),
            )
            .with_scan_tuning(lifecycle_throughput.scan()),
        );
        let secondary_scheduling = index_scheduling.resolve(match secondary_tuning.worker_mode() {
            config::SecondaryIndexLifecycleWorkerMode::Enabled => {
                index_lifecycle::worker::IndexDriverScheduling::Automatic
            }
            config::SecondaryIndexLifecycleWorkerMode::Disabled => {
                index_lifecycle::worker::IndexDriverScheduling::ExplicitOnly
            }
        });
        let automatic_scheduling =
            index_scheduling.resolve(index_lifecycle::worker::IndexDriverScheduling::Automatic);
        let text_driver = Arc::new(
            index_lifecycle::text::driver::TextIndexDriver::with_storage(
                Arc::clone(&index_scope_gates),
                Arc::clone(storage.object_store()),
                storage.path().to_string(),
                config.db().search_index_backfill().text_compaction(),
            )
            .with_scan_tuning(lifecycle_throughput.scan()),
        );
        let text_operation_driver: Arc<dyn index_lifecycle::outbox::IndexOperationDriver> =
            text_driver.clone();
        let text_compactor: Arc<dyn index_lifecycle::worker::ActiveTextCompactionDriver> =
            text_driver;
        let text_capability = index_lifecycle::worker::IndexFamilyCapability::new(
            text_operation_driver,
            config.db().search_index_backfill().batch(),
            automatic_scheduling,
        );
        let secondary_capability = index_lifecycle::worker::IndexFamilyCapability::new(
            secondary_driver,
            secondary_limits,
            secondary_scheduling,
        );
        let vector_capability = index_lifecycle::worker::IndexFamilyCapability::new(
            vector_driver,
            config.db().search_index_backfill().batch(),
            automatic_scheduling,
        );
        let index_capabilities = index_lifecycle::worker::IndexFamilyCapabilities::new(
            secondary_capability,
            vector_capability,
            text_capability,
            text_compactor,
        );
        #[cfg(feature = "index-lifecycle-testing")]
        let lifecycle_metrics = Arc::new(index_lifecycle_testing::AutomaticLifecycleMetrics::new());
        let index_claim_sequences =
            Arc::new(index_lifecycle::worker::ClaimSequenceAllocator::new());
        let index_worker = match storage.handle() {
            HelixStorage::Writer(writer) => {
                Some(index_lifecycle::worker::IndexWorkerSupervisor::start(
                    Arc::clone(&writer.db),
                    index_capabilities.clone(),
                    config.db().index_lifecycle_throughput().concurrency(),
                    Arc::clone(&index_claim_sequences),
                    #[cfg(feature = "index-lifecycle-testing")]
                    Arc::clone(&lifecycle_metrics),
                ))
            }
            HelixStorage::Reader(_) => None,
        };
        let index_worker_wake = index_worker
            .as_ref()
            .map(index_lifecycle::worker::IndexWorkerSupervisor::wake_handle);
        Self {
            inner: Arc::new(HelixDBInner {
                storage,
                caches: HelixCaches {
                    slate_db: slate_db_cache,
                    fts: fts_cache,
                    vector_memory,
                    slate_last_warm: Mutex::new(None),
                    startup_tasks: StartupCacheTasks::new(),
                },
                index_scope_gates,
                runtime_state: RwLock::new(HelixRuntimeState::new(
                    DataScope::LegacyUnscoped,
                    indexes,
                )),
                index_capabilities,
                index_worker_wake,
                index_worker: Mutex::new(index_worker),
                index_claim_sequences,
                secondary_lifecycle_step: Mutex::new(()),
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_test_scheduling: index_scheduling,
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_metrics,
                query_metrics: RwLock::new(None),
                query_metrics_runtime: Mutex::new(None),
                close_state: Mutex::new(CloseState::Open),
                config,
            }),
        }
    }

    /// Return the handle mode.
    pub fn mode(&self) -> HelixDbMode {
        match self.inner.storage.handle() {
            HelixStorage::Reader(_) => HelixDbMode::ReadOnly,
            HelixStorage::Writer(_) => HelixDbMode::Writer,
        }
    }

    /// Whether this handle can execute write plans.
    pub fn is_writer_mode(&self) -> bool {
        self.mode() == HelixDbMode::Writer
    }

    /// Reports readiness of graph, secondary, and vector operations.
    pub const fn index_runtime_readiness(&self) -> IndexRuntimeReadiness {
        IndexRuntimeReadiness::Ready
    }

    /// Whether this handle is read-only.
    pub fn is_reader_mode(&self) -> bool {
        self.mode() == HelixDbMode::ReadOnly
    }

    /// Returns the exact storage sequence selected by a fresh handle snapshot.
    pub async fn visible_sequence(&self) -> Result<DatabaseSequence> {
        let sequence = match self.storage() {
            HelixStorage::Reader(reader) => reader.snapshot().await?.seq(),
            HelixStorage::Writer(writer) => writer.db().snapshot().await?.seq(),
        };
        Ok(DatabaseSequence(sequence))
    }

    /// Flushes an acknowledged writer state for reader-replica publication.
    ///
    /// The returned sequence is measured after the flush. Read-only handles
    /// reject this writer-only operation with a typed mode error.
    pub async fn flush_writer(&self) -> Result<DatabaseSequence> {
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        writer.db().flush().await?;
        Ok(DatabaseSequence(writer.db().snapshot().await?.seq()))
    }

    /// Borrow the database configuration used to open this handle.
    pub fn config(&self) -> &HelixConfig {
        &self.inner.config
    }

    /// Reads one retained index operation in exactly `scope`.
    ///
    /// A wrong scope is intentionally indistinguishable from an unknown or
    /// evicted operation ID.
    pub async fn get_index_operation(
        &self,
        scope: DataScope,
        operation_id: index_lifecycle::IndexOperationId,
    ) -> Result<index_lifecycle::IndexOperationStatus> {
        let operation = match self.storage() {
            HelixStorage::Reader(reader) => {
                index_lifecycle::outbox::read_operation(reader.as_ref(), scope, operation_id)
                    .await?
            }
            HelixStorage::Writer(writer) => {
                let snapshot = writer.db().snapshot().await?;
                index_lifecycle::outbox::read_operation(snapshot.as_ref(), scope, operation_id)
                    .await?
            }
        };
        let Some(operation) = operation else {
            return Err(HelixDbError::IndexOperationNotFound {
                operation_id: operation_id.as_uuid().to_string(),
            });
        };
        Ok(index_lifecycle::IndexOperationStatus::from_record(
            &operation,
        ))
    }

    /// Advances at most one immediately eligible secondary lifecycle step.
    ///
    /// This writer-only surface is available only when secondary background
    /// scheduling is Disabled. It returns `true` after one durable queue or
    /// operation transition and `false` only after a complete queue scan finds
    /// no immediately eligible secondary work.
    pub async fn process_secondary_index_lifecycle_once(&self) -> Result<bool> {
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        if self.config().db().secondary_index_lifecycle().worker_mode()
            != config::SecondaryIndexLifecycleWorkerMode::Disabled
        {
            return Err(HelixDbError::SecondaryLifecycleSteppingRequiresDisabledMode);
        }

        let _step = self.inner.secondary_lifecycle_step.lock().await;
        if !matches!(*self.inner.close_state.lock().await, CloseState::Open) {
            return Err(HelixDbError::DatabaseClosed);
        }
        let writer_epoch = self
            .inner
            .index_worker
            .lock()
            .await
            .as_ref()
            .map(index_lifecycle::worker::IndexWorkerSupervisor::writer_epoch)
            .ok_or(HelixDbError::DatabaseClosed)?;
        index_lifecycle::worker::process_secondary_once(
            writer.db(),
            &self.inner.index_capabilities,
            writer_epoch,
            &self.inner.index_claim_sequences,
        )
        .await
    }

    /// Validates and atomically enqueues one public CREATE against the current source cut.
    pub(crate) async fn enqueue_index_create(
        &self,
        scope: DataScope,
        spec: &ir::IndexDdlCreateSpec,
        mode: ir::IndexCreateMode,
    ) -> Result<index_lifecycle::IndexDdlReceipt> {
        let definition = runtime_catalog::dynamic_index_definition_from_create_spec(spec)?;
        let family = match definition.family() {
            index_lifecycle::IndexDefinitionFamily::Secondary => error::IndexFamily::Secondary,
            index_lifecycle::IndexDefinitionFamily::Vector => error::IndexFamily::Vector,
            index_lifecycle::IndexDefinitionFamily::Text => error::IndexFamily::Text,
        };
        if let Some(reason) = self.index_lifecycle_unavailable_reason(family) {
            return Err(HelixDbError::IndexLifecycleUnavailable { family, reason });
        }
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        let receipt = index_lifecycle::lifecycle::create_index_operation_from_current_source(
            writer.db(),
            scope,
            definition,
            mode,
        )
        .await?;
        self.notify_index_worker();
        Ok(receipt)
    }

    /// Resolves canonical settings and atomically enqueues one public DROP or abort.
    pub(crate) async fn enqueue_index_drop(
        &self,
        scope: DataScope,
        spec: &ir::IndexDdlDropSpec,
    ) -> Result<index_lifecycle::IndexDdlReceipt> {
        let identity = runtime_catalog::dynamic_index_identity_from_drop_spec(spec)?;
        let family = match identity.family() {
            index_lifecycle::IndexIdentityFamily::SecondaryEquality
            | index_lifecycle::IndexIdentityFamily::SecondaryRange => error::IndexFamily::Secondary,
            index_lifecycle::IndexIdentityFamily::Vector => error::IndexFamily::Vector,
            index_lifecycle::IndexIdentityFamily::Text => error::IndexFamily::Text,
        };
        if let Some(reason) = self.index_lifecycle_unavailable_reason(family) {
            return Err(HelixDbError::IndexLifecycleUnavailable { family, reason });
        }
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        let _catalog_permit = self
            .inner
            .index_scope_gates
            .catalog_change_permit(scope)
            .await;
        let Some(record) =
            index_lifecycle::repository::load_index_record(writer.db(), scope, &identity).await?
        else {
            return Err(HelixDbError::IndexNotFound(format!("{identity:?}")));
        };
        let definition = runtime_catalog::dynamic_index_definition_from_canonical_drop_spec(
            spec,
            record.definition(),
        )?;
        let receipt =
            index_lifecycle::lifecycle::drop_index_operation(writer.db(), scope, &definition)
                .await?;
        self.notify_index_worker();
        Ok(receipt)
    }

    /// Convergently requeues a blocked operation at its exact checkpoint.
    pub async fn retry_index_operation(
        &self,
        scope: DataScope,
        operation_id: index_lifecycle::IndexOperationId,
    ) -> Result<index_lifecycle::IndexOperationStatus> {
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        let operation =
            index_lifecycle::outbox::retry_operation(writer.db(), scope, operation_id).await?;
        if matches!(
            operation.execution_state(),
            index_lifecycle::IndexOperationExecutionState::Queued { .. }
        ) {
            self.notify_index_worker();
        }
        Ok(index_lifecycle::IndexOperationStatus::from_record(
            &operation,
        ))
    }

    /// Converts a constructing BUILD into cleanup, while converging on the
    /// same already-aborting or aborted BUILD.
    pub async fn abort_index_operation(
        &self,
        scope: DataScope,
        operation_id: index_lifecycle::IndexOperationId,
    ) -> Result<index_lifecycle::IndexOperationStatus> {
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        let operation =
            index_lifecycle::outbox::abort_operation(writer.db(), scope, operation_id).await?;
        if matches!(
            operation.execution_state(),
            index_lifecycle::IndexOperationExecutionState::Queued { .. }
        ) {
            self.notify_index_worker();
        }
        Ok(index_lifecycle::IndexOperationStatus::from_record(
            &operation,
        ))
    }

    /// Return the database path inside the object store.
    pub fn path(&self) -> &str {
        self.inner.storage.path()
    }

    /// Borrow the object store backing this handle.
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        self.inner.storage.object_store()
    }

    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn inner_db(&self) -> Arc<Db> {
        match self.inner.storage.handle() {
            HelixStorage::Writer(writer) => Arc::clone(&writer.db),
            HelixStorage::Reader(_) => panic!("read-only handles do not expose writer storage"),
        }
    }

    /// Return the planner-visible index catalog snapshot.
    pub fn index_catalog_snapshot(&self) -> IndexCatalogSnapshot {
        self.runtime_catalog_snapshot()
    }

    /// Build the immutable planner context for a request.
    pub fn planner_context(&self, params: ParamBindings) -> PlannerContext {
        let indexes = self.runtime_catalog_snapshot();
        PlannerContext {
            params,
            indexes,
            stats: Default::default(),
            runtime_feedback: Default::default(),
            storage: Default::default(),
            limits: Default::default(),
            optimizer_limits: Default::default(),
        }
    }

    /// Build the immutable planner context for a request storage namespace.
    pub async fn planner_context_scoped(
        &self,
        params: ParamBindings,
        tenant_scope: DataScope,
    ) -> Result<PlannerContext> {
        let indexes = self.runtime_catalog_snapshot_scoped(tenant_scope).await?;
        Ok(PlannerContext {
            params,
            indexes,
            stats: Default::default(),
            runtime_feedback: Default::default(),
            storage: Default::default(),
            limits: Default::default(),
            optimizer_limits: Default::default(),
        })
    }

    /// Build a planner context coupled to proof of its runtime catalog view.
    pub(crate) async fn planner_context_scoped_prepared(
        &self,
        params: ParamBindings,
        tenant_scope: DataScope,
    ) -> Result<PreparedPlannerContext> {
        let catalog_permit = self.index_catalog_scope_permit(tenant_scope).await;
        let _refresh_permit = self
            .inner
            .index_scope_gates
            .catalog_refresh_permit(tenant_scope)
            .await;
        if let HelixStorage::Writer(writer) = self.storage() {
            let repaired =
                index_lifecycle::outbox::reconcile_legacy_reader_coordination_operations(
                    writer.db(),
                    tenant_scope,
                )
                .await?;
            if repaired > 0 {
                self.notify_index_worker();
            }
        }
        let read_view =
            execution::interpreter::read_view::StableRequestReadView::open(self).await?;
        let loaded =
            index_lifecycle::repository::load_scope_catalog(&read_view, tenant_scope).await?;
        let (indexes, catalog) = {
            let mut state = self
                .inner
                .runtime_state
                .write()
                .expect("runtime state lock is not poisoned");
            state.replace_catalog(tenant_scope, loaded);
            state.planner_snapshot_with_catalog(tenant_scope)
        };
        Ok(PreparedPlannerContext {
            context: PlannerContext {
                params,
                indexes,
                stats: Default::default(),
                runtime_feedback: Default::default(),
                storage: Default::default(),
                limits: Default::default(),
                optimizer_limits: Default::default(),
            },
            proof: CatalogRefreshProof {
                runtime: Arc::downgrade(&self.inner),
                scope: tenant_scope,
                catalog_permit,
                catalog,
                read_view: Box::new(read_view),
            },
        })
    }

    /// Execute a physical plan exactly as emitted by the planner.
    pub async fn execute(
        &self,
        plan: &exec::ExecutablePlan,
        params: ParamBindings,
    ) -> Result<ExecutionResult> {
        self.execute_scoped(plan, params, DataScope::LegacyUnscoped)
            .await
    }

    /// Execute a physical plan in a request storage namespace.
    pub async fn execute_scoped(
        &self,
        plan: &exec::ExecutablePlan,
        params: ParamBindings,
        tenant_scope: DataScope,
    ) -> Result<ExecutionResult> {
        self.execute_scoped_controlled(
            plan,
            params,
            tenant_scope,
            execution_control::ExecutionControl::unlimited(),
        )
        .await
    }

    /// Execute a physical plan with request-scoped monotonic cancellation.
    pub async fn execute_scoped_controlled(
        &self,
        plan: &exec::ExecutablePlan,
        params: ParamBindings,
        tenant_scope: DataScope,
        execution_control: execution_control::ExecutionControl,
    ) -> Result<ExecutionResult> {
        Interpreter::new_scoped_controlled(self, params, tenant_scope, execution_control)
            .execute(plan)
            .await
    }

    /// Execute a plan with the non-forgeable catalog observation used to plan it.
    pub(crate) async fn execute_prepared_scoped_controlled(
        &self,
        plan: &exec::ExecutablePlan,
        params: ParamBindings,
        tenant_scope: DataScope,
        execution_control: execution_control::ExecutionControl,
        proof: CatalogRefreshProof,
    ) -> Result<ExecutionResult> {
        Interpreter::new_scoped_controlled_prepared(
            self,
            params,
            tenant_scope,
            execution_control,
            proof,
        )
        .execute(plan)
        .await
    }

    pub(crate) async fn prepare_idempotent_write_scoped_controlled(
        &self,
        plan: &exec::ExecutablePlan,
        params: ParamBindings,
        tenant_scope: DataScope,
        execution_control: execution_control::ExecutionControl,
        proof: CatalogRefreshProof,
        receipt_key: &[u8],
        now_unix_ms: u64,
    ) -> Result<execution::interpreter::PreparedWrite<'_>> {
        Interpreter::new_scoped_controlled_prepared(
            self,
            params,
            tenant_scope,
            execution_control,
            proof,
        )
        .prepare_idempotent_write(plan, receipt_key, now_unix_ms)
        .await
    }

    /// Execute an SDK-built query request.
    pub async fn query(&self, request: QueryRequest) -> Result<JsonValue> {
        let query_metrics = self.embedded_query_metrics();
        query_service::execute_query_on_observed(
            self,
            request,
            query_service::QueryMode::Execute,
            None,
            query_metrics.as_ref(),
        )
        .await
        .map(|response| JsonValue::Object(response.returns().clone().into_iter().collect()))
        .map_err(HelixDbError::from)
    }

    /// Execute an SDK-built query request in a request storage namespace.
    pub async fn query_scoped(
        &self,
        request: QueryRequest,
        tenant_scope: DataScope,
    ) -> Result<JsonValue> {
        let query_metrics = self.embedded_query_metrics();
        query_service::execute_query_on_scoped_observed(
            self,
            request,
            query_service::QueryMode::Execute,
            tenant_scope,
            None,
            query_metrics.as_ref(),
            execution_control::ExecutionControl::unlimited(),
        )
        .await
        .map(|response| JsonValue::Object(response.returns().clone().into_iter().collect()))
        .map_err(HelixDbError::from)
    }

    /// Execute an SDK-built query encoded as JSON bytes.
    pub async fn query_json(&self, request_json: &[u8]) -> Result<Vec<u8>> {
        self.query_json_scoped(request_json, DataScope::LegacyUnscoped)
            .await
    }

    /// Execute an SDK-built query encoded as JSON bytes in a request storage namespace.
    pub async fn query_json_scoped(
        &self,
        request_json: &[u8],
        tenant_scope: DataScope,
    ) -> Result<Vec<u8>> {
        let request = sonic_rs::from_slice::<QueryRequest>(request_json)
            .map_err(|err| HelixDbError::Query(format!("invalid query JSON: {err}")))?;
        let query_metrics = self.embedded_query_metrics();
        query_service::execute_query_on_scoped_observed(
            self,
            request,
            query_service::QueryMode::Execute,
            tenant_scope,
            None,
            query_metrics.as_ref(),
            execution_control::ExecutionControl::unlimited(),
        )
        .await
        .map_err(HelixDbError::from)?
        .to_json_bytes()
        .map_err(HelixDbError::from)
    }

    /// Cancels owned tasks and idempotently closes the underlying storage.
    ///
    /// Concurrent callers either perform the close or wait for the current
    /// attempt. The outbox worker is always joined before SlateDB or its cache
    /// closes, preserving its acyclic ownership contract.
    pub async fn close(&self) -> Result<()> {
        loop {
            let wait = {
                let mut state = self.inner.close_state.lock().await;
                match &mut *state {
                    CloseState::Open => {
                        *state = CloseState::Closing {
                            waiters: Vec::new(),
                        };
                        None
                    }
                    CloseState::Closing { waiters } => {
                        let (sender, receiver) = oneshot::channel();
                        waiters.push(sender);
                        Some(receiver)
                    }
                    CloseState::Closed => return Ok(()),
                }
            };
            let Some(wait) = wait else {
                break;
            };
            let _ = wait.await;
        }

        let result = async {
            let _secondary_step = self.inner.secondary_lifecycle_step.lock().await;
            self.inner
                .query_metrics
                .write()
                .expect("query metrics lock is not poisoned")
                .take();
            if let Some(runtime) = self.inner.query_metrics_runtime.lock().await.take() {
                runtime.shutdown().await;
            }
            if let Some(worker) = self.inner.index_worker.lock().await.take() {
                worker.stop().await;
            }
            if let Some(task) = self.inner.caches.startup_tasks.slate.lock().await.take() {
                task.stop().await;
            }
            if let Some(task) = self.inner.caches.startup_tasks.fts.lock().await.take() {
                task.stop().await;
            }
            if let Some(task) = self
                .inner
                .caches
                .vector_memory
                .refresh_task
                .lock()
                .await
                .take()
            {
                task.stop().await;
            }
            if let Some(cache) = &self.inner.caches.fts {
                cache.close().await;
            }
            match self.inner.storage.handle() {
                HelixStorage::Reader(reader) => reader.close().await?,
                HelixStorage::Writer(writer) => writer.close().await?,
            }
            if let Some(cache) = &self.inner.caches.slate_db {
                cache.close().await?;
            }
            Ok(())
        }
        .await;

        let waiters = {
            let mut state = self.inner.close_state.lock().await;
            let CloseState::Closing { waiters } = std::mem::replace(
                &mut *state,
                if result.is_ok() {
                    CloseState::Closed
                } else {
                    CloseState::Open
                },
            ) else {
                unreachable!("only the elected close caller may finish the close protocol");
            };
            waiters
        };
        for waiter in waiters {
            let _ = waiter.send(());
        }
        result
    }

    async fn with_embedded_query_metrics(self) -> Self {
        if cfg!(test) {
            return self;
        }
        match helix_metrics::query::transport::start_oss_from_env(telemetry::Source::Embedded) {
            Ok(Some(started)) => {
                *self
                    .inner
                    .query_metrics
                    .write()
                    .expect("query metrics lock is not poisoned") = Some(started.recorder);
                *self.inner.query_metrics_runtime.lock().await = Some(started.runtime);
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%error, "embedded query metrics are disabled");
            }
        }
        self
    }

    fn embedded_query_metrics(&self) -> Option<OssQueryMetrics> {
        self.inner
            .query_metrics
            .read()
            .expect("query metrics lock is not poisoned")
            .clone()
    }

    #[cfg(test)]
    fn set_embedded_query_metrics_for_tests(&self, query_metrics: OssQueryMetrics) {
        *self
            .inner
            .query_metrics
            .write()
            .expect("query metrics lock is not poisoned") = Some(query_metrics);
    }

    /// Refresh descriptor-bound vector caches from canonical Active generations.
    ///
    /// Writer storage supplies the exact snapshot sequence required by cache
    /// visibility checks. Standalone readers expose no comparable WAL-inclusive
    /// sequence and therefore remain on durable-storage fallback.
    pub async fn refresh_vector_memory_cache(&self) -> Result<()> {
        self.refresh_loaded_vector_memory_caches(
            self.inner.config.db().cache().vector_memory().budget(),
            None,
        )
        .await
    }

    /// Snapshot shared FTS split-cache state.
    pub async fn fts_cache_state(&self) -> Result<search::text::FtsCacheStateSnapshot> {
        match &self.inner.caches.fts {
            Some(cache) => Ok(cache.snapshot()),
            None => Ok(search::text::FtsCacheStateSnapshot::default()),
        }
    }

    /// Return a synchronous cache snapshot suitable for heartbeat sampling.
    ///
    /// The snapshot reads only bounded in-memory state and atomics.
    pub fn cache_stats(&self) -> DatabaseCacheStats {
        let slate = self.inner.storage.cache_usage_snapshot();
        let (fts_memory, fts_disk) = self.inner.caches.fts.as_ref().map_or_else(
            || {
                (
                    CacheTierSnapshot::disabled(CacheUsageSemantics::WeightedResident),
                    CacheTierSnapshot::disabled(CacheUsageSemantics::PhysicalFiles),
                )
            },
            |cache| {
                let snapshot = cache.snapshot();
                (
                    CacheTierSnapshot::ready(
                        CacheUsageSemantics::WeightedResident,
                        snapshot.retained_split_bytes,
                        Some(snapshot.resolved_memory_budget_bytes),
                    ),
                    if snapshot.resolved_disk_budget_bytes == 0 {
                        CacheTierSnapshot::disabled(CacheUsageSemantics::PhysicalFiles)
                    } else {
                        CacheTierSnapshot::ready(
                            CacheUsageSemantics::PhysicalFiles,
                            snapshot.disk_artifact_bytes,
                            Some(snapshot.resolved_disk_budget_bytes),
                        )
                    },
                )
            },
        );
        DatabaseCacheStats {
            slate_memory: CacheTierSnapshot::from_slate(
                CacheUsageSemantics::WeightedResident,
                slate.db_cache.memory,
            ),
            slate_object_store_disk: CacheTierSnapshot::from_slate(
                CacheUsageSemantics::PhysicalFiles,
                slate.object_store,
            ),
            foyer_hybrid_disk: if matches!(
                self.inner.config.db().cache().mode(),
                CacheMode::Hybrid { .. }
            ) {
                CacheTierSnapshot::from_slate(
                    CacheUsageSemantics::AllocatedBlocks,
                    slate.db_cache.disk,
                )
            } else {
                CacheTierSnapshot::disabled(CacheUsageSemantics::AllocatedBlocks)
            },
            fts_memory,
            fts_disk,
            vector_memory: CacheTierSnapshot::ready(
                CacheUsageSemantics::EstimatedResident,
                self.inner.caches.vector_memory.registry.estimated_bytes(),
                self.inner
                    .config
                    .db()
                    .cache()
                    .vector_memory()
                    .budget()
                    .bytes(),
            ),
        }
    }

    /// Snapshot SlateDB metadata-warm state.
    pub async fn slate_cache_state(&self) -> SlateCacheStateSnapshot {
        let warm = self.inner.config.db().cache().slate_warm();
        SlateCacheStateSnapshot {
            enabled: warm.is_some_and(|warm| warm.mode() != config::CacheWarmMode::Off),
            warm_mode: warm.map(config::SlateWarmConfig::mode),
            last_warm: self.inner.caches.slate_last_warm.lock().await.clone(),
        }
    }

    /// Warm index, filter, and stats entries for the newest live physical SSTs.
    pub async fn warm_slate_cache(&self) -> Result<SlateWarmSummary> {
        let Some(config) = self.inner.config.db().cache().slate_warm() else {
            return Ok(SlateWarmSummary::default());
        };
        if config.mode() == config::CacheWarmMode::Off {
            return Ok(SlateWarmSummary::default());
        }
        let summary = match self.storage() {
            HelixStorage::Writer(writer) => {
                warm_slate_metadata(
                    writer.db(),
                    config.concurrency(),
                    config.startup_sst_limit(),
                )
                .await
            }
            HelixStorage::Reader(reader) => {
                warm_slate_metadata(
                    reader.as_ref(),
                    config.concurrency(),
                    config.startup_sst_limit(),
                )
                .await
            }
        };
        *self.inner.caches.slate_last_warm.lock().await = Some(summary.clone());
        Ok(summary)
    }

    /// Wait for owned startup warm tasks and the initial vector refresh to finish.
    pub async fn wait_for_startup_cache_warm(&self) {
        if let Some(task) = self.inner.caches.startup_tasks.slate.lock().await.take() {
            task.wait().await;
        }
        if let Some(task) = self.inner.caches.startup_tasks.fts.lock().await.take() {
            task.wait().await;
        }
        let initial_vector_refresh = self
            .inner
            .caches
            .vector_memory
            .refresh_task
            .lock()
            .await
            .as_ref()
            .map(|task| task.initial_refresh.clone());
        let Some(mut initial_vector_refresh) = initial_vector_refresh else {
            return;
        };
        while !*initial_vector_refresh.borrow() {
            if initial_vector_refresh.changed().await.is_err() {
                break;
            }
        }
    }

    /// Best-effort warm of currently loaded Active V2 text splits from one snapshot.
    pub async fn warm_fts_cache(&self) -> Result<search::text::FtsWarmSummary> {
        let Some(cache) = &self.inner.caches.fts else {
            return Ok(search::text::FtsWarmSummary::default());
        };
        // Retain one guard for the complete runtime snapshot. Reacquiring a
        // read lock after a writer queues can deadlock on writer-preferring
        // `RwLock` implementations. Drop the guard before async warm I/O.
        let mut handles = {
            let runtime_state = self
                .inner
                .runtime_state
                .read()
                .expect("runtime state lock is not poisoned");
            runtime_state
                .loaded_scopes()
                .into_iter()
                .flat_map(|scope| runtime_state.active_handles(scope))
                .filter(|handle| matches!(handle, index_lifecycle::ActiveIndexHandle::Text { .. }))
                .collect::<Vec<_>>()
        };
        handles.sort_by_key(|handle| (handle.scope(), handle.index_id(), handle.generation()));
        if let Some(limit) = self
            .inner
            .config
            .db()
            .cache()
            .fts_startup_generation_limit()
        {
            handles.truncate(limit);
        }

        let snapshot = match self.storage() {
            HelixStorage::Writer(writer) => writer.db().snapshot().await?,
            HelixStorage::Reader(reader) => reader.snapshot().await?,
        };
        Ok(self
            .warm_fts_for_reader(snapshot.as_ref(), cache, handles)
            .await)
    }

    async fn warm_fts_for_reader(
        &self,
        reader: &(impl slatedb::DbReadOps + Sync),
        cache: &Arc<search::text::FtsCache>,
        handles: Vec<index_lifecycle::ActiveIndexHandle>,
    ) -> search::text::FtsWarmSummary {
        let started = std::time::Instant::now();
        let generation_count = handles.len();
        let mut summary = search::text::FtsWarmSummary {
            generation_count,
            ..Default::default()
        };
        for handle in handles {
            let warmed: Result<search::text::FtsWarmSummary> = async {
                let authority =
                    index_lifecycle::text::serving::ActiveTextServingAuthority::try_from_active(
                        &handle,
                    )?;
                let roots =
                    index_lifecycle::text::serving::load_active_manifest_roots(reader, &authority)
                        .await?;
                let mut splits = Vec::new();
                for root in roots {
                    for page in 0..root.page_count() {
                        for split in index_lifecycle::text::serving::load_active_manifest_page(
                            reader, &root, page,
                        )
                        .await?
                        {
                            splits.push(search::text::TextSplitRef {
                                blob: search::text::TextBlobRef {
                                    sha256: *split.blob().hash(),
                                    size_bytes: split.blob().size(),
                                },
                                footer_offset: split.footer_offset(),
                                footer_len: split.footer_length(),
                                hotcache_len: split.hot_cache_length(),
                                total_size_bytes: split.total_size(),
                            });
                        }
                    }
                }
                let warmed = cache.warm_splits(1, splits).await;
                Ok(warmed)
            }
            .await;
            match warmed {
                Ok(warmed) => {
                    summary.split_count += warmed.split_count;
                    summary.opened_splits += warmed.opened_splits;
                    summary.hydrated_splits += warmed.hydrated_splits;
                    summary.hydrated_bytes += warmed.hydrated_bytes;
                    summary.warm_errors += warmed.warm_errors;
                }
                Err(error) => {
                    summary.warm_errors += 1;
                    tracing::warn!(%error, "FTS warm generation failed");
                }
            }
        }
        summary.warm_elapsed_ms = started.elapsed().as_millis() as u64;
        summary
    }

    /// Refreshes every loaded runtime scope from a fresh Active inventory.
    ///
    /// A bounded global budget is divided deterministically across scopes before
    /// per-index admission. The inventory is reread on every pass, so tenant
    /// scopes loaded after task startup participate without restarting the
    /// worker.
    async fn refresh_loaded_vector_memory_caches(
        &self,
        budget: config::VectorMemoryBudget,
        mut shutdown: Option<&mut watch::Receiver<bool>>,
    ) -> Result<()> {
        let scopes = self
            .inner
            .runtime_state
            .read()
            .expect("runtime state lock is not poisoned")
            .loaded_scopes();
        let scope_count = u64::try_from(scopes.len()).map_err(|_| {
            HelixDbError::InvariantViolation(
                "vector memory loaded scope count exceeds u64".to_string(),
            )
        })?;
        for (ordinal, scope) in scopes.into_iter().enumerate() {
            if shutdown.as_ref().is_some_and(|rx| *rx.borrow()) {
                break;
            }
            let scope_budget = match budget.bytes() {
                Some(bytes) => {
                    let ordinal = u64::try_from(ordinal).map_err(|_| {
                        HelixDbError::InvariantViolation(
                            "vector memory scope ordinal exceeds u64".to_string(),
                        )
                    })?;
                    let equal = bytes / scope_count;
                    let remainder = bytes % scope_count;
                    Some(equal + u64::from(ordinal < remainder))
                }
                None => None,
            };
            self.refresh_one_vector_memory_scope(
                scope,
                search::vector::VectorCacheHydrationBudget::from_optional_bytes(scope_budget),
                shutdown.as_deref_mut(),
            )
            .await?;
        }
        Ok(())
    }

    /// Hydrates one scope from exact Active handles and one stable writer snapshot.
    async fn refresh_one_vector_memory_scope(
        &self,
        scope: DataScope,
        budget: search::vector::VectorCacheHydrationBudget,
        shutdown: Option<&mut watch::Receiver<bool>>,
    ) -> Result<()> {
        let HelixStorage::Writer(writer) = self.storage() else {
            return Ok(());
        };
        self.refresh_runtime_catalog(scope).await?;
        let active = self.active_index_handles_loaded(scope);
        search::vector::hydrate_active_generations(
            writer.db(),
            active,
            &self.inner.caches.vector_memory.registry,
            budget,
            shutdown,
        )
        .await
    }

    async fn run_configured_vector_memory_warm(
        &self,
        settings: config::VectorMemorySettings,
    ) -> Result<()> {
        if matches!(self.storage(), HelixStorage::Reader(_)) {
            return Ok(());
        }
        match settings.hydration() {
            config::VectorMemoryHydrationMode::BlockingThenBackground { .. } => {
                self.refresh_vector_memory_cache().await?;
            }
            config::VectorMemoryHydrationMode::Background { .. } => {}
        }

        let runtime = Arc::downgrade(&self.inner);
        let budget = settings.budget();
        let interval = Duration::from_secs(settings.poll_interval_secs());
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let (initial_refresh_tx, initial_refresh) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let mut initial_refresh_tx = Some(initial_refresh_tx);
            loop {
                if *shutdown_rx.borrow() {
                    break;
                }
                let Some(inner) = runtime.upgrade() else {
                    break;
                };
                let database = HelixDB { inner };
                let result = database
                    .refresh_loaded_vector_memory_caches(budget, Some(&mut shutdown_rx))
                    .await;
                drop(database);
                if let Err(err) = result {
                    tracing::warn!(error = %err, "failed to refresh vector memory stores");
                }
                if let Some(initial_refresh_tx) = initial_refresh_tx.take() {
                    let _ = initial_refresh_tx.send(true);
                }
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        match changed {
                            Ok(()) => {
                                if *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });
        *self.inner.caches.vector_memory.refresh_task.lock().await =
            Some(VectorMemoryRefreshTask {
                shutdown,
                initial_refresh,
                handle,
            });
        Ok(())
    }

    async fn run_configured_startup_cache_warm(&self) -> Result<()> {
        match self
            .inner
            .config
            .db()
            .cache()
            .slate_warm()
            .map(config::SlateWarmConfig::mode)
        {
            Some(config::CacheWarmMode::Blocking) => {
                if let Err(error) = self.warm_slate_cache().await {
                    tracing::warn!(%error, "SlateDB blocking startup cache warm failed");
                }
            }
            Some(config::CacheWarmMode::Background) => {
                let runtime = Arc::downgrade(&self.inner);
                let handle = tokio::spawn(async move {
                    let Some(inner) = runtime.upgrade() else {
                        return;
                    };
                    let database = HelixDB { inner };
                    if let Err(error) = database.warm_slate_cache().await {
                        tracing::warn!(%error, "SlateDB startup cache warm failed");
                    }
                });
                *self.inner.caches.startup_tasks.slate.lock().await =
                    Some(CacheWarmTask { handle });
            }
            Some(config::CacheWarmMode::Off) | None => {}
        }

        match self.inner.config.db().cache().fts_warm_mode() {
            config::CacheWarmMode::Blocking => {
                if let Err(error) = self.warm_fts_cache().await {
                    tracing::warn!(%error, "FTS blocking startup cache warm failed");
                }
            }
            config::CacheWarmMode::Background => {
                let runtime = Arc::downgrade(&self.inner);
                let handle = tokio::spawn(async move {
                    let Some(inner) = runtime.upgrade() else {
                        return;
                    };
                    let database = HelixDB { inner };
                    if let Err(error) = database.warm_fts_cache().await {
                        tracing::warn!(%error, "FTS startup cache warm failed");
                    }
                });
                *self.inner.caches.startup_tasks.fts.lock().await = Some(CacheWarmTask { handle });
            }
            config::CacheWarmMode::Off => {}
        }
        Ok(())
    }

    pub(crate) fn storage(&self) -> &HelixStorage {
        self.inner.storage.handle()
    }

    /// Returns why one family cannot cross the public lifecycle boundary.
    pub(crate) fn index_lifecycle_unavailable_reason(
        &self,
        family: error::IndexFamily,
    ) -> Option<error::IndexLifecycleUnavailableReason> {
        match family {
            error::IndexFamily::Secondary
            | error::IndexFamily::Vector
            | error::IndexFamily::Text => None,
            error::IndexFamily::DynamicIndexes => {
                Some(error::IndexLifecycleUnavailableReason::MutationMaintenanceUnavailable)
            }
        }
    }

    pub(crate) fn fts_cache(&self) -> Option<&Arc<search::text::FtsCache>> {
        self.inner.caches.fts.as_ref()
    }

    /// Acquires shared mutation authority before a request-owned write snapshot.
    pub(crate) async fn index_mutation_scope_permit(
        &self,
        scope: DataScope,
    ) -> index_lifecycle::IndexScopeMutationPermit {
        self.inner.index_scope_gates.mutation_permit(scope).await
    }

    /// Acquires shared catalog authority across planning and write-view open.
    pub(crate) async fn index_catalog_scope_permit(
        &self,
        scope: DataScope,
    ) -> index_lifecycle::IndexScopeCatalogPermit {
        self.inner.index_scope_gates.catalog_permit(scope).await
    }

    /// Wakes the parent-owned lifecycle worker after migration enqueues work.
    pub(crate) async fn wake_index_worker(&self) {
        self.notify_index_worker();
    }

    /// Notifies the lifecycle worker without borrowing supervisor ownership.
    fn notify_index_worker(&self) {
        let Some(worker) = &self.inner.index_worker_wake else {
            return;
        };
        worker.wake();
    }

    pub(crate) async fn index_worker_epoch(&self) -> Result<index_lifecycle::WriterEpoch> {
        self.inner
            .index_worker
            .lock()
            .await
            .as_ref()
            .map(index_lifecycle::worker::IndexWorkerSupervisor::writer_epoch)
            .ok_or(HelixDbError::DatabaseClosed)
    }

    #[cfg(any(test, feature = "migration-parity", feature = "production-coverage"))]
    pub async fn process_migration_once(&self) -> Result<bool> {
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        migrations::process_migration_once(
            writer,
            DataScope::LegacyUnscoped,
            self.config().db().migrations(),
        )
        .await
    }

    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) async fn install_index_for_tests(
        &self,
        definition: ValidatedDynamicIndexDefinition,
    ) -> Result<()> {
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        let receipt = index_lifecycle::lifecycle::create_index_operation_from_current_source(
            writer.db(),
            DataScope::LegacyUnscoped,
            definition,
            ir::IndexCreateMode::IfNotExists,
        )
        .await?;
        let Some(operation_id) = receipt.operation_id() else {
            self.refresh_runtime_catalog(DataScope::LegacyUnscoped)
                .await?;
            return Ok(());
        };
        self.wake_index_worker().await;
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                match self
                    .get_index_operation(DataScope::LegacyUnscoped, operation_id)
                    .await?
                {
                    index_lifecycle::IndexOperationStatus::Succeeded { .. } => break,
                    index_lifecycle::IndexOperationStatus::Blocked { .. }
                    | index_lifecycle::IndexOperationStatus::Aborted { .. } => {
                        return Err(HelixDbError::InvariantViolation(
                            "test index lifecycle did not succeed".to_string(),
                        ));
                    }
                    index_lifecycle::IndexOperationStatus::Queued { .. }
                    | index_lifecycle::IndexOperationStatus::Running { .. } => {
                        tokio::task::yield_now().await;
                    }
                }
            }
            Result::<()>::Ok(())
        })
        .await
        .map_err(|_| {
            HelixDbError::InvariantViolation("test index lifecycle timed out".to_string())
        })??;
        self.refresh_runtime_catalog(DataScope::LegacyUnscoped)
            .await
    }

    /// Returns the descriptor-bound managed vector cache registry.
    ///
    /// Hydration, managed reads, mutation publication, and cleanup share this
    /// owner so exact-generation retirement cannot race a detached cache map.
    pub(crate) fn vector_cache_registry(&self) -> &search::vector::VectorCacheRegistry {
        &self.inner.caches.vector_memory.registry
    }

    /// Returns the runtime-owned bounded SimHasher projection registry.
    pub(crate) fn simhasher_registry(&self) -> &Arc<search::vector::SimHasherRegistry> {
        &self.inner.caches.vector_memory.simhasher_registry
    }

    pub(crate) fn runtime_config_snapshot_loaded(&self, scope: DataScope) -> RuntimeIndexCatalog {
        self.inner
            .runtime_state
            .read()
            .expect("runtime state lock is not poisoned")
            .catalog(scope)
    }

    pub(crate) fn runtime_catalog_snapshot(&self) -> IndexCatalogSnapshot {
        self.inner
            .runtime_state
            .read()
            .expect("runtime state lock is not poisoned")
            .planner_snapshot(DataScope::LegacyUnscoped)
    }

    pub(crate) async fn runtime_catalog_snapshot_scoped(
        &self,
        scope: DataScope,
    ) -> Result<IndexCatalogSnapshot> {
        self.refresh_runtime_catalog(scope).await?;
        Ok(self
            .inner
            .runtime_state
            .read()
            .expect("runtime state lock is not poisoned")
            .planner_snapshot(scope))
    }

    /// Returns whether a proof and its exact read view belong to this scope.
    pub(crate) fn catalog_refresh_proof_belongs_to(
        &self,
        proof: &CatalogRefreshProof,
        scope: DataScope,
    ) -> bool {
        proof
            .runtime
            .upgrade()
            .is_some_and(|runtime| Arc::ptr_eq(&runtime, &self.inner) && proof.scope == scope)
    }

    /// Transfers catalog authority while its gate still excludes durable publication.
    pub(crate) fn consume_catalog_refresh_proof(
        &self,
        proof: CatalogRefreshProof,
        scope: DataScope,
    ) -> Option<index_lifecycle::IndexScopeCatalogPermit> {
        if !self.catalog_refresh_proof_belongs_to(&proof, scope) {
            return None;
        }
        Some(proof.into_write_permit())
    }

    #[cfg(test)]
    pub(crate) fn runtime_catalog_generation_for_tests(&self, scope: DataScope) -> u64 {
        self.inner
            .runtime_state
            .read()
            .expect("runtime state lock is not poisoned")
            .generation(scope)
            .0
            .get()
    }

    /// Rebuilds one planner catalog from persisted Active rows.
    ///
    /// The per-scope refresh permit prevents an older overlapping scan from
    /// publishing after a newer scan. Writer scans use a SlateDB snapshot;
    /// read-only scans reject a concurrently advancing reader view.
    pub(crate) async fn refresh_runtime_catalog(&self, scope: DataScope) -> Result<()> {
        let _refresh_permit = self
            .inner
            .index_scope_gates
            .catalog_refresh_permit(scope)
            .await;
        if let HelixStorage::Writer(writer) = self.storage() {
            let repaired =
                index_lifecycle::outbox::reconcile_legacy_reader_coordination_operations(
                    writer.db(),
                    scope,
                )
                .await?;
            if repaired > 0 {
                self.notify_index_worker();
            }
        }
        let loaded = match self.storage() {
            HelixStorage::Reader(reader) => {
                let observed = reader.status();
                let loaded =
                    index_lifecycle::repository::load_scope_catalog(reader.as_ref(), scope).await?;
                if reader.status() != observed {
                    return Err(HelixDbError::RequestReadViewChanged);
                }
                loaded
            }
            HelixStorage::Writer(writer) => {
                let snapshot = writer.db().snapshot().await?;
                index_lifecycle::repository::load_scope_catalog(snapshot.as_ref(), scope).await?
            }
        };
        self.inner
            .runtime_state
            .write()
            .expect("runtime state lock is not poisoned")
            .replace_catalog(scope, loaded);
        Ok(())
    }

    pub(crate) fn active_index_handles_loaded(
        &self,
        scope: DataScope,
    ) -> Vec<index_lifecycle::ActiveIndexHandle> {
        self.inner
            .runtime_state
            .read()
            .expect("runtime state lock is not poisoned")
            .active_handles(scope)
    }
}

async fn warm_slate_metadata<T>(
    database: &T,
    concurrency: usize,
    startup_sst_limit: usize,
) -> SlateWarmSummary
where
    T: DbCacheManagerOps + DbMetadataOps + Sync + ?Sized,
{
    let started = std::time::Instant::now();
    let manifest = database.manifest();
    let mut sst_ids = manifest
        .l0()
        .iter()
        .flat_map(|view| std::iter::once(view.sst.id))
        .chain(
            manifest
                .compacted()
                .iter()
                .flat_map(|run| run.sst_views.iter().map(|view| view.sst.id)),
        )
        .chain(manifest.segments().iter().flat_map(|segment| {
            segment
                .l0()
                .iter()
                .flat_map(|view| std::iter::once(view.sst.id))
                .chain(
                    segment
                        .compacted()
                        .iter()
                        .flat_map(|run| run.sst_views.iter().map(|view| view.sst.id)),
                )
        }))
        .collect::<Vec<_>>();
    sst_ids.sort_unstable_by(|left, right| right.cmp(left));
    sst_ids.dedup();
    sst_ids.truncate(startup_sst_limit);

    let outcomes = stream::iter(sst_ids.iter().copied())
        .map(|sst_id| async move {
            database
                .warm_sst(
                    sst_id,
                    &[CacheTarget::Index, CacheTarget::Filters, CacheTarget::Stats],
                )
                .await
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    let warm_errors = outcomes.iter().filter(|outcome| outcome.is_err()).count() as u64;
    for error in outcomes.into_iter().filter_map(|outcome| outcome.err()) {
        tracing::warn!(%error, "SlateDB SST metadata warm failed");
    }
    SlateWarmSummary {
        sst_count: sst_ids.len(),
        warmed_ssts: sst_ids.len().saturating_sub(warm_errors as usize),
        warm_errors,
        warm_elapsed_ms: started.elapsed().as_millis() as u64,
    }
}

fn build_fts_cache(
    path: &str,
    object_store: &Arc<dyn ObjectStore>,
    config: &DbConfig,
) -> Result<Option<Arc<search::text::FtsCache>>> {
    let cache_config = match config.cache().mode() {
        CacheMode::Memory { fts: Some(fts), .. } => {
            search::text::FtsCacheConfig::Memory(fts.clone())
        }
        CacheMode::Hybrid { fts: Some(fts), .. } => {
            search::text::FtsCacheConfig::Hybrid(fts.clone())
        }
        CacheMode::VectorMemoryOnly
        | CacheMode::Memory { fts: None, .. }
        | CacheMode::Hybrid { fts: None, .. } => return Ok(None),
    };
    search::text::FtsCache::new(path, Arc::clone(object_store), cache_config)
        .map(Arc::new)
        .map(Some)
}

const FOYER_MIN_DISK_BLOCK_SIZE_BYTES: usize = 64 * 1024;
const FOYER_MAX_DISK_BLOCK_SIZE_BYTES: usize = 16 * 1024 * 1024;
const FOYER_TARGET_MAX_DISK_PARTITIONS: usize = 32 * 1024;

fn foyer_disk_block_size(disk_capacity_bytes: usize) -> usize {
    debug_assert!(disk_capacity_bytes > 0);
    disk_capacity_bytes
        .div_ceil(FOYER_TARGET_MAX_DISK_PARTITIONS)
        .next_power_of_two()
        .clamp(
            FOYER_MIN_DISK_BLOCK_SIZE_BYTES,
            FOYER_MAX_DISK_BLOCK_SIZE_BYTES,
        )
}

async fn build_slate_db_cache(config: &CacheMode) -> Result<Option<Arc<dyn DbCache>>> {
    match config {
        CacheMode::VectorMemoryOnly => Ok(None),
        CacheMode::Memory { slate_db, .. } => {
            let block_cache: Arc<dyn DbCache> =
                Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                    max_capacity: slate_db.block_bytes(),
                    ..Default::default()
                }));
            let metadata_cache: Arc<dyn DbCache> =
                Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                    max_capacity: slate_db.metadata_bytes(),
                    ..Default::default()
                }));
            Ok(Some(Arc::new(
                SplitCache::new()
                    .with_block_cache(Some(block_cache))
                    .with_meta_cache(Some(metadata_cache))
                    .build(),
            )))
        }
        CacheMode::Hybrid { slate_db, .. } => {
            let metrics = FoyerHybridCacheMetrics::new();
            let cache = HybridCacheBuilder::new()
                .with_name("helix-slate-hybrid")
                .with_metrics_registry(metrics.registry())
                .memory(slate_db.memory_bytes())
                .with_weighter(|_, value: &CachedEntry| value.size())
                .storage()
                .with_io_engine_config(PsyncIoEngineConfig::new())
                .with_engine_config(
                    BlockEngineConfig::new(
                        FsDeviceBuilder::new(slate_db.disk().root())
                            .with_capacity(slate_db.disk().bytes())
                            .build()
                            .map_err(|err| {
                                HelixDbError::Config(format!(
                                    "failed to build Slate hybrid cache device: {err}"
                                ))
                            })?,
                    )
                    .with_block_size(foyer_disk_block_size(slate_db.disk().bytes())),
                )
                .build()
                .await
                .map_err(|err| {
                    HelixDbError::Config(format!("failed to build Slate hybrid cache: {err}"))
                })?;
            Ok(Some(Arc::new(
                FoyerHybridCache::new_with_cache_and_metrics(cache, metrics),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::keys::tenant::TenantId;
    use helix_ast::batch::{read_batch, write_batch};
    use helix_ast::graph::NodeRef;
    use helix_ast::query::QueryRequest;
    use helix_ast::traversal::g;
    use helix_ast::value::PropertyInput;

    fn tenant_scope(value: &str) -> DataScope {
        DataScope::Tenant(TenantId::from_ulid_str(value).expect("valid tenant"))
    }

    #[tokio::test]
    async fn slate_hybrid_cache_uses_bounded_foyer_partitions() {
        use crate::config::{
            ObjectStoreWarmLevel, SlateHybridCacheConfig, SlateObjectStoreCacheSettings,
            SlateWarmConfig,
        };

        let root = tempfile::tempdir().expect("temporary cache root");
        let foyer_root = root.path().join("foyer");
        let disk_bytes = 16 * 1024 * 1024;
        let mode = CacheMode::Hybrid {
            slate_db: SlateHybridCacheConfig::try_new(1024 * 1024, &foyer_root, disk_bytes)
                .expect("valid Slate hybrid cache"),
            object_store: SlateObjectStoreCacheSettings::try_new(
                root.path().join("object-store"),
                Some(1024 * 1024),
                4096,
                false,
                ObjectStoreWarmLevel::Off,
                None,
                1,
            )
            .expect("valid object-store cache"),
            slate_warm: SlateWarmConfig::Off,
            fts: None,
        };

        let cache = build_slate_db_cache(&mode)
            .await
            .expect("Foyer cache builds")
            .expect("hybrid mode enables Foyer");
        let partition_count = std::fs::read_dir(&foyer_root)
            .expect("Foyer cache directory exists")
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("foyer-storage-direct-fs-")
            })
            .count();
        assert_eq!(
            partition_count,
            disk_bytes / foyer_disk_block_size(disk_bytes)
        );
        cache.close().await.expect("Foyer cache closes");
    }

    #[test]
    fn foyer_disk_block_size_scales_embedded_and_managed_caches() {
        assert_eq!(
            foyer_disk_block_size(16 * 1024 * 1024),
            FOYER_MIN_DISK_BLOCK_SIZE_BYTES
        );
        assert_eq!(
            foyer_disk_block_size(352 * 1024 * 1024 * 1024),
            FOYER_MAX_DISK_BLOCK_SIZE_BYTES
        );
        assert_eq!(
            352 * 1024 * 1024 * 1024 / foyer_disk_block_size(352 * 1024 * 1024 * 1024),
            22_528
        );
    }

    #[test]
    fn embedded_sources_select_bounded_local_defaults() {
        for (source, flush_interval, block_bytes, metadata_bytes) in [
            (
                HelixDbSource::InMemory {
                    database: "memory-defaults".to_string(),
                },
                Duration::from_millis(1),
                16 * 1024 * 1024,
                8 * 1024 * 1024,
            ),
            (
                HelixDbSource::Disk {
                    root: PathBuf::from("/tmp/helix-embedded-default-contract"),
                    database: "disk-defaults".to_string(),
                },
                Duration::from_millis(3),
                48 * 1024 * 1024,
                16 * 1024 * 1024,
            ),
        ] {
            let config = source.embedded_default_config();
            let slate = config.slate().to_writer_settings(None);
            assert_eq!(slate.flush_interval, Some(flush_interval));
            assert_eq!(slate.l0_sst_size_bytes, 16 * 1024 * 1024);
            assert_eq!(slate.max_unflushed_bytes, 64 * 1024 * 1024);
            assert_eq!(
                config.cache().vector_memory().budget().bytes(),
                Some(64 * 1024 * 1024)
            );
            assert_eq!(
                config.cache().vector_memory().simhasher_cache().bytes(),
                8 * 1024 * 1024
            );
            let CacheMode::Memory {
                slate_db,
                slate_warm,
                fts: Some(fts),
            } = config.cache().mode()
            else {
                panic!("embedded local defaults must use bounded memory caches");
            };
            assert_eq!(slate_db.block_bytes(), block_bytes);
            assert_eq!(slate_db.metadata_bytes(), metadata_bytes);
            assert_eq!(slate_warm, &crate::config::SlateWarmConfig::Off);
            assert_eq!(fts.memory_bytes(), 16 * 1024 * 1024);
            assert_eq!(fts.warm(), &crate::config::FtsWarmConfig::Off);
        }

        let object_storage = HelixDbSource::ObjectStorage {
            database: "remote-defaults".to_string(),
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: None,
            allow_http: false,
        }
        .embedded_default_config();
        assert_eq!(
            object_storage
                .slate()
                .to_writer_settings(None)
                .flush_interval,
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            object_storage.cache().vector_memory().budget().bytes(),
            Some(256 * 1024 * 1024)
        );
        let CacheMode::Memory { slate_db, .. } = object_storage.cache().mode() else {
            panic!("object storage retains the general memory-cache profile");
        };
        assert_eq!(slate_db.block_bytes(), 512 * 1024 * 1024);
        assert_eq!(slate_db.metadata_bytes(), 128 * 1024 * 1024);
    }

    #[tokio::test]
    async fn cache_stats_are_publishable_without_cache_io() {
        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "cache-stats-contract".to_owned(),
        })
        .await
        .expect("open database");

        let snapshot = db.cache_stats();
        assert!(snapshot.is_publishable());
        assert_eq!(
            snapshot.foyer_hybrid_disk,
            CacheTierSnapshot::disabled(CacheUsageSemantics::AllocatedBlocks)
        );
        assert!(matches!(
            snapshot.slate_memory.state,
            CacheTierState::Ready {
                capacity_bytes: Some(25_165_824),
                ..
            }
        ));
        assert!(matches!(
            snapshot.fts_memory.state,
            CacheTierState::Ready {
                used_bytes: 0,
                capacity_bytes: Some(16_777_216),
            }
        ));
        assert!(matches!(
            snapshot.vector_memory.state,
            CacheTierState::Ready {
                used_bytes: 0,
                capacity_bytes: Some(67_108_864),
            }
        ));
        db.close().await.expect("close database");

        let root = tempfile::tempdir().expect("disk cache-stats root");
        let db = HelixDB::open(HelixDbSource::Disk {
            root: root.path().to_path_buf(),
            database: "disk-cache-stats-contract".to_string(),
        })
        .await
        .expect("open disk database");
        assert!(matches!(
            db.cache_stats().slate_memory.state,
            CacheTierState::Ready {
                capacity_bytes: Some(67_108_864),
                ..
            }
        ));
        db.close().await.expect("close disk database");
    }

    #[tokio::test]
    async fn embedded_query_entrypoints_emit_exactly_once() {
        use helix_metrics::query::{transport, InstallationId, OssIdentity};
        use helix_metrics::telemetry;

        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "embedded-query-metrics".to_owned(),
        })
        .await
        .expect("open database");
        let identity = OssIdentity::new(InstallationId::now(), None);
        let started =
            transport::start(telemetry::Source::Embedded, &identity, "http://127.0.0.1:9")
                .expect("test metrics config");
        let recorder = started.recorder.clone();
        db.set_embedded_query_metrics_for_tests(recorder.clone());

        let request = QueryRequest::read(
            read_batch()
                .var_as("count", g().n(NodeRef::id(999)).count())
                .returning(["count"]),
        );
        db.query(request.clone()).await.expect("query");
        db.query_scoped(request.clone(), DataScope::LegacyUnscoped)
            .await
            .expect("scoped query");
        let encoded = sonic_rs::to_vec(&request).expect("encode request");
        db.query_json(&encoded).await.expect("JSON query");
        db.query_json_scoped(&encoded, DataScope::LegacyUnscoped)
            .await
            .expect("scoped JSON query");

        assert_eq!(recorder.counters().emitted_events, 4);
        drop(started.runtime);
        db.close().await.expect("close database");
    }

    #[test]
    fn object_storage_source_is_constructed_without_io() {
        let (path, _store) = HelixDbSource::ObjectStorage {
            database: "facade-object-storage".into(),
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://127.0.0.1:9000".into()),
            allow_http: true,
        }
        .into_parts()
        .expect("object storage source builds without a request");
        assert_eq!(path, "facade-object-storage");
    }

    #[tokio::test]
    async fn default_cache_warm_tasks_are_owned_and_observable() {
        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "facade-default-cache-warm".to_string(),
        })
        .await
        .expect("writer opens");

        db.wait_for_startup_cache_warm().await;
        let fts = db.fts_cache_state().await.expect("FTS cache state");
        assert!(fts.enabled);
        assert_eq!(fts.resolved_memory_budget_bytes, 16 * 1024 * 1024);
        let slate = db.slate_cache_state().await;
        assert!(!slate.enabled);
        assert_eq!(slate.warm_mode, Some(config::CacheWarmMode::Off));
        assert!(slate.last_warm.is_none());
        assert_eq!(
            db.warm_fts_cache()
                .await
                .expect("empty FTS warm succeeds")
                .generation_count,
            0
        );

        db.close().await.expect("writer closes");
    }
    #[tokio::test]
    async fn process_local_token_routes_share_one_in_memory_store() {
        let token = ProcessLocalDatabaseToken::new("facade-process-local-token").unwrap();
        let expected_store = token.object_store();

        let writer = HelixDB::open(HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .unwrap();
        assert!(Arc::ptr_eq(writer.object_store(), &expected_store,));
        writer.close().await.unwrap();

        let reader = HelixDB::open_reader(HelixDbSource::InMemoryToken { token })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(reader.object_store(), &expected_store,));
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn reader_catalog_refresh_observes_remote_activation_and_drop_without_reopen() {
        let token = ProcessLocalDatabaseToken::new("facade-catalog-refresh").unwrap();
        let writer = HelixDB::open(HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .expect("writer opens");
        writer
            .inner_db()
            .flush()
            .await
            .expect("bootstrap becomes reader-visible");
        let reader = HelixDB::open_reader(HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .expect("reader opens before lifecycle changes");
        let scope = DataScope::LegacyUnscoped;
        let key = helix_planner::catalog::ScopedPropertyKey::try_new("User", "email")
            .expect("valid index key");
        let receipt = writer
            .enqueue_index_create(
                scope,
                &ir::IndexDdlCreateSpec::NodeEquality {
                    key: key.clone(),
                    uniqueness: helix_planner::catalog::IndexUniqueness::NonUnique,
                },
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("create is enqueued");
        let index_lifecycle::IndexDdlReceipt::Accepted {
            operation_id: create_operation,
            ..
        } = receipt
        else {
            panic!("new CREATE must return an accepted receipt");
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match writer
                    .get_index_operation(scope, create_operation)
                    .await
                    .expect("create status loads")
                {
                    index_lifecycle::IndexOperationStatus::Succeeded { .. } => break,
                    index_lifecycle::IndexOperationStatus::Blocked { .. }
                    | index_lifecycle::IndexOperationStatus::Aborted { .. } => {
                        panic!("empty secondary build must succeed")
                    }
                    index_lifecycle::IndexOperationStatus::Queued { .. }
                    | index_lifecycle::IndexOperationStatus::Running { .. } => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
        })
        .await
        .expect("create worker converges");
        writer
            .inner_db()
            .flush()
            .await
            .expect("activation becomes reader-visible");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match reader.refresh_runtime_catalog(scope).await {
                    Ok(())
                        if reader
                            .runtime_config_snapshot_loaded(scope)
                            .has_scoped_equality_index("User", "email") =>
                    {
                        break;
                    }
                    Ok(()) | Err(HelixDbError::RequestReadViewChanged) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("reader activation refresh failed: {error}"),
                }
            }
        })
        .await
        .expect("already-open reader observes activation");

        let receipt = writer
            .enqueue_index_drop(
                scope,
                &ir::IndexDdlDropSpec::NodeEquality {
                    key,
                    uniqueness: helix_planner::catalog::IndexUniqueness::NonUnique,
                },
            )
            .await
            .expect("drop is enqueued");
        let index_lifecycle::IndexDdlReceipt::Accepted {
            operation_id: drop_operation,
            ..
        } = receipt
        else {
            panic!("new DROP must return an accepted receipt");
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match writer
                    .get_index_operation(scope, drop_operation)
                    .await
                    .expect("drop status loads")
                {
                    index_lifecycle::IndexOperationStatus::Succeeded { .. } => break,
                    index_lifecycle::IndexOperationStatus::Blocked { .. }
                    | index_lifecycle::IndexOperationStatus::Aborted { .. } => {
                        panic!("unleased empty secondary drop must succeed")
                    }
                    index_lifecycle::IndexOperationStatus::Queued { .. }
                    | index_lifecycle::IndexOperationStatus::Running { .. } => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
        })
        .await
        .expect("drop worker converges");
        writer
            .inner_db()
            .flush()
            .await
            .expect("drop becomes reader-visible");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match reader.refresh_runtime_catalog(scope).await {
                    Ok(())
                        if !reader
                            .runtime_config_snapshot_loaded(scope)
                            .has_scoped_equality_index("User", "email") =>
                    {
                        break;
                    }
                    Ok(()) | Err(HelixDbError::RequestReadViewChanged) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("reader drop refresh failed: {error}"),
                }
            }
        })
        .await
        .expect("already-open reader observes drop");

        reader.close().await.expect("reader closes");
        writer.close().await.expect("writer closes");
    }
    #[test]
    fn vector_runtime_owns_configured_simhasher_limits() {
        let settings = config::VectorMemorySettings::default().with_simhasher_cache(
            config::SimHasherCacheSettings::try_new(3 * 64 * core::mem::size_of::<f32>(), 2)
                .unwrap(),
        );
        let cache = VectorMemoryCache::new(settings);
        assert!(cache.simhasher_registry.validate_dimension(3).is_ok());
        assert!(cache.simhasher_registry.validate_dimension(4).is_err());
    }

    #[tokio::test]
    async fn public_vector_refresh_hydrates_the_canonical_active_generation() {
        use bytes::Bytes;
        use slatedb::IsolationLevel;

        use crate::encoding::v1::keys::vectors::{VectorKey, VectorUpperVectorKey};
        use crate::encoding::v1::keys::{DataKeyKind, Key};
        use crate::encoding::v2::keys::ScopedKey;
        use crate::encoding::v2::values::encode_index_record;

        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "facade-canonical-vector-hydration".to_string(),
        })
        .await
        .expect("writer opens");
        let refresh_task = db
            .inner
            .caches
            .vector_memory
            .refresh_task
            .lock()
            .await
            .take()
            .expect("writer owns the refresh task");
        refresh_task.stop().await;

        let scope = DataScope::LegacyUnscoped;
        let definition = ValidatedDynamicIndexDefinition::try_from(
            config::VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                search::vector::VectorDistanceMetric::Euclidean,
            )
            .unwrap(),
        )
        .unwrap();
        let ValidatedDynamicIndexDefinition::Vector(vector) = &definition else {
            unreachable!("the fixture constructs a vector definition")
        };
        let descriptor = index_lifecycle::VectorGenerationDescriptor::for_definition(vector);
        let physical_index_id = index_lifecycle::VectorPhysicalIndexId::new(707).unwrap();
        let record = index_lifecycle::IndexRecordV2::building(
            index_lifecycle::IndexId::new(70).unwrap(),
            definition,
            index_lifecycle::IndexRevision::initial(),
            index_lifecycle::PhysicalGeneration::Vector {
                generation: index_lifecycle::IndexGenerationId::initial(),
                layout: index_lifecycle::VectorPhysicalLayout::Unpartitioned { physical_index_id },
                descriptor,
            },
            index_lifecycle::IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(index_lifecycle::IndexStateTransition::Activate)
        .unwrap();
        let active = index_lifecycle::ActiveIndexHandle::try_from_record(scope, &record).unwrap();
        let generation = search::vector::ValidatedVectorGenerationHandle::try_from_active_current(
            &active,
            physical_index_id,
        )
        .unwrap();
        let record_key = crate::encoding::v2::keys::Key::Data {
            scope,
            kind: ScopedKey::index_record(record.identity().clone()),
        }
        .to_bytes();
        let vector_key = Key::Data {
            scope,
            kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                physical_index_id.get(),
                7,
            ))),
        }
        .to_bytes();
        let HelixStorage::Writer(writer) = db.storage() else {
            unreachable!("the fixture opens writer storage")
        };
        let transaction = writer.db().begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(record_key, encode_index_record(&record))
            .unwrap();
        transaction
            .put(vector_key, Bytes::from_static(b"canonical"))
            .unwrap();
        transaction.commit().await.unwrap();

        db.refresh_vector_memory_cache().await.unwrap();

        let guard = db
            .vector_cache_registry()
            .read_guard_for(&generation)
            .unwrap();
        assert_eq!(
            guard.store().get_upper_vector(7).as_deref(),
            Some(b"canonical".as_slice())
        );
        drop(guard);
        db.close().await.unwrap();
    }

    #[test]
    fn runtime_state_tracks_loaded_catalogs_per_scope() {
        let scope = tenant_scope("0000000000000000000000000A");
        let mut state = HelixRuntimeState::new(
            DataScope::LegacyUnscoped,
            index_lifecycle::LoadedV2ScopeCatalog::new(DataScope::LegacyUnscoped),
        );
        assert_eq!(
            state.generation(DataScope::LegacyUnscoped),
            CatalogGeneration::INITIAL
        );
        state.replace_catalog(scope, index_lifecycle::LoadedV2ScopeCatalog::new(scope));
        assert_eq!(state.generation(scope), CatalogGeneration::INITIAL);
        state.replace_catalog(scope, index_lifecycle::LoadedV2ScopeCatalog::new(scope));
        assert_eq!(state.generation(scope), CatalogGeneration::INITIAL.next());
        let _ = state.planner_snapshot(scope);
        assert_eq!(
            state.loaded_scopes(),
            vec![DataScope::LegacyUnscoped, scope]
        );
    }

    #[test]
    fn runtime_catalog_replacement_removes_stale_dynamic_rows() {
        let scope = DataScope::LegacyUnscoped;
        let definition = ValidatedDynamicIndexDefinition::try_from(
            config::SecondaryIndexDefinition::node_equality("User", "email")
                .expect("dynamic index is valid"),
        )
        .expect("dynamic definition validates");
        let building = index_lifecycle::IndexRecordV2::building(
            index_lifecycle::IndexId::initial(),
            definition.clone(),
            index_lifecycle::IndexRevision::initial(),
            index_lifecycle::PhysicalGeneration::Secondary {
                generation: index_lifecycle::IndexGenerationId::initial(),
            },
            index_lifecycle::IndexOperationId::new_v4(),
        )
        .expect("building record is valid");
        let active = building
            .transition(index_lifecycle::IndexStateTransition::Activate)
            .expect("building record activates");
        let mut loaded = index_lifecycle::LoadedV2ScopeCatalog::new(scope);
        loaded
            .insert_active(&active)
            .expect("active row enters initial catalog");
        let mut state = HelixRuntimeState::new(scope, loaded);

        assert!(state
            .catalog(scope)
            .has_scoped_equality_index("User", "email"));
        state.replace_catalog(scope, index_lifecycle::LoadedV2ScopeCatalog::new(scope));
        assert!(!state
            .catalog(scope)
            .has_scoped_equality_index("User", "email"));
    }

    #[tokio::test]
    async fn writer_facade_exposes_storage_cache_and_scoped_runtime_contracts() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = HelixDB::open_with_object_store("facade-writer", Arc::clone(&store))
            .await
            .expect("writer opens");
        assert_eq!(db.mode(), HelixDbMode::Writer);
        assert_eq!(db.mode().as_str(), "writer");
        assert!(db.is_writer_mode());
        assert!(!db.is_reader_mode());
        assert_eq!(db.path(), "facade-writer");
        assert!(Arc::ptr_eq(db.object_store(), &store));
        let _ = db.config();
        let _ = db.index_catalog_snapshot();
        let _ = db.planner_context(ParamBindings::default());
        let scope = tenant_scope("0000000000000000000000000A");
        let _ = db
            .planner_context_scoped(ParamBindings::default(), scope)
            .await
            .expect("scoped planner context");
        assert_eq!(
            db.runtime_config_snapshot_loaded(scope)
                .vector_indexes()
                .count(),
            0
        );
        let _ = db.runtime_catalog_snapshot();
        let _ = db
            .runtime_catalog_snapshot_scoped(scope)
            .await
            .expect("scoped catalog");

        let HelixStorage::Writer(writer) = db.storage() else {
            panic!("writer handle expected");
        };
        let _ = writer.db();
        let first_node = writer.node_ids().allocate().await.expect("node id");
        let first_edge = writer.edge_ids().allocate().await.expect("edge id");
        assert_eq!(first_node, 0);
        assert_eq!(first_edge, 0);
        assert_eq!(
            db.inner_db().as_ref() as *const Db,
            writer.db() as *const Db
        );
        db.refresh_vector_memory_cache()
            .await
            .expect("empty vector refresh");

        let request = QueryRequest::read(
            read_batch()
                .var_as("nodes", g().n_with_label("Missing").count())
                .returning(["nodes"]),
        );
        assert_eq!(
            db.query(request.clone()).await.expect("query").get("nodes"),
            Some(&serde_json::json!(0))
        );
        assert_eq!(
            db.query_scoped(request, scope)
                .await
                .expect("scoped query")
                .get("nodes"),
            Some(&serde_json::json!(0))
        );
        assert!(db.query_json(b"not-json").await.is_err());
        db.close().await.expect("writer closes");
    }

    #[tokio::test]
    async fn concurrent_close_joins_one_writer_worker_and_is_idempotent() {
        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: "concurrent-close-worker".to_string(),
            })
            .await
            .expect("writer opens"),
        );
        assert!(db.inner.index_worker.lock().await.is_some());

        let (first, second, third) = tokio::join!(db.close(), db.close(), db.close());
        first.expect("first close succeeds");
        second.expect("concurrent close succeeds");
        third.expect("concurrent close succeeds");
        db.close().await.expect("later close remains idempotent");

        assert!(db.inner.index_worker.lock().await.is_none());
        assert!(matches!(
            *db.inner.close_state.lock().await,
            CloseState::Closed
        ));
    }

    #[tokio::test]
    async fn disabled_secondary_public_steps_survive_concurrency_close_and_reopen() {
        let token = ProcessLocalDatabaseToken::new("disabled-secondary-public-steps").unwrap();
        let disabled_config = || {
            DbConfig::new().with_secondary_index_lifecycle_tuning(
                config::SecondaryIndexLifecycleTuning::default()
                    .with_worker_mode(config::SecondaryIndexLifecycleWorkerMode::Disabled),
            )
        };
        let db = HelixDB::open_with_config(
            HelixDbSource::InMemoryToken {
                token: token.clone(),
            },
            disabled_config(),
        )
        .await
        .expect("Disabled-mode writer opens");
        assert!(!db
            .process_secondary_index_lifecycle_once()
            .await
            .expect("empty queue scan succeeds"));

        let scope = DataScope::LegacyUnscoped;
        let key = helix_planner::catalog::ScopedPropertyKey::try_new("User", "email")
            .expect("valid secondary key");
        let receipt = db
            .enqueue_index_create(
                scope,
                &ir::IndexDdlCreateSpec::NodeEquality {
                    key: key.clone(),
                    uniqueness: helix_planner::catalog::IndexUniqueness::NonUnique,
                },
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("CREATE is durably accepted");
        let index_lifecycle::IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("fresh CREATE must enqueue");
        };

        let (first, second) = tokio::join!(
            db.process_secondary_index_lifecycle_once(),
            db.process_secondary_index_lifecycle_once()
        );
        assert!(first.expect("first serialized step succeeds"));
        assert!(second.expect("second serialized step succeeds"));
        db.close().await.expect("partially built writer closes");

        let reopened = HelixDB::open_with_config(
            HelixDbSource::InMemoryToken {
                token: token.clone(),
            },
            disabled_config(),
        )
        .await
        .expect("queued build reopens");
        for _ in 0..16 {
            match reopened
                .get_index_operation(scope, operation_id)
                .await
                .expect("CREATE status remains readable")
            {
                index_lifecycle::IndexOperationStatus::Succeeded { .. } => break,
                index_lifecycle::IndexOperationStatus::Blocked { .. }
                | index_lifecycle::IndexOperationStatus::Aborted { .. } => {
                    panic!("empty CREATE must succeed")
                }
                index_lifecycle::IndexOperationStatus::Queued { .. }
                | index_lifecycle::IndexOperationStatus::Running { .. } => {
                    assert!(reopened
                        .process_secondary_index_lifecycle_once()
                        .await
                        .expect("reopened CREATE step succeeds"));
                }
            }
        }
        assert!(matches!(
            reopened
                .get_index_operation(scope, operation_id)
                .await
                .expect("terminal CREATE reads"),
            index_lifecycle::IndexOperationStatus::Succeeded { .. }
        ));

        let receipt = reopened
            .enqueue_index_drop(
                scope,
                &ir::IndexDdlDropSpec::NodeEquality {
                    key,
                    uniqueness: helix_planner::catalog::IndexUniqueness::NonUnique,
                },
            )
            .await
            .expect("DROP is durably accepted");
        let index_lifecycle::IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("fresh DROP must enqueue");
        };
        for _ in 0..16 {
            match reopened
                .get_index_operation(scope, operation_id)
                .await
                .expect("DROP status remains readable")
            {
                index_lifecycle::IndexOperationStatus::Succeeded { .. } => break,
                index_lifecycle::IndexOperationStatus::Blocked { .. }
                | index_lifecycle::IndexOperationStatus::Aborted { .. } => {
                    panic!("empty DROP must succeed")
                }
                index_lifecycle::IndexOperationStatus::Queued { .. }
                | index_lifecycle::IndexOperationStatus::Running { .. } => {
                    assert!(reopened
                        .process_secondary_index_lifecycle_once()
                        .await
                        .expect("DROP step succeeds"));
                }
            }
        }
        assert!(matches!(
            reopened
                .get_index_operation(scope, operation_id)
                .await
                .expect("terminal DROP reads"),
            index_lifecycle::IndexOperationStatus::Succeeded { .. }
        ));
        assert!(!reopened
            .process_secondary_index_lifecycle_once()
            .await
            .expect("drained queue scan succeeds"));
        reopened.close().await.expect("reopened writer closes");

        let reader = HelixDB::open_reader(HelixDbSource::InMemoryToken { token })
            .await
            .expect("reader opens");
        assert!(matches!(
            reader.process_secondary_index_lifecycle_once().await,
            Err(HelixDbError::WriterModeRequired { .. })
        ));
        reader.close().await.expect("reader closes");

        let enabled = HelixDB::open(HelixDbSource::InMemory {
            database: "enabled-secondary-public-step-rejection".to_string(),
        })
        .await
        .expect("Enabled-mode writer opens");
        assert!(matches!(
            enabled.process_secondary_index_lifecycle_once().await,
            Err(HelixDbError::SecondaryLifecycleSteppingRequiresDisabledMode)
        ));
        enabled.close().await.expect("Enabled-mode writer closes");
    }

    #[tokio::test]
    async fn lifecycle_control_facade_and_dsl_use_the_exact_request_scope() {
        use crate::encoding::v1::keys::{DataKeyKind, Key, NodePropertyKey};

        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "lifecycle-control-facade".to_string(),
        })
        .await
        .expect("writer opens");
        let scope = DataScope::LegacyUnscoped;
        let definition = ValidatedDynamicIndexDefinition::try_from(
            config::SecondaryIndexDefinition::node_equality("User", "email")
                .expect("secondary definition"),
        )
        .expect("validated secondary definition");
        let cursor = index_lifecycle::IndexCursor::try_new(
            Key::Data {
                scope,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(42)),
            }
            .to_bytes(),
        )
        .expect("typed source cursor");
        let HelixStorage::Writer(writer) = db.storage() else {
            panic!("writer handle expected");
        };
        let receipt = index_lifecycle::lifecycle::create_index_operation(
            writer.db(),
            scope,
            definition,
            ir::IndexCreateMode::ErrorIfExists,
            index_lifecycle::lifecycle::InitialBuildProgress::secondary(cursor),
        )
        .await
        .expect("build operation is accepted");
        let index_lifecycle::IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("new operation must return accepted receipt");
        };
        let operation_id_string = operation_id.as_uuid().to_string();

        assert!(matches!(
            db.get_index_operation(scope, operation_id)
                .await
                .expect("direct status lookup"),
            index_lifecycle::IndexOperationStatus::Queued { .. }
        ));
        let get_request = QueryRequest::read(
            read_batch()
                .var_as(
                    "status",
                    g().get_index_operation(operation_id_string.clone()),
                )
                .returning(["status"]),
        );
        let get_result = db.query(get_request).await.expect("DSL status lookup");
        assert_eq!(get_result["status"]["status"], "queued");
        assert_eq!(get_result["status"]["operation_id"], operation_id_string);

        let abort_request = QueryRequest::write(
            write_batch()
                .var_as(
                    "status",
                    g().abort_index_operation(operation_id_string.clone()),
                )
                .returning(["status"]),
        );
        let abort_result = db.query(abort_request).await.expect("DSL abort");
        assert_eq!(abort_result["status"]["status"], "queued");
        assert_eq!(abort_result["status"]["stage"], "aborting_delete_entries");

        let retry_request = QueryRequest::write(
            write_batch()
                .var_as(
                    "status",
                    g().retry_index_operation(operation_id_string.clone()),
                )
                .returning(["status"]),
        );
        let retry_result = db
            .query(retry_request)
            .await
            .expect("DSL retry convergence");
        assert_eq!(retry_result, abort_result);

        let wrong_scope = tenant_scope("0000000000000000000000000A");
        assert!(matches!(
            db.get_index_operation(wrong_scope, operation_id).await,
            Err(HelixDbError::IndexOperationNotFound { .. })
        ));
        assert!(matches!(
            db.retry_index_operation(wrong_scope, operation_id).await,
            Err(HelixDbError::IndexOperationNotFound { .. })
        ));
        assert!(matches!(
            db.abort_index_operation(wrong_scope, operation_id).await,
            Err(HelixDbError::IndexOperationNotFound { .. })
        ));
        db.close().await.expect("writer closes");
    }

    #[tokio::test]
    async fn disk_source_reopens_as_reader() {
        use crate::encoding::v1::keys::{DataKeyKind, Key, NodePropertyKey};

        let root = tempfile::tempdir().expect("disk root");
        let database = "facade-disk".to_string();
        let writer = HelixDB::open(HelixDbSource::Disk {
            root: root.path().to_path_buf(),
            database: database.clone(),
        })
        .await
        .expect("disk writer opens");
        let scope = DataScope::LegacyUnscoped;
        let definition = ValidatedDynamicIndexDefinition::try_from(
            config::SecondaryIndexDefinition::node_equality("User", "email")
                .expect("secondary definition"),
        )
        .expect("validated secondary definition");
        let cursor = index_lifecycle::IndexCursor::try_new(
            Key::Data {
                scope,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(42)),
            }
            .to_bytes(),
        )
        .expect("typed source cursor");
        let HelixStorage::Writer(storage) = writer.storage() else {
            panic!("writer handle expected");
        };
        let receipt = index_lifecycle::lifecycle::create_index_operation(
            storage.db(),
            scope,
            definition,
            ir::IndexCreateMode::ErrorIfExists,
            index_lifecycle::lifecycle::InitialBuildProgress::secondary(cursor),
        )
        .await
        .expect("reader fixture operation is accepted");
        let index_lifecycle::IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("new operation must return accepted receipt");
        };
        writer.close().await.expect("disk writer closes");

        let reader = HelixDB::open_reader(HelixDbSource::Disk {
            root: root.path().to_path_buf(),
            database,
        })
        .await
        .expect("disk reader opens");
        assert_eq!(reader.mode(), HelixDbMode::ReadOnly);
        assert_eq!(reader.mode().as_str(), "reader");
        assert!(reader.is_reader_mode());
        assert!(!reader.is_writer_mode());
        assert_eq!(
            reader.index_runtime_readiness(),
            IndexRuntimeReadiness::Ready
        );
        let HelixStorage::Reader(_) = reader.storage() else {
            panic!("reader handle expected");
        };
        assert!(matches!(
            reader
                .get_index_operation(scope, operation_id)
                .await
                .expect("reader can point-read lifecycle status"),
            index_lifecycle::IndexOperationStatus::Queued { .. }
        ));
        assert!(matches!(
            reader.retry_index_operation(scope, operation_id).await,
            Err(HelixDbError::WriterModeRequired { .. })
        ));
        assert!(matches!(
            reader.abort_index_operation(scope, operation_id).await,
            Err(HelixDbError::WriterModeRequired { .. })
        ));
        reader
            .refresh_vector_memory_cache()
            .await
            .expect("reader vector refresh");
        reader.close().await.expect("reader closes");
    }

    #[tokio::test]
    async fn disk_and_shared_object_store_run_core_index_lifecycles_without_dependencies() {
        async fn exercise(db: HelixDB) {
            assert_eq!(db.index_runtime_readiness(), IndexRuntimeReadiness::Ready);
            let definitions = [
                ValidatedDynamicIndexDefinition::try_from(
                    config::SecondaryIndexDefinition::node_equality("CoreIndex", "value")
                        .expect("core secondary definition validates"),
                )
                .expect("core secondary definition converts"),
                ValidatedDynamicIndexDefinition::try_from(
                    config::VectorIndexDefinition::new_node(
                        "CoreVector",
                        "value",
                        3,
                        search::vector::VectorDistanceMetric::Cosine,
                    )
                    .expect("core vector definition validates"),
                )
                .expect("core vector definition converts"),
                ValidatedDynamicIndexDefinition::try_from(
                    config::TextIndexDefinition::new_node("CoreText", "value")
                        .expect("core text definition validates"),
                )
                .expect("core text definition converts"),
            ];
            for definition in definitions {
                db.install_index_for_tests(definition.clone())
                    .await
                    .expect("core index activates without runtime dependencies");
                let before = index_lifecycle::repository::load_index_record(
                    db.inner_db().as_ref(),
                    DataScope::LegacyUnscoped,
                    &definition.identity(),
                )
                .await
                .expect("active core index decodes")
                .expect("active core index exists");
                let receipt = index_lifecycle::lifecycle::drop_index_operation(
                    db.inner_db().as_ref(),
                    DataScope::LegacyUnscoped,
                    &definition,
                )
                .await
                .expect("core index DROP enqueues without runtime dependencies");
                let index_lifecycle::IndexDdlReceipt::Accepted { operation_id, .. } = receipt
                else {
                    panic!("first core index DROP must be accepted");
                };
                db.wake_index_worker().await;
                tokio::time::timeout(Duration::from_secs(30), async {
                    loop {
                        match db
                            .get_index_operation(DataScope::LegacyUnscoped, operation_id)
                            .await
                            .expect("core index DROP status loads")
                        {
                            index_lifecycle::IndexOperationStatus::Succeeded { .. } => break,
                            index_lifecycle::IndexOperationStatus::Blocked { .. }
                            | index_lifecycle::IndexOperationStatus::Aborted { .. } => {
                                panic!("core index DROP must succeed")
                            }
                            index_lifecycle::IndexOperationStatus::Queued { .. }
                            | index_lifecycle::IndexOperationStatus::Running { .. } => {
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                })
                .await
                .expect("core index DROP converges");
                db.install_index_for_tests(definition.clone())
                    .await
                    .expect("core index recreates without runtime dependencies");
                let recreated = index_lifecycle::repository::load_index_record(
                    db.inner_db().as_ref(),
                    DataScope::LegacyUnscoped,
                    &definition.identity(),
                )
                .await
                .expect("recreated core index decodes")
                .expect("recreated core index exists");
                assert_eq!(
                    recreated.state().generation().get(),
                    before.state().generation().get() + 1
                );
            }
            db.close().await.expect("core lifecycle database closes");
        }

        let disk_root = tempfile::tempdir().expect("core lifecycle disk root");
        exercise(
            HelixDB::open(HelixDbSource::Disk {
                root: disk_root.path().to_path_buf(),
                database: "core-index-lifecycle-disk".to_string(),
            })
            .await
            .expect("disk database opens without runtime dependencies"),
        )
        .await;
        exercise(
            HelixDB::open_with_object_store(
                "core-index-lifecycle-object-store",
                Arc::new(InMemory::new()),
            )
            .await
            .expect("shared object-store database opens without runtime dependencies"),
        )
        .await;
    }

    #[tokio::test]
    async fn query_json_scoped_isolates_requests_by_data_scope() {
        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "query-json-scoped-isolation".to_string(),
        })
        .await
        .expect("db opens");
        let scope_a = tenant_scope("0000000000000000000000000A");
        let scope_b = tenant_scope("0000000000000000000000000B");
        let write = QueryRequest::write(
            write_batch()
                .var_as(
                    "created",
                    g().add_n("User", vec![("name", PropertyInput::from("Ada"))])
                        .count(),
                )
                .returning(["created"]),
        )
        .to_json_bytes()
        .expect("write request should serialize");
        let read = QueryRequest::read(
            read_batch()
                .var_as("users", g().n_with_label("User").count())
                .returning(["users"]),
        )
        .to_json_bytes()
        .expect("read request should serialize");

        db.query_json_scoped(&write, scope_a)
            .await
            .expect("tenant a write succeeds");
        db.query_json_scoped(&write, scope_b)
            .await
            .expect("tenant b write succeeds");
        db.query_json_scoped(&write, scope_b)
            .await
            .expect("tenant b second write succeeds");

        let tenant_a_json: serde_json::Value = sonic_rs::from_slice(
            &db.query_json_scoped(&read, scope_a)
                .await
                .expect("tenant a read succeeds"),
        )
        .expect("tenant a response decodes");
        let tenant_b_json: serde_json::Value = sonic_rs::from_slice(
            &db.query_json_scoped(&read, scope_b)
                .await
                .expect("tenant b read succeeds"),
        )
        .expect("tenant b response decodes");
        let legacy_json: serde_json::Value =
            sonic_rs::from_slice(&db.query_json(&read).await.expect("legacy read succeeds"))
                .expect("legacy response decodes");

        assert_eq!(tenant_a_json.get("users"), Some(&serde_json::json!(1)));
        assert_eq!(tenant_b_json.get("users"), Some(&serde_json::json!(2)));
        assert_eq!(legacy_json.get("users"), Some(&serde_json::json!(0)));
    }
}
