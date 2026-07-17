#![recursion_limit = "256"]

//! Helix database runtime, storage, query execution, and index lifecycle API.

pub mod config;
pub mod encoding;
pub mod error;
pub mod execution;
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
pub mod id_allocator;
pub mod index_v2;
pub mod query_service;
mod runtime_dependencies;
pub mod search;

pub use runtime_dependencies::{
    DatabaseAccessTopology, HelixRuntimeDependencies, ProcessLocalDatabaseToken,
    SharedBlobPublicationMode, SharedReaderLeaseMode,
};

#[cfg(feature = "production-coverage")]
#[path = "../tests/production_support/mod.rs"]
pub mod production_coverage;

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub use config::{DbConfig, HelixConfig, IndexConfig};

#[cfg(test)]
use config::ValidatedDynamicIndexDefinition;
use config::{runtime_catalog, CacheMode, RuntimeIndexCatalog};
use error::{HelixDbError, Result};
use execution::interpreter::{ExecutionResult, Interpreter};
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
};
use helix_ast::query::QueryRequest;
use helix_planner::{
    catalog::IndexCatalogSnapshot,
    context::{ParamBindings, PlannerContext},
    exec, ir,
};
use id_allocator::{EdgeIdAllocator, NodeIdAllocator};
use serde_json::Value as JsonValue;
use slatedb::db_cache::{foyer_hybrid::FoyerHybridCache, CachedEntry, DbCache};
#[cfg(test)]
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};
use slatedb::{Db, DbReader};
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
        /// Non-forgeable object-store and coordinator identity.
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
    fn into_parts(self) -> Result<(String, Arc<dyn ObjectStore>, HelixRuntimeDependencies)> {
        match self {
            Self::InMemory { database } => {
                let token = ProcessLocalDatabaseToken::new(database)?;
                Ok((
                    token.database().to_string(),
                    token.object_store(),
                    token.runtime_dependencies(),
                ))
            }
            Self::InMemoryToken { token } => Ok((
                token.database().to_string(),
                token.object_store(),
                token.runtime_dependencies(),
            )),
            Self::Disk { root, database } => {
                let object_store: Arc<dyn ObjectStore> =
                    Arc::new(LocalFileSystem::new_with_prefix(root)?);
                Ok((
                    database,
                    object_store,
                    HelixRuntimeDependencies::shared_unavailable(),
                ))
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
                Ok((
                    database,
                    Arc::new(builder.build()?),
                    HelixRuntimeDependencies::shared_unavailable(),
                ))
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

struct HelixCaches {
    slate_db: Option<Arc<dyn DbCache>>,
    vector_memory: VectorMemoryCache,
}

struct HelixRuntimeState {
    catalogs: HashMap<DataScope, Box<index_v2::LoadedV2ScopeCatalog>>,
}

impl HelixRuntimeState {
    fn new(scope: DataScope, catalog: index_v2::LoadedV2ScopeCatalog) -> Self {
        assert_eq!(
            scope,
            catalog.scope(),
            "loaded catalog scope must match its map key"
        );
        let mut catalogs = HashMap::new();
        catalogs.insert(scope, Box::new(catalog));
        Self { catalogs }
    }

    /// Replaces one scope with a fresh configured-plus-canonical projection.
    fn replace_catalog(&mut self, scope: DataScope, catalog: index_v2::LoadedV2ScopeCatalog) {
        assert_eq!(
            scope,
            catalog.scope(),
            "loaded catalog scope must match its map key"
        );
        self.catalogs.insert(scope, Box::new(catalog));
    }

    fn catalog(&self, scope: DataScope) -> RuntimeIndexCatalog {
        self.catalogs
            .get(&scope)
            .map(|catalog| catalog.runtime())
            .cloned()
            .expect("scoped catalog must be loaded before runtime access")
    }

    fn planner_snapshot(&self, scope: DataScope) -> IndexCatalogSnapshot {
        self.catalog(scope).planner_snapshot()
    }

    fn configured_catalog_for_load(&self, scope: DataScope) -> RuntimeIndexCatalog {
        self.catalogs
            .get(&scope)
            .map(|catalog| catalog.configured().runtime().clone())
            .unwrap_or_default()
    }

    /// Returns configured-only authority for retained static index maintenance.
    fn configured_catalog(&self, scope: DataScope) -> index_v2::ConfiguredIndexCatalog {
        self.catalogs
            .get(&scope)
            .map(|catalog| catalog.configured().clone())
            .expect("scoped catalog must be loaded before configured maintenance")
    }

    fn active_handles(&self, scope: DataScope) -> Vec<index_v2::ActiveIndexHandle> {
        let Some(catalog) = self.catalogs.get(&scope) else {
            return Vec::new();
        };
        catalog.active_handles().cloned().collect()
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
    blob_gc_gate: search::text::BlobGcGate,
    active_text_mutations: index_v2::text::active_mutation::ActiveTextMutationRegistry,
    blob_publication_coordinator:
        Option<Arc<dyn index_v2::blob_publication::BlobPublicationCoordinator>>,
    reader_lease_coordinator: Option<Arc<dyn index_v2::reader_lease::IndexLeaseCoordinator>>,
    reader_lease_holder: index_v2::reader_lease::LeaseHolderId,
    index_scope_gates: Arc<index_v2::IndexScopeGates>,
    config: HelixConfig,
    runtime_state: RwLock<HelixRuntimeState>,
    index_capabilities: index_v2::worker::IndexFamilyCapabilities,
    index_worker: Mutex<Option<index_v2::worker::IndexWorkerSupervisor>>,
    close_state: Mutex<CloseState>,
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
        Self::open_with_config(source, DbConfig::new()).await
    }

    /// Open a read/write database handle backed by a caller-provided object store.
    pub async fn open_with_object_store(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_writer_inner(
            database.into(),
            object_store,
            DbConfig::new(),
            HelixRuntimeDependencies::shared_unavailable(),
        )
        .await
    }

    /// Open a read/write database handle backed by a caller-provided object store and config.
    pub async fn open_with_object_store_and_config(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
    ) -> Result<Self> {
        Self::open_writer_inner(
            database.into(),
            object_store,
            config,
            HelixRuntimeDependencies::shared_unavailable(),
        )
        .await
    }

    /// Opens a caller-provided shared object store with trusted runtime adapters.
    pub async fn open_with_object_store_and_runtime_dependencies(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
        runtime_dependencies: HelixRuntimeDependencies,
    ) -> Result<Self> {
        runtime_dependencies.validate_for_topology(DatabaseAccessTopology::shared())?;
        Self::open_writer_inner(database.into(), object_store, config, runtime_dependencies).await
    }

    /// Open a read/write database handle with explicit tuning config.
    pub async fn open_with_config(source: HelixDbSource, config: DbConfig) -> Result<Self> {
        let (path, object_store, runtime_dependencies) = source.into_parts()?;
        Self::open_writer_inner(path, object_store, config, runtime_dependencies).await
    }

    /// Opens a shared disk/object-storage source with trusted runtime adapters.
    ///
    /// Reusable process-local opens must use [`HelixDbSource::InMemoryToken`],
    /// whose token owns the only valid coordinator instances for its store.
    pub async fn open_with_runtime_dependencies(
        source: HelixDbSource,
        config: DbConfig,
        runtime_dependencies: HelixRuntimeDependencies,
    ) -> Result<Self> {
        let (path, object_store, source_dependencies) = source.into_parts()?;
        if source_dependencies.topology().is_process_local() {
            return Err(HelixDbError::Config(
                "process-local sources must use the coordinators owned by their token".to_string(),
            ));
        }
        runtime_dependencies.validate_for_topology(source_dependencies.topology())?;
        Self::open_writer_inner(path, object_store, config, runtime_dependencies).await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_object_store_and_index_config_for_tests(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        indexes: IndexConfig,
    ) -> Result<Self> {
        Self::open_with_object_store_and_index_config_for_tests_inner(
            database,
            object_store,
            indexes,
            HelixRuntimeDependencies::shared_unavailable(),
        )
        .await
    }

    /// Opens a process-local test database from its non-forgeable identity.
    #[cfg(test)]
    pub(crate) async fn open_with_process_local_token_and_index_config_for_tests(
        token: ProcessLocalDatabaseToken,
        indexes: IndexConfig,
    ) -> Result<Self> {
        Self::open_with_object_store_and_index_config_for_tests_inner(
            token.database().to_string(),
            token.object_store(),
            indexes,
            token.runtime_dependencies(),
        )
        .await
    }

    #[cfg(test)]
    async fn open_with_object_store_and_index_config_for_tests_inner(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        indexes: IndexConfig,
        runtime_dependencies: HelixRuntimeDependencies,
    ) -> Result<Self> {
        runtime_dependencies.validate_for_topology(runtime_dependencies.topology())?;
        let path = database.into();
        let config = DbConfig::new();
        let db = Arc::new(
            Db::builder(path.clone(), Arc::clone(&object_store))
                .with_settings(
                    config
                        .slate()
                        .to_writer_settings(config.cache().object_store_cache()),
                )
                .build()
                .await?,
        );
        index_v2::repository::bootstrap_writer(&db).await?;
        index_v2::outbox::reconcile_operation_queue(&db).await?;
        index_v2::reconciliation::reconcile_blob_gc(&db).await?;
        let loaded_catalog = index_v2::repository::load_scope_catalog(
            db.as_ref(),
            DataScope::LegacyUnscoped,
            indexes,
        )
        .await?;
        let writer = HelixWriter::new(Arc::clone(&db), config.id_lease_size());
        let storage = HelixStorage::Writer(Arc::new(writer));
        Ok(Self::from_storage(
            HelixStorageParts::new(path, object_store, storage),
            HelixConfig::new(config),
            loaded_catalog,
            None,
            runtime_dependencies,
        ))
    }

    /// Opens writer storage and starts disposable cache warming.
    async fn open_writer_inner(
        path: String,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
        runtime_dependencies: HelixRuntimeDependencies,
    ) -> Result<Self> {
        runtime_dependencies.validate_for_topology(runtime_dependencies.topology())?;
        let vector_memory_settings = *config.cache().vector_memory();
        let slate_db_cache = build_slate_db_cache(config.cache().mode()).await?;
        let mut builder = Db::builder(path.clone(), Arc::clone(&object_store)).with_settings(
            config
                .slate()
                .to_writer_settings(config.cache().object_store_cache()),
        );

        match config.cache().mode() {
            CacheMode::VectorMemoryOnly => builder = builder.with_db_cache_disabled(),
            CacheMode::Memory => {}
            CacheMode::Hybrid { .. } => {
                let Some(cache) = &slate_db_cache else {
                    return Err(HelixDbError::Config(
                        "hybrid cache mode must build a SlateDB cache".into(),
                    ));
                };
                builder = builder.with_db_cache(Arc::clone(cache));
            }
        }
        let db = Arc::new(builder.build().await?);
        index_v2::repository::bootstrap_writer(&db).await?;
        index_v2::outbox::reconcile_operation_queue(&db).await?;
        index_v2::reconciliation::reconcile_blob_gc(&db).await?;
        let loaded_catalog = index_v2::repository::load_scope_catalog(
            db.as_ref(),
            DataScope::LegacyUnscoped,
            IndexConfig::new(),
        )
        .await?;
        register_loaded_index_generations(&loaded_catalog, &runtime_dependencies).await?;
        let writer = HelixWriter::new(Arc::clone(&db), config.id_lease_size());
        let storage = HelixStorage::Writer(Arc::new(writer));
        let db = Self::from_storage(
            HelixStorageParts::new(path, object_store, storage),
            HelixConfig::new(config),
            loaded_catalog,
            slate_db_cache,
            runtime_dependencies,
        );
        db.run_configured_vector_memory_warm(vector_memory_settings)
            .await?;
        Ok(db)
    }

    /// Open a read-only database handle.
    pub async fn open_reader(source: HelixDbSource) -> Result<Self> {
        Self::open_reader_with_config(source, DbConfig::new()).await
    }

    /// Open a read-only database handle with explicit tuning config.
    pub async fn open_reader_with_config(source: HelixDbSource, config: DbConfig) -> Result<Self> {
        let (path, object_store, runtime_dependencies) = source.into_parts()?;
        Self::open_reader_inner(path, object_store, config, runtime_dependencies).await
    }

    /// Opens a shared reader source with trusted runtime adapters.
    pub async fn open_reader_with_runtime_dependencies(
        source: HelixDbSource,
        config: DbConfig,
        runtime_dependencies: HelixRuntimeDependencies,
    ) -> Result<Self> {
        let (path, object_store, source_dependencies) = source.into_parts()?;
        if source_dependencies.topology().is_process_local() {
            return Err(HelixDbError::Config(
                "process-local readers must use the coordinators owned by their token".to_string(),
            ));
        }
        runtime_dependencies.validate_for_topology(source_dependencies.topology())?;
        Self::open_reader_inner(path, object_store, config, runtime_dependencies).await
    }

    /// Opens a reader over a caller-provided shared store and trusted adapters.
    pub async fn open_reader_with_object_store_and_runtime_dependencies(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
        runtime_dependencies: HelixRuntimeDependencies,
    ) -> Result<Self> {
        runtime_dependencies.validate_for_topology(DatabaseAccessTopology::shared())?;
        Self::open_reader_inner(database.into(), object_store, config, runtime_dependencies).await
    }

    #[cfg(test)]
    pub(crate) async fn open_reader_with_object_store_and_index_config_for_tests(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        indexes: IndexConfig,
    ) -> Result<Self> {
        Self::open_reader_with_object_store_and_index_config_for_tests_inner(
            database,
            object_store,
            indexes,
            HelixRuntimeDependencies::shared_unavailable(),
        )
        .await
    }

    #[cfg(test)]
    async fn open_reader_with_object_store_and_index_config_for_tests_inner(
        database: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        indexes: IndexConfig,
        runtime_dependencies: HelixRuntimeDependencies,
    ) -> Result<Self> {
        runtime_dependencies.validate_for_topology(runtime_dependencies.topology())?;
        let path = database.into();
        let config = DbConfig::new();
        let reader = DbReader::builder(path.clone(), Arc::clone(&object_store))
            .with_options(
                config
                    .slate()
                    .to_reader_options(config.cache().object_store_cache()),
            )
            .build()
            .await?;
        index_v2::repository::require_reader_bootstrap(&reader).await?;
        let loaded_catalog =
            index_v2::repository::load_scope_catalog(&reader, DataScope::LegacyUnscoped, indexes)
                .await?;
        let storage = HelixStorage::Reader(Arc::new(reader));
        Ok(Self::from_storage(
            HelixStorageParts::new(path, object_store, storage),
            HelixConfig::new(config),
            loaded_catalog,
            None,
            runtime_dependencies,
        ))
    }

    async fn open_reader_inner(
        path: String,
        object_store: Arc<dyn ObjectStore>,
        config: DbConfig,
        runtime_dependencies: HelixRuntimeDependencies,
    ) -> Result<Self> {
        runtime_dependencies.validate_for_topology(runtime_dependencies.topology())?;
        let vector_memory_settings = *config.cache().vector_memory();
        let slate_db_cache = build_slate_db_cache(config.cache().mode()).await?;
        let mut builder = DbReader::builder(path.clone(), Arc::clone(&object_store)).with_options(
            config
                .slate()
                .to_reader_options(config.cache().object_store_cache()),
        );
        match config.cache().mode() {
            CacheMode::VectorMemoryOnly => builder = builder.with_db_cache_disabled(),
            CacheMode::Memory => {}
            CacheMode::Hybrid { .. } => {
                let Some(cache) = &slate_db_cache else {
                    return Err(HelixDbError::Config(
                        "hybrid cache mode must build a SlateDB cache".into(),
                    ));
                };
                builder = builder.with_db_cache(Arc::clone(cache));
            }
        }
        let reader = builder.build().await?;
        index_v2::repository::require_reader_bootstrap(&reader).await?;
        let loaded_catalog = index_v2::repository::load_scope_catalog(
            &reader,
            DataScope::LegacyUnscoped,
            IndexConfig::new(),
        )
        .await?;
        register_loaded_index_generations(&loaded_catalog, &runtime_dependencies).await?;
        let storage = HelixStorage::Reader(Arc::new(reader));
        let db = Self::from_storage(
            HelixStorageParts::new(path, object_store, storage),
            HelixConfig::new(config),
            loaded_catalog,
            slate_db_cache,
            runtime_dependencies,
        );
        db.run_configured_vector_memory_warm(vector_memory_settings)
            .await?;
        Ok(db)
    }

    fn from_storage(
        storage: HelixStorageParts,
        config: HelixConfig,
        indexes: index_v2::LoadedV2ScopeCatalog,
        slate_db_cache: Option<Arc<dyn DbCache>>,
        runtime_dependencies: HelixRuntimeDependencies,
    ) -> Self {
        let vector_memory = VectorMemoryCache::new(*config.db().cache().vector_memory());
        let index_scope_gates = Arc::new(index_v2::IndexScopeGates::default());
        let secondary_tuning = config.db().secondary_backfill();
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
        let reader_lease_coordinator = runtime_dependencies.reader_lease_coordinator();
        let blob_publication_coordinator = runtime_dependencies.blob_publication_coordinator();
        let secondary_driver: Arc<dyn index_v2::outbox::IndexOperationDriver> = Arc::new(
            index_v2::secondary::SecondaryIndexDriver::with_reader_leases(
                Arc::clone(&index_scope_gates),
                reader_lease_coordinator.clone(),
            ),
        );
        let vector_driver: Arc<dyn index_v2::outbox::IndexOperationDriver> =
            Arc::new(index_v2::vector::VectorIndexDriver::with_reader_leases(
                Arc::clone(&index_scope_gates),
                Arc::clone(&vector_memory.registry),
                Arc::clone(&vector_memory.simhasher_registry),
                reader_lease_coordinator.clone(),
            ));
        let secondary_scheduling = match secondary_tuning.worker_mode() {
            config::SecondaryBackfillWorkerMode::Enabled => {
                index_v2::worker::IndexDriverScheduling::Automatic
            }
            config::SecondaryBackfillWorkerMode::Disabled => {
                index_v2::worker::IndexDriverScheduling::ExplicitOnly
            }
        };
        let blob_gc_gate = search::text::BlobGcGate::new();
        let text_capability = match &blob_publication_coordinator {
            Some(coordinator) => {
                let driver = Arc::new(
                    index_v2::text::driver::TextIndexDriver::with_lifecycle_runtime(
                        Arc::clone(&index_scope_gates),
                        Arc::clone(coordinator),
                        Arc::clone(storage.object_store()),
                        storage.path().to_string(),
                        config.db().search_index_backfill().text_compaction(),
                        blob_gc_gate.clone(),
                        reader_lease_coordinator.clone(),
                    ),
                );
                let upload_driver = Arc::new(
                    index_v2::text::reconciliation::CoordinatorTextUploadDriver::new(Arc::clone(
                        coordinator,
                    )),
                );
                let gc_driver = Arc::new(index_v2::text::blob_gc::TextBlobGcDriver::new(
                    Arc::clone(coordinator),
                    blob_gc_gate.clone(),
                ));
                if reader_lease_coordinator.is_some() {
                    index_v2::worker::TextIndexCapability::FullyReady {
                        driver,
                        upload_driver,
                        gc_driver,
                        limits: config.db().search_index_backfill().batch(),
                        scheduling: index_v2::worker::IndexDriverScheduling::Automatic,
                    }
                } else {
                    index_v2::worker::TextIndexCapability::DriverReady {
                        driver,
                        upload_driver,
                        gc_driver,
                        limits: config.db().search_index_backfill().batch(),
                        scheduling: index_v2::worker::IndexDriverScheduling::Automatic,
                    }
                }
            }
            None => index_v2::worker::TextIndexCapability::Unavailable,
        };
        let secondary_capability = if reader_lease_coordinator.is_some() {
            index_v2::worker::IndexFamilyCapability::FullyReady {
                driver: secondary_driver,
                limits: secondary_limits,
                scheduling: secondary_scheduling,
            }
        } else {
            index_v2::worker::IndexFamilyCapability::DriverReady {
                driver: secondary_driver,
                limits: secondary_limits,
                scheduling: secondary_scheduling,
            }
        };
        let vector_capability = if reader_lease_coordinator.is_some() {
            index_v2::worker::IndexFamilyCapability::FullyReady {
                driver: vector_driver,
                limits: config.db().search_index_backfill().batch(),
                scheduling: index_v2::worker::IndexDriverScheduling::Automatic,
            }
        } else {
            index_v2::worker::IndexFamilyCapability::DriverReady {
                driver: vector_driver,
                limits: config.db().search_index_backfill().batch(),
                scheduling: index_v2::worker::IndexDriverScheduling::Automatic,
            }
        };
        let index_capabilities = index_v2::worker::IndexFamilyCapabilities::new(
            secondary_capability,
            vector_capability,
            text_capability,
        );
        let active_text_mutations =
            index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let index_worker = match storage.handle() {
            HelixStorage::Writer(writer) => Some(index_v2::worker::IndexWorkerSupervisor::start(
                Arc::clone(&writer.db),
                index_capabilities.clone(),
                active_text_mutations.clone(),
            )),
            HelixStorage::Reader(_) => None,
        };
        Self {
            inner: Arc::new(HelixDBInner {
                storage,
                caches: HelixCaches {
                    slate_db: slate_db_cache,
                    vector_memory,
                },
                blob_gc_gate,
                active_text_mutations,
                blob_publication_coordinator,
                reader_lease_coordinator,
                reader_lease_holder: index_v2::reader_lease::LeaseHolderId::new_v4(),
                index_scope_gates,
                runtime_state: RwLock::new(HelixRuntimeState::new(
                    DataScope::LegacyUnscoped,
                    indexes,
                )),
                index_capabilities,
                index_worker: Mutex::new(index_worker),
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

    /// Whether this handle is read-only.
    pub fn is_reader_mode(&self) -> bool {
        self.mode() == HelixDbMode::ReadOnly
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
        operation_id: index_v2::IndexOperationId,
    ) -> Result<index_v2::IndexOperationStatus> {
        let operation = match self.storage() {
            HelixStorage::Reader(reader) => {
                index_v2::outbox::read_operation(reader.as_ref(), scope, operation_id).await?
            }
            HelixStorage::Writer(writer) => {
                let snapshot = writer.db().snapshot().await?;
                index_v2::outbox::read_operation(snapshot.as_ref(), scope, operation_id).await?
            }
        };
        let Some(operation) = operation else {
            return Err(HelixDbError::IndexOperationNotFound {
                operation_id: operation_id.as_uuid().to_string(),
            });
        };
        Ok(index_v2::IndexOperationStatus::from_record(&operation))
    }

    /// Validates and atomically enqueues one public CREATE against the current source cut.
    pub(crate) async fn enqueue_index_create(
        &self,
        scope: DataScope,
        spec: &ir::IndexDdlCreateSpec,
        mode: ir::IndexCreateMode,
    ) -> Result<index_v2::IndexDdlReceipt> {
        let definition = runtime_catalog::dynamic_index_definition_from_create_spec(spec)?;
        let family = match definition.family() {
            index_v2::IndexDefinitionFamily::Secondary => error::IndexFamily::Secondary,
            index_v2::IndexDefinitionFamily::Vector => error::IndexFamily::Vector,
            index_v2::IndexDefinitionFamily::Text => error::IndexFamily::Text,
        };
        if let Some(reason) = self.index_lifecycle_unavailable_reason(family) {
            return Err(HelixDbError::IndexLifecycleUnavailable { family, reason });
        }
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        let receipt = index_v2::lifecycle::create_index_operation_from_current_source(
            writer.db(),
            scope,
            definition,
            mode,
        )
        .await?;
        let worker = self.inner.index_worker.lock().await;
        if let Some(worker) = worker.as_ref() {
            worker.wake();
        }
        Ok(receipt)
    }

    /// Resolves canonical settings and atomically enqueues one public DROP or abort.
    pub(crate) async fn enqueue_index_drop(
        &self,
        scope: DataScope,
        spec: &ir::IndexDdlDropSpec,
    ) -> Result<index_v2::IndexDdlReceipt> {
        let identity = runtime_catalog::dynamic_index_identity_from_drop_spec(spec)?;
        let family = match identity.family() {
            index_v2::IndexIdentityFamily::SecondaryEquality
            | index_v2::IndexIdentityFamily::SecondaryRange => error::IndexFamily::Secondary,
            index_v2::IndexIdentityFamily::Vector => error::IndexFamily::Vector,
            index_v2::IndexIdentityFamily::Text => error::IndexFamily::Text,
        };
        if let Some(reason) = self.index_lifecycle_unavailable_reason(family) {
            return Err(HelixDbError::IndexLifecycleUnavailable { family, reason });
        }
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        let Some(record) =
            index_v2::repository::load_index_record(writer.db(), scope, &identity).await?
        else {
            return Err(HelixDbError::IndexNotFound(format!("{identity:?}")));
        };
        let definition = runtime_catalog::dynamic_index_definition_from_canonical_drop_spec(
            spec,
            record.definition(),
        )?;
        let receipt =
            index_v2::lifecycle::drop_index_operation(writer.db(), scope, &definition).await?;
        let worker = self.inner.index_worker.lock().await;
        if let Some(worker) = worker.as_ref() {
            worker.wake();
        }
        Ok(receipt)
    }

    /// Convergently requeues a blocked operation at its exact checkpoint.
    pub async fn retry_index_operation(
        &self,
        scope: DataScope,
        operation_id: index_v2::IndexOperationId,
    ) -> Result<index_v2::IndexOperationStatus> {
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        let operation = index_v2::outbox::retry_operation(writer.db(), scope, operation_id).await?;
        if matches!(
            operation.execution_state(),
            index_v2::IndexOperationExecutionState::Queued { .. }
        ) {
            let worker = self.inner.index_worker.lock().await;
            if let Some(worker) = worker.as_ref() {
                worker.wake();
            }
        }
        Ok(index_v2::IndexOperationStatus::from_record(&operation))
    }

    /// Converts a constructing BUILD into cleanup, while converging on the
    /// same already-aborting or aborted BUILD.
    pub async fn abort_index_operation(
        &self,
        scope: DataScope,
        operation_id: index_v2::IndexOperationId,
    ) -> Result<index_v2::IndexOperationStatus> {
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        let operation = index_v2::outbox::abort_operation(writer.db(), scope, operation_id).await?;
        if matches!(
            operation.execution_state(),
            index_v2::IndexOperationExecutionState::Queued { .. }
        ) {
            let worker = self.inner.index_worker.lock().await;
            if let Some(worker) = worker.as_ref() {
                worker.wake();
            }
        }
        Ok(index_v2::IndexOperationStatus::from_record(&operation))
    }

    /// Return the database path inside the object store.
    pub fn path(&self) -> &str {
        self.inner.storage.path()
    }

    /// Borrow the object store backing this handle.
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        self.inner.storage.object_store()
    }

    #[cfg(test)]
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
        Interpreter::new_scoped(self, params, tenant_scope)
            .execute(plan)
            .await
    }

    /// Execute an SDK-built query request.
    pub async fn query(&self, request: QueryRequest) -> Result<JsonValue> {
        query_service::execute_query_on(self, request, query_service::QueryMode::Execute)
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
        query_service::execute_query_on_scoped(
            self,
            request,
            query_service::QueryMode::Execute,
            tenant_scope,
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
        query_service::execute_query_on_scoped(
            self,
            request,
            query_service::QueryMode::Execute,
            tenant_scope,
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
            if let Some(worker) = self.inner.index_worker.lock().await.take() {
                worker.stop().await;
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
        let handle = tokio::spawn(async move {
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
            Some(VectorMemoryRefreshTask { shutdown, handle });
        Ok(())
    }

    pub(crate) fn storage(&self) -> &HelixStorage {
        self.inner.storage.handle()
    }

    /// Returns why one family cannot cross the complete public lifecycle boundary.
    pub(crate) fn index_lifecycle_unavailable_reason(
        &self,
        family: error::IndexFamily,
    ) -> Option<error::IndexLifecycleUnavailableReason> {
        let family = match family {
            error::IndexFamily::Secondary => index_v2::IndexOperationFamily::Secondary,
            error::IndexFamily::Vector => index_v2::IndexOperationFamily::Vector,
            error::IndexFamily::Text => index_v2::IndexOperationFamily::Text,
            error::IndexFamily::DynamicIndexes => {
                return Some(
                    error::IndexLifecycleUnavailableReason::MutationMaintenanceUnavailable,
                );
            }
        };
        if self.inner.index_capabilities.public_ddl_ready(family) {
            return None;
        }
        if family == index_v2::IndexOperationFamily::Text
            && self.inner.blob_publication_coordinator.is_none()
        {
            return Some(
                error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
            );
        }
        if self.inner.reader_lease_coordinator.is_none() {
            return Some(error::IndexLifecycleUnavailableReason::ReaderCoordinationUnavailable);
        }
        Some(error::IndexLifecycleUnavailableReason::CanonicalStateUnavailable)
    }

    /// Clones the narrow coordinator used by one request-owned index lease.
    pub(crate) fn reader_lease_coordinator(
        &self,
    ) -> Option<Arc<dyn index_v2::reader_lease::IndexLeaseCoordinator>> {
        self.inner.reader_lease_coordinator.clone()
    }

    /// Returns this database handle's stable reader-lease holder identity.
    pub(crate) fn reader_lease_holder(&self) -> index_v2::reader_lease::LeaseHolderId {
        self.inner.reader_lease_holder
    }

    /// Returns the process-local text blob publication/deletion coordinator.
    pub(crate) fn blob_gc_gate(&self) -> &search::text::BlobGcGate {
        &self.inner.blob_gc_gate
    }

    /// Returns process-local ownership for request-driven Active text mutations.
    pub(crate) fn active_text_mutations(
        &self,
    ) -> &index_v2::text::active_mutation::ActiveTextMutationRegistry {
        &self.inner.active_text_mutations
    }

    /// Captures one request's inseparable Active-text coordinator/writer authority.
    pub(crate) async fn active_text_mutation_runtime(
        &self,
    ) -> index_v2::text::active_publication::ActiveTextMutationRuntime {
        let Some(coordinator) = &self.inner.blob_publication_coordinator else {
            return index_v2::text::active_publication::ActiveTextMutationRuntime::Unavailable;
        };
        let writer_epoch = self
            .inner
            .index_worker
            .lock()
            .await
            .as_ref()
            .map(index_v2::worker::IndexWorkerSupervisor::writer_epoch);
        let Some(writer_epoch) = writer_epoch else {
            return index_v2::text::active_publication::ActiveTextMutationRuntime::Unavailable;
        };
        index_v2::text::active_publication::ActiveTextMutationRuntime::Ready {
            coordinator: Arc::clone(coordinator),
            writer_epoch,
        }
    }

    /// Acquires shared mutation authority before a request-owned write snapshot.
    pub(crate) async fn index_mutation_scope_permit(
        &self,
        scope: DataScope,
    ) -> index_v2::IndexScopeMutationPermit {
        self.inner.index_scope_gates.mutation_permit(scope).await
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

    pub(crate) fn runtime_config_snapshot_loaded(&self, scope: DataScope) -> IndexConfig {
        self.inner
            .runtime_state
            .read()
            .expect("runtime state lock is not poisoned")
            .catalog(scope)
    }

    /// Returns configured-only index authority for a previously loaded scope.
    pub(crate) fn configured_index_catalog_loaded(
        &self,
        scope: DataScope,
    ) -> index_v2::ConfiguredIndexCatalog {
        self.inner
            .runtime_state
            .read()
            .expect("runtime state lock is not poisoned")
            .configured_catalog(scope)
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

    /// Rebuilds one planner catalog from its configured base and persisted Active rows.
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
        let configured = self
            .inner
            .runtime_state
            .read()
            .expect("runtime state lock is not poisoned")
            .configured_catalog_for_load(scope);
        let loaded = match self.storage() {
            HelixStorage::Reader(reader) => {
                let observed = reader.status();
                let loaded =
                    index_v2::repository::load_scope_catalog(reader.as_ref(), scope, configured)
                        .await?;
                if reader.status() != observed {
                    return Err(HelixDbError::RequestReadViewChanged);
                }
                loaded
            }
            HelixStorage::Writer(writer) => {
                let snapshot = writer.db().snapshot().await?;
                index_v2::repository::load_scope_catalog(snapshot.as_ref(), scope, configured)
                    .await?
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
    ) -> Vec<index_v2::ActiveIndexHandle> {
        self.inner
            .runtime_state
            .read()
            .expect("runtime state lock is not poisoned")
            .active_handles(scope)
    }
}

async fn register_loaded_index_generations(
    indexes: &index_v2::LoadedV2ScopeCatalog,
    runtime_dependencies: &HelixRuntimeDependencies,
) -> Result<()> {
    let Some(coordinator) = runtime_dependencies.reader_lease_coordinator() else {
        return Ok(());
    };
    for handle in indexes.active_handles() {
        coordinator
            .register_generation(index_v2::reader_lease::LeaseGenerationKey::new(
                handle.scope(),
                handle.index_id(),
                handle.generation(),
            ))
            .await
            .map_err(|_| HelixDbError::IndexLifecycleUnavailable {
                family: handle.family(),
                reason: error::IndexLifecycleUnavailableReason::ReaderCoordinationUnavailable,
            })?;
    }
    Ok(())
}

async fn build_slate_db_cache(config: &CacheMode) -> Result<Option<Arc<dyn DbCache>>> {
    match config {
        CacheMode::VectorMemoryOnly | CacheMode::Memory => Ok(None),
        CacheMode::Hybrid { slate_db, .. } => {
            let cache = HybridCacheBuilder::new()
                .with_name("helix-slate-hybrid")
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
                    .with_block_size(64 * 1024),
                )
                .build()
                .await
                .map_err(|err| {
                    HelixDbError::Config(format!("failed to build Slate hybrid cache: {err}"))
                })?;
            Ok(Some(Arc::new(FoyerHybridCache::new_with_cache(cache))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::keys::tenant::TenantId;
    use helix_ast::batch::{read_batch, write_batch};
    use helix_ast::query::QueryRequest;
    use helix_ast::traversal::g;
    use helix_ast::value::PropertyInput;

    fn tenant_scope(value: &str) -> DataScope {
        DataScope::Tenant(TenantId::from_ulid_str(value).expect("valid tenant"))
    }

    #[test]
    fn object_storage_source_is_constructed_without_io() {
        let (path, _store, runtime_dependencies) = HelixDbSource::ObjectStorage {
            database: "facade-object-storage".into(),
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://127.0.0.1:9000".into()),
            allow_http: true,
        }
        .into_parts()
        .expect("object storage source builds without a request");
        assert_eq!(path, "facade-object-storage");
        assert_eq!(
            runtime_dependencies.topology(),
            DatabaseAccessTopology::shared()
        );
    }

    #[tokio::test]
    async fn process_local_text_runtime_is_fully_ready_and_shared_defaults_fail_closed() {
        let process_local = HelixDB::open(HelixDbSource::InMemory {
            database: "facade-active-text-process-local".to_string(),
        })
        .await
        .expect("process-local writer opens");
        assert!(process_local.inner.reader_lease_coordinator.is_some());
        assert!(process_local
            .active_text_mutation_runtime()
            .await
            .ready()
            .is_some());
        for family in [
            index_v2::IndexOperationFamily::Secondary,
            index_v2::IndexOperationFamily::Vector,
            index_v2::IndexOperationFamily::Text,
        ] {
            assert!(process_local
                .inner
                .index_capabilities
                .lifecycle_driver_ready(family));
            assert!(process_local
                .inner
                .index_capabilities
                .public_ddl_ready(family));
            assert!(process_local
                .inner
                .index_capabilities
                .public_serving_ready(family));
        }
        assert_eq!(
            process_local.index_lifecycle_unavailable_reason(error::IndexFamily::Text),
            None
        );
        process_local.close().await.expect("writer closes");

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared = HelixDB::open_with_object_store_and_index_config_for_tests(
            "facade-active-text-shared",
            Arc::clone(&object_store),
            IndexConfig::new(),
        )
        .await
        .expect("shared writer opens");
        assert!(shared.inner.reader_lease_coordinator.is_none());
        assert!(shared
            .active_text_mutation_runtime()
            .await
            .ready()
            .is_none());
        assert!(!shared
            .inner
            .index_capabilities
            .lifecycle_driver_ready(index_v2::IndexOperationFamily::Text));
        assert_eq!(
            shared.index_lifecycle_unavailable_reason(error::IndexFamily::Text),
            Some(error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable)
        );
        shared.close().await.expect("shared writer closes");

        let reader = HelixDB::open_reader_with_object_store_and_index_config_for_tests(
            "facade-active-text-shared",
            object_store,
            IndexConfig::new(),
        )
        .await
        .expect("shared reader opens");
        assert!(reader.inner.reader_lease_coordinator.is_none());
        assert!(reader
            .active_text_mutation_runtime()
            .await
            .ready()
            .is_none());
        assert!(!reader
            .inner
            .index_capabilities
            .lifecycle_driver_ready(index_v2::IndexOperationFamily::Text));
        reader.close().await.expect("reader closes");
    }

    #[tokio::test]
    async fn process_local_token_routes_share_store_and_both_coordinators() {
        let token = ProcessLocalDatabaseToken::new("facade-process-local-token").unwrap();
        let expected = token.runtime_dependencies();
        let expected_reader = expected.reader_lease_coordinator().unwrap();
        let expected_blob = expected.blob_publication_coordinator().unwrap();

        let writer = HelixDB::open(HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .unwrap();
        assert!(Arc::ptr_eq(
            writer.inner.reader_lease_coordinator.as_ref().unwrap(),
            &expected_reader,
        ));
        assert!(Arc::ptr_eq(
            writer.inner.blob_publication_coordinator.as_ref().unwrap(),
            &expected_blob,
        ));
        writer.close().await.unwrap();

        let reader = HelixDB::open_reader(HelixDbSource::InMemoryToken { token })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            reader.inner.reader_lease_coordinator.as_ref().unwrap(),
            &expected_reader,
        ));
        assert!(Arc::ptr_eq(
            reader.inner.blob_publication_coordinator.as_ref().unwrap(),
            &expected_blob,
        ));
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
        let index_v2::IndexDdlReceipt::Accepted {
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
                    index_v2::IndexOperationStatus::Succeeded { .. } => break,
                    index_v2::IndexOperationStatus::Blocked { .. }
                    | index_v2::IndexOperationStatus::Aborted { .. } => {
                        panic!("empty secondary build must succeed")
                    }
                    index_v2::IndexOperationStatus::Queued { .. }
                    | index_v2::IndexOperationStatus::Running { .. } => {
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
        let index_v2::IndexDdlReceipt::Accepted {
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
                    index_v2::IndexOperationStatus::Succeeded { .. } => break,
                    index_v2::IndexOperationStatus::Blocked { .. }
                    | index_v2::IndexOperationStatus::Aborted { .. } => {
                        panic!("unleased empty secondary drop must succeed")
                    }
                    index_v2::IndexOperationStatus::Queued { .. }
                    | index_v2::IndexOperationStatus::Running { .. } => {
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

    #[tokio::test]
    async fn process_local_source_rejects_external_dependency_override() {
        let token = ProcessLocalDatabaseToken::new("facade-process-local-override").unwrap();
        let dependencies = token.runtime_dependencies();
        let result = HelixDB::open_with_runtime_dependencies(
            HelixDbSource::InMemoryToken { token },
            DbConfig::new(),
            dependencies,
        )
        .await;
        let Err(HelixDbError::Config(message)) = result else {
            panic!("process-local dependency override must fail before database open");
        };
        assert!(message.contains("owned by their token"));
    }

    #[tokio::test]
    async fn caller_store_accepts_only_explicit_shared_dependency_modes() {
        let donor = ProcessLocalDatabaseToken::new("facade-shared-adapter-donor").unwrap();
        let donor_dependencies = donor.runtime_dependencies();
        let reader_coordinator = donor_dependencies.reader_lease_coordinator().unwrap();
        let blob_coordinator = donor_dependencies.blob_publication_coordinator().unwrap();
        let dependencies = HelixRuntimeDependencies::shared(
            SharedReaderLeaseMode::Installed(Arc::clone(&reader_coordinator)),
            SharedBlobPublicationMode::Installed(Arc::clone(&blob_coordinator)),
        );
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = HelixDB::open_with_object_store_and_runtime_dependencies(
            "facade-shared-adapter-injection",
            object_store,
            DbConfig::new(),
            dependencies,
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(
            db.inner.reader_lease_coordinator.as_ref().unwrap(),
            &reader_coordinator,
        ));
        assert!(Arc::ptr_eq(
            db.inner.blob_publication_coordinator.as_ref().unwrap(),
            &blob_coordinator,
        ));
        db.close().await.unwrap();

        let process_local_dependencies = donor.runtime_dependencies();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let result = HelixDB::open_with_object_store_and_runtime_dependencies(
            "facade-process-local-adapter-rejected",
            object_store,
            DbConfig::new(),
            process_local_dependencies,
        )
        .await;
        let Err(HelixDbError::Config(message)) = result else {
            panic!("a caller-provided store cannot claim process-local coordination");
        };
        assert!(message.contains("source topology"));
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

        use crate::encoding::v1::keys::index_v2::IndexV2Key;
        use crate::encoding::v1::keys::vectors::{VectorKey, VectorUpperVectorKey};
        use crate::encoding::v1::keys::{DataKeyKind, Key};
        use crate::encoding::v1::values::index_v2::encode_index_record;

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
        let descriptor = index_v2::VectorGenerationDescriptor::for_definition(vector);
        let physical_index_id = index_v2::VectorPhysicalIndexId::new(707).unwrap();
        let record = index_v2::IndexRecordV2::building(
            index_v2::IndexId::new(70).unwrap(),
            definition,
            index_v2::IndexRevision::initial(),
            index_v2::PhysicalGeneration::Vector {
                generation: index_v2::IndexGenerationId::initial(),
                layout: index_v2::VectorPhysicalLayout::Unpartitioned { physical_index_id },
                descriptor,
            },
            index_v2::IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(index_v2::IndexStateTransition::Activate)
        .unwrap();
        let active = index_v2::ActiveIndexHandle::try_from_record(scope, &record).unwrap();
        let generation = search::vector::ValidatedVectorGenerationHandle::try_from_active_current(
            &active,
            physical_index_id,
        )
        .unwrap();
        let record_key = Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(IndexV2Key::index_record(record.identity().clone())),
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

        let lease = db.vector_cache_registry().lease_for(&generation).unwrap();
        assert_eq!(
            lease.store().get_upper_vector(7).as_deref(),
            Some(b"canonical".as_slice())
        );
        drop(lease);
        db.close().await.unwrap();
    }

    #[test]
    fn runtime_state_tracks_loaded_catalogs_per_scope() {
        let scope = tenant_scope("0000000000000000000000000A");
        let mut state = HelixRuntimeState::new(
            DataScope::LegacyUnscoped,
            index_v2::LoadedV2ScopeCatalog::new(DataScope::LegacyUnscoped, IndexConfig::new()),
        );

        let configured = IndexConfig::new()
            .with_equality_index("User", "email")
            .expect("configured index is valid");
        state.replace_catalog(
            scope,
            index_v2::LoadedV2ScopeCatalog::new(scope, configured),
        );
        assert!(state
            .catalog(scope)
            .has_scoped_equality_index("User", "email"));
        let _ = state.planner_snapshot(scope);
        assert_eq!(
            state.loaded_scopes(),
            vec![DataScope::LegacyUnscoped, scope]
        );
    }

    #[test]
    fn runtime_catalog_replacement_preserves_configured_and_removes_stale_dynamic_rows() {
        let scope = DataScope::LegacyUnscoped;
        let configured = IndexConfig::new()
            .with_equality_index("Configured", "slug")
            .expect("configured index is valid");
        let definition = ValidatedDynamicIndexDefinition::try_from(
            config::SecondaryIndexDefinition::node_equality("User", "email")
                .expect("dynamic index is valid"),
        )
        .expect("dynamic definition validates");
        let building = index_v2::IndexRecordV2::building(
            index_v2::IndexId::initial(),
            definition.clone(),
            index_v2::IndexRevision::initial(),
            index_v2::PhysicalGeneration::Secondary {
                generation: index_v2::IndexGenerationId::initial(),
            },
            index_v2::IndexOperationId::new_v4(),
        )
        .expect("building record is valid");
        let active = building
            .transition(index_v2::IndexStateTransition::Activate)
            .expect("building record activates");
        let mut loaded = index_v2::LoadedV2ScopeCatalog::new(scope, configured);
        loaded
            .insert_active(&active)
            .expect("active row enters initial catalog");
        let mut state = HelixRuntimeState::new(scope, loaded);

        assert!(state
            .catalog(scope)
            .has_scoped_equality_index("User", "email"));
        let configured = state.configured_catalog_for_load(scope);
        assert!(configured.has_scoped_equality_index("Configured", "slug"));
        assert!(!configured.has_scoped_equality_index("User", "email"));

        state.replace_catalog(
            scope,
            index_v2::LoadedV2ScopeCatalog::new(scope, configured),
        );
        assert!(state
            .catalog(scope)
            .has_scoped_equality_index("Configured", "slug"));
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
        let cursor = index_v2::IndexCursor::try_new(
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
        let receipt = index_v2::lifecycle::create_index_operation(
            writer.db(),
            scope,
            definition,
            ir::IndexCreateMode::ErrorIfExists,
            index_v2::lifecycle::InitialBuildProgress::secondary(cursor),
        )
        .await
        .expect("build operation is accepted");
        let index_v2::IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("new operation must return accepted receipt");
        };
        let operation_id_string = operation_id.as_uuid().to_string();

        assert!(matches!(
            db.get_index_operation(scope, operation_id)
                .await
                .expect("direct status lookup"),
            index_v2::IndexOperationStatus::Queued { .. }
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
        assert_eq!(abort_result["status"]["stage"], "aborting_begin_drain");

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
        let cursor = index_v2::IndexCursor::try_new(
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
        let receipt = index_v2::lifecycle::create_index_operation(
            storage.db(),
            scope,
            definition,
            ir::IndexCreateMode::ErrorIfExists,
            index_v2::lifecycle::InitialBuildProgress::secondary(cursor),
        )
        .await
        .expect("reader fixture operation is accepted");
        let index_v2::IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
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
        let HelixStorage::Reader(_) = reader.storage() else {
            panic!("reader handle expected");
        };
        assert!(matches!(
            reader
                .get_index_operation(scope, operation_id)
                .await
                .expect("reader can point-read lifecycle status"),
            index_v2::IndexOperationStatus::Queued { .. }
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
