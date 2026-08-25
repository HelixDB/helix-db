//! Public vector-index façade and transaction delegation.
//!
//! `VectorIndex` binds one physical row namespace, validated dimension,
//! projection identity, randomness policy, and closed memory-access capability.
//! Public create/drop/metadata/search/insert/delete calls begin or receive the
//! transaction boundary and delegate graph algorithms to `search` and
//! `mutation`, persisted row work to `storage`, and shared-cache hydration to
//! `memory_store`.
//!
//! Two coordination contracts intentionally remain here. Canonical deployed
//! vector rows are addressed by a SimHash-derived token, so resolving that token
//! must combine dirty-aware memory hydration with typed storage without exposing
//! raw keys. Mutation-local item caching similarly bridges decoded items into a
//! `MutationOpCache` while the mutation module owns graph decisions. Keeping
//! those cross-contract joins on the façade avoids broad traits or one-line
//! helper modules; their component I/O and policies remain independently tested.

#[cfg(test)]
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

#[cfg(test)]
use bytes::Bytes;
use slatedb::DbTransaction;
#[cfg(test)]
use slatedb::IsolationLevel;

use crate::encoding::keys::scope::DataScope;
#[cfg(test)]
use crate::encoding::v2::keys::indexes::vector::VectorKey;
#[cfg(test)]
use crate::encoding::v2::keys::indexes::vector::{
    VectorEntryCandidateKey, VectorEntryCandidateNodeKey, VectorIndexMetadataKey, VectorItemKey,
    VectorL0PrefixKey, VectorLayer0NeighborsKey, VectorMemoryPrefixKey, VectorReverseEdgeKey,
    VectorReverseEdgePrefixKey, VectorSimHashKey, VectorUpperVectorKey,
};
#[cfg(test)]
use crate::encoding::v2::legacy::vector::transaction_guard::LegacyVectorTxnGuardKey as VectorTxnGuardKey;
#[cfg(test)]
use crate::encoding::v2::values::indexes::vector::simhash::encode_simhash;
use crate::encoding::NodeId;
use crate::error::HelixDbError;
#[cfg(test)]
use crate::search::vector::unaligned_vector::UnalignedVector;
use slatedb::DbReadOps;

use super::distance::{ActiveVectorSemantics, Distance};
use super::item::Item;
use super::memory_store::{
    SimHashReadStats, VectorMemoryAccess, VectorMemoryDirtyRows, VectorMemoryPendingDirtyRows,
    VectorMemoryStore,
};
#[cfg(test)]
use super::model::Candidate;
#[cfg(test)]
use super::mutation::NeighborRowValue;
use super::mutation::{MutationOpCache, VectorBuildSession, VectorInsertContract};
#[cfg(test)]
use super::neighbor_set::{NeighborDegreeLimit, NeighborSet};
#[cfg(any(test, feature = "production-coverage"))]
use super::randomness::ScriptedLayerSelectorError;
use super::randomness::{LayerSelector, SearchRandomness};
#[cfg(test)]
use super::search;
use super::search::{SearchObserver, SearchSession};
use super::simhash::{order_code_from_simhash_bits, SimHashCache};
use super::storage::{
    CanonicalVectorDirectoryBackfillOutcome, CanonicalVectorRowKey, LegacyVectorMigrationRead,
    LegacyVectorValidationOutcome, LegacyVectorValidationPass, SimHashDirectoryValidationMode,
    SimHashDirectoryValidationOutcome, VectorCleanupRow, VectorCleanupScan, VectorRowKeyspace,
    VectorRows, VectorSimHashDirectoryCleanupScan, VectorWriteRows,
};
use super::{
    decode_item, encode_item, MeasuredVectorTransaction, SearchParams, SearchResult,
    VectorIndexConfig, VectorIndexMetadata, VectorWriteMeasurement,
};
#[cfg(test)]
use super::{encode_metadata, index_id_from_name, SimHashMode};

/// Read accounting for one canonical vector payload lookup.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct VectorFetchReadStats {
    /// SimHash rows consulted to derive the canonical payload key.
    pub(super) simhash_reads: usize,
    /// Canonical payload rows consulted after key derivation.
    pub(super) vector_reads: usize,
    /// Physical batch calls used for SimHash derivation.
    pub(super) simhash_multi_get_calls: usize,
    /// Time spent fetching the SimHash derivation rows.
    pub(super) simhash_fetch_ns: u64,
}

impl VectorFetchReadStats {
    /// Returns the combined logical read count for diagnostics.
    #[inline]
    pub(super) fn total_reads(self) -> usize {
        self.simhash_reads.saturating_add(self.vector_reads)
    }
}

/// A vector index for ANN (Approximate Nearest Neighbor) search
///
/// This struct manages a HNSW-based vector index stored in the database.
/// Each index is independent and can have its own configuration.
/// The generic parameter D specifies the distance metric.
pub(crate) struct VectorIndex<D: Distance> {
    /// Complete physical identity and tenant namespace for every persisted row.
    rows: VectorRowKeyspace,
    /// Complete managed-generation identity used only by build-session caches.
    generation_identity: Option<super::VectorGenerationIdentity>,
    /// Bounded owner for deterministic projection tables used by this handle.
    simhasher_registry: Arc<super::SimHasherRegistry>,
    /// Descriptor-proven projection identity for managed generations.
    simhash_identity: Option<super::SimHashIdentity>,
    /// Complete-generation proof that every vector has a directory row.
    simhash_directory_enabled: bool,
    /// Identity-bound row helper retained after the registry's first admission.
    ///
    /// The handle's write-once dimension and immutable descriptor identity make
    /// a second cache state unrepresentable. Retaining the helper here avoids a
    /// registry mutex acquisition on every search while the registry continues
    /// to own bounded projection-table admission across handles.
    simhash_cache: OnceLock<SimHashCache>,
    /// Complete shared-cache capability for this handle.
    memory_access: VectorMemoryAccess,
    /// Layer policy owned by this index handle.
    layer_selector: LayerSelector,
    /// Factory for query-local sampling state.
    search_randomness: SearchRandomness,
    /// Authoritative dimension loaded from validated metadata for this handle.
    ///
    /// This is write-once because a handle must never decode rows under two
    /// incompatible schemas. Generation publication will supply the same proof
    /// through its validated generation handle without changing item row bytes.
    dimension: OnceLock<super::VectorDimension>,
    /// Phantom data to hold the distance type
    _phantom: PhantomData<D>,
}

impl<D: Distance> VectorIndex<D> {
    /// Create a new vector index handle
    ///
    /// This does not create the index in the database. Use `create` for that.
    #[cfg(any(test, feature = "production-coverage"))]
    pub fn new(name: impl Into<String>) -> Self {
        Self::new_scoped(name, DataScope::LegacyUnscoped)
    }

    /// Create a new tenant-scoped vector index handle.
    ///
    /// This does not create the index in the database. Use `create` for that.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn new_scoped(name: impl Into<String>, tenant_scope: DataScope) -> Self {
        let name = name.into();
        Self {
            rows: VectorRowKeyspace::new(name, tenant_scope),
            generation_identity: None,
            simhasher_registry: Arc::new(super::SimHasherRegistry::default()),
            simhash_identity: None,
            simhash_directory_enabled: false,
            simhash_cache: OnceLock::new(),
            memory_access: VectorMemoryAccess::uncached(),
            layer_selector: LayerSelector::random(),
            search_randomness: SearchRandomness::QueryDerived,
            dimension: OnceLock::new(),
            _phantom: PhantomData,
        }
    }

    /// Opens a persisted pre-V2 physical namespace only while its exact legacy
    /// catalog row is still present. Callers must establish that catalog proof
    /// before constructing this handle.
    pub(crate) fn for_legacy_migration(name: impl Into<String>, tenant_scope: DataScope) -> Self {
        let name = name.into();
        Self {
            rows: VectorRowKeyspace::from_legacy_name(name, tenant_scope),
            generation_identity: None,
            simhasher_registry: Arc::new(super::SimHasherRegistry::default()),
            simhash_identity: None,
            simhash_directory_enabled: false,
            simhash_cache: OnceLock::new(),
            memory_access: VectorMemoryAccess::uncached(),
            layer_selector: LayerSelector::random(),
            search_randomness: SearchRandomness::QueryDerived,
            dimension: OnceLock::new(),
            _phantom: PhantomData,
        }
    }

    /// Creates a descriptor-bound handle using its allocated physical ID.
    pub(crate) fn from_generation(handle: &super::ValidatedVectorGenerationHandle) -> Self {
        Self {
            rows: VectorRowKeyspace::from_allocated(
                handle.physical_name().to_string(),
                handle.identity().physical_index_id(),
                handle.scope(),
            ),
            generation_identity: Some(handle.identity().clone()),
            simhasher_registry: Arc::new(super::SimHasherRegistry::default()),
            simhash_identity: Some(handle.simhash_identity()),
            simhash_directory_enabled: handle.has_simhash_directory(),
            simhash_cache: OnceLock::new(),
            memory_access: VectorMemoryAccess::uncached(),
            layer_selector: LayerSelector::random(),
            search_randomness: SearchRandomness::QueryDerived,
            dimension: OnceLock::new(),
            _phantom: PhantomData,
        }
    }

    /// Rebinds projection retention to the registry owned by the database runtime.
    ///
    /// Production factories use this before any vector operation. Standalone
    /// public handles retain a private bounded owner for API compatibility.
    pub(crate) fn with_simhasher_registry(
        mut self,
        registry: Arc<super::SimHasherRegistry>,
    ) -> Self {
        self.simhasher_registry = registry;
        self.simhash_cache = OnceLock::new();
        self
    }

    /// Opens an exhaustive typed scan for bounded lifecycle cleanup.
    pub(crate) async fn cleanup_scan(
        &self,
        reader: &(impl DbReadOps + Send + Sync),
    ) -> Result<VectorCleanupScan, HelixDbError> {
        VectorRows::new(reader, &self.rows).cleanup_scan().await
    }

    /// Opens cleanup over only this generation's typed SimHash-directory prefix.
    pub(crate) async fn simhash_directory_cleanup_scan(
        &self,
        reader: &(impl DbReadOps + Send + Sync),
    ) -> Result<VectorSimHashDirectoryCleanupScan, HelixDbError> {
        VectorRows::new(reader, &self.rows)
            .simhash_directory_cleanup_scan()
            .await
    }

    /// Validates one bounded page of this proven legacy namespace without writes.
    pub(crate) async fn validate_legacy_physical(
        &self,
        reader: &(impl DbReadOps + Send + Sync),
        pass: LegacyVectorValidationPass,
        cursor: Option<&[u8]>,
        definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
        max_entities: usize,
        max_input_bytes: u64,
    ) -> Result<LegacyVectorValidationOutcome, HelixDbError> {
        VectorRows::new(reader, &self.rows)
            .validate_legacy_physical::<D>(
                pass.lane(),
                cursor,
                definition,
                pass.mode(),
                max_entities,
                max_input_bytes,
            )
            .await
    }

    /// Validates one bounded page of only this generation's compact directory.
    pub(crate) async fn validate_simhash_directory(
        &self,
        reader: &(impl DbReadOps + Send + Sync),
        cursor: Option<&[u8]>,
        definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
        mode: SimHashDirectoryValidationMode,
        max_entities: usize,
        max_input_bytes: u64,
    ) -> Result<SimHashDirectoryValidationOutcome, HelixDbError> {
        VectorRows::new(reader, &self.rows)
            .validate_simhash_directory::<D>(
                cursor,
                definition,
                mode,
                max_entities,
                max_input_bytes,
            )
            .await
    }

    /// Scans canonical payloads exactly once and emits only missing marker tokens.
    pub(crate) async fn backfill_missing_simhash_directory(
        &self,
        reader: &(impl DbReadOps + Send + Sync),
        cursor: Option<&[u8]>,
        definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
        limits: crate::config::SearchIndexBatchLimits,
    ) -> Result<CanonicalVectorDirectoryBackfillOutcome, HelixDbError> {
        VectorRows::new(reader, &self.rows)
            .backfill_missing_simhash_directory::<D>(
                cursor,
                definition,
                limits.max_entities().get(),
                limits.max_input_bytes().get(),
                limits.max_output_operations(),
                limits.max_output_bytes(),
            )
            .await
    }

    /// Stages one opaque canonical-row token as a measured directory marker.
    pub(crate) fn stage_simhash_directory_entry(
        &self,
        transaction: &MeasuredVectorTransaction<'_>,
        entry: &CanonicalVectorRowKey,
    ) -> Result<(), HelixDbError> {
        VectorWriteRows::new(transaction, &self.rows).put_simhash_directory_entry(entry)
    }

    /// Revalidates and rewrites only the legacy metadata row into current form.
    pub(crate) async fn transcode_legacy_metadata(
        &self,
        transaction: &DbTransaction,
        definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
        canonical_physical_name: &str,
    ) -> Result<VectorWriteMeasurement, HelixDbError> {
        let Some(mut metadata) = VectorRows::new(transaction, &self.rows)
            .legacy_metadata()
            .await?
        else {
            return Err(HelixDbError::InvariantViolation(
                "legacy vector metadata disappeared before adoption activation".to_string(),
            ));
        };
        let expected_legacy =
            VectorIndexConfig::from_v2_definition(definition, self.rows.physical_name());
        if !metadata.config.has_same_physical_contract(&expected_legacy) {
            return Err(HelixDbError::InvariantViolation(
                "legacy vector metadata changed before adoption activation".to_string(),
            ));
        }
        metadata.config.index_name = canonical_physical_name.to_string();
        let expected_current =
            VectorIndexConfig::from_v2_definition(definition, canonical_physical_name);
        if !metadata
            .config
            .has_same_physical_contract(&expected_current)
        {
            return Err(HelixDbError::InvariantViolation(
                "transcoded vector metadata differs from the canonical descriptor".to_string(),
            ));
        }
        let recorder = super::VectorWriteRecorder::new();
        let write = recorder.bind(transaction);
        VectorWriteRows::new(&write, &self.rows).put_metadata(&metadata)?;
        match write.measurement() {
            Ok(measurement) => Ok(measurement),
            Err(error) => Err(HelixDbError::InvariantViolation(format!(
                "vector metadata transcode measurement failed: {error}"
            ))),
        }
    }

    /// Validates the frozen legacy metadata against one persisted definition.
    pub(crate) async fn validate_legacy_metadata_contract(
        &self,
        reader: &(impl DbReadOps + Send + Sync),
        definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
    ) -> Result<(), HelixDbError> {
        let Some(metadata) = VectorRows::new(reader, &self.rows)
            .legacy_metadata()
            .await?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "persisted legacy vector definition has no physical metadata".to_string(),
            ));
        };
        let expected = VectorIndexConfig::from_v2_definition(definition, self.rows.physical_name());
        if !metadata.config.has_same_physical_contract(&expected) {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector metadata conflicts with its persisted definition".to_string(),
            ));
        }
        Ok(())
    }

    /// Stages one keyspace-checked cleanup token in the measured transaction.
    pub(crate) fn stage_cleanup_row(
        &self,
        transaction: &MeasuredVectorTransaction<'_>,
        row: &VectorCleanupRow,
    ) -> Result<(), HelixDbError> {
        VectorWriteRows::new(transaction, &self.rows).delete_cleanup_row(row)
    }

    /// Binds managed projection semantics to an already validated descriptor.
    pub(super) fn with_simhash_identity(mut self, identity: super::SimHashIdentity) -> Self {
        self.simhash_identity = Some(identity);
        self.simhash_cache = OnceLock::new();
        self
    }

    /// Enables directory maintenance for generation-capability tests.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(super) fn with_simhash_directory(mut self) -> Self {
        self.simhash_directory_enabled = true;
        self
    }

    /// Returns the handle-retained SimHash row helper for validated metadata.
    ///
    /// The first call binds the write-once handle dimension and admits the
    /// descriptor identity through the bounded registry. Later calls borrow the
    /// same helper without reacquiring the registry mutex. Legacy handles derive
    /// the deployed current identity from metadata, preserving existing rows.
    pub(super) fn simhash_cache(&self, dimension: usize) -> Result<&SimHashCache, HelixDbError> {
        match self.simhash_identity {
            Some(identity) if identity.dimension().get() != dimension => {
                return Err(HelixDbError::InvariantViolation(format!(
                    "descriptor SimHash dimension {} disagrees with metadata dimension {dimension}",
                    identity.dimension().get()
                )));
            }
            Some(_) | None => {}
        }
        self.remember_dimension(dimension)?;
        if let Some(cache) = self.simhash_cache.get() {
            return Ok(cache);
        }

        let cache = match self.simhash_identity {
            Some(identity) => SimHashCache::try_new_scoped_with_identity(
                self.id(),
                self.scope(),
                identity,
                &self.simhasher_registry,
            ),
            None => SimHashCache::try_new_scoped_in(
                self.id(),
                dimension,
                self.scope(),
                &self.simhasher_registry,
            ),
        }
        .map_err(|error| HelixDbError::Config(error.to_string()))?;
        let _ = self.simhash_cache.set(cache);
        Ok(self
            .simhash_cache
            .get()
            .expect("this call or a concurrent call initialized the SimHash cache"))
    }

    /// Selects the HNSW layer for one mutation when no replay layer was supplied.
    ///
    /// The façade owns the configured selector, while the mutation module owns
    /// graph insertion. Keeping selection behind this method avoids exposing
    /// mutable randomness state across the module boundary.
    pub(super) fn select_mutation_layer(&self, level_multiplier: f32) -> u16 {
        self.layer_selector.select(level_multiplier)
    }

    /// Starts isolated sampling state for one layer-zero search.
    ///
    /// The handle retains the configured seed policy while `search.rs` owns all
    /// mutable query-local randomness and traversal decisions.
    pub(super) fn start_search_randomness(
        &self,
        query_simhash: &super::SimHash,
        entry_point: NodeId,
        ef: usize,
    ) -> super::randomness::SearchSession {
        self.search_randomness.start(query_simhash, entry_point, ef)
    }

    #[cfg(any(test, feature = "production-coverage"))]
    /// Installs deterministic non-empty layer choices for cross-module tests.
    pub(crate) fn with_scripted_layers(
        mut self,
        layers: Vec<u16>,
    ) -> Result<Self, ScriptedLayerSelectorError> {
        self.layer_selector = LayerSelector::scripted(layers)?;
        Ok(self)
    }

    /// Installs the deterministic Active-row shape used by the batch benchmark.
    #[cfg(feature = "production-coverage")]
    pub(crate) fn with_batch_benchmark_contract(
        self,
        layers: Vec<u16>,
    ) -> Result<Self, ScriptedLayerSelectorError> {
        self.with_scripted_layers(layers)
            .map(Self::with_simhash_directory)
    }

    #[cfg(test)]
    /// Installs a query-independent seed so complete search choices can replay.
    fn with_search_seed(mut self, seed: u64) -> Self {
        self.search_randomness = SearchRandomness::Seeded(seed);
        self
    }

    /// Returns the stable ID bound to every persisted row for this handle.
    pub(super) const fn id(&self) -> u64 {
        self.rows.index_id()
    }

    /// Returns the current compact namespace to the opt-in scale fixture.
    #[cfg(test)]
    const fn scale_index_id(&self) -> u64 {
        self.id()
    }

    /// Returns the descriptor-bound typed row namespace used by session owners.
    pub(super) const fn row_keyspace(&self) -> &VectorRowKeyspace {
        &self.rows
    }

    /// Returns the complete managed identity required by a reusable build session.
    pub(super) fn build_session_identity(
        &self,
    ) -> Result<&super::VectorGenerationIdentity, HelixDbError> {
        self.generation_identity.as_ref().ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "vector build session requires a validated managed generation".to_string(),
            )
        })
    }

    /// Returns the tenant namespace bound to every persisted row for this handle.
    const fn scope(&self) -> DataScope {
        self.rows.scope()
    }

    /// Delegates typed row-key encoding to the bound storage namespace.
    #[inline]
    #[cfg(test)]
    fn vector_key(&self, key: VectorKey) -> Bytes {
        self.rows.key(key)
    }

    /// Delegates scan-key validation and tenant-prefix removal to storage.
    #[inline]
    #[cfg(test)]
    fn strip_physical_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8], HelixDbError> {
        self.rows.strip_physical_key(key)
    }

    /// Binds an identity-checked managed read cache and commit-window fences.
    pub(super) fn with_managed_read_cache(
        mut self,
        store: Arc<VectorMemoryStore>,
        pending_rows: Arc<VectorMemoryPendingDirtyRows>,
    ) -> Result<Self, super::VectorGenerationValidationError> {
        self.validate_memory_store_identity(&store)?;
        self.memory_access = VectorMemoryAccess::read_snapshot(store, pending_rows);
        Ok(self)
    }

    /// Attach transaction-local dirty tracking even when no shared memory store exists yet.
    pub(super) fn with_write_dirty_rows(mut self, dirty_rows: Arc<VectorMemoryDirtyRows>) -> Self {
        self.memory_access = VectorMemoryAccess::write_tracking(dirty_rows);
        self
    }

    /// Rejects cache attachment when scope or physical index identity differs.
    fn validate_memory_store_identity(
        &self,
        store: &VectorMemoryStore,
    ) -> Result<(), super::VectorGenerationValidationError> {
        if store.scope() != self.scope() || store.index_id() != self.id() {
            return Err(super::VectorGenerationValidationError::CacheIdentityMismatch);
        }
        Ok(())
    }

    #[inline]
    #[cfg(test)]
    fn is_memory_node_dirty(&self, node_id: NodeId) -> bool {
        self.memory_access.is_node_dirty(node_id)
    }

    #[inline]
    #[cfg(test)]
    fn is_memory_upper_neighbors_dirty(&self, layer: u16, node_id: NodeId) -> bool {
        self.memory_access.is_upper_neighbors_dirty(layer, node_id)
    }

    /// Fences one node's SimHash and upper-vector rows from shared-cache reads.
    ///
    /// Mutation paths call this immediately after staging either row family;
    /// the surrounding write transaction owns publication or abort cleanup.
    #[inline]
    pub(super) fn mark_memory_node_dirty(&self, node_id: NodeId) {
        self.memory_access.mark_node_dirty(node_id);
    }

    /// Marks an upper-neighbor row unsafe for shared-cache reads in this write.
    #[inline]
    pub(super) fn mark_memory_upper_neighbors_dirty(&self, layer: u16, node_id: NodeId) {
        self.memory_access
            .mark_upper_neighbors_dirty(layer, node_id);
    }

    /// Get the index name
    pub fn name(&self) -> &str {
        self.rows.physical_name()
    }

    /// Binds this handle to the dimension of validated index metadata.
    ///
    /// Repeating the same binding is idempotent. Observing another dimension on
    /// the same handle is an invariant violation rather than a mutable schema
    /// transition, so no row can be decoded under ambiguous dimensions.
    pub(super) fn remember_dimension(
        &self,
        dimension: usize,
    ) -> Result<super::VectorDimension, HelixDbError> {
        let dimension = super::VectorDimension::try_new(dimension).map_err(|error| {
            HelixDbError::InvalidVectorConfig(super::VectorConfigError::Dimension(error))
        })?;
        if let Some(remembered) = self.dimension.get() {
            if *remembered != dimension {
                return Err(HelixDbError::InvariantViolation(format!(
                    "vector index '{}' dimension changed within one handle: {} -> {}",
                    self.name(),
                    remembered.get(),
                    dimension.get()
                )));
            }
            return Ok(*remembered);
        }
        let _ = self.dimension.set(dimension);
        Ok(*self
            .dimension
            .get()
            .expect("dimension is initialized by this method"))
    }

    /// Returns the dimension proof required by the item decoder.
    ///
    /// The first call validates and loads metadata; later calls are lock-free
    /// reads from the write-once binding. Callers processing multiple rows should
    /// obtain this once per operation and reuse it throughout the loop.
    pub(super) async fn expected_dimension(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
    ) -> Result<super::VectorDimension, HelixDbError> {
        if let Some(dimension) = self.dimension.get() {
            return Ok(*dimension);
        }
        self.get_metadata(txn)
            .await?
            .ok_or_else(|| HelixDbError::IndexNotFound(self.name().to_string()))?;
        Ok(*self
            .dimension
            .get()
            .expect("validated metadata initializes the dimension"))
    }

    // =========================================================================
    // Index Management
    // =========================================================================

    /// Create a new vector index in the database
    ///
    /// # Arguments
    /// * `txn` - Database transaction
    /// * `config` - Index configuration including dimension, metric, and HNSW parameters
    ///
    /// # Errors
    /// Returns an error if an index with the same name already exists
    #[allow(
        dead_code,
        reason = "retained as the direct legacy and migration mutation contract"
    )]
    pub async fn create(
        &self,
        txn: &DbTransaction,
        config: VectorIndexConfig,
    ) -> Result<(), HelixDbError> {
        let txn = MeasuredVectorTransaction::new(txn);
        self.stage_create(&txn, config).await
    }

    /// Stages current-format empty metadata through a shared measured write set.
    ///
    /// A bounded lifecycle planner uses this when it discovers a physical
    /// partition for the first time. Keeping creation inside the same recorder
    /// as the first HNSW insertion ensures admission includes metadata without
    /// changing its deployed encoding. No generation-wide mutation sentinel is
    /// written: unrelated same-index writes must not serialize on one hot key.
    pub(crate) async fn stage_create(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        config: VectorIndexConfig,
    ) -> Result<(), HelixDbError> {
        config.validate()?;
        let Some(_semantics) = ActiveVectorSemantics::for_distance::<D>() else {
            return Err(HelixDbError::Config(format!(
                "vector distance '{}' has no stable durable semantic identity",
                D::name()
            )));
        };
        if config.index_name != self.name() {
            return Err(HelixDbError::Config(format!(
                "Vector index name mismatch: handle='{}', config='{}'",
                self.name(),
                config.index_name
            )));
        }

        // Check if index already exists
        let rows = VectorWriteRows::new(txn, &self.rows);
        if rows.metadata_exists().await? {
            return Err(HelixDbError::IndexAlreadyExists(self.name().to_string()));
        }

        self.remember_dimension(config.dimension)?;
        // Create metadata
        let metadata = VectorIndexMetadata::new(config);
        // Store metadata
        rows.put_metadata(&metadata)?;

        Ok(())
    }

    /// Drop (delete) a vector index from the database
    ///
    /// This removes all data associated with the index including metadata,
    /// vectors, and HNSW graph structure.
    ///
    /// # Arguments
    /// * `txn` - Database transaction
    #[cfg(any(test, feature = "production-coverage"))]
    pub async fn drop(&self, txn: &DbTransaction) -> Result<(), HelixDbError> {
        let txn = MeasuredVectorTransaction::new(txn);
        VectorWriteRows::new(&txn, &self.rows).delete_all().await
    }

    /// Get the metadata for this index
    ///
    /// Returns None if the index does not exist.
    pub async fn get_metadata(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
    ) -> Result<Option<VectorIndexMetadata>, HelixDbError> {
        let metadata = VectorRows::new(txn, &self.rows).metadata().await?;
        if let Some(metadata) = &metadata {
            self.remember_dimension(metadata.config.dimension)?;
        }
        Ok(metadata)
    }

    /// Loads one exact legacy vector plus all physical input bytes.
    pub(crate) async fn legacy_vector_for_migration(
        &self,
        transaction: &(impl DbReadOps + Send + Sync),
        entity_id: NodeId,
        definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
    ) -> Result<LegacyVectorMigrationRead, HelixDbError> {
        let read = VectorRows::new(transaction, &self.rows)
            .legacy_vector_for_migration::<D>(entity_id, definition)
            .await?;
        self.remember_dimension(definition.dimension() as usize)?;
        Ok(read)
    }

    /// Measures the exact current metadata point-read key/value bytes.
    ///
    /// Lifecycle activation uses this after semantic validation so fixed input
    /// admission includes the unchanged current-format metadata row. Absence
    /// still charges the complete typed key lookup; this method neither stages
    /// writes nor changes the metadata codec.
    #[cfg(feature = "production-coverage")]
    pub(crate) async fn measure_metadata_input(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
    ) -> Result<u64, HelixDbError> {
        VectorRows::new(txn, &self.rows)
            .metadata_input_bytes()
            .await
    }

    // =========================================================================
    // Vector Operations
    // =========================================================================

    /// Insert a vector into the index
    ///
    /// # Arguments
    /// * `txn` - Database transaction
    /// * `node_id` - The node ID to associate with this vector
    /// * `vector` - The vector data (must match index dimension)
    /// # Errors
    /// Returns error if:
    /// - Index does not exist
    /// - Vector dimension doesn't match index dimension
    /// - HNSW insertion fails
    #[allow(
        dead_code,
        reason = "retained as the direct legacy and migration mutation contract"
    )]
    pub async fn insert(
        &self,
        txn: &DbTransaction,
        node_id: NodeId,
        vector: &[f32],
    ) -> Result<(), HelixDbError> {
        self.insert_with_contract(txn, node_id, vector, VectorInsertContract::Upsert)
            .await
    }

    /// Executes the façade-selected insertion contract in one measured write set.
    #[allow(
        dead_code,
        reason = "retained behind the direct legacy insertion contract"
    )]
    async fn insert_with_contract(
        &self,
        txn: &DbTransaction,
        node_id: NodeId,
        vector: &[f32],
        contract: VectorInsertContract,
    ) -> Result<(), HelixDbError> {
        self.insert_with_contract_at_layer(txn, node_id, vector, contract, None)
            .await
            .map(|_| ())
    }

    /// Stages one known-fresh insertion at an already selected HNSW layer and
    /// returns its exact final transaction write set.
    ///
    /// A bounded lifecycle builder first calls this in an uncommitted planning
    /// transaction, admits the returned complete measurement, and then applies
    /// its captured final writes in the commit transaction. The physical
    /// generation must remain exclusively builder-owned between those
    /// transactions. Supplying the layer explicitly prevents random layer
    /// selection from changing the measured graph invariant. Planning must use
    /// a write cache-disabled index handle so an aborted planning transaction
    /// cannot affect a resident runtime snapshot.
    #[cfg(test)]
    async fn insert_known_fresh_at_layer_measured(
        &self,
        txn: &DbTransaction,
        node_id: NodeId,
        vector: &[f32],
        node_layer: u16,
    ) -> Result<VectorWriteMeasurement, HelixDbError> {
        let measured = MeasuredVectorTransaction::new(txn);
        self.stage_known_fresh_at_layer(
            &measured,
            node_id,
            vector,
            node_layer,
            super::mutation::FreshVectorBuildProof::for_test(),
        )
        .await?;
        measured
            .measurement()
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))
    }

    /// Stages one known-fresh insertion into a shared measured batch transaction.
    ///
    /// Text/vector backfill owns the wrapper across successive source entities,
    /// captures a write checkpoint before this call, and then reads both the
    /// atomic `plan_since(checkpoint)` measurement and cumulative `measurement()`. The
    /// closed memory-access ADT has no resident-store mutation mode, so
    /// abandoned planning work cannot change shared runtime snapshot state.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) async fn stage_known_fresh_at_layer(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        vector: &[f32],
        node_layer: u16,
        proof: super::mutation::FreshVectorBuildProof,
    ) -> Result<(), HelixDbError> {
        self.insert_with_measured_transaction(
            txn,
            node_id,
            vector,
            VectorInsertContract::ProvenFresh(proof),
            Some(node_layer),
        )
        .await
    }

    /// Stages one builder-exclusive insertion through a reusable planning session.
    pub(crate) async fn stage_known_fresh_at_layer_with_session(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        vector: &[f32],
        node_layer: u16,
        proof: super::mutation::FreshVectorBuildProof,
        session: &mut VectorBuildSession<D>,
    ) -> Result<(), HelixDbError> {
        self.insert_with_build_session(
            txn,
            node_id,
            vector,
            VectorInsertContract::ProvenFresh(proof),
            Some(node_layer),
            session,
        )
        .await
    }

    /// Stages a deterministic replacement in a caller-owned measured write set.
    ///
    /// Catch-up selects and retains `node_layer` during throwaway planning, then
    /// applies the captured delete-plus-insert graph mutation at commit. The
    /// existing vector bytes and HNSW encodings remain unchanged by this
    /// transaction-boundary optimization.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) async fn stage_upsert_at_layer(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        vector: &[f32],
        node_layer: u16,
    ) -> Result<(), HelixDbError> {
        self.insert_with_measured_transaction(
            txn,
            node_id,
            vector,
            VectorInsertContract::Upsert,
            Some(node_layer),
        )
        .await
    }

    /// Stages one builder-exclusive replacement through a reusable planning session.
    pub(crate) async fn stage_upsert_at_layer_with_session(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        vector: &[f32],
        node_layer: u16,
        session: &mut VectorBuildSession<D>,
    ) -> Result<(), HelixDbError> {
        self.insert_with_build_session(
            txn,
            node_id,
            vector,
            VectorInsertContract::Upsert,
            Some(node_layer),
            session,
        )
        .await
    }

    /// Executes insertion through the measured vector-write boundary.
    #[allow(
        dead_code,
        reason = "retained behind the direct legacy insertion contract"
    )]
    async fn insert_with_contract_at_layer(
        &self,
        txn: &DbTransaction,
        node_id: NodeId,
        vector: &[f32],
        contract: VectorInsertContract,
        selected_layer: Option<u16>,
    ) -> Result<VectorWriteMeasurement, HelixDbError> {
        let txn = MeasuredVectorTransaction::new(txn);
        self.insert_with_measured_transaction(&txn, node_id, vector, contract, selected_layer)
            .await?;
        match txn.measurement() {
            Ok(measurement) => Ok(measurement),
            Err(error) => Err(HelixDbError::InvariantViolation(error.to_string())),
        }
    }

    // =========================================================================
    // HNSW Algorithm Helper Methods
    // =========================================================================

    /// Loads one layer-specific item through the bounded mutation-local cache.
    ///
    /// Cached absence remains authoritative for the operation. Reaching the
    /// item limit clears only this disposable operation cache; persisted and
    /// shared memory state are not changed.
    pub(super) async fn get_item_for_layer_cached(
        &self,
        txn: &DbTransaction,
        layer: u16,
        node_id: NodeId,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<Option<Arc<Item<'static, D>>>, HelixDbError> {
        while mutation_cache.enforces_local_limits()
            && mutation_cache.item_count() > super::mutation::VECTOR_BUILD_ITEM_CACHE_LIMIT
        {
            if !mutation_cache.evict_oldest_item() {
                break;
            }
        }
        if let Some(cached) = mutation_cache.item(layer, node_id) {
            return Ok(cached);
        }

        while mutation_cache.enforces_local_limits()
            && mutation_cache.item_count() >= super::mutation::VECTOR_BUILD_ITEM_CACHE_LIMIT
        {
            if !mutation_cache.evict_oldest_item() {
                break;
            }
        }

        let loaded = self
            .get_item_for_layer(txn, layer, node_id)
            .await?
            .map(Arc::new);
        let payload_bytes = loaded
            .as_ref()
            .map(|item| encode_item(item.as_ref()).len())
            .unwrap_or(0);
        mutation_cache.put_item(layer, node_id, loaded.clone(), payload_bytes);
        Ok(loaded)
    }

    /// Batch-loads layer-specific items without overwriting staged cache state.
    ///
    /// The result omits absent nodes and deduplicates physical reads. Upper
    /// layers may use validated hot rows, while layer zero resolves opaque
    /// canonical payload tokens through typed storage.
    pub(super) async fn get_items_for_layer_cached_batch(
        &self,
        txn: &DbTransaction,
        layer: u16,
        node_ids: &[NodeId],
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<HashMap<NodeId, Arc<Item<'static, D>>>, HelixDbError> {
        let mut result = HashMap::new();
        if node_ids.is_empty() {
            return Ok(result);
        }
        while mutation_cache.enforces_local_limits()
            && mutation_cache.item_count() > super::mutation::VECTOR_BUILD_ITEM_CACHE_LIMIT
        {
            if !mutation_cache.evict_oldest_item() {
                break;
            }
        }

        let expected_dimension = self.expected_dimension(txn).await?;

        let mut missing = Vec::new();
        let mut seen_missing = HashSet::new();
        for &node_id in node_ids {
            if let Some(cached) = mutation_cache.item(layer, node_id) {
                if let Some(item) = cached {
                    result.insert(node_id, item);
                }
                continue;
            }

            if seen_missing.insert(node_id) {
                missing.push(node_id);
            }
        }

        if missing.is_empty() {
            return Ok(result);
        }

        if layer > 0 {
            let upper_rows = self
                .memory_access
                .read_upper_vector_rows(txn, &self.rows, &missing)
                .await?;
            let mut layer_zero_fallback = Vec::new();
            for (node_id, maybe_row) in missing.into_iter().zip(upper_rows) {
                let Some(data) = maybe_row else {
                    layer_zero_fallback.push(node_id);
                    continue;
                };
                let payload_bytes = data.len();
                let item = decode_item::<D>(&data, expected_dimension)?;
                let item = Arc::new(item);
                while mutation_cache.enforces_local_limits()
                    && mutation_cache.item_count() >= super::mutation::VECTOR_BUILD_ITEM_CACHE_LIMIT
                {
                    if !mutation_cache.evict_oldest_item() {
                        break;
                    }
                }
                mutation_cache.put_item(layer, node_id, Some(item.clone()), payload_bytes);
                result.insert(node_id, item);
            }
            missing = layer_zero_fallback;

            if missing.is_empty() {
                return Ok(result);
            }
        }

        let (canonical_keys, _) = self
            .resolve_canonical_vector_keys_batch_cached(
                txn,
                &missing,
                mutation_cache,
                "resolving canonical vector key",
            )
            .await?;

        let mut missing_simhash_ids = Vec::new();
        let mut vector_fetches = Vec::new();
        for (node_id, maybe_key) in missing.iter().copied().zip(canonical_keys) {
            match maybe_key {
                Some(key) => vector_fetches.push((node_id, key)),
                None => missing_simhash_ids.push(node_id),
            }
        }

        if !missing_simhash_ids.is_empty() {
            let layer0_rows_exist = VectorRows::new(txn, &self.rows)
                .layer0_rows_exist(&missing_simhash_ids)
                .await?;
            for (node_id, row_exists) in missing_simhash_ids.into_iter().zip(layer0_rows_exist) {
                if row_exists {
                    return Err(
                        self.missing_simhash_error(node_id, "resolving canonical vector key")
                    );
                }
                while mutation_cache.enforces_local_limits()
                    && mutation_cache.item_count() >= super::mutation::VECTOR_BUILD_ITEM_CACHE_LIMIT
                {
                    if !mutation_cache.evict_oldest_item() {
                        break;
                    }
                }
                mutation_cache.put_item(layer, node_id, None, 0);
            }
        }

        vector_fetches.sort_by(|a, b| a.1.physical_order(&b.1));
        if vector_fetches.is_empty() {
            return Ok(result);
        }
        let vector_keys = vector_fetches
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        let vector_rows = VectorRows::new(txn, &self.rows)
            .canonical_vector_rows(&vector_keys)
            .await?;

        for ((node_id, _), maybe_row) in vector_fetches.into_iter().zip(vector_rows) {
            match maybe_row {
                Some(data) => {
                    let payload_bytes = data.len();
                    let item = decode_item::<D>(&data, expected_dimension)?;
                    let item = Arc::new(item);
                    while mutation_cache.enforces_local_limits()
                        && mutation_cache.item_count()
                            >= super::mutation::VECTOR_BUILD_ITEM_CACHE_LIMIT
                    {
                        if !mutation_cache.evict_oldest_item() {
                            break;
                        }
                    }
                    mutation_cache.put_item(layer, node_id, Some(item.clone()), payload_bytes);
                    result.insert(node_id, item);
                }
                None => {
                    while mutation_cache.enforces_local_limits()
                        && mutation_cache.item_count()
                            >= super::mutation::VECTOR_BUILD_ITEM_CACHE_LIMIT
                    {
                        if !mutation_cache.evict_oldest_item() {
                            break;
                        }
                    }
                    mutation_cache.put_item(layer, node_id, None, 0);
                }
            }
        }
        Ok(result)
    }

    /// Load upper-layer neighbors.
    pub(super) async fn load_upper_neighbors(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        layer: u16,
        node_id: NodeId,
    ) -> Result<Option<Vec<NodeId>>, HelixDbError> {
        self.memory_access
            .read_upper_neighbors(txn, &self.rows, layer, node_id)
            .await
    }

    /// Delete a vector from the index
    ///
    /// # Arguments
    /// * `txn` - Database transaction
    /// * `node_id` - The node ID whose vector should be removed
    /// # Algorithm
    /// Implements Algorithm 2 from LSM-VEC paper:
    /// 1. For each layer where node exists in memory (layers 1+):
    ///    - Get neighbors of node
    ///    - Remove bidirectional edges
    ///    - Collect candidates from neighbors' neighbors
    ///    - Relink each neighbor to new connections
    /// 2. For disk layer (layer 0):
    ///    - Same relinking process
    /// 3. Remove vector data and update metadata
    #[allow(
        dead_code,
        reason = "retained as the direct legacy and migration mutation contract"
    )]
    pub async fn delete(&self, txn: &DbTransaction, node_id: NodeId) -> Result<(), HelixDbError> {
        let txn = MeasuredVectorTransaction::new(txn);
        self.stage_delete(&txn, node_id).await
    }

    #[inline]
    /// Binds a node and SimHash to its opaque deployed payload-row token.
    pub(super) fn canonical_vector_key_from_simhash(
        &self,
        node_id: NodeId,
        simhash: super::SimHash,
    ) -> CanonicalVectorRowKey {
        let order_code = order_code_from_simhash_bits(simhash.bits());
        self.rows.canonical_vector_row_key(node_id, order_code)
    }

    /// Returns the descriptor-bound directory capability for this generation.
    pub(super) const fn simhash_directory_enabled(&self) -> bool {
        self.simhash_directory_enabled
    }

    #[inline]
    /// Builds the invariant error used when canonical key derivation lacks SimHash state.
    pub(super) fn missing_simhash_error(&self, node_id: NodeId, context: &str) -> HelixDbError {
        HelixDbError::InvariantViolation(format!(
            "missing simhash for node {node_id} in index {} while {context}",
            self.id()
        ))
    }

    /// Fills an operation-local SimHash cache and returns exact physical read costs.
    ///
    /// Shared memory is consulted only when the row is not transaction-dirty;
    /// typed storage distinguishes missing, valid, and corrupt deployed rows.
    /// `COLLECT_TIMING` controls only wall-clock diagnostics; logical read counts
    /// and all cache behavior are identical in both specializations.
    pub(super) async fn fill_simhash_cache_for_nodes_counted<const COLLECT_TIMING: bool>(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        node_ids: &[NodeId],
        simhash_local_cache: &mut HashMap<NodeId, Option<super::SimHash>>,
        context: &'static str,
    ) -> Result<SimHashReadStats, HelixDbError> {
        self.memory_access
            .fill_simhash_cache::<COLLECT_TIMING, _>(
                txn,
                &self.rows,
                node_ids,
                simhash_local_cache,
                context,
            )
            .await
    }

    /// Resolves a caller-ordered node batch to optional canonical payload tokens.
    ///
    /// Mutation cleanup uses absence to represent a node with neither SimHash nor
    /// layer-zero state. Search uses
    /// [`Self::resolve_required_canonical_vector_keys_batch_counted`] so an
    /// unresolved key cannot cross its boundary.
    pub(super) async fn resolve_canonical_vector_keys_batch_counted<const COLLECT_TIMING: bool>(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        node_ids: &[NodeId],
        simhash_local_cache: &mut HashMap<NodeId, Option<super::SimHash>>,
        context: &'static str,
    ) -> Result<(Vec<Option<CanonicalVectorRowKey>>, SimHashReadStats), HelixDbError> {
        let stats = self
            .fill_simhash_cache_for_nodes_counted::<COLLECT_TIMING>(
                txn,
                node_ids,
                simhash_local_cache,
                context,
            )
            .await?;

        let keys = node_ids
            .iter()
            .map(|&node_id| {
                simhash_local_cache
                    .get(&node_id)
                    .copied()
                    .flatten()
                    .map(|hash| self.canonical_vector_key_from_simhash(node_id, hash))
            })
            .collect();

        Ok((keys, stats))
    }

    /// Resolves canonical payload tokens through the reusable mutation cache.
    async fn resolve_canonical_vector_keys_batch_cached(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        node_ids: &[NodeId],
        mutation_cache: &mut MutationOpCache<D>,
        context: &'static str,
    ) -> Result<(Vec<Option<CanonicalVectorRowKey>>, SimHashReadStats), HelixDbError> {
        let mut missing = Vec::new();
        let mut seen = HashSet::new();
        for &node_id in node_ids {
            if !seen.insert(node_id) {
                continue;
            }
            match mutation_cache.simhash(node_id) {
                Some(_) => {}
                None => missing.push(node_id),
            }
        }
        let (loaded, stats) = self
            .memory_access
            .read_simhash_rows_counted::<true, _>(txn, &self.rows, &missing, context)
            .await?;
        for (node_id, value) in missing.into_iter().zip(loaded) {
            while mutation_cache.enforces_local_limits()
                && mutation_cache.simhash_count()
                    >= super::mutation::VECTOR_BUILD_SIMHASH_CACHE_LIMIT
            {
                if !mutation_cache.evict_oldest_simhash() {
                    break;
                }
            }
            mutation_cache.put_simhash(node_id, value);
        }
        let keys = node_ids
            .iter()
            .map(|&node_id| {
                mutation_cache
                    .simhash(node_id)
                    .flatten()
                    .map(|hash| self.canonical_vector_key_from_simhash(node_id, hash))
            })
            .collect();
        Ok((keys, stats))
    }

    /// Resolves one optional canonical token through reusable SimHash state.
    pub(super) async fn resolve_canonical_vector_key_cached(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        node_id: NodeId,
        mutation_cache: &mut MutationOpCache<D>,
        context: &'static str,
    ) -> Result<(Option<CanonicalVectorRowKey>, SimHashReadStats), HelixDbError> {
        let (mut keys, mut stats) = self
            .resolve_canonical_vector_keys_batch_cached(txn, &[node_id], mutation_cache, context)
            .await?;
        let key = keys.pop().unwrap_or(None);
        if key.is_none() {
            stats.reads = stats.reads.saturating_add(1);
            if VectorRows::new(txn, &self.rows)
                .layer0_row_exists(node_id)
                .await?
            {
                return Err(self.missing_simhash_error(node_id, context));
            }
        }
        Ok((key, stats))
    }

    /// Resolves a caller-ordered node batch to complete canonical payload tokens.
    ///
    /// The non-optional result is the search boundary: a missing SimHash fails
    /// before vector-row I/O, so callers cannot accidentally handle a strict
    /// resolution as though absence were permitted.
    pub(super) async fn resolve_required_canonical_vector_keys_batch_counted<
        const COLLECT_TIMING: bool,
    >(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        node_ids: &[NodeId],
        simhash_local_cache: &mut HashMap<NodeId, Option<super::SimHash>>,
        context: &'static str,
    ) -> Result<(Vec<CanonicalVectorRowKey>, SimHashReadStats), HelixDbError> {
        let stats = self
            .fill_simhash_cache_for_nodes_counted::<COLLECT_TIMING>(
                txn,
                node_ids,
                simhash_local_cache,
                context,
            )
            .await?;
        let keys = node_ids
            .iter()
            .map(|&node_id| {
                let Some(hash) = simhash_local_cache.get(&node_id).copied().flatten() else {
                    return Err(self.missing_simhash_error(node_id, context));
                };
                Ok(self.canonical_vector_key_from_simhash(node_id, hash))
            })
            .collect::<Result<Vec<_>, HelixDbError>>()?;
        Ok((keys, stats))
    }

    /// Resolves one canonical payload token and returns its exact SimHash reads.
    ///
    /// Mutation deletion permits absence only when both the SimHash and
    /// layer-zero row are absent. Strict callers use the required batch boundary.
    pub(super) async fn resolve_canonical_vector_key_counted<const COLLECT_TIMING: bool>(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        node_id: NodeId,
        context: &'static str,
    ) -> Result<(Option<CanonicalVectorRowKey>, SimHashReadStats), HelixDbError> {
        let mut simhash_local_cache = HashMap::new();
        let (mut keys, mut stats) = self
            .resolve_canonical_vector_keys_batch_counted::<COLLECT_TIMING>(
                txn,
                &[node_id],
                &mut simhash_local_cache,
                context,
            )
            .await?;

        let key = keys.pop().unwrap_or(None);
        if key.is_none() {
            stats.reads = stats.reads.saturating_add(1);
            if VectorRows::new(txn, &self.rows)
                .layer0_row_exists(node_id)
                .await?
            {
                return Err(self.missing_simhash_error(node_id, context));
            }
        }

        Ok((key, stats))
    }

    /// Resolves one required canonical payload token with exact SimHash reads.
    ///
    /// Search and corruption fixtures use this boundary when absence is itself
    /// an invariant violation. The return type cannot represent a missing key.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(super) async fn resolve_required_canonical_vector_key_counted(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        node_id: NodeId,
        context: &'static str,
    ) -> Result<(CanonicalVectorRowKey, SimHashReadStats), HelixDbError> {
        let mut simhash_local_cache = HashMap::new();
        let stats = self
            .fill_simhash_cache_for_nodes_counted::<true>(
                txn,
                &[node_id],
                &mut simhash_local_cache,
                context,
            )
            .await?;
        let Some(hash) = simhash_local_cache.remove(&node_id).flatten() else {
            return Err(self.missing_simhash_error(node_id, context));
        };
        Ok((self.canonical_vector_key_from_simhash(node_id, hash), stats))
    }

    /// Get an item (vector + header) from the index
    ///
    /// Returns None if the item does not exist.
    pub async fn get_item(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        node_id: NodeId,
    ) -> Result<Option<Item<'static, D>>, HelixDbError> {
        let (vec_key_opt, _) = self
            .resolve_canonical_vector_key_counted::<true>(
                txn,
                node_id,
                "resolving canonical vector key",
            )
            .await?;
        let Some(vec_key) = vec_key_opt else {
            return Ok(None);
        };

        match VectorRows::new(txn, &self.rows)
            .canonical_vector_row(&vec_key)
            .await?
        {
            Some(data) => {
                let item = decode_item::<D>(&data, self.expected_dimension(txn).await?)?;
                Ok(Some(item))
            }
            None => Ok(None),
        }
    }

    /// Returns canonical vector bytes plus transactional read counts.
    pub(super) async fn get_canonical_vector_bytes_counted<const COLLECT_TIMING: bool>(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        node_id: NodeId,
    ) -> Result<(Option<bytes::Bytes>, VectorFetchReadStats), HelixDbError> {
        let (vec_key_opt, key_stats) = self
            .resolve_canonical_vector_key_counted::<COLLECT_TIMING>(
                txn,
                node_id,
                "resolving canonical vector key",
            )
            .await?;
        let mut reads = VectorFetchReadStats {
            simhash_reads: key_stats.reads,
            vector_reads: 0,
            simhash_multi_get_calls: key_stats.multi_get_calls,
            simhash_fetch_ns: key_stats.fetch_ns,
        };
        let Some(vec_key) = vec_key_opt else {
            return Ok((None, reads));
        };
        reads.vector_reads = reads.vector_reads.saturating_add(1);
        let result = VectorRows::new(txn, &self.rows)
            .canonical_vector_row(&vec_key)
            .await?;
        Ok((result, reads))
    }

    /// Get an item from upper-layer vector hot cache.
    async fn get_item_upper_hot(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        node_id: NodeId,
    ) -> Result<Option<Item<'static, D>>, HelixDbError> {
        let Some(data) = self
            .memory_access
            .read_upper_vector_row(txn, &self.rows, node_id)
            .await?
        else {
            return Ok(None);
        };
        let item = decode_item::<D>(&data, self.expected_dimension(txn).await?)?;
        Ok(Some(item))
    }

    /// Get an item for traversal at the requested layer.
    ///
    /// Upper layers try memory-hot vector cache first, then fall back to persistent vectors.
    pub(super) async fn get_item_for_layer(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        layer: u16,
        node_id: NodeId,
    ) -> Result<Option<Item<'static, D>>, HelixDbError> {
        if layer > 0
            && let Some(item) = self.get_item_upper_hot(txn, node_id).await?
        {
            return Ok(Some(item));
        }

        self.get_item(txn, node_id).await
    }

    // =========================================================================
    // Search Operations
    // =========================================================================

    /// Search for k nearest neighbors
    ///
    /// # Arguments
    /// * `txn` - Read backend snapshot/transaction
    /// * `query` - Query vector (must match index dimension)
    /// * `params` - Search parameters (k, ef)
    ///
    /// # Returns
    /// Vector of search results ordered by distance (nearest first)
    ///
    /// # Errors
    /// Returns error if:
    /// - Index does not exist
    /// - Query dimension doesn't match index dimension
    /// - Index is empty
    pub async fn search(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        query: &[f32],
        params: &SearchParams,
    ) -> Result<Vec<SearchResult>, HelixDbError> {
        SearchSession::new(self, txn, SearchObserver::disabled())
            .run(query, params)
            .await
    }

    /// Runs the same traversal while collecting crate-internal diagnostics.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) async fn search_with_stats(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        query: &[f32],
        params: &SearchParams,
    ) -> Result<(Vec<SearchResult>, super::SearchStats), HelixDbError> {
        let mut stats = super::SearchStats::default();
        let results = {
            SearchSession::new(self, txn, SearchObserver::collecting(&mut stats))
                .run(query, params)
                .await?
        };
        Ok((results, stats))
    }
}

/// Projects the current raw result identity for the cross-revision scale fixture.
#[cfg(test)]
const fn scale_result_id(result: &SearchResult) -> NodeId {
    result.entity_id()
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/index.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
#[path = "scale_contracts.rs"]
mod scale_contracts;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytemuck::{Pod, Zeroable};
    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::encoding::v2::values::indexes::vector::{
        decode_layer0_neighbors, encode_layer0_neighbors,
    };
    use crate::search::vector::distance::{Cosine, Euclidean};
    use crate::search::vector::{SimHash, VectorConfigError, VectorDimensionError};

    #[derive(Debug, Clone)]
    enum CustomDistance {}

    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, Pod, Zeroable)]
    struct CustomHeader(f32);

    impl Distance for CustomDistance {
        type Header = CustomHeader;
        type VectorCodec = f32;

        fn name() -> &'static str {
            "custom"
        }

        fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {
            CustomHeader(0.0)
        }

        fn distance(_p: &Item<Self>, _q: &Item<Self>) -> f32 {
            0.0
        }

        fn norm_no_header(_v: &UnalignedVector<Self::VectorCodec>) -> f32 {
            0.0
        }
    }

    impl super::super::distance::sealed::Sealed for CustomDistance {}

    async fn test_inner_db(name: &str) -> Arc<slatedb::Db> {
        let object_store = Arc::new(InMemory::new());
        Arc::new(slatedb::Db::open(name, object_store).await.unwrap())
    }

    async fn create_test_vector_index(
        db: &Arc<slatedb::Db>,
        index_name: &str,
    ) -> VectorIndex<Cosine> {
        let index = VectorIndex::<Cosine>::new(index_name);
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .create(&txn, VectorIndexConfig::new(index_name, "embedding", 2))
            .await
            .unwrap();
        txn.commit().await.unwrap();
        index
    }

    /// Builds canonical test cache state under the cache's configured layer limit.
    fn cached_neighbor_set<D: Distance>(
        cache: &MutationOpCache<D>,
        layer: u16,
        owner: NodeId,
        nodes: Vec<NodeId>,
    ) -> NeighborSet {
        NeighborSet::try_from_canonical(owner, cache.degree_limit(layer), nodes).unwrap()
    }

    /// Builds a canonical standalone set for delta-planning unit tests.
    fn canonical_neighbor_set(owner: NodeId, nodes: Vec<NodeId>) -> NeighborSet {
        let degree_limit = NeighborDegreeLimit::try_new(nodes.len().max(1)).unwrap();
        NeighborSet::try_from_canonical(owner, degree_limit, nodes).unwrap()
    }

    fn cached_current_neighbors<D: Distance>(
        cache: &MutationOpCache<D>,
        layer: u16,
        node_id: NodeId,
    ) -> Option<&[NodeId]> {
        cache
            .neighbor(MutationOpCache::<D>::node_row_id(layer, node_id))
            .map(|cached| match cached.current() {
                NeighborRowValue::KnownAbsent => &[][..],
                NeighborRowValue::Present(neighbors) => neighbors.as_slice(),
            })
    }

    fn cached_original_neighbors<D: Distance>(
        cache: &MutationOpCache<D>,
        layer: u16,
        node_id: NodeId,
    ) -> Option<&[NodeId]> {
        cache
            .neighbor(MutationOpCache::<D>::node_row_id(layer, node_id))
            .and_then(|cached| cached.original())
            .map(|original| match original {
                NeighborRowValue::KnownAbsent => &[][..],
                NeighborRowValue::Present(neighbors) => neighbors.as_slice(),
            })
    }

    fn install_clean_test_neighbors<D: Distance>(
        cache: &mut MutationOpCache<D>,
        layer: u16,
        node_id: NodeId,
        nodes: Vec<NodeId>,
    ) {
        let neighbors = cached_neighbor_set(cache, layer, node_id, nodes);
        cache.install_loaded_neighbor(
            MutationOpCache::<D>::node_row_id(layer, node_id),
            NeighborRowValue::Present(neighbors),
        );
    }

    fn install_dirty_test_neighbors<D: Distance>(
        cache: &mut MutationOpCache<D>,
        layer: u16,
        node_id: NodeId,
        original: Vec<NodeId>,
        current: Vec<NodeId>,
    ) {
        install_clean_test_neighbors(cache, layer, node_id, original);
        let current = cached_neighbor_set(cache, layer, node_id, current);
        cache
            .stage_loaded_neighbor(
                MutationOpCache::<D>::node_row_id(layer, node_id),
                NeighborRowValue::Present(current),
            )
            .unwrap();
    }

    #[test]
    fn simhash_cache_is_bound_once_to_the_handle_dimension() {
        let index = VectorIndex::<Cosine>::new("cached-simhasher");
        let first = index.simhash_cache(32).unwrap();
        let second = index.simhash_cache(32).unwrap();

        assert!(std::ptr::eq(first, second));
        assert!(std::ptr::eq(first.simhasher(), second.simhasher()));
        assert!(matches!(
            index.simhash_cache(64),
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("dimension changed within one handle")
        ));
    }

    #[tokio::test]
    async fn create_rejects_distance_without_stable_durable_semantics() {
        let db = test_inner_db("create_rejects_unbound_vector_distance").await;
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let index = VectorIndex::<CustomDistance>::new("custom");
        let error = index
            .create(&txn, VectorIndexConfig::new("custom", "embedding", 2))
            .await
            .unwrap_err();
        assert!(
            matches!(error, HelixDbError::Config(message) if message.contains("no stable durable semantic identity"))
        );
    }

    #[tokio::test]
    async fn create_and_reopen_reject_invalid_physical_config_before_index_work() {
        let db = test_inner_db("invalid_physical_vector_config").await;
        let index = VectorIndex::<Cosine>::new("invalid-config");

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let error = index
            .create(
                &txn,
                VectorIndexConfig::new("invalid-config", "embedding", 0),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            HelixDbError::InvalidVectorConfig(VectorConfigError::Dimension(
                VectorDimensionError::ZeroDimension
            ))
        ));
        let metadata_key = index.vector_key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            index.id(),
        )));
        assert!(txn.get(&metadata_key).await.unwrap().is_none());

        let corrupt_metadata =
            VectorIndexMetadata::new(VectorIndexConfig::new("invalid-config", "embedding", 0));
        txn.put(metadata_key, encode_metadata(&corrupt_metadata))
            .unwrap();
        txn.commit().await.unwrap();

        let read_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            index.get_metadata(&read_txn).await,
            Err(HelixDbError::InvalidVectorConfig(
                VectorConfigError::Dimension(VectorDimensionError::ZeroDimension)
            ))
        ));
        drop(read_txn);

        let contradictory_index = VectorIndex::<Cosine>::new("contradictory-metadata");
        let mut contradictory_metadata = VectorIndexMetadata::new(VectorIndexConfig::new(
            contradictory_index.name(),
            "embedding",
            2,
        ));
        contradictory_metadata.max_layer = 1;
        let contradictory_key = contradictory_index.vector_key(VectorKey::IndexMetadata(
            VectorIndexMetadataKey::new(contradictory_index.id()),
        ));
        let write_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        write_txn
            .put(contradictory_key, encode_metadata(&contradictory_metadata))
            .unwrap();
        write_txn.commit().await.unwrap();

        let read_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            contradictory_index.get_metadata(&read_txn).await,
            Err(HelixDbError::InvalidVectorConfig(
                VectorConfigError::MissingEntryPointForPopulatedLayer { max_layer: 1 }
            ))
        ));
    }

    async fn insert_test_vector(
        db: &Arc<slatedb::Db>,
        index: &VectorIndex<Cosine>,
        node_id: NodeId,
        vector: &[f32],
    ) {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.insert(&txn, node_id, vector).await.unwrap();
        txn.commit().await.unwrap();
    }

    fn cosine_test_item(vector: &[f32]) -> Item<'_, Cosine> {
        let vector = UnalignedVector::from_slice(vector);
        Item::<Cosine> {
            header: Cosine::new_header(&vector),
            vector,
        }
    }

    #[tokio::test]
    async fn get_item_rejects_a_stored_row_outside_the_validated_dimension() {
        let db = test_inner_db("stored_vector_dimension_mismatch").await;
        let index = create_test_vector_index(&db, "dimension-bound").await;
        insert_test_vector(&db, &index, 1, &[1.0, 2.0]).await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let (vector_key, _) = index
            .resolve_required_canonical_vector_key_counted(
                &txn,
                1,
                "corrupting a vector row in a decoder regression test",
            )
            .await
            .unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        VectorWriteRows::new(&measured, index.row_keyspace())
            .put_canonical_vector(
                &vector_key,
                encode_item(&cosine_test_item(&[1.0, 2.0, 3.0])),
            )
            .unwrap();

        assert!(matches!(
            index.get_item(&txn, 1).await,
            Err(HelixDbError::InvalidVectorItem(
                super::super::VectorItemDecodeError::DimensionMismatch {
                    expected: 2,
                    actual: 3,
                }
            ))
        ));
    }

    fn assert_cosine_items_match(expected: &Item<'_, Cosine>, actual: &Item<'_, Cosine>) {
        assert!(
            Cosine::distance(expected, actual) < 1e-6,
            "expected matching item, distance was {}",
            Cosine::distance(expected, actual)
        );
    }

    // Note: These tests require a real database transaction to run
    // They serve as documentation for the expected API

    #[test]
    fn test_vector_index_creation() {
        let index = VectorIndex::<Cosine>::new("embeddings");
        assert_eq!(index.name(), "embeddings");
    }

    #[tokio::test]
    async fn test_scripted_layers_drive_exact_inserted_graph_levels() {
        let db = test_inner_db("scripted_layers_drive_exact_inserted_graph_levels").await;
        let index = VectorIndex::<Cosine>::new("scripted_layers_drive_exact_inserted_graph_levels")
            .with_scripted_layers(vec![0, 1, 3])
            .unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .create(&txn, VectorIndexConfig::new(index.name(), "embedding", 2))
            .await
            .unwrap();
        txn.commit().await.unwrap();

        insert_test_vector(&db, &index, 10, &[1.0, 0.0]).await;
        insert_test_vector(&db, &index, 11, &[0.0, 1.0]).await;
        insert_test_vector(&db, &index, 12, &[0.5, 0.5]).await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let metadata = index.get_metadata(&txn).await.unwrap().unwrap();
        assert_eq!(metadata.entry_point, Some(12));
        assert_eq!(metadata.max_layer, 3);

        for node_id in [10, 11, 12] {
            let upper_vector_key = index.vector_key(VectorKey::UpperVector(
                VectorUpperVectorKey::new(index.id(), node_id),
            ));
            assert_eq!(
                txn.get(upper_vector_key).await.unwrap().is_some(),
                node_id > 10
            );
        }

        assert_eq!(
            index
                .get_entry_candidate_layer(&measured, 10)
                .await
                .unwrap(),
            Some(0)
        );
        assert_eq!(
            index
                .get_entry_candidate_layer(&measured, 11)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            index
                .get_entry_candidate_layer(&measured, 12)
                .await
                .unwrap(),
            Some(3)
        );
    }

    #[tokio::test]
    async fn measured_known_fresh_insert_reports_a_complete_hnsw_write_set() {
        let db = test_inner_db("measured_known_fresh_insert_replays_write_set").await;
        let index =
            create_test_vector_index(&db, "measured_known_fresh_insert_replays_write_set_idx")
                .await;

        let seed_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let seed = index
            .insert_known_fresh_at_layer_measured(&seed_txn, 1, &[1.0, 0.0], 0)
            .await
            .unwrap();
        assert!(seed.operations() > 0);
        assert!(seed.encoded_bytes() > 0);
        seed_txn.commit().await.unwrap();
        let planning_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let planned = index
            .insert_known_fresh_at_layer_measured(&planning_txn, 2, &[0.0, 1.0], 2)
            .await
            .unwrap();
        assert!(planned.operations() > seed.operations());
        assert!(planned.encoded_bytes() > seed.encoded_bytes());
        planning_txn.rollback();

        let replay_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let replayed = index
            .insert_known_fresh_at_layer_measured(&replay_txn, 2, &[0.0, 1.0], 2)
            .await
            .unwrap();
        assert_eq!(replayed, planned);
        replay_txn.commit().await.unwrap();

        let read = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(index.get_item(&read, 1).await.unwrap().is_some());
        assert!(index.get_item(&read, 2).await.unwrap().is_some());
        assert_eq!(
            index.get_metadata(&read).await.unwrap().unwrap().max_layer,
            2
        );
    }

    /// Proves upsert measurement equals explicit delete-plus-insert composition.
    #[tokio::test]
    async fn measured_upsert_includes_delete_and_replacement_in_one_write_set() {
        let db = test_inner_db("measured_upsert_owns_complete_write_set").await;
        let index =
            create_test_vector_index(&db, "measured_upsert_owns_complete_write_set_idx").await;
        let seed_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .stage_known_fresh_at_layer(
                &MeasuredVectorTransaction::new(&seed_txn),
                1,
                &[1.0, 0.0],
                2,
                crate::search::vector::mutation::FreshVectorBuildProof::for_test(),
            )
            .await
            .unwrap();
        seed_txn.commit().await.unwrap();

        let upsert_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured_upsert = MeasuredVectorTransaction::new(&upsert_txn);
        index
            .stage_upsert_at_layer(&measured_upsert, 1, &[0.0, 1.0], 0)
            .await
            .unwrap();

        let composed_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured_composition = MeasuredVectorTransaction::new(&composed_txn);
        index.stage_delete(&measured_composition, 1).await.unwrap();
        index
            .stage_known_fresh_at_layer(
                &measured_composition,
                1,
                &[0.0, 1.0],
                0,
                crate::search::vector::mutation::FreshVectorBuildProof::for_test(),
            )
            .await
            .unwrap();

        assert_eq!(
            measured_upsert.measurement().unwrap(),
            measured_composition.measurement().unwrap()
        );
        upsert_txn.rollback();
        composed_txn.rollback();
    }

    #[test]
    fn test_config_validation() {
        let config = VectorIndexConfig::new("test", "emb", 128);
        assert_eq!(config.dimension, 128);
        assert_eq!(config.property_name, "emb");
    }

    #[test]
    fn test_handle_modes_scopes_and_candidate_ordering_contracts() {
        let scope = DataScope::Tenant(
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000A")
                .unwrap(),
        );
        let scoped = VectorIndex::<Cosine>::new_scoped("scoped", scope);
        let physical = scoped.vector_key(VectorKey::TxnGuard(VectorTxnGuardKey::new(scoped.id())));
        assert!(scoped.strip_physical_key(&physical).is_ok());
        assert!(scoped.strip_physical_key(b"outside-tenant").is_err());

        let store = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            index_id_from_name("modes"),
            u64::MAX,
        ));
        let managed_read = VectorIndex::<Cosine>::new("modes")
            .with_managed_read_cache(
                Arc::clone(&store),
                Arc::new(VectorMemoryPendingDirtyRows::default()),
            )
            .unwrap();
        assert!(!managed_read.is_memory_node_dirty(1));
        assert!(!managed_read.is_memory_upper_neighbors_dirty(1, 1));
        let wrong_store = Arc::new(VectorMemoryStore::new(
            scope,
            index_id_from_name("modes"),
            u64::MAX,
        ));
        assert!(matches!(
            VectorIndex::<Cosine>::new("modes").with_managed_read_cache(
                wrong_store,
                Arc::new(VectorMemoryPendingDirtyRows::default()),
            ),
            Err(crate::search::vector::VectorGenerationValidationError::CacheIdentityMismatch)
        ));

        let dirty = Arc::new(VectorMemoryDirtyRows::default());
        let write = VectorIndex::<Cosine>::new("modes").with_write_dirty_rows(Arc::clone(&dirty));
        write.mark_memory_node_dirty(7);
        write.mark_memory_upper_neighbors_dirty(2, 9);
        assert!(write.is_memory_node_dirty(7));
        assert!(write.is_memory_upper_neighbors_dirty(2, 9));

        let close = Candidate::try_new(1, 1.0).unwrap();
        let far = Candidate::try_new(2, 2.0).unwrap();
        let same_distance = Candidate::try_new(3, 1.0).unwrap();
        assert!(far > close);
        assert!(same_distance > close);
        assert_eq!(close.partial_cmp(&far), Some(std::cmp::Ordering::Less));
        assert!(Candidate::try_new(4, f32::NAN).is_err());
        assert!(Candidate::try_new(4, f32::INFINITY).is_err());
        assert!(Candidate::try_new(4, -1.0).is_err());
        assert_eq!(
            VectorFetchReadStats {
                simhash_reads: 2,
                vector_reads: 3,
                simhash_multi_get_calls: 1,
                simhash_fetch_ns: 4,
            }
            .total_reads(),
            5
        );
    }

    #[tokio::test]
    async fn current_mutations_never_write_legacy_guard_and_drop_scrubs_it() {
        let db = test_inner_db("current_mutations_without_legacy_guard").await;
        let index =
            create_test_vector_index(&db, "current_mutations_without_legacy_guard_idx").await;
        let guard_key = index.vector_key(VectorKey::TxnGuard(VectorTxnGuardKey::new(index.id())));

        assert!(db.get(&guard_key).await.unwrap().is_none());
        insert_test_vector(&db, &index, 1, &[1.0, 0.0]).await;
        assert!(db.get(&guard_key).await.unwrap().is_none());
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.delete(&txn, 1).await.unwrap();
        txn.commit().await.unwrap();
        assert!(db.get(&guard_key).await.unwrap().is_none());

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(
            &guard_key,
            crate::encoding::v2::legacy::vector::transaction_guard::encode_active_txn_guard(),
        )
        .unwrap();
        txn.commit().await.unwrap();
        assert!(db.get(&guard_key).await.unwrap().is_some());

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.drop(&txn).await.unwrap();
        txn.commit().await.unwrap();
        assert!(db.get(&guard_key).await.unwrap().is_none());
        assert!(index.get_metadata(db.as_ref()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_public_index_lifecycle_covers_errors_search_stats_and_drop() {
        let db = test_inner_db("public_vector_index_lifecycle").await;
        let index = VectorIndex::<Cosine>::new("public_vector_index_lifecycle_idx");
        let params = SearchParams::new(3).unwrap().with_ef(8).unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(index.get_metadata(&txn).await.unwrap().is_none());
        assert!(matches!(
            index.search(&txn, &[1.0, 0.0], &params).await,
            Err(HelixDbError::IndexNotFound(_))
        ));
        assert!(matches!(
            index.search_with_stats(&txn, &[1.0, 0.0], &params).await,
            Err(HelixDbError::IndexNotFound(_))
        ));
        assert!(matches!(
            index.insert(&txn, 1, &[1.0, 0.0]).await,
            Err(HelixDbError::IndexNotFound(_))
        ));
        assert!(matches!(
            index.delete(&txn, 1).await,
            Err(HelixDbError::IndexNotFound(_))
        ));
        assert!(index
            .create(
                &txn,
                VectorIndexConfig::new("different-name", "embedding", 2),
            )
            .await
            .is_err());
        index
            .create(&txn, VectorIndexConfig::new(index.name(), "embedding", 2))
            .await
            .unwrap();
        assert!(matches!(
            index
                .create(&txn, VectorIndexConfig::new(index.name(), "embedding", 2),)
                .await,
            Err(HelixDbError::IndexAlreadyExists(_))
        ));
        assert!(index
            .search(&txn, &[1.0, 0.0], &params)
            .await
            .unwrap()
            .is_empty());
        let (empty, empty_stats) = index
            .search_with_stats(&txn, &[1.0, 0.0], &params)
            .await
            .unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty_stats.expansion_steps, 0);
        assert_eq!(empty_stats.txn_get_total, 0);
        assert!(matches!(
            index.search(&txn, &[1.0], &params).await,
            Err(HelixDbError::InvalidDimension {
                expected: 2,
                got: 1,
            })
        ));
        assert!(matches!(
            index.search_with_stats(&txn, &[1.0], &params).await,
            Err(HelixDbError::InvalidDimension {
                expected: 2,
                got: 1,
            })
        ));
        assert!(matches!(
            index.insert(&txn, 1, &[1.0]).await,
            Err(HelixDbError::InvalidDimension {
                expected: 2,
                got: 1,
            })
        ));
        assert!(matches!(
            index.insert(&txn, 1, &[f32::NAN, 0.0]).await,
            Err(HelixDbError::InvalidVectorComponent { index: 0 })
        ));
        assert!(matches!(
            index.search(&txn, &[0.0, f32::INFINITY], &params).await,
            Err(HelixDbError::InvalidVectorComponent { index: 1 })
        ));
        assert!(matches!(
            index
                .search_with_stats(&txn, &[f32::NEG_INFINITY, 0.0], &params)
                .await,
            Err(HelixDbError::InvalidVectorComponent { index: 0 })
        ));
        assert!(matches!(
            index.insert(&txn, 1, &[0.0, 0.0]).await,
            Err(HelixDbError::ZeroNormCosineVector)
        ));
        assert!(matches!(
            index.search(&txn, &[0.0, 0.0], &params).await,
            Err(HelixDbError::ZeroNormCosineVector)
        ));
        index.delete(&txn, 999).await.unwrap();
        txn.commit().await.unwrap();

        insert_test_vector(&db, &index, 1, &[1.0, 0.0]).await;
        insert_test_vector(&db, &index, 2, &[0.0, 1.0]).await;
        insert_test_vector(&db, &index, 3, &[0.8, 0.2]).await;
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(index.get_item(&txn, 999).await.unwrap().is_none());
        let (results, stats) = index
            .search_with_stats(&txn, &[1.0, 0.0], &params)
            .await
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].entity_id(), 1);
        assert!(stats.expansion_steps > 0);
        assert!(SearchParams::new(0).is_err());
        drop(txn);

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let metadata_key = index.vector_key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            index.id(),
        )));
        txn.put(&metadata_key, bytes::Bytes::from_static(b"malformed"))
            .unwrap();
        assert!(matches!(
            index.get_metadata(&txn).await,
            Err(HelixDbError::Encoding(_))
        ));
        drop(txn);

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.delete(&txn, 2).await.unwrap();
        index.delete(&txn, 2).await.unwrap();
        txn.put(
            index.vector_key(VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(
                index.id(),
            ))),
            bytes::Bytes::from_static(b"hot"),
        )
        .unwrap();
        txn.put(
            index.vector_key(VectorKey::L0Prefix(VectorL0PrefixKey::new(index.id()))),
            bytes::Bytes::from_static(b"l0"),
        )
        .unwrap();
        index.drop(&txn).await.unwrap();
        index.drop(&txn).await.unwrap();
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(index.get_metadata(&txn).await.unwrap().is_none());
        assert!(index.get_item(&txn, 1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_phase0_public_result_and_io_baseline() {
        let db = test_inner_db("phase0_public_result_and_io_baseline").await;
        let index = VectorIndex::<Cosine>::new("phase0_public_result_and_io_baseline_idx")
            .with_scripted_layers(vec![0, 1, 2, 0])
            .unwrap()
            .with_search_seed(0x5EED);
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .create(
                &txn,
                VectorIndexConfig::new(index.name(), "embedding", 2)
                    .with_m(4)
                    .with_m0(8)
                    .with_ef_construction(16),
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        for (node_id, vector) in [
            (1, [1.0, 0.0]),
            (2, [0.0, 1.0]),
            (3, [-1.0, 0.0]),
            (4, [0.0, -1.0]),
        ] {
            insert_test_vector(&db, &index, node_id, &vector).await;
        }

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let params = SearchParams::new(4)
            .unwrap()
            .with_ef(16)
            .unwrap()
            .with_simhash_mode(SimHashMode::Off)
            .with_pre_simhash_sampling_ratio(1.0)
            .unwrap();
        let (results, stats) = index
            .search_with_stats(&txn, &[1.0, 0.0], &params)
            .await
            .unwrap();
        let plain_results = index.search(&txn, &[1.0, 0.0], &params).await.unwrap();

        assert_eq!(
            plain_results
                .iter()
                .map(|result| (result.entity_id(), result.score().get().to_bits()))
                .collect::<Vec<_>>(),
            results
                .iter()
                .map(|result| (result.entity_id(), result.score().get().to_bits()))
                .collect::<Vec<_>>()
        );

        assert_eq!(
            results
                .iter()
                .map(|result| (result.entity_id(), result.score().get().to_bits()))
                .collect::<Vec<_>>(),
            vec![
                (1, 0.0f32.to_bits()),
                (2, 0.5f32.to_bits()),
                (4, 0.5f32.to_bits()),
                (3, 1.0f32.to_bits()),
            ]
        );
        assert_eq!(stats.expansion_steps, 4);
        assert_eq!(stats.neighbors_examined, 12);
        assert_eq!(stats.vectors_loaded, 3);
        assert_eq!(stats.distance_computations, 4);
        assert_eq!(stats.txn_get_total, 12);
        assert_eq!(stats.txn_get_neighbors, 4);
        assert_eq!(stats.txn_get_simhash_filter, 0);
        assert_eq!(stats.txn_get_simhash_key_derivation, 4);
        assert_eq!(stats.txn_get_simhash, 4);
        assert_eq!(stats.txn_get_vectors, 4);
        assert_eq!(stats.txn_multi_get_calls_total, 3);
        assert_eq!(stats.txn_multi_get_calls_simhash_filter, 0);
        assert_eq!(stats.txn_multi_get_calls_simhash_key_derivation, 2);
        assert_eq!(stats.txn_multi_get_calls_simhash, 2);
        assert_eq!(stats.txn_multi_get_calls_vectors, 1);
        assert_eq!(
            stats.txn_get_total,
            stats
                .txn_get_neighbors
                .saturating_add(stats.txn_get_simhash)
                .saturating_add(stats.txn_get_vectors)
        );
        assert_eq!(
            stats.txn_multi_get_calls_total,
            stats
                .txn_multi_get_calls_simhash
                .saturating_add(stats.txn_multi_get_calls_vectors)
        );
    }

    #[tokio::test]
    async fn test_layer0_search_modes_cover_sampling_filtering_and_adaptive_bypass() {
        let db = test_inner_db("layer0_search_modes").await;
        let index = VectorIndex::<Cosine>::new("layer0_search_modes_idx");
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .create(
                &txn,
                VectorIndexConfig::new(index.name(), "embedding", 2)
                    .with_m(8)
                    .with_m0(16)
                    .with_ef_construction(32)
                    .with_simhash_threshold(0)
                    .with_sampling_ratio(0.5),
            )
            .await
            .unwrap();
        for offset in 0..32u64 {
            let angle = (offset as f32) * std::f32::consts::TAU / 32.0;
            index
                .insert_with_contract(
                    &txn,
                    offset + 1,
                    &[angle.cos(), angle.sin()],
                    VectorInsertContract::ProvenFresh(
                        crate::search::vector::mutation::FreshVectorBuildProof::for_test(),
                    ),
                )
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let (off_results, off_stats) = index
            .search_with_stats(
                &txn,
                &[1.0, 0.0],
                &SearchParams::new(5)
                    .unwrap()
                    .with_ef(16)
                    .unwrap()
                    .with_simhash_mode(SimHashMode::Off)
                    .with_pre_simhash_sampling_ratio(1.0)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!off_results.is_empty());
        assert_eq!(off_stats.simhash_examined, 0);
        assert_eq!(off_stats.txn_get_simhash_filter, 0);
        assert_eq!(off_stats.simhash_filtered, 0);
        assert!(off_stats.expansion_steps > 0);

        let (always_results, always_stats) = index
            .search_with_stats(
                &txn,
                &[1.0, 0.0],
                &SearchParams::new(5)
                    .unwrap()
                    .with_ef(16)
                    .unwrap()
                    .with_simhash_mode(SimHashMode::Always)
                    .with_pre_simhash_sampling_ratio(1.0)
                    .unwrap()
                    .with_simhash_sampling_ratio(0.0)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!always_results.is_empty());
        assert!(always_stats.simhash_examined > 0);
        assert!(always_stats.simhash_passed_before_sampling > 0);
        assert_eq!(always_stats.avg_active_simhash_threshold, 0.0);

        let (fixed_exhaustive_results, fixed_exhaustive_stats) = index
            .search_with_stats(
                &txn,
                &[1.0, 0.0],
                &SearchParams::new(5)
                    .unwrap()
                    .with_ef(16)
                    .unwrap()
                    .with_simhash_mode(SimHashMode::Always)
                    .with_pre_simhash_sampling_ratio(1.0)
                    .unwrap()
                    .with_simhash_sampling_ratio(1.0)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            fixed_exhaustive_results
                .iter()
                .map(|result| result.entity_id())
                .collect::<Vec<_>>(),
            off_results
                .iter()
                .map(|result| result.entity_id())
                .collect::<Vec<_>>()
        );

        let (adaptive_results, adaptive_stats) = index
            .search_with_stats(
                &txn,
                &[0.0, 1.0],
                &SearchParams::new(5)
                    .unwrap()
                    .with_ef(16)
                    .unwrap()
                    .with_simhash_mode(SimHashMode::Adaptive)
                    .with_pre_simhash_sampling_ratio(0.25)
                    .unwrap()
                    .with_simhash_sampling_ratio(0.5)
                    .unwrap()
                    .with_simhash_failure_prob(0.5)
                    .unwrap()
                    .with_simhash_bypass_tuning(1, 1, 1.0, 1)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!adaptive_results.is_empty());
        assert!(adaptive_stats.expansion_steps > 0);
        assert!(adaptive_stats.txn_get_total > 0);
        assert!(adaptive_stats.avg_effective_beam_len >= 1.0);
        assert!(adaptive_stats.simhash_bypass_expansions > 0);

        for stats in [
            &off_stats,
            &always_stats,
            &fixed_exhaustive_stats,
            &adaptive_stats,
        ] {
            assert_eq!(
                stats.txn_get_simhash,
                stats
                    .txn_get_simhash_filter
                    .saturating_add(stats.txn_get_simhash_key_derivation)
            );
            assert_eq!(
                stats.txn_multi_get_calls_simhash,
                stats
                    .txn_multi_get_calls_simhash_filter
                    .saturating_add(stats.txn_multi_get_calls_simhash_key_derivation)
            );
            assert_eq!(
                stats.simhash_fetch_ns,
                stats
                    .simhash_fetch_ns_filter
                    .saturating_add(stats.simhash_fetch_ns_key_derivation)
            );
        }
    }

    #[tokio::test]
    async fn non_angular_metric_disables_filter_phase_without_disabling_sampling_policy() {
        let db = test_inner_db("euclidean_filter_policy_disabled").await;
        let index = VectorIndex::<Euclidean>::new("euclidean_filter_policy_disabled_idx");
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .create(
                &txn,
                VectorIndexConfig::new(index.name(), "embedding", 2)
                    .with_simhash_threshold(64)
                    .with_sampling_ratio(0.5),
            )
            .await
            .unwrap();
        for node_id in 1..=24 {
            index
                .insert(&txn, node_id, &[node_id as f32, 1.0])
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let (results, stats) = index
            .search_with_stats(
                &txn,
                &[1.0, 1.0],
                &SearchParams::new(5)
                    .unwrap()
                    .with_ef(16)
                    .unwrap()
                    .with_simhash_mode(SimHashMode::Always)
                    .with_pre_simhash_sampling_ratio(1.0)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(stats.txn_get_simhash_filter, 0);
        assert_eq!(stats.simhash_examined, 0);
        assert_eq!(stats.simhash_filtered, 0);
        assert_eq!(stats.avg_active_sampling_ratio, 0.5);
    }

    #[test]
    fn test_decode_layer0_neighbors_from_compact_payload() {
        let encoded = encode_layer0_neighbors(&[4, 2, 2]);
        let decoded = decode_layer0_neighbors(&encoded).expect("compact payload should decode");
        assert_eq!(decoded, vec![2, 4]);
    }

    #[test]
    fn test_layer0_neighbor_deltas_detects_adds_and_removals() {
        let degree_limit = NeighborDegreeLimit::try_new(3).unwrap();
        let old_neighbors =
            NeighborSet::try_from_canonical(99, degree_limit, vec![2, 4, 8]).unwrap();
        let new_neighbors =
            NeighborSet::try_from_canonical(99, degree_limit, vec![4, 8, 16]).unwrap();

        let (removed, added) =
            VectorIndex::<Cosine>::neighbor_deltas(&old_neighbors, &new_neighbors)
                .unwrap()
                .into_parts();

        assert_eq!(removed, vec![2]);
        assert_eq!(added, vec![16]);
    }

    #[tokio::test]
    async fn unchanged_canonical_neighbors_flush_without_row_or_reverse_writes() {
        let db = test_inner_db("unchanged_neighbors_have_no_writes").await;
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let index = VectorIndex::<Cosine>::new("embeddings");
        let key = (0, 99);
        let neighbors = canonical_neighbor_set(key.1, vec![2, 4, 8]);
        let mut mutation_cache = MutationOpCache::<Cosine>::default();
        let row = MutationOpCache::<Cosine>::node_row_id(key.0, key.1);
        mutation_cache.install_loaded_neighbor(row, NeighborRowValue::Present(neighbors.clone()));
        mutation_cache
            .stage_loaded_neighbor(row, NeighborRowValue::Present(neighbors))
            .unwrap();

        index
            .flush_one_cached_neighbor(&measured, &mut mutation_cache, row, false)
            .await
            .unwrap();

        assert_eq!(measured.measurement().unwrap().operations(), 0);
        assert_eq!(measured.measurement().unwrap().encoded_bytes(), 0);
    }

    #[tokio::test]
    async fn failed_neighbor_flush_retains_dirty_state_for_exact_retry() {
        let db = test_inner_db("failed_neighbor_flush_retries_exactly").await;
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let index = VectorIndex::<Cosine>::new("embeddings");
        let row = MutationOpCache::<Cosine>::node_row_id(0, 99);
        let mut mutation_cache = MutationOpCache::<Cosine>::default();
        install_dirty_test_neighbors(&mut mutation_cache, 0, 99, vec![2], vec![3]);

        measured.fail_next_write();
        assert!(index
            .flush_one_cached_neighbor(&measured, &mut mutation_cache, row, false)
            .await
            .is_err());
        assert_eq!(
            cached_original_neighbors(&mutation_cache, 0, 99),
            Some(&[2][..])
        );
        assert_eq!(
            cached_current_neighbors(&mutation_cache, 0, 99),
            Some(&[3][..])
        );
        assert_eq!(mutation_cache.oldest_dirty_neighbor(), Some(row));
        assert_eq!(measured.measurement().unwrap().operations(), 0);

        index
            .flush_one_cached_neighbor(&measured, &mut mutation_cache, row, false)
            .await
            .unwrap();
        assert_eq!(mutation_cache.oldest_dirty_neighbor(), None);
        assert_eq!(measured.measurement().unwrap().operations(), 3);

        index
            .flush_one_cached_neighbor(&measured, &mut mutation_cache, row, false)
            .await
            .unwrap();
        assert_eq!(measured.measurement().unwrap().operations(), 3);
    }

    #[tokio::test]
    async fn test_stage_neighbors_preserves_first_original_snapshot() {
        let db = test_inner_db("stage_neighbors_preserves_original").await;
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let index = VectorIndex::<Cosine>::new("embeddings");
        let key = (0, 42);
        let mut mutation_cache = MutationOpCache::<Cosine>::default();
        install_clean_test_neighbors(&mut mutation_cache, key.0, key.1, vec![1, 2]);

        index
            .stage_neighbors_vec_for_mutation(
                &measured,
                key.0,
                key.1,
                vec![1, 2, 3],
                &mut mutation_cache,
            )
            .await
            .unwrap();
        index
            .stage_neighbors_vec_for_mutation(
                &measured,
                key.0,
                key.1,
                vec![3, 4],
                &mut mutation_cache,
            )
            .await
            .unwrap();

        assert_eq!(
            cached_original_neighbors(&mutation_cache, key.0, key.1),
            Some([1, 2].as_slice())
        );
        assert_eq!(
            cached_current_neighbors(&mutation_cache, key.0, key.1),
            Some([3, 4].as_slice())
        );
    }

    #[tokio::test]
    async fn staging_neighbors_canonicalizes_order_and_rejects_invalid_sets_without_writes() {
        let db = test_inner_db("stage_neighbors_validates_canonical_set").await;
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let index = VectorIndex::<Cosine>::new("embeddings");
        let mut mutation_cache = MutationOpCache::<Cosine>::with_degree_limits(2, 1).unwrap();

        let unloaded = index
            .stage_neighbors_vec_for_mutation(&measured, 0, 42, vec![3, 2], &mut mutation_cache)
            .await
            .unwrap_err();
        assert!(
            matches!(unloaded, HelixDbError::InvariantViolation(message) if message.contains("unloaded"))
        );

        index
            .stage_new_neighbors_for_mutation(&measured, 0, 42, vec![3, 2], &mut mutation_cache)
            .await
            .unwrap();
        assert_eq!(
            cached_current_neighbors(&mutation_cache, 0, 42),
            Some([2, 3].as_slice())
        );

        for (layer, owner, invalid) in [(0, 42, vec![2, 2]), (0, 42, vec![42]), (1, 42, vec![2, 3])]
        {
            let error = index
                .stage_neighbors_vec_for_mutation(
                    &measured,
                    layer,
                    owner,
                    invalid,
                    &mut mutation_cache,
                )
                .await
                .unwrap_err();
            assert!(matches!(error, HelixDbError::InvariantViolation(_)));
        }

        assert_eq!(measured.measurement().unwrap().operations(), 0);
        assert_eq!(measured.measurement().unwrap().encoded_bytes(), 0);
    }

    #[test]
    fn test_clean_neighbor_cache_entry_evicts_without_original_snapshot() {
        let index = VectorIndex::<Cosine>::new("embeddings");
        let key = (0, 42);
        let mut mutation_cache = MutationOpCache::<Cosine>::default();
        install_clean_test_neighbors(&mut mutation_cache, key.0, key.1, vec![1, 2]);

        assert!(index.evict_oldest_clean_neighbor(&mut mutation_cache));
        let row = MutationOpCache::<Cosine>::node_row_id(key.0, key.1);
        assert!(!mutation_cache.contains_neighbor(row));
        assert_eq!(mutation_cache.oldest_dirty_neighbor(), None);
    }

    #[test]
    fn test_fixed_search_seed_replays_independently_of_query_inputs() {
        let index = VectorIndex::<Cosine>::new("fixed_search_seed").with_search_seed(7);
        let mut first = index.search_randomness.start(&SimHash::from_bits(1), 2, 3);
        let mut second =
            index
                .search_randomness
                .start(&SimHash::from_bits(u64::MAX), u64::MAX, usize::MAX);

        for _ in 0..100 {
            assert_eq!(first.should_sample(0.37), second.should_sample(0.37));
            assert_eq!(first.choose_index(11), second.choose_index(11));
        }
    }

    #[test]
    fn test_mark_sampled_neighbors_visited_allows_deferred_reconsideration() {
        let mut visited = HashSet::new();
        visited.insert(1);

        // First encounter: neighbor 2 is deferred (not sampled), so it must remain unvisited.
        let first_pass = search::mark_sampled_neighbors_visited(&mut visited, vec![]);
        assert!(first_pass.is_empty());
        assert!(!visited.contains(&2));

        // Later encounter: neighbor 2 is sampled and should now be accepted.
        let second_pass = search::mark_sampled_neighbors_visited(&mut visited, vec![(2, 60)]);
        assert_eq!(second_pass, vec![(2, 60)]);
        assert!(visited.contains(&2));
    }

    #[test]
    fn test_mark_sampled_neighbors_visited_deduplicates_with_existing_visited() {
        let mut visited = HashSet::new();
        visited.insert(3);

        let accepted =
            search::mark_sampled_neighbors_visited(&mut visited, vec![(3, 58), (4, 59), (4, 59)]);
        assert_eq!(accepted, vec![(4, 59)]);
    }

    #[test]
    fn test_select_layer0_neighbor_prefetch_targets_prefers_closest_uncached_nodes() {
        let admitted = vec![(14u64, 0.05), (12u64, 0.10), (13u64, 0.22), (11u64, 0.31)];

        let mut neighbor_cache = HashMap::new();
        neighbor_cache.insert(14u64, vec![1, 2]);

        let mut prefetched_neighbor_cache = HashMap::new();
        prefetched_neighbor_cache.insert(13u64, vec![3, 4]);

        let targets = search::select_layer0_neighbor_prefetch_targets(
            &admitted,
            &neighbor_cache,
            &prefetched_neighbor_cache,
            8,
        );

        assert_eq!(targets, vec![12u64, 11u64]);
    }

    #[test]
    fn test_select_layer0_neighbor_prefetch_targets_respects_minimum_and_budget() {
        let single_admitted = vec![(5u64, 0.2)];
        let empty_cache = HashMap::new();
        let empty_prefetched = HashMap::new();

        let skipped = search::select_layer0_neighbor_prefetch_targets(
            &single_admitted,
            &empty_cache,
            &empty_prefetched,
            8,
        );
        assert!(skipped.is_empty());

        let admitted = vec![(5u64, 0.2), (2u64, 0.1), (8u64, 0.4)];
        let budget_limited = search::select_layer0_neighbor_prefetch_targets(
            &admitted,
            &empty_cache,
            &empty_prefetched,
            1,
        );

        assert_eq!(budget_limited, vec![2u64]);
    }

    #[tokio::test]
    async fn test_resolve_canonical_key_missing_simhash_is_invariant_violation() {
        let db = test_inner_db("resolve_missing_simhash_invariant").await;
        let index = VectorIndex::<Cosine>::new("resolve_missing_simhash_invariant_idx");
        let node_id = 91u64;
        let layer0_key =
            VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(index.id(), node_id))
                .to_bytes();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(&layer0_key, bytes::Bytes::from_static(b"malformed"))
            .unwrap();
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let err = index
            .resolve_canonical_vector_key_counted::<true>(
                &txn,
                node_id,
                "resolving canonical vector key in test",
            )
            .await
            .expect_err("expected missing simhash to fail when node exists");

        let err_debug = format!("{err:?}");
        let HelixDbError::InvariantViolation(message) = err else {
            panic!("expected InvariantViolation, got {err_debug}");
        };
        assert!(message.contains("missing simhash"));
        assert!(message.contains("node 91"));
    }

    #[tokio::test]
    async fn test_insert_repairs_stale_metadata_entry_point() {
        let db = test_inner_db("insert_repairs_stale_metadata_entry_point").await;
        let index =
            create_test_vector_index(&db, "insert_repairs_stale_metadata_entry_point_idx").await;

        insert_test_vector(&db, &index, 1, &[1.0, 0.0]).await;
        insert_test_vector(&db, &index, 2, &[0.0, 1.0]).await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let mut metadata = index.get_metadata(&txn).await.unwrap().unwrap();
        metadata.entry_point = Some(9_999);
        metadata.max_layer = metadata.max_layer.max(1);
        {
            let measured = MeasuredVectorTransaction::new(&txn);
            index.update_metadata(&measured, &metadata).await.unwrap();
        }
        txn.commit().await.unwrap();

        insert_test_vector(&db, &index, 3, &[0.5, 0.5]).await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let repaired = index.get_metadata(&txn).await.unwrap().unwrap();
        let entry_point = repaired
            .entry_point
            .expect("entry point should be repaired");
        assert!(index.get_item(&txn, entry_point).await.unwrap().is_some());
        assert_eq!(repaired.count, 3);
    }

    #[tokio::test]
    async fn test_insert_multiple_vectors_in_one_transaction_with_pending_write() {
        let db = test_inner_db("insert_multi_vectors_one_txn_pending_write").await;
        let index =
            create_test_vector_index(&db, "insert_multi_vectors_one_txn_pending_write_idx").await;

        let vectors = [
            [1.0f32, 0.0f32],
            [0.0f32, 1.0f32],
            [0.7f32, 0.3f32],
            [0.3f32, 0.7f32],
            [0.9f32, 0.1f32],
            [0.1f32, 0.9f32],
        ];

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(
            b"unrelated_pending_write",
            bytes::Bytes::from_static(b"value"),
        )
        .unwrap();
        for (idx, vector) in vectors.iter().enumerate() {
            index
                .insert(&txn, (idx + 1) as NodeId, vector)
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for node_id in 1..=vectors.len() as NodeId {
            assert!(index.get_item(&txn, node_id).await.unwrap().is_some());
        }

        let results = index
            .search(
                &txn,
                &[0.8, 0.2],
                &SearchParams::new(3).unwrap().with_ef(8).unwrap(),
            )
            .await
            .unwrap();
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_known_fresh_insert_adds_new_vector() {
        let db = test_inner_db("known_fresh_insert_adds_new_vector").await;
        let index = create_test_vector_index(&db, "known_fresh_insert_adds_new_vector_idx")
            .await
            .with_scripted_layers(vec![0])
            .expect("known-fresh contract uses one deterministic layer");

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .insert_with_contract(
                &txn,
                1,
                &[1.0, 0.0],
                VectorInsertContract::ProvenFresh(
                    crate::search::vector::mutation::FreshVectorBuildProof::for_test(),
                ),
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let item = index
            .get_item(&txn, 1)
            .await
            .unwrap()
            .expect("known-fresh insert should persist the vector");
        assert_eq!(item.vector.to_vec(), vec![1.0, 0.0]);
        assert_eq!(index.get_metadata(&txn).await.unwrap().unwrap().count, 1);
        let measured = MeasuredVectorTransaction::new(&txn);
        assert_eq!(
            index.get_entry_candidate_layer(&measured, 1).await.unwrap(),
            Some(0)
        );
        assert_eq!(
            index.find_best_entry_candidate(&measured).await.unwrap(),
            Some((1, 0))
        );
    }

    #[tokio::test]
    async fn test_upsert_insert_replaces_existing_vector() {
        let db = test_inner_db("upsert_insert_replaces_existing_vector").await;
        let index =
            create_test_vector_index(&db, "upsert_insert_replaces_existing_vector_idx").await;

        insert_test_vector(&db, &index, 1, &[1.0, 0.0]).await;
        insert_test_vector(&db, &index, 1, &[0.0, 1.0]).await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let item = index
            .get_item(&txn, 1)
            .await
            .unwrap()
            .expect("upsert should keep the replacement vector");
        assert_eq!(item.vector.to_vec(), vec![0.0, 1.0]);
        assert_eq!(index.get_metadata(&txn).await.unwrap().unwrap().count, 1);
    }

    /// Exhausts small layer assignments for entry deletion plus reinsertion.
    #[tokio::test]
    async fn test_upsert_entry_reconnects_layer0_for_every_layer_assignment() {
        let db = test_inner_db("upsert_entry_reconnects_layer0").await;
        for first_layer in 0..=2 {
            for fallback_layer in 0..=2 {
                for replacement_layer in 0..=2 {
                    let name = format!(
                        "upsert-entry-reconnect-{first_layer}-{fallback_layer}-{replacement_layer}"
                    );
                    let index = VectorIndex::<Cosine>::new(&name)
                        .with_scripted_layers(vec![first_layer, fallback_layer, replacement_layer])
                        .unwrap();
                    let create = db.begin(IsolationLevel::Snapshot).await.unwrap();
                    index
                        .create(&create, VectorIndexConfig::new(&name, "embedding", 2))
                        .await
                        .unwrap();
                    index.insert(&create, 0, &[1.0, 0.0]).await.unwrap();
                    index.insert(&create, 1, &[0.0, 1.0]).await.unwrap();
                    create.commit().await.unwrap();

                    let replace = db.begin(IsolationLevel::Snapshot).await.unwrap();
                    index.delete(&replace, 0).await.unwrap();
                    index.insert(&replace, 0, &[-1.0, 0.0]).await.unwrap();
                    replace.commit().await.unwrap();

                    let read = db.snapshot().await.unwrap();
                    let replacement_neighbors =
                        index.load_neighbors_layer0(read.as_ref(), 0).await.unwrap();
                    let fallback_neighbors =
                        index.load_neighbors_layer0(read.as_ref(), 1).await.unwrap();
                    assert!(
                        replacement_neighbors.contains(&1) && fallback_neighbors.contains(&0),
                        "entry replacement did not restore bidirectional layer-0 links for layers {first_layer}/{fallback_layer}/{replacement_layer}: replacement={replacement_neighbors:?}, fallback={fallback_neighbors:?}"
                    );
                    let results = index
                        .search(read.as_ref(), &[1.0, 0.0], &SearchParams::new(1).unwrap())
                        .await
                        .unwrap();
                    assert_eq!(
                        results.first().map(|result| result.entity_id()),
                        Some(1),
                        "entry replacement lost the closer fallback for layers {first_layer}/{fallback_layer}/{replacement_layer}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_batch_item_fetch_ignores_dirty_upper_vector_cache_in_write_mode() {
        let db = test_inner_db("batch_fetch_dirty_upper_vector_cache").await;
        let base_index = VectorIndex::<Cosine>::new("batch_fetch_dirty_upper_vector_cache_idx");
        let store = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            base_index.id(),
            u64::MAX,
        ));
        let dirty_rows = Arc::new(VectorMemoryDirtyRows::default());
        let index = base_index.with_write_dirty_rows(Arc::clone(&dirty_rows));
        index.remember_dimension(2).unwrap();

        let node_id = 7u64;
        let stale_item = cosine_test_item(&[1.0, 0.0]);
        store.insert_upper_vector(node_id, encode_item(&stale_item));
        dirty_rows.mark_node_dirty(node_id);

        let fresh_item = cosine_test_item(&[-1.0, 0.0]);
        let upper_key =
            VectorKey::UpperVector(VectorUpperVectorKey::new(index.id(), node_id)).to_bytes();
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(&upper_key, encode_item(&fresh_item)).unwrap();

        let mut mutation_cache = MutationOpCache::<Cosine>::default();
        let items = index
            .get_items_for_layer_cached_batch(&txn, 1, &[node_id], &mut mutation_cache)
            .await
            .unwrap();
        let fetched = items.get(&node_id).expect("expected fresh upper vector");

        assert!(
            Cosine::distance(&fresh_item, fetched.as_ref()) < 1e-6,
            "write-mode batch fetch should bypass stale memory cache for dirty nodes"
        );
        assert!(
            Cosine::distance(&stale_item, fetched.as_ref()) > 0.5,
            "write-mode batch fetch returned the stale cached vector"
        );
    }

    #[tokio::test]
    async fn test_batch_item_fetch_does_not_mutate_upper_vector_cache_in_write_mode() {
        let db = test_inner_db("batch_fetch_no_write_cache_mutation").await;
        let base_index = VectorIndex::<Cosine>::new("batch_fetch_no_write_cache_mutation_idx");
        let store = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            base_index.id(),
            u64::MAX,
        ));
        let dirty_rows = Arc::new(VectorMemoryDirtyRows::default());
        let index = base_index.with_write_dirty_rows(Arc::clone(&dirty_rows));
        index.remember_dimension(2).unwrap();

        let node_id = 8u64;
        dirty_rows.mark_node_dirty(node_id);
        let fresh_item = cosine_test_item(&[0.25, 0.75]);
        let upper_key =
            VectorKey::UpperVector(VectorUpperVectorKey::new(index.id(), node_id)).to_bytes();
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(&upper_key, encode_item(&fresh_item)).unwrap();

        let mut mutation_cache = MutationOpCache::<Cosine>::default();
        let items = index
            .get_items_for_layer_cached_batch(&txn, 1, &[node_id], &mut mutation_cache)
            .await
            .unwrap();

        assert!(items.contains_key(&node_id));
        assert!(
            store.get_upper_vector(node_id).is_none(),
            "write-mode batch fetch must not publish uncommitted upper vectors to shared memory cache"
        );
    }

    #[tokio::test]
    async fn write_tracking_get_item_uses_authoritative_simhash_for_dirty_node() {
        let db = test_inner_db("write_mode_get_item_dirty_simhash_bypass").await;
        let base_index = VectorIndex::<Cosine>::new("write_mode_get_item_dirty_simhash_bypass_idx");
        let dirty_rows = Arc::new(VectorMemoryDirtyRows::default());
        let index = base_index.with_write_dirty_rows(Arc::clone(&dirty_rows));
        index.remember_dimension(2).unwrap();

        let node_id = 33u64;
        let stale_simhash =
            crate::search::vector::SimHash::from_bits(0x1111_0000_0000_0000 ^ node_id);
        let fresh_simhash =
            crate::search::vector::SimHash::from_bits(0x2222_0000_0000_0000 ^ node_id);
        let stale_item = cosine_test_item(&[1.0, 0.0]);
        let fresh_item = cosine_test_item(&[0.0, 1.0]);
        let stale_key = VectorKey::Vector(VectorItemKey::new(
            index.id(),
            order_code_from_simhash_bits(stale_simhash.bits()),
            node_id,
        ))
        .to_bytes();
        let fresh_key = VectorKey::Vector(VectorItemKey::new(
            index.id(),
            order_code_from_simhash_bits(fresh_simhash.bits()),
            node_id,
        ))
        .to_bytes();
        let simhash_key = VectorKey::SimHash(VectorSimHashKey::new(index.id(), node_id)).to_bytes();

        dirty_rows.mark_node_dirty(node_id);

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(&simhash_key, encode_simhash(fresh_simhash.bits()))
            .unwrap();
        txn.put(&stale_key, encode_item(&stale_item)).unwrap();
        txn.put(&fresh_key, encode_item(&fresh_item)).unwrap();

        let fetched = index
            .get_item(&txn, node_id)
            .await
            .unwrap()
            .expect("dirty write-mode fetch should use fresh simhash row");
        assert_eq!(fetched.vector.to_vec(), fresh_item.vector.to_vec());
        assert_ne!(fetched.vector.to_vec(), stale_item.vector.to_vec());
    }

    #[tokio::test]
    async fn test_batch_item_fetch_matches_single_fetch_for_layer0_duplicates_and_missing() {
        let db = test_inner_db("batch_fetch_layer0_parity").await;
        let index = create_test_vector_index(&db, "batch_fetch_layer0_parity_idx").await;

        insert_test_vector(&db, &index, 1, &[1.0, 0.0]).await;
        insert_test_vector(&db, &index, 2, &[0.0, 1.0]).await;
        insert_test_vector(&db, &index, 3, &[0.7, 0.3]).await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let node_ids = [1, 2, 999, 2, 3];
        let mut batch_cache = MutationOpCache::<Cosine>::default();
        let batch_items = index
            .get_items_for_layer_cached_batch(&txn, 0, &node_ids, &mut batch_cache)
            .await
            .unwrap();

        let mut single_cache = MutationOpCache::<Cosine>::default();
        for node_id in node_ids {
            let single = index
                .get_item_for_layer_cached(&txn, 0, node_id, &mut single_cache)
                .await
                .unwrap();

            match single {
                Some(single_item) => {
                    let batch_item = batch_items
                        .get(&node_id)
                        .expect("batch fetch should return every item returned by single fetch");
                    assert_cosine_items_match(single_item.as_ref(), batch_item.as_ref());
                }
                None => {
                    assert!(
                        !batch_items.contains_key(&node_id),
                        "batch fetch should omit missing node {node_id}"
                    );
                    assert!(
                        batch_cache.item_is_known_absent(0, node_id),
                        "batch fetch should cache missing node {node_id} as a transaction-local miss"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_layer0_neighbor_prefetch_loads_missing_rows_into_mutation_cache() {
        let db = test_inner_db("layer0_neighbor_prefetch_loads_rows").await;
        let index = create_test_vector_index(&db, "layer0_neighbor_prefetch_loads_rows_idx").await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        {
            let measured = MeasuredVectorTransaction::new(&txn);
            index
                .store_neighbors_layer0(&measured, 3, &[30, 31])
                .await
                .unwrap();
            index
                .store_neighbors_layer0(&measured, 1, &[10])
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let mut mutation_cache = MutationOpCache::<Cosine>::default();
        let fetched = index
            .prefetch_layer0_neighbors_for_mutation(&measured, &[3, 2, 1, 3], &mut mutation_cache)
            .await
            .unwrap();

        assert_eq!(fetched, 3);
        assert_eq!(
            cached_current_neighbors(&mutation_cache, 0, 1),
            Some([10].as_slice())
        );
        assert_eq!(
            cached_current_neighbors(&mutation_cache, 0, 2),
            Some([].as_slice())
        );
        assert!(matches!(
            mutation_cache
                .neighbor(MutationOpCache::<Cosine>::node_row_id(0, 2))
                .map(|cached| cached.current()),
            Some(NeighborRowValue::KnownAbsent)
        ));
        assert_eq!(
            cached_current_neighbors(&mutation_cache, 0, 3),
            Some([30, 31].as_slice())
        );
        assert_eq!(cached_original_neighbors(&mutation_cache, 0, 3), None);

        index
            .stage_neighbors_vec_for_mutation(
                &measured,
                0,
                3,
                vec![30, 31, 32],
                &mut mutation_cache,
            )
            .await
            .unwrap();
        assert_eq!(
            cached_original_neighbors(&mutation_cache, 0, 3),
            Some([30, 31].as_slice())
        );
        assert_eq!(
            cached_current_neighbors(&mutation_cache, 0, 3),
            Some([30, 31, 32].as_slice())
        );
    }

    #[tokio::test]
    async fn test_layer0_neighbor_prefetch_does_not_overwrite_staged_rows() {
        let db = test_inner_db("layer0_neighbor_prefetch_preserves_staged").await;
        let index =
            create_test_vector_index(&db, "layer0_neighbor_prefetch_preserves_staged_idx").await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        {
            let measured = MeasuredVectorTransaction::new(&txn);
            index
                .store_neighbors_layer0(&measured, 1, &[10])
                .await
                .unwrap();
            index
                .store_neighbors_layer0(&measured, 2, &[20])
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let mut mutation_cache = MutationOpCache::<Cosine>::default();
        install_dirty_test_neighbors(&mut mutation_cache, 0, 1, vec![98], vec![99]);
        install_dirty_test_neighbors(&mut mutation_cache, 0, 2, vec![87], vec![88]);

        let fetched = index
            .prefetch_layer0_neighbors_for_mutation(&measured, &[1, 2, 3], &mut mutation_cache)
            .await
            .unwrap();

        assert_eq!(fetched, 1);
        assert_eq!(
            cached_current_neighbors(&mutation_cache, 0, 1),
            Some([99].as_slice())
        );
        assert_eq!(
            cached_original_neighbors(&mutation_cache, 0, 1),
            Some([98].as_slice())
        );
        assert_eq!(
            cached_current_neighbors(&mutation_cache, 0, 2),
            Some([88].as_slice())
        );
        assert_eq!(
            cached_original_neighbors(&mutation_cache, 0, 2),
            Some([87].as_slice())
        );
        assert_eq!(
            cached_current_neighbors(&mutation_cache, 0, 3),
            Some([].as_slice())
        );
        assert_eq!(cached_original_neighbors(&mutation_cache, 0, 3), None);
    }

    #[tokio::test]
    async fn test_batch_item_fetch_matches_single_fetch_for_upper_cache_db_and_missing() {
        let db = test_inner_db("batch_fetch_upper_parity").await;
        let base_index = VectorIndex::<Cosine>::new("batch_fetch_upper_parity_idx");
        let store = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            base_index.id(),
            u64::MAX,
        ));
        let index = base_index
            .with_managed_read_cache(
                Arc::clone(&store),
                Arc::new(VectorMemoryPendingDirtyRows::default()),
            )
            .unwrap();
        index.remember_dimension(2).unwrap();

        let cached_item = cosine_test_item(&[1.0, 0.0]);
        let pending_item = cosine_test_item(&[0.0, 1.0]);
        store.insert_upper_vector(11, encode_item(&cached_item));

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(
            VectorKey::UpperVector(VectorUpperVectorKey::new(index.id(), 12)).to_bytes(),
            encode_item(&pending_item),
        )
        .unwrap();

        let node_ids = [11, 12, 13, 12, 11];
        let mut batch_cache = MutationOpCache::<Cosine>::default();
        let batch_items = index
            .get_items_for_layer_cached_batch(&txn, 2, &node_ids, &mut batch_cache)
            .await
            .unwrap();

        let mut single_cache = MutationOpCache::<Cosine>::default();
        for node_id in node_ids {
            let single = index
                .get_item_for_layer_cached(&txn, 2, node_id, &mut single_cache)
                .await
                .unwrap();

            match single {
                Some(single_item) => {
                    let batch_item = batch_items.get(&node_id).expect(
                        "batch fetch should return every upper item returned by single fetch",
                    );
                    assert_cosine_items_match(single_item.as_ref(), batch_item.as_ref());
                }
                None => {
                    assert!(
                        !batch_items.contains_key(&node_id),
                        "batch fetch should omit missing upper node {node_id}"
                    );
                    assert!(
                        batch_cache.item_is_known_absent(2, node_id),
                        "batch fetch should cache missing upper node {node_id} as a transaction-local miss"
                    );
                }
            }
        }

        assert_cosine_items_match(&cached_item, batch_items.get(&11).unwrap().as_ref());
        assert_cosine_items_match(&pending_item, batch_items.get(&12).unwrap().as_ref());
    }

    #[test]
    fn managed_read_attachment_validates_identity() {
        let base_index = VectorIndex::<Cosine>::new("snapshot_lookup_only_idx");
        let store = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            base_index.id(),
            u64::MAX,
        ));

        let read_index = base_index
            .with_managed_read_cache(store, Arc::new(VectorMemoryPendingDirtyRows::default()))
            .unwrap();
        assert!(!read_index.is_memory_node_dirty(1));
    }

    #[tokio::test]
    async fn test_pending_dirty_rows_bypass_stale_upper_vector_cache() {
        let db = test_inner_db("pending_dirty_bypasses_upper_vector_cache").await;
        let base_index =
            VectorIndex::<Cosine>::new("pending_dirty_bypasses_upper_vector_cache_idx");
        let store = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            base_index.id(),
            u64::MAX,
        ));
        let pending_rows = Arc::new(VectorMemoryPendingDirtyRows::default());
        let index = base_index
            .with_managed_read_cache(Arc::clone(&store), Arc::clone(&pending_rows))
            .unwrap();
        index.remember_dimension(2).unwrap();

        let node_id = 42;
        let stale_item = cosine_test_item(&[1.0, 0.0]);
        let fresh_item = cosine_test_item(&[0.0, 1.0]);
        store.insert_upper_vector(node_id, encode_item(&stale_item));

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(
            VectorKey::UpperVector(VectorUpperVectorKey::new(index.id(), node_id)).to_bytes(),
            encode_item(&fresh_item),
        )
        .unwrap();

        let dirty_rows = VectorMemoryDirtyRows::default();
        dirty_rows.mark_node_dirty(node_id);
        let _guard = pending_rows.acquire(&dirty_rows);

        let loaded = index
            .get_item_for_layer(&txn, 2, node_id)
            .await
            .unwrap()
            .expect("fresh upper vector should load from DB while pending dirty");

        assert_cosine_items_match(&fresh_item, &loaded);
    }

    #[test]
    fn test_pending_dirty_rows_are_reference_counted_until_guard_release() {
        let pending_rows = Arc::new(VectorMemoryPendingDirtyRows::default());
        let dirty_rows = VectorMemoryDirtyRows::default();
        dirty_rows.mark_node_dirty(7);
        dirty_rows.mark_upper_neighbors_dirty(2, 11);

        let first_guard = pending_rows.acquire(&dirty_rows);
        let second_guard = pending_rows.acquire(&dirty_rows);
        assert!(pending_rows.is_node_dirty(7));
        assert!(pending_rows.is_upper_neighbors_dirty(2, 11));

        drop(first_guard);
        assert!(
            pending_rows.is_node_dirty(7),
            "second guard should keep node dirty"
        );
        assert!(
            pending_rows.is_upper_neighbors_dirty(2, 11),
            "second guard should keep upper-neighbor row dirty"
        );

        drop(second_guard);
        assert!(!pending_rows.is_node_dirty(7));
        assert!(!pending_rows.is_upper_neighbors_dirty(2, 11));
    }

    #[tokio::test]
    async fn test_delete_repairs_preexisting_stale_metadata_entry_point() {
        let db = test_inner_db("delete_repairs_stale_metadata_entry_point").await;
        let index =
            create_test_vector_index(&db, "delete_repairs_stale_metadata_entry_point_idx").await;

        insert_test_vector(&db, &index, 1, &[1.0, 0.0]).await;
        insert_test_vector(&db, &index, 2, &[0.0, 1.0]).await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let mut metadata = index.get_metadata(&txn).await.unwrap().unwrap();
        metadata.entry_point = Some(8_888);
        metadata.max_layer = metadata.max_layer.max(1);
        {
            let measured = MeasuredVectorTransaction::new(&txn);
            index.update_metadata(&measured, &metadata).await.unwrap();
        }
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.delete(&txn, 2).await.unwrap();
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let repaired = index.get_metadata(&txn).await.unwrap().unwrap();
        let entry_point = repaired
            .entry_point
            .expect("entry point should be repaired");
        assert!(index.get_item(&txn, entry_point).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_delete_removes_reverse_locator_rows_from_single_scan() {
        let db = test_inner_db("delete_removes_reverse_locator_rows").await;
        let index = create_test_vector_index(&db, "delete_removes_reverse_locator_rows_idx").await;

        insert_test_vector(&db, &index, 1, &[1.0, 0.0]).await;
        insert_test_vector(&db, &index, 2, &[0.0, 1.0]).await;

        let locator_key =
            VectorKey::ReverseEdge(VectorReverseEdgeKey::new(index.id(), 2, 0, 1)).to_bytes();
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(&locator_key, bytes::Bytes::new()).unwrap();
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.delete(&txn, 2).await.unwrap();
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let prefix =
            VectorKey::ReverseEdgePrefix(VectorReverseEdgePrefixKey::new(index.id(), 2)).to_bytes();
        let mut iter = txn.scan_prefix(prefix, ..).await.unwrap();
        assert!(
            iter.next().await.unwrap().is_none(),
            "reverse locator rows for deleted node should be removed"
        );

        let results = index
            .search(&txn, &[0.0, 1.0], &SearchParams::new(10).unwrap())
            .await
            .unwrap();
        assert!(results.iter().all(|result| result.entity_id() != 2));
    }

    /// Builds a stable finite vector whose components vary by node and dimension.
    fn invariant_matrix_vector(node_id: NodeId, dimension: usize) -> Vec<f32> {
        (0..dimension)
            .map(|component| {
                let mixed = node_id
                    .wrapping_mul(31)
                    .wrapping_add((component as u64).wrapping_mul(17))
                    % 101;
                mixed as f32 / 50.0 - 1.0
            })
            .collect()
    }

    #[tokio::test]
    async fn degree_pruning_and_reverse_locators_hold_across_m_and_beam_matrix() {
        const DIMENSION: usize = 8;
        const ENTITY_COUNT: NodeId = 160;

        for connections in [16usize, 32, 64] {
            for construction_beam in [64usize, 200, 512] {
                let layer0_connections = connections.checked_mul(2).unwrap();
                let db = test_inner_db(&format!(
                    "neighbor-invariants-m{connections}-beam{construction_beam}"
                ))
                .await;
                let index = VectorIndex::<Cosine>::new(format!(
                    "neighbor-invariants-m{connections}-beam{construction_beam}"
                ));
                let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
                index
                    .create(
                        &txn,
                        VectorIndexConfig::new(index.name(), "embedding", DIMENSION)
                            .with_m(connections)
                            .with_m0(layer0_connections)
                            .with_ef_construction(construction_beam)
                            .with_ml(f32::MIN_POSITIVE),
                    )
                    .await
                    .unwrap();
                for node_id in 1..=ENTITY_COUNT {
                    index
                        .insert(&txn, node_id, &invariant_matrix_vector(node_id, DIMENSION))
                        .await
                        .unwrap();
                }
                txn.commit().await.unwrap();

                let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
                let mut adjacency = BTreeMap::<NodeId, BTreeSet<NodeId>>::new();
                for node_id in 1..=ENTITY_COUNT {
                    let neighbors = index.load_neighbors_layer0(&txn, node_id).await.unwrap();
                    assert!(
                        neighbors.len() <= layer0_connections,
                        "m={connections}, beam={construction_beam}, node={node_id} exceeded m0"
                    );
                    assert!(!neighbors.contains(&node_id));
                    assert!(neighbors.windows(2).all(|pair| pair[0] < pair[1]));
                    adjacency.insert(node_id, neighbors.into_iter().collect());
                }

                for (&source, targets) in &adjacency {
                    for target in targets {
                        assert!(
                            adjacency
                                .get(target)
                                .is_some_and(|neighbors| neighbors.contains(&source)),
                            "m={connections}, beam={construction_beam}: {source}->{target} was not bidirectional"
                        );
                    }
                }

                for target in 1..=ENTITY_COUNT {
                    let reverse = index
                        .load_reverse_sources_for_target(&txn, target)
                        .await
                        .unwrap();
                    let actual = reverse
                        .sources_by_layer()
                        .get(&0)
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .collect::<BTreeSet<_>>();
                    let expected = adjacency
                        .iter()
                        .filter_map(|(&source, targets)| {
                            targets.contains(&target).then_some(source)
                        })
                        .collect::<BTreeSet<_>>();
                    assert_eq!(
                        actual, expected,
                        "m={connections}, beam={construction_beam}, target={target} locator mismatch"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_delete_missing_canonical_item_still_removes_all_residue() {
        let db = test_inner_db("delete_missing_canonical_item_residue").await;
        let index =
            create_test_vector_index(&db, "delete_missing_canonical_item_residue_idx").await;

        insert_test_vector(&db, &index, 1, &[1.0, 0.0]).await;
        insert_test_vector(&db, &index, 2, &[0.0, 1.0]).await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let (canonical_key, _) = index
            .resolve_required_canonical_vector_key_counted(
                &txn,
                2,
                "seeding a missing canonical delete fixture",
            )
            .await
            .unwrap();
        let candidate_layer = {
            let measured = MeasuredVectorTransaction::new(&txn);
            index
                .get_entry_candidate_layer(&measured, 2)
                .await
                .unwrap()
                .expect("inserted vector has an entry candidate")
        };
        let mut source_neighbors = index.load_neighbors_layer0(&txn, 1).await.unwrap();
        if !source_neighbors.contains(&2) {
            source_neighbors.push(2);
        }
        txn.put(
            index.vector_key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                index.id(),
                1,
            ))),
            encode_layer0_neighbors(&source_neighbors),
        )
        .unwrap();
        txn.put(
            index.vector_key(VectorKey::ReverseEdge(VectorReverseEdgeKey::new(
                index.id(),
                2,
                0,
                1,
            ))),
            bytes::Bytes::new(),
        )
        .unwrap();
        txn.put(
            index.vector_key(VectorKey::UpperVector(VectorUpperVectorKey::new(
                index.id(),
                2,
            ))),
            bytes::Bytes::from_static(b"orphan-hot-row"),
        )
        .unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        VectorWriteRows::new(&measured, index.row_keyspace())
            .delete_canonical_vector(&canonical_key)
            .unwrap();
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.delete(&txn, 2).await.unwrap();
        txn.commit().await.unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for key in [
            index.vector_key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                index.id(),
                2,
            ))),
            index.vector_key(VectorKey::UpperVector(VectorUpperVectorKey::new(
                index.id(),
                2,
            ))),
            index.vector_key(VectorKey::SimHash(VectorSimHashKey::new(index.id(), 2))),
            index.vector_key(VectorKey::EntryCandidateNode(
                VectorEntryCandidateNodeKey::new(index.id(), 2),
            )),
            index.vector_key(VectorKey::EntryCandidateSorted(
                VectorEntryCandidateKey::new(index.id(), candidate_layer, 2),
            )),
        ] {
            assert!(txn.get(key).await.unwrap().is_none());
        }
        let source_neighbors = index.load_neighbors_layer0(&txn, 1).await.unwrap();
        assert!(!source_neighbors.contains(&2));
        let reverse_prefix = index.vector_key(VectorKey::ReverseEdgePrefix(
            VectorReverseEdgePrefixKey::new(index.id(), 2),
        ));
        let mut reverse_rows = txn.scan_prefix(reverse_prefix, ..).await.unwrap();
        assert!(reverse_rows.next().await.unwrap().is_none());
        let metadata = index.get_metadata(&txn).await.unwrap().unwrap();
        assert_ne!(metadata.entry_point, Some(2));
    }

    #[tokio::test]
    async fn test_search_layer_beam_recovers_missing_entry_point() {
        let db = test_inner_db("search_layer_beam_recovers_missing_entry_point").await;
        let index =
            create_test_vector_index(&db, "search_layer_beam_recovers_missing_entry_point_idx")
                .await;

        insert_test_vector(&db, &index, 1, &[1.0, 0.0]).await;
        insert_test_vector(&db, &index, 2, &[0.0, 1.0]).await;

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let metadata = index.get_metadata(&txn).await.unwrap().unwrap();
        let stale_entry = metadata.entry_point.expect("index should have entry point");
        let (vec_key_opt, _) = index
            .resolve_required_canonical_vector_key_counted(
                &txn,
                stale_entry,
                "test stale beam entry point",
            )
            .await
            .unwrap();
        let vec_key = vec_key_opt;
        let measured = MeasuredVectorTransaction::new(&txn);
        VectorWriteRows::new(&measured, index.row_keyspace())
            .delete_canonical_vector(&vec_key)
            .unwrap();
        txn.commit().await.unwrap();

        let query_vec = [0.25f32, 0.75f32];
        let vector = UnalignedVector::from_slice(&query_vec);
        let query_item = Item::<Cosine> {
            header: Cosine::new_header(&vector),
            vector,
        };

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let mut mutation_cache = MutationOpCache::<Cosine>::default();
        let results = index
            .search_layer_beam(
                &measured,
                &query_item,
                stale_entry,
                0,
                8,
                42,
                &mut mutation_cache,
            )
            .await
            .unwrap();
        assert!(
            !results.is_empty(),
            "beam search should fall back to a live candidate instead of failing"
        );
    }
}
