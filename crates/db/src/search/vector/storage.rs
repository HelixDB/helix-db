//! Typed physical namespace for persisted vector rows.
//!
//! This module is the storage boundary between logical `VectorKey` values and
//! tenant-scoped SlateDB bytes. A `VectorRowKeyspace` binds the complete physical
//! name and request scope once and derives the stable index ID internally, so
//! callers cannot pair a name with the wrong compact namespace. It delegates
//! logical serialization to the existing `encoding::v1` key types and therefore
//! does not change persisted key bytes or row codecs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU64;
use std::ops::Bound;

use bytes::Bytes;
use slatedb::DbReadOps;

use crate::encoding::error::EncodingError;
use crate::encoding::keys::{tenant::DataScope, DataKeyKind, Key};
use crate::encoding::v1::keys::vectors::{
    VectorEntryCandidateKey, VectorEntryCandidateNodeKey, VectorEntryCandidatePrefixKey,
    VectorIndexMetadataKey, VectorItemKey, VectorItemPrefixKey, VectorKey,
    VectorLayer0NeighborsKey, VectorReverseEdgeKey, VectorReverseEdgePrefixKey,
    VectorSimHashDirectoryKey, VectorSimHashDirectoryPrefixKey, VectorSimHashKey,
    VectorStorageLane, VectorUpperNeighborsKey, VectorUpperVectorKey,
};
use crate::encoding::v1::values::vectors::{
    decode_layer0_neighbors, encode_layer0_neighbors,
    entry::{decode_entry_candidate_layer, encode_entry_candidate_layer},
    markers::{
        decode_active_txn_guard, decode_empty_marker, decode_simhash_directory_marker_v1,
        encode_empty_marker, encode_simhash_directory_marker_v1,
    },
    metadata::decode_legacy_metadata,
    neighbors::{decode_upper_neighbors, encode_upper_neighbors},
    simhash::decode_simhash,
};
use crate::encoding::NodeId;
use crate::error::HelixDbError;
use crate::index_lifecycle::{ValidatedVectorIndexDefinition, VectorPhysicalIndexId};

// Legacy item validation delegates metric semantics to the central decoder.
#[cfg(any(test, feature = "production-coverage"))]
use super::index_id_from_name;
use super::{
    decode_item_borrowed, decode_metadata, encode_metadata, Distance, MeasuredVectorTransaction,
    SimHash, VectorDimension, VectorIndexConfig, VectorIndexMetadata, VectorWriteMeasurement,
};

/// Bound physical namespace for every current-format row of one vector index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorRowKeyspace {
    physical_name: String,
    index_id: u64,
    scope: DataScope,
}

/// Opaque keyspace-bound identity of one canonical deployed vector payload row.
///
/// Search and mutation may order or pass this token back to typed storage, but
/// cannot access or construct its physical bytes. This prevents raw key handling
/// from leaking out of the storage boundary without changing deployed keys.
#[derive(Debug, Clone)]
pub(crate) struct CanonicalVectorRowKey {
    scope: DataScope,
    index_id: u64,
    order_code: u64,
    node_id: NodeId,
    physical_key: Bytes,
}

impl CanonicalVectorRowKey {
    /// Compares two tokens in their deployed physical-key order.
    ///
    /// Vector fetches use this ordering to preserve SlateDB locality before a
    /// batch read. It intentionally exposes only the comparison result: callers
    /// still cannot inspect, construct, or submit raw physical key bytes.
    pub(crate) fn physical_order(&self, other: &Self) -> std::cmp::Ordering {
        self.physical_key.cmp(&other.physical_key)
    }
}

impl VectorRowKeyspace {
    /// Binds a complete physical name and derives its compact row namespace.
    ///
    /// Derivation inside this constructor, together with private fields,
    /// prevents a full name and its persisted `u64` namespace from disagreeing
    /// at any storage call site.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn new(physical_name: String, scope: DataScope) -> Self {
        let index_id = index_id_from_name(&physical_name);
        Self {
            physical_name,
            index_id,
            scope,
        }
    }

    /// Binds the hash-derived namespace used by a persisted pre-V2 definition.
    pub(crate) fn from_legacy_name(physical_name: String, scope: DataScope) -> Self {
        let index_id = super::index_id_from_name(&physical_name);
        Self {
            physical_name,
            index_id,
            scope,
        }
    }

    /// Binds a canonical V2 physical ID without hashing its diagnostic name.
    pub(crate) fn from_allocated(
        physical_name: String,
        physical_index_id: VectorPhysicalIndexId,
        scope: DataScope,
    ) -> Self {
        Self {
            physical_name,
            index_id: physical_index_id.get(),
            scope,
        }
    }

    /// Returns the complete physical name bound to this row namespace.
    pub(crate) fn physical_name(&self) -> &str {
        &self.physical_name
    }

    /// Returns the stable ID encoded by every logical vector row key.
    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    /// Returns the outer tenant namespace applied to every physical row key.
    pub(crate) const fn scope(&self) -> DataScope {
        self.scope
    }

    /// Encodes one typed logical key in this keyspace's physical namespace.
    ///
    /// Logical key serialization remains owned by `encoding::v1`; this method
    /// only applies the bound tenant scope at the final storage boundary.
    pub(crate) fn key(&self, key: VectorKey) -> Bytes {
        Key::Data {
            scope: self.scope,
            kind: DataKeyKind::Vector(key),
        }
        .to_bytes()
    }

    /// Binds one node and SimHash order code to its deployed payload row.
    ///
    /// Construction remains here so callers cannot bypass tenant scoping or
    /// accidentally pair a node with another index's compact ID. The resulting
    /// token carries only the copyable namespace identity needed to reject
    /// cross-index use; it adds no persisted state and retains the existing
    /// `VectorItemKey` bytes exactly.
    pub(crate) fn canonical_vector_row_key(
        &self,
        node_id: NodeId,
        order_code: u64,
    ) -> CanonicalVectorRowKey {
        CanonicalVectorRowKey {
            scope: self.scope,
            index_id: self.index_id,
            order_code,
            node_id,
            physical_key: self.key(VectorKey::Vector(VectorItemKey::new(
                self.index_id,
                order_code,
                node_id,
            ))),
        }
    }

    /// Removes this keyspace's tenant prefix from a key returned by a scan.
    ///
    /// A key outside the bound namespace is an invariant violation rather than
    /// a skippable row because accepting it could cross tenant boundaries.
    pub(crate) fn strip_physical_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8], HelixDbError> {
        self.scope.strip_key(key).ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "tenant-scoped vector scan returned key outside tenant prefix".to_string(),
            )
        })
    }
}

/// Typed read access to current-format rows in one bound vector namespace.
///
/// The wrapper borrows an arbitrary `DbReadOps` snapshot or transaction so
/// search tests can supply narrow read fakes without exposing raw row bytes to
/// the search algorithm.
pub(crate) struct VectorRows<'a, R: ?Sized> {
    read: &'a R,
    keyspace: &'a VectorRowKeyspace,
}

/// Opaque current-format row selected for bounded generation cleanup.
///
/// The physical key never leaves this module. Input/output measurements are
/// exposed so the lifecycle driver can admit a complete batch before passing
/// the token back to [`VectorWriteRows::delete_cleanup_row`].
#[derive(Debug)]
pub(crate) struct VectorCleanupRow {
    keyspace: VectorRowKeyspace,
    physical_key: Bytes,
    input_bytes: u64,
}

/// One migration-only legacy payload lookup with exact physical input cost.
///
/// Absence and presence are distinct states because both consume typed point
/// reads, while only a present payload can restore a graph property. Keeping
/// the byte count inside each variant prevents callers from dropping the I/O
/// cost when no HNSW item exists.
#[derive(Debug)]
pub(crate) enum LegacyVectorMigrationRead {
    /// Metadata or the canonical payload is absent after all required probes.
    Absent { input_bytes: u64 },
    /// A validated canonical f32 payload was read from the legacy namespace.
    Present { vector: Vec<f32>, input_bytes: u64 },
}

impl LegacyVectorMigrationRead {
    /// Returns every typed key and present value byte read for this lookup.
    pub(crate) const fn input_bytes(&self) -> u64 {
        match self {
            Self::Absent { input_bytes } | Self::Present { input_bytes, .. } => *input_bytes,
        }
    }

    /// Consumes the lookup while preserving physical absence.
    pub(crate) fn into_vector(self) -> Option<Vec<f32>> {
        match self {
            Self::Absent { .. } => None,
            Self::Present { vector, .. } => Some(vector),
        }
    }
}

impl VectorCleanupRow {
    /// Returns the exact scanned key-plus-value byte count.
    pub(crate) const fn input_bytes(&self) -> u64 {
        self.input_bytes
    }

    /// Returns the exact staged-delete key byte count.
    pub(crate) fn output_bytes(&self) -> u64 {
        self.physical_key.len() as u64
    }
}

/// Exhaustive typed scan over the three current vector storage lanes.
pub(crate) struct VectorCleanupScan {
    keyspace: VectorRowKeyspace,
    lanes: VecDeque<(VectorStorageLane, slatedb::DbIterator)>,
}

/// Exhaustive typed scan over only one vector SimHash-directory prefix.
pub(crate) struct VectorSimHashDirectoryCleanupScan {
    keyspace: VectorRowKeyspace,
    rows: slatedb::DbIterator,
}

/// One bounded result from structurally validating an unchanged legacy namespace.
pub(crate) enum LegacyVectorValidationOutcome {
    /// Every admitted row decoded under the expected physical contract.
    Valid {
        /// Last complete physical key validated in this lane.
        last_key: Option<Bytes>,
        /// Exact row count admitted by this step.
        rows: u64,
        /// Exact physical key-plus-value bytes admitted by this step.
        input_bytes: u64,
        /// Whether this lane and its terminal point proofs are complete.
        exhausted: bool,
        /// Opaque canonical-row tokens admitted for directory writes.
        directory_entries: Vec<CanonicalVectorRowKey>,
        /// Exact typed marker-write measurement predicted for these tokens.
        predicted_directory_writes: VectorWriteMeasurement,
    },
    /// One physical row exceeds the configured atomic input bound.
    Oversized {
        /// Exact physical key-plus-value bytes observed.
        observed: u64,
        /// Configured per-step input bound.
        limit: u64,
    },
    /// A key, value, metadata contract, or entry-point proof is invalid.
    Invalid {
        /// Non-contractual diagnostic retained only for logs.
        reason: String,
    },
}

/// Output policy for one bounded legacy-vector validation page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegacyVectorValidationMode {
    /// Preserve the frozen behavior of already-persisted `LegacyHnsw` adoptions.
    ReadOnly,
    /// Emit one bounded marker token per canonical layer-zero vector row.
    BackfillSimHashDirectory {
        /// Maximum marker operations admitted in this page.
        max_output_operations: NonZeroU64,
        /// Maximum encoded marker bytes admitted in this page.
        max_output_bytes: NonZeroU64,
    },
}

/// Complete policy for one bounded legacy-vector validation pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LegacyVectorValidationPass {
    lane: VectorStorageLane,
    mode: LegacyVectorValidationMode,
}

impl LegacyVectorValidationPass {
    /// Binds a physical lane to its validation and output policy.
    pub(crate) const fn new(lane: VectorStorageLane, mode: LegacyVectorValidationMode) -> Self {
        Self { lane, mode }
    }

    /// Returns the only physical lane admitted by this pass.
    pub(crate) const fn lane(self) -> VectorStorageLane {
        self.lane
    }

    /// Returns the validation and output policy for the admitted lane.
    pub(crate) const fn mode(self) -> LegacyVectorValidationMode {
        self.mode
    }
}

/// One bounded result from validating only compact SimHash-directory rows.
pub(crate) enum SimHashDirectoryValidationOutcome {
    /// Every admitted marker decoded and remained inside the typed prefix.
    Valid {
        /// Last complete physical marker key validated.
        last_key: Option<Bytes>,
        /// Exact marker count admitted by this step.
        markers: u64,
        /// Exact physical key-plus-value bytes admitted by this step.
        input_bytes: u64,
        /// Whether the directory and entry-point proof are complete.
        exhausted: bool,
    },
    /// One marker exceeds the configured atomic input bound.
    Oversized {
        /// Exact physical key-plus-value bytes observed.
        observed: u64,
        /// Configured per-step input bound.
        limit: u64,
    },
    /// A key, marker, metadata contract, or entry-point proof is invalid.
    Invalid {
        /// Non-contractual diagnostic retained only for logs.
        reason: String,
    },
}

/// Proof required while scanning one compact SimHash directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SimHashDirectoryValidationMode {
    /// Prove every pre-existing marker resolves to its canonical vector.
    PreflightCanonicalCorrespondence,
    /// Validate compact markers against frozen legacy metadata before adoption.
    FinalLegacyWithEntryPoint,
    /// Validate compact markers against current metadata for an active generation.
    FinalCurrentWithEntryPoint,
}

/// One bounded result from the canonical-vector-only active-generation pass.
pub(crate) enum CanonicalVectorDirectoryBackfillOutcome {
    /// Every admitted payload and present marker decoded under the expected contract.
    Valid {
        /// Last complete canonical vector key admitted.
        last_key: Option<Bytes>,
        /// Exact canonical vector count admitted by this step.
        canonical_vectors: u64,
        /// Exact pre-existing marker count observed for admitted vectors.
        existing_markers: u64,
        /// Exact key-plus-value bytes read by this step.
        input_bytes: u64,
        /// Missing-marker tokens admitted for measured writes.
        directory_entries: Vec<CanonicalVectorRowKey>,
        /// Exact typed marker-write measurement predicted for these tokens.
        predicted_directory_writes: VectorWriteMeasurement,
        /// Whether the canonical-vector prefix is complete.
        exhausted: bool,
    },
    /// One canonical vector plus its marker lookup exceeds an atomic bound.
    Oversized {
        /// Exact input or output bytes observed.
        observed: u64,
        /// Configured bound for that resource.
        limit: u64,
    },
    /// A key, payload, or present marker is invalid.
    Invalid {
        /// Non-contractual diagnostic retained only for logs.
        reason: String,
    },
}

fn validate_legacy_row<D: Distance>(
    key: &VectorKey,
    value: &[u8],
    expected_config: &VectorIndexConfig,
    dimension: VectorDimension,
) -> Result<(), String> {
    match key {
        VectorKey::IndexMetadata(_) => {
            let metadata = decode_legacy_metadata(value)
                .map_err(|error| format!("legacy vector metadata is malformed: {error}"))?;
            metadata
                .validated_state()
                .map_err(|error| format!("legacy vector metadata state is invalid: {error}"))?;
            if !metadata.config.has_same_physical_contract(expected_config) {
                return Err(
                    "legacy vector metadata differs from its persisted definition".to_string(),
                );
            }
        }
        VectorKey::TxnGuard(_) => decode_active_txn_guard(value)
            .map_err(|error| format!("legacy vector transaction guard is malformed: {error}"))?,
        VectorKey::Layer0Neighbors(_) => {
            decode_layer0_neighbors(value).map_err(|error| {
                format!("legacy vector layer-0 neighbors are malformed: {error}")
            })?;
        }
        VectorKey::UpperNeighbors(_) => {
            decode_upper_neighbors(value)
                .map_err(|error| format!("legacy vector upper neighbors are malformed: {error}"))?;
        }
        VectorKey::SimHash(_) => {
            decode_simhash(value)
                .map_err(|error| format!("legacy vector SimHash is malformed: {error}"))?;
        }
        VectorKey::UpperVector(_) | VectorKey::Vector(_) => {
            match decode_item_borrowed::<D>(value, dimension) {
                Ok(_) => {}
                Err(super::VectorItemDecodeError::ZeroNormCosineVector) => {
                    return Err("legacy cosine vector payload has zero norm".to_string());
                }
                Err(error) => {
                    return Err(format!("legacy vector payload is malformed: {error}"));
                }
            }
        }
        VectorKey::EntryCandidateSorted(_) | VectorKey::ReverseEdge(_) => {
            decode_empty_marker(value)
                .map_err(|error| format!("legacy vector marker is malformed: {error}"))?;
        }
        VectorKey::SimHashDirectory(_) => {
            decode_simhash_directory_marker_v1(value).map_err(|error| {
                format!("legacy vector SimHash directory marker is malformed: {error}")
            })?;
        }
        VectorKey::EntryCandidateNode(_) => {
            decode_entry_candidate_layer(value).map_err(|error| {
                format!("legacy vector entry-candidate layer is malformed: {error}")
            })?;
        }
        VectorKey::IndexPrefix(_)
        | VectorKey::VectorPrefix(_)
        | VectorKey::EntryCandidatePrefix(_)
        | VectorKey::MemoryPrefix(_)
        | VectorKey::L0Prefix(_)
        | VectorKey::SimHashDirectoryPrefix(_)
        | VectorKey::ReverseEdgePrefix(_) => {
            return Err("legacy vector prefix key was persisted as a physical row".to_string());
        }
    }
    Ok(())
}

impl VectorCleanupScan {
    /// Returns the next exact row while rejecting cross-lane or cross-index data.
    pub(crate) async fn next(&mut self) -> Result<Option<VectorCleanupRow>, HelixDbError> {
        loop {
            let Some((expected_lane, rows)) = self.lanes.front_mut() else {
                return Ok(None);
            };
            let Some(row) = rows.next().await? else {
                self.lanes.pop_front();
                continue;
            };
            let logical = self.keyspace.strip_physical_key(&row.key)?;
            let key = VectorKey::parse_from_slice(logical)?;
            if key.index_id() != self.keyspace.index_id() || key.storage_lane() != *expected_lane {
                return Err(HelixDbError::InvariantViolation(
                    "vector cleanup scan escaped its bound physical lane".to_string(),
                ));
            }
            let input_bytes = row
                .key
                .len()
                .checked_add(row.value.len())
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(|| {
                    HelixDbError::InvariantViolation(
                        "vector cleanup input measurement overflowed u64".to_string(),
                    )
                })?;
            return Ok(Some(VectorCleanupRow {
                keyspace: self.keyspace.clone(),
                physical_key: row.key,
                input_bytes,
            }));
        }
    }
}

impl VectorSimHashDirectoryCleanupScan {
    /// Returns the next exact directory row while rejecting namespace escape.
    pub(crate) async fn next(&mut self) -> Result<Option<VectorCleanupRow>, HelixDbError> {
        let Some(row) = self.rows.next().await? else {
            return Ok(None);
        };
        let logical = self.keyspace.strip_physical_key(&row.key)?;
        let VectorKey::SimHashDirectory(key) = VectorKey::parse_from_slice(logical)? else {
            return Err(HelixDbError::InvariantViolation(
                "SimHash directory cleanup scan returned another vector key kind".to_string(),
            ));
        };
        if key.index_id() != self.keyspace.index_id() {
            return Err(HelixDbError::InvariantViolation(
                "SimHash directory cleanup scan escaped its physical index".to_string(),
            ));
        }
        let input_bytes = row
            .key
            .len()
            .checked_add(row.value.len())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "SimHash directory cleanup input measurement overflowed u64".to_string(),
                )
            })?;
        Ok(Some(VectorCleanupRow {
            keyspace: self.keyspace.clone(),
            physical_key: row.key,
            input_bytes,
        }))
    }
}

/// Decoded state of one entry-candidate node-layer row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntryCandidateLayerRow {
    /// No node-layer row exists.
    Missing,
    /// The row contains this valid deployed layer value.
    Present(u16),
    /// Bytes exist but do not decode as the deployed layer value.
    Corrupt,
}

/// Decoded state of one deployed SimHash row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SimHashRow {
    /// No SimHash row exists for the requested node.
    Missing,
    /// The row contains one valid deployed 64-bit SimHash.
    Present(SimHash),
    /// Bytes exist but do not match the deployed SimHash codec.
    Corrupt,
}

/// One predicate-agnostic routing entry ordered by persisted SimHash code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SimHashDirectoryEntry {
    order_code: u64,
    node_id: NodeId,
}

impl SimHashDirectoryEntry {
    /// Returns the persisted locality-preserving SimHash order code.
    pub(crate) const fn order_code(self) -> u64 {
        self.order_code
    }

    /// Returns the exact entity ID represented by this routing row.
    pub(crate) const fn node_id(self) -> NodeId {
        self.node_id
    }
}

/// One bounded directory scan with exact decoded key/value input measurement.
#[derive(Debug)]
pub(crate) struct SimHashDirectoryWindow {
    entries: Vec<SimHashDirectoryEntry>,
    decoded_bytes: usize,
}

impl SimHashDirectoryWindow {
    /// Returns the validated entries in persisted locality order.
    pub(crate) fn entries(&self) -> &[SimHashDirectoryEntry] {
        &self.entries
    }

    /// Consumes the window and returns its validated entries.
    pub(crate) fn into_entries(self) -> Vec<SimHashDirectoryEntry> {
        self.entries
    }

    /// Returns exact physical key-plus-value bytes decoded by this scan.
    pub(crate) const fn decoded_bytes(&self) -> usize {
        self.decoded_bytes
    }
}

/// One typed row from the sorted entry-candidate scan.
///
/// The physical key remains private so repair code can request deletion without
/// gaining raw-byte access.
pub(crate) struct EntryCandidateRow<'a> {
    keyspace: &'a VectorRowKeyspace,
    physical_key: Bytes,
    node_id: NodeId,
    layer: u16,
}

impl EntryCandidateRow<'_> {
    /// Returns the candidate node encoded in the sorted row.
    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the candidate layer encoded in descending-priority order.
    pub(crate) const fn layer(&self) -> u16 {
        self.layer
    }
}

/// Typed iterator over parseable sorted entry-candidate rows.
pub(crate) struct EntryCandidateScan<'a> {
    rows: slatedb::DbIterator,
    keyspace: &'a VectorRowKeyspace,
}

/// Typed reverse-edge sources and opaque cleanup tokens for one target node.
///
/// Sources are canonicalized by layer for graph repair. Physical locator keys
/// remain private and are bound to the originating keyspace, allowing deletion
/// without exposing raw bytes or permitting cross-index cleanup.
pub(crate) struct ReverseSourcesForTarget {
    keyspace: VectorRowKeyspace,
    sources_by_layer: BTreeMap<u16, Vec<NodeId>>,
    locator_keys: Vec<Bytes>,
}

impl ReverseSourcesForTarget {
    /// Returns the canonical source map used to discover repair layers.
    pub(crate) fn sources_by_layer(&self) -> &BTreeMap<u16, Vec<NodeId>> {
        &self.sources_by_layer
    }

    /// Returns canonical sources that reference the target on one layer.
    pub(crate) fn sources_at(&self, layer: u16) -> &[NodeId] {
        self.sources_by_layer
            .get(&layer)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

impl<'a> EntryCandidateScan<'a> {
    /// Returns the next typed candidate, skipping foreign or malformed row kinds.
    ///
    /// Tenant-prefix mismatches fail closed because they indicate a storage
    /// isolation violation; malformed logical keys retain the deployed tolerant
    /// scan behavior and are ignored.
    pub(crate) async fn next(&mut self) -> Result<Option<EntryCandidateRow<'a>>, HelixDbError> {
        while let Some(row) = self.rows.next().await? {
            let logical_key = self.keyspace.strip_physical_key(&row.key)?;
            let Ok(VectorKey::EntryCandidateSorted(candidate)) =
                VectorKey::parse_from_slice(logical_key)
            else {
                continue;
            };
            return Ok(Some(EntryCandidateRow {
                keyspace: self.keyspace,
                physical_key: row.key,
                node_id: candidate.node_id(),
                layer: candidate.layer(),
            }));
        }
        Ok(None)
    }
}

impl<'a, R: ?Sized> VectorRows<'a, R> {
    /// Binds a read backend to one already complete physical row namespace.
    pub(crate) const fn new(read: &'a R, keyspace: &'a VectorRowKeyspace) -> Self {
        Self { read, keyspace }
    }
}

impl<R> VectorRows<'_, R>
where
    R: DbReadOps + Send + Sync + ?Sized,
{
    /// Reads one legacy payload and accounts every typed point-read byte.
    ///
    /// This is the only migration boundary that combines frozen legacy
    /// metadata, SimHash-derived canonical addressing, and payload decoding.
    /// A missing SimHash with a surviving layer-zero row remains corruption;
    /// complete physical absence is returned as an explicit state.
    pub(crate) async fn legacy_vector_for_migration<D: Distance>(
        &self,
        entity_id: NodeId,
        definition: &ValidatedVectorIndexDefinition,
    ) -> Result<LegacyVectorMigrationRead, HelixDbError> {
        let metadata_key =
            self.keyspace
                .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                    self.keyspace.index_id(),
                )));
        let metadata_value = self.read.get(&metadata_key).await?;
        let mut input_bytes = u64::try_from(metadata_key.len())
            .ok()
            .and_then(|bytes| {
                bytes.checked_add(
                    metadata_value
                        .as_ref()
                        .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                )
            })
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "legacy vector migration metadata bytes overflowed u64".to_string(),
                )
            })?;
        let Some(metadata_value) = metadata_value else {
            return Ok(LegacyVectorMigrationRead::Absent { input_bytes });
        };
        let metadata = decode_legacy_metadata(&metadata_value).map_err(HelixDbError::Encoding)?;
        metadata.validated_state()?;
        let expected =
            VectorIndexConfig::from_v2_definition(definition, self.keyspace.physical_name());
        if !metadata.config.has_same_physical_contract(&expected) {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "legacy vector metadata for '{}' conflicts with its persisted definition",
                self.keyspace.physical_name()
            )));
        }

        let simhash_key = self.keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
            self.keyspace.index_id(),
            entity_id,
        )));
        let simhash_value = self.read.get(&simhash_key).await?;
        input_bytes = input_bytes
            .checked_add(u64::try_from(simhash_key.len()).unwrap_or(u64::MAX))
            .and_then(|bytes| {
                bytes.checked_add(
                    simhash_value
                        .as_ref()
                        .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                )
            })
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "legacy vector migration SimHash bytes overflowed u64".to_string(),
                )
            })?;
        let Some(simhash_value) = simhash_value else {
            let layer0_key =
                self.keyspace
                    .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                        self.keyspace.index_id(),
                        entity_id,
                    )));
            let layer0_value = self.read.get(&layer0_key).await?;
            input_bytes = input_bytes
                .checked_add(u64::try_from(layer0_key.len()).unwrap_or(u64::MAX))
                .and_then(|bytes| {
                    bytes.checked_add(
                        layer0_value
                            .as_ref()
                            .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                    )
                })
                .ok_or_else(|| {
                    HelixDbError::InvariantViolation(
                        "legacy vector migration layer-zero bytes overflowed u64".to_string(),
                    )
                })?;
            if layer0_value.is_some() {
                return Err(HelixDbError::InvariantViolation(format!(
                    "missing simhash for node {entity_id} in index {} while materializing a legacy vector property",
                    self.keyspace.index_id()
                )));
            }
            return Ok(LegacyVectorMigrationRead::Absent { input_bytes });
        };
        let simhash_bits = decode_simhash(&simhash_value)?;
        let vector_key = self.keyspace.key(VectorKey::Vector(VectorItemKey::new(
            self.keyspace.index_id(),
            super::simhash::order_code_from_simhash_bits(simhash_bits),
            entity_id,
        )));
        let vector_value = self.read.get(&vector_key).await?;
        input_bytes = input_bytes
            .checked_add(u64::try_from(vector_key.len()).unwrap_or(u64::MAX))
            .and_then(|bytes| {
                bytes.checked_add(
                    vector_value
                        .as_ref()
                        .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                )
            })
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "legacy vector migration payload bytes overflowed u64".to_string(),
                )
            })?;
        let Some(vector_value) = vector_value else {
            return Ok(LegacyVectorMigrationRead::Absent { input_bytes });
        };
        let dimension = VectorDimension::try_new(definition.dimension() as usize)
            .map_err(|error| HelixDbError::Config(error.to_string()))?;
        let item = decode_item_borrowed::<D>(&vector_value, dimension)?;
        Ok(LegacyVectorMigrationRead::Present {
            vector: item.vector.to_vec(),
            input_bytes,
        })
    }

    /// Validates one bounded page of a frozen pre-V2 physical lane.
    ///
    /// This is intentionally read-only. It parses every key through `encoding/v1`,
    /// decodes every value through the owning vector codec, and performs terminal
    /// metadata and entry-point proofs without checking graph-wide neighbor links.
    pub(crate) async fn validate_legacy_physical<D: Distance>(
        &self,
        lane: VectorStorageLane,
        cursor: Option<&[u8]>,
        definition: &ValidatedVectorIndexDefinition,
        mode: LegacyVectorValidationMode,
        max_entities: usize,
        max_input_bytes: u64,
    ) -> Result<LegacyVectorValidationOutcome, HelixDbError> {
        let prefix = self.keyspace.key(lane.prefix_key(self.keyspace.index_id()));
        let start = match cursor {
            None => Bound::Unbounded,
            Some(cursor) => {
                let Some(suffix) = cursor.strip_prefix(prefix.as_ref()) else {
                    return Ok(LegacyVectorValidationOutcome::Invalid {
                        reason: "legacy vector validation cursor escaped its physical lane"
                            .to_string(),
                    });
                };
                Bound::Excluded(Bytes::copy_from_slice(suffix))
            }
        };
        let expected_config =
            VectorIndexConfig::from_v2_definition(definition, self.keyspace.physical_name());
        let dimension = match VectorDimension::try_new(definition.dimension() as usize) {
            Ok(dimension) => dimension,
            Err(error) => {
                return Ok(LegacyVectorValidationOutcome::Invalid {
                    reason: format!("legacy vector dimension is invalid: {error}"),
                });
            }
        };
        let mut rows = self
            .read
            .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
            .await?;
        let mut validated_rows = 0_usize;
        let mut input_bytes = 0_u64;
        let mut last_key = None;
        let mut exhausted = true;
        let mut directory_entries = Vec::new();
        let mut predicted_directory_operations = 0_u64;
        let mut predicted_directory_bytes = 0_u64;
        while validated_rows < max_entities {
            let Some(row) = rows.next().await? else {
                break;
            };
            let row_bytes = match row
                .key
                .len()
                .checked_add(row.value.len())
                .and_then(|bytes| u64::try_from(bytes).ok())
            {
                Some(row_bytes) => row_bytes,
                None => {
                    return Ok(LegacyVectorValidationOutcome::Invalid {
                        reason: "legacy vector validation byte count overflowed u64".to_string(),
                    });
                }
            };
            let Some(next_input_bytes) = input_bytes.checked_add(row_bytes) else {
                return Ok(LegacyVectorValidationOutcome::Invalid {
                    reason: "legacy vector validation cumulative bytes overflowed u64".to_string(),
                });
            };
            if next_input_bytes > max_input_bytes {
                if validated_rows == 0 {
                    return Ok(LegacyVectorValidationOutcome::Oversized {
                        observed: row_bytes,
                        limit: max_input_bytes,
                    });
                }
                exhausted = false;
                break;
            }
            let logical = match self.keyspace.strip_physical_key(&row.key) {
                Ok(logical) => logical,
                Err(error) => {
                    return Ok(LegacyVectorValidationOutcome::Invalid {
                        reason: error.to_string(),
                    });
                }
            };
            let key = match VectorKey::parse_from_slice(logical) {
                Ok(key) => key,
                Err(error) => {
                    return Ok(LegacyVectorValidationOutcome::Invalid {
                        reason: format!("malformed legacy vector key: {error}"),
                    });
                }
            };
            if key.index_id() != self.keyspace.index_id() || key.storage_lane() != lane {
                return Ok(LegacyVectorValidationOutcome::Invalid {
                    reason: "legacy vector scan escaped its reserved namespace".to_string(),
                });
            }
            if let Err(error) =
                validate_legacy_row::<D>(&key, &row.value, &expected_config, dimension)
            {
                return Ok(LegacyVectorValidationOutcome::Invalid { reason: error });
            }
            let directory_entry = match (mode, &key) {
                (
                    LegacyVectorValidationMode::BackfillSimHashDirectory {
                        max_output_operations,
                        max_output_bytes,
                    },
                    VectorKey::Vector(item_key),
                ) => {
                    let token = CanonicalVectorRowKey {
                        scope: self.keyspace.scope(),
                        index_id: item_key.index_id(),
                        order_code: item_key.order_code(),
                        node_id: item_key.node_id(),
                        physical_key: row.key.clone(),
                    };
                    let marker_key = self.keyspace.key(VectorKey::SimHashDirectory(
                        VectorSimHashDirectoryKey::new(
                            item_key.index_id(),
                            item_key.order_code(),
                            item_key.node_id(),
                        ),
                    ));
                    let marker_bytes = match marker_key
                        .len()
                        .checked_add(encode_simhash_directory_marker_v1().len())
                        .and_then(|bytes| u64::try_from(bytes).ok())
                    {
                        Some(marker_bytes) => marker_bytes,
                        None => {
                            return Ok(LegacyVectorValidationOutcome::Invalid {
                                reason: "legacy vector directory output bytes overflowed u64"
                                    .to_string(),
                            });
                        }
                    };
                    let Some(next_operations) = predicted_directory_operations.checked_add(1)
                    else {
                        return Ok(LegacyVectorValidationOutcome::Invalid {
                            reason: "legacy vector directory operations overflowed u64".to_string(),
                        });
                    };
                    let Some(next_bytes) = predicted_directory_bytes.checked_add(marker_bytes)
                    else {
                        return Ok(LegacyVectorValidationOutcome::Invalid {
                            reason: "legacy vector directory cumulative bytes overflowed u64"
                                .to_string(),
                        });
                    };
                    if next_operations > max_output_operations.get()
                        || next_bytes > max_output_bytes.get()
                    {
                        if validated_rows == 0 {
                            return Ok(LegacyVectorValidationOutcome::Oversized {
                                observed: marker_bytes,
                                limit: max_output_bytes.get(),
                            });
                        }
                        exhausted = false;
                        break;
                    }
                    predicted_directory_operations = next_operations;
                    predicted_directory_bytes = next_bytes;
                    Some(token)
                }
                (LegacyVectorValidationMode::ReadOnly, _)
                | (LegacyVectorValidationMode::BackfillSimHashDirectory { .. }, _) => None,
            };
            validated_rows += 1;
            input_bytes = next_input_bytes;
            last_key = Some(row.key);
            if let Some(directory_entry) = directory_entry {
                directory_entries.push(directory_entry);
            }
        }
        if validated_rows == max_entities {
            exhausted = false;
        }
        if exhausted {
            let metadata = match self.legacy_metadata().await {
                Ok(Some(metadata)) => metadata,
                Ok(None) => {
                    return Ok(LegacyVectorValidationOutcome::Invalid {
                        reason: "legacy vector metadata row is missing".to_string(),
                    });
                }
                Err(error) => {
                    return Ok(LegacyVectorValidationOutcome::Invalid {
                        reason: format!("legacy vector metadata is invalid: {error}"),
                    });
                }
            };
            if !metadata.config.has_same_physical_contract(&expected_config) {
                return Ok(LegacyVectorValidationOutcome::Invalid {
                    reason: "legacy vector metadata differs from its persisted definition"
                        .to_string(),
                });
            }
            if lane == VectorStorageLane::Layer0
                && let Some(entry_point) = metadata.entry_point
            {
                let simhash_key = self.keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                    self.keyspace.index_id(),
                    entry_point,
                )));
                let Some(simhash_value) = self.read.get(simhash_key).await? else {
                    return Ok(LegacyVectorValidationOutcome::Invalid {
                        reason: "legacy vector entry point has no SimHash row".to_string(),
                    });
                };
                let bits = match decode_simhash(&simhash_value) {
                    Ok(bits) => bits,
                    Err(error) => {
                        return Ok(LegacyVectorValidationOutcome::Invalid {
                            reason: format!(
                                "legacy vector entry-point SimHash is invalid: {error}"
                            ),
                        });
                    }
                };
                let item_key = self.keyspace.key(VectorKey::Vector(VectorItemKey::new(
                    self.keyspace.index_id(),
                    super::simhash::order_code_from_simhash_bits(bits),
                    entry_point,
                )));
                let Some(item_value) = self.read.get(item_key).await? else {
                    return Ok(LegacyVectorValidationOutcome::Invalid {
                        reason: "legacy vector entry point does not resolve to a payload row"
                            .to_string(),
                    });
                };
                if let Err(error) = decode_item_borrowed::<D>(&item_value, dimension) {
                    return Ok(LegacyVectorValidationOutcome::Invalid {
                        reason: format!("legacy vector entry-point payload is invalid: {error}"),
                    });
                }
            }
        }
        Ok(LegacyVectorValidationOutcome::Valid {
            last_key,
            rows: validated_rows as u64,
            input_bytes,
            exhausted,
            directory_entries,
            predicted_directory_writes: VectorWriteMeasurement::from_exact_parts(
                predicted_directory_operations,
                predicted_directory_bytes,
            ),
        })
    }

    /// Validates one bounded page of compact directory rows and its terminal entry point.
    pub(crate) async fn validate_simhash_directory<D: Distance>(
        &self,
        cursor: Option<&[u8]>,
        definition: &ValidatedVectorIndexDefinition,
        mode: SimHashDirectoryValidationMode,
        max_entities: usize,
        max_input_bytes: u64,
    ) -> Result<SimHashDirectoryValidationOutcome, HelixDbError> {
        let prefix = self.keyspace.key(VectorKey::SimHashDirectoryPrefix(
            VectorSimHashDirectoryPrefixKey::new(self.keyspace.index_id()),
        ));
        let start = match cursor {
            None => Bound::Unbounded,
            Some(cursor) => {
                let Some(suffix) = cursor.strip_prefix(prefix.as_ref()) else {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "SimHash directory cursor escaped its typed prefix".to_string(),
                    });
                };
                Bound::Excluded(Bytes::copy_from_slice(suffix))
            }
        };
        let expected_config =
            VectorIndexConfig::from_v2_definition(definition, self.keyspace.physical_name());
        let mut rows = self
            .read
            .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
            .await?;
        let mut markers = 0_usize;
        let mut input_bytes = 0_u64;
        let mut last_key = None;
        let mut exhausted = true;
        while markers < max_entities {
            let Some(row) = rows.next().await? else {
                break;
            };
            let row_bytes = match row
                .key
                .len()
                .checked_add(row.value.len())
                .and_then(|bytes| u64::try_from(bytes).ok())
            {
                Some(row_bytes) => row_bytes,
                None => {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "SimHash directory input bytes overflowed u64".to_string(),
                    });
                }
            };
            let Some(next_input_bytes) = input_bytes.checked_add(row_bytes) else {
                return Ok(SimHashDirectoryValidationOutcome::Invalid {
                    reason: "SimHash directory cumulative bytes overflowed u64".to_string(),
                });
            };
            if next_input_bytes > max_input_bytes {
                if markers == 0 {
                    return Ok(SimHashDirectoryValidationOutcome::Oversized {
                        observed: row_bytes,
                        limit: max_input_bytes,
                    });
                }
                exhausted = false;
                break;
            }
            let logical = match self.keyspace.strip_physical_key(&row.key) {
                Ok(logical) => logical,
                Err(error) => {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: error.to_string(),
                    });
                }
            };
            let key = match VectorKey::parse_from_slice(logical) {
                Ok(VectorKey::SimHashDirectory(key)) => key,
                Ok(_) => {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "SimHash directory scan returned a non-directory key".to_string(),
                    });
                }
                Err(error) => {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: format!("malformed SimHash directory key: {error}"),
                    });
                }
            };
            if key.index_id() != self.keyspace.index_id() {
                return Ok(SimHashDirectoryValidationOutcome::Invalid {
                    reason: "SimHash directory scan escaped its physical index".to_string(),
                });
            }
            if let Err(error) = decode_simhash_directory_marker_v1(&row.value) {
                return Ok(SimHashDirectoryValidationOutcome::Invalid {
                    reason: format!("invalid SimHash directory marker: {error}"),
                });
            }
            if mode == SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence {
                let canonical_key = self.keyspace.key(VectorKey::Vector(VectorItemKey::new(
                    key.index_id(),
                    key.order_code(),
                    key.node_id(),
                )));
                let canonical_value = self.read.get(&canonical_key).await?;
                let canonical_input_bytes = canonical_key
                    .len()
                    .checked_add(canonical_value.as_ref().map_or(0, Bytes::len))
                    .and_then(|bytes| u64::try_from(bytes).ok());
                let Some(canonical_input_bytes) = canonical_input_bytes else {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "canonical marker proof bytes overflowed u64".to_string(),
                    });
                };
                let Some(with_canonical_bytes) =
                    next_input_bytes.checked_add(canonical_input_bytes)
                else {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "directory preflight cumulative bytes overflowed u64".to_string(),
                    });
                };
                if with_canonical_bytes > max_input_bytes {
                    if markers == 0 {
                        return Ok(SimHashDirectoryValidationOutcome::Oversized {
                            observed: row_bytes.saturating_add(canonical_input_bytes),
                            limit: max_input_bytes,
                        });
                    }
                    exhausted = false;
                    break;
                }
                let Some(canonical_value) = canonical_value else {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "SimHash directory marker has no canonical vector".to_string(),
                    });
                };
                let dimension = match VectorDimension::try_new(definition.dimension() as usize) {
                    Ok(dimension) => dimension,
                    Err(error) => {
                        return Ok(SimHashDirectoryValidationOutcome::Invalid {
                            reason: format!("legacy vector dimension is invalid: {error}"),
                        });
                    }
                };
                if let Err(error) = decode_item_borrowed::<D>(&canonical_value, dimension) {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: format!("SimHash directory canonical payload is invalid: {error}"),
                    });
                }
                input_bytes = with_canonical_bytes;
            } else {
                input_bytes = next_input_bytes;
            }
            markers += 1;
            last_key = Some(row.key);
        }
        if markers == max_entities {
            exhausted = false;
        }
        if exhausted && mode != SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence {
            let metadata_key =
                self.keyspace
                    .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                        self.keyspace.index_id(),
                    )));
            let metadata_value = self.read.get(&metadata_key).await?;
            let metadata_input_bytes = metadata_key
                .len()
                .checked_add(metadata_value.as_ref().map_or(0, Bytes::len))
                .and_then(|bytes| u64::try_from(bytes).ok());
            let Some(metadata_input_bytes) = metadata_input_bytes else {
                return Ok(SimHashDirectoryValidationOutcome::Invalid {
                    reason: "vector metadata proof bytes overflowed u64".to_string(),
                });
            };
            let Some(with_metadata_bytes) = input_bytes.checked_add(metadata_input_bytes) else {
                return Ok(SimHashDirectoryValidationOutcome::Invalid {
                    reason: "directory proof cumulative bytes overflowed u64".to_string(),
                });
            };
            if with_metadata_bytes > max_input_bytes {
                if markers == 0 {
                    return Ok(SimHashDirectoryValidationOutcome::Oversized {
                        observed: metadata_input_bytes,
                        limit: max_input_bytes,
                    });
                }
                return Ok(SimHashDirectoryValidationOutcome::Valid {
                    last_key,
                    markers: markers as u64,
                    input_bytes,
                    exhausted: false,
                });
            }
            let Some(metadata_value) = metadata_value else {
                return Ok(SimHashDirectoryValidationOutcome::Invalid {
                    reason: "vector metadata row is missing".to_string(),
                });
            };
            let metadata = match mode {
                SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint => {
                    decode_legacy_metadata(&metadata_value).map_err(HelixDbError::Encoding)
                }
                SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint => {
                    decode_metadata(&metadata_value)
                        .map_err(|error| HelixDbError::Encoding(EncodingError::Rkyv(error)))
                }
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence => unreachable!(),
            };
            let metadata = match metadata {
                Ok(metadata) => metadata,
                Err(error) => {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: format!("vector metadata is invalid: {error}"),
                    });
                }
            };
            if let Err(error) = metadata.validated_state() {
                return Ok(SimHashDirectoryValidationOutcome::Invalid {
                    reason: format!("vector metadata state is invalid: {error}"),
                });
            }
            if metadata.config.index_name != self.keyspace.physical_name() {
                return Ok(SimHashDirectoryValidationOutcome::Invalid {
                    reason: "vector metadata physical name differs from its namespace".to_string(),
                });
            }
            if !metadata.config.has_same_physical_contract(&expected_config) {
                return Ok(SimHashDirectoryValidationOutcome::Invalid {
                    reason: "legacy vector metadata differs from its persisted definition"
                        .to_string(),
                });
            }
            input_bytes = with_metadata_bytes;
            if let Some(entry_point) = metadata.entry_point {
                let simhash_key = self.keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                    self.keyspace.index_id(),
                    entry_point,
                )));
                let simhash_value = self.read.get(&simhash_key).await?;
                let simhash_input_bytes = simhash_key
                    .len()
                    .checked_add(simhash_value.as_ref().map_or(0, Bytes::len))
                    .and_then(|bytes| u64::try_from(bytes).ok());
                let Some(simhash_input_bytes) = simhash_input_bytes else {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "entry-point SimHash proof bytes overflowed u64".to_string(),
                    });
                };
                let Some(with_simhash_bytes) = input_bytes.checked_add(simhash_input_bytes) else {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "entry-point proof cumulative bytes overflowed u64".to_string(),
                    });
                };
                if with_simhash_bytes > max_input_bytes {
                    if markers == 0 {
                        return Ok(SimHashDirectoryValidationOutcome::Oversized {
                            observed: metadata_input_bytes.saturating_add(simhash_input_bytes),
                            limit: max_input_bytes,
                        });
                    }
                    return Ok(SimHashDirectoryValidationOutcome::Valid {
                        last_key,
                        markers: markers as u64,
                        input_bytes: input_bytes.saturating_sub(metadata_input_bytes),
                        exhausted: false,
                    });
                }
                let Some(simhash_value) = simhash_value else {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "legacy vector entry point has no SimHash row".to_string(),
                    });
                };
                let bits = match decode_simhash(&simhash_value) {
                    Ok(bits) => bits,
                    Err(error) => {
                        return Ok(SimHashDirectoryValidationOutcome::Invalid {
                            reason: format!(
                                "legacy vector entry-point SimHash is invalid: {error}"
                            ),
                        });
                    }
                };
                let marker_key =
                    self.keyspace
                        .key(VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
                            self.keyspace.index_id(),
                            super::simhash::order_code_from_simhash_bits(bits),
                            entry_point,
                        )));
                let marker_value = self.read.get(&marker_key).await?;
                let marker_input_bytes = marker_key
                    .len()
                    .checked_add(marker_value.as_ref().map_or(0, Bytes::len))
                    .and_then(|bytes| u64::try_from(bytes).ok());
                let Some(marker_input_bytes) = marker_input_bytes else {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "entry-point marker proof bytes overflowed u64".to_string(),
                    });
                };
                let Some(with_marker_bytes) = with_simhash_bytes.checked_add(marker_input_bytes)
                else {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "entry-point proof cumulative bytes overflowed u64".to_string(),
                    });
                };
                if with_marker_bytes > max_input_bytes {
                    if markers == 0 {
                        return Ok(SimHashDirectoryValidationOutcome::Oversized {
                            observed: metadata_input_bytes
                                .saturating_add(simhash_input_bytes)
                                .saturating_add(marker_input_bytes),
                            limit: max_input_bytes,
                        });
                    }
                    return Ok(SimHashDirectoryValidationOutcome::Valid {
                        last_key,
                        markers: markers as u64,
                        input_bytes: input_bytes.saturating_sub(metadata_input_bytes),
                        exhausted: false,
                    });
                }
                let Some(marker_value) = marker_value else {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: "legacy vector entry point has no SimHash directory marker"
                            .to_string(),
                    });
                };
                if let Err(error) = decode_simhash_directory_marker_v1(&marker_value) {
                    return Ok(SimHashDirectoryValidationOutcome::Invalid {
                        reason: format!(
                            "legacy vector entry-point directory marker is invalid: {error}"
                        ),
                    });
                }
                input_bytes = with_marker_bytes;
            }
        }
        Ok(SimHashDirectoryValidationOutcome::Valid {
            last_key,
            markers: markers as u64,
            input_bytes,
            exhausted,
        })
    }

    /// Scans canonical vector payloads exactly once and emits only missing marker tokens.
    pub(crate) async fn backfill_missing_simhash_directory<D: Distance>(
        &self,
        cursor: Option<&[u8]>,
        definition: &ValidatedVectorIndexDefinition,
        max_entities: usize,
        max_input_bytes: u64,
        max_output_operations: NonZeroU64,
        max_output_bytes: NonZeroU64,
    ) -> Result<CanonicalVectorDirectoryBackfillOutcome, HelixDbError> {
        let prefix = self
            .keyspace
            .key(VectorKey::VectorPrefix(VectorItemPrefixKey::new(
                self.keyspace.index_id(),
            )));
        let start = match cursor {
            None => Bound::Unbounded,
            Some(cursor) => {
                let Some(suffix) = cursor.strip_prefix(prefix.as_ref()) else {
                    return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                        reason: "canonical vector cursor escaped its typed prefix".to_string(),
                    });
                };
                Bound::Excluded(Bytes::copy_from_slice(suffix))
            }
        };
        let dimension = match VectorDimension::try_new(definition.dimension() as usize) {
            Ok(dimension) => dimension,
            Err(error) => {
                return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                    reason: format!("legacy vector dimension is invalid: {error}"),
                });
            }
        };
        let mut rows = self
            .read
            .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
            .await?;
        let mut canonical_vectors = 0_usize;
        let mut existing_markers = 0_u64;
        let mut input_bytes = 0_u64;
        let mut directory_entries = Vec::new();
        let mut predicted_operations = 0_u64;
        let mut predicted_bytes = 0_u64;
        let mut last_key = None;
        let mut exhausted = true;
        while canonical_vectors < max_entities {
            let Some(row) = rows.next().await? else {
                break;
            };
            let logical = match self.keyspace.strip_physical_key(&row.key) {
                Ok(logical) => logical,
                Err(error) => {
                    return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                        reason: error.to_string(),
                    });
                }
            };
            let item_key = match VectorKey::parse_from_slice(logical) {
                Ok(VectorKey::Vector(item_key)) => item_key,
                Ok(_) => {
                    return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                        reason: "canonical vector scan returned another key kind".to_string(),
                    });
                }
                Err(error) => {
                    return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                        reason: format!("malformed canonical vector key: {error}"),
                    });
                }
            };
            if item_key.index_id() != self.keyspace.index_id() {
                return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                    reason: "canonical vector scan escaped its physical index".to_string(),
                });
            }
            if let Err(error) = decode_item_borrowed::<D>(&row.value, dimension) {
                return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                    reason: format!("canonical vector payload is invalid: {error}"),
                });
            }
            let marker_key =
                self.keyspace
                    .key(VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
                        item_key.index_id(),
                        item_key.order_code(),
                        item_key.node_id(),
                    )));
            let marker_value = self.read.get(&marker_key).await?;
            if let Some(marker_value) = &marker_value
                && let Err(error) = decode_simhash_directory_marker_v1(marker_value)
            {
                return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                    reason: format!("existing SimHash directory marker is invalid: {error}"),
                });
            }
            let row_input_bytes = row
                .key
                .len()
                .checked_add(row.value.len())
                .and_then(|bytes| bytes.checked_add(marker_key.len()))
                .and_then(|bytes| bytes.checked_add(marker_value.as_ref().map_or(0, Bytes::len)))
                .and_then(|bytes| u64::try_from(bytes).ok());
            let Some(row_input_bytes) = row_input_bytes else {
                return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                    reason: "canonical vector backfill input bytes overflowed u64".to_string(),
                });
            };
            let Some(next_input_bytes) = input_bytes.checked_add(row_input_bytes) else {
                return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                    reason: "canonical vector backfill cumulative bytes overflowed u64".to_string(),
                });
            };
            let marker_output_bytes = if marker_value.is_none() {
                marker_key
                    .len()
                    .checked_add(encode_simhash_directory_marker_v1().len())
                    .and_then(|bytes| u64::try_from(bytes).ok())
            } else {
                Some(0)
            };
            let Some(marker_output_bytes) = marker_output_bytes else {
                return Ok(CanonicalVectorDirectoryBackfillOutcome::Invalid {
                    reason: "canonical vector marker bytes overflowed u64".to_string(),
                });
            };
            let next_operations =
                predicted_operations.saturating_add(u64::from(marker_value.is_none()));
            let next_output_bytes = predicted_bytes.saturating_add(marker_output_bytes);
            if next_input_bytes > max_input_bytes
                || next_operations > max_output_operations.get()
                || next_output_bytes > max_output_bytes.get()
            {
                if canonical_vectors == 0 {
                    return Ok(CanonicalVectorDirectoryBackfillOutcome::Oversized {
                        observed: row_input_bytes.max(marker_output_bytes),
                        limit: max_input_bytes.min(max_output_bytes.get()),
                    });
                }
                exhausted = false;
                break;
            }
            if marker_value.is_none() {
                directory_entries.push(CanonicalVectorRowKey {
                    scope: self.keyspace.scope(),
                    index_id: item_key.index_id(),
                    order_code: item_key.order_code(),
                    node_id: item_key.node_id(),
                    physical_key: row.key.clone(),
                });
            } else {
                existing_markers = existing_markers.saturating_add(1);
            }
            canonical_vectors += 1;
            input_bytes = next_input_bytes;
            predicted_operations = next_operations;
            predicted_bytes = next_output_bytes;
            last_key = Some(row.key);
        }
        if canonical_vectors == max_entities {
            exhausted = false;
        }
        Ok(CanonicalVectorDirectoryBackfillOutcome::Valid {
            last_key,
            canonical_vectors: canonical_vectors as u64,
            existing_markers,
            input_bytes,
            directory_entries,
            predicted_directory_writes: VectorWriteMeasurement::from_exact_parts(
                predicted_operations,
                predicted_bytes,
            ),
            exhausted,
        })
    }

    /// Opens one exhaustive cleanup scan from each current physical lane.
    ///
    /// Callers intentionally restart at the lane prefixes after each committed
    /// batch. Previously deleted rows are absent, so no separate physical-row
    /// cursor or side record is required.
    pub(crate) async fn cleanup_scan(&self) -> Result<VectorCleanupScan, HelixDbError> {
        let mut lanes = VecDeque::with_capacity(VectorStorageLane::ALL.len());
        for lane in VectorStorageLane::ALL {
            let prefix = self.keyspace.key(lane.prefix_key(self.keyspace.index_id()));
            lanes.push_back((lane, self.read.scan_prefix(prefix, ..).await?));
        }
        Ok(VectorCleanupScan {
            keyspace: self.keyspace.clone(),
            lanes,
        })
    }

    /// Opens an exhaustive scan over only the typed SimHash-directory prefix.
    pub(crate) async fn simhash_directory_cleanup_scan(
        &self,
    ) -> Result<VectorSimHashDirectoryCleanupScan, HelixDbError> {
        let prefix = self.keyspace.key(VectorKey::SimHashDirectoryPrefix(
            VectorSimHashDirectoryPrefixKey::new(self.keyspace.index_id()),
        ));
        Ok(VectorSimHashDirectoryCleanupScan {
            keyspace: self.keyspace.clone(),
            rows: self.read.scan_prefix(prefix, ..).await?,
        })
    }

    /// Reads and validates the deployed metadata row without exposing bytes.
    ///
    /// Structural state and the complete physical name are checked before the
    /// value can enter search or mutation. Absence remains `None`; malformed or
    /// colliding rows fail closed with the existing public error variants.
    pub(crate) async fn metadata(&self) -> Result<Option<VectorIndexMetadata>, HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                self.keyspace.index_id(),
            )));
        let Some(data) = self.read.get(key).await? else {
            return Ok(None);
        };
        let metadata = decode_metadata(&data)
            .map_err(|error| HelixDbError::Encoding(EncodingError::Rkyv(error)))?;
        metadata.validated_state()?;
        if metadata.config.index_name != self.keyspace.physical_name() {
            return Err(HelixDbError::Config(format!(
                "Vector index id collision: requested '{}', stored '{}'",
                self.keyspace.physical_name(),
                metadata.config.index_name
            )));
        }
        Ok(Some(metadata))
    }

    /// Reads the frozen pre-V2 production metadata shape for migration only.
    pub(crate) async fn legacy_metadata(
        &self,
    ) -> Result<Option<VectorIndexMetadata>, HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                self.keyspace.index_id(),
            )));
        let Some(data) = self.read.get(key).await? else {
            return Ok(None);
        };
        let metadata = decode_legacy_metadata(&data).map_err(HelixDbError::Encoding)?;
        metadata.validated_state()?;
        if metadata.config.index_name != self.keyspace.physical_name() {
            return Err(HelixDbError::Config(format!(
                "Vector index id collision: requested '{}', stored '{}'",
                self.keyspace.physical_name(),
                metadata.config.index_name
            )));
        }
        Ok(Some(metadata))
    }

    /// Measures the exact deployed metadata point-read key and value bytes.
    ///
    /// Absence still charges the complete typed lookup key. The operation is
    /// read-only and does not decode or rewrite the stored value.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) async fn metadata_input_bytes(&self) -> Result<u64, HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                self.keyspace.index_id(),
            )));
        let value = self.read.get(&key).await?;
        key.len()
            .checked_add(value.as_ref().map_or(0, Bytes::len))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "vector metadata input measurement overflowed u64".to_string(),
                )
            })
    }

    /// Reads one deployed layer-0 neighbor row as a typed list.
    ///
    /// A missing row is the deployed empty-neighbor state. Decoding remains in
    /// this storage boundary so graph traversal never handles persisted bytes.
    pub(crate) async fn layer0_neighbors(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<NodeId>, HelixDbError> {
        Ok(self.layer0_neighbor_row(node_id).await?.unwrap_or_default())
    }

    /// Reads one layer-0 row while preserving physical absence.
    ///
    /// Mutation caching uses this distinction so an unloaded row, a known
    /// absent row, and a present encoded empty set cannot collapse together.
    pub(crate) async fn layer0_neighbor_row(
        &self,
        node_id: NodeId,
    ) -> Result<Option<Vec<NodeId>>, HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                self.keyspace.index_id(),
                node_id,
            )));
        let Some(value) = self.read.get(key).await? else {
            return Ok(None);
        };
        decode_layer0_neighbors(&value)
            .map(Some)
            .map_err(Into::into)
    }

    /// Tests physical presence of one layer-0 row without decoding its value.
    ///
    /// Missing-SimHash recovery intentionally treats any companion row bytes,
    /// including malformed ones, as evidence that the node is not absent.
    pub(crate) async fn layer0_row_exists(&self, node_id: NodeId) -> Result<bool, HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                self.keyspace.index_id(),
                node_id,
            )));
        Ok(self.read.get(key).await?.is_some())
    }

    /// Batch-tests layer-0 row presence while preserving caller order.
    ///
    /// Values are deliberately not decoded for the same corruption-recovery
    /// contract as [`Self::layer0_row_exists`].
    pub(crate) async fn layer0_rows_exist(
        &self,
        node_ids: &[NodeId],
    ) -> Result<Vec<bool>, HelixDbError> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys = node_ids
            .iter()
            .map(|node_id| {
                self.keyspace
                    .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                        self.keyspace.index_id(),
                        *node_id,
                    )))
            })
            .collect::<Vec<_>>();
        Ok(self
            .read
            .multi_get(&keys)
            .await?
            .into_iter()
            .map(|row| row.is_some())
            .collect())
    }

    /// Batch-reads deployed layer-0 rows while preserving caller order.
    ///
    /// `None` distinguishes a physically absent row from a present encoded
    /// empty list, which corruption recovery uses when validating companion
    /// state. Present rows are decoded before crossing the storage boundary.
    pub(crate) async fn layer0_neighbor_rows(
        &self,
        node_ids: &[NodeId],
    ) -> Result<Vec<Option<Vec<NodeId>>>, HelixDbError> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys = node_ids
            .iter()
            .map(|node_id| {
                self.keyspace
                    .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                        self.keyspace.index_id(),
                        *node_id,
                    )))
            })
            .collect::<Vec<_>>();
        self.read
            .multi_get(&keys)
            .await?
            .into_iter()
            .map(|row| {
                row.map(|value| decode_layer0_neighbors(&value))
                    .transpose()
                    .map_err(Into::into)
            })
            .collect()
    }

    /// Reads and decodes one deployed upper-layer neighbor row.
    ///
    /// Physical absence remains `None`; present bytes are validated before the
    /// graph traversal can observe them. The deployed key and value codecs are
    /// unchanged and remain confined to this storage boundary.
    pub(crate) async fn upper_neighbors(
        &self,
        layer: u16,
        node_id: NodeId,
    ) -> Result<Option<Vec<NodeId>>, HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
                self.keyspace.index_id(),
                layer,
                node_id,
            )));
        self.read
            .get(key)
            .await?
            .map(|value| decode_upper_neighbors(&value))
            .transpose()
            .map_err(Into::into)
    }

    /// Reads one deployed upper-layer vector payload by typed node identity.
    ///
    /// The opaque payload is returned only to the item-decoding/cache boundary;
    /// callers cannot construct or submit its physical key.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) async fn upper_vector_row(
        &self,
        node_id: NodeId,
    ) -> Result<Option<Bytes>, HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::UpperVector(VectorUpperVectorKey::new(
                self.keyspace.index_id(),
                node_id,
            )));
        self.read.get(key).await.map_err(Into::into)
    }

    /// Batch-reads deployed upper-layer payloads while preserving node order.
    ///
    /// An empty input performs no I/O. Key construction stays tenant- and
    /// index-bound even when mutation hydration batches many nodes.
    pub(crate) async fn upper_vector_rows(
        &self,
        node_ids: &[NodeId],
    ) -> Result<Vec<Option<Bytes>>, HelixDbError> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys = node_ids
            .iter()
            .map(|node_id| {
                self.keyspace
                    .key(VectorKey::UpperVector(VectorUpperVectorKey::new(
                        self.keyspace.index_id(),
                        *node_id,
                    )))
            })
            .collect::<Vec<_>>();
        self.read.multi_get(&keys).await.map_err(Into::into)
    }

    /// Batch-reads deployed SimHash rows as closed decoded states.
    ///
    /// Corruption is kept distinct from absence so the owning search or
    /// mutation operation can attach node-specific diagnostic context without
    /// handling raw persisted bytes.
    pub(crate) async fn simhash_rows(
        &self,
        node_ids: &[NodeId],
    ) -> Result<Vec<SimHashRow>, HelixDbError> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys = node_ids
            .iter()
            .map(|node_id| {
                self.keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                    self.keyspace.index_id(),
                    *node_id,
                )))
            })
            .collect::<Vec<_>>();
        Ok(self
            .read
            .multi_get(&keys)
            .await?
            .into_iter()
            .map(|row| match row {
                None => SimHashRow::Missing,
                Some(value) => match decode_simhash(&value) {
                    Ok(bits) => SimHashRow::Present(SimHash::from_bits(bits)),
                    Err(_) => SimHashRow::Corrupt,
                },
            })
            .collect())
    }

    /// Reads at most `max_rows` routing entries from one inclusive order-code window.
    ///
    /// The range is relative to the typed directory prefix, so one object-store
    /// iterator can cover a contiguous SimHash block without fetching vectors.
    /// Every returned marker and key is validated because a directory-capable
    /// generation promises that this row family is complete and well formed.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) async fn simhash_directory_window(
        &self,
        min_order_code: u64,
        max_order_code: u64,
        max_rows: usize,
    ) -> Result<Vec<SimHashDirectoryEntry>, HelixDbError> {
        self.simhash_directory_window_measured(min_order_code, max_order_code, max_rows, usize::MAX)
            .await
            .map(SimHashDirectoryWindow::into_entries)
    }

    /// Reads one directory window and measures every decoded physical byte.
    pub(crate) async fn simhash_directory_window_measured(
        &self,
        min_order_code: u64,
        max_order_code: u64,
        max_rows: usize,
        max_decoded_bytes: usize,
    ) -> Result<SimHashDirectoryWindow, HelixDbError> {
        if max_rows == 0 || max_decoded_bytes == 0 || min_order_code > max_order_code {
            return Ok(SimHashDirectoryWindow {
                entries: Vec::new(),
                decoded_bytes: 0,
            });
        }
        let prefix = self.keyspace.key(VectorKey::SimHashDirectoryPrefix(
            VectorSimHashDirectoryPrefixKey::new(self.keyspace.index_id()),
        ));
        let mut lower = Vec::with_capacity(core::mem::size_of::<u64>() * 2);
        lower.extend_from_slice(&min_order_code.to_be_bytes());
        lower.extend_from_slice(&NodeId::MIN.to_be_bytes());
        let mut upper = Vec::with_capacity(core::mem::size_of::<u64>() * 2);
        upper.extend_from_slice(&max_order_code.to_be_bytes());
        upper.extend_from_slice(&NodeId::MAX.to_be_bytes());
        let mut rows = self
            .read
            .scan_prefix(
                &prefix,
                (
                    Bound::Included(Bytes::from(lower)),
                    Bound::Included(Bytes::from(upper)),
                ),
            )
            .await?;
        let mut entries = Vec::with_capacity(max_rows.min(256));
        let mut decoded_bytes = 0_usize;
        while entries.len() < max_rows {
            let Some(row) = rows.next().await? else {
                break;
            };
            let row_bytes = row.key.len().checked_add(row.value.len()).ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "SimHash directory row-byte measurement overflowed usize".to_string(),
                )
            })?;
            let Some(next_decoded_bytes) = decoded_bytes.checked_add(row_bytes) else {
                return Err(HelixDbError::InvariantViolation(
                    "SimHash directory decoded-byte measurement overflowed usize".to_string(),
                ));
            };
            if next_decoded_bytes > max_decoded_bytes {
                break;
            }
            decoded_bytes = next_decoded_bytes;
            decode_simhash_directory_marker_v1(&row.value)?;
            let logical_key = self.keyspace.strip_physical_key(&row.key)?;
            let VectorKey::SimHashDirectory(key) = VectorKey::parse_from_slice(logical_key)? else {
                return Err(HelixDbError::InvariantViolation(
                    "SimHash directory scan returned another vector row family".to_string(),
                ));
            };
            entries.push(SimHashDirectoryEntry {
                order_code: key.order_code(),
                node_id: key.node_id(),
            });
        }
        Ok(SimHashDirectoryWindow {
            entries,
            decoded_bytes,
        })
    }

    /// Constructs the canonical vector token carried by one validated directory row.
    pub(crate) fn canonical_vector_key_from_directory(
        &self,
        entry: SimHashDirectoryEntry,
    ) -> CanonicalVectorRowKey {
        self.keyspace
            .canonical_vector_row_key(entry.node_id(), entry.order_code())
    }

    /// Reads one canonical vector payload through its opaque bound token.
    ///
    /// A token from another namespace is an invariant violation and is rejected
    /// before storage is accessed. Missing rows remain `None` so search and
    /// corruption-recovery policy stays with the owning caller.
    pub(crate) async fn canonical_vector_row(
        &self,
        key: &CanonicalVectorRowKey,
    ) -> Result<Option<Bytes>, HelixDbError> {
        if key.scope != self.keyspace.scope || key.index_id != self.keyspace.index_id {
            return Err(HelixDbError::InvariantViolation(
                "canonical vector row token belongs to another keyspace".to_string(),
            ));
        }
        self.read.get(&key.physical_key).await.map_err(Into::into)
    }

    /// Batch-reads canonical vector payloads while preserving token order.
    ///
    /// The method validates the complete batch before issuing I/O, preventing a
    /// mixed-index request from partially reading data. Callers may first sort
    /// with [`CanonicalVectorRowKey::physical_order`] to improve storage locality.
    pub(crate) async fn canonical_vector_rows(
        &self,
        keys: &[CanonicalVectorRowKey],
    ) -> Result<Vec<Option<Bytes>>, HelixDbError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys
            .iter()
            .any(|key| key.scope != self.keyspace.scope || key.index_id != self.keyspace.index_id)
        {
            return Err(HelixDbError::InvariantViolation(
                "canonical vector row batch contains another keyspace".to_string(),
            ));
        }
        let physical_keys = keys
            .iter()
            .map(|key| key.physical_key.clone())
            .collect::<Vec<_>>();
        self.read
            .multi_get(&physical_keys)
            .await
            .map_err(Into::into)
    }

    /// Reads one entry-candidate node-layer row as a closed typed state.
    pub(crate) async fn entry_candidate_layer(
        &self,
        node_id: NodeId,
    ) -> Result<EntryCandidateLayerRow, HelixDbError> {
        let key = self.keyspace.key(VectorKey::EntryCandidateNode(
            VectorEntryCandidateNodeKey::new(self.keyspace.index_id(), node_id),
        ));
        let Some(value) = self.read.get(key).await? else {
            return Ok(EntryCandidateLayerRow::Missing);
        };
        Ok(match decode_entry_candidate_layer(&value) {
            Ok(layer) => EntryCandidateLayerRow::Present(layer),
            Err(_) => EntryCandidateLayerRow::Corrupt,
        })
    }

    /// Starts the highest-layer-first sorted entry-candidate scan.
    pub(crate) async fn entry_candidates(&self) -> Result<EntryCandidateScan<'_>, HelixDbError> {
        let prefix = self.keyspace.key(VectorKey::EntryCandidatePrefix(
            VectorEntryCandidatePrefixKey::new(self.keyspace.index_id()),
        ));
        Ok(EntryCandidateScan {
            rows: self.read.scan_prefix(prefix, ..).await?,
            keyspace: self.keyspace,
        })
    }

    /// Loads every reverse locator targeting one node using one prefix scan.
    ///
    /// Parseable rows are grouped into sorted, deduplicated source lists.
    /// Every scanned key is retained privately for the deletion path so cleanup
    /// does not issue a second scan and preserves tolerant malformed-row removal.
    pub(crate) async fn reverse_sources_for_target(
        &self,
        target_node_id: NodeId,
    ) -> Result<ReverseSourcesForTarget, HelixDbError> {
        let prefix = self.keyspace.key(VectorKey::ReverseEdgePrefix(
            VectorReverseEdgePrefixKey::new(self.keyspace.index_id(), target_node_id),
        ));
        let mut rows = self.read.scan_prefix(prefix, ..).await?;
        let mut sources_by_layer = BTreeMap::<u16, BTreeSet<NodeId>>::new();
        let mut locator_keys = Vec::new();

        while let Some(row) = rows.next().await? {
            locator_keys.push(row.key.clone());
            let logical_key = self.keyspace.strip_physical_key(&row.key)?;
            let Ok(VectorKey::ReverseEdge(locator)) = VectorKey::parse_from_slice(logical_key)
            else {
                continue;
            };
            if locator.target_node_id() != target_node_id {
                continue;
            }
            sources_by_layer
                .entry(locator.layer())
                .or_default()
                .insert(locator.source_node_id());
        }

        Ok(ReverseSourcesForTarget {
            keyspace: self.keyspace.clone(),
            sources_by_layer: sources_by_layer
                .into_iter()
                .map(|(layer, sources)| (layer, sources.into_iter().collect()))
                .collect(),
            locator_keys,
        })
    }
}

/// Typed metadata writes in one measured vector transaction.
///
/// This wrapper preserves the transaction recorder's last-write-wins accounting
/// while keeping metadata keys and deployed value encoding inside storage.
pub(crate) struct VectorWriteRows<'a, 'txn> {
    write: &'a MeasuredVectorTransaction<'txn>,
    keyspace: &'a VectorRowKeyspace,
}

impl<'a, 'txn> VectorWriteRows<'a, 'txn> {
    /// Binds measured writes to one physical vector namespace.
    pub(crate) const fn new(
        write: &'a MeasuredVectorTransaction<'txn>,
        keyspace: &'a VectorRowKeyspace,
    ) -> Self {
        Self { write, keyspace }
    }

    /// Stages deletion of one token issued by this exact physical keyspace.
    pub(crate) fn delete_cleanup_row(&self, row: &VectorCleanupRow) -> Result<(), HelixDbError> {
        if &row.keyspace != self.keyspace {
            return Err(HelixDbError::InvariantViolation(
                "vector cleanup row belongs to another physical keyspace".to_string(),
            ));
        }
        self.write.delete(&row.physical_key)?;
        Ok(())
    }

    /// Returns whether the deployed metadata row already exists.
    pub(crate) async fn metadata_exists(&self) -> Result<bool, HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                self.keyspace.index_id(),
            )));
        Ok(self.write.get(key).await?.is_some())
    }

    /// Batch-reads layer-0 rows through the transaction's write view.
    pub(crate) async fn layer0_neighbor_rows(
        &self,
        node_ids: &[NodeId],
    ) -> Result<Vec<Option<Vec<NodeId>>>, HelixDbError> {
        VectorRows::new(self.write, self.keyspace)
            .layer0_neighbor_rows(node_ids)
            .await
    }

    /// Validates and stages the deployed metadata bytes unchanged.
    pub(crate) fn put_metadata(&self, metadata: &VectorIndexMetadata) -> Result<(), HelixDbError> {
        metadata.validated_state()?;
        let key = self
            .keyspace
            .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                self.keyspace.index_id(),
            )));
        self.write.put(key, encode_metadata(metadata))?;
        Ok(())
    }

    /// Encodes and stages one deployed layer-0 neighbor row unchanged.
    pub(crate) fn put_layer0_neighbors(
        &self,
        node_id: NodeId,
        neighbors: &[NodeId],
    ) -> Result<(), HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                self.keyspace.index_id(),
                node_id,
            )));
        self.write.put(key, encode_layer0_neighbors(neighbors))?;
        Ok(())
    }

    /// Deletes one deployed layer-0 neighbor row by typed node identity.
    pub(crate) fn delete_layer0_neighbors(&self, node_id: NodeId) -> Result<(), HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                self.keyspace.index_id(),
                node_id,
            )));
        self.write.delete(key)?;
        Ok(())
    }

    /// Stages deletion of one canonical payload in the measured transaction.
    ///
    /// Namespace validation happens before mutation staging, so a token cannot
    /// delete another index's row. Durability remains owned by the caller that
    /// commits the surrounding transaction.
    pub(crate) fn delete_canonical_vector(
        &self,
        key: &CanonicalVectorRowKey,
    ) -> Result<(), HelixDbError> {
        if key.scope != self.keyspace.scope || key.index_id != self.keyspace.index_id {
            return Err(HelixDbError::InvariantViolation(
                "canonical vector row token belongs to another write keyspace".to_string(),
            ));
        }
        self.write.delete(&key.physical_key)?;
        Ok(())
    }

    /// Stages one canonical payload in the measured transaction.
    ///
    /// The token proves the deployed key was constructed by this storage
    /// boundary; this method changes neither the key nor value codec. Durability
    /// remains owned by the caller that commits the surrounding transaction.
    pub(crate) fn put_canonical_vector(
        &self,
        key: &CanonicalVectorRowKey,
        value: Bytes,
    ) -> Result<(), HelixDbError> {
        if key.scope != self.keyspace.scope || key.index_id != self.keyspace.index_id {
            return Err(HelixDbError::InvariantViolation(
                "canonical vector row token belongs to another write keyspace".to_string(),
            ));
        }
        self.write.put_bytes(key.physical_key.clone(), value)?;
        Ok(())
    }

    /// Stages one deployed upper-layer payload used by hot graph traversal.
    ///
    /// The value is the unchanged encoded `Item`; only key construction moves
    /// behind this typed boundary. Durability remains owned by the caller that
    /// commits the surrounding measured transaction.
    pub(crate) fn put_upper_vector(
        &self,
        node_id: NodeId,
        value: Bytes,
    ) -> Result<(), HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::UpperVector(VectorUpperVectorKey::new(
                self.keyspace.index_id(),
                node_id,
            )));
        self.write.put_bytes(key, value)?;
        Ok(())
    }

    /// Encodes and stages one deployed upper-layer neighbor row unchanged.
    pub(crate) fn put_upper_neighbors(
        &self,
        layer: u16,
        node_id: NodeId,
        neighbors: &[NodeId],
    ) -> Result<(), HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
                self.keyspace.index_id(),
                layer,
                node_id,
            )));
        self.write.put(key, encode_upper_neighbors(neighbors)?)?;
        Ok(())
    }

    /// Deletes one deployed upper-layer neighbor row by typed layer and node.
    pub(crate) fn delete_upper_neighbors(
        &self,
        layer: u16,
        node_id: NodeId,
    ) -> Result<(), HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
                self.keyspace.index_id(),
                layer,
                node_id,
            )));
        self.write.delete(key)?;
        Ok(())
    }

    /// Deletes one deployed upper-vector hot row by typed node identity.
    pub(crate) fn delete_upper_vector(&self, node_id: NodeId) -> Result<(), HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::UpperVector(VectorUpperVectorKey::new(
                self.keyspace.index_id(),
                node_id,
            )));
        self.write.delete(key)?;
        Ok(())
    }

    /// Deletes one deployed SimHash row by typed node identity.
    pub(crate) fn delete_simhash(&self, node_id: NodeId) -> Result<(), HelixDbError> {
        let key = self.keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
            self.keyspace.index_id(),
            node_id,
        )));
        self.write.delete(key)?;
        Ok(())
    }

    /// Stages the versioned routing marker paired with one canonical vector row.
    pub(crate) fn put_simhash_directory_entry(
        &self,
        canonical_key: &CanonicalVectorRowKey,
    ) -> Result<(), HelixDbError> {
        if canonical_key.scope != self.keyspace.scope
            || canonical_key.index_id != self.keyspace.index_id
        {
            return Err(HelixDbError::InvariantViolation(
                "canonical vector row token belongs to another directory keyspace".to_string(),
            ));
        }
        let key = self
            .keyspace
            .key(VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
                self.keyspace.index_id(),
                canonical_key.order_code,
                canonical_key.node_id,
            )));
        self.write.put(key, encode_simhash_directory_marker_v1())?;
        Ok(())
    }

    /// Deletes the exact routing marker paired with one canonical vector row.
    pub(crate) fn delete_simhash_directory_entry(
        &self,
        canonical_key: &CanonicalVectorRowKey,
    ) -> Result<(), HelixDbError> {
        if canonical_key.scope != self.keyspace.scope
            || canonical_key.index_id != self.keyspace.index_id
        {
            return Err(HelixDbError::InvariantViolation(
                "canonical vector row token belongs to another directory keyspace".to_string(),
            ));
        }
        let key = self
            .keyspace
            .key(VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
                self.keyspace.index_id(),
                canonical_key.order_code,
                canonical_key.node_id,
            )));
        self.write.delete(key)?;
        Ok(())
    }

    /// Reads one writable entry-candidate node-layer row as a closed state.
    pub(crate) async fn entry_candidate_layer(
        &self,
        node_id: NodeId,
    ) -> Result<EntryCandidateLayerRow, HelixDbError> {
        VectorRows::new(self.write, self.keyspace)
            .entry_candidate_layer(node_id)
            .await
    }

    /// Starts the writable highest-layer-first candidate scan.
    pub(crate) async fn entry_candidates(&self) -> Result<EntryCandidateScan<'_>, HelixDbError> {
        let prefix = self.keyspace.key(VectorKey::EntryCandidatePrefix(
            VectorEntryCandidatePrefixKey::new(self.keyspace.index_id()),
        ));
        Ok(EntryCandidateScan {
            rows: self.write.scan_prefix(prefix, ..).await?,
            keyspace: self.keyspace,
        })
    }

    /// Loads typed reverse sources through this transaction's read view.
    pub(crate) async fn reverse_sources_for_target(
        &self,
        target_node_id: NodeId,
    ) -> Result<ReverseSourcesForTarget, HelixDbError> {
        VectorRows::new(self.write, self.keyspace)
            .reverse_sources_for_target(target_node_id)
            .await
    }

    /// Stages both deployed rows that represent one entry candidate.
    pub(crate) fn put_entry_candidate(
        &self,
        node_id: NodeId,
        layer: u16,
    ) -> Result<(), HelixDbError> {
        let sorted_key = self.keyspace.key(VectorKey::EntryCandidateSorted(
            VectorEntryCandidateKey::new(self.keyspace.index_id(), layer, node_id),
        ));
        self.write.put(sorted_key, encode_empty_marker())?;

        let node_key = self.keyspace.key(VectorKey::EntryCandidateNode(
            VectorEntryCandidateNodeKey::new(self.keyspace.index_id(), node_id),
        ));
        self.write
            .put(node_key, encode_entry_candidate_layer(layer))?;
        Ok(())
    }

    /// Deletes one known sorted candidate row by its typed identity.
    pub(crate) fn delete_entry_candidate_sorted(
        &self,
        node_id: NodeId,
        layer: u16,
    ) -> Result<(), HelixDbError> {
        let key = self.keyspace.key(VectorKey::EntryCandidateSorted(
            VectorEntryCandidateKey::new(self.keyspace.index_id(), layer, node_id),
        ));
        self.write.delete(key)?;
        Ok(())
    }

    /// Deletes the node-to-layer row for one candidate.
    pub(crate) fn delete_entry_candidate_node(&self, node_id: NodeId) -> Result<(), HelixDbError> {
        let key = self.keyspace.key(VectorKey::EntryCandidateNode(
            VectorEntryCandidateNodeKey::new(self.keyspace.index_id(), node_id),
        ));
        self.write.delete(key)?;
        Ok(())
    }

    /// Deletes a row yielded by this namespace's typed candidate scan.
    pub(crate) fn delete_scanned_entry_candidate(
        &self,
        candidate: &EntryCandidateRow<'_>,
    ) -> Result<(), HelixDbError> {
        if candidate.keyspace != self.keyspace {
            return Err(HelixDbError::InvariantViolation(
                "entry-candidate scan token belongs to another vector keyspace".to_string(),
            ));
        }
        self.write.delete(&candidate.physical_key)?;
        Ok(())
    }

    /// Stages one deployed reverse locator marker.
    pub(crate) fn put_reverse_locator(
        &self,
        target_node_id: NodeId,
        layer: u16,
        source_node_id: NodeId,
    ) -> Result<(), HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::ReverseEdge(VectorReverseEdgeKey::new(
                self.keyspace.index_id(),
                target_node_id,
                layer,
                source_node_id,
            )));
        self.write.put(key, encode_empty_marker())?;
        Ok(())
    }

    /// Deletes one reverse locator by its typed graph identity.
    pub(crate) fn delete_reverse_locator(
        &self,
        target_node_id: NodeId,
        layer: u16,
        source_node_id: NodeId,
    ) -> Result<(), HelixDbError> {
        let key = self
            .keyspace
            .key(VectorKey::ReverseEdge(VectorReverseEdgeKey::new(
                self.keyspace.index_id(),
                target_node_id,
                layer,
                source_node_id,
            )));
        self.write.delete(key)?;
        Ok(())
    }

    /// Deletes every locator token captured by a single target scan.
    pub(crate) fn delete_reverse_sources(
        &self,
        sources: &ReverseSourcesForTarget,
    ) -> Result<(), HelixDbError> {
        if &sources.keyspace != self.keyspace {
            return Err(HelixDbError::InvariantViolation(
                "reverse-source cleanup belongs to another vector keyspace".to_string(),
            ));
        }
        for key in &sources.locator_keys {
            self.write.delete(key)?;
        }
        Ok(())
    }

    /// Deletes every current-format row family owned by this vector namespace.
    ///
    /// The exhaustive lane list covers core, hot, and layer-0 keyspaces even
    /// when metadata is absent. Adding a new `VectorStorageLane` therefore
    /// requires updating its closed `ALL` set before cleanup can pass review.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) async fn delete_all(&self) -> Result<(), HelixDbError> {
        for lane in VectorStorageLane::ALL {
            let prefix = self.keyspace.key(lane.prefix_key(self.keyspace.index_id()));
            let mut rows = self.write.scan_prefix(prefix, ..).await?;
            while let Some(row) = rows.next().await? {
                self.write.delete(&row.key)?;
            }
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "production-coverage"))]
#[path = "../../../tests/production_support/vector/storage.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::config::VectorIndexDefinition;
    use crate::encoding::keys::tenant::TenantId;
    use crate::encoding::v1::keys::vectors::VectorIndexMetadataKey;
    use crate::encoding::v1::values::vectors::simhash::encode_simhash;
    use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
    use crate::search::vector::{distance, Item, VectorDistanceMetric};

    fn legacy_definition() -> ValidatedVectorIndexDefinition {
        let definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Cosine,
        )
        .unwrap()
        .try_into()
        .unwrap();
        let ValidatedDynamicIndexDefinition::Vector(definition) = definition else {
            unreachable!("vector definition validates as vector")
        };
        definition
    }

    async fn database(label: &str) -> slatedb::Db {
        slatedb::Db::open(
            format!("vector-storage-{label}-{}", uuid::Uuid::new_v4()),
            Arc::new(InMemory::new()),
        )
        .await
        .expect("vector storage test database opens")
    }

    fn legacy_metadata_bytes(
        definition: &ValidatedVectorIndexDefinition,
        physical_name: &str,
        entry_point: Option<NodeId>,
    ) -> Bytes {
        let mut metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
            definition,
            physical_name,
        ));
        metadata.entry_point = entry_point;
        Bytes::copy_from_slice(
            &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                &metadata,
            ),
        )
    }

    fn current_metadata_bytes(
        definition: &ValidatedVectorIndexDefinition,
        physical_name: &str,
        entry_point: Option<NodeId>,
    ) -> Bytes {
        let mut metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
            definition,
            physical_name,
        ));
        metadata.entry_point = entry_point;
        Bytes::copy_from_slice(&encode_metadata(&metadata))
    }

    #[test]
    fn legacy_validation_dispatches_every_physical_value_codec() {
        let physical_name = "legacy-validation-codecs";
        let index_id = index_id_from_name(physical_name);
        let definition = legacy_definition();
        let config = VectorIndexConfig::from_v2_definition(&definition, physical_name);
        let dimension = VectorDimension::try_new(3).unwrap();
        let metadata = VectorIndexMetadata::new(config.clone());
        let item =
            crate::search::vector::encode_item(&Item::<distance::Cosine>::new(vec![1.0, 0.0, 0.0]));
        let rows = [
            (
                VectorKey::IndexMetadata(VectorIndexMetadataKey::new(index_id)),
                Bytes::copy_from_slice(
                    &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            ),
            (
                VectorKey::TxnGuard(
                    crate::encoding::v1::keys::vectors::VectorTxnGuardKey::new(index_id),
                ),
                crate::encoding::v1::values::vectors::markers::encode_active_txn_guard(),
            ),
            (
                VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(index_id, 1)),
                encode_layer0_neighbors(&[2, 3]),
            ),
            (
                VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 2, 1)),
                encode_upper_neighbors(&[2, 3]).unwrap(),
            ),
            (
                VectorKey::SimHash(VectorSimHashKey::new(index_id, 1)),
                Bytes::copy_from_slice(&encode_simhash(17)),
            ),
            (
                VectorKey::UpperVector(VectorUpperVectorKey::new(index_id, 1)),
                item.clone(),
            ),
            (
                VectorKey::Vector(VectorItemKey::new(index_id, 17, 1)),
                item,
            ),
            (
                VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(index_id, 2, 1)),
                encode_empty_marker(),
            ),
            (
                VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(index_id, 1)),
                encode_entry_candidate_layer(2),
            ),
            (
                VectorKey::ReverseEdge(VectorReverseEdgeKey::new(index_id, 2, 1, 3)),
                encode_empty_marker(),
            ),
        ];

        for (key, value) in rows {
            assert!(
                validate_legacy_row::<distance::Cosine>(&key, &value, &config, dimension).is_ok(),
                "valid {key:?} value must decode"
            );
            assert!(
                validate_legacy_row::<distance::Cosine>(&key, b"malformed", &config, dimension,)
                    .is_err(),
                "malformed {key:?} value must fail closed"
            );
        }

        for prefix in [
            VectorStorageLane::Core.prefix_key(index_id),
            VectorStorageLane::Hot.prefix_key(index_id),
            VectorStorageLane::Layer0.prefix_key(index_id),
            VectorKey::EntryCandidatePrefix(VectorEntryCandidatePrefixKey::new(index_id)),
            VectorKey::ReverseEdgePrefix(VectorReverseEdgePrefixKey::new(index_id, 1)),
        ] {
            assert!(
                validate_legacy_row::<distance::Cosine>(&prefix, &[], &config, dimension,).is_err(),
                "persisted prefix {prefix:?} must fail closed"
            );
        }
    }

    #[test]
    fn legacy_validation_rejects_zero_norm_cosine_payloads_only() {
        let physical_name = "legacy-validation-zero-norm";
        let index_id = index_id_from_name(physical_name);
        let definition = legacy_definition();
        let config = VectorIndexConfig::from_v2_definition(&definition, physical_name);
        let dimension = VectorDimension::try_new(3).unwrap();
        let key = VectorKey::Vector(VectorItemKey::new(index_id, 0, 1));
        let cosine =
            crate::search::vector::encode_item(&Item::<distance::Cosine>::new(vec![0.0, 0.0, 0.0]));
        let euclidean =
            crate::search::vector::encode_item(&Item::<distance::Euclidean>::new(vec![
                0.0, 0.0, 0.0,
            ]));

        assert_eq!(
            validate_legacy_row::<distance::Cosine>(&key, &cosine, &config, dimension),
            Err("legacy cosine vector payload has zero norm".to_string())
        );
        assert!(
            validate_legacy_row::<distance::Euclidean>(&key, &euclidean, &config, dimension)
                .is_ok(),
            "zero is valid for non-cosine metrics"
        );
    }

    #[tokio::test]
    async fn legacy_validation_pages_by_rows_bytes_and_exact_lane_cursor() {
        let db = slatedb::Db::open("legacy-validation-pages", Arc::new(InMemory::new()))
            .await
            .unwrap();
        let physical_name = "legacy-validation-pages";
        let definition = legacy_definition();
        let keyspace =
            VectorRowKeyspace::from_legacy_name(physical_name.into(), DataScope::LegacyUnscoped);
        let metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
            &definition,
            physical_name,
        ));
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(
                keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                    keyspace.index_id(),
                ))),
                Bytes::copy_from_slice(
                    &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            )
            .unwrap();
        transaction
            .put(
                keyspace.key(VectorKey::TxnGuard(
                    crate::encoding::v1::keys::vectors::VectorTxnGuardKey::new(keyspace.index_id()),
                )),
                crate::encoding::v1::values::vectors::markers::encode_active_txn_guard(),
            )
            .unwrap();
        transaction.commit().await.unwrap();

        let rows = VectorRows::new(&db, &keyspace);
        let first = rows
            .validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Core,
                None,
                &definition,
                LegacyVectorValidationMode::ReadOnly,
                1,
                u64::MAX,
            )
            .await
            .unwrap();
        let LegacyVectorValidationOutcome::Valid {
            last_key: Some(first_cursor),
            rows: 1,
            exhausted: false,
            ..
        } = first
        else {
            panic!("first row-limited page must retain its exact cursor")
        };
        let second = rows
            .validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Core,
                Some(&first_cursor),
                &definition,
                LegacyVectorValidationMode::ReadOnly,
                2,
                u64::MAX,
            )
            .await
            .unwrap();
        assert!(matches!(
            second,
            LegacyVectorValidationOutcome::Valid {
                rows: 1,
                exhausted: true,
                ..
            }
        ));
        assert!(matches!(
            rows.validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Core,
                None,
                &definition,
                LegacyVectorValidationMode::ReadOnly,
                usize::MAX,
                1,
            )
            .await
            .unwrap(),
            LegacyVectorValidationOutcome::Oversized { limit: 1, .. }
        ));
        let foreign_cursor = keyspace.key(VectorStorageLane::Hot.prefix_key(keyspace.index_id()));
        assert!(matches!(
            rows.validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Core,
                Some(&foreign_cursor),
                &definition,
                LegacyVectorValidationMode::ReadOnly,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            LegacyVectorValidationOutcome::Invalid { .. }
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn legacy_layer_zero_emits_exact_bounded_directory_tokens() {
        let db = slatedb::Db::open(
            "legacy-validation-directory-tokens",
            Arc::new(InMemory::new()),
        )
        .await
        .unwrap();
        let physical_name = "legacy-validation-directory-tokens";
        let definition = legacy_definition();
        let keyspace =
            VectorRowKeyspace::from_legacy_name(physical_name.into(), DataScope::LegacyUnscoped);
        let metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
            &definition,
            physical_name,
        ));
        let order_code = 17;
        let node_id = 23;
        let vector_key = keyspace.key(VectorKey::Vector(VectorItemKey::new(
            keyspace.index_id(),
            order_code,
            node_id,
        )));
        let vector_value =
            crate::search::vector::encode_item(&Item::<distance::Cosine>::new(vec![1.0, 0.0, 0.0]));
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(
                keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                    keyspace.index_id(),
                ))),
                Bytes::copy_from_slice(
                    &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            )
            .unwrap();
        transaction.put(&vector_key, &vector_value).unwrap();
        transaction.commit().await.unwrap();

        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let rows = VectorRows::new(&transaction, &keyspace);
        assert!(matches!(
            rows.validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Layer0,
                None,
                &definition,
                LegacyVectorValidationMode::BackfillSimHashDirectory {
                    max_output_operations: NonZeroU64::MIN,
                    max_output_bytes: NonZeroU64::MIN,
                },
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            LegacyVectorValidationOutcome::Oversized { limit: 1, .. }
        ));
        let outcome = rows
            .validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Layer0,
                None,
                &definition,
                LegacyVectorValidationMode::BackfillSimHashDirectory {
                    max_output_operations: NonZeroU64::MIN,
                    max_output_bytes: NonZeroU64::new(u64::MAX).unwrap(),
                },
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap();
        let LegacyVectorValidationOutcome::Valid {
            directory_entries,
            predicted_directory_writes,
            rows: 1,
            exhausted: true,
            ..
        } = outcome
        else {
            panic!("one canonical vector must emit one complete marker token");
        };
        assert_eq!(directory_entries.len(), 1);
        assert_eq!(predicted_directory_writes.operations(), 1);
        let expected_marker_key = keyspace.key(VectorKey::SimHashDirectory(
            VectorSimHashDirectoryKey::new(keyspace.index_id(), order_code, node_id),
        ));
        assert_eq!(
            predicted_directory_writes.encoded_bytes(),
            u64::try_from(expected_marker_key.len() + encode_simhash_directory_marker_v1().len())
                .unwrap()
        );
        let recorder = super::super::VectorWriteRecorder::new();
        let measured = recorder.bind(&transaction);
        VectorWriteRows::new(&measured, &keyspace)
            .put_simhash_directory_entry(&directory_entries[0])
            .unwrap();
        assert_eq!(measured.measurement().unwrap(), predicted_directory_writes);
        transaction.commit().await.unwrap();
        assert_eq!(
            db.get(expected_marker_key).await.unwrap().unwrap(),
            Bytes::copy_from_slice(&encode_simhash_directory_marker_v1())
        );
        db.close().await.unwrap();
    }

    #[test]
    fn keyspace_preserves_legacy_bytes_and_rejects_cross_tenant_scan_keys() {
        let physical_name = "typed-row-keyspace";
        let index_id = index_id_from_name(physical_name);
        let logical_key = VectorKey::IndexMetadata(VectorIndexMetadataKey::new(index_id));
        let legacy = VectorRowKeyspace::new(physical_name.to_string(), DataScope::LegacyUnscoped);
        assert_eq!(legacy.physical_name(), physical_name);
        assert_eq!(legacy.index_id(), index_id);
        assert_eq!(legacy.key(logical_key), logical_key.to_bytes());

        let first_scope = DataScope::Tenant(TenantId::from_u128(1));
        let second_scope = DataScope::Tenant(TenantId::from_u128(2));
        let first = VectorRowKeyspace::new(physical_name.to_string(), first_scope);
        let second = VectorRowKeyspace::new(physical_name.to_string(), second_scope);
        let first_key = first.key(logical_key);
        let second_key = second.key(logical_key);

        assert_eq!(
            first.strip_physical_key(&first_key).unwrap(),
            logical_key.to_bytes()
        );
        assert!(first.strip_physical_key(&second_key).is_err());
    }

    #[test]
    fn canonical_keyspace_uses_allocated_id_without_name_hashing() {
        let physical_index_id = VectorPhysicalIndexId::new(42).unwrap();
        let keyspace = VectorRowKeyspace::from_allocated(
            "diagnostic-name-is-not-row-identity".to_string(),
            physical_index_id,
            DataScope::LegacyUnscoped,
        );
        assert_eq!(keyspace.index_id(), 42);
        assert_ne!(
            keyspace.index_id(),
            index_id_from_name(keyspace.physical_name())
        );
        assert_eq!(
            keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(42))),
            VectorKey::IndexMetadata(VectorIndexMetadataKey::new(42)).to_bytes()
        );
    }

    #[test]
    fn canonical_vector_tokens_preserve_deployed_bytes_and_physical_order() {
        let physical_name = "typed-row-keyspace";
        let keyspace = VectorRowKeyspace::new(physical_name.to_string(), DataScope::LegacyUnscoped);
        let first_node_id = 7;
        let first_order_code = 11;
        let second_node_id = 3;
        let second_order_code = 12;

        let first = keyspace.canonical_vector_row_key(first_node_id, first_order_code);
        let second = keyspace.canonical_vector_row_key(second_node_id, second_order_code);
        let deployed_first = keyspace.key(VectorKey::Vector(VectorItemKey::new(
            keyspace.index_id(),
            first_order_code,
            first_node_id,
        )));

        assert_eq!(first.physical_key, deployed_first);
        assert_eq!(
            first.physical_order(&second),
            first.physical_key.cmp(&second.physical_key)
        );
        assert_eq!(first.physical_order(&second), std::cmp::Ordering::Less);
    }

    /// Proves typed reads preserve absence and reject malformed row state.
    #[tokio::test]
    async fn typed_hot_rows_decode_without_exposing_physical_keys() {
        let db = slatedb::Db::open("typed-hot-vector-rows", Arc::new(InMemory::new()))
            .await
            .unwrap();
        let keyspace =
            VectorRowKeyspace::new("typed-hot-vector-rows".into(), DataScope::LegacyUnscoped);
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let valid_simhash = SimHash::from_bits(0x1234_5678_9abc_def0);
        txn.put(
            keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                keyspace.index_id(),
                2,
            ))),
            encode_simhash(valid_simhash.bits()),
        )
        .unwrap();
        txn.put(
            keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                keyspace.index_id(),
                3,
            ))),
            Bytes::from_static(b"invalid"),
        )
        .unwrap();
        txn.put(
            keyspace.key(VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
                keyspace.index_id(),
                2,
                7,
            ))),
            encode_upper_neighbors(&[4, 9]).unwrap(),
        )
        .unwrap();
        txn.put(
            keyspace.key(VectorKey::UpperVector(VectorUpperVectorKey::new(
                keyspace.index_id(),
                7,
            ))),
            Bytes::from_static(b"item-payload"),
        )
        .unwrap();

        let rows = VectorRows::new(&txn, &keyspace);
        assert_eq!(
            rows.simhash_rows(&[1, 2, 3]).await.unwrap(),
            vec![
                SimHashRow::Missing,
                SimHashRow::Present(valid_simhash),
                SimHashRow::Corrupt,
            ]
        );
        assert_eq!(rows.upper_neighbors(2, 7).await.unwrap(), Some(vec![4, 9]));
        assert_eq!(rows.upper_neighbors(2, 8).await.unwrap(), None);
        assert_eq!(
            rows.upper_vector_rows(&[7, 8]).await.unwrap(),
            vec![Some(Bytes::from_static(b"item-payload")), None]
        );
        assert_eq!(
            rows.upper_vector_row(7).await.unwrap(),
            Some(Bytes::from_static(b"item-payload"))
        );
        txn.rollback();
    }

    #[tokio::test]
    async fn legacy_migration_read_classifies_every_persisted_row_state() {
        let db = database("migration-read").await;
        let physical_name = "legacy-migration-read";
        let definition = legacy_definition();
        let keyspace =
            VectorRowKeyspace::from_legacy_name(physical_name.into(), DataScope::LegacyUnscoped);
        let rows = VectorRows::new(&db, &keyspace);
        let metadata_key = keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            keyspace.index_id(),
        )));

        assert!(matches!(
            rows.legacy_vector_for_migration::<distance::Cosine>(1, &definition)
                .await
                .unwrap(),
            LegacyVectorMigrationRead::Absent { input_bytes } if input_bytes > 0
        ));
        db.put(
            &metadata_key,
            legacy_metadata_bytes(&definition, physical_name, None),
        )
        .await
        .unwrap();
        assert!(matches!(
            rows.legacy_vector_for_migration::<distance::Cosine>(1, &definition)
                .await
                .unwrap(),
            LegacyVectorMigrationRead::Absent { input_bytes } if input_bytes > 0
        ));

        let orphan_layer0 = keyspace.key(VectorKey::Layer0Neighbors(
            VectorLayer0NeighborsKey::new(keyspace.index_id(), 2),
        ));
        db.put(&orphan_layer0, encode_layer0_neighbors(&[]))
            .await
            .unwrap();
        assert!(matches!(
            rows.legacy_vector_for_migration::<distance::Cosine>(2, &definition)
                .await,
            Err(HelixDbError::InvariantViolation(_))
        ));

        let simhash_bits = 17;
        for entity_id in [3, 4] {
            db.put(
                keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                    keyspace.index_id(),
                    entity_id,
                ))),
                encode_simhash(simhash_bits),
            )
            .await
            .unwrap();
        }
        assert!(matches!(
            rows.legacy_vector_for_migration::<distance::Cosine>(3, &definition)
                .await
                .unwrap(),
            LegacyVectorMigrationRead::Absent { input_bytes } if input_bytes > 0
        ));

        let vector = vec![1.0, 0.0, 0.0];
        db.put(
            keyspace.key(VectorKey::Vector(VectorItemKey::new(
                keyspace.index_id(),
                super::super::simhash::order_code_from_simhash_bits(simhash_bits),
                4,
            ))),
            crate::search::vector::encode_item(&Item::<distance::Cosine>::new(vector.clone())),
        )
        .await
        .unwrap();
        assert!(matches!(
            rows.legacy_vector_for_migration::<distance::Cosine>(4, &definition)
                .await
                .unwrap(),
            LegacyVectorMigrationRead::Present { vector: observed, input_bytes }
                if observed == vector && input_bytes > 0
        ));

        db.put(
            keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                keyspace.index_id(),
                5,
            ))),
            Bytes::from_static(b"malformed"),
        )
        .await
        .unwrap();
        assert!(rows
            .legacy_vector_for_migration::<distance::Cosine>(5, &definition)
            .await
            .is_err());

        db.put(
            &metadata_key,
            legacy_metadata_bytes(&definition, "different-physical-name", None),
        )
        .await
        .unwrap();
        assert!(matches!(
            rows.legacy_vector_for_migration::<distance::Cosine>(1, &definition)
                .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        db.put(&metadata_key, Bytes::from_static(b"malformed"))
            .await
            .unwrap();
        assert!(rows
            .legacy_vector_for_migration::<distance::Cosine>(1, &definition)
            .await
            .is_err());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn simhash_directory_validation_covers_preflight_and_terminal_proofs() {
        let db = database("directory-validation").await;
        let physical_name = "directory-validation";
        let definition = legacy_definition();
        let keyspace =
            VectorRowKeyspace::from_legacy_name(physical_name.into(), DataScope::LegacyUnscoped);
        let rows = VectorRows::new(&db, &keyspace);
        let entity_id = 7;
        let simhash_bits = 17;
        let order_code = super::super::simhash::order_code_from_simhash_bits(simhash_bits);
        let marker_key = keyspace.key(VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
            keyspace.index_id(),
            order_code,
            entity_id,
        )));
        let canonical_key = keyspace.key(VectorKey::Vector(VectorItemKey::new(
            keyspace.index_id(),
            order_code,
            entity_id,
        )));
        let metadata_key = keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            keyspace.index_id(),
        )));
        let simhash_key = keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
            keyspace.index_id(),
            entity_id,
        )));
        let valid_marker = encode_simhash_directory_marker_v1();
        let valid_vector =
            crate::search::vector::encode_item(&Item::<distance::Cosine>::new(vec![1.0, 0.0, 0.0]));

        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Valid {
                markers: 0,
                exhausted: true,
                ..
            }
        ));
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                Some(b"foreign-cursor"),
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Invalid { .. }
        ));

        db.put(&marker_key, valid_marker).await.unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Invalid { .. }
        ));
        db.put(&canonical_key, &valid_vector).await.unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                usize::MAX,
                1,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Oversized { limit: 1, .. }
        ));
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                1,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Valid {
                markers: 1,
                exhausted: false,
                ..
            }
        ));
        db.put(&canonical_key, Bytes::from_static(b"malformed"))
            .await
            .unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Invalid { .. }
        ));
        db.put(&canonical_key, valid_vector).await.unwrap();
        db.put(&marker_key, Bytes::from_static(b"malformed"))
            .await
            .unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Invalid { .. }
        ));
        db.put(&marker_key, encode_simhash_directory_marker_v1())
            .await
            .unwrap();

        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Invalid { .. }
        ));
        db.put(&metadata_key, Bytes::from_static(b"malformed"))
            .await
            .unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Invalid { .. }
        ));
        db.put(
            &metadata_key,
            legacy_metadata_bytes(&definition, physical_name, Some(entity_id)),
        )
        .await
        .unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Invalid { .. }
        ));
        db.put(&simhash_key, Bytes::from_static(b"malformed"))
            .await
            .unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Invalid { .. }
        ));
        db.put(&simhash_key, encode_simhash(simhash_bits))
            .await
            .unwrap();
        db.delete(&marker_key).await.unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Invalid { .. }
        ));
        db.put(&marker_key, Bytes::from_static(b"malformed"))
            .await
            .unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Invalid { .. }
        ));
        db.put(&marker_key, encode_simhash_directory_marker_v1())
            .await
            .unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Valid {
                markers: 1,
                exhausted: true,
                ..
            }
        ));

        db.put(
            &metadata_key,
            current_metadata_bytes(&definition, physical_name, Some(entity_id)),
        )
        .await
        .unwrap();
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Valid {
                markers: 1,
                exhausted: true,
                ..
            }
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn production_storage_contract_matrix_runs_in_workspace_tests() {
        production_contracts::run().await;
    }
}
