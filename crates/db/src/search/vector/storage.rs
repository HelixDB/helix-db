//! Typed physical namespace for persisted vector rows.
//!
//! This module is the storage boundary between logical `VectorKey` values and
//! tenant-scoped SlateDB bytes. A `VectorRowKeyspace` binds the complete physical
//! name and request scope once and derives the stable index ID internally, so
//! callers cannot pair a name with the wrong compact namespace. It delegates
//! logical serialization to the existing `encoding::v1` key types and therefore
//! does not change persisted key bytes or row codecs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::Bytes;
use slatedb::DbReadOps;

use crate::encoding::error::EncodingError;
use crate::encoding::keys::{tenant::DataScope, DataKeyKind, Key};
use crate::encoding::v1::keys::vectors::{
    VectorEntryCandidateKey, VectorEntryCandidateNodeKey, VectorEntryCandidatePrefixKey,
    VectorIndexMetadataKey, VectorItemKey, VectorKey, VectorLayer0NeighborsKey,
    VectorReverseEdgeKey, VectorReverseEdgePrefixKey, VectorSimHashKey, VectorStorageLane,
    VectorUpperNeighborsKey, VectorUpperVectorKey,
};
use crate::encoding::v1::values::vectors::{
    decode_layer0_neighbors, encode_layer0_neighbors,
    entry::{decode_entry_candidate_layer, encode_entry_candidate_layer},
    markers::encode_empty_marker,
    neighbors::{decode_upper_neighbors, encode_upper_neighbors},
    simhash::decode_simhash,
};
use crate::encoding::NodeId;
use crate::error::HelixDbError;
use crate::index_v2::VectorPhysicalIndexId;

#[cfg(any(test, feature = "production-coverage"))]
use super::index_id_from_name;
use super::{
    decode_metadata, encode_metadata, MeasuredVectorTransaction, SimHash, VectorIndexMetadata,
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

    /// Measures the exact deployed metadata point-read key and value bytes.
    ///
    /// Absence still charges the complete typed lookup key. The operation is
    /// read-only and does not decode or rewrite the stored value.
    #[cfg(feature = "production-coverage")]
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

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/storage.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::encoding::keys::tenant::TenantId;
    use crate::encoding::v1::keys::vectors::VectorIndexMetadataKey;
    use crate::encoding::v1::values::vectors::simhash::encode_simhash;

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
}
