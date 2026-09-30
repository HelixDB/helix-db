//! Operation-local state for vector graph mutations.
//!
//! This module owns mutable state that exists only while one insert, upsert, or
//! delete repairs the HNSW graph. Keeping that state outside the public index
//! façade makes the later mutation-session boundary explicit without changing
//! transaction timing, persisted rows, cache limits, or graph algorithms. The
//! cache stores each loaded row in one closed state ADT, so absence, cleanliness,
//! the first storage snapshot, and the latest staged value cannot disagree
//! across parallel collections.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use std::num::NonZeroU64;
use std::sync::Arc;

#[cfg(any(test, feature = "production-coverage"))]
use slatedb::DbReadOps;
use slatedb::DbTransaction;

use crate::encoding::v2::values::indexes::vector::encode_layer0_neighbors;
use crate::encoding::v2::values::indexes::vector::neighbors::encode_upper_neighbors;
use crate::encoding::NodeId;
use crate::error::HelixDbError;
use crate::search::vector::unaligned_vector::UnalignedVector;

use super::index::VectorIndex;
use super::model::Candidate;
use super::neighbor_set::{
    NeighborDegreeLimit, NeighborDegreeLimits, NeighborDifference, NeighborSet,
};
use crate::search::vector::distance::{ActiveVectorSemantics, Distance};
use crate::search::vector::item::Item;
use crate::search::vector::result::VectorEntityId;
#[cfg(any(test, feature = "production-coverage"))]
use crate::search::vector::storage::ReverseSourcesForTarget;
use crate::search::vector::storage::{
    EntryCandidateLayerRow, VectorRowKeyspace, VectorRows, VectorWriteRows,
};
use crate::search::vector::{
    encode_item, select_diverse, Connections, Layer0Connections, MeasuredVectorTransaction,
    ValidatedMetricVector, VectorDimension, VectorGenerationIdentity, VectorIndexMetadata,
    VectorIndexState,
};

const LAYER0_NEIGHBOR_PREFETCH_MAX_PER_STEP: usize = 2;
const LAYER0_NEIGHBOR_PREFETCH_MIN_TARGETS: usize = 2;
const LAYER0_NEIGHBOR_PREFETCH_MAX_PER_MUTATION: usize = 8;
pub(in crate::search::vector) const VECTOR_BUILD_ITEM_CACHE_LIMIT: usize = 4_096;
pub(in crate::search::vector) const VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT: usize = 2_048;
pub(in crate::search::vector) const VECTOR_BUILD_SIMHASH_CACHE_LIMIT: usize = 4_096;

#[cfg(test)]
tokio::task_local! {
    /// Plans every upsert in scope through the full delete-and-reinsert path,
    /// the unskipped reference replay tests compare against.
    pub(crate) static REPLACE_REPLAYS: ();
}

/// Whether an upsert may keep a node that already holds its exact state.
#[cfg(not(test))]
const fn replays_may_skip() -> bool {
    true
}

/// Whether an upsert may keep a node that already holds its exact state:
/// outside a [`REPLACE_REPLAYS`] scope.
#[cfg(test)]
fn replays_may_skip() -> bool {
    REPLACE_REPLAYS.try_with(|()| ()).is_err()
}

/// Bookkeeping charged against a build session budget per retained item.
///
/// Each per-entry charge covers the entry's hash-map slot, the four
/// recency-order entries its value may allocate, a dirty row's dirty-set key,
/// and the headers of its heap allocations (an item's shared decoded value and
/// vector buffer, a neighbor row's one or two neighbor lists), so a session's
/// resident memory stays near its byte budget even for tiny payloads such as
/// 8-byte SimHashes. A unit test pins every charge above the structural size
/// of the types it covers.
const VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES: usize = 224;
/// Bookkeeping charged against a build session budget per retained neighbor row.
const VECTOR_BUILD_SESSION_NEIGHBOR_OVERHEAD_BYTES: usize = 320;
/// Bookkeeping charged against a build session budget per retained SimHash.
const VECTOR_BUILD_SESSION_SIMHASH_OVERHEAD_BYTES: usize = 104;
/// Budget bytes that admit one retained entry of each class.
///
/// Byte charges already bound realistic entries. Capping every class at one
/// entry per KiB of budget also bounds how many tiny-payload entries, whose
/// resident bookkeeping the fixed charges only estimate, a session can hold.
const VECTOR_BUILD_SESSION_BUDGET_BYTES_PER_ENTRY: usize = 1024;
/// Charged size from which a dropped build session frees off the dropping thread.
const VECTOR_BUILD_SESSION_BACKGROUND_DROP_BYTES: usize = 16 * 1024 * 1024;
/// Conservative per-namespace bookkeeping charged against a build session budget.
///
/// Covers one generation namespace's cache struct and hash-map slot, its
/// identity key and physical-name allocation, its eviction-index and dirty-set
/// entries, and the smallest allocations of its per-kind maps, recency orders,
/// and dirty-row set, so a build touching many small tenant partitions stays
/// near its budget.
const VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES: usize = 4_096;

/// Aggregate observable behavior of one reusable vector build session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct VectorBuildSessionStats {
    item_hits: u64,
    item_misses: u64,
    neighbor_hits: u64,
    neighbor_misses: u64,
    simhash_hits: u64,
    simhash_misses: u64,
    item_evictions: u64,
    neighbor_evictions: u64,
    simhash_evictions: u64,
    dirty_neighbor_flushes: u64,
    max_retained_payload_bytes: u64,
}

impl VectorBuildSessionStats {
    pub(crate) const fn item_hits(self) -> u64 {
        self.item_hits
    }

    pub(crate) const fn item_misses(self) -> u64 {
        self.item_misses
    }

    pub(crate) const fn neighbor_hits(self) -> u64 {
        self.neighbor_hits
    }

    pub(crate) const fn neighbor_misses(self) -> u64 {
        self.neighbor_misses
    }

    pub(crate) const fn simhash_hits(self) -> u64 {
        self.simhash_hits
    }

    pub(crate) const fn simhash_misses(self) -> u64 {
        self.simhash_misses
    }

    pub(crate) const fn item_evictions(self) -> u64 {
        self.item_evictions
    }

    pub(crate) const fn neighbor_evictions(self) -> u64 {
        self.neighbor_evictions
    }

    pub(crate) const fn simhash_evictions(self) -> u64 {
        self.simhash_evictions
    }

    pub(crate) const fn dirty_neighbor_flushes(self) -> u64 {
        self.dirty_neighbor_flushes
    }

    pub(crate) const fn max_retained_payload_bytes(self) -> u64 {
        self.max_retained_payload_bytes
    }

    fn merge(&mut self, other: Self) {
        self.item_hits = self.item_hits.saturating_add(other.item_hits);
        self.item_misses = self.item_misses.saturating_add(other.item_misses);
        self.neighbor_hits = self.neighbor_hits.saturating_add(other.neighbor_hits);
        self.neighbor_misses = self.neighbor_misses.saturating_add(other.neighbor_misses);
        self.simhash_hits = self.simhash_hits.saturating_add(other.simhash_hits);
        self.simhash_misses = self.simhash_misses.saturating_add(other.simhash_misses);
        self.item_evictions = self.item_evictions.saturating_add(other.item_evictions);
        self.neighbor_evictions = self
            .neighbor_evictions
            .saturating_add(other.neighbor_evictions);
        self.simhash_evictions = self
            .simhash_evictions
            .saturating_add(other.simhash_evictions);
        self.dirty_neighbor_flushes = self
            .dirty_neighbor_flushes
            .saturating_add(other.dirty_neighbor_flushes);
        self.max_retained_payload_bytes = self
            .max_retained_payload_bytes
            .max(other.max_retained_payload_bytes);
    }
}

/// Capability proving a vector insertion targets a fresh node in a build-owned generation.
///
/// Construction remains private to the vector module. The V2 lifecycle driver
/// obtains it only after validating durable generation ownership and checking
/// that its source cursor has not already produced applied state for the node.
/// Consequently the node's payload, graph, SimHash, and entry-candidate rows
/// are all absent and mutation code may skip existence reads for those rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FreshVectorBuildProof {
    _private: (),
}

impl FreshVectorBuildProof {
    /// Issues freshness only after the generation module validates durable
    /// `Building` ownership for the exact operation and physical namespace.
    pub(in crate::search::vector) const fn for_building_generation() -> Self {
        Self { _private: () }
    }

    #[cfg(any(test, feature = "production-coverage"))]
    pub(in crate::search::vector) const fn for_test() -> Self {
        Self { _private: () }
    }
}

/// Internal mutation contract selected by the public or generation façade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::search::vector) enum VectorInsertContract {
    /// Remove an existing graph row before inserting its replacement, unless
    /// the row already holds exactly the replacement at its selected layer.
    Upsert,
    /// Insert after consuming lifecycle-owned proof that the target is fresh.
    ProvenFresh(FreshVectorBuildProof),
}

/// Validated upper- and layer-zero degree limits shared by graph mutations.
#[derive(Debug, Clone, Copy)]
struct MutationDegreeLimits {
    upper: Connections,
    conventional_layer0: Layer0Connections,
    layer0: Layer0Connections,
}

impl MutationDegreeLimits {
    /// Validates persisted degree settings and retains the conventional doubled floor.
    fn try_from_metadata(metadata: &VectorIndexMetadata) -> Result<Self, HelixDbError> {
        let upper = Connections::try_new(metadata.config.m)
            .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?;
        let doubled_layer0 = upper
            .checked_double()
            .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?;
        let configured_layer0 = Layer0Connections::try_new(metadata.config.m0, upper)
            .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?;
        let layer0 = if configured_layer0.get() >= doubled_layer0.get() {
            configured_layer0
        } else {
            doubled_layer0
        };
        Ok(Self {
            upper,
            conventional_layer0: doubled_layer0,
            layer0,
        })
    }
}

/// Complete input for insertion into one already-populated HNSW graph.
struct PopulatedHnswInsertion<'item, 'vector, D: Distance> {
    node_id: NodeId,
    item: &'item Item<'vector, D>,
    node_layer: u16,
    entry_point: NodeId,
    metadata: &'item VectorIndexMetadata,
}

/// Selects the nearest unloaded layer-0 rows within a mutation read budget.
///
/// Mutation prefetch consults the authoritative neighbor-row ADT, so a dirty,
/// clean-present, or clean-absent entry is never overwritten by speculative I/O.
pub(in crate::search::vector) fn select_layer0_neighbor_prefetch_targets<D: Distance>(
    newly_admitted_neighbors: &[(NodeId, f32)],
    mutation_cache: &MutationOpCache<D>,
    remaining_prefetch_budget: usize,
) -> Vec<NodeId> {
    if newly_admitted_neighbors.len() < LAYER0_NEIGHBOR_PREFETCH_MIN_TARGETS
        || remaining_prefetch_budget == 0
    {
        return Vec::new();
    }

    let target_limit = LAYER0_NEIGHBOR_PREFETCH_MAX_PER_STEP.min(remaining_prefetch_budget);
    let mut ranked = newly_admitted_neighbors.to_vec();
    ranked.sort_by(|(left_id, left_dist), (right_id, right_dist)| {
        left_dist
            .partial_cmp(right_dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left_id.cmp(right_id))
    });

    let mut targets = Vec::with_capacity(target_limit);
    let mut seen = HashSet::with_capacity(ranked.len());
    for (node_id, _) in ranked {
        let row = MutationOpCache::<D>::node_row_id(0, node_id);
        if !seen.insert(node_id) || mutation_cache.contains_neighbor(row) {
            continue;
        }
        targets.push(node_id);
        if targets.len() >= target_limit {
            break;
        }
    }
    targets
}

impl<D: Distance> VectorIndex<D> {
    /// Validates and stages current-format metadata without changing its codec.
    ///
    /// Insert/delete recovery owns the surrounding measured transaction. This
    /// boundary also binds the handle's write-once dimension before any row can
    /// subsequently be decoded under the updated metadata.
    pub(in crate::search::vector) async fn update_metadata(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        metadata: &VectorIndexMetadata,
    ) -> Result<(), HelixDbError> {
        metadata.validated_state()?;
        self.remember_dimension(metadata.config.dimension)?;
        VectorWriteRows::new(txn, self.row_keyspace()).put_metadata(metadata)
    }

    /// Looks up the maximum HNSW layer tracked for one entry candidate.
    ///
    /// Corrupt node-layer bytes are removed in the caller-owned transaction and
    /// represented as absence, preventing invalid persisted state from crossing
    /// into graph mutation.
    pub(in crate::search::vector) async fn get_entry_candidate_layer(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
    ) -> Result<Option<u16>, HelixDbError> {
        let rows = VectorWriteRows::new(txn, self.row_keyspace());
        match rows.entry_candidate_layer(node_id).await? {
            EntryCandidateLayerRow::Missing => Ok(None),
            EntryCandidateLayerRow::Present(layer) => Ok(Some(layer)),
            EntryCandidateLayerRow::Corrupt => {
                rows.delete_entry_candidate_node(node_id)?;
                Ok(None)
            }
        }
    }

    /// Stages the paired entry-candidate rows for one mutation.
    ///
    /// A prior sorted row is deleted when the node changed layers, keeping the
    /// node-to-layer row and highest-layer-first scan mutually consistent inside
    /// the caller-owned transaction.
    pub(in crate::search::vector) async fn upsert_entry_candidate(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        layer: u16,
    ) -> Result<(), HelixDbError> {
        let rows = VectorWriteRows::new(txn, self.row_keyspace());
        if let Some(previous_layer) = self.get_entry_candidate_layer(txn, node_id).await?
            && previous_layer != layer
        {
            rows.delete_entry_candidate_sorted(node_id, previous_layer)?;
        }
        rows.put_entry_candidate(node_id, layer)
    }

    /// Stages paired entry-candidate rows under build-owned node freshness.
    ///
    /// [`FreshVectorBuildProof`] makes a previous node-to-layer row
    /// unrepresentable at this boundary, so source backfill avoids one storage
    /// lookup for every planned insertion.
    pub(in crate::search::vector) fn stage_fresh_entry_candidate(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        layer: u16,
        _proof: FreshVectorBuildProof,
    ) -> Result<(), HelixDbError> {
        VectorWriteRows::new(txn, self.row_keyspace()).put_entry_candidate(node_id, layer)
    }

    /// Removes both deployed entry-candidate rows for one mutation target.
    pub(in crate::search::vector) async fn remove_entry_candidate(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
    ) -> Result<(), HelixDbError> {
        let rows = VectorWriteRows::new(txn, self.row_keyspace());
        if let Some(layer) = self.get_entry_candidate_layer(txn, node_id).await? {
            rows.delete_entry_candidate_sorted(node_id, layer)?;
        }
        rows.delete_entry_candidate_node(node_id)
    }

    /// Finds the highest live entry candidate while staging stale-row cleanup.
    ///
    /// The caller owns the measured transaction. Corrupt, mismatched, or
    /// payload-less candidates are pruned before a replacement is returned.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(in crate::search::vector) async fn find_best_entry_candidate(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
    ) -> Result<Option<(NodeId, u16)>, HelixDbError> {
        let mut mutation_cache = MutationOpCache::default();
        self.find_best_entry_candidate_cached(txn, &mut mutation_cache)
            .await
    }

    /// Finds the highest live candidate through reusable decoded-item state.
    async fn find_best_entry_candidate_cached(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<Option<(NodeId, u16)>, HelixDbError> {
        let rows = VectorWriteRows::new(txn, self.row_keyspace());
        let mut candidates = rows.entry_candidates().await?;

        while let Some(candidate) = candidates.next().await? {
            let layer = candidate.layer();
            let node_id = candidate.node_id();
            let node_layer = match rows.entry_candidate_layer(node_id).await? {
                EntryCandidateLayerRow::Missing => None,
                EntryCandidateLayerRow::Present(node_layer) => Some(node_layer),
                EntryCandidateLayerRow::Corrupt => {
                    rows.delete_scanned_entry_candidate(&candidate)?;
                    rows.delete_entry_candidate_node(node_id)?;
                    None
                }
            };

            let Some(node_layer) = node_layer else {
                rows.delete_scanned_entry_candidate(&candidate)?;
                continue;
            };
            if node_layer != layer {
                rows.delete_scanned_entry_candidate(&candidate)?;
                continue;
            }
            if self
                .get_item_for_layer_cached(txn, 0, node_id, mutation_cache)
                .await?
                .is_some()
            {
                return Ok(Some((node_id, layer)));
            }
            rows.delete_scanned_entry_candidate(&candidate)?;
            rows.delete_entry_candidate_node(node_id)?;
        }
        Ok(None)
    }

    /// Repairs stale entry metadata before an insert or deletion mutates graph rows.
    ///
    /// Replacement selection and metadata repair are staged in the caller's
    /// transaction, so no independently visible repair state is introduced.
    pub(in crate::search::vector) async fn repair_stale_entry_point_for_write(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        metadata: &mut VectorIndexMetadata,
        operation: &'static str,
        node_id: NodeId,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<bool, HelixDbError> {
        let Some(stale_entry_point) = metadata.entry_point else {
            return Ok(false);
        };
        if self
            .get_item_for_layer_cached(txn, 0, stale_entry_point, mutation_cache)
            .await?
            .is_some()
        {
            return Ok(false);
        }

        let old_max_layer = metadata.max_layer;
        if let Some((replacement_entry_point, replacement_layer)) = self
            .find_best_entry_candidate_cached(txn, mutation_cache)
            .await?
        {
            metadata.entry_point = Some(replacement_entry_point);
            metadata.max_layer = replacement_layer;
            self.update_metadata(txn, metadata).await?;
            tracing::warn!(
                index_name = %self.name(),
                index_id = self.id(),
                operation,
                node_id,
                stale_entry_point,
                replacement_entry_point,
                old_max_layer,
                new_max_layer = replacement_layer,
                "repaired stale vector entry point"
            );
        } else {
            metadata.entry_point = None;
            metadata.max_layer = 0;
            self.update_metadata(txn, metadata).await?;
            tracing::warn!(
                index_name = %self.name(),
                index_id = self.id(),
                operation,
                node_id,
                stale_entry_point,
                old_max_layer,
                "cleared stale vector entry point with no live replacement candidate"
            );
        }
        Ok(true)
    }

    /// Resolves a live traversal root before mutation beam expansion.
    ///
    /// Missing items fall through to the writable candidate index and return an
    /// owned item, or `None` when insertion must continue with an empty candidate
    /// set. Any candidate cleanup remains staged in the caller's measured
    /// transaction; this method never mutates a resident snapshot.
    pub(in crate::search::vector) async fn resolve_beam_entry_point_for_insert(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        entry_point: NodeId,
        layer: u16,
        inserting_node_id: NodeId,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<Option<(NodeId, Item<'static, D>)>, HelixDbError> {
        if let Some(item) = self
            .get_item_for_layer_cached(txn, layer, entry_point, mutation_cache)
            .await?
        {
            return Ok(Some((entry_point, item.as_ref().clone())));
        }

        if let Some((replacement_entry_point, replacement_layer)) = self
            .find_best_entry_candidate_cached(txn, mutation_cache)
            .await?
            && replacement_entry_point != entry_point
            && let Some(item) = self
                .get_item_for_layer_cached(txn, layer, replacement_entry_point, mutation_cache)
                .await?
        {
            tracing::warn!(
                index_name = %self.name(),
                index_id = self.id(),
                operation = "insert_beam",
                inserting_node_id,
                traversal_layer = layer,
                stale_entry_point = entry_point,
                replacement_entry_point,
                replacement_candidate_layer = replacement_layer,
                "recovered missing HNSW traversal entry point during insert"
            );
            return Ok(Some((replacement_entry_point, item.as_ref().clone())));
        }

        tracing::warn!(
            index_name = %self.name(),
            index_id = self.id(),
            operation = "insert_beam",
            inserting_node_id,
            traversal_layer = layer,
            stale_entry_point = entry_point,
            "missing HNSW traversal entry point during insert; continuing with empty candidate set"
        );
        Ok(None)
    }

    /// Stages one validated insert or upsert in a caller-owned measured write set.
    ///
    /// This is the mutation module's coarse insertion boundary. It validates the
    /// logical vector before staging rows, preserves the deployed item/SimHash
    /// codecs, and keeps graph repair, entry-candidate updates, metadata changes,
    /// and cache fencing inside the same caller-owned transaction. A supplied
    /// layer makes planning deterministic; otherwise the façade's configured
    /// selector chooses it exactly once.
    #[allow(
        dead_code,
        reason = "retained behind direct legacy and measured lifecycle mutation contracts"
    )]
    pub(in crate::search::vector) async fn insert_with_measured_transaction(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        vector: &[f32],
        contract: VectorInsertContract,
        selected_layer: Option<u16>,
    ) -> Result<(), HelixDbError> {
        let Some(mut metadata) = self.get_metadata(txn).await? else {
            return Err(HelixDbError::IndexNotFound(self.name().to_string()));
        };
        let semantics = ActiveVectorSemantics::for_distance::<D>().ok_or_else(|| {
            HelixDbError::Config(format!(
                "vector distance '{}' has no stable durable semantic identity",
                D::name()
            ))
        })?;
        let dimension = VectorDimension::try_new(metadata.config.dimension)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
        let vector = ValidatedMetricVector::try_new(
            UnalignedVector::<D::VectorCodec>::from_slice(vector),
            semantics.distance_metric(),
            dimension,
        )
        .map_err(HelixDbError::from)?;
        let upper_connections = metadata.config.m;
        let connections = match Connections::try_new(upper_connections) {
            Ok(connections) => connections,
            Err(error) => return Err(HelixDbError::InvalidVectorConfig(error.into())),
        };
        let layer0_connections = match Layer0Connections::try_new(metadata.config.m0, connections) {
            Ok(connections) => connections,
            Err(error) => return Err(HelixDbError::InvalidVectorConfig(error.into())),
        };
        let doubled_connections = match connections.checked_double() {
            Ok(connections) => connections,
            Err(error) => return Err(HelixDbError::InvalidVectorConfig(error.into())),
        };
        let layer0_connections = layer0_connections.get().max(doubled_connections.get());
        let mut mutation_cache =
            MutationOpCache::with_degree_limits(layer0_connections, upper_connections)?;
        let result = self
            .insert_with_mutation_cache(
                txn,
                node_id,
                &vector,
                contract,
                selected_layer,
                &mut metadata,
                &mut mutation_cache,
                true,
            )
            .await;
        #[cfg(feature = "production-coverage")]
        crate::search::vector::record_benchmark_cache_stats(mutation_cache.stats);
        result
    }

    /// Reuses one generation-qualified build cache across successive entities.
    pub(in crate::search::vector) async fn insert_with_build_session(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        vector: &[f32],
        contract: VectorInsertContract,
        selected_layer: Option<u16>,
        session: &mut VectorBuildSession<D>,
    ) -> Result<(), HelixDbError> {
        let Some(mut metadata) = self.get_metadata(txn).await? else {
            return Err(HelixDbError::IndexNotFound(self.name().to_string()));
        };
        let semantics = ActiveVectorSemantics::for_distance::<D>().ok_or_else(|| {
            HelixDbError::Config(format!(
                "vector distance '{}' has no stable durable semantic identity",
                D::name()
            ))
        })?;
        let dimension = VectorDimension::try_new(metadata.config.dimension)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
        let vector = ValidatedMetricVector::try_new(
            UnalignedVector::<D::VectorCodec>::from_slice(vector),
            semantics.distance_metric(),
            dimension,
        )
        .map_err(HelixDbError::from)?;
        let upper_connections = metadata.config.m;
        let connections = match Connections::try_new(upper_connections) {
            Ok(connections) => connections,
            Err(error) => return Err(HelixDbError::InvalidVectorConfig(error.into())),
        };
        let layer0_connections = match Layer0Connections::try_new(metadata.config.m0, connections) {
            Ok(connections) => connections,
            Err(error) => return Err(HelixDbError::InvalidVectorConfig(error.into())),
        };
        let doubled_connections = match connections.checked_double() {
            Ok(connections) => connections,
            Err(error) => return Err(HelixDbError::InvalidVectorConfig(error.into())),
        };
        let layer0_connections = layer0_connections.get().max(doubled_connections.get());
        let identity = self.build_session_identity()?.clone();
        let mut mutation_cache =
            session.take_cache(&identity, layer0_connections, upper_connections)?;
        mutation_cache.begin_entity();
        let result = self
            .insert_with_mutation_cache(
                txn,
                node_id,
                &vector,
                contract,
                selected_layer,
                &mut metadata,
                &mut mutation_cache,
                false,
            )
            .await;
        let changed = mutation_cache.finish_entity_changes();
        session.restore_cache(identity.clone(), mutation_cache);
        session.record_entity_changes(&identity, node_id, changed.into_keys());
        result
    }

    /// Inserts `node_id`, first deleting an existing node under
    /// [`VectorInsertContract::Upsert`].
    ///
    /// An upsert that replays the node's exact state stages nothing. That
    /// means the node holds `vector` byte for byte (`-0.0` never matches
    /// `0.0`) and its entry-candidate row records `selected_layer`. Deleting
    /// and reinserting it would restage the same item rows, so the node keeps
    /// its links: no row or SimHash is written and no cache entry is
    /// invalidated. A random layer (`None`) and a missing or corrupt
    /// entry-candidate row always take the full path.
    #[allow(
        clippy::too_many_arguments,
        reason = "the mutation cache binds the exact contract, metadata, and flush ownership"
    )]
    async fn insert_with_mutation_cache(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        vector: &ValidatedMetricVector<'_, D::VectorCodec>,
        contract: VectorInsertContract,
        selected_layer: Option<u16>,
        metadata: &mut VectorIndexMetadata,
        mutation_cache: &mut MutationOpCache<D>,
        flush_after: bool,
    ) -> Result<(), HelixDbError> {
        if matches!(contract, VectorInsertContract::Upsert)
            && let Some(existing) = self
                .get_item_for_layer_cached(txn, 0, node_id, mutation_cache)
                .await?
        {
            if let Some(layer) = selected_layer
                && replays_may_skip()
                && existing.vector.as_bytes() == vector.values().as_bytes()
                && VectorWriteRows::new(txn, self.row_keyspace())
                    .entry_candidate_layer(node_id)
                    .await?
                    == EntryCandidateLayerRow::Present(layer)
            {
                return Ok(());
            }
            let _ = self
                .stage_delete_with_metadata(txn, node_id, metadata, mutation_cache)
                .await?;
        }

        let item = Item::<D> {
            header: D::new_header(vector.values()),
            vector: std::borrow::Cow::Borrowed(vector.values()),
        };

        let simhash_cache = self.simhash_cache(metadata.config.dimension)?;
        let simhash = simhash_cache.compute_and_cache_measured(txn, node_id, vector)?;
        mutation_cache.put_simhash(node_id, Some(simhash));

        let node_layer =
            selected_layer.unwrap_or_else(|| self.select_mutation_layer(metadata.config.ml));
        mutation_cache.invalidate_neighbors(node_id);
        let canonical_key = self.canonical_vector_key_from_simhash(node_id, simhash);
        let encoded_item = encode_item(&item);
        let encoded_item_bytes = encoded_item.len();
        let rows = VectorWriteRows::new(txn, self.row_keyspace());

        if node_layer > 0 {
            rows.put_canonical_vector(&canonical_key, encoded_item.clone())?;
            rows.put_upper_vector(node_id, encoded_item)?;
        } else {
            rows.put_canonical_vector(&canonical_key, encoded_item)?;
        }
        if self.simhash_directory_enabled() {
            rows.put_simhash_directory_entry(&canonical_key)?;
        }
        let retained_item = Arc::new(item.clone().into_owned());
        mutation_cache.invalidate_items(node_id);
        for layer in 0..=node_layer {
            mutation_cache.put_item(
                layer,
                node_id,
                Some(Arc::clone(&retained_item)),
                encoded_item_bytes,
            );
        }

        self.repair_stale_entry_point_for_write(txn, metadata, "insert", node_id, mutation_cache)
            .await?;

        let VectorIndexState::Populated {
            entry_point,
            max_layer: previous_max_layer,
        } = metadata.validated_state()?
        else {
            metadata.entry_point = Some(node_id);
            metadata.max_layer = node_layer;
            metadata.count = 1;

            for layer in 0..=node_layer {
                self.stage_new_neighbors_for_mutation(
                    txn,
                    layer,
                    node_id,
                    Vec::new(),
                    mutation_cache,
                )
                .await?;
            }
            match contract {
                VectorInsertContract::Upsert => {
                    self.upsert_entry_candidate(txn, node_id, node_layer)
                        .await?;
                }
                VectorInsertContract::ProvenFresh(proof) => {
                    self.stage_fresh_entry_candidate(txn, node_id, node_layer, proof)?;
                }
            }
            self.update_metadata(txn, metadata).await?;
            if flush_after {
                self.flush_mutation_cache(txn, mutation_cache).await?;
            }
            return Ok(());
        };

        self.insert_hnsw(
            txn,
            PopulatedHnswInsertion {
                node_id,
                item: &item,
                node_layer,
                entry_point,
                metadata,
            },
            mutation_cache,
        )
        .await?;
        match contract {
            VectorInsertContract::Upsert => {
                self.upsert_entry_candidate(txn, node_id, node_layer)
                    .await?;
            }
            VectorInsertContract::ProvenFresh(proof) => {
                self.stage_fresh_entry_candidate(txn, node_id, node_layer, proof)?;
            }
        }

        metadata.count = metadata.count.checked_add(1).ok_or_else(|| {
            HelixDbError::InvariantViolation(format!(
                "vector index '{}' count overflowed during insert",
                self.name()
            ))
        })?;
        if node_layer > previous_max_layer {
            metadata.entry_point = Some(node_id);
            metadata.max_layer = node_layer;
        }
        self.update_metadata(txn, metadata).await?;

        if flush_after {
            self.flush_mutation_cache(txn, mutation_cache).await?;
        }

        Ok(())
    }

    /// Inserts one row into an already-populated, validated HNSW graph.
    ///
    /// The caller supplies metadata whose populated state yielded `entry_point`.
    /// This operation owns traversal, bounded neighbor selection, reciprocal-link
    /// staging, and the final cache flush, while typed storage owns row encoding.
    async fn insert_hnsw(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        insertion: PopulatedHnswInsertion<'_, '_, D>,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<(), HelixDbError> {
        let PopulatedHnswInsertion {
            node_id,
            item,
            node_layer,
            entry_point,
            metadata,
        } = insertion;
        let old_max_layer = metadata.max_layer;
        let degree_limits = MutationDegreeLimits::try_from_metadata(metadata)?;
        let maximum_upper_connections = degree_limits.upper.get();
        let doubled_upper_connections = degree_limits.conventional_layer0.get();
        let maximum_layer0_connections = degree_limits.layer0.get();
        let ef_construction = metadata.config.ef_construction;
        let mut current_entry_point = entry_point;
        if node_layer < old_max_layer {
            for layer in (node_layer + 1..=old_max_layer).rev() {
                current_entry_point = self
                    .search_layer_greedy_for_mutation(
                        txn,
                        item,
                        current_entry_point,
                        layer,
                        mutation_cache,
                    )
                    .await?;
            }
        }

        let insertion_top_layer = old_max_layer.min(node_layer);
        for layer in (0..=insertion_top_layer).rev() {
            let ef = if layer == 0 {
                ef_construction.max(maximum_layer0_connections)
            } else {
                ef_construction.max(doubled_upper_connections)
            };
            let candidates = self
                .search_layer_beam(
                    txn,
                    item,
                    current_entry_point,
                    layer,
                    ef,
                    node_id,
                    mutation_cache,
                )
                .await?;
            let maximum_neighbors = if layer == 0 {
                maximum_layer0_connections
            } else {
                maximum_upper_connections
            };
            let neighbors = self
                .select_neighbors_heuristic(
                    txn,
                    item,
                    &candidates,
                    maximum_neighbors,
                    layer,
                    mutation_cache,
                )
                .await?;

            self.stage_new_neighbors_for_mutation(
                txn,
                layer,
                node_id,
                neighbors.clone(),
                mutation_cache,
            )
            .await?;
            for neighbor_id in neighbors {
                self.add_bidirectional_link(
                    txn,
                    layer,
                    node_id,
                    neighbor_id,
                    item,
                    maximum_neighbors,
                    mutation_cache,
                )
                .await?;
            }

            if let Some(candidate) = candidates.first() {
                current_entry_point = candidate.node_id;
            }
        }

        if node_layer > old_max_layer {
            for layer in old_max_layer + 1..=node_layer {
                self.stage_new_neighbors_for_mutation(
                    txn,
                    layer,
                    node_id,
                    Vec::new(),
                    mutation_cache,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Searches one HNSW layer while constructing or repairing a mutation.
    ///
    /// Unlike read-only greedy traversal, this beam consults the operation-local
    /// neighbor/item cache so staged rows are authoritative and speculative
    /// layer-0 reads remain bounded. Missing entry points are resolved through
    /// the write-side recovery contract before expansion begins.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::search::vector) async fn search_layer_beam(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        query: &Item<'_, D>,
        entry_point: NodeId,
        layer: u16,
        ef: usize,
        inserting_node_id: NodeId,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<Vec<Candidate>, HelixDbError> {
        let mut visited = foldhash::HashSet::default();
        let mut candidates = BinaryHeap::new();
        let mut w = BinaryHeap::new();
        let mut remaining_layer0_neighbor_prefetch_budget = if layer == 0 {
            LAYER0_NEIGHBOR_PREFETCH_MAX_PER_MUTATION
        } else {
            0
        };

        let Some((resolved_entry_point, entry_item)) = self
            .resolve_beam_entry_point_for_insert(
                txn,
                entry_point,
                layer,
                inserting_node_id,
                mutation_cache,
            )
            .await?
        else {
            return Ok(Vec::new());
        };
        let entry_distance = D::distance(query, &entry_item);
        candidates.push(Reverse(Candidate::try_new(
            resolved_entry_point,
            entry_distance,
        )?));
        w.push(Candidate::try_new(resolved_entry_point, entry_distance)?);
        visited.insert(resolved_entry_point);

        while !candidates.is_empty() {
            let Reverse(current) = candidates.pop().unwrap();
            let current_distance = current.score();
            if w.len() >= ef && current_distance > w.peek().unwrap().score() {
                break;
            }

            let neighbors = self
                .load_neighbors_for_mutation(txn, layer, current.node_id, mutation_cache)
                .await?;
            let mut frontier = Vec::new();
            for &neighbor_id in &neighbors {
                if visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);
                frontier.push(neighbor_id);
            }
            let neighbor_items = self
                .get_items_for_layer_cached_batch(txn, layer, &frontier, mutation_cache)
                .await?;
            let mut newly_admitted_neighbors = Vec::new();
            for neighbor_id in frontier {
                let Some(neighbor_item) = neighbor_items.get(&neighbor_id) else {
                    continue;
                };
                let candidate =
                    Candidate::try_new(neighbor_id, D::distance(query, neighbor_item.as_ref()))?;
                let distance = candidate.score();
                if w.len() < ef || distance < w.peek().unwrap().score() {
                    candidates.push(Reverse(candidate));
                    w.push(candidate);
                    newly_admitted_neighbors.push((neighbor_id, distance));
                    if w.len() > ef {
                        w.pop();
                    }
                }
            }

            if remaining_layer0_neighbor_prefetch_budget > 0 {
                let prefetch_targets = select_layer0_neighbor_prefetch_targets(
                    &newly_admitted_neighbors,
                    mutation_cache,
                    remaining_layer0_neighbor_prefetch_budget,
                );
                if !prefetch_targets.is_empty() {
                    let fetched = self
                        .prefetch_layer0_neighbors_for_mutation(
                            txn,
                            &prefetch_targets,
                            mutation_cache,
                        )
                        .await?;
                    remaining_layer0_neighbor_prefetch_budget =
                        remaining_layer0_neighbor_prefetch_budget.saturating_sub(fetched);
                }
            }
        }

        let mut results = w.into_iter().collect::<Vec<_>>();
        results.sort();
        Ok(results)
    }

    /// Greedily descends through reusable item and neighbor cache state.
    async fn search_layer_greedy_for_mutation(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        query: &Item<'_, D>,
        entry_point: NodeId,
        layer: u16,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<NodeId, HelixDbError> {
        let mut visited = foldhash::HashSet::default();
        let mut current = entry_point;
        let Some(current_item) = self
            .get_item_for_layer_cached(txn, layer, current, mutation_cache)
            .await?
        else {
            tracing::warn!(
                index_name = %self.name(),
                index_id = self.id(),
                operation = "greedy_insert",
                traversal_layer = layer,
                stale_entry_point = entry_point,
                "missing HNSW greedy entry point item; reusing caller-provided entry point"
            );
            return Ok(entry_point);
        };
        let mut current_distance =
            Candidate::try_new(current, D::distance(query, current_item.as_ref()))?.score();
        visited.insert(current);

        loop {
            let neighbors = self
                .load_neighbors_for_mutation(txn, layer, current, mutation_cache)
                .await?;
            let frontier = neighbors
                .into_iter()
                .filter(|neighbor_id| visited.insert(*neighbor_id))
                .collect::<Vec<_>>();
            let items = self
                .get_items_for_layer_cached_batch(txn, layer, &frontier, mutation_cache)
                .await?;
            let mut changed = false;
            for neighbor_id in frontier {
                let Some(item) = items.get(&neighbor_id) else {
                    continue;
                };
                let distance =
                    Candidate::try_new(neighbor_id, D::distance(query, item.as_ref()))?.score();
                if distance < current_distance {
                    current = neighbor_id;
                    current_distance = distance;
                    changed = true;
                }
            }
            if !changed {
                return Ok(current);
            }
        }
    }

    /// Selects a bounded diverse neighbor set for one mutation layer.
    ///
    /// Candidate vectors are hydrated through the operation-local item cache
    /// before applying HNSW Algorithm 4. The method owns graph-selection policy
    /// only; row encoding and write staging remain behind the index storage
    /// primitives used by the surrounding mutation session.
    pub(in crate::search::vector) async fn select_neighbors_heuristic(
        &self,
        txn: &DbTransaction,
        query: &Item<'_, D>,
        candidates: &[Candidate],
        maximum_neighbors: usize,
        layer: u16,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<Vec<NodeId>, HelixDbError> {
        let candidate_ids = candidates
            .iter()
            .take(maximum_neighbors * 2)
            .map(|candidate| candidate.node_id)
            .collect::<Vec<_>>();
        let items = self
            .get_items_for_layer_cached_batch(txn, layer, &candidate_ids, mutation_cache)
            .await?;
        select_diverse(
            query,
            candidates,
            &|node_id| items.get(&node_id).map(|item| item.as_ref()),
            maximum_neighbors,
        )
    }

    /// Stages one canonical layer-0 neighbor row through typed storage.
    pub(in crate::search::vector) async fn store_neighbors_layer0(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        neighbors: &[NodeId],
    ) -> Result<(), HelixDbError> {
        VectorWriteRows::new(txn, self.row_keyspace()).put_layer0_neighbors(node_id, neighbors)
    }

    /// Stages one canonical upper-neighbor row through typed storage.
    pub(in crate::search::vector) fn store_upper_neighbors(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        node_id: NodeId,
        neighbors: &[NodeId],
    ) -> Result<(), HelixDbError> {
        VectorWriteRows::new(txn, self.row_keyspace())
            .put_upper_neighbors(layer, node_id, neighbors)
    }

    /// Computes the exact linear reverse-locator delta between canonical rows.
    pub(in crate::search::vector) fn neighbor_deltas(
        old_neighbors: &NeighborSet,
        new_neighbors: &NeighborSet,
    ) -> Result<NeighborDifference, HelixDbError> {
        old_neighbors
            .difference(new_neighbors)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))
    }

    /// Stages only reverse-locator changes implied by a canonical row update.
    pub(in crate::search::vector) fn update_reverse_edge_locator(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        source_node_id: NodeId,
        old_neighbors: &NeighborSet,
        new_neighbors: &NeighborSet,
    ) -> Result<(), HelixDbError> {
        let (removed, added) = Self::neighbor_deltas(old_neighbors, new_neighbors)?.into_parts();
        let rows = VectorWriteRows::new(txn, self.row_keyspace());
        for target_node_id in removed {
            rows.delete_reverse_locator(target_node_id, layer, source_node_id)?;
        }
        for target_node_id in added {
            rows.put_reverse_locator(target_node_id, layer, source_node_id)?;
        }
        Ok(())
    }

    /// Loads every reverse source grouped by layer for deletion repair.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(in crate::search::vector) async fn load_reverse_sources_for_target(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        target_node_id: NodeId,
    ) -> Result<ReverseSourcesForTarget, HelixDbError> {
        VectorRows::new(read, self.row_keyspace())
            .reverse_sources_for_target(target_node_id)
            .await
    }

    /// Loads one neighbor row into the authoritative mutation cache on demand.
    ///
    /// Cache absence means only “not loaded.” A storage miss is installed as
    /// `KnownAbsent`, while a present row is validated against its layer degree
    /// before use. Admission then enforces the operation cache bound.
    pub(in crate::search::vector) async fn load_neighbors_for_mutation(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        node_id: NodeId,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<Vec<NodeId>, HelixDbError> {
        let row = MutationOpCache::<D>::node_row_id(layer, node_id);
        if mutation_cache.contains_neighbor(row) {
            let cached = mutation_cache
                .touched_neighbor(row)
                .expect("contained vector neighbor row has authoritative cache state");
            return Ok(match cached.current() {
                NeighborRowValue::KnownAbsent => Vec::new(),
                NeighborRowValue::Present(neighbors) => neighbors.to_vec(),
            });
        }

        let loaded = if layer == 0 {
            VectorRows::new(txn, self.row_keyspace())
                .layer0_neighbor_row(node_id)
                .await?
        } else {
            self.load_upper_neighbors(txn, layer, node_id).await?
        };
        let (value, result) = match loaded {
            Some(loaded) => {
                let loaded = NeighborSet::try_from_deployed(
                    node_id,
                    mutation_cache.degree_limit(layer),
                    loaded,
                )
                .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
                let result = loaded.to_vec();
                (NeighborRowValue::Present(loaded), result)
            }
            None => (NeighborRowValue::KnownAbsent, Vec::new()),
        };
        mutation_cache.install_loaded_neighbor(row, value);
        self.enforce_mutation_cache_bounds(txn, mutation_cache)
            .await?;
        Ok(result)
    }

    /// Prefetches unique unloaded layer-0 rows without overwriting cached state.
    ///
    /// Both clean and dirty entries are protected. Returned rows are validated
    /// and installed as explicit present/absent states before bounded eviction.
    pub(in crate::search::vector) async fn prefetch_layer0_neighbors_for_mutation(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_ids: &[NodeId],
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<usize, HelixDbError> {
        if node_ids.is_empty() {
            return Ok(0);
        }
        let fetch_ids = node_ids
            .iter()
            .copied()
            .filter(|node_id| {
                let row = MutationOpCache::<D>::node_row_id(0, *node_id);
                !mutation_cache.contains_neighbor(row)
            })
            .collect::<BTreeSet<_>>();
        if fetch_ids.is_empty() {
            return Ok(0);
        }

        let fetch_ids = fetch_ids.into_iter().collect::<Vec<_>>();
        let rows = VectorWriteRows::new(txn, self.row_keyspace())
            .layer0_neighbor_rows(&fetch_ids)
            .await?;
        let mut loaded_count = 0usize;
        for (node_id, maybe_row) in fetch_ids.into_iter().zip(rows) {
            let row = MutationOpCache::<D>::node_row_id(0, node_id);
            if mutation_cache.contains_neighbor(row) {
                continue;
            }
            let value = match maybe_row {
                Some(neighbors) => NeighborRowValue::Present(
                    NeighborSet::try_from_deployed(
                        node_id,
                        mutation_cache.degree_limit(0),
                        neighbors,
                    )
                    .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?,
                ),
                None => NeighborRowValue::KnownAbsent,
            };
            mutation_cache.install_loaded_neighbor(row, value);
            loaded_count = loaded_count.saturating_add(1);
        }
        self.enforce_mutation_cache_bounds(txn, mutation_cache)
            .await?;
        Ok(loaded_count)
    }

    /// Copies borrowed algorithm output into the canonical staging boundary.
    pub(in crate::search::vector) async fn stage_neighbors_for_mutation(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        node_id: NodeId,
        neighbors: &[NodeId],
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<(), HelixDbError> {
        self.stage_neighbors_vec_for_mutation(
            txn,
            layer,
            node_id,
            neighbors.to_vec(),
            mutation_cache,
        )
        .await
    }

    /// Canonicalizes algorithm output under the validated layer degree limit.
    ///
    /// Distance-ranked output is sorted into stable node-ID order. Duplicate,
    /// self-neighbor, or excessive-degree states fail before cache or DB writes.
    pub(in crate::search::vector) async fn stage_neighbors_vec_for_mutation(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        node_id: NodeId,
        mut neighbors: Vec<NodeId>,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<(), HelixDbError> {
        let row = MutationOpCache::<D>::node_row_id(layer, node_id);
        neighbors.sort_unstable();
        let neighbors =
            NeighborSet::try_from_canonical(node_id, mutation_cache.degree_limit(layer), neighbors)
                .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
        mutation_cache.stage_loaded_neighbor(row, NeighborRowValue::Present(neighbors))?;
        self.enforce_mutation_cache_bounds(txn, mutation_cache)
            .await
    }

    /// Stages a freshly allocated row using the cache’s private absent-row proof.
    ///
    /// The proof prevents an unloaded existing row from being misclassified as
    /// absent; canonical validation still occurs before the cache is mutated.
    pub(in crate::search::vector) async fn stage_new_neighbors_for_mutation(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        node_id: NodeId,
        mut neighbors: Vec<NodeId>,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<(), HelixDbError> {
        let row = MutationOpCache::<D>::node_row_id(layer, node_id);
        let proof = mutation_cache.prove_new_neighbor_row(row)?;
        neighbors.sort_unstable();
        let neighbors =
            NeighborSet::try_from_canonical(node_id, mutation_cache.degree_limit(layer), neighbors)
                .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
        mutation_cache.stage_new_neighbor(proof, NeighborRowValue::Present(neighbors));
        self.enforce_mutation_cache_bounds(txn, mutation_cache)
            .await
    }

    /// Flushes every dirty neighbor row while retaining clean cache entries.
    ///
    /// Rows are processed oldest-first. A failed storage write returns before
    /// the authoritative cache state changes, allowing an exact retry.
    pub(in crate::search::vector) async fn flush_mutation_cache(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<(), HelixDbError> {
        while let Some(row) = mutation_cache.oldest_dirty_neighbor() {
            self.flush_one_cached_neighbor(txn, mutation_cache, row, false)
                .await?;
        }
        Ok(())
    }

    /// Flushes or evicts oldest entries until the operation cache is bounded.
    ///
    /// Dirty entries are durably staged before eviction; clean entries require
    /// no write. The bounded scan order is deterministic under equal recency.
    pub(in crate::search::vector) async fn enforce_mutation_cache_bounds(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<(), HelixDbError> {
        if !mutation_cache.enforces_local_limits() {
            return Ok(());
        }
        while mutation_cache.neighbor_count() > VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT {
            if self
                .flush_and_evict_oldest_dirty_neighbor(txn, mutation_cache)
                .await?
            {
                continue;
            }
            if self.evict_oldest_clean_neighbor(mutation_cache) {
                continue;
            }
            break;
        }
        Ok(())
    }

    /// Flushes and removes the oldest dirty entry, if one exists.
    async fn flush_and_evict_oldest_dirty_neighbor(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<bool, HelixDbError> {
        let Some(row) = mutation_cache.oldest_dirty_neighbor() else {
            return Ok(false);
        };
        self.flush_one_cached_neighbor(txn, mutation_cache, row, true)
            .await?;
        Ok(true)
    }

    /// Removes the oldest clean neighbor and its same-row item entry without I/O.
    pub(in crate::search::vector) fn evict_oldest_clean_neighbor(
        &self,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> bool {
        let Some(row) = mutation_cache.oldest_clean_neighbor() else {
            return false;
        };
        mutation_cache.remove_neighbor(row);
        let (layer, node_id) = row.storage_parts();
        mutation_cache.remove_item(layer, node_id);
        true
    }

    /// Flushes one dirty row and transitions it only after all writes succeed.
    ///
    /// Reverse locators are staged before the canonical neighbor row. If the
    /// original and current values agree, no storage operation is emitted.
    /// Successful callers may retain the row as clean or evict it atomically
    /// from the operation cache; any error preserves the exact dirty state.
    pub(in crate::search::vector) async fn flush_one_cached_neighbor(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        mutation_cache: &mut MutationOpCache<D>,
        row: NeighborRowId,
        evict_after_flush: bool,
    ) -> Result<(), HelixDbError> {
        let Some(cached) = mutation_cache.neighbor(row).cloned() else {
            return Ok(());
        };
        if !cached.is_dirty() {
            return Ok(());
        }
        let (layer, node_id) = row.storage_parts();
        let NeighborRowValue::Present(current_neighbors) = cached.current() else {
            return Err(HelixDbError::InvariantViolation(
                "vector mutation cannot flush a deleted neighbor row".to_string(),
            ));
        };
        let original = cached
            .original()
            .expect("dirty vector neighbor rows retain an original value");
        let previous_neighbors = match original {
            NeighborRowValue::KnownAbsent => {
                NeighborSet::empty(node_id, mutation_cache.degree_limit(layer))
            }
            NeighborRowValue::Present(neighbors) => neighbors.clone(),
        };

        if original != cached.current() {
            self.update_reverse_edge_locator(
                txn,
                layer,
                node_id,
                &previous_neighbors,
                current_neighbors,
            )?;
            if layer == 0 {
                self.store_neighbors_layer0(txn, node_id, current_neighbors.as_slice())
                    .await?;
            } else {
                self.store_upper_neighbors(txn, layer, node_id, current_neighbors.as_slice())?;
            }
        }

        if evict_after_flush {
            mutation_cache.remove_neighbor(row);
            mutation_cache.remove_item(layer, node_id);
        } else {
            mutation_cache.mark_neighbor_flushed(row);
        }
        #[cfg(feature = "production-coverage")]
        crate::search::vector::record_benchmark_dirty_neighbor_flush();
        Ok(())
    }

    /// Adds one reciprocal HNSW link and prunes the destination to its degree.
    ///
    /// Selection uses cached vectors when available and falls back to stable
    /// truncation only when the destination vector is absent. Every candidate
    /// rejected by pruning is removed from its reciprocal row in the same
    /// operation cache, preserving bidirectionality before the flush boundary.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::search::vector) async fn add_bidirectional_link(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        from_node: NodeId,
        to_node: NodeId,
        from_item: &Item<'_, D>,
        maximum_neighbors: usize,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<(), HelixDbError> {
        let mut to_neighbors = self
            .load_neighbors_for_mutation(txn, layer, to_node, mutation_cache)
            .await?;
        if !to_neighbors.contains(&from_node) {
            to_neighbors.push(from_node);
        }
        let candidate_neighbors = to_neighbors.clone();

        if to_neighbors.len() > maximum_neighbors {
            // One batched read hydrates the destination and every candidate,
            // including the inserting node whose cached item feeds selection.
            let hydrate = core::iter::once(to_node)
                .chain(to_neighbors.iter().copied())
                .collect::<Vec<_>>();
            let items = self
                .get_items_for_layer_cached_batch(txn, layer, &hydrate, mutation_cache)
                .await?;
            match items.get(&to_node) {
                Some(to_item) => {
                    let mut distances = Vec::with_capacity(to_neighbors.len());
                    for &neighbor_id in &to_neighbors {
                        if neighbor_id == from_node {
                            distances.push(Candidate::try_new(
                                neighbor_id,
                                D::distance(to_item.as_ref(), from_item),
                            )?);
                            continue;
                        }
                        let Some(neighbor_item) = items.get(&neighbor_id) else {
                            continue;
                        };
                        distances.push(Candidate::try_new(
                            neighbor_id,
                            D::distance(to_item.as_ref(), neighbor_item.as_ref()),
                        )?);
                    }
                    distances.sort();
                    to_neighbors = select_diverse(
                        to_item.as_ref(),
                        &distances,
                        &|node_id| items.get(&node_id).map(|item| item.as_ref()),
                        maximum_neighbors,
                    )?;
                }
                None => to_neighbors.truncate(maximum_neighbors),
            }
        }

        self.stage_neighbors_vec_for_mutation(txn, layer, to_node, to_neighbors, mutation_cache)
            .await?;
        let retained_row = MutationOpCache::<D>::node_row_id(layer, to_node);
        let NeighborRowValue::Present(retained_neighbors) = mutation_cache
            .neighbor(retained_row)
            .expect("staging installs canonical neighbors before reciprocal cleanup")
            .current()
        else {
            return Err(HelixDbError::InvariantViolation(
                "staged vector neighbor row cannot be absent".to_string(),
            ));
        };
        let retained_neighbors = retained_neighbors.clone();
        for rejected_neighbor in candidate_neighbors
            .into_iter()
            .filter(|neighbor| !retained_neighbors.contains(*neighbor))
        {
            self.remove_edge_from_neighbor(txn, layer, rejected_neighbor, to_node, mutation_cache)
                .await?;
        }
        Ok(())
    }

    /// Stages one complete HNSW deletion in a caller-owned measured write set.
    ///
    /// Lifecycle catch-up uses this boundary to measure deletion, optional
    /// replacement insertion, additive applied proof, and delta consumption as
    /// one indivisible transaction. The method preserves the deployed vector
    /// keys and row codecs; only ownership of measurement moves to the caller.
    /// A missing canonical item is treated as corrupt residue, not proof that
    /// deletion is complete: reverse locators, neighbor references, hot rows,
    /// SimHash, and entry-candidate state are still removed exhaustively.
    #[allow(
        dead_code,
        reason = "retained behind direct legacy and measured lifecycle mutation contracts"
    )]
    pub(crate) async fn stage_delete(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
    ) -> Result<(), HelixDbError> {
        let Some(mut metadata) = self.get_metadata(txn).await? else {
            return Err(HelixDbError::IndexNotFound(self.name().to_string()));
        };
        let degree_limits = MutationDegreeLimits::try_from_metadata(&metadata)?;
        let mut mutation_cache = MutationOpCache::<D>::with_degree_limits(
            degree_limits.layer0.get(),
            degree_limits.upper.get(),
        )?;
        let result = match self
            .stage_delete_with_metadata(txn, node_id, &mut metadata, &mut mutation_cache)
            .await
        {
            Ok(_) => self.flush_mutation_cache(txn, &mut mutation_cache).await,
            Err(error) => Err(error),
        };
        #[cfg(feature = "production-coverage")]
        crate::search::vector::record_benchmark_cache_stats(mutation_cache.stats);
        result
    }

    /// Reuses generation-qualified cache state for one builder-owned deletion.
    pub(crate) async fn stage_delete_with_build_session(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        session: &mut VectorBuildSession<D>,
    ) -> Result<(), HelixDbError> {
        let Some(mut metadata) = self.get_metadata(txn).await? else {
            return Err(HelixDbError::IndexNotFound(self.name().to_string()));
        };
        let degree_limits = MutationDegreeLimits::try_from_metadata(&metadata)?;
        let identity = self.build_session_identity()?.clone();
        let mut mutation_cache = session.take_cache(
            &identity,
            degree_limits.layer0.get(),
            degree_limits.upper.get(),
        )?;
        mutation_cache.begin_entity();
        let result = self
            .stage_delete_with_metadata(txn, node_id, &mut metadata, &mut mutation_cache)
            .await
            .map(|_| ());
        let changed = mutation_cache.finish_entity_changes();
        session.restore_cache(identity.clone(), mutation_cache);
        session.record_entity_changes(&identity, node_id, changed.into_keys());
        result
    }

    pub(in crate::search::vector) async fn stage_delete_with_metadata(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        metadata: &mut VectorIndexMetadata,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<bool, HelixDbError> {
        let item_existed = self
            .get_item_for_layer_cached(txn, 0, node_id, mutation_cache)
            .await?
            .is_some();

        self.repair_stale_entry_point_for_write(txn, metadata, "delete", node_id, mutation_cache)
            .await?;

        let node_max_layer = self
            .get_node_max_layer_cached(txn, node_id, metadata, mutation_cache)
            .await?;
        let degree_limits = MutationDegreeLimits::try_from_metadata(metadata)?;
        let maximum_upper_connections = degree_limits.upper.get();
        let maximum_layer0_connections = degree_limits.layer0.get();
        let rows = VectorWriteRows::new(txn, self.row_keyspace());
        let reverse_sources = rows.reverse_sources_for_target(node_id).await?;
        let mut layers_to_process = (0..=node_max_layer).collect::<BTreeSet<_>>();
        layers_to_process.extend(reverse_sources.sources_by_layer().keys().copied());

        let mut deleted_node_outgoing_by_layer = HashMap::<u16, Vec<NodeId>>::new();
        for layer in layers_to_process.iter().rev().copied() {
            let maximum_neighbors = if layer == 0 {
                maximum_layer0_connections
            } else {
                maximum_upper_connections
            };
            let outgoing_neighbors = self
                .delete_from_layer(
                    txn,
                    node_id,
                    layer,
                    maximum_neighbors,
                    reverse_sources.sources_at(layer),
                    mutation_cache,
                )
                .await?;
            deleted_node_outgoing_by_layer.insert(layer, outgoing_neighbors);
        }

        for (layer, neighbors) in deleted_node_outgoing_by_layer {
            for target_node_id in neighbors {
                rows.delete_reverse_locator(target_node_id, layer, node_id)?;
            }
        }
        rows.delete_reverse_sources(&reverse_sources)?;

        let (canonical_key, _) = self
            .resolve_canonical_vector_key_cached(
                txn,
                node_id,
                mutation_cache,
                "deleting canonical vector payload",
            )
            .await?;
        if let Some(canonical_key) = canonical_key {
            if self.simhash_directory_enabled() {
                rows.delete_simhash_directory_entry(&canonical_key)?;
            }
            rows.delete_canonical_vector(&canonical_key)?;
        }

        rows.delete_layer0_neighbors(node_id)?;
        for layer in 1..=node_max_layer {
            rows.delete_upper_neighbors(layer, node_id)?;
        }

        rows.delete_upper_vector(node_id)?;
        rows.delete_simhash(node_id)?;
        self.remove_entry_candidate(txn, node_id).await?;
        mutation_cache.invalidate_items(node_id);
        mutation_cache.invalidate_simhash(node_id);
        mutation_cache.put_simhash(node_id, None);
        mutation_cache.invalidate_neighbors(node_id);
        for layer in layers_to_process {
            let row = MutationOpCache::<D>::node_row_id(layer, node_id);
            mutation_cache.install_loaded_neighbor(row, NeighborRowValue::KnownAbsent);
            mutation_cache.record_neighbor_change(row, NeighborRowValue::KnownAbsent);
            mutation_cache.put_item(layer, node_id, None, 0);
        }

        if item_existed {
            // Pre-V2 point inserts did not maintain this advisory count, so a
            // legacy physical index can legitimately report zero while rows
            // remain. Saturation preserves compatibility; new V2 generations
            // start exact and therefore remain exact.
            metadata.count = metadata.count.saturating_sub(1);
        }
        if metadata.entry_point == Some(node_id) {
            if let Some((new_entry, new_max_layer)) = self
                .find_best_entry_candidate_cached(txn, mutation_cache)
                .await?
            {
                metadata.entry_point = Some(new_entry);
                metadata.max_layer = new_max_layer;
            } else {
                metadata.entry_point = None;
                metadata.max_layer = 0;
            }
        }
        if item_existed {
            self.update_metadata(txn, metadata).await?;
        }

        Ok(item_existed)
    }

    /// Finds the highest layer containing a node for production coverage contracts.
    #[cfg(feature = "production-coverage")]
    pub(in crate::search::vector) async fn get_node_max_layer(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        metadata: &VectorIndexMetadata,
    ) -> Result<u16, HelixDbError> {
        let mut mutation_cache = MutationOpCache::default();
        self.get_node_max_layer_cached(txn, node_id, metadata, &mut mutation_cache)
            .await
    }

    /// Resolves a deletion layer through reusable neighbor rows.
    async fn get_node_max_layer_cached(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        metadata: &VectorIndexMetadata,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<u16, HelixDbError> {
        let Some(candidate_layer) = self.get_entry_candidate_layer(txn, node_id).await? else {
            for layer in (1..=metadata.max_layer).rev() {
                self.load_neighbors_for_mutation(txn, layer, node_id, mutation_cache)
                    .await?;
                let row = MutationOpCache::<D>::node_row_id(layer, node_id);
                if matches!(
                    mutation_cache.neighbor(row).map(CachedNeighbor::current),
                    Some(NeighborRowValue::Present(_))
                ) {
                    return Ok(layer);
                }
            }
            return Ok(0);
        };
        Ok(candidate_layer)
    }

    /// Removes one node from a layer and relinks every affected source.
    ///
    /// Outgoing neighbors and reverse-locator-only sources are combined so
    /// asymmetric residue is repaired rather than skipped. The deleted node is
    /// removed first, then candidates are collected from the remaining local
    /// neighborhoods before Algorithm 2 relinking is staged.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::search::vector) async fn delete_from_layer(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        layer: u16,
        maximum_neighbors: usize,
        extra_sources: &[NodeId],
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<Vec<NodeId>, HelixDbError> {
        let outgoing_neighbors = self
            .load_neighbors_for_mutation(txn, layer, node_id, mutation_cache)
            .await?;
        let mandatory_relink = outgoing_neighbors
            .iter()
            .copied()
            .filter(|neighbor_id| *neighbor_id != node_id)
            .collect::<BTreeSet<_>>();
        let mut affected_sources = mandatory_relink.clone();
        affected_sources.extend(
            extra_sources
                .iter()
                .copied()
                .filter(|source_id| *source_id != node_id),
        );
        if affected_sources.is_empty() {
            return Ok(outgoing_neighbors);
        }

        let mut relink_sources = mandatory_relink;
        for neighbor_id in affected_sources {
            if self
                .remove_edge_from_neighbor(txn, layer, neighbor_id, node_id, mutation_cache)
                .await?
            {
                relink_sources.insert(neighbor_id);
            }
        }
        if relink_sources.is_empty() {
            return Ok(outgoing_neighbors);
        }
        let relink_sources = relink_sources.into_iter().collect::<Vec<_>>();
        let mut candidates = relink_sources
            .iter()
            .copied()
            .filter(|candidate| *candidate != node_id)
            .collect::<HashSet<_>>();
        for &neighbor_id in &relink_sources {
            let neighbors = self
                .load_neighbors_for_mutation(txn, layer, neighbor_id, mutation_cache)
                .await?;
            candidates.extend(
                neighbors
                    .into_iter()
                    .filter(|candidate| *candidate != node_id && *candidate != neighbor_id),
            );
        }
        // A source without an item relinks nothing, so candidates are read
        // only once some source can use them.
        if self
            .get_items_for_layer_cached_batch(txn, layer, &relink_sources, mutation_cache)
            .await?
            .is_empty()
        {
            return Ok(outgoing_neighbors);
        }
        let candidates = self
            .load_relink_candidates(txn, layer, &candidates, mutation_cache)
            .await?;
        for &neighbor_id in &relink_sources {
            self.relink_neighbor(
                txn,
                neighbor_id,
                &candidates,
                maximum_neighbors,
                mutation_cache,
            )
            .await?;
        }
        Ok(outgoing_neighbors)
    }

    /// Loads every relink candidate of one layer with a single batched read.
    ///
    /// Each deletion relinks ~M sources against the same candidates, so one
    /// load replaces a cache lookup per source and candidate pair.
    pub(in crate::search::vector) async fn load_relink_candidates(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        candidates: &HashSet<NodeId>,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<RelinkCandidates<D>, HelixDbError> {
        let node_ids = candidates.iter().copied().collect::<Vec<_>>();
        let mut loaded = self
            .get_items_for_layer_cached_batch(txn, layer, &node_ids, mutation_cache)
            .await?;
        Ok(RelinkCandidates {
            layer,
            items: node_ids
                .into_iter()
                .map(|node_id| (node_id, loaded.remove(&node_id)))
                .collect(),
        })
    }

    /// Returns a candidate's hydrated item, reading through the mutation cache
    /// only for nodes outside the candidate set.
    async fn relink_item(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        candidates: &RelinkCandidates<D>,
        node_id: NodeId,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<Option<Arc<Item<'static, D>>>, HelixDbError> {
        let Some(item) = candidates.items.get(&node_id) else {
            return self
                .get_item_for_layer_cached(txn, candidates.layer, node_id, mutation_cache)
                .await;
        };
        Ok(item.clone())
    }

    /// Removes one reciprocal reference and stages the row only when changed.
    pub(in crate::search::vector) async fn remove_edge_from_neighbor(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        neighbor_id: NodeId,
        node_to_remove: NodeId,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<bool, HelixDbError> {
        let mut neighbors = self
            .load_neighbors_for_mutation(txn, layer, neighbor_id, mutation_cache)
            .await?;
        if !neighbors.contains(&node_to_remove) {
            return Ok(false);
        }
        neighbors.retain(|neighbor| *neighbor != node_to_remove);
        self.stage_neighbors_vec_for_mutation(txn, layer, neighbor_id, neighbors, mutation_cache)
            .await?;
        Ok(true)
    }

    /// Relinks one affected source after a node is removed from an HNSW layer.
    ///
    /// Algorithm 2 candidates are ranked against the source vector, merged with
    /// retained neighbors, diversity-pruned to the layer degree, and staged.
    /// Every newly selected connection is then inserted and independently
    /// pruned on the reciprocal row before the operation-level flush.
    pub(in crate::search::vector) async fn relink_neighbor(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        neighbor_id: NodeId,
        candidates: &RelinkCandidates<D>,
        maximum_neighbors: usize,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<(), HelixDbError> {
        let layer = candidates.layer;
        let Some(neighbor_item) = self
            .relink_item(txn, candidates, neighbor_id, mutation_cache)
            .await?
        else {
            return Ok(());
        };
        let old_neighbors = self
            .load_neighbors_for_mutation(txn, layer, neighbor_id, mutation_cache)
            .await?;
        let mut current_neighbors = old_neighbors.clone();
        let mut nearest = candidates
            .items
            .iter()
            .filter(|(candidate_id, _)| **candidate_id != neighbor_id)
            .filter_map(|(candidate_id, item)| {
                item.as_ref().map(|item| {
                    Candidate::try_new(
                        *candidate_id,
                        D::distance(neighbor_item.as_ref(), item.as_ref()),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Candidates have distinct node IDs and `Candidate` orders by distance
        // then node ID, so the partitioned prefix holds exactly the nearest
        // `maximum_neighbors` a full sort would keep.
        if nearest.len() > maximum_neighbors {
            nearest.select_nth_unstable(maximum_neighbors);
            nearest.truncate(maximum_neighbors);
        }
        nearest.sort_unstable();
        for candidate in &nearest {
            if !current_neighbors.contains(&candidate.node_id) {
                current_neighbors.push(candidate.node_id);
            }
        }

        if current_neighbors.len() > maximum_neighbors {
            let mut distances = Vec::new();
            let mut items = HashMap::<NodeId, Arc<Item<'static, D>>>::new();
            for &node_id in &current_neighbors {
                let Some(item) = self
                    .relink_item(txn, candidates, node_id, mutation_cache)
                    .await?
                else {
                    continue;
                };
                distances.push(Candidate::try_new(
                    node_id,
                    D::distance(neighbor_item.as_ref(), item.as_ref()),
                )?);
                items.insert(node_id, item);
            }
            distances.sort();
            current_neighbors = select_diverse(
                neighbor_item.as_ref(),
                &distances,
                &|node_id| items.get(&node_id).map(|item| item.as_ref()),
                maximum_neighbors,
            )?;
        }

        self.stage_neighbors_for_mutation(
            txn,
            layer,
            neighbor_id,
            &current_neighbors,
            mutation_cache,
        )
        .await?;
        for &new_neighbor_id in &current_neighbors {
            if old_neighbors.contains(&new_neighbor_id) {
                continue;
            }
            let mut reverse_neighbors = self
                .load_neighbors_for_mutation(txn, layer, new_neighbor_id, mutation_cache)
                .await?;
            if reverse_neighbors.contains(&neighbor_id) {
                continue;
            }
            reverse_neighbors.push(neighbor_id);
            if reverse_neighbors.len() > maximum_neighbors {
                let Some(reverse_item) = self
                    .relink_item(txn, candidates, new_neighbor_id, mutation_cache)
                    .await?
                else {
                    self.stage_neighbors_vec_for_mutation(
                        txn,
                        layer,
                        new_neighbor_id,
                        reverse_neighbors,
                        mutation_cache,
                    )
                    .await?;
                    continue;
                };
                let mut reverse_distances = Vec::new();
                let mut items = HashMap::<NodeId, Arc<Item<'static, D>>>::new();
                for &node_id in &reverse_neighbors {
                    let Some(item) = self
                        .relink_item(txn, candidates, node_id, mutation_cache)
                        .await?
                    else {
                        continue;
                    };
                    reverse_distances.push(Candidate::try_new(
                        node_id,
                        D::distance(reverse_item.as_ref(), item.as_ref()),
                    )?);
                    items.insert(node_id, item);
                }
                reverse_distances.sort();
                reverse_neighbors = select_diverse(
                    reverse_item.as_ref(),
                    &reverse_distances,
                    &|node_id| items.get(&node_id).map(|item| item.as_ref()),
                    maximum_neighbors,
                )?;
            }
            self.stage_neighbors_vec_for_mutation(
                txn,
                layer,
                new_neighbor_id,
                reverse_neighbors,
                mutation_cache,
            )
            .await?;
        }
        Ok(())
    }
}

/// Relink candidates of one layer with their items, loaded once per deletion.
///
/// Every candidate maps to its item, or to `None` when it has no item at the
/// layer, so a lookup of a candidate never reads again.
pub(in crate::search::vector) struct RelinkCandidates<D: Distance> {
    layer: u16,
    items: HashMap<NodeId, Option<Arc<Item<'static, D>>>>,
}

/// Physical HNSW layer bound into one cached neighbor-row identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::search::vector) struct HnswLayer(u16);

impl HnswLayer {
    /// Wraps a layer decoded from the deployed key without changing its value.
    pub(in crate::search::vector) const fn from_deployed(layer: u16) -> Self {
        Self(layer)
    }

    /// Returns the deployed layer number.
    pub(in crate::search::vector) const fn number(self) -> u16 {
        self.0
    }
}

/// Complete identity of one operation-local neighbor row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::search::vector) struct NeighborRowId {
    layer: HnswLayer,
    entity: VectorEntityId,
}

impl NeighborRowId {
    /// Binds one layer to the descriptor-proven node or edge identity.
    pub(in crate::search::vector) const fn new(layer: HnswLayer, entity: VectorEntityId) -> Self {
        Self { layer, entity }
    }

    /// Returns the row's physical layer.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(in crate::search::vector) const fn layer(self) -> HnswLayer {
        self.layer
    }

    /// Returns the row's descriptor-proven entity identity.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(in crate::search::vector) const fn entity(self) -> VectorEntityId {
        self.entity
    }

    /// Returns the deployed layer and local entity ID used by row storage.
    pub(in crate::search::vector) const fn storage_parts(self) -> (u16, u64) {
        let entity_id = match self.entity {
            VectorEntityId::Node(node_id) => node_id,
            VectorEntityId::Edge(edge_id) => edge_id,
        };
        (self.layer.number(), entity_id)
    }
}

/// Monotonic operation-local recency assigned on every cache touch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::search::vector) struct CacheSequence(u64);

impl CacheSequence {
    /// Creates the first sequence in a new operation-local cache.
    pub(in crate::search::vector) const fn initial() -> Self {
        Self(0)
    }

    /// Advances recency or reports that bounded renumbering is required.
    pub(in crate::search::vector) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

/// Authoritative decoded value of one loaded neighbor row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::search::vector) enum NeighborRowValue {
    /// Storage proved that the row does not currently exist.
    KnownAbsent,
    /// Storage returned one validated canonical neighbor set.
    Present(NeighborSet),
}

/// Closed write state for one loaded neighbor row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::search::vector) enum NeighborCacheState {
    /// The current value agrees with the transaction's storage view.
    Clean { current: NeighborRowValue },
    /// The first loaded value and latest staged value are retained together.
    Dirty {
        original: NeighborRowValue,
        current: NeighborRowValue,
    },
}

/// One neighbor row with authoritative state and bounded-scan recency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::search::vector) struct CachedNeighbor {
    state: NeighborCacheState,
    last_touch: CacheSequence,
}

/// One decoded item or negative lookup retained with global session recency.
#[derive(Debug, Clone)]
struct CachedItem<D: Distance> {
    value: Option<Arc<Item<'static, D>>>,
    payload_bytes: usize,
    last_touch: CacheSequence,
}

/// One SimHash or negative lookup retained with global session recency.
#[derive(Debug, Clone, Copy)]
struct CachedSimHash {
    value: Option<crate::search::vector::SimHash>,
    last_touch: CacheSequence,
}

/// Proof that one row was allocated fresh in the current mutation session.
///
/// The field and constructor remain private to this module, so ordinary staging
/// cannot manufacture an absent original value for an unloaded existing row.
pub(in crate::search::vector) struct NewNeighborRowProof {
    row: NeighborRowId,
}

impl CachedNeighbor {
    /// Installs a storage-proven clean row in the operation cache.
    pub(in crate::search::vector) const fn clean(
        current: NeighborRowValue,
        last_touch: CacheSequence,
    ) -> Self {
        Self {
            state: NeighborCacheState::Clean { current },
            last_touch,
        }
    }

    /// Returns the latest authoritative value used by graph mutation.
    pub(in crate::search::vector) const fn current(&self) -> &NeighborRowValue {
        match &self.state {
            NeighborCacheState::Clean { current } | NeighborCacheState::Dirty { current, .. } => {
                current
            }
        }
    }

    /// Returns the first storage value only while a write remains pending.
    pub(in crate::search::vector) const fn original(&self) -> Option<&NeighborRowValue> {
        match &self.state {
            NeighborCacheState::Clean { .. } => None,
            NeighborCacheState::Dirty { original, .. } => Some(original),
        }
    }

    /// Returns whether this row has a pending staged value.
    pub(in crate::search::vector) const fn is_dirty(&self) -> bool {
        matches!(self.state, NeighborCacheState::Dirty { .. })
    }

    /// Stages a new value while preserving the first pre-mutation snapshot.
    pub(in crate::search::vector) fn stage(
        &mut self,
        staged: NeighborRowValue,
        last_touch: CacheSequence,
    ) {
        let previous = core::mem::replace(
            &mut self.state,
            NeighborCacheState::Clean {
                current: NeighborRowValue::KnownAbsent,
            },
        );
        self.state = match previous {
            NeighborCacheState::Clean { current } => NeighborCacheState::Dirty {
                original: current,
                current: staged,
            },
            NeighborCacheState::Dirty { original, .. } => NeighborCacheState::Dirty {
                original,
                current: staged,
            },
        };
        self.last_touch = last_touch;
    }

    /// Marks a successfully flushed row clean without changing its value.
    pub(in crate::search::vector) fn mark_flushed(&mut self) {
        let previous = core::mem::replace(
            &mut self.state,
            NeighborCacheState::Clean {
                current: NeighborRowValue::KnownAbsent,
            },
        );
        self.state = match previous {
            NeighborCacheState::Clean { current } | NeighborCacheState::Dirty { current, .. } => {
                NeighborCacheState::Clean { current }
            }
        };
    }

    /// Returns the recency used by bounded oldest-clean selection.
    pub(in crate::search::vector) const fn last_touch(&self) -> CacheSequence {
        self.last_touch
    }
}

/// A cached value stamped with the touch that last used it.
trait Touched {
    /// Returns the touch that last used this value.
    fn last_touch(&self) -> CacheSequence;

    /// Stamps this value as used by `touch`.
    fn set_last_touch(&mut self, touch: CacheSequence);
}

impl<D: Distance> Touched for CachedItem<D> {
    fn last_touch(&self) -> CacheSequence {
        self.last_touch
    }

    fn set_last_touch(&mut self, touch: CacheSequence) {
        self.last_touch = touch;
    }
}

impl Touched for CachedSimHash {
    fn last_touch(&self) -> CacheSequence {
        self.last_touch
    }

    fn set_last_touch(&mut self, touch: CacheSequence) {
        self.last_touch = touch;
    }
}

impl Touched for CachedNeighbor {
    fn last_touch(&self) -> CacheSequence {
        self.last_touch
    }

    fn set_last_touch(&mut self, touch: CacheSequence) {
        self.last_touch = touch;
    }
}

/// Cached values of one kind, ordered from least to most recently used.
///
/// A touch appends `(touch, key)` to the order instead of moving the key's
/// earlier entry, which goes stale: an entry is current exactly while its
/// value still records `touch`. The order stays sorted by `(touch, key)`, its
/// front stays current, and it holds at most two entries per value in an
/// allocation of at most four per value (four while empty). So
/// [`Self::oldest`] returns what the first element of an ordered set of
/// current entries would, and a touch costs a hash update and an append,
/// amortized, instead of an ordered-set removal and insertion. Stale entries
/// leave the front as they reach it, and all at once when the order would
/// outgrow either bound.
#[derive(Debug)]
struct RecencyMap<K, V> {
    values: foldhash::HashMap<K, V>,
    order: VecDeque<(CacheSequence, K)>,
}

impl<K, V> Default for RecencyMap<K, V> {
    fn default() -> Self {
        Self {
            values: foldhash::HashMap::default(),
            order: VecDeque::new(),
        }
    }
}

impl<K: Copy + Ord + core::hash::Hash, V: Touched> RecencyMap<K, V> {
    /// Returns a cached value without touching it.
    fn get(&self, key: &K) -> Option<&V> {
        self.values.get(key)
    }

    /// Returns whether `key` is cached.
    fn contains_key(&self, key: &K) -> bool {
        self.values.contains_key(key)
    }

    /// Returns the number of cached values.
    fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns every cached value in arbitrary order.
    fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.values.iter()
    }

    /// Returns the least recently used key and its touch.
    fn oldest(&self) -> Option<(CacheSequence, K)> {
        self.order.front().copied()
    }

    /// Returns every cached value from least to most recently used.
    fn oldest_first(&self) -> impl Iterator<Item = (K, &V)> {
        self.order.iter().filter_map(|&(touch, key)| {
            self.values
                .get(&key)
                .filter(|value| value.last_touch() == touch)
                .map(|value| (key, value))
        })
    }

    /// Stamps the cached value of `key` with `touch` and returns it.
    fn touch(&mut self, key: K, touch: CacheSequence) -> Option<&V> {
        let value = self.values.get_mut(&key)?;
        if value.last_touch() != touch {
            value.set_last_touch(touch);
            self.record(touch, key);
        }
        self.values.get(&key)
    }

    /// Caches `value` at the touch it records, returning the value it replaces.
    ///
    /// A replacement stamped with the replaced value's touch keeps its place.
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        let touch = value.last_touch();
        let replaced = self.values.insert(key, value);
        if replaced
            .as_ref()
            .is_none_or(|replaced| replaced.last_touch() != touch)
        {
            self.record(touch, key);
        }
        replaced
    }

    /// Removes the cached value of `key`.
    fn remove(&mut self, key: &K) -> Option<V> {
        let removed = self.values.remove(key)?;
        self.settle(*key);
        Some(removed)
    }

    /// Restamps every value with `renumbered`, which must keep their order.
    fn renumber(&mut self, mut renumbered: impl FnMut(&K) -> CacheSequence) {
        for (key, value) in &mut self.values {
            value.set_last_touch(renumbered(key));
        }
        let mut order = self
            .values
            .iter()
            .map(|(key, value)| (value.last_touch(), *key))
            .collect::<Vec<_>>();
        order.sort_unstable();
        self.order = order.into();
    }

    /// Orders `key` as last used at `touch`, which its value already records.
    fn record(&mut self, touch: CacheSequence, key: K) {
        let entry = (touch, key);
        // Touches grow within a namespace unless another namespace of its
        // session renumbered the shared sequence; the order then still sorts.
        match self.order.back() {
            Some(newest) if *newest > entry => {
                let position = self.order.partition_point(|queued| *queued < entry);
                self.order.insert(position, entry);
            }
            _ => self.order.push_back(entry),
        }
        self.settle(key);
    }

    /// Restores the order's invariants after `key` was touched or removed.
    ///
    /// Only entries of `key` went stale, so a front of another key is current.
    /// Compaction looks up each entry once, and only once stale entries
    /// outnumber current ones or removals left the allocation above four
    /// entries per value. It then shrinks an oversized allocation to twice the
    /// values plus one, which the next compaction fits without growing. A
    /// shrink needs about half the values removed, or a regrowth after as many
    /// appends as values, so settling costs `O(1)` amortized.
    fn settle(&mut self, key: K) {
        let capacity_bound = self.values.len().max(1).saturating_mul(4);
        if self.order.len() > self.values.len().saturating_mul(2)
            || self.order.capacity() > capacity_bound
        {
            let values = &self.values;
            self.order.retain(|(touch, key)| {
                values
                    .get(key)
                    .is_some_and(|value| value.last_touch() == *touch)
            });
            if self.order.capacity() > capacity_bound {
                self.order
                    .shrink_to(self.values.len().saturating_mul(2).saturating_add(1));
            }
            return;
        }
        if self.order.front().is_none_or(|(_, oldest)| *oldest != key) {
            return;
        }
        while let Some((touch, oldest)) = self.order.front() {
            if self
                .values
                .get(oldest)
                .is_some_and(|value| value.last_touch() == *touch)
            {
                return;
            }
            self.order.pop_front();
        }
    }
}

/// Entry whose recency one cache renumbering rewrites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MutationCacheTouchTarget {
    Item { layer: u16, node_id: NodeId },
    Neighbor(NeighborRowId),
    SimHash(NodeId),
}

/// Operation-local canonical neighbor and item state for one HNSW mutation.
///
/// The cache validates layer limits once, retains the first pre-mutation
/// snapshot for linear reverse-edge differences, and never changes persisted
/// row codecs. Dirty entries are encoded only at the existing flush boundary.
#[derive(Debug)]
pub(in crate::search::vector) struct MutationOpCache<D: Distance> {
    neighbor_rows: RecencyMap<NeighborRowId, CachedNeighbor>,
    /// Dirty rows by recency. Flushing a row changes its state but keeps its
    /// recency, so this stays an ordered set rather than an append order.
    dirty_neighbor_recency: BTreeSet<(CacheSequence, NeighborRowId)>,
    items: RecencyMap<(u16, NodeId), CachedItem<D>>,
    simhashes: RecencyMap<NodeId, CachedSimHash>,
    retained_payload_bytes: usize,
    degree_limits: NeighborDegreeLimits,
    next_touch: CacheSequence,
    stats: VectorBuildSessionStats,
    enforce_local_limits: bool,
    entity_changed_neighbors: BTreeMap<NeighborRowId, NeighborRowValue>,
    /// Highest layer ever installed in `items`; bounds targeted invalidation.
    max_item_layer: u16,
    /// Highest layer ever installed in `neighbor_rows`; bounds targeted invalidation.
    max_neighbor_layer: u16,
}

impl<D: Distance> Default for MutationOpCache<D> {
    fn default() -> Self {
        Self::with_degree_limits(usize::MAX, usize::MAX)
            .expect("maximum test compatibility degree limits are non-zero")
    }
}

impl<D: Distance> MutationOpCache<D> {
    /// Creates an operation-local cache with validated final layer degrees.
    pub(in crate::search::vector) fn with_degree_limits(
        layer0: usize,
        upper: usize,
    ) -> Result<Self, HelixDbError> {
        Ok(Self {
            neighbor_rows: RecencyMap::default(),
            dirty_neighbor_recency: BTreeSet::new(),
            items: RecencyMap::default(),
            simhashes: RecencyMap::default(),
            retained_payload_bytes: 0,
            degree_limits: NeighborDegreeLimits::try_new(layer0, upper)
                .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?,
            next_touch: CacheSequence::initial(),
            stats: VectorBuildSessionStats::default(),
            enforce_local_limits: true,
            entity_changed_neighbors: BTreeMap::new(),
            max_item_layer: 0,
            max_neighbor_layer: 0,
        })
    }

    /// Transfers limit ownership to the complete reusable build session.
    fn into_build_session_cache(mut self) -> Self {
        self.enforce_local_limits = false;
        self
    }

    /// Returns whether one-operation eviction remains authoritative.
    pub(in crate::search::vector) const fn enforces_local_limits(&self) -> bool {
        self.enforce_local_limits
    }

    /// Starts one entity boundary for explicit Serializable Snapshot read intent.
    pub(in crate::search::vector) fn begin_entity(&mut self) {
        self.entity_changed_neighbors.clear();
    }

    /// Records that one canonical row changed logically during this entity.
    pub(in crate::search::vector) fn record_neighbor_change(
        &mut self,
        row: NeighborRowId,
        original: NeighborRowValue,
    ) {
        self.entity_changed_neighbors.entry(row).or_insert(original);
    }

    /// Finishes one entity and returns every canonical row that changed.
    pub(in crate::search::vector) fn finish_entity_changes(
        &mut self,
    ) -> BTreeMap<NeighborRowId, NeighborRowValue> {
        core::mem::take(&mut self.entity_changed_neighbors)
    }

    fn replace_retained_payload(
        &mut self,
        previous: usize,
        next: usize,
    ) -> Result<(), HelixDbError> {
        let Some(retained_payload_bytes) = self
            .retained_payload_bytes
            .checked_sub(previous)
            .and_then(|remaining| remaining.checked_add(next))
        else {
            return Err(HelixDbError::InvariantViolation(
                "vector mutation cache payload accounting overflowed".to_string(),
            ));
        };
        self.retained_payload_bytes = retained_payload_bytes;
        self.stats.max_retained_payload_bytes = self
            .stats
            .max_retained_payload_bytes
            .max(u64::try_from(retained_payload_bytes).unwrap_or(u64::MAX));
        #[cfg(feature = "production-coverage")]
        crate::search::vector::observe_benchmark_retained_payload(
            u64::try_from(retained_payload_bytes).unwrap_or(u64::MAX),
        );
        Ok(())
    }

    /// Returns the validated final degree for one physical layer.
    pub(in crate::search::vector) fn degree_limit(&self, layer: u16) -> NeighborDegreeLimit {
        self.degree_limits.for_layer(layer)
    }

    /// Returns the node-row identity used by the current node-only HNSW core.
    pub(in crate::search::vector) const fn node_row_id(
        layer: u16,
        node_id: NodeId,
    ) -> NeighborRowId {
        NeighborRowId::new(
            HnswLayer::from_deployed(layer),
            VectorEntityId::Node(node_id),
        )
    }

    /// Returns the current state for a previously loaded row.
    pub(in crate::search::vector) fn neighbor(
        &self,
        row: NeighborRowId,
    ) -> Option<&CachedNeighbor> {
        self.neighbor_rows.get(&row)
    }

    /// Returns and touches one authoritative neighbor row.
    pub(in crate::search::vector) fn touched_neighbor(
        &mut self,
        row: NeighborRowId,
    ) -> Option<&CachedNeighbor> {
        if !self.neighbor_rows.contains_key(&row) {
            self.stats.neighbor_misses = self.stats.neighbor_misses.saturating_add(1);
            return None;
        }
        self.stats.neighbor_hits = self.stats.neighbor_hits.saturating_add(1);
        let touch = self.take_touch();
        let cached = self
            .neighbor_rows
            .get(&row)
            .expect("contained vector neighbor row remains cached");
        if cached.is_dirty() {
            assert!(
                self.dirty_neighbor_recency
                    .remove(&(cached.last_touch(), row)),
                "dirty vector neighbor retains one recency entry"
            );
            assert!(
                self.dirty_neighbor_recency.insert((touch, row)),
                "vector neighbor recency is unique"
            );
        }
        self.neighbor_rows.touch(row, touch)
    }

    /// Returns whether a row is loaded, independently of whether it exists.
    pub(in crate::search::vector) fn contains_neighbor(&self, row: NeighborRowId) -> bool {
        self.neighbor_rows.contains_key(&row)
    }

    /// Installs one storage-proven row unless staging already owns it.
    pub(in crate::search::vector) fn install_loaded_neighbor(
        &mut self,
        row: NeighborRowId,
        value: NeighborRowValue,
    ) -> bool {
        if self.neighbor_rows.contains_key(&row) {
            return false;
        }
        let touch = self.take_touch();
        let cached = CachedNeighbor::clean(value, touch);
        let payload_bytes = cached_neighbor_payload_bytes(row, &cached)
            .expect("validated vector neighbor cache state has measurable payload");
        self.replace_retained_payload(0, payload_bytes)
            .expect("bounded vector neighbor cache payload cannot overflow");
        assert!(self.neighbor_rows.insert(row, cached).is_none());
        self.max_neighbor_layer = self.max_neighbor_layer.max(row.layer.number());
        true
    }

    /// Stages a row that must already have storage-proven cache state.
    pub(in crate::search::vector) fn stage_loaded_neighbor(
        &mut self,
        row: NeighborRowId,
        value: NeighborRowValue,
    ) -> Result<(), HelixDbError> {
        let touch = self.take_touch();
        let Some(previous) = self.neighbor_rows.get(&row) else {
            return Err(HelixDbError::InvariantViolation(
                "cannot stage an unloaded vector neighbor row without new-row proof".to_string(),
            ));
        };
        let (last_touch, dirty) = (previous.last_touch(), previous.is_dirty());
        let changed = previous.current() != &value;
        let boundary_original = previous.current().clone();
        let previous_payload = cached_neighbor_payload_bytes(row, previous)?;
        let mut staged = previous.clone();
        if changed {
            self.record_neighbor_change(row, boundary_original);
        }
        staged.stage(value, touch);
        let staged_payload = cached_neighbor_payload_bytes(row, &staged)?;
        self.replace_retained_payload(previous_payload, staged_payload)?;
        if dirty {
            assert!(
                self.dirty_neighbor_recency.remove(&(last_touch, row)),
                "dirty vector neighbor retains one recency entry"
            );
        }
        assert!(
            self.dirty_neighbor_recency.insert((touch, row)),
            "vector neighbor recency is unique"
        );
        let replaced = self
            .neighbor_rows
            .insert(row, staged)
            .expect("loaded vector neighbor remains cached while staging");
        debug_assert_eq!(replaced.last_touch(), last_touch);
        Ok(())
    }

    /// Issues the unforgeable token used only after allocating a fresh row.
    pub(in crate::search::vector) fn prove_new_neighbor_row(
        &self,
        row: NeighborRowId,
    ) -> Result<NewNeighborRowProof, HelixDbError> {
        if self.neighbor_rows.contains_key(&row) {
            return Err(HelixDbError::InvariantViolation(
                "cannot prove an already loaded vector neighbor row is new".to_string(),
            ));
        }
        Ok(NewNeighborRowProof { row })
    }

    /// Stages a freshly allocated row with a proven absent original value.
    pub(in crate::search::vector) fn stage_new_neighbor(
        &mut self,
        proof: NewNeighborRowProof,
        value: NeighborRowValue,
    ) {
        self.record_neighbor_change(proof.row, NeighborRowValue::KnownAbsent);
        let touch = self.take_touch();
        let mut cached = CachedNeighbor::clean(NeighborRowValue::KnownAbsent, touch);
        cached.stage(value, touch);
        let payload_bytes = cached_neighbor_payload_bytes(proof.row, &cached)
            .expect("validated new vector neighbor cache state has measurable payload");
        self.replace_retained_payload(0, payload_bytes)
            .expect("bounded vector neighbor cache payload cannot overflow");
        assert!(self.neighbor_rows.insert(proof.row, cached).is_none());
        self.max_neighbor_layer = self.max_neighbor_layer.max(proof.row.layer.number());
        assert!(
            self.dirty_neighbor_recency.insert((touch, proof.row)),
            "vector neighbor recency is unique"
        );
    }

    /// Removes one row after clean eviction or a successful evicting flush.
    pub(in crate::search::vector) fn remove_neighbor(
        &mut self,
        row: NeighborRowId,
    ) -> Option<CachedNeighbor> {
        let cached = self.neighbor_rows.get(&row)?;
        let payload_bytes = cached_neighbor_payload_bytes(row, cached)
            .expect("validated vector neighbor cache state has measurable payload");
        self.replace_retained_payload(payload_bytes, 0)
            .expect("retained vector neighbor payload covers every cached row");
        let removed = self
            .neighbor_rows
            .remove(&row)
            .expect("selected vector neighbor remains cached while removing");
        if removed.is_dirty() {
            assert!(
                self.dirty_neighbor_recency
                    .remove(&(removed.last_touch(), row)),
                "dirty vector neighbor retains one recency entry"
            );
        }
        Some(removed)
    }

    /// Marks one successfully flushed row clean while preserving global recency.
    pub(in crate::search::vector) fn mark_neighbor_flushed(&mut self, row: NeighborRowId) {
        let Some(previous) = self
            .neighbor_rows
            .get(&row)
            .filter(|cached| cached.is_dirty())
        else {
            return;
        };
        let last_touch = previous.last_touch();
        let previous_payload = cached_neighbor_payload_bytes(row, previous)
            .expect("validated dirty vector neighbor has measurable payload");
        let mut flushed = previous.clone();
        flushed.mark_flushed();
        let flushed_payload = cached_neighbor_payload_bytes(row, &flushed)
            .expect("validated flushed vector neighbor has measurable payload");
        self.replace_retained_payload(previous_payload, flushed_payload)
            .expect("flushing a vector neighbor cannot overflow cache payload");
        assert!(
            self.dirty_neighbor_recency.remove(&(last_touch, row)),
            "dirty vector neighbor retains one recency entry"
        );
        let replaced = self
            .neighbor_rows
            .insert(row, flushed)
            .expect("flushed vector neighbor remains cached");
        debug_assert!(replaced.is_dirty());
    }

    /// Removes every layer-specific neighbor state for one entity.
    ///
    /// Rows are addressed directly for every layer up to the highest one ever
    /// installed, so the cost is independent of the cache size. Both entity
    /// identities sharing the local ID are removed, matching the storage-ID
    /// equality this method has always used.
    pub(in crate::search::vector) fn invalidate_neighbors(&mut self, node_id: NodeId) {
        for layer in 0..=self.max_neighbor_layer {
            for entity in [VectorEntityId::Node(node_id), VectorEntityId::Edge(node_id)] {
                let row = NeighborRowId::new(HnswLayer::from_deployed(layer), entity);
                if self.neighbor_rows.contains_key(&row) {
                    self.entity_changed_neighbors.remove(&row);
                    self.remove_neighbor(row);
                }
            }
        }
    }

    /// Returns the number of loaded neighbor rows.
    pub(in crate::search::vector) fn neighbor_count(&self) -> usize {
        self.neighbor_rows.len()
    }

    /// Returns and touches one decoded item or negative lookup.
    pub(in crate::search::vector) fn item(
        &mut self,
        layer: u16,
        node_id: NodeId,
    ) -> Option<Option<Arc<Item<'static, D>>>> {
        let key = (layer, node_id);
        if !self.items.contains_key(&key) {
            self.stats.item_misses = self.stats.item_misses.saturating_add(1);
            return None;
        }
        self.stats.item_hits = self.stats.item_hits.saturating_add(1);
        let touch = self.take_touch();
        self.items
            .touch(key, touch)
            .map(|cached| cached.value.clone())
    }

    /// Installs or replaces one decoded item state.
    pub(in crate::search::vector) fn put_item(
        &mut self,
        layer: u16,
        node_id: NodeId,
        value: Option<Arc<Item<'static, D>>>,
        payload_bytes: usize,
    ) {
        let previous_payload = self
            .items
            .get(&(layer, node_id))
            .map_or(0, |cached| cached.payload_bytes);
        self.replace_retained_payload(previous_payload, payload_bytes)
            .expect("bounded vector item cache payload cannot overflow");
        let touch = self.take_touch();
        self.max_item_layer = self.max_item_layer.max(layer);
        self.items.insert(
            (layer, node_id),
            CachedItem {
                value,
                payload_bytes,
                last_touch: touch,
            },
        );
    }

    /// Removes every layer-specific item state for one entity.
    ///
    /// Items are addressed directly for every layer up to the highest one ever
    /// installed, so the cost is independent of the cache size.
    pub(in crate::search::vector) fn invalidate_items(&mut self, node_id: NodeId) {
        for layer in 0..=self.max_item_layer {
            self.remove_item(layer, node_id);
        }
    }

    /// Removes the item entry paired with one evicted neighbor row.
    pub(in crate::search::vector) fn remove_item(&mut self, layer: u16, node_id: NodeId) {
        let Some(removed) = self.items.remove(&(layer, node_id)) else {
            return;
        };
        self.replace_retained_payload(removed.payload_bytes, 0)
            .expect("retained vector item payload covers every cached item");
    }

    /// Returns the number of retained decoded item states.
    pub(in crate::search::vector) fn item_count(&self) -> usize {
        self.items.len()
    }

    /// Reports a retained negative lookup without changing its recency.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(in crate::search::vector) fn item_is_known_absent(
        &self,
        layer: u16,
        node_id: NodeId,
    ) -> bool {
        self.items
            .get(&(layer, node_id))
            .is_some_and(|cached| cached.value.is_none())
    }

    /// Evicts the deterministic least-recently-used item in this namespace.
    pub(in crate::search::vector) fn evict_oldest_item(&mut self) -> bool {
        let Some((_, (layer, node_id))) = self.items.oldest() else {
            return false;
        };
        self.remove_item(layer, node_id);
        self.stats.item_evictions = self.stats.item_evictions.saturating_add(1);
        true
    }

    /// Returns and touches one SimHash or negative lookup.
    pub(in crate::search::vector) fn simhash(
        &mut self,
        node_id: NodeId,
    ) -> Option<Option<crate::search::vector::SimHash>> {
        if !self.simhashes.contains_key(&node_id) {
            self.stats.simhash_misses = self.stats.simhash_misses.saturating_add(1);
            return None;
        }
        self.stats.simhash_hits = self.stats.simhash_hits.saturating_add(1);
        let touch = self.take_touch();
        self.simhashes
            .touch(node_id, touch)
            .map(|cached| cached.value)
    }

    /// Installs or replaces one SimHash state.
    pub(in crate::search::vector) fn put_simhash(
        &mut self,
        node_id: NodeId,
        value: Option<crate::search::vector::SimHash>,
    ) {
        let previous_payload = self
            .simhashes
            .get(&node_id)
            .map_or(0, |cached| simhash_payload_bytes(cached.value));
        let payload_bytes = simhash_payload_bytes(value);
        self.replace_retained_payload(previous_payload, payload_bytes)
            .expect("bounded vector SimHash cache payload cannot overflow");
        let touch = self.take_touch();
        self.simhashes.insert(
            node_id,
            CachedSimHash {
                value,
                last_touch: touch,
            },
        );
    }

    /// Invalidates one SimHash or negative lookup after mutation.
    pub(in crate::search::vector) fn invalidate_simhash(&mut self, node_id: NodeId) {
        let Some(removed) = self.simhashes.remove(&node_id) else {
            return;
        };
        self.replace_retained_payload(simhash_payload_bytes(removed.value), 0)
            .expect("retained vector SimHash payload covers every cached value");
    }

    /// Returns the number of retained SimHash states.
    pub(in crate::search::vector) fn simhash_count(&self) -> usize {
        self.simhashes.len()
    }

    /// Evicts the deterministic least-recently-used SimHash in this namespace.
    pub(in crate::search::vector) fn evict_oldest_simhash(&mut self) -> bool {
        let Some((_, node_id)) = self.simhashes.oldest() else {
            return false;
        };
        self.invalidate_simhash(node_id);
        self.stats.simhash_evictions = self.stats.simhash_evictions.saturating_add(1);
        true
    }

    /// Returns retained decoded payload bytes for this namespace.
    #[cfg(test)]
    fn retained_payload_bytes(&self) -> Result<usize, HelixDbError> {
        Ok(self.retained_payload_bytes)
    }

    /// Returns the least recently used dirty row.
    pub(in crate::search::vector) fn oldest_dirty_neighbor(&self) -> Option<NeighborRowId> {
        self.dirty_neighbor_recency.first().map(|(_, row)| *row)
    }

    /// Returns the least recently used clean row.
    ///
    /// Dirty rows used before it are skipped, so this costs `O(1)` whenever no
    /// row is dirty, as when bounded eviction has flushed every dirty row.
    pub(in crate::search::vector) fn oldest_clean_neighbor(&self) -> Option<NeighborRowId> {
        self.neighbor_rows
            .oldest_first()
            .find(|(_, cached)| !cached.is_dirty())
            .map(|(row, _)| row)
    }

    /// Allocates the next recency value, renumbering the bounded cache on overflow.
    fn take_touch(&mut self) -> CacheSequence {
        let current = self.next_touch;
        let Some(next) = current.checked_next() else {
            self.renumber_touches();
            let renumbered = self.next_touch;
            self.next_touch = renumbered
                .checked_next()
                .expect("bounded cache renumbering leaves sequence capacity");
            return renumbered;
        };
        self.next_touch = next;
        current
    }

    /// Compacts recency values without changing oldest-entry ordering.
    fn renumber_touches(&mut self) {
        let mut entries = self
            .items
            .iter()
            .map(|(&(layer, node_id), cached)| {
                (
                    cached.last_touch,
                    MutationCacheTouchTarget::Item { layer, node_id },
                )
            })
            .chain(self.neighbor_rows.iter().map(|(row, cached)| {
                (
                    cached.last_touch(),
                    MutationCacheTouchTarget::Neighbor(*row),
                )
            }))
            .chain(self.simhashes.iter().map(|(node_id, cached)| {
                (
                    cached.last_touch,
                    MutationCacheTouchTarget::SimHash(*node_id),
                )
            }))
            .collect::<Vec<_>>();
        entries.sort_unstable();
        let renumbered = (0_u64..)
            .map(CacheSequence)
            .zip(entries)
            .map(|(touch, (_, target))| (target, touch))
            .collect::<BTreeMap<_, _>>();
        self.items.renumber(|&(layer, node_id)| {
            renumbered[&MutationCacheTouchTarget::Item { layer, node_id }]
        });
        self.neighbor_rows
            .renumber(|row| renumbered[&MutationCacheTouchTarget::Neighbor(*row)]);
        self.simhashes
            .renumber(|node_id| renumbered[&MutationCacheTouchTarget::SimHash(*node_id)]);
        self.dirty_neighbor_recency = self
            .neighbor_rows
            .iter()
            .filter(|(_, cached)| cached.is_dirty())
            .map(|(row, cached)| (cached.last_touch(), *row))
            .collect();
        self.next_touch = CacheSequence(
            u64::try_from(renumbered.len())
                .expect("bounded vector mutation cache length fits in u64"),
        );
    }
}

/// One bounded cache session shared by vector Scan and CatchUp planning.
///
/// Entries are nested under the complete generation identity, so equal physical
/// node/layer numbers from another scope, logical generation, record revision,
/// or physical partition cannot alias. The session owns no database or resident
/// vector-memory handle. A lifecycle driver may retain it across committed
/// steps of one builder-exclusive generation; the entity journal lets the
/// driver discard exactly the state of an entity it planned but did not admit.
///
/// Session-level bookkeeping never scans namespaces, because a retained build
/// over a tenant-partitioned index can touch one namespace per tenant. Every
/// attached namespace is summarized by running entry and payload totals, by its
/// least recently used entry of each kind in a global recency index, and by its
/// membership in the dirty set; detaching and attaching a namespace keeps all
/// three current. A namespace is dropped as soon as it retains no entry, and
/// every attached namespace is charged to the byte budget.
#[derive(Debug)]
pub(crate) struct VectorBuildSession<D: Distance> {
    caches: HashMap<VectorGenerationIdentity, MutationOpCache<D>>,
    next_touch: CacheSequence,
    /// Budget for retained payload plus per-entry and per-namespace bookkeeping.
    max_payload_bytes: usize,
    max_items: usize,
    max_neighbors: usize,
    max_simhashes: usize,
    /// Telemetry since the last reset; attached namespaces fold theirs in.
    session_stats: VectorBuildSessionStats,
    /// Cache state changed by the entity currently being planned.
    entity_changes: BTreeMap<VectorGenerationIdentity, EntityCacheChanges>,
    /// Entry counts and payload summed over every attached namespace.
    footprint: SessionFootprint,
    /// Least recently used entry of every attached namespace, per kind.
    victims: SessionVictims,
    /// Attached namespaces holding at least one dirty neighbor row.
    dirty: BTreeSet<VectorGenerationIdentity>,
}

/// Cache state one planned entity changed inside one generation namespace.
#[derive(Debug, Default)]
struct EntityCacheChanges {
    /// Planning this entity created the namespace, so none of it predates it.
    created_namespace: bool,
    /// Mutated graph nodes whose items, SimHash, and neighbor rows changed.
    nodes: BTreeSet<NodeId>,
    /// Other neighbor rows whose logical value changed.
    rows: BTreeSet<NeighborRowId>,
}

/// Entry counts and payload summed over a session's attached namespaces.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SessionFootprint {
    items: usize,
    neighbors: usize,
    simhashes: usize,
    /// Exact sum of namespace payloads. A sum of `usize` values cannot overflow
    /// `u128`, so an impossible total is reported when read, never lost here.
    payload_bytes: u128,
}

/// Least recently used entry of every attached namespace, one set per kind.
///
/// Each namespace contributes at most one entry per kind, so the session-wide
/// least recently used entry of a kind is the first element of its set.
#[derive(Debug, Default)]
struct SessionVictims {
    items: BTreeSet<SessionEvictionOrder>,
    neighbors: BTreeSet<SessionEvictionOrder>,
    simhashes: BTreeSet<SessionEvictionOrder>,
}

impl SessionVictims {
    /// Returns the eviction index for one cache kind.
    fn of_kind(&mut self, kind: SessionCacheKind) -> &mut BTreeSet<SessionEvictionOrder> {
        match kind {
            SessionCacheKind::Item => &mut self.items,
            SessionCacheKind::Neighbor => &mut self.neighbors,
            SessionCacheKind::SimHash => &mut self.simhashes,
        }
    }
}

impl<D: Distance> VectorBuildSession<D> {
    /// Creates one empty session bounded by its retained byte budget.
    ///
    /// [`Self::set_max_retained_bytes`] describes how the budget is charged.
    pub(crate) fn new(max_retained_bytes: NonZeroU64) -> Self {
        let mut session = Self {
            caches: HashMap::new(),
            next_touch: CacheSequence::initial(),
            max_payload_bytes: 0,
            max_items: 0,
            max_neighbors: 0,
            max_simhashes: 0,
            session_stats: VectorBuildSessionStats::default(),
            entity_changes: BTreeMap::new(),
            footprint: SessionFootprint::default(),
            victims: SessionVictims::default(),
            dirty: BTreeSet::new(),
        };
        session.set_max_retained_bytes(max_retained_bytes);
        session
    }

    /// Rebinds the retained byte budget and the class caps that scale with it.
    ///
    /// The budget charges every entry its encoded payload plus its kind's
    /// per-entry bookkeeping ([`VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES`],
    /// [`VECTOR_BUILD_SESSION_NEIGHBOR_OVERHEAD_BYTES`], or
    /// [`VECTOR_BUILD_SESSION_SIMHASH_OVERHEAD_BYTES`]), and every attached
    /// namespace [`VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES`]. Each entry
    /// class is also capped at one entry per
    /// [`VECTOR_BUILD_SESSION_BUDGET_BYTES_PER_ENTRY`] budget bytes, so the caps
    /// scale with the budget: 8-byte SimHashes stay resident far beyond the old
    /// per-step class caps while a large budget is available, yet tiny payloads
    /// cannot multiply the estimated bookkeeping without bound. Eviction remains
    /// deterministic global LRU.
    ///
    /// A lifecycle driver rebinds a retained session to its fair share of the
    /// budget it splits across builds. Nothing is evicted here: a smaller
    /// budget also lowers every class cap, so a session within its old byte
    /// budget can still hold more entries of a class than the new cap allows.
    /// [`Self::exceeds_limits`] reports that excess, and it goes at the next
    /// [`Self::enforce_limits`] or [`Self::shrink_to`].
    pub(crate) fn set_max_retained_bytes(&mut self, max_retained_bytes: NonZeroU64) {
        let max_payload_bytes = usize::try_from(max_retained_bytes.get()).unwrap_or(usize::MAX);
        let max_entries = (max_payload_bytes / VECTOR_BUILD_SESSION_BUDGET_BYTES_PER_ENTRY).max(1);
        self.max_payload_bytes = max_payload_bytes;
        self.max_items = max_entries;
        self.max_neighbors = max_entries;
        self.max_simhashes = max_entries;
    }

    /// Creates a small-limit session for deterministic cache contract tests.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn with_test_limits(
        max_input_bytes: NonZeroU64,
        max_items: usize,
        max_neighbors: usize,
        max_simhashes: usize,
    ) -> Self {
        assert!(max_items > 0);
        assert!(max_neighbors > 0);
        assert!(max_simhashes > 0);
        let mut session = Self::new(max_input_bytes);
        session.max_items = max_items;
        session.max_neighbors = max_neighbors;
        session.max_simhashes = max_simhashes;
        session
    }

    /// Creates a clean session retaining `simhashes` SimHashes in one namespace.
    ///
    /// Lifecycle driver contracts use it to retain sessions of an exact
    /// charged size: one namespace plus one SimHash charge per entry.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn with_test_simhashes(max_retained_bytes: NonZeroU64, simhashes: u64) -> Self {
        let identity = VectorGenerationIdentity::try_new(
            crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            1,
            "vector-build-session-fixture".to_string(),
            1,
            NonZeroU64::MIN,
            1,
            crate::index_lifecycle::IndexElementKind::Node,
            VectorDimension::try_new(2).expect("fixture dimension is valid"),
        )
        .expect("fixture generation identity is valid");
        let mut session = Self::new(max_retained_bytes);
        let mut cache = session
            .take_cache(&identity, 8, 4)
            .expect("fixture degree limits are valid");
        for node_id in 0..simhashes {
            cache.put_simhash(
                node_id,
                Some(crate::search::vector::SimHash::from_bits(node_id)),
            );
        }
        session.restore_cache(identity, cache);
        session.admit_entity();
        session
    }

    /// Returns the eviction order of each kind's least recently used entry.
    fn namespace_victims(
        identity: &VectorGenerationIdentity,
        cache: &MutationOpCache<D>,
    ) -> impl Iterator<Item = SessionEvictionOrder> {
        let item = cache
            .items
            .oldest()
            .map(|(touch, _)| (touch, SessionCacheKind::Item));
        let neighbor = cache
            .neighbor_rows
            .oldest()
            .map(|(touch, _)| (touch, SessionCacheKind::Neighbor));
        let simhash = cache
            .simhashes
            .oldest()
            .map(|(touch, _)| (touch, SessionCacheKind::SimHash));
        [item, neighbor, simhash]
            .into_iter()
            .flatten()
            .map(|(touch, kind)| SessionEvictionOrder {
                touch,
                kind,
                identity: identity.clone(),
            })
    }

    /// Detaches one namespace from the map and every session-level index.
    fn detach(&mut self, identity: &VectorGenerationIdentity) -> Option<MutationOpCache<D>> {
        let cache = self.caches.remove(identity)?;
        for victim in Self::namespace_victims(identity, &cache) {
            assert!(
                self.victims.of_kind(victim.kind).remove(&victim),
                "attached vector build namespace indexes its eviction victims"
            );
        }
        self.dirty.remove(identity);
        let footprint = &mut self.footprint;
        footprint.items = footprint
            .items
            .checked_sub(cache.item_count())
            .expect("attached vector build items are counted");
        footprint.neighbors = footprint
            .neighbors
            .checked_sub(cache.neighbor_count())
            .expect("attached vector build neighbor rows are counted");
        footprint.simhashes = footprint
            .simhashes
            .checked_sub(cache.simhash_count())
            .expect("attached vector build SimHashes are counted");
        footprint.payload_bytes = footprint
            .payload_bytes
            .checked_sub(cache.retained_payload_bytes as u128)
            .expect("attached vector build payload is counted");
        Some(cache)
    }

    /// Attaches one namespace to the map and every index, or drops it if empty.
    ///
    /// The namespace's telemetry moves into the session totals first, so an
    /// attached namespace never holds uncounted telemetry and a dropped one
    /// loses none. Only the session records its retained-payload maximum.
    fn attach(&mut self, identity: VectorGenerationIdentity, mut cache: MutationOpCache<D>) {
        let max_retained_payload_bytes = self.session_stats.max_retained_payload_bytes;
        self.session_stats.merge(core::mem::take(&mut cache.stats));
        self.session_stats.max_retained_payload_bytes = max_retained_payload_bytes;
        if cache.item_count() == 0 && cache.neighbor_count() == 0 && cache.simhash_count() == 0 {
            return;
        }
        let footprint = &mut self.footprint;
        footprint.items = footprint.items.saturating_add(cache.item_count());
        footprint.neighbors = footprint.neighbors.saturating_add(cache.neighbor_count());
        footprint.simhashes = footprint.simhashes.saturating_add(cache.simhash_count());
        footprint.payload_bytes = footprint
            .payload_bytes
            .saturating_add(cache.retained_payload_bytes as u128);
        for victim in Self::namespace_victims(&identity, &cache) {
            assert!(
                self.victims.of_kind(victim.kind).insert(victim),
                "one vector build namespace indexes one victim per kind"
            );
        }
        if cache.oldest_dirty_neighbor().is_some() {
            self.dirty.insert(identity.clone());
        }
        assert!(
            self.caches.insert(identity, cache).is_none(),
            "a vector build session cannot attach one identity twice"
        );
    }

    /// Temporarily detaches one identity cache for an async mutation operation.
    pub(in crate::search::vector) fn take_cache(
        &mut self,
        identity: &VectorGenerationIdentity,
        layer0_degree: usize,
        upper_degree: usize,
    ) -> Result<MutationOpCache<D>, HelixDbError> {
        let expected = match NeighborDegreeLimits::try_new(layer0_degree, upper_degree) {
            Ok(expected) => expected,
            Err(error) => return Err(HelixDbError::InvariantViolation(error.to_string())),
        };
        let mut cache = match self.detach(identity) {
            Some(cache) => {
                if cache.degree_limits != expected {
                    return Err(HelixDbError::InvariantViolation(
                        "vector build session generation changed neighbor degree limits"
                            .to_string(),
                    ));
                }
                cache
            }
            None => {
                let cache = MutationOpCache::with_degree_limits(layer0_degree, upper_degree)?
                    .into_build_session_cache();
                self.entity_changes
                    .entry(identity.clone())
                    .or_default()
                    .created_namespace = true;
                cache
            }
        };
        cache.next_touch = self.next_touch;
        Ok(cache)
    }

    /// Restores one detached identity cache even when mutation planning failed.
    ///
    /// A namespace left without entries is dropped rather than restored.
    pub(in crate::search::vector) fn restore_cache(
        &mut self,
        identity: VectorGenerationIdentity,
        cache: MutationOpCache<D>,
    ) {
        self.next_touch = cache.next_touch;
        assert!(
            !self.caches.contains_key(&identity),
            "a vector build session cannot restore one identity twice"
        );
        self.attach(identity, cache);
    }

    /// Journals the cache state one entity mutation changed in `identity`.
    pub(in crate::search::vector) fn record_entity_changes(
        &mut self,
        identity: &VectorGenerationIdentity,
        node_id: NodeId,
        rows: impl IntoIterator<Item = NeighborRowId>,
    ) {
        let changes = self.entity_changes.entry(identity.clone()).or_default();
        changes.nodes.insert(node_id);
        changes.rows.extend(rows);
    }

    /// Accepts every change journaled since the previous entity boundary.
    ///
    /// Call after the entity's planned writes were staged in the transaction
    /// that commits them.
    pub(crate) fn admit_entity(&mut self) {
        self.entity_changes.clear();
    }

    /// Removes all cache state changed by the planned but unadmitted entity.
    ///
    /// Namespaces the entity created are dropped whole; otherwise its mutated
    /// nodes lose every item, SimHash, and neighbor row, and each other changed
    /// row is removed. Remaining entries were loaded from storage the entity did
    /// not write, so they still equal the state before the entity. Call only
    /// after [`Self::flush_all`], because a dirty row of an earlier admitted
    /// entity cannot be told apart from this entity's change.
    pub(crate) fn discard_entity(&mut self) {
        debug_assert!(
            !self.has_dirty_neighbors(),
            "vector build entity discard follows a complete flush"
        );
        for (identity, changes) in core::mem::take(&mut self.entity_changes) {
            let Some(mut cache) = self.detach(&identity) else {
                continue;
            };
            if changes.created_namespace {
                continue;
            }
            for node_id in changes.nodes {
                cache.invalidate_items(node_id);
                cache.invalidate_simhash(node_id);
                cache.invalidate_neighbors(node_id);
            }
            for row in changes.rows {
                cache.remove_neighbor(row);
            }
            self.attach(identity, cache);
        }
    }

    /// Returns whether any neighbor row still awaits a flush.
    pub(crate) fn has_dirty_neighbors(&self) -> bool {
        !self.dirty.is_empty()
    }

    /// Starts per-step telemetry without dropping any cached state.
    pub(crate) fn reset_stats(&mut self) {
        self.session_stats = VectorBuildSessionStats::default();
    }

    /// Flushes every dirty neighbor in deterministic identity/recency order.
    ///
    /// Only namespaces in the dirty set are visited. A failed typed write
    /// leaves the selected cache row dirty. The caller must abort planning and
    /// discard the complete session and transaction.
    pub(crate) fn flush_all(
        &mut self,
        txn: &MeasuredVectorTransaction<'_>,
    ) -> Result<(), HelixDbError> {
        while let Some(identity) = self.dirty.first().cloned() {
            let mut cache = self
                .detach(&identity)
                .expect("dirty vector build namespace is attached");
            let flushed = loop {
                let Some(row) = cache.oldest_dirty_neighbor() else {
                    break Ok(());
                };
                match flush_build_session_neighbor(txn, &identity, &mut cache, row) {
                    Ok(()) => {
                        self.session_stats.dirty_neighbor_flushes =
                            self.session_stats.dirty_neighbor_flushes.saturating_add(1);
                    }
                    Err(error) => break Err(error),
                }
            };
            self.attach(identity, cache);
            flushed?;
        }
        Ok(())
    }

    /// Enforces class counts and the global retained-byte ceiling.
    ///
    /// Selection is deterministic global LRU through the per-kind victim index,
    /// so each eviction costs `O(log namespaces)` rather than a namespace scan.
    /// Equal recency resolves by cache kind and complete generation identity. A
    /// dirty neighbor is flushed through the typed recorder before it is removed.
    pub(crate) fn enforce_limits(
        &mut self,
        txn: &MeasuredVectorTransaction<'_>,
    ) -> Result<(), HelixDbError> {
        self.evict_until(self.max_payload_bytes, Some(txn))
    }

    /// Evicts least-recently-used entries until at most `max_bytes` are charged.
    ///
    /// A lifecycle driver shrinks retained sessions so the builds it retains
    /// share one budget. A retained session holds no dirty neighbor row, so no
    /// flush is needed; selecting one is an invariant violation. The session's
    /// own budget and class caps keep applying.
    pub(crate) fn shrink_to(&mut self, max_bytes: usize) -> Result<(), HelixDbError> {
        self.evict_until(max_bytes.min(self.max_payload_bytes), None)
    }

    /// Returns whether the byte budget or any class cap is exceeded.
    ///
    /// Exactly then does the next [`Self::enforce_limits`] or
    /// [`Self::shrink_to`] evict. An unmeasurable session exceeds its limits.
    /// Costs `O(1)`, so a lifecycle driver can check a rebound session and run
    /// the eviction it needs off the async executor.
    pub(crate) fn exceeds_limits(&self) -> bool {
        self.pressure(self.max_payload_bytes)
            .map_or(true, |pressure| pressure.contains(&true))
    }

    /// Returns, for items, neighbor rows, and SimHashes in that order, whether
    /// the class must evict to meet its cap and `max_bytes`.
    fn pressure(&self, max_bytes: usize) -> Result<[bool; 3], HelixDbError> {
        let bytes_over = self.retained_bytes()? > max_bytes;
        Ok([
            bytes_over || self.footprint.items > self.max_items,
            bytes_over || self.footprint.neighbors > self.max_neighbors,
            bytes_over || self.footprint.simhashes > self.max_simhashes,
        ])
    }

    /// Evicts in global LRU order until every class cap and `max_bytes` hold.
    ///
    /// A dirty neighbor victim is flushed through `txn` first, and is an
    /// invariant violation without one.
    fn evict_until(
        &mut self,
        max_bytes: usize,
        txn: Option<&MeasuredVectorTransaction<'_>>,
    ) -> Result<(), HelixDbError> {
        loop {
            let pressure = self.pressure(max_bytes)?;
            if !pressure.contains(&true) {
                self.session_stats.max_retained_payload_bytes = self
                    .session_stats
                    .max_retained_payload_bytes
                    .max(u64::try_from(self.footprint.payload_bytes).unwrap_or(u64::MAX));
                return Ok(());
            }
            let Some(victim) = pressure
                .into_iter()
                .zip([
                    &self.victims.items,
                    &self.victims.neighbors,
                    &self.victims.simhashes,
                ])
                .filter_map(|(pressure, victims)| victims.first().filter(|_| pressure))
                .min()
                .cloned()
            else {
                return Err(HelixDbError::InvariantViolation(
                    "vector build session exceeded a limit without an evictable entry".to_string(),
                ));
            };
            let mut cache = self
                .detach(&victim.identity)
                .expect("indexed vector build eviction victim is attached");
            let evicted = match victim.kind {
                SessionCacheKind::Item => {
                    let (_, (layer, node_id)) = cache
                        .items
                        .oldest()
                        .expect("indexed vector item victim remains cached");
                    cache.remove_item(layer, node_id);
                    self.session_stats.item_evictions =
                        self.session_stats.item_evictions.saturating_add(1);
                    Ok(())
                }
                SessionCacheKind::Neighbor => {
                    let (_, row) = cache
                        .neighbor_rows
                        .oldest()
                        .expect("indexed vector neighbor victim remains cached");
                    let dirty = cache.neighbor(row).is_some_and(CachedNeighbor::is_dirty);
                    let flushed = match (dirty, txn) {
                        (false, _) => Ok(()),
                        (true, Some(txn)) => {
                            flush_build_session_neighbor(txn, &victim.identity, &mut cache, row)
                        }
                        (true, None) => Err(HelixDbError::InvariantViolation(
                            "vector build session eviction met a dirty neighbor row without a \
                             transaction to flush it"
                                .to_string(),
                        )),
                    };
                    flushed.map(|()| {
                        self.session_stats.dirty_neighbor_flushes = self
                            .session_stats
                            .dirty_neighbor_flushes
                            .saturating_add(u64::from(dirty));
                        cache.remove_neighbor(row);
                        self.session_stats.neighbor_evictions =
                            self.session_stats.neighbor_evictions.saturating_add(1);
                    })
                }
                SessionCacheKind::SimHash => {
                    let (_, node_id) = cache
                        .simhashes
                        .oldest()
                        .expect("indexed vector SimHash victim remains cached");
                    cache.invalidate_simhash(node_id);
                    self.session_stats.simhash_evictions =
                        self.session_stats.simhash_evictions.saturating_add(1);
                    Ok(())
                }
            };
            self.attach(victim.identity, cache);
            evicted?;
        }
    }

    /// Returns aggregate cache behavior for lifecycle-testing metrics.
    ///
    /// Planning reports the maximum retained payload after enforcing the
    /// session's global ceiling. Cache-local telemetry also observes transient
    /// peaks within one entity, before that ceiling is enforced, so
    /// [`Self::attach`] folds every other counter only.
    pub(crate) fn stats(&self) -> VectorBuildSessionStats {
        self.session_stats
    }

    /// Returns retained decoded-item and negative-lookup entries.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn item_count(&self) -> usize {
        self.footprint.items
    }

    /// Returns retained upper/layer-zero neighbor rows.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn neighbor_count(&self) -> usize {
        self.footprint.neighbors
    }

    /// Returns retained SimHash and negative-lookup entries.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn simhash_count(&self) -> usize {
        self.footprint.simhashes
    }

    /// Returns payload plus per-entry and per-namespace bookkeeping charged to the budget.
    pub(crate) fn retained_bytes(&self) -> Result<usize, HelixDbError> {
        [
            (
                self.footprint.items,
                VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES,
            ),
            (
                self.footprint.neighbors,
                VECTOR_BUILD_SESSION_NEIGHBOR_OVERHEAD_BYTES,
            ),
            (
                self.footprint.simhashes,
                VECTOR_BUILD_SESSION_SIMHASH_OVERHEAD_BYTES,
            ),
            (
                self.caches.len(),
                VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES,
            ),
        ]
        .into_iter()
        .try_fold(self.retained_payload_bytes()?, |total, (count, charge)| {
            count
                .checked_mul(charge)
                .and_then(|charged| total.checked_add(charged))
        })
        .ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "vector build session byte accounting overflowed".to_string(),
            )
        })
    }

    /// Returns retained decoded payload bytes across every namespace.
    pub(crate) fn retained_payload_bytes(&self) -> Result<usize, HelixDbError> {
        usize::try_from(self.footprint.payload_bytes).map_err(|_| {
            HelixDbError::InvariantViolation(
                "vector build session payload accounting overflowed".to_string(),
            )
        })
    }

    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) const fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }

    /// Hands a large session's state to a new thread that frees it.
    ///
    /// Everything whose size grows with the entries or attached namespaces
    /// moves together: the caches, their eviction index, the dirty set, and
    /// the entity journal. The session is left empty, holding only fixed-size
    /// state. Each attached namespace is charged only
    /// [`VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES`], so a large session can
    /// attach hundreds of thousands of namespaces, each owning up to three
    /// eviction-index entries with a cloned identity.
    ///
    /// Returns the freeing thread, or `None` when the session charges less
    /// than [`VECTOR_BUILD_SESSION_BACKGROUND_DROP_BYTES`] and keeps its state,
    /// or when no thread could be spawned and the state was freed here.
    fn free_state_in_background(&mut self) -> Option<std::thread::JoinHandle<()>> {
        if self
            .retained_bytes()
            .is_ok_and(|bytes| bytes < VECTOR_BUILD_SESSION_BACKGROUND_DROP_BYTES)
        {
            return None;
        }
        let state = (
            core::mem::take(&mut self.caches),
            core::mem::take(&mut self.victims),
            core::mem::take(&mut self.dirty),
            core::mem::take(&mut self.entity_changes),
        );
        self.footprint = SessionFootprint::default();
        // A failed spawn drops the closure, and the state with it, right here.
        std::thread::Builder::new()
            .name("vector-build-session-drop".to_string())
            .spawn(move || drop(state))
            .ok()
    }
}

impl<D: Distance> Drop for VectorBuildSession<D> {
    /// Frees a large session's state on a background thread.
    ///
    /// A retained session can hold millions of allocations. Freeing them on
    /// the async executor thread that drops the session would stall it, and
    /// every task queued behind it, for the whole teardown. A session below
    /// [`VECTOR_BUILD_SESSION_BACKGROUND_DROP_BYTES`] frees inline, where a
    /// thread spawn would cost more than the teardown.
    fn drop(&mut self) {
        // The freeing thread runs detached.
        let _ = self.free_state_in_background();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SessionCacheKind {
    Item,
    Neighbor,
    SimHash,
}

/// Global LRU position of one namespace's oldest entry of one kind.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SessionEvictionOrder {
    touch: CacheSequence,
    kind: SessionCacheKind,
    identity: VectorGenerationIdentity,
}

fn flush_build_session_neighbor<D: Distance>(
    txn: &MeasuredVectorTransaction<'_>,
    identity: &VectorGenerationIdentity,
    cache: &mut MutationOpCache<D>,
    row: NeighborRowId,
) -> Result<(), HelixDbError> {
    let Some(cached) = cache.neighbor(row).cloned() else {
        return Ok(());
    };
    if !cached.is_dirty() {
        return Ok(());
    }
    let (layer, node_id) = row.storage_parts();
    let NeighborRowValue::Present(current) = cached.current() else {
        return Err(HelixDbError::InvariantViolation(
            "vector build session cannot flush a deleted neighbor row".to_string(),
        ));
    };
    let original = cached
        .original()
        .expect("dirty vector build neighbors retain their original value");
    let previous = match original {
        NeighborRowValue::KnownAbsent => NeighborSet::empty(node_id, cache.degree_limit(layer)),
        NeighborRowValue::Present(neighbors) => neighbors.clone(),
    };
    if original != cached.current() {
        let keyspace = VectorRowKeyspace::from_allocated(
            identity.physical_name().to_string(),
            identity.physical_index_id(),
            identity.scope(),
        );
        let rows = VectorWriteRows::new(txn, &keyspace);
        let difference = match previous.difference(current) {
            Ok(difference) => difference,
            Err(error) => return Err(HelixDbError::InvariantViolation(error.to_string())),
        };
        let (removed, added) = difference.into_parts();
        for target_node_id in removed {
            rows.delete_reverse_locator(target_node_id, layer, node_id)?;
        }
        for target_node_id in added {
            rows.put_reverse_locator(target_node_id, layer, node_id)?;
        }
        if layer == 0 {
            rows.put_layer0_neighbors(node_id, current.as_slice())?;
        } else {
            rows.put_upper_neighbors(layer, node_id, current.as_slice())?;
        }
    }
    cache.mark_neighbor_flushed(row);
    Ok(())
}

fn neighbor_payload_bytes(layer: u16, value: &NeighborRowValue) -> Result<usize, HelixDbError> {
    let NeighborRowValue::Present(neighbors) = value else {
        return Ok(0);
    };
    if layer == 0 {
        return Ok(encode_layer0_neighbors(neighbors.as_slice()).len());
    }
    encode_upper_neighbors(neighbors.as_slice())
        .map(|encoded| encoded.len())
        .map_err(HelixDbError::from)
}

fn cached_neighbor_payload_bytes(
    row: NeighborRowId,
    cached: &CachedNeighbor,
) -> Result<usize, HelixDbError> {
    let current = neighbor_payload_bytes(row.layer.number(), cached.current())?;
    let original = cached
        .original()
        .map(|value| neighbor_payload_bytes(row.layer.number(), value))
        .transpose()?
        .unwrap_or(0);
    let Some(payload_bytes) = current.checked_add(original) else {
        return Err(HelixDbError::InvariantViolation(
            "vector build neighbor-cache payload accounting overflowed".to_string(),
        ));
    };
    Ok(payload_bytes)
}

const fn simhash_payload_bytes(value: Option<crate::search::vector::SimHash>) -> usize {
    if value.is_some() {
        core::mem::size_of::<u64>()
    } else {
        0
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../../../tests/production_support/vector/mutation.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod relink_tests;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use proptest::prelude::*;
    use slatedb::object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::encoding::v2::keys::indexes::vector::{VectorEntryCandidateNodeKey, VectorKey};
    use crate::search::vector::distance::Euclidean;

    fn session_identity(
        scope: crate::encoding::v2::keys::scope::DataScope,
        physical_index_id: u64,
    ) -> VectorGenerationIdentity {
        VectorGenerationIdentity::try_new(
            scope,
            7,
            format!("vector-session-{physical_index_id}"),
            physical_index_id,
            NonZeroU64::new(3).unwrap(),
            11,
            crate::index_lifecycle::IndexElementKind::Node,
            crate::search::vector::VectorDimension::try_new(2).unwrap(),
        )
        .unwrap()
    }

    async fn session_test_db(name: &str) -> Arc<slatedb::Db> {
        Arc::new(
            slatedb::Db::open(name, Arc::new(InMemory::new()))
                .await
                .unwrap(),
        )
    }

    /// Builds a canonical present-row value for concise transition fixtures.
    fn neighbors(owner: NodeId, nodes: Vec<NodeId>) -> NeighborRowValue {
        let limit = NeighborDegreeLimit::try_new(8).unwrap();
        NeighborRowValue::Present(NeighborSet::try_from_canonical(owner, limit, nodes).unwrap())
    }

    /// Minimal independent state used to check the production cache transition table.
    #[derive(Debug, Clone)]
    struct ReferenceNeighbor {
        original: Option<NeighborRowValue>,
        current: NeighborRowValue,
        last_touch: u64,
    }

    impl ReferenceNeighbor {
        /// Creates the reference equivalent of one storage-proven clean row.
        fn clean(current: NeighborRowValue, last_touch: u64) -> Self {
            Self {
                original: None,
                current,
                last_touch,
            }
        }

        /// Applies the reference first-stage/restage rule.
        fn stage(&mut self, staged: NeighborRowValue, last_touch: u64) {
            if self.original.is_none() {
                self.original = Some(self.current.clone());
            }
            self.current = staged;
            self.last_touch = last_touch;
        }

        /// Applies the reference successful-flush transition.
        fn mark_flushed(&mut self) {
            self.original = None;
        }

        /// Reports whether the reference row requires a flush.
        fn is_dirty(&self) -> bool {
            self.original.is_some()
        }
    }

    /// Maps a compact generated token to a valid absent or present row value.
    fn generated_value(owner: NodeId, token: u8) -> NeighborRowValue {
        if token == 0 {
            return NeighborRowValue::KnownAbsent;
        }
        neighbors(owner, vec![NodeId::from(token) + 100])
    }

    /// Selects the oldest reference row of the requested state.
    fn reference_oldest(rows: &HashMap<NodeId, ReferenceNeighbor>, dirty: bool) -> Option<NodeId> {
        rows.iter()
            .filter(|(_, cached)| cached.is_dirty() == dirty)
            .min_by_key(|(node_id, cached)| (cached.last_touch, **node_id))
            .map(|(node_id, _)| *node_id)
    }

    /// Compares every observable production row and eviction choice with the model.
    fn assert_matches_reference(
        cache: &MutationOpCache<crate::search::vector::distance::Cosine>,
        rows: &HashMap<NodeId, ReferenceNeighbor>,
    ) {
        assert_eq!(cache.neighbor_count(), rows.len());
        assert_recency_is_consistent(cache);
        let expected_payload: usize = rows
            .values()
            .map(|cached| {
                neighbor_payload_bytes(0, &cached.current).unwrap()
                    + cached
                        .original
                        .as_ref()
                        .map(|original| neighbor_payload_bytes(0, original).unwrap())
                        .unwrap_or(0)
            })
            .sum();
        assert_eq!(cache.retained_payload_bytes().unwrap(), expected_payload);
        for node_id in 1..=4 {
            let row =
                MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, node_id);
            match (cache.neighbor(row), rows.get(&node_id)) {
                (Some(actual), Some(expected)) => {
                    assert_eq!(actual.current(), &expected.current);
                    assert_eq!(actual.original(), expected.original.as_ref());
                    assert_eq!(actual.is_dirty(), expected.is_dirty());
                    assert_eq!(actual.last_touch(), CacheSequence(expected.last_touch));
                }
                (None, None) => {}
                (actual, expected) => panic!(
                    "production/reference cache presence differs: {actual:?} versus {expected:?}"
                ),
            }
        }
        assert_eq!(
            cache.oldest_clean_neighbor(),
            reference_oldest(rows, false).map(|node_id| {
                MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, node_id)
            })
        );
        assert_eq!(
            cache.oldest_dirty_neighbor(),
            reference_oldest(rows, true).map(|node_id| {
                MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, node_id)
            })
        );
        assert_eq!(
            cache.neighbor_rows.oldest().map(|(_, row)| row),
            rows.iter()
                .min_by_key(|(node_id, cached)| (cached.last_touch, **node_id))
                .map(|(node_id, _)| {
                    MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(
                        0, *node_id,
                    )
                }),
            "session eviction takes the least recent row, clean or dirty"
        );
    }

    /// Asserts that every recency order of `cache` agrees with its values.
    ///
    /// Each order sorts, starts current, lists every value exactly once among
    /// its current entries, and stays within its compaction and allocation
    /// bounds; the dirty set holds exactly the dirty rows at their recency.
    fn assert_recency_is_consistent<D: Distance>(cache: &MutationOpCache<D>) {
        fn check<K: Copy + Ord + core::hash::Hash + core::fmt::Debug, V: Touched>(
            map: &RecencyMap<K, V>,
        ) {
            assert!(map
                .order
                .iter()
                .zip(map.order.iter().skip(1))
                .all(|(older, newer)| older <= newer));
            assert!(map.order.len() <= 2 * map.len());
            assert!(map.order.capacity() <= 4 * map.len().max(1));
            let current = map
                .oldest_first()
                .map(|(key, value)| (value.last_touch(), key))
                .collect::<Vec<_>>();
            let mut expected = map
                .iter()
                .map(|(key, value)| (value.last_touch(), *key))
                .collect::<Vec<_>>();
            expected.sort_unstable();
            assert_eq!(current, expected);
            assert_eq!(map.oldest(), expected.first().copied());
        }
        check(&cache.items);
        check(&cache.neighbor_rows);
        check(&cache.simhashes);
        let dirty = cache
            .neighbor_rows
            .iter()
            .filter(|(_, cached)| cached.is_dirty())
            .map(|(row, cached)| (cached.last_touch(), *row))
            .collect::<BTreeSet<_>>();
        assert_eq!(cache.dirty_neighbor_recency, dirty);
    }

    #[test]
    fn row_identity_and_sequence_preserve_closed_components() {
        let row = NeighborRowId::new(HnswLayer::from_deployed(3), VectorEntityId::Node(42));
        assert_eq!(row.layer().number(), 3);
        assert_eq!(row.entity(), VectorEntityId::Node(42));

        let first = CacheSequence::initial();
        assert!(first < first.checked_next().unwrap());
        assert_eq!(CacheSequence(u64::MAX).checked_next(), None);
    }

    #[test]
    fn staging_and_flushing_preserve_the_first_original() {
        let first = CacheSequence::initial();
        let second = first.checked_next().unwrap();
        let third = second.checked_next().unwrap();
        let original = neighbors(1, vec![2]);
        let restaged = neighbors(1, vec![3, 4]);
        let mut cached = CachedNeighbor::clean(original.clone(), first);

        cached.stage(NeighborRowValue::KnownAbsent, second);
        assert_eq!(cached.original(), Some(&original));
        assert_eq!(cached.current(), &NeighborRowValue::KnownAbsent);

        cached.stage(restaged.clone(), third);
        assert_eq!(cached.original(), Some(&original));
        assert_eq!(cached.current(), &restaged);
        assert_eq!(cached.last_touch(), third);

        cached.mark_flushed();
        assert_eq!(cached.original(), None);
        assert_eq!(cached.current(), &restaged);
    }

    #[test]
    fn known_absent_is_distinct_from_an_empty_present_row() {
        let empty = neighbors(7, Vec::new());
        assert_ne!(NeighborRowValue::KnownAbsent, empty);
    }

    #[test]
    fn bounded_oldest_selection_uses_recency_and_state() {
        let mut cache = MutationOpCache::<crate::search::vector::distance::Cosine>::default();
        let first = MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, 1);
        let second = MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, 2);
        let third = MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, 3);
        cache.install_loaded_neighbor(first, neighbors(1, vec![4]));
        cache.install_loaded_neighbor(second, neighbors(2, vec![4]));
        cache
            .stage_loaded_neighbor(first, neighbors(1, vec![5]))
            .unwrap();
        cache.install_loaded_neighbor(third, neighbors(3, vec![4]));
        cache
            .stage_loaded_neighbor(third, neighbors(3, vec![5]))
            .unwrap();

        assert_eq!(cache.oldest_clean_neighbor(), Some(second));
        assert_eq!(cache.oldest_dirty_neighbor(), Some(first));
    }

    #[test]
    fn sequence_rollover_renumbers_without_reversing_recency() {
        let mut cache = MutationOpCache::<crate::search::vector::distance::Cosine>::default();
        let first = MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, 1);
        let second = MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, 2);
        cache.install_loaded_neighbor(first, neighbors(1, vec![3]));
        cache.install_loaded_neighbor(second, neighbors(2, vec![3]));
        cache.next_touch = CacheSequence(u64::MAX);

        cache
            .stage_loaded_neighbor(first, neighbors(1, vec![4]))
            .unwrap();
        cache
            .stage_loaded_neighbor(second, neighbors(2, vec![4]))
            .unwrap();

        assert_eq!(cache.oldest_dirty_neighbor(), Some(first));
        assert!(
            cache.neighbor(first).unwrap().last_touch()
                < cache.neighbor(second).unwrap().last_touch()
        );
    }

    #[test]
    fn neighbor_state_transitions_do_not_mutate_the_item_cache() {
        let mut cache = MutationOpCache::<crate::search::vector::distance::Cosine>::default();
        let row = MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, 1);
        cache.put_item(0, 1, None, 0);

        cache.install_loaded_neighbor(row, neighbors(1, vec![2]));
        cache
            .stage_loaded_neighbor(row, neighbors(1, vec![3]))
            .unwrap();
        cache.mark_neighbor_flushed(row);
        cache.remove_neighbor(row);

        assert!(cache.item_is_known_absent(0, 1));
        assert_eq!(cache.item_count(), 1);
    }

    #[test]
    fn retained_payload_accounting_tracks_replacement_flush_and_invalidation() {
        let mut cache = MutationOpCache::<crate::search::vector::distance::Cosine>::default();
        let row = MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, 1);
        let present = neighbors(1, vec![2]);
        let neighbor_bytes = neighbor_payload_bytes(0, &present).unwrap();

        cache.put_item(0, 1, None, 11);
        cache.put_item(0, 1, None, 7);
        cache.put_simhash(1, Some(crate::search::vector::SimHash::from_bits(9)));
        assert_eq!(
            cache.retained_payload_bytes().unwrap(),
            7 + core::mem::size_of::<u64>()
        );

        cache.install_loaded_neighbor(row, NeighborRowValue::KnownAbsent);
        cache.stage_loaded_neighbor(row, present).unwrap();
        assert_eq!(
            cache.retained_payload_bytes().unwrap(),
            7 + core::mem::size_of::<u64>() + neighbor_bytes
        );
        cache.mark_neighbor_flushed(row);
        assert_eq!(
            cache.retained_payload_bytes().unwrap(),
            7 + core::mem::size_of::<u64>() + neighbor_bytes
        );

        cache.put_simhash(1, None);
        cache.remove_neighbor(row);
        cache.remove_item(0, 1);
        assert_eq!(cache.retained_payload_bytes().unwrap(), 0);
    }

    #[test]
    fn build_session_namespaces_positive_and_negative_state_by_complete_identity() {
        use crate::encoding::v2::keys::scope::{DataScope, TenantId};

        let first = session_identity(DataScope::Tenant(TenantId::from_u128(1)), 31);
        let second = session_identity(DataScope::Tenant(TenantId::from_u128(2)), 31);
        let mut session = VectorBuildSession::<crate::search::vector::distance::Cosine>::new(
            NonZeroU64::new(1024).unwrap(),
        );

        let mut first_cache = session.take_cache(&first, 8, 4).unwrap();
        first_cache.put_item(0, 9, None, 0);
        first_cache.put_simhash(9, None);
        first_cache.install_loaded_neighbor(
            MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, 9),
            NeighborRowValue::KnownAbsent,
        );
        session.restore_cache(first.clone(), first_cache);

        let mut second_cache = session.take_cache(&second, 8, 4).unwrap();
        assert!(second_cache.item(0, 9).is_none());
        assert_eq!(second_cache.simhash(9), None);
        assert!(!second_cache.contains_neighbor(MutationOpCache::<
            crate::search::vector::distance::Cosine,
        >::node_row_id(0, 9)));
        second_cache.put_item(0, 9, None, 0);
        session.restore_cache(second, second_cache);

        let mut first_cache = session.take_cache(&first, 8, 4).unwrap();
        assert!(matches!(first_cache.item(0, 9), Some(None)));
        assert!(matches!(first_cache.simhash(9), Some(None)));
        session.restore_cache(first, first_cache);
        assert_eq!(session.item_count(), 2);
        assert_eq!(session.stats().item_hits(), 1);
        assert_eq!(session.stats().simhash_hits(), 1);
    }

    #[tokio::test]
    async fn build_session_enforces_every_class_limit_and_payload_ceiling() {
        use crate::encoding::v2::keys::scope::DataScope;

        let identity = session_identity(DataScope::LegacyUnscoped, 41);
        let mut session =
            VectorBuildSession::<crate::search::vector::distance::Cosine>::with_test_limits(
                NonZeroU64::new(1).unwrap(),
                1,
                1,
                1,
            );
        let mut cache = session.take_cache(&identity, 8, 4).unwrap();
        cache.put_item(0, 1, None, 8);
        cache.put_item(0, 2, None, 8);
        cache.put_simhash(1, None);
        cache.put_simhash(2, None);
        for node_id in [1, 2] {
            cache.install_loaded_neighbor(
                MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, node_id),
                NeighborRowValue::KnownAbsent,
            );
        }
        session.restore_cache(identity, cache);

        let db = session_test_db("vector-build-session-limits").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        session.enforce_limits(&measured).unwrap();

        assert!(session.item_count() <= 1);
        assert!(session.neighbor_count() <= 1);
        assert!(session.simhash_count() <= 1);
        assert!(session.retained_payload_bytes().unwrap() <= session.max_payload_bytes());
        let stats = session.stats();
        assert!(stats.item_evictions() >= 1);
        assert!(stats.neighbor_evictions() >= 1);
        assert!(stats.simhash_evictions() >= 1);
        assert!(stats.max_retained_payload_bytes() <= 1);
    }

    #[tokio::test]
    async fn failed_session_neighbor_flush_preserves_dirty_state() {
        use crate::encoding::v2::keys::scope::DataScope;

        let identity = session_identity(DataScope::LegacyUnscoped, 51);
        let row = MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(0, 1);
        let mut session = VectorBuildSession::<crate::search::vector::distance::Cosine>::new(
            NonZeroU64::new(1024).unwrap(),
        );
        let mut cache = session.take_cache(&identity, 8, 4).unwrap();
        cache.install_loaded_neighbor(row, NeighborRowValue::KnownAbsent);
        cache
            .stage_loaded_neighbor(row, neighbors(1, vec![2]))
            .unwrap();
        session.restore_cache(identity.clone(), cache);

        let db = session_test_db("vector-build-session-flush-failure").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        measured.fail_next_write();
        assert!(session.flush_all(&measured).is_err());
        assert!(session
            .caches
            .get(&identity)
            .unwrap()
            .neighbor(row)
            .unwrap()
            .is_dirty());

        session.flush_all(&measured).unwrap();
        assert!(!session
            .caches
            .get(&identity)
            .unwrap()
            .neighbor(row)
            .unwrap()
            .is_dirty());
        assert_eq!(session.stats().dirty_neighbor_flushes(), 1);
        assert!(measured.measurement().unwrap().operations() > 0);
    }

    #[tokio::test]
    async fn build_session_reuses_upper_beam_neighbor_item_and_simhash_state() {
        use crate::encoding::v2::keys::scope::DataScope;

        let identity = session_identity(DataScope::LegacyUnscoped, 61);
        let handle = crate::search::vector::ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Cosine,
        >(identity)
        .unwrap();
        let index =
            VectorIndex::<crate::search::vector::distance::Cosine>::from_generation(&handle);
        let db = session_test_db("vector-build-session-traversal-reuse").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        index
            .stage_create(
                &measured,
                crate::search::vector::VectorIndexConfig::new(
                    handle.physical_name(),
                    "embedding",
                    2,
                ),
            )
            .await
            .unwrap();
        let mut session = VectorBuildSession::new(NonZeroU64::new(64 * 1024).unwrap());

        index
            .stage_known_fresh_at_layer_with_session(
                &measured,
                1,
                &[1.0, 0.0],
                2,
                FreshVectorBuildProof::for_test(),
                &mut session,
            )
            .await
            .unwrap();
        session.flush_all(&measured).unwrap();
        index
            .stage_known_fresh_at_layer_with_session(
                &measured,
                2,
                &[0.0, 1.0],
                0,
                FreshVectorBuildProof::for_test(),
                &mut session,
            )
            .await
            .unwrap();
        session.flush_all(&measured).unwrap();
        index
            .stage_upsert_at_layer_with_session(&measured, 2, &[0.2, 0.8], 0, &mut session)
            .await
            .unwrap();
        session.flush_all(&measured).unwrap();

        let stats = session.stats();
        assert!(stats.item_hits() > 0);
        assert!(stats.neighbor_hits() > 0);
        assert!(stats.simhash_hits() > 0);
    }

    /// Nodes of the replay graph; [`replay_layer`] puts some on upper layers.
    const REPLAY_NODES: NodeId = 24;

    /// Distinct vector of replay-graph node `node_id`. Nodes 2, 7, 12, 17,
    /// and 22 have a `0.0` first component.
    fn replay_vector(node_id: NodeId) -> [f32; 2] {
        [
            (node_id % 5) as f32 - 2.0,
            (node_id / 5) as f32 * 0.5 + 0.25,
        ]
    }

    /// Layer of replay-graph node `node_id`.
    const fn replay_layer(node_id: NodeId) -> u16 {
        match node_id % 7 {
            2 | 5 => 1,
            4 => 2,
            _ => 0,
        }
    }

    /// Every committed row of `db`, by key.
    async fn committed_rows(db: &slatedb::Db) -> BTreeMap<bytes::Bytes, bytes::Bytes> {
        let mut rows = db.scan::<std::ops::RangeFull>(..).await.unwrap();
        let mut committed = BTreeMap::new();
        while let Some(row) = rows.next().await.unwrap() {
            committed.insert(row.key, row.value);
        }
        committed
    }

    /// Opens `name` with the replay graph committed through `session`.
    async fn replay_graph(
        name: &str,
        session: &mut VectorBuildSession<Euclidean>,
    ) -> (Arc<slatedb::Db>, VectorIndex<Euclidean>) {
        let handle = crate::search::vector::ValidatedVectorGenerationHandle::create_current::<
            Euclidean,
        >(session_identity(
            crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            91,
        ))
        .unwrap();
        let index = VectorIndex::<Euclidean>::from_generation(&handle);
        let db = session_test_db(name).await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        index
            .stage_create(
                &measured,
                crate::search::vector::VectorIndexConfig::new(
                    handle.physical_name(),
                    "embedding",
                    2,
                ),
            )
            .await
            .unwrap();
        for node_id in 0..REPLAY_NODES {
            index
                .stage_known_fresh_at_layer_with_session(
                    &measured,
                    node_id,
                    &replay_vector(node_id),
                    replay_layer(node_id),
                    FreshVectorBuildProof::for_test(),
                    session,
                )
                .await
                .unwrap();
            session.flush_all(&measured).unwrap();
            session.admit_entity();
        }
        transaction.commit().await.unwrap();
        (db, index)
    }

    fn replay_session() -> VectorBuildSession<Euclidean> {
        VectorBuildSession::new(NonZeroU64::new(1 << 20).unwrap())
    }

    /// Replaying every node's exact vector at its layer, through the session
    /// that built the graph, a cold session, and a one-off cache, attempts no
    /// write and leaves the building session's entries as they were.
    #[tokio::test]
    async fn replayed_upserts_stage_no_row_and_leave_the_session_untouched() {
        let mut session = replay_session();
        let (db, index) = replay_graph("vector-replay-skip", &mut session).await;
        let committed = committed_rows(&db).await;
        let footprint = |session: &VectorBuildSession<Euclidean>| {
            (
                session.item_count(),
                session.neighbor_count(),
                session.simhash_count(),
                session.retained_bytes().unwrap(),
            )
        };
        let built = footprint(&session);
        let mut cold = replay_session();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        // Any attempted write, even a blind delete, fails its upsert.
        measured.fail_next_write();
        for node_id in 0..REPLAY_NODES {
            let (vector, layer) = (replay_vector(node_id), replay_layer(node_id));
            index
                .stage_upsert_at_layer_with_session(
                    &measured,
                    node_id,
                    &vector,
                    layer,
                    &mut session,
                )
                .await
                .unwrap();
            index
                .stage_upsert_at_layer_with_session(&measured, node_id, &vector, layer, &mut cold)
                .await
                .unwrap();
            index
                .stage_upsert_at_layer(&measured, node_id, &vector, layer)
                .await
                .unwrap();
        }
        session.flush_all(&measured).unwrap();
        cold.flush_all(&measured).unwrap();

        let measurement = measured.measurement().unwrap();
        assert_eq!(
            (measurement.operations(), measurement.encoded_bytes()),
            (0, 0)
        );
        assert!(!session.has_dirty_neighbors());
        assert_eq!(footprint(&session), built);
        transaction.commit().await.unwrap();
        assert_eq!(committed_rows(&db).await, committed);
    }

    /// An upsert that differs from the stored node in its vector bytes, even
    /// only in the sign of a zero, or in its layer writes exactly what
    /// deleting the node and then inserting the replacement writes: the path
    /// every upsert took before replays were recognized.
    #[tokio::test]
    async fn upserts_that_change_the_node_write_what_delete_then_insert_writes() {
        for (node_id, vector, layer) in [
            // A new vector at the node's layer.
            (9, [4.5, -1.5], replay_layer(9)),
            // The node's vector one layer up.
            (9, replay_vector(9), replay_layer(9) + 1),
            // An upper-layer node's vector at layer zero.
            (4, replay_vector(4), 0),
            // `-0.0` where the node holds `0.0`.
            (7, [-0.0, replay_vector(7)[1]], replay_layer(7)),
        ] {
            let mut upserted_session = replay_session();
            let (upserted, index) =
                replay_graph("vector-replay-changed", &mut upserted_session).await;
            let mut replaced_session = replay_session();
            let (replaced, _) =
                replay_graph("vector-replay-reference", &mut replaced_session).await;
            assert_eq!(
                committed_rows(&upserted).await,
                committed_rows(&replaced).await
            );

            let transaction = upserted.begin(IsolationLevel::Snapshot).await.unwrap();
            let measured = MeasuredVectorTransaction::new(&transaction);
            index
                .stage_upsert_at_layer_with_session(
                    &measured,
                    node_id,
                    &vector,
                    layer,
                    &mut upserted_session,
                )
                .await
                .unwrap();
            upserted_session.flush_all(&measured).unwrap();
            assert!(measured.measurement().unwrap().operations() > 0);
            transaction.commit().await.unwrap();

            let transaction = replaced.begin(IsolationLevel::Snapshot).await.unwrap();
            let measured = MeasuredVectorTransaction::new(&transaction);
            index
                .stage_delete_with_build_session(&measured, node_id, &mut replaced_session)
                .await
                .unwrap();
            index
                .stage_upsert_at_layer_with_session(
                    &measured,
                    node_id,
                    &vector,
                    layer,
                    &mut replaced_session,
                )
                .await
                .unwrap();
            replaced_session.flush_all(&measured).unwrap();
            transaction.commit().await.unwrap();

            assert_eq!(
                committed_rows(&upserted).await,
                committed_rows(&replaced).await,
                "node {node_id} upserted as {vector:?} at layer {layer}"
            );
            let stored = index.get_item(upserted.as_ref(), node_id).await.unwrap();
            assert_eq!(
                stored.unwrap().vector.as_bytes(),
                UnalignedVector::<f32>::from_slice(&vector).as_bytes()
            );
        }
    }

    /// Without a selected layer, an upsert of the node's exact vector still
    /// replaces it.
    #[tokio::test]
    async fn upserts_at_a_random_layer_always_replace_the_node() {
        let mut session = replay_session();
        let (db, index) = replay_graph("vector-replay-random-layer", &mut session).await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        index
            .insert_with_measured_transaction(
                &measured,
                9,
                &replay_vector(9),
                VectorInsertContract::Upsert,
                None,
            )
            .await
            .unwrap();
        assert!(measured.measurement().unwrap().operations() > 0);
    }

    /// A replay whose entry-candidate row is missing or corrupt takes the
    /// full path, which restores the row.
    #[tokio::test]
    async fn replays_without_a_valid_entry_candidate_row_restore_it() {
        let node_id = 4;
        for corrupt in [false, true] {
            let mut session = replay_session();
            let (db, index) = replay_graph("vector-replay-entry-candidate", &mut session).await;
            let key = index.row_keyspace().key(VectorKey::EntryCandidateNode(
                VectorEntryCandidateNodeKey::new(index.id(), node_id),
            ));
            let damage = db.begin(IsolationLevel::Snapshot).await.unwrap();
            if corrupt {
                damage.put(&key, b"not a layer").unwrap();
            } else {
                damage.delete(&key).unwrap();
            }
            damage.commit().await.unwrap();

            let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let measured = MeasuredVectorTransaction::new(&transaction);
            index
                .stage_upsert_at_layer_with_session(
                    &measured,
                    node_id,
                    &replay_vector(node_id),
                    replay_layer(node_id),
                    &mut session,
                )
                .await
                .unwrap();
            session.flush_all(&measured).unwrap();
            assert!(measured.measurement().unwrap().operations() > 0);
            assert_eq!(
                VectorWriteRows::new(&measured, index.row_keyspace())
                    .entry_candidate_layer(node_id)
                    .await
                    .unwrap(),
                EntryCandidateLayerRow::Present(replay_layer(node_id))
            );
        }
    }

    /// Inside a [`REPLACE_REPLAYS`] scope, a replay takes the full path.
    #[tokio::test]
    async fn replays_inside_a_replace_scope_take_the_full_path() {
        let mut session = replay_session();
        let (db, index) = replay_graph("vector-replay-replace-scope", &mut session).await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        REPLACE_REPLAYS
            .scope(
                (),
                index.stage_upsert_at_layer_with_session(
                    &measured,
                    9,
                    &replay_vector(9),
                    replay_layer(9),
                    &mut session,
                ),
            )
            .await
            .unwrap();
        session.flush_all(&measured).unwrap();
        assert!(measured.measurement().unwrap().operations() > 0);
    }

    #[test]
    fn targeted_invalidation_removes_every_layer_of_exactly_one_node() {
        type Cache = MutationOpCache<crate::search::vector::distance::Cosine>;
        let mut cache = Cache::default();
        for (layer, node_id) in [(0, 1), (3, 1), (5, 1), (0, 2), (5, 2), (3, 3)] {
            cache.put_item(layer, node_id, None, 8);
            cache.install_loaded_neighbor(
                Cache::node_row_id(layer, node_id),
                NeighborRowValue::KnownAbsent,
            );
        }
        cache.record_neighbor_change(Cache::node_row_id(3, 1), NeighborRowValue::KnownAbsent);
        cache.record_neighbor_change(Cache::node_row_id(0, 2), NeighborRowValue::KnownAbsent);

        cache.invalidate_items(1);
        cache.invalidate_neighbors(1);

        for layer in 0..=5 {
            assert!(!cache.items.contains_key(&(layer, 1)));
            assert!(!cache.contains_neighbor(Cache::node_row_id(layer, 1)));
        }
        for (layer, node_id) in [(0, 2), (5, 2), (3, 3)] {
            assert!(cache.item_is_known_absent(layer, node_id));
            assert!(cache.contains_neighbor(Cache::node_row_id(layer, node_id)));
        }
        assert_eq!(cache.item_count(), 3);
        assert_eq!(cache.neighbor_count(), 3);
        assert_recency_is_consistent(&cache);
        assert_eq!(cache.retained_payload_bytes().unwrap(), 3 * 8);
        assert_eq!(
            cache
                .finish_entity_changes()
                .into_keys()
                .collect::<Vec<_>>(),
            vec![Cache::node_row_id(0, 2)]
        );
    }

    #[tokio::test]
    async fn build_session_discard_removes_only_the_unadmitted_entity() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;
        type Cache = MutationOpCache<Cosine>;

        let existing = session_identity(DataScope::LegacyUnscoped, 71);
        let created = session_identity(DataScope::LegacyUnscoped, 72);
        let mut session = VectorBuildSession::<Cosine>::new(NonZeroU64::new(1 << 20).unwrap());

        // An admitted entity leaves clean state for nodes 1 and 2.
        let mut cache = session.take_cache(&existing, 8, 4).unwrap();
        cache.put_item(0, 1, None, 8);
        cache.put_simhash(1, None);
        cache.install_loaded_neighbor(Cache::node_row_id(0, 1), neighbors(1, vec![2]));
        cache.install_loaded_neighbor(Cache::node_row_id(0, 2), neighbors(2, vec![1]));
        session.restore_cache(existing.clone(), cache);
        session.record_entity_changes(&existing, 1, []);
        session.admit_entity();

        // The unadmitted entity inserts node 3, relinks node 2, and creates a namespace.
        let row_2 = Cache::node_row_id(0, 2);
        let row_3 = Cache::node_row_id(0, 3);
        let mut cache = session.take_cache(&existing, 8, 4).unwrap();
        cache.begin_entity();
        cache.put_item(0, 3, None, 8);
        cache.put_item(1, 3, None, 8);
        cache.put_simhash(3, None);
        let proof = cache.prove_new_neighbor_row(row_3).unwrap();
        cache.stage_new_neighbor(proof, neighbors(3, vec![2]));
        cache
            .stage_loaded_neighbor(row_2, neighbors(2, vec![1, 3]))
            .unwrap();
        cache.mark_neighbor_flushed(row_3);
        cache.mark_neighbor_flushed(row_2);
        let changed = cache.finish_entity_changes();
        session.restore_cache(existing.clone(), cache);
        session.record_entity_changes(&existing, 3, changed.into_keys());
        let mut created_cache = session.take_cache(&created, 8, 4).unwrap();
        created_cache.put_item(0, 3, None, 8);
        session.restore_cache(created.clone(), created_cache);
        session.record_entity_changes(&created, 3, []);
        assert!(!session.has_dirty_neighbors());

        session.discard_entity();

        assert!(!session.caches.contains_key(&created));
        let cache = session.caches.get(&existing).unwrap();
        assert!(!cache.items.contains_key(&(0, 3)));
        assert!(!cache.items.contains_key(&(1, 3)));
        assert!(!cache.simhashes.contains_key(&3));
        assert!(!cache.contains_neighbor(row_3));
        assert!(!cache.contains_neighbor(row_2));
        assert!(cache.item_is_known_absent(0, 1));
        assert!(cache.simhashes.contains_key(&1));
        assert_eq!(
            cache
                .neighbor(Cache::node_row_id(0, 1))
                .map(CachedNeighbor::current),
            Some(&neighbors(1, vec![2]))
        );

        // An empty journal discards nothing.
        let retained = session.retained_bytes().unwrap();
        session.discard_entity();
        assert_eq!(session.retained_bytes().unwrap(), retained);
    }

    #[tokio::test]
    async fn build_session_budget_charges_entry_overhead_and_evicts_least_recent() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;

        let identity = session_identity(DataScope::LegacyUnscoped, 81);
        let per_simhash = VECTOR_BUILD_SESSION_SIMHASH_OVERHEAD_BYTES + core::mem::size_of::<u64>();
        let namespace = VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES;
        let mut session = VectorBuildSession::<Cosine>::new(
            NonZeroU64::new(u64::try_from(namespace + per_simhash * 3).unwrap()).unwrap(),
        );
        let mut cache = session.take_cache(&identity, 8, 4).unwrap();
        for node_id in 1..=5 {
            cache.put_simhash(
                node_id,
                Some(crate::search::vector::SimHash::from_bits(node_id)),
            );
        }
        // Touching node 1 leaves nodes 2 and 3 as the least recently used.
        assert!(cache.simhash(1).is_some());
        session.restore_cache(identity.clone(), cache);
        assert_eq!(
            session.retained_bytes().unwrap(),
            namespace + per_simhash * 5
        );
        assert_eq!(
            session.retained_payload_bytes().unwrap(),
            5 * core::mem::size_of::<u64>()
        );

        let db = session_test_db("vector-build-session-byte-budget").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        session
            .enforce_limits(&MeasuredVectorTransaction::new(&transaction))
            .unwrap();

        assert_eq!(
            session.retained_bytes().unwrap(),
            namespace + per_simhash * 3
        );
        let cache = session.caches.get(&identity).unwrap();
        for evicted in [2, 3] {
            assert!(!cache.simhashes.contains_key(&evicted));
        }
        for kept in [1, 4, 5] {
            assert!(cache.simhashes.contains_key(&kept));
        }
        assert_eq!(session.stats().simhash_evictions(), 2);
    }

    #[tokio::test]
    async fn build_session_class_counts_scale_with_the_budget() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;

        let db = session_test_db("vector-build-session-class-counts").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        let identity = session_identity(DataScope::LegacyUnscoped, 82);
        let entries = VECTOR_BUILD_SIMHASH_CACHE_LIMIT + VECTOR_BUILD_ITEM_CACHE_LIMIT;
        // 16 MiB keeps more SimHashes than the old per-step class cap. 1 MiB
        // holds their bytes too, but caps the class at one entry per KiB.
        for (budget, kept) in [(16 << 20, entries), (1 << 20, 1 << 10)] {
            let mut session = VectorBuildSession::<Cosine>::new(NonZeroU64::new(budget).unwrap());
            let mut cache = session.take_cache(&identity, 8, 4).unwrap();
            for node_id in 0..u64::try_from(entries).unwrap() {
                cache.put_simhash(
                    node_id,
                    Some(crate::search::vector::SimHash::from_bits(node_id)),
                );
            }
            session.restore_cache(identity.clone(), cache);
            assert!(session.retained_bytes().unwrap() <= usize::try_from(budget).unwrap());

            session.enforce_limits(&measured).unwrap();

            assert_eq!(session.simhash_count(), kept);
            assert_eq!(
                session.stats().simhash_evictions(),
                u64::try_from(entries - kept).unwrap()
            );
            let cache = session.caches.get(&identity).unwrap();
            assert!(cache
                .simhashes
                .contains_key(&u64::try_from(entries - 1).unwrap()));
            assert_eq!(
                cache.simhashes.contains_key(&0),
                kept == entries,
                "the least recently used SimHashes go first"
            );
        }
    }

    #[test]
    fn build_session_rebinds_its_budget_without_evicting() {
        use crate::search::vector::distance::Cosine;

        let per_simhash = VECTOR_BUILD_SESSION_SIMHASH_OVERHEAD_BYTES + core::mem::size_of::<u64>();
        let full = VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES + 10 * per_simhash;
        let mut session = VectorBuildSession::<Cosine>::with_test_simhashes(
            NonZeroU64::new(1 << 20).unwrap(),
            10,
        );
        let share = full - per_simhash;
        assert!(!session.exceeds_limits());
        session.set_max_retained_bytes(NonZeroU64::new(u64::try_from(share).unwrap()).unwrap());
        assert_eq!(session.max_payload_bytes(), share);
        assert_eq!(
            session.max_simhashes,
            share / VECTOR_BUILD_SESSION_BUDGET_BYTES_PER_ENTRY,
            "class caps scale with the rebound budget"
        );
        assert_eq!(session.simhash_count(), 10, "rebinding evicts nothing");
        assert_eq!(session.retained_bytes().unwrap(), full);
        assert!(session.exceeds_limits());

        // The next eviction honours the rebound byte budget and class caps.
        session.shrink_to(usize::MAX).unwrap();
        assert_eq!(session.simhash_count(), session.max_simhashes);
        assert!(session.retained_bytes().unwrap() <= share);
        assert!(!session.exceeds_limits());
        assert_session_bookkeeping(&session);

        // A budget whose bytes still fit can lower a class cap below the count.
        let mut session = VectorBuildSession::<Cosine>::with_test_simhashes(
            NonZeroU64::new(1 << 20).unwrap(),
            10,
        );
        let dense = 8 * VECTOR_BUILD_SESSION_BUDGET_BYTES_PER_ENTRY;
        assert!(full <= dense);
        session.set_max_retained_bytes(NonZeroU64::new(u64::try_from(dense).unwrap()).unwrap());
        assert!(
            session.exceeds_limits(),
            "10 SimHashes exceed a cap of 8 within the byte budget"
        );
        session.shrink_to(usize::MAX).unwrap();
        assert_eq!(session.simhash_count(), 8);
        assert_eq!(session.stats().simhash_evictions(), 2);
        assert!(!session.exceeds_limits());
        session.set_max_retained_bytes(NonZeroU64::new(1 << 20).unwrap());
        assert!(
            !session.exceeds_limits(),
            "a larger budget only loosens limits"
        );
    }

    #[test]
    fn build_session_reports_each_exceeded_limit() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;

        let identity = session_identity(DataScope::LegacyUnscoped, 87);
        let session = |max_bytes: usize, max_items, max_neighbors, max_simhashes| {
            let mut session = VectorBuildSession::<Cosine>::with_test_limits(
                NonZeroU64::new(u64::try_from(max_bytes).unwrap()).unwrap(),
                max_items,
                max_neighbors,
                max_simhashes,
            );
            let mut cache = session.take_cache(&identity, 8, 4).unwrap();
            for node_id in [1, 2] {
                cache.put_item(0, node_id, None, 8);
                cache.put_simhash(node_id, None);
                cache.install_loaded_neighbor(
                    MutationOpCache::<Cosine>::node_row_id(0, node_id),
                    NeighborRowValue::KnownAbsent,
                );
            }
            session.restore_cache(identity.clone(), cache);
            session
        };
        let charged = session(1 << 20, 2, 2, 2).retained_bytes().unwrap();

        for (max_bytes, max_items, max_neighbors, max_simhashes, exceeds) in [
            (charged, 2, 2, 2, false),
            (charged - 1, 2, 2, 2, true),
            (charged, 1, 2, 2, true),
            (charged, 2, 1, 2, true),
            (charged, 2, 2, 1, true),
        ] {
            let mut session = session(max_bytes, max_items, max_neighbors, max_simhashes);
            assert_eq!(
                session.exceeds_limits(),
                exceeds,
                "{max_bytes} bytes, caps {max_items}/{max_neighbors}/{max_simhashes}"
            );
            // Exactly an exceeded limit makes the next eviction pass evict.
            session.shrink_to(usize::MAX).unwrap();
            let stats = session.stats();
            assert_eq!(
                stats.item_evictions() + stats.neighbor_evictions() + stats.simhash_evictions() > 0,
                exceeds
            );
            assert!(!session.exceeds_limits());
        }
    }

    #[test]
    fn build_session_shrinks_clean_state_without_a_transaction() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;

        let per_simhash = VECTOR_BUILD_SESSION_SIMHASH_OVERHEAD_BYTES + core::mem::size_of::<u64>();
        let full = VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES + 10 * per_simhash;
        let mut session = VectorBuildSession::<Cosine>::with_test_simhashes(
            NonZeroU64::new(1 << 20).unwrap(),
            10,
        );
        assert_eq!(session.retained_bytes().unwrap(), full);

        session.shrink_to(full - per_simhash).unwrap();
        assert_eq!(session.simhash_count(), 9);
        assert!(
            session
                .caches
                .values()
                .all(|cache| !cache.simhashes.contains_key(&0)),
            "the least recently used SimHash goes first"
        );
        session.shrink_to(0).unwrap();
        assert!(session.caches.is_empty());
        assert_eq!(session.retained_bytes().unwrap(), 0);
        assert_eq!(session.stats().simhash_evictions(), 10);

        // A dirty row cannot be evicted without a transaction to flush it.
        let identity = session_identity(DataScope::LegacyUnscoped, 85);
        let row = MutationOpCache::<Cosine>::node_row_id(0, 1);
        let mut dirty = VectorBuildSession::<Cosine>::new(NonZeroU64::new(1 << 20).unwrap());
        let mut cache = dirty.take_cache(&identity, 8, 4).unwrap();
        cache.install_loaded_neighbor(row, NeighborRowValue::KnownAbsent);
        cache
            .stage_loaded_neighbor(row, neighbors(1, vec![2]))
            .unwrap();
        dirty.restore_cache(identity.clone(), cache);
        assert!(matches!(
            dirty.shrink_to(0),
            Err(HelixDbError::InvariantViolation(_))
        ));
        assert!(
            dirty
                .caches
                .get(&identity)
                .unwrap()
                .neighbor(row)
                .unwrap()
                .is_dirty(),
            "a refused eviction keeps the dirty row"
        );
        assert_session_bookkeeping(&dirty);
    }

    #[test]
    fn large_build_sessions_free_their_entries_off_the_dropping_thread() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;

        const NAMESPACES: u64 = 64;
        let item = Arc::new(Item::<Cosine>::new(vec![1.0, 0.0]));
        let session = |payload_bytes| {
            let mut session = VectorBuildSession::<Cosine>::new(NonZeroU64::new(1 << 40).unwrap());
            for physical_index_id in 0..NAMESPACES {
                let identity = session_identity(DataScope::LegacyUnscoped, 86 + physical_index_id);
                let mut cache = session.take_cache(&identity, 8, 4).unwrap();
                cache.put_item(0, 1, Some(Arc::clone(&item)), payload_bytes);
                cache.put_simhash(1, None);
                session.restore_cache(identity, cache);
            }
            session.admit_entity();
            // One unflushed row and one unadmitted entity in the first namespace.
            let identity = session_identity(DataScope::LegacyUnscoped, 86);
            let row = MutationOpCache::<Cosine>::node_row_id(0, 1);
            let mut cache = session.take_cache(&identity, 8, 4).unwrap();
            cache.install_loaded_neighbor(row, NeighborRowValue::KnownAbsent);
            cache
                .stage_loaded_neighbor(row, neighbors(1, vec![2]))
                .unwrap();
            session.restore_cache(identity.clone(), cache);
            session.record_entity_changes(&identity, 1, [row]);
            assert!(session.has_dirty_neighbors());
            assert_eq!(
                session.victims.items.len(),
                usize::try_from(NAMESPACES).unwrap()
            );
            assert_session_bookkeeping(&session);
            session
        };

        // A small session keeps its state and frees it inline, before `drop` returns.
        let mut small = session(8);
        assert!(small.free_state_in_background().is_none());
        assert_eq!(small.caches.len(), usize::try_from(NAMESPACES).unwrap());
        drop(small);
        assert_eq!(Arc::strong_count(&item), 1);

        // A large one hands its entries, eviction index, dirty set, and entity
        // journal to another thread, which frees them while the session lives.
        let mut large = session(VECTOR_BUILD_SESSION_BACKGROUND_DROP_BYTES);
        assert!(large.retained_bytes().unwrap() >= VECTOR_BUILD_SESSION_BACKGROUND_DROP_BYTES);
        let freeing = large
            .free_state_in_background()
            .expect("a large session frees on another thread");
        assert_ne!(freeing.thread().id(), std::thread::current().id());
        assert!(large.caches.is_empty());
        assert!(large.victims.items.is_empty());
        assert!(large.victims.neighbors.is_empty());
        assert!(large.victims.simhashes.is_empty());
        assert!(large.dirty.is_empty());
        assert!(large.entity_changes.is_empty());
        assert_eq!(large.retained_bytes().unwrap(), 0);
        assert_session_bookkeeping(&large);
        freeing.join().unwrap();
        assert_eq!(
            Arc::strong_count(&item),
            1,
            "the freeing thread dropped every entry"
        );
        drop(large);

        // Dropping a large session hands its state off the same way.
        drop(session(VECTOR_BUILD_SESSION_BACKGROUND_DROP_BYTES));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while Arc::strong_count(&item) > 1 {
            assert!(
                std::time::Instant::now() < deadline,
                "a background drop frees the session's entries"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[test]
    fn build_session_stats_reset_without_dropping_state() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;

        let identity = session_identity(DataScope::LegacyUnscoped, 83);
        let mut session = VectorBuildSession::<Cosine>::new(NonZeroU64::new(1 << 20).unwrap());
        let mut cache = session.take_cache(&identity, 8, 4).unwrap();
        cache.put_item(0, 1, None, 8);
        assert!(cache.item(0, 1).is_some());
        assert!(cache.item(0, 2).is_none());
        session.restore_cache(identity, cache);
        assert_eq!(session.stats().item_hits(), 1);
        assert_eq!(session.stats().item_misses(), 1);

        session.reset_stats();

        assert_eq!(session.stats(), VectorBuildSessionStats::default());
        assert_eq!(session.item_count(), 1);
    }

    /// Recomputes every session-level index by scanning namespaces.
    ///
    /// Attached namespaces must be non-empty and hold no unfolded telemetry, and
    /// the running totals, victim index, and dirty set must equal the scan.
    fn assert_session_bookkeeping<D: Distance>(session: &VectorBuildSession<D>) {
        let mut footprint = SessionFootprint::default();
        let mut victims = SessionVictims::default();
        let mut dirty = BTreeSet::new();
        for (identity, cache) in &session.caches {
            assert!(
                cache.item_count() + cache.neighbor_count() + cache.simhash_count() > 0,
                "an attached namespace retains an entry"
            );
            assert_eq!(cache.stats, VectorBuildSessionStats::default());
            footprint.items += cache.item_count();
            footprint.neighbors += cache.neighbor_count();
            footprint.simhashes += cache.simhash_count();
            footprint.payload_bytes += cache.retained_payload_bytes as u128;
            for victim in VectorBuildSession::<D>::namespace_victims(identity, cache) {
                assert!(victims.of_kind(victim.kind).insert(victim));
            }
            if cache.oldest_dirty_neighbor().is_some() {
                dirty.insert(identity.clone());
            }
        }
        assert_eq!(session.footprint, footprint);
        assert_eq!(session.victims.items, victims.items);
        assert_eq!(session.victims.neighbors, victims.neighbors);
        assert_eq!(session.victims.simhashes, victims.simhashes);
        assert_eq!(session.dirty, dirty);
    }

    /// Returns the recency of every attached entry, asserting each is unique.
    fn entry_touches<D: Distance>(session: &VectorBuildSession<D>) -> BTreeSet<CacheSequence> {
        let touches = session
            .caches
            .values()
            .flat_map(|cache| {
                assert_recency_is_consistent(cache);
                cache
                    .items
                    .iter()
                    .map(|(_, cached)| cached.last_touch)
                    .chain(
                        cache
                            .neighbor_rows
                            .iter()
                            .map(|(_, cached)| cached.last_touch()),
                    )
                    .chain(cache.simhashes.iter().map(|(_, cached)| cached.last_touch))
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            touches.len(),
            session.item_count() + session.neighbor_count() + session.simhash_count()
        );
        touches
    }

    #[test]
    fn build_session_entry_overheads_cover_their_structures() {
        use crate::search::vector::distance::Cosine;
        use core::mem::size_of;

        // A hash-map slot holds its key, its value, and one control byte, and a
        // table is at most seven-eighths full. A recency order allocates at
        // most four entries per value. Every heap allocation carries at least
        // a 16-byte allocator header.
        const ALLOCATION_HEADER: usize = 16;
        let slot = |key: usize, value: usize| (key + value + 1) * 8 / 7;
        // An item shares one reference-counted decoded value and its vector buffer.
        let item = slot(size_of::<(u16, NodeId)>(), size_of::<CachedItem<Cosine>>())
            + 4 * size_of::<(CacheSequence, (u16, NodeId))>()
            + 2 * size_of::<usize>()
            + size_of::<Item<'static, Cosine>>()
            + 2 * ALLOCATION_HEADER;
        // A dirty neighbor row holds its original and current neighbor lists
        // and a dirty-set entry.
        let neighbor = slot(size_of::<NeighborRowId>(), size_of::<CachedNeighbor>())
            + 5 * size_of::<(CacheSequence, NeighborRowId)>()
            + 2 * ALLOCATION_HEADER;
        let simhash = slot(size_of::<NodeId>(), size_of::<CachedSimHash>())
            + 4 * size_of::<(CacheSequence, NodeId)>();
        for (kind, needed, charged) in [
            ("item", item, VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES),
            (
                "neighbor",
                neighbor,
                VECTOR_BUILD_SESSION_NEIGHBOR_OVERHEAD_BYTES,
            ),
            (
                "SimHash",
                simhash,
                VECTOR_BUILD_SESSION_SIMHASH_OVERHEAD_BYTES,
            ),
        ] {
            assert!(
                needed <= charged,
                "a {kind} entry needs {needed} bookkeeping bytes but is charged {charged}"
            );
        }
    }

    #[test]
    fn build_session_namespace_overhead_covers_its_structures() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;
        use core::mem::size_of;

        // A hash-map slot holds its key, its value, and one control byte, and a
        // table is at most seven-eighths full. The smallest table has four
        // buckets plus a control group, the smallest recency order four
        // entries, and the smallest B-tree set one leaf.
        let slot = |key: usize, value: usize| (key + value + 1) * 8 / 7;
        let table = |key: usize, value: usize| 4 * (key + value) + 4 + 16;
        let order = |entry: usize| 4 * entry;
        let leaf = |key: usize| 16 + 11 * key;
        let identity = session_identity(DataScope::LegacyUnscoped, 1);
        let name = identity.physical_name().len();
        let namespace = slot(
            size_of::<VectorGenerationIdentity>(),
            size_of::<MutationOpCache<Cosine>>(),
        ) + name
            + 3 * (size_of::<SessionEvictionOrder>() + name)
            + size_of::<VectorGenerationIdentity>()
            + name
            + table(size_of::<(u16, NodeId)>(), size_of::<CachedItem<Cosine>>())
            + table(size_of::<NeighborRowId>(), size_of::<CachedNeighbor>())
            + table(size_of::<NodeId>(), size_of::<CachedSimHash>())
            + order(size_of::<(CacheSequence, (u16, NodeId))>())
            + order(size_of::<(CacheSequence, NeighborRowId)>())
            + leaf(size_of::<(CacheSequence, NeighborRowId)>())
            + order(size_of::<(CacheSequence, NodeId)>());
        assert!(
            namespace <= VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES,
            "a namespace needs {namespace} bookkeeping bytes"
        );
    }

    #[tokio::test]
    async fn build_session_evicts_global_lru_across_namespaces_and_drops_emptied_ones() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;

        let identities = [91, 92, 93].map(|id| session_identity(DataScope::LegacyUnscoped, id));
        let mut session = VectorBuildSession::<Cosine>::new(NonZeroU64::new(1 << 20).unwrap());
        // Namespace 91 holds the two oldest entries; the others interleave.
        for (namespace, node_id) in [(0, 1), (0, 2), (1, 1), (2, 1), (1, 2), (2, 2)] {
            let identity = &identities[namespace];
            let mut cache = session.take_cache(identity, 8, 4).unwrap();
            cache.put_simhash(
                node_id,
                Some(crate::search::vector::SimHash::from_bits(node_id)),
            );
            session.restore_cache(identity.clone(), cache);
        }
        assert_eq!(session.caches.len(), 3);
        assert_session_bookkeeping(&session);

        let per_simhash = VECTOR_BUILD_SESSION_SIMHASH_OVERHEAD_BYTES + core::mem::size_of::<u64>();
        let budget = 2 * VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES + 4 * per_simhash;
        session.max_payload_bytes = budget;
        let db = session_test_db("vector-build-session-global-lru").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        session
            .enforce_limits(&MeasuredVectorTransaction::new(&transaction))
            .unwrap();

        assert!(
            !session.caches.contains_key(&identities[0]),
            "a namespace emptied by eviction is dropped and uncharged"
        );
        assert_eq!(session.caches.len(), 2);
        assert_eq!(session.simhash_count(), 4);
        assert_eq!(session.retained_bytes().unwrap(), budget);
        assert_eq!(session.stats().simhash_evictions(), 2);
        assert_session_bookkeeping(&session);
    }

    #[tokio::test]
    async fn build_session_keeps_only_budgeted_namespaces_of_many_partitions() {
        use crate::encoding::v2::keys::scope::{DataScope, TenantId};
        use crate::search::vector::distance::Cosine;

        const TENANTS: u64 = 2_000;
        const KEPT: u64 = 64;
        let per_namespace = VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES
            + VECTOR_BUILD_SESSION_SIMHASH_OVERHEAD_BYTES
            + core::mem::size_of::<u64>();
        let kept_bytes = usize::try_from(KEPT).unwrap() * per_namespace;
        let mut session = VectorBuildSession::<Cosine>::new(
            NonZeroU64::new(u64::try_from(kept_bytes).unwrap()).unwrap(),
        );
        let db = session_test_db("vector-build-session-many-partitions").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        let tenant = |tenant: u64| {
            session_identity(
                DataScope::Tenant(TenantId::from_u128(u128::from(tenant))),
                1,
            )
        };
        for id in 0..TENANTS {
            let identity = tenant(id);
            let mut cache = session.take_cache(&identity, 8, 4).unwrap();
            cache.put_simhash(1, Some(crate::search::vector::SimHash::from_bits(1)));
            session.restore_cache(identity, cache);
            session.enforce_limits(&measured).unwrap();
        }

        assert_eq!(session.caches.len(), usize::try_from(KEPT).unwrap());
        assert_eq!(session.retained_bytes().unwrap(), kept_bytes);
        assert!((TENANTS - KEPT..TENANTS).all(|id| session.caches.contains_key(&tenant(id))));
        assert_eq!(session.stats().simhash_evictions(), TENANTS - KEPT);
        assert_session_bookkeeping(&session);
    }

    /// One random transition of a build session for the bookkeeping model.
    #[derive(Debug, Clone)]
    enum SessionOp {
        Item { namespace: u64, node_id: NodeId },
        SimHash { namespace: u64, node_id: NodeId },
        Neighbor { namespace: u64, node_id: NodeId },
        Stage { namespace: u64, node_id: NodeId },
        Enforce { budget: usize },
        Flush,
        Admit,
        Discard,
    }

    fn session_op() -> impl Strategy<Value = SessionOp> {
        let target = || (0_u64..4, 1_u64..8);
        prop_oneof![
            target().prop_map(|(namespace, node_id)| SessionOp::Item { namespace, node_id }),
            target().prop_map(|(namespace, node_id)| SessionOp::SimHash { namespace, node_id }),
            target().prop_map(|(namespace, node_id)| SessionOp::Neighbor { namespace, node_id }),
            target().prop_map(|(namespace, node_id)| SessionOp::Stage { namespace, node_id }),
            (0_usize..4 * VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES)
                .prop_map(|budget| SessionOp::Enforce { budget }),
            Just(SessionOp::Flush),
            Just(SessionOp::Admit),
            Just(SessionOp::Discard),
        ]
    }

    /// Applies one entity mutation to a namespace the way planning does.
    fn mutate_session_namespace<D: Distance>(
        session: &mut VectorBuildSession<D>,
        namespace: u64,
        node_id: NodeId,
        mutate: impl FnOnce(&mut MutationOpCache<D>),
    ) {
        use crate::encoding::v2::keys::scope::DataScope;

        let identity = session_identity(DataScope::LegacyUnscoped, 200 + namespace);
        let mut cache = session.take_cache(&identity, 8, 4).unwrap();
        cache.begin_entity();
        mutate(&mut cache);
        let changed = cache.finish_entity_changes();
        session.restore_cache(identity.clone(), cache);
        session.record_entity_changes(&identity, node_id, changed.into_keys());
    }

    proptest! {
        #[test]
        fn build_session_bookkeeping_matches_a_namespace_scan(
            ops in prop::collection::vec(session_op(), 1..48),
        ) {
            use crate::search::vector::distance::Cosine;
            type Cache = MutationOpCache<Cosine>;
            const UNBOUNDED: usize = 1 << 20;

            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let db = session_test_db("vector-build-session-bookkeeping").await;
                    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
                    let measured = MeasuredVectorTransaction::new(&transaction);
                    let mut session = VectorBuildSession::<Cosine>::new(
                        NonZeroU64::new(u64::try_from(UNBOUNDED).unwrap()).unwrap(),
                    );
                    for op in ops {
                        match op {
                            SessionOp::Item { namespace, node_id } => {
                                mutate_session_namespace(&mut session, namespace, node_id, |cache| {
                                    cache.put_item(0, node_id, None, 8);
                                });
                            }
                            SessionOp::SimHash { namespace, node_id } => {
                                mutate_session_namespace(&mut session, namespace, node_id, |cache| {
                                    cache.put_simhash(
                                        node_id,
                                        Some(crate::search::vector::SimHash::from_bits(node_id)),
                                    );
                                });
                            }
                            SessionOp::Neighbor { namespace, node_id } => {
                                mutate_session_namespace(&mut session, namespace, node_id, |cache| {
                                    cache.install_loaded_neighbor(
                                        Cache::node_row_id(0, node_id),
                                        neighbors(node_id, vec![node_id + 8]),
                                    );
                                });
                            }
                            SessionOp::Stage { namespace, node_id } => {
                                mutate_session_namespace(&mut session, namespace, node_id, |cache| {
                                    let row = Cache::node_row_id(0, node_id);
                                    if cache.contains_neighbor(row) {
                                        cache
                                            .stage_loaded_neighbor(
                                                row,
                                                neighbors(node_id, vec![node_id + 9]),
                                            )
                                            .unwrap();
                                    }
                                });
                            }
                            SessionOp::Enforce { budget } => {
                                session.max_payload_bytes = budget;
                                let before = entry_touches(&session);
                                session.enforce_limits(&measured).unwrap();
                                let after = entry_touches(&session);
                                session.max_payload_bytes = UNBOUNDED;
                                assert!(session.retained_bytes().unwrap() <= budget);
                                assert!(after.is_subset(&before));
                                let newest_evicted = before.difference(&after).last();
                                assert!(
                                    newest_evicted
                                        .zip(after.first())
                                        .is_none_or(|(evicted, kept)| evicted < kept),
                                    "eviction removes the globally least recent entries"
                                );
                            }
                            SessionOp::Flush => {
                                session.flush_all(&measured).unwrap();
                                assert!(!session.has_dirty_neighbors());
                            }
                            SessionOp::Admit => session.admit_entity(),
                            SessionOp::Discard => {
                                session.flush_all(&measured).unwrap();
                                session.discard_entity();
                            }
                        }
                        assert_session_bookkeeping(&session);
                    }
                });
        }
    }

    proptest! {
        #[test]
        fn targeted_invalidation_matches_a_full_scan_model(
            entries in prop::collection::vec((0_u16..6, 1_u64..6, any::<bool>()), 0..48),
            victim in 1_u64..6,
        ) {
            type Cache = MutationOpCache<crate::search::vector::distance::Cosine>;
            let mut cache = Cache::default();
            let mut items = BTreeSet::new();
            let mut rows = BTreeSet::new();
            for (layer, node_id, neighbor) in entries {
                if neighbor {
                    cache.install_loaded_neighbor(
                        Cache::node_row_id(layer, node_id),
                        NeighborRowValue::KnownAbsent,
                    );
                    rows.insert((layer, node_id));
                } else {
                    cache.put_item(layer, node_id, None, 1);
                    items.insert((layer, node_id));
                }
            }
            cache.invalidate_items(victim);
            cache.invalidate_neighbors(victim);
            items.retain(|(_, node_id)| *node_id != victim);
            rows.retain(|(_, node_id)| *node_id != victim);
            for layer in 0..6 {
                for node_id in 1..6 {
                    prop_assert_eq!(
                        cache.items.contains_key(&(layer, node_id)),
                        items.contains(&(layer, node_id))
                    );
                    prop_assert_eq!(
                        cache.contains_neighbor(Cache::node_row_id(layer, node_id)),
                        rows.contains(&(layer, node_id))
                    );
                }
            }
            prop_assert_eq!(cache.item_count(), items.len());
            prop_assert_eq!(cache.neighbor_count(), rows.len());
            assert_recency_is_consistent(&cache);
            prop_assert_eq!(cache.retained_payload_bytes().unwrap(), items.len());
        }
    }

    proptest! {
        #[test]
        fn random_neighbor_operations_match_the_reference_model(
            operations in prop::collection::vec((0_u8..6, 1_u64..=4, 0_u8..=8), 0..128),
        ) {
            let mut cache = MutationOpCache::<crate::search::vector::distance::Cosine>::default();
            let mut reference = HashMap::<NodeId, ReferenceNeighbor>::new();
            let mut next_touch = 0_u64;

            for (operation, node_id, value_token) in operations {
                let row = MutationOpCache::<crate::search::vector::distance::Cosine>::node_row_id(
                    0,
                    node_id,
                );
                let value = generated_value(node_id, value_token);
                match operation {
                    0 => {
                        let installed = cache.install_loaded_neighbor(row, value.clone());
                        let expected = match reference.entry(node_id) {
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert(ReferenceNeighbor::clean(value, next_touch));
                                next_touch += 1;
                                true
                            }
                            std::collections::hash_map::Entry::Occupied(_) => false,
                        };
                        prop_assert_eq!(installed, expected);
                    }
                    1 => {
                        let result = cache.stage_loaded_neighbor(row, value.clone());
                        let Some(cached) = reference.get_mut(&node_id) else {
                            prop_assert!(result.is_err());
                            next_touch += 1;
                            assert_matches_reference(&cache, &reference);
                            continue;
                        };
                        prop_assert!(result.is_ok());
                        cached.stage(value, next_touch);
                        next_touch += 1;
                    }
                    2 => {
                        cache.mark_neighbor_flushed(row);
                        reference
                            .get_mut(&node_id)
                            .into_iter()
                            .for_each(ReferenceNeighbor::mark_flushed);
                    }
                    3 => {
                        prop_assert_eq!(
                            cache.remove_neighbor(row).is_some(),
                            reference.remove(&node_id).is_some(),
                        );
                    }
                    4 => match cache.prove_new_neighbor_row(row) {
                        Ok(proof) => {
                            prop_assert!(!reference.contains_key(&node_id));
                            cache.stage_new_neighbor(proof, value.clone());
                            let mut cached = ReferenceNeighbor::clean(
                                NeighborRowValue::KnownAbsent,
                                next_touch,
                            );
                            cached.stage(value, next_touch);
                            reference.insert(node_id, cached);
                            next_touch += 1;
                        }
                        Err(_) => prop_assert!(reference.contains_key(&node_id)),
                    },
                    5 => {
                        let touched = cache.touched_neighbor(row).map(CachedNeighbor::current);
                        let expected = reference.get_mut(&node_id).map(|cached| {
                            cached.last_touch = next_touch;
                            next_touch += 1;
                            &cached.current
                        });
                        prop_assert_eq!(touched, expected);
                    }
                    _ => unreachable!("generated operation is in the closed 0..6 range"),
                }
                assert_matches_reference(&cache, &reference);
            }
        }
    }

    /// One transition of a [`RecencyMap`] and its ordered-set model.
    #[derive(Debug, Clone)]
    enum RecencyOp {
        /// Caches a key at a touch newer than every other.
        Insert(NodeId),
        /// Touches a key, if cached, at a touch newer than every other.
        Touch(NodeId),
        /// Caches a key at a touch older than every earlier one, as after
        /// another namespace renumbered the session's sequence.
        Older(NodeId),
        Remove(NodeId),
        Renumber,
    }

    fn recency_op() -> impl Strategy<Value = RecencyOp> {
        prop_oneof![
            (0_u64..24).prop_map(RecencyOp::Insert),
            (0_u64..24).prop_map(RecencyOp::Touch),
            (0_u64..24).prop_map(RecencyOp::Older),
            (0_u64..24).prop_map(RecencyOp::Remove),
            Just(RecencyOp::Renumber),
        ]
    }

    proptest! {
        /// The append order reports exactly what the ordered set it replaced
        /// reported, and stays within its compaction and allocation bounds.
        #[test]
        fn recency_map_orders_values_like_an_ordered_set(
            ops in prop::collection::vec(recency_op(), 0..400),
        ) {
            let mut map = RecencyMap::<NodeId, CachedSimHash>::default();
            let mut model = BTreeMap::<NodeId, CacheSequence>::new();
            let (mut newer, mut older) = (1_u64 << 40, (1_u64 << 40) - 1);
            let simhash = |last_touch| CachedSimHash { value: None, last_touch };
            for op in ops {
                match op {
                    RecencyOp::Insert(key) => {
                        let touch = CacheSequence(newer);
                        newer += 1;
                        map.insert(key, simhash(touch));
                        model.insert(key, touch);
                    }
                    RecencyOp::Touch(key) => {
                        let touch = CacheSequence(newer);
                        newer += 1;
                        let touched = map.touch(key, touch).map(|cached| cached.last_touch);
                        let expected = model.get_mut(&key).map(|last_touch| {
                            *last_touch = touch;
                            touch
                        });
                        prop_assert_eq!(touched, expected);
                    }
                    RecencyOp::Older(key) => {
                        let touch = CacheSequence(older);
                        older -= 1;
                        map.insert(key, simhash(touch));
                        model.insert(key, touch);
                    }
                    RecencyOp::Remove(key) => {
                        prop_assert_eq!(map.remove(&key).is_some(), model.remove(&key).is_some());
                    }
                    RecencyOp::Renumber => {
                        let mut ranked = model
                            .iter()
                            .map(|(key, touch)| (*touch, *key))
                            .collect::<Vec<_>>();
                        ranked.sort_unstable();
                        model = (0_u64..)
                            .map(CacheSequence)
                            .zip(ranked)
                            .map(|(touch, (_, key))| (key, touch))
                            .collect();
                        map.renumber(|key| model[key]);
                    }
                }
                let expected = model
                    .iter()
                    .map(|(key, touch)| (*touch, *key))
                    .collect::<BTreeSet<_>>();
                prop_assert_eq!(map.oldest(), expected.first().copied());
                prop_assert_eq!(
                    map.oldest_first()
                        .map(|(key, cached)| (cached.last_touch, key))
                        .collect::<Vec<_>>(),
                    expected.into_iter().collect::<Vec<_>>()
                );
                prop_assert_eq!(map.len(), model.len());
                prop_assert!(map.order.len() <= 2 * map.len());
                prop_assert!(map.order.capacity() <= 4 * map.len().max(1));
            }
        }
    }

    /// Random touches keep the order's allocation within four entries per
    /// value without reallocating each compaction, and evicting to a tenth,
    /// then to empty, shrinks it with the values while it keeps LRU order.
    #[test]
    fn recency_order_allocation_follows_its_values() {
        let within_bound =
            |map: &RecencyMap<NodeId, CachedSimHash>| map.order.capacity() <= 4 * map.len().max(1);
        let simhash_limit = u64::try_from(VECTOR_BUILD_SIMHASH_CACHE_LIMIT).unwrap();
        for values in [1, 3, 512, simhash_limit] {
            let mut map = RecencyMap::<NodeId, CachedSimHash>::default();
            let mut model = BTreeMap::<NodeId, CacheSequence>::new();
            let mut sequence = 0_u64;
            let mut stamp = || {
                sequence += 1;
                CacheSequence(sequence)
            };
            for node_id in 0..values {
                let touch = stamp();
                map.insert(
                    node_id,
                    CachedSimHash {
                        value: None,
                        last_touch: touch,
                    },
                );
                model.insert(node_id, touch);
                assert!(within_bound(&map));
            }
            let mut random = 0x9E37_79B9_7F4A_7C15_u64;
            let mut capacity = map.order.capacity();
            let mut reallocations = 0;
            for _ in 0..16 * values {
                random = random
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let (node_id, touch) = ((random >> 33) % values, stamp());
                assert!(map.touch(node_id, touch).is_some());
                model.insert(node_id, touch);
                assert!(within_bound(&map), "{values} values");
                if map.order.capacity() != capacity {
                    capacity = map.order.capacity();
                    reallocations += 1;
                }
            }
            assert!(
                reallocations <= 4,
                "{values} values reallocated {reallocations} times"
            );
            for retained in [(values / 10).max(1), 0] {
                while map.len() as u64 > retained {
                    let (touch, oldest) = map.oldest().unwrap();
                    assert_eq!(model.remove(&oldest), Some(touch));
                    assert!(map.remove(&oldest).is_some());
                    assert!(
                        within_bound(&map),
                        "{values} values evicted to {}",
                        map.len()
                    );
                }
                let mut expected = model
                    .iter()
                    .map(|(node_id, touch)| (*touch, *node_id))
                    .collect::<Vec<_>>();
                expected.sort_unstable();
                assert_eq!(
                    map.oldest_first()
                        .map(|(node_id, cached)| (cached.last_touch, node_id))
                        .collect::<Vec<_>>(),
                    expected
                );
            }
            assert!(map.order.is_empty());
        }
    }

    #[test]
    fn hits_and_replacements_defer_local_eviction() {
        let mut cache = MutationOpCache::<crate::search::vector::distance::Cosine>::default();
        for node_id in 1..=3 {
            cache.put_item(0, node_id, None, 1);
            cache.put_simhash(node_id, None);
        }
        assert!(matches!(cache.item(0, 1), Some(None)));
        cache.put_item(0, 2, None, 1);
        assert_eq!(cache.simhash(2), Some(None));
        assert_eq!(cache.simhash(1), Some(None));
        assert_recency_is_consistent(&cache);

        let evicted_items = std::iter::from_fn(|| {
            let (_, (_, node_id)) = cache.items.oldest()?;
            assert!(cache.evict_oldest_item());
            Some(node_id)
        })
        .collect::<Vec<_>>();
        assert_eq!(evicted_items, [3, 1, 2]);
        let evicted_simhashes = std::iter::from_fn(|| {
            let (_, node_id) = cache.simhashes.oldest()?;
            assert!(cache.evict_oldest_simhash());
            Some(node_id)
        })
        .collect::<Vec<_>>();
        assert_eq!(evicted_simhashes, [3, 2, 1]);
        assert!(!cache.evict_oldest_item());
        assert!(!cache.evict_oldest_simhash());
        assert_eq!(cache.retained_payload_bytes().unwrap(), 0);
        assert_eq!(
            (cache.stats.item_evictions, cache.stats.simhash_evictions),
            (3, 3)
        );
        assert!(cache.items.order.is_empty() && cache.simhashes.order.is_empty());
    }

    /// Hits, inserts, and staged rows across namespaces keep every entry
    /// within the budget after each eviction, and every recency order within
    /// its bound, while eviction flushes dirty rows it takes.
    #[tokio::test]
    async fn build_session_budget_holds_under_hit_and_insert_churn() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;
        type Cache = MutationOpCache<Cosine>;

        let db = session_test_db("vector-build-session-churn").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        let item = Arc::new(Item::<Cosine>::new(vec![1.0, 0.0]));
        let budget = 3 * VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES
            + 48 * (VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES + 16);
        let mut session = VectorBuildSession::<Cosine>::new(
            NonZeroU64::new(u64::try_from(budget).unwrap()).unwrap(),
        );
        let mut random = 0x9E37_79B9_7F4A_7C15_u64;
        for round in 0..300_u64 {
            let identity = session_identity(DataScope::LegacyUnscoped, 300 + round % 3);
            let mut cache = session.take_cache(&identity, 8, 4).unwrap();
            for _ in 0..64 {
                random = random
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let node_id = (random >> 33) % 96;
                let row = Cache::node_row_id(0, node_id);
                match random >> 60 {
                    0..=8 => {
                        if cache.item(0, node_id).is_none() {
                            cache.put_item(0, node_id, Some(Arc::clone(&item)), 16);
                        }
                    }
                    9..=11 => {
                        if cache.simhash(node_id).is_none() {
                            cache.put_simhash(node_id, None);
                        }
                    }
                    12..=13 => {
                        if cache.touched_neighbor(row).is_none() {
                            cache.install_loaded_neighbor(row, NeighborRowValue::KnownAbsent);
                        }
                    }
                    _ => {
                        if cache.contains_neighbor(row) {
                            cache
                                .stage_loaded_neighbor(
                                    row,
                                    neighbors(node_id, vec![node_id + 100 + round]),
                                )
                                .unwrap();
                        }
                    }
                }
            }
            assert_recency_is_consistent(&cache);
            session.restore_cache(identity, cache);
            session.admit_entity();
            session.enforce_limits(&measured).unwrap();
            assert!(session.retained_bytes().unwrap() <= budget);
            assert_session_bookkeeping(&session);
        }
        let stats = session.stats();
        assert!(stats.item_hits() > 0 && stats.neighbor_hits() > 0 && stats.simhash_hits() > 0);
        assert!(stats.item_evictions() > 0);
        assert!(stats.neighbor_evictions() > 0);
        assert!(stats.simhash_evictions() > 0);
        assert!(stats.dirty_neighbor_flushes() > 0);
    }

    /// Eviction under pressure never drops a staged row: without a
    /// transaction it refuses, and with one every evicted dirty row is
    /// written before it leaves the cache.
    #[tokio::test]
    async fn dirty_rows_survive_eviction_pressure() {
        use crate::encoding::v2::keys::scope::DataScope;
        use crate::search::vector::distance::Cosine;
        type Cache = MutationOpCache<Cosine>;

        let identity = session_identity(DataScope::LegacyUnscoped, 310);
        let staged = |node_id: NodeId| vec![node_id + 1_000, node_id + 2_000];
        let mut session = VectorBuildSession::<Cosine>::new(NonZeroU64::new(1 << 20).unwrap());
        let mut cache = session.take_cache(&identity, 8, 4).unwrap();
        for node_id in 0..64 {
            let row = Cache::node_row_id(0, node_id);
            let proof = cache.prove_new_neighbor_row(row).unwrap();
            cache.stage_new_neighbor(proof, neighbors(node_id, staged(node_id)));
            // Clean rows and hits interleave with the dirty rows' recency.
            cache.install_loaded_neighbor(
                Cache::node_row_id(0, node_id + 500),
                NeighborRowValue::KnownAbsent,
            );
            if node_id % 3 == 0 {
                assert!(cache
                    .touched_neighbor(Cache::node_row_id(0, node_id / 2))
                    .is_some());
            }
            cache.put_simhash(node_id, None);
        }
        session.restore_cache(identity.clone(), cache);
        session.admit_entity();
        let dirty_rows = |session: &VectorBuildSession<Cosine>| {
            (0..64)
                .filter(|node_id| {
                    session
                        .caches
                        .get(&identity)
                        .and_then(|cache| cache.neighbor(Cache::node_row_id(0, *node_id)))
                        .is_some_and(CachedNeighbor::is_dirty)
                })
                .count()
        };

        assert!(matches!(
            session.shrink_to(0),
            Err(HelixDbError::InvariantViolation(_))
        ));
        assert_eq!(
            dirty_rows(&session),
            64,
            "a refused eviction keeps every dirty row"
        );
        assert_session_bookkeeping(&session);

        let db = session_test_db("vector-build-session-dirty-pressure").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        let budget = VECTOR_BUILD_SESSION_NAMESPACE_OVERHEAD_BYTES
            + 8 * (VECTOR_BUILD_SESSION_NEIGHBOR_OVERHEAD_BYTES + 64);
        session.max_payload_bytes = budget;
        session.enforce_limits(&measured).unwrap();
        assert!(session.retained_bytes().unwrap() <= budget);
        let kept = dirty_rows(&session);
        assert!(kept < 64, "the budget evicted dirty rows");
        assert_eq!(
            session.stats().dirty_neighbor_flushes(),
            u64::try_from(64 - kept).unwrap(),
            "each evicted dirty row was flushed once"
        );
        assert_session_bookkeeping(&session);

        let keyspace = VectorRowKeyspace::from_allocated(
            identity.physical_name().to_string(),
            identity.physical_index_id(),
            identity.scope(),
        );
        let node_ids = (0..64).collect::<Vec<_>>();
        let read_rows = || async {
            VectorRows::new(&transaction, &keyspace)
                .layer0_neighbor_rows(&node_ids)
                .await
                .unwrap()
        };
        for (node_id, written) in node_ids.iter().zip(read_rows().await) {
            let row = Cache::node_row_id(0, *node_id);
            match session
                .caches
                .get(&identity)
                .and_then(|cache| cache.neighbor(row))
            {
                Some(cached) => {
                    assert!(cached.is_dirty());
                    assert_eq!(cached.current(), &neighbors(*node_id, staged(*node_id)));
                    assert_eq!(written, None);
                }
                None => assert_eq!(written, Some(staged(*node_id))),
            }
        }

        session.flush_all(&measured).unwrap();
        assert_eq!(dirty_rows(&session), 0);
        assert!(read_rows()
            .await
            .into_iter()
            .zip(&node_ids)
            .all(|(written, node_id)| written == Some(staged(*node_id))));
    }
}
