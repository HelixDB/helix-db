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
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;

use slatedb::{DbReadOps, DbTransaction};

use crate::encoding::v2::values::indexes::vector::encode_layer0_neighbors;
use crate::encoding::v2::values::indexes::vector::neighbors::encode_upper_neighbors;
use crate::encoding::NodeId;
use crate::error::HelixDbError;
use crate::search::vector::unaligned_vector::UnalignedVector;

use super::distance::{ActiveVectorSemantics, Distance};
use super::index::VectorIndex;
use super::item::Item;
use super::model::Candidate;
use super::neighbor_set::{
    NeighborDegreeLimit, NeighborDegreeLimits, NeighborDifference, NeighborSet,
};
use super::result::VectorEntityId;
use super::storage::ReverseSourcesForTarget;
use super::storage::{EntryCandidateLayerRow, VectorRowKeyspace, VectorRows, VectorWriteRows};
use super::{
    encode_item, select_diverse, Connections, Layer0Connections, MeasuredVectorTransaction,
    ValidatedMetricVector, VectorDimension, VectorGenerationIdentity, VectorIndexMetadata,
    VectorIndexState,
};

mod active;
pub(crate) use active::ActiveVectorMutationRuntime;

/// Complete physical-generation state after one non-empty deletion cohort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorGenerationAfterDeletion {
    /// At least one canonical layer-0 vector remains searchable.
    Retained,
    /// No canonical layer-0 vector remains in the generation.
    Empty,
}

const LAYER0_NEIGHBOR_PREFETCH_MAX_PER_STEP: usize = 2;
const LAYER0_NEIGHBOR_PREFETCH_MIN_TARGETS: usize = 2;
const LAYER0_NEIGHBOR_PREFETCH_MAX_PER_MUTATION: usize = 8;
pub(super) const VECTOR_BUILD_ITEM_CACHE_LIMIT: usize = 4_096;
pub(super) const VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT: usize = 2_048;
pub(super) const VECTOR_BUILD_SIMHASH_CACHE_LIMIT: usize = 4_096;

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
    pub(super) const fn for_building_generation() -> Self {
        Self { _private: () }
    }

    #[cfg(any(test, feature = "production-coverage"))]
    pub(super) const fn for_test() -> Self {
        Self { _private: () }
    }
}

/// Internal mutation contract selected by the public or generation façade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VectorInsertContract {
    /// Remove an existing graph row before inserting its replacement.
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
pub(super) fn select_layer0_neighbor_prefetch_targets<D: Distance>(
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
    pub(super) async fn update_metadata(
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
    pub(super) async fn get_entry_candidate_layer(
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
    pub(super) async fn upsert_entry_candidate(
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
    pub(super) fn stage_fresh_entry_candidate(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        layer: u16,
        _proof: FreshVectorBuildProof,
    ) -> Result<(), HelixDbError> {
        VectorWriteRows::new(txn, self.row_keyspace()).put_entry_candidate(node_id, layer)
    }

    /// Removes both deployed entry-candidate rows for one mutation target.
    pub(super) async fn remove_entry_candidate(
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
    pub(super) async fn find_best_entry_candidate(
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
    pub(super) async fn repair_stale_entry_point_for_write(
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
    pub(super) async fn resolve_beam_entry_point_for_insert(
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
    pub(super) async fn insert_with_measured_transaction(
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
        super::record_benchmark_cache_stats(mutation_cache.stats);
        result
    }

    /// Reuses one generation-qualified build cache across successive entities.
    pub(super) async fn insert_with_build_session(
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
        mutation_cache.finish_entity_changes();
        session.restore_cache(identity, mutation_cache);
        result
    }

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
            && self
                .get_item_for_layer_cached(txn, 0, node_id, mutation_cache)
                .await?
                .is_some()
        {
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
        self.mark_memory_node_dirty(node_id);

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
            self.mark_memory_node_dirty(node_id);
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
    pub(super) async fn search_layer_beam(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        query: &Item<'_, D>,
        entry_point: NodeId,
        layer: u16,
        ef: usize,
        inserting_node_id: NodeId,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<Vec<Candidate>, HelixDbError> {
        let mut visited = HashSet::new();
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
        let mut visited = HashSet::new();
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
    pub(super) async fn select_neighbors_heuristic(
        &self,
        txn: &DbTransaction,
        query: &Item<'_, D>,
        candidates: &[Candidate],
        maximum_neighbors: usize,
        layer: u16,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<Vec<NodeId>, HelixDbError> {
        let mut items = HashMap::<NodeId, Arc<Item<'static, D>>>::new();
        for candidate in candidates.iter().take(maximum_neighbors * 2) {
            let Some(item) = self
                .get_item_for_layer_cached(txn, layer, candidate.node_id, mutation_cache)
                .await?
            else {
                continue;
            };
            items.insert(candidate.node_id, item);
        }
        select_diverse(
            query,
            candidates,
            &|node_id| items.get(&node_id).map(|item| item.as_ref()),
            maximum_neighbors,
        )
    }

    /// Stages one canonical layer-0 neighbor row through typed storage.
    pub(super) async fn store_neighbors_layer0(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        neighbors: &[NodeId],
    ) -> Result<(), HelixDbError> {
        VectorWriteRows::new(txn, self.row_keyspace()).put_layer0_neighbors(node_id, neighbors)
    }

    /// Stages one canonical upper-neighbor row and fences its shared-cache copy.
    pub(super) fn store_upper_neighbors(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        node_id: NodeId,
        neighbors: &[NodeId],
    ) -> Result<(), HelixDbError> {
        VectorWriteRows::new(txn, self.row_keyspace())
            .put_upper_neighbors(layer, node_id, neighbors)?;
        self.mark_memory_upper_neighbors_dirty(layer, node_id);
        Ok(())
    }

    /// Computes the exact linear reverse-locator delta between canonical rows.
    pub(super) fn neighbor_deltas(
        old_neighbors: &NeighborSet,
        new_neighbors: &NeighborSet,
    ) -> Result<NeighborDifference, HelixDbError> {
        old_neighbors
            .difference(new_neighbors)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))
    }

    /// Stages only reverse-locator changes implied by a canonical row update.
    pub(super) fn update_reverse_edge_locator(
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
    pub(super) async fn load_reverse_sources_for_target(
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
    pub(super) async fn load_neighbors_for_mutation(
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
    pub(super) async fn prefetch_layer0_neighbors_for_mutation(
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
    pub(super) async fn stage_neighbors_for_mutation(
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
    pub(super) async fn stage_neighbors_vec_for_mutation(
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
    pub(super) async fn stage_new_neighbors_for_mutation(
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
    pub(super) async fn flush_mutation_cache(
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
    pub(super) async fn enforce_mutation_cache_bounds(
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
    pub(super) fn evict_oldest_clean_neighbor(
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
    pub(super) async fn flush_one_cached_neighbor(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        mutation_cache: &mut MutationOpCache<D>,
        row: NeighborRowId,
        evict_after_flush: bool,
    ) -> Result<(), HelixDbError> {
        self.flush_one_cached_neighbor_mode(txn, mutation_cache, row, evict_after_flush, true)
            .await
    }

    /// Flushes an Active-session canonical row whose locator delta was staged at its entity boundary.
    pub(super) async fn flush_one_active_cached_neighbor(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        mutation_cache: &mut MutationOpCache<D>,
        row: NeighborRowId,
        evict_after_flush: bool,
    ) -> Result<(), HelixDbError> {
        self.flush_one_cached_neighbor_mode(txn, mutation_cache, row, evict_after_flush, false)
            .await
    }

    async fn flush_one_cached_neighbor_mode(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        mutation_cache: &mut MutationOpCache<D>,
        row: NeighborRowId,
        evict_after_flush: bool,
        stage_reverse_locators: bool,
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
            if stage_reverse_locators {
                self.update_reverse_edge_locator(
                    txn,
                    layer,
                    node_id,
                    &previous_neighbors,
                    current_neighbors,
                )?;
            }
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
        super::record_benchmark_dirty_neighbor_flush();
        Ok(())
    }

    /// Adds one reciprocal HNSW link and prunes the destination to its degree.
    ///
    /// Selection uses cached vectors when available and falls back to stable
    /// truncation only when the destination vector is absent. Every candidate
    /// rejected by pruning is removed from its reciprocal row in the same
    /// operation cache, preserving bidirectionality before the flush boundary.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn add_bidirectional_link(
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
            let to_item = self
                .get_item_for_layer_cached(txn, layer, to_node, mutation_cache)
                .await?;
            match to_item {
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
                        let Some(neighbor_item) = self
                            .get_item_for_layer_cached(txn, layer, neighbor_id, mutation_cache)
                            .await?
                        else {
                            continue;
                        };
                        distances.push(Candidate::try_new(
                            neighbor_id,
                            D::distance(to_item.as_ref(), neighbor_item.as_ref()),
                        )?);
                    }
                    distances.sort();

                    let mut items = HashMap::<NodeId, Arc<Item<'static, D>>>::new();
                    for candidate in &distances {
                        let Some(item) = self
                            .get_item_for_layer_cached(
                                txn,
                                layer,
                                candidate.node_id,
                                mutation_cache,
                            )
                            .await?
                        else {
                            continue;
                        };
                        items.insert(candidate.node_id, item);
                    }
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
        super::record_benchmark_cache_stats(mutation_cache.stats);
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
        mutation_cache.finish_entity_changes();
        session.restore_cache(identity, mutation_cache);
        result
    }

    pub(super) async fn stage_delete_with_metadata(
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
            self.mark_memory_upper_neighbors_dirty(layer, node_id);
        }

        rows.delete_upper_vector(node_id)?;
        self.mark_memory_node_dirty(node_id);
        rows.delete_simhash(node_id)?;
        self.mark_memory_node_dirty(node_id);
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

    /// Removes one sorted cohort and repairs each affected HNSW component once.
    ///
    /// All target and survivor rows are observed before mutation. Each layer is
    /// then repaired highest-first after cohort references have been removed,
    /// and the operation cache emits only final canonical rows and their linear
    /// reverse-locator differences. Canonical payload cleanup tolerates missing
    /// target rows, while metadata count changes only for observed layer-0
    /// items and the final entry point is selected exactly once.
    pub(super) async fn stage_delete_batch_with_metadata(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_ids: &[NodeId],
        metadata: &mut VectorIndexMetadata,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<VectorGenerationAfterDeletion, HelixDbError> {
        if node_ids.is_empty() || node_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(HelixDbError::InvariantViolation(
                "vector deletion cohort must be non-empty, sorted, and unique".to_string(),
            ));
        }
        metadata.validated_state()?;
        let cohort = node_ids.iter().copied().collect::<BTreeSet<_>>();
        let degree_limits = MutationDegreeLimits::try_from_metadata(metadata)?;
        let rows = VectorWriteRows::new(txn, self.row_keyspace());
        let mut existing_items = 0u64;
        let mut target_layers = BTreeMap::<NodeId, BTreeSet<u16>>::new();
        let mut outgoing = BTreeMap::<(u16, NodeId), Vec<NodeId>>::new();
        let mut reverse_sources = BTreeMap::<NodeId, ReverseSourcesForTarget>::new();
        let mut all_layers = BTreeSet::new();
        #[cfg(feature = "production-coverage")]
        let mut repair_sources = BTreeSet::new();

        // Snapshot the complete cohort before any row is staged. This avoids
        // order-sensitive repair when deleted nodes are adjacent to each other.
        for &node_id in node_ids {
            if self
                .get_item_for_layer_cached(txn, 0, node_id, mutation_cache)
                .await?
                .is_some()
            {
                existing_items = existing_items.saturating_add(1);
            }
            let node_max_layer = self
                .get_node_max_layer_cached(txn, node_id, metadata, mutation_cache)
                .await?;
            let reverse = self.load_reverse_sources_for_target(txn, node_id).await?;
            let mut layers = (0..=node_max_layer).collect::<BTreeSet<_>>();
            layers.extend(reverse.sources_by_layer().keys().copied());
            for layer in layers.iter().copied() {
                let neighbors = self
                    .load_neighbors_for_mutation(txn, layer, node_id, mutation_cache)
                    .await?;
                outgoing.insert((layer, node_id), neighbors);
                all_layers.insert(layer);
            }
            target_layers.insert(node_id, layers);
            reverse_sources.insert(node_id, reverse);
        }

        for layer in all_layers.iter().rev().copied() {
            let maximum_neighbors = if layer == 0 {
                degree_limits.layer0.get()
            } else {
                degree_limits.upper.get()
            };
            let targets_on_layer = node_ids
                .iter()
                .copied()
                .filter(|node_id| {
                    target_layers
                        .get(node_id)
                        .is_some_and(|layers| layers.contains(&layer))
                })
                .collect::<BTreeSet<_>>();
            let mut affected_survivors = BTreeSet::new();
            for &target in &targets_on_layer {
                affected_survivors.extend(
                    outgoing
                        .get(&(layer, target))
                        .into_iter()
                        .flatten()
                        .copied()
                        .filter(|node_id| !cohort.contains(node_id)),
                );
                affected_survivors.extend(
                    reverse_sources
                        .get(&target)
                        .expect("every cohort target retains its reverse snapshot")
                        .sources_at(layer)
                        .iter()
                        .copied()
                        .filter(|node_id| !cohort.contains(node_id)),
                );
            }

            let mut retained_rows = BTreeMap::<NodeId, Vec<NodeId>>::new();
            for &source in &affected_survivors {
                #[cfg(feature = "production-coverage")]
                repair_sources.insert((layer, source));
                let observed = self
                    .load_neighbors_for_mutation(txn, layer, source, mutation_cache)
                    .await?;
                let retained = observed
                    .iter()
                    .copied()
                    .filter(|node_id| !cohort.contains(node_id))
                    .collect::<Vec<_>>();
                if retained != observed {
                    self.stage_neighbors_vec_for_mutation(
                        txn,
                        layer,
                        source,
                        retained.clone(),
                        mutation_cache,
                    )
                    .await?;
                }
                retained_rows.insert(source, retained);
            }

            let mut incidence = BTreeMap::<NodeId, BTreeSet<NodeId>>::new();
            for node_id in targets_on_layer
                .iter()
                .chain(affected_survivors.iter())
                .copied()
            {
                incidence.entry(node_id).or_default();
            }
            for &target in &targets_on_layer {
                let observed = outgoing
                    .get(&(layer, target))
                    .expect("snapshotted target layer retains its outgoing row");
                for &neighbor in observed {
                    if !incidence.contains_key(&neighbor) {
                        continue;
                    }
                    incidence.entry(target).or_default().insert(neighbor);
                    incidence.entry(neighbor).or_default().insert(target);
                }
                for &source in reverse_sources
                    .get(&target)
                    .expect("every cohort target retains its reverse snapshot")
                    .sources_at(layer)
                {
                    if !incidence.contains_key(&source) {
                        continue;
                    }
                    incidence.entry(target).or_default().insert(source);
                    incidence.entry(source).or_default().insert(target);
                }
            }
            for (&source, neighbors) in &retained_rows {
                for &neighbor in neighbors {
                    if !incidence.contains_key(&neighbor) {
                        continue;
                    }
                    incidence.entry(source).or_default().insert(neighbor);
                    incidence.entry(neighbor).or_default().insert(source);
                }
            }

            let mut unvisited = incidence.keys().copied().collect::<BTreeSet<_>>();
            while let Some(first) = unvisited.pop_first() {
                let mut frontier = BTreeSet::from([first]);
                let mut component = BTreeSet::new();
                while let Some(current) = frontier.pop_first() {
                    if !component.insert(current) {
                        continue;
                    }
                    let neighbors = incidence
                        .get(&current)
                        .expect("component member retains an incidence row");
                    for &neighbor in neighbors {
                        if unvisited.remove(&neighbor) {
                            frontier.insert(neighbor);
                        }
                    }
                }
                let survivors = component
                    .iter()
                    .copied()
                    .filter(|node_id| !cohort.contains(node_id))
                    .collect::<BTreeSet<_>>();
                if survivors.is_empty() {
                    continue;
                }
                let mut candidates = survivors.clone();
                for survivor in &survivors {
                    candidates.extend(
                        retained_rows
                            .get(survivor)
                            .into_iter()
                            .flatten()
                            .copied()
                            .filter(|node_id| !cohort.contains(node_id)),
                    );
                }
                let candidates = candidates.into_iter().collect::<HashSet<_>>();
                for survivor in survivors {
                    self.relink_neighbor(
                        txn,
                        layer,
                        survivor,
                        &candidates,
                        maximum_neighbors,
                        mutation_cache,
                    )
                    .await?;
                }
            }
        }

        for &node_id in node_ids {
            let layers = target_layers
                .get(&node_id)
                .expect("every cohort target retains its observed layers");
            for &layer in layers {
                for &target_node_id in outgoing
                    .get(&(layer, node_id))
                    .expect("every observed target layer retains its outgoing row")
                {
                    rows.delete_reverse_locator(target_node_id, layer, node_id)?;
                }
            }
            rows.delete_reverse_sources(
                reverse_sources
                    .get(&node_id)
                    .expect("every cohort target retains its reverse snapshot"),
            )?;

            let (canonical_key, _) = self
                .resolve_canonical_vector_key_cached(
                    txn,
                    node_id,
                    mutation_cache,
                    "deleting batched canonical vector payload",
                )
                .await?;
            if let Some(canonical_key) = canonical_key {
                if self.simhash_directory_enabled() {
                    rows.delete_simhash_directory_entry(&canonical_key)?;
                }
                rows.delete_canonical_vector(&canonical_key)?;
            }
            rows.delete_layer0_neighbors(node_id)?;
            for layer in 1..=layers.iter().next_back().copied().unwrap_or(0) {
                rows.delete_upper_neighbors(layer, node_id)?;
                self.mark_memory_upper_neighbors_dirty(layer, node_id);
            }
            rows.delete_upper_vector(node_id)?;
            self.mark_memory_node_dirty(node_id);
            rows.delete_simhash(node_id)?;
            self.mark_memory_node_dirty(node_id);
            self.remove_entry_candidate(txn, node_id).await?;

            let originals = layers
                .iter()
                .filter_map(|&layer| {
                    let row = MutationOpCache::<D>::node_row_id(layer, node_id);
                    mutation_cache
                        .neighbor(row)
                        .map(|cached| (row, cached.current().clone()))
                })
                .collect::<Vec<_>>();
            mutation_cache.invalidate_items(node_id);
            mutation_cache.invalidate_simhash(node_id);
            mutation_cache.put_simhash(node_id, None);
            mutation_cache.invalidate_neighbors(node_id);
            for (&layer, (row, original)) in layers.iter().zip(originals) {
                mutation_cache.install_loaded_neighbor(row, NeighborRowValue::KnownAbsent);
                mutation_cache.record_neighbor_change(row, original);
                mutation_cache.put_item(layer, node_id, None, 0);
            }
        }

        #[cfg(feature = "production-coverage")]
        crate::execution::interpreter::mutation::benchmark_telemetry::record_vector_deletion(
            node_ids.len(),
            repair_sources.len(),
        );
        metadata.count = metadata.count.saturating_sub(existing_items);
        let final_entry = self
            .find_best_entry_candidate_cached(txn, mutation_cache)
            .await?;
        match final_entry {
            Some((entry_point, max_layer)) => {
                metadata.entry_point = Some(entry_point);
                metadata.max_layer = max_layer;
            }
            None => {
                metadata.entry_point = None;
                metadata.max_layer = 0;
            }
        }
        self.update_metadata(txn, metadata).await?;
        Ok(match final_entry {
            Some(_) => VectorGenerationAfterDeletion::Retained,
            None => VectorGenerationAfterDeletion::Empty,
        })
    }

    /// Finds the highest layer containing a node for production coverage contracts.
    #[cfg(feature = "production-coverage")]
    pub(super) async fn get_node_max_layer(
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
    pub(super) async fn delete_from_layer(
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
        for &neighbor_id in &relink_sources {
            self.relink_neighbor(
                txn,
                layer,
                neighbor_id,
                &candidates,
                maximum_neighbors,
                mutation_cache,
            )
            .await?;
        }
        Ok(outgoing_neighbors)
    }

    /// Removes one reciprocal reference and stages the row only when changed.
    pub(super) async fn remove_edge_from_neighbor(
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
    pub(super) async fn relink_neighbor(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        layer: u16,
        neighbor_id: NodeId,
        candidates: &HashSet<NodeId>,
        maximum_neighbors: usize,
        mutation_cache: &mut MutationOpCache<D>,
    ) -> Result<(), HelixDbError> {
        let Some(neighbor_item) = self
            .get_item_for_layer_cached(txn, layer, neighbor_id, mutation_cache)
            .await?
        else {
            return Ok(());
        };
        let old_neighbors = self
            .load_neighbors_for_mutation(txn, layer, neighbor_id, mutation_cache)
            .await?;
        let mut current_neighbors = old_neighbors.clone();
        let mut candidate_distances = Vec::new();
        for &candidate_id in candidates {
            if candidate_id == neighbor_id {
                continue;
            }
            let Some(candidate_item) = self
                .get_item_for_layer_cached(txn, layer, candidate_id, mutation_cache)
                .await?
            else {
                continue;
            };
            candidate_distances.push(Candidate::try_new(
                candidate_id,
                D::distance(neighbor_item.as_ref(), candidate_item.as_ref()),
            )?);
        }
        candidate_distances.sort();
        for candidate in candidate_distances.iter().take(maximum_neighbors) {
            if !current_neighbors.contains(&candidate.node_id) {
                current_neighbors.push(candidate.node_id);
            }
        }

        if current_neighbors.len() > maximum_neighbors {
            let mut distances = Vec::new();
            let mut items = HashMap::<NodeId, Arc<Item<'static, D>>>::new();
            for &node_id in &current_neighbors {
                let Some(item) = self
                    .get_item_for_layer_cached(txn, layer, node_id, mutation_cache)
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
                    .get_item_for_layer_cached(txn, layer, new_neighbor_id, mutation_cache)
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
                        .get_item_for_layer_cached(txn, layer, node_id, mutation_cache)
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

/// Physical HNSW layer bound into one cached neighbor-row identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct HnswLayer(u16);

impl HnswLayer {
    /// Wraps a layer decoded from the deployed key without changing its value.
    pub(super) const fn from_deployed(layer: u16) -> Self {
        Self(layer)
    }

    /// Returns the deployed layer number.
    pub(super) const fn number(self) -> u16 {
        self.0
    }
}

/// Complete identity of one operation-local neighbor row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct NeighborRowId {
    layer: HnswLayer,
    entity: VectorEntityId,
}

impl NeighborRowId {
    /// Binds one layer to the descriptor-proven node or edge identity.
    pub(super) const fn new(layer: HnswLayer, entity: VectorEntityId) -> Self {
        Self { layer, entity }
    }

    /// Returns the row's physical layer.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(super) const fn layer(self) -> HnswLayer {
        self.layer
    }

    /// Returns the row's descriptor-proven entity identity.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(super) const fn entity(self) -> VectorEntityId {
        self.entity
    }

    /// Returns the deployed layer and local entity ID used by row storage.
    pub(super) const fn storage_parts(self) -> (u16, u64) {
        let entity_id = match self.entity {
            VectorEntityId::Node(node_id) => node_id,
            VectorEntityId::Edge(edge_id) => edge_id,
        };
        (self.layer.number(), entity_id)
    }
}

/// Monotonic operation-local recency assigned on every cache touch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CacheSequence(u64);

impl CacheSequence {
    /// Creates the first sequence in a new operation-local cache.
    pub(super) const fn initial() -> Self {
        Self(0)
    }

    /// Advances recency or reports that bounded renumbering is required.
    pub(super) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

/// Authoritative decoded value of one loaded neighbor row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NeighborRowValue {
    /// Storage proved that the row does not currently exist.
    KnownAbsent,
    /// Storage returned one validated canonical neighbor set.
    Present(NeighborSet),
}

/// Closed write state for one loaded neighbor row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NeighborCacheState {
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
pub(super) struct CachedNeighbor {
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
    value: Option<super::SimHash>,
    last_touch: CacheSequence,
}

/// Proof that one row was allocated fresh in the current mutation session.
///
/// The field and constructor remain private to this module, so ordinary staging
/// cannot manufacture an absent original value for an unloaded existing row.
pub(super) struct NewNeighborRowProof {
    row: NeighborRowId,
}

impl CachedNeighbor {
    /// Installs a storage-proven clean row in the operation cache.
    pub(super) const fn clean(current: NeighborRowValue, last_touch: CacheSequence) -> Self {
        Self {
            state: NeighborCacheState::Clean { current },
            last_touch,
        }
    }

    /// Returns the latest authoritative value used by graph mutation.
    pub(super) const fn current(&self) -> &NeighborRowValue {
        match &self.state {
            NeighborCacheState::Clean { current } | NeighborCacheState::Dirty { current, .. } => {
                current
            }
        }
    }

    /// Returns the first storage value only while a write remains pending.
    pub(super) const fn original(&self) -> Option<&NeighborRowValue> {
        match &self.state {
            NeighborCacheState::Clean { .. } => None,
            NeighborCacheState::Dirty { original, .. } => Some(original),
        }
    }

    /// Returns whether this row has a pending staged value.
    pub(super) const fn is_dirty(&self) -> bool {
        matches!(self.state, NeighborCacheState::Dirty { .. })
    }

    /// Stages a new value while preserving the first pre-mutation snapshot.
    pub(super) fn stage(&mut self, staged: NeighborRowValue, last_touch: CacheSequence) {
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
    pub(super) fn mark_flushed(&mut self) {
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
    pub(super) const fn last_touch(&self) -> CacheSequence {
        self.last_touch
    }
}

/// Operation-local canonical neighbor and item state for one HNSW mutation.
///
/// The cache validates layer limits once, retains the first pre-mutation
/// snapshot for linear reverse-edge differences, and never changes persisted
/// row codecs. Dirty entries are encoded only at the existing flush boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MutationCacheTouchTarget {
    Item { layer: u16, node_id: NodeId },
    Neighbor(NeighborRowId),
    SimHash(NodeId),
}

#[derive(Debug)]
pub(super) struct MutationOpCache<D: Distance> {
    neighbor_rows: HashMap<NeighborRowId, CachedNeighbor>,
    clean_neighbor_recency: BTreeSet<(CacheSequence, NeighborRowId)>,
    dirty_neighbor_recency: BTreeSet<(CacheSequence, NeighborRowId)>,
    items: HashMap<(u16, NodeId), CachedItem<D>>,
    item_recency: BTreeSet<(CacheSequence, u16, NodeId)>,
    simhashes: HashMap<NodeId, CachedSimHash>,
    simhash_recency: BTreeSet<(CacheSequence, NodeId)>,
    retained_payload_bytes: usize,
    degree_limits: NeighborDegreeLimits,
    next_touch: CacheSequence,
    stats: VectorBuildSessionStats,
    enforce_local_limits: bool,
    entity_changed_neighbors: BTreeMap<NeighborRowId, NeighborRowValue>,
}

impl<D: Distance> Default for MutationOpCache<D> {
    fn default() -> Self {
        Self::with_degree_limits(usize::MAX, usize::MAX)
            .expect("maximum test compatibility degree limits are non-zero")
    }
}

impl<D: Distance> MutationOpCache<D> {
    /// Creates an operation-local cache with validated final layer degrees.
    pub(super) fn with_degree_limits(layer0: usize, upper: usize) -> Result<Self, HelixDbError> {
        Ok(Self {
            neighbor_rows: HashMap::new(),
            clean_neighbor_recency: BTreeSet::new(),
            dirty_neighbor_recency: BTreeSet::new(),
            items: HashMap::new(),
            item_recency: BTreeSet::new(),
            simhashes: HashMap::new(),
            simhash_recency: BTreeSet::new(),
            retained_payload_bytes: 0,
            degree_limits: NeighborDegreeLimits::try_new(layer0, upper)
                .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?,
            next_touch: CacheSequence::initial(),
            stats: VectorBuildSessionStats::default(),
            enforce_local_limits: true,
            entity_changed_neighbors: BTreeMap::new(),
        })
    }

    /// Transfers limit ownership to the complete reusable build session.
    fn into_build_session_cache(mut self) -> Self {
        self.enforce_local_limits = false;
        self
    }

    /// Returns whether one-operation eviction remains authoritative.
    pub(super) const fn enforces_local_limits(&self) -> bool {
        self.enforce_local_limits
    }

    /// Starts one entity boundary for explicit Serializable Snapshot read intent.
    pub(super) fn begin_entity(&mut self) {
        self.entity_changed_neighbors.clear();
    }

    /// Records that one canonical row changed logically during this entity.
    pub(super) fn record_neighbor_change(
        &mut self,
        row: NeighborRowId,
        original: NeighborRowValue,
    ) {
        self.entity_changed_neighbors.entry(row).or_insert(original);
    }

    /// Finishes one entity and returns every canonical row that changed.
    pub(super) fn finish_entity_changes(&mut self) -> BTreeMap<NeighborRowId, NeighborRowValue> {
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
        super::observe_benchmark_retained_payload(
            u64::try_from(retained_payload_bytes).unwrap_or(u64::MAX),
        );
        Ok(())
    }

    /// Returns the validated final degree for one physical layer.
    pub(super) fn degree_limit(&self, layer: u16) -> NeighborDegreeLimit {
        self.degree_limits.for_layer(layer)
    }

    /// Returns the node-row identity used by the current node-only HNSW core.
    pub(super) const fn node_row_id(layer: u16, node_id: NodeId) -> NeighborRowId {
        NeighborRowId::new(
            HnswLayer::from_deployed(layer),
            VectorEntityId::Node(node_id),
        )
    }

    /// Returns the current state for a previously loaded row.
    pub(super) fn neighbor(&self, row: NeighborRowId) -> Option<&CachedNeighbor> {
        self.neighbor_rows.get(&row)
    }

    fn remove_neighbor_recency(
        &mut self,
        row: NeighborRowId,
        last_touch: CacheSequence,
        dirty: bool,
    ) {
        let removed = if dirty {
            self.dirty_neighbor_recency.remove(&(last_touch, row))
        } else {
            self.clean_neighbor_recency.remove(&(last_touch, row))
        };
        assert!(removed, "cached vector neighbor retains one recency entry");
    }

    fn insert_neighbor_recency(
        &mut self,
        row: NeighborRowId,
        last_touch: CacheSequence,
        dirty: bool,
    ) {
        let inserted = if dirty {
            self.dirty_neighbor_recency.insert((last_touch, row))
        } else {
            self.clean_neighbor_recency.insert((last_touch, row))
        };
        assert!(inserted, "vector neighbor recency is unique");
    }

    /// Returns and touches one authoritative neighbor row.
    pub(super) fn touched_neighbor(&mut self, row: NeighborRowId) -> Option<&CachedNeighbor> {
        if !self.neighbor_rows.contains_key(&row) {
            self.stats.neighbor_misses = self.stats.neighbor_misses.saturating_add(1);
            return None;
        }
        self.stats.neighbor_hits = self.stats.neighbor_hits.saturating_add(1);
        let (last_touch, dirty) = self
            .neighbor_rows
            .get(&row)
            .map(|cached| (cached.last_touch(), cached.is_dirty()))
            .expect("contained vector neighbor row remains cached");
        self.remove_neighbor_recency(row, last_touch, dirty);
        let touch = self.take_touch();
        self.neighbor_rows
            .get_mut(&row)
            .expect("contained vector neighbor row remains cached")
            .last_touch = touch;
        self.insert_neighbor_recency(row, touch, dirty);
        self.neighbor_rows.get(&row)
    }

    /// Returns whether a row is loaded, independently of whether it exists.
    pub(super) fn contains_neighbor(&self, row: NeighborRowId) -> bool {
        self.neighbor_rows.contains_key(&row)
    }

    /// Installs one storage-proven row unless staging already owns it.
    pub(super) fn install_loaded_neighbor(
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
        self.insert_neighbor_recency(row, touch, false);
        true
    }

    /// Stages a row that must already have storage-proven cache state.
    pub(super) fn stage_loaded_neighbor(
        &mut self,
        row: NeighborRowId,
        value: NeighborRowValue,
    ) -> Result<(), HelixDbError> {
        let touch = self.take_touch();
        let Some((last_touch, dirty)) = self
            .neighbor_rows
            .get(&row)
            .map(|cached| (cached.last_touch(), cached.is_dirty()))
        else {
            return Err(HelixDbError::InvariantViolation(
                "cannot stage an unloaded vector neighbor row without new-row proof".to_string(),
            ));
        };
        let previous = self
            .neighbor_rows
            .get(&row)
            .expect("loaded vector neighbor remains cached while staging");
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
        self.remove_neighbor_recency(row, last_touch, dirty);
        let replaced = self
            .neighbor_rows
            .insert(row, staged)
            .expect("loaded vector neighbor remains cached while staging");
        debug_assert_eq!(replaced.last_touch(), last_touch);
        self.insert_neighbor_recency(row, touch, true);
        Ok(())
    }

    /// Issues the unforgeable token used only after allocating a fresh row.
    pub(super) fn prove_new_neighbor_row(
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
    pub(super) fn stage_new_neighbor(
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
        self.insert_neighbor_recency(proof.row, touch, true);
    }

    /// Removes one row after clean eviction or a successful evicting flush.
    pub(super) fn remove_neighbor(&mut self, row: NeighborRowId) -> Option<CachedNeighbor> {
        let cached = self.neighbor_rows.get(&row)?;
        let payload_bytes = cached_neighbor_payload_bytes(row, cached)
            .expect("validated vector neighbor cache state has measurable payload");
        self.replace_retained_payload(payload_bytes, 0)
            .expect("retained vector neighbor payload covers every cached row");
        let removed = self
            .neighbor_rows
            .remove(&row)
            .expect("selected vector neighbor remains cached while removing");
        self.remove_neighbor_recency(row, removed.last_touch(), removed.is_dirty());
        Some(removed)
    }

    /// Marks one successfully flushed row clean while preserving global recency.
    pub(super) fn mark_neighbor_flushed(&mut self, row: NeighborRowId) {
        let Some((last_touch, true)) = self
            .neighbor_rows
            .get(&row)
            .map(|cached| (cached.last_touch(), cached.is_dirty()))
        else {
            return;
        };
        let previous = self
            .neighbor_rows
            .get(&row)
            .expect("flushed vector neighbor remains cached");
        let previous_payload = cached_neighbor_payload_bytes(row, previous)
            .expect("validated dirty vector neighbor has measurable payload");
        let mut flushed = previous.clone();
        flushed.mark_flushed();
        let flushed_payload = cached_neighbor_payload_bytes(row, &flushed)
            .expect("validated flushed vector neighbor has measurable payload");
        self.replace_retained_payload(previous_payload, flushed_payload)
            .expect("flushing a vector neighbor cannot overflow cache payload");
        self.remove_neighbor_recency(row, last_touch, true);
        let replaced = self
            .neighbor_rows
            .insert(row, flushed)
            .expect("flushed vector neighbor remains cached");
        debug_assert!(replaced.is_dirty());
        self.insert_neighbor_recency(row, last_touch, false);
    }

    /// Removes every layer-specific neighbor state for one entity.
    pub(super) fn invalidate_neighbors(&mut self, node_id: NodeId) {
        let rows = self
            .neighbor_rows
            .keys()
            .copied()
            .filter(|row| row.storage_parts().1 == node_id)
            .collect::<Vec<_>>();
        for row in rows {
            self.entity_changed_neighbors.remove(&row);
            self.remove_neighbor(row);
        }
    }

    /// Returns the number of loaded neighbor rows.
    pub(super) fn neighbor_count(&self) -> usize {
        self.neighbor_rows.len()
    }

    /// Returns and touches one decoded item or negative lookup.
    pub(super) fn item(
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
        let last_touch = self
            .items
            .get(&key)
            .expect("contained vector item remains cached")
            .last_touch;
        assert!(
            self.item_recency.remove(&(last_touch, layer, node_id)),
            "cached vector item retains one recency entry"
        );
        let touch = self.take_touch();
        let value = {
            let cached = self
                .items
                .get_mut(&key)
                .expect("contained vector item remains cached");
            cached.last_touch = touch;
            cached.value.clone()
        };
        assert!(
            self.item_recency.insert((touch, layer, node_id)),
            "vector item recency is unique"
        );
        Some(value)
    }

    /// Installs or replaces one decoded item state.
    pub(super) fn put_item(
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
        let replaced = self.items.insert(
            (layer, node_id),
            CachedItem {
                value,
                payload_bytes,
                last_touch: touch,
            },
        );
        if let Some(replaced) = replaced {
            assert!(
                self.item_recency
                    .remove(&(replaced.last_touch, layer, node_id)),
                "replaced vector item retains one recency entry"
            );
        }
        assert!(
            self.item_recency.insert((touch, layer, node_id)),
            "vector item recency is unique"
        );
    }

    /// Removes every layer-specific item state for one entity.
    pub(super) fn invalidate_items(&mut self, node_id: NodeId) {
        let keys = self
            .items
            .keys()
            .copied()
            .filter(|(_, cached_node_id)| *cached_node_id == node_id)
            .collect::<Vec<_>>();
        for (layer, node_id) in keys {
            self.remove_item(layer, node_id);
        }
    }

    /// Removes the item entry paired with one evicted neighbor row.
    pub(super) fn remove_item(&mut self, layer: u16, node_id: NodeId) {
        let Some(cached) = self.items.get(&(layer, node_id)) else {
            return;
        };
        self.replace_retained_payload(cached.payload_bytes, 0)
            .expect("retained vector item payload covers every cached item");
        let removed = self
            .items
            .remove(&(layer, node_id))
            .expect("selected vector item remains cached while removing");
        assert!(
            self.item_recency
                .remove(&(removed.last_touch, layer, node_id)),
            "removed vector item retains one recency entry"
        );
    }

    /// Returns the number of retained decoded item states.
    pub(super) fn item_count(&self) -> usize {
        self.items.len()
    }

    /// Reports a retained negative lookup without changing its recency.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(super) fn item_is_known_absent(&self, layer: u16, node_id: NodeId) -> bool {
        self.items
            .get(&(layer, node_id))
            .is_some_and(|cached| cached.value.is_none())
    }

    /// Evicts the deterministic least-recently-used item in this namespace.
    pub(super) fn evict_oldest_item(&mut self) -> bool {
        let Some((_, layer, node_id)) = self.item_recency.first().copied() else {
            return false;
        };
        self.remove_item(layer, node_id);
        self.stats.item_evictions = self.stats.item_evictions.saturating_add(1);
        true
    }

    /// Returns and touches one SimHash or negative lookup.
    pub(super) fn simhash(&mut self, node_id: NodeId) -> Option<Option<super::SimHash>> {
        if !self.simhashes.contains_key(&node_id) {
            self.stats.simhash_misses = self.stats.simhash_misses.saturating_add(1);
            return None;
        }
        self.stats.simhash_hits = self.stats.simhash_hits.saturating_add(1);
        let last_touch = self
            .simhashes
            .get(&node_id)
            .expect("contained vector SimHash remains cached")
            .last_touch;
        assert!(
            self.simhash_recency.remove(&(last_touch, node_id)),
            "cached vector SimHash retains one recency entry"
        );
        let touch = self.take_touch();
        let value = {
            let cached = self
                .simhashes
                .get_mut(&node_id)
                .expect("contained vector SimHash remains cached");
            cached.last_touch = touch;
            cached.value
        };
        assert!(
            self.simhash_recency.insert((touch, node_id)),
            "vector SimHash recency is unique"
        );
        Some(value)
    }

    /// Installs or replaces one SimHash state.
    pub(super) fn put_simhash(&mut self, node_id: NodeId, value: Option<super::SimHash>) {
        let previous_payload = self
            .simhashes
            .get(&node_id)
            .map_or(0, |cached| simhash_payload_bytes(cached.value));
        let payload_bytes = simhash_payload_bytes(value);
        self.replace_retained_payload(previous_payload, payload_bytes)
            .expect("bounded vector SimHash cache payload cannot overflow");
        let touch = self.take_touch();
        let replaced = self.simhashes.insert(
            node_id,
            CachedSimHash {
                value,
                last_touch: touch,
            },
        );
        if let Some(replaced) = replaced {
            assert!(
                self.simhash_recency.remove(&(replaced.last_touch, node_id)),
                "replaced vector SimHash retains one recency entry"
            );
        }
        assert!(
            self.simhash_recency.insert((touch, node_id)),
            "vector SimHash recency is unique"
        );
    }

    /// Invalidates one SimHash or negative lookup after mutation.
    pub(super) fn invalidate_simhash(&mut self, node_id: NodeId) {
        let Some(cached) = self.simhashes.get(&node_id) else {
            return;
        };
        self.replace_retained_payload(simhash_payload_bytes(cached.value), 0)
            .expect("retained vector SimHash payload covers every cached value");
        let removed = self
            .simhashes
            .remove(&node_id)
            .expect("selected vector SimHash remains cached while removing");
        assert!(
            self.simhash_recency.remove(&(removed.last_touch, node_id)),
            "removed vector SimHash retains one recency entry"
        );
    }

    /// Returns the number of retained SimHash states.
    pub(super) fn simhash_count(&self) -> usize {
        self.simhashes.len()
    }

    /// Evicts the deterministic least-recently-used SimHash in this namespace.
    pub(super) fn evict_oldest_simhash(&mut self) -> bool {
        let Some((_, node_id)) = self.simhash_recency.first().copied() else {
            return false;
        };
        self.invalidate_simhash(node_id);
        self.stats.simhash_evictions = self.stats.simhash_evictions.saturating_add(1);
        true
    }

    /// Returns retained decoded payload bytes for this namespace.
    fn retained_payload_bytes(&self) -> Result<usize, HelixDbError> {
        Ok(self.retained_payload_bytes)
    }

    /// Returns the oldest dirty row using one bounded cache scan.
    pub(super) fn oldest_dirty_neighbor(&self) -> Option<NeighborRowId> {
        self.dirty_neighbor_recency.first().map(|(_, row)| *row)
    }

    /// Returns the oldest clean row using one bounded cache scan.
    pub(super) fn oldest_clean_neighbor(&self) -> Option<NeighborRowId> {
        self.clean_neighbor_recency.first().map(|(_, row)| *row)
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
            .map(|((layer, node_id), cached)| {
                (
                    cached.last_touch,
                    MutationCacheTouchTarget::Item {
                        layer: *layer,
                        node_id: *node_id,
                    },
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
        self.item_recency.clear();
        self.clean_neighbor_recency.clear();
        self.dirty_neighbor_recency.clear();
        self.simhash_recency.clear();
        for (sequence, (_, target)) in entries.into_iter().enumerate() {
            let touch = CacheSequence(
                u64::try_from(sequence).expect("bounded vector mutation cache length fits in u64"),
            );
            match target {
                MutationCacheTouchTarget::Item { layer, node_id } => {
                    self.items
                        .get_mut(&(layer, node_id))
                        .expect("renumbered vector item still exists")
                        .last_touch = touch;
                    assert!(self.item_recency.insert((touch, layer, node_id)));
                }
                MutationCacheTouchTarget::Neighbor(row) => {
                    let dirty = {
                        let cached = self
                            .neighbor_rows
                            .get_mut(&row)
                            .expect("renumbered vector neighbor still exists");
                        cached.last_touch = touch;
                        cached.is_dirty()
                    };
                    self.insert_neighbor_recency(row, touch, dirty);
                }
                MutationCacheTouchTarget::SimHash(node_id) => {
                    self.simhashes
                        .get_mut(&node_id)
                        .expect("renumbered vector SimHash still exists")
                        .last_touch = touch;
                    assert!(self.simhash_recency.insert((touch, node_id)));
                }
            }
        }
        self.next_touch = CacheSequence(
            u64::try_from(
                self.items
                    .len()
                    .saturating_add(self.neighbor_rows.len())
                    .saturating_add(self.simhashes.len()),
            )
            .expect("bounded vector mutation cache length fits in u64"),
        );
    }
}

/// One bounded cache session shared by a vector Scan or CatchUp planning transaction.
///
/// Entries are nested under the complete generation identity, so equal physical
/// node/layer numbers from another scope, logical generation, record revision,
/// or physical partition cannot alias. The session owns no database or resident
/// vector-memory handle and is dropped with the disposable planning transaction.
#[derive(Debug)]
pub(crate) struct VectorBuildSession<D: Distance> {
    caches: HashMap<VectorGenerationIdentity, MutationOpCache<D>>,
    next_touch: CacheSequence,
    max_payload_bytes: usize,
    max_items: usize,
    max_neighbors: usize,
    max_simhashes: usize,
    session_stats: VectorBuildSessionStats,
}

impl<D: Distance> VectorBuildSession<D> {
    /// Creates one session with the batch input-byte ceiling as retained payload budget.
    pub(crate) fn new(max_input_bytes: NonZeroU64) -> Self {
        Self {
            caches: HashMap::new(),
            next_touch: CacheSequence::initial(),
            max_payload_bytes: usize::try_from(max_input_bytes.get()).unwrap_or(usize::MAX),
            max_items: VECTOR_BUILD_ITEM_CACHE_LIMIT,
            max_neighbors: VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT,
            max_simhashes: VECTOR_BUILD_SIMHASH_CACHE_LIMIT,
            session_stats: VectorBuildSessionStats::default(),
        }
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

    /// Temporarily detaches one identity cache for an async mutation operation.
    pub(super) fn take_cache(
        &mut self,
        identity: &VectorGenerationIdentity,
        layer0_degree: usize,
        upper_degree: usize,
    ) -> Result<MutationOpCache<D>, HelixDbError> {
        let expected = match NeighborDegreeLimits::try_new(layer0_degree, upper_degree) {
            Ok(expected) => expected,
            Err(error) => return Err(HelixDbError::InvariantViolation(error.to_string())),
        };
        let mut cache = match self.caches.remove(identity) {
            Some(cache) => {
                if cache.degree_limits != expected {
                    return Err(HelixDbError::InvariantViolation(
                        "vector build session generation changed neighbor degree limits"
                            .to_string(),
                    ));
                }
                cache
            }
            None => MutationOpCache::with_degree_limits(layer0_degree, upper_degree)?
                .into_build_session_cache(),
        };
        cache.next_touch = self.next_touch;
        Ok(cache)
    }

    /// Restores one detached identity cache even when mutation planning failed.
    pub(super) fn restore_cache(
        &mut self,
        identity: VectorGenerationIdentity,
        cache: MutationOpCache<D>,
    ) {
        self.next_touch = cache.next_touch;
        assert!(
            self.caches.insert(identity, cache).is_none(),
            "a vector build session cannot restore one identity twice"
        );
    }

    /// Flushes every dirty neighbor in deterministic identity/recency order.
    ///
    /// A failed typed write leaves the selected cache row dirty. The caller must
    /// abort planning and discard the complete session and transaction.
    pub(crate) fn flush_all(
        &mut self,
        txn: &MeasuredVectorTransaction<'_>,
    ) -> Result<(), HelixDbError> {
        let mut identities = self.caches.keys().cloned().collect::<Vec<_>>();
        identities.sort();
        for identity in identities {
            loop {
                let row = self
                    .caches
                    .get(&identity)
                    .and_then(MutationOpCache::oldest_dirty_neighbor);
                let Some(row) = row else {
                    break;
                };
                let cache = self
                    .caches
                    .get_mut(&identity)
                    .expect("selected vector build namespace remains cached");
                flush_build_session_neighbor(txn, &identity, cache, row)?;
                self.session_stats.dirty_neighbor_flushes =
                    self.session_stats.dirty_neighbor_flushes.saturating_add(1);
            }
        }
        Ok(())
    }

    /// Enforces class counts and the global retained-payload ceiling.
    ///
    /// Selection is deterministic LRU. Equal recency resolves by cache kind,
    /// complete generation identity, layer, and entity ID. A dirty neighbor is
    /// flushed through the typed recorder before it is removed.
    pub(crate) fn enforce_limits(
        &mut self,
        txn: &MeasuredVectorTransaction<'_>,
    ) -> Result<(), HelixDbError> {
        loop {
            let item_count = self.item_count();
            let neighbor_count = self.neighbor_count();
            let simhash_count = self.simhash_count();
            let payload_bytes = self.retained_payload_bytes()?;
            let payload_pressure = payload_bytes > self.max_payload_bytes;
            let item_pressure = item_count > self.max_items;
            let neighbor_pressure = neighbor_count > self.max_neighbors;
            let simhash_pressure = simhash_count > self.max_simhashes;
            if !payload_pressure && !item_pressure && !neighbor_pressure && !simhash_pressure {
                self.session_stats.max_retained_payload_bytes = self
                    .session_stats
                    .max_retained_payload_bytes
                    .max(u64::try_from(payload_bytes).unwrap_or(u64::MAX));
                return Ok(());
            }

            let mut selected = None::<SessionEvictionCandidate>;
            for (identity, cache) in &self.caches {
                if (payload_pressure || item_pressure)
                    && let Some((touch, layer, node_id)) = cache.item_recency.first().copied()
                {
                    let candidate = SessionEvictionCandidate {
                        order: SessionEvictionOrder {
                            touch,
                            kind: SessionCacheKind::Item,
                            identity: identity.clone(),
                            layer,
                            node_id,
                        },
                        target: SessionEvictionTarget::Item {
                            identity: identity.clone(),
                            layer,
                            node_id,
                        },
                    };
                    if selected
                        .as_ref()
                        .is_none_or(|current| candidate.order < current.order)
                    {
                        selected = Some(candidate);
                    }
                }
                let oldest_neighbor = cache
                    .clean_neighbor_recency
                    .first()
                    .into_iter()
                    .chain(cache.dirty_neighbor_recency.first())
                    .min()
                    .copied();
                if (payload_pressure || neighbor_pressure)
                    && let Some((touch, row)) = oldest_neighbor
                {
                    let (layer, node_id) = row.storage_parts();
                    let candidate = SessionEvictionCandidate {
                        order: SessionEvictionOrder {
                            touch,
                            kind: SessionCacheKind::Neighbor,
                            identity: identity.clone(),
                            layer,
                            node_id,
                        },
                        target: SessionEvictionTarget::Neighbor {
                            identity: identity.clone(),
                            row,
                        },
                    };
                    if selected
                        .as_ref()
                        .is_none_or(|current| candidate.order < current.order)
                    {
                        selected = Some(candidate);
                    }
                }
                if (payload_pressure || simhash_pressure)
                    && let Some((touch, node_id)) = cache.simhash_recency.first().copied()
                {
                    let candidate = SessionEvictionCandidate {
                        order: SessionEvictionOrder {
                            touch,
                            kind: SessionCacheKind::SimHash,
                            identity: identity.clone(),
                            layer: 0,
                            node_id,
                        },
                        target: SessionEvictionTarget::SimHash {
                            identity: identity.clone(),
                            node_id,
                        },
                    };
                    if selected
                        .as_ref()
                        .is_none_or(|current| candidate.order < current.order)
                    {
                        selected = Some(candidate);
                    }
                }
            }

            let Some(selected) = selected else {
                return Err(HelixDbError::InvariantViolation(
                    "vector build session exceeded a limit without an evictable entry".to_string(),
                ));
            };
            match selected.target {
                SessionEvictionTarget::Item {
                    identity,
                    layer,
                    node_id,
                } => {
                    let cache = self
                        .caches
                        .get_mut(&identity)
                        .expect("selected vector item namespace remains cached");
                    cache.remove_item(layer, node_id);
                    cache.stats.item_evictions = cache.stats.item_evictions.saturating_add(1);
                }
                SessionEvictionTarget::Neighbor { identity, row } => {
                    let cache = self
                        .caches
                        .get_mut(&identity)
                        .expect("selected vector neighbor namespace remains cached");
                    let dirty = cache.neighbor(row).is_some_and(CachedNeighbor::is_dirty);
                    if dirty {
                        flush_build_session_neighbor(txn, &identity, cache, row)?;
                        self.session_stats.dirty_neighbor_flushes =
                            self.session_stats.dirty_neighbor_flushes.saturating_add(1);
                    }
                    cache.remove_neighbor(row);
                    cache.stats.neighbor_evictions =
                        cache.stats.neighbor_evictions.saturating_add(1);
                }
                SessionEvictionTarget::SimHash { identity, node_id } => {
                    let cache = self
                        .caches
                        .get_mut(&identity)
                        .expect("selected vector SimHash namespace remains cached");
                    cache.invalidate_simhash(node_id);
                    cache.stats.simhash_evictions = cache.stats.simhash_evictions.saturating_add(1);
                }
            }
        }
    }

    /// Returns aggregate cache behavior for lifecycle-testing metrics.
    pub(crate) fn stats(&self) -> VectorBuildSessionStats {
        let mut stats = self.session_stats;
        let max_retained_payload_bytes = stats.max_retained_payload_bytes;
        for cache in self.caches.values() {
            stats.merge(cache.stats);
        }
        // Hidden-generation planning has always reported the maximum retained
        // payload after enforcing its global ceiling. Cache-local telemetry also
        // observes transient Active-mutation peaks, which must not change that
        // lifecycle contract.
        stats.max_retained_payload_bytes = max_retained_payload_bytes;
        stats
    }

    /// Returns retained decoded-item and negative-lookup entries.
    pub(crate) fn item_count(&self) -> usize {
        self.caches.values().map(MutationOpCache::item_count).sum()
    }

    /// Returns retained upper/layer-zero neighbor rows.
    pub(crate) fn neighbor_count(&self) -> usize {
        self.caches
            .values()
            .map(MutationOpCache::neighbor_count)
            .sum()
    }

    /// Returns retained SimHash and negative-lookup entries.
    pub(crate) fn simhash_count(&self) -> usize {
        self.caches
            .values()
            .map(MutationOpCache::simhash_count)
            .sum()
    }

    /// Returns retained decoded payload bytes across every namespace.
    pub(crate) fn retained_payload_bytes(&self) -> Result<usize, HelixDbError> {
        let mut total = 0_usize;
        for cache in self.caches.values() {
            let Some(next_total) = total.checked_add(cache.retained_payload_bytes()?) else {
                return Err(HelixDbError::InvariantViolation(
                    "vector build session payload accounting overflowed".to_string(),
                ));
            };
            total = next_total;
        }
        Ok(total)
    }

    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) const fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SessionCacheKind {
    Item,
    Neighbor,
    SimHash,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SessionEvictionOrder {
    touch: CacheSequence,
    kind: SessionCacheKind,
    identity: VectorGenerationIdentity,
    layer: u16,
    node_id: NodeId,
}

struct SessionEvictionCandidate {
    order: SessionEvictionOrder,
    target: SessionEvictionTarget,
}

enum SessionEvictionTarget {
    Item {
        identity: VectorGenerationIdentity,
        layer: u16,
        node_id: NodeId,
    },
    Neighbor {
        identity: VectorGenerationIdentity,
        row: NeighborRowId,
    },
    SimHash {
        identity: VectorGenerationIdentity,
        node_id: NodeId,
    },
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

const fn simhash_payload_bytes(value: Option<super::SimHash>) -> usize {
    if value.is_some() {
        core::mem::size_of::<u64>()
    } else {
        0
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/mutation.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use proptest::prelude::*;
    use slatedb::object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;

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
            super::super::VectorDimension::try_new(2).unwrap(),
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
        cache: &MutationOpCache<super::super::distance::Cosine>,
        rows: &HashMap<NodeId, ReferenceNeighbor>,
    ) {
        assert_eq!(cache.neighbor_count(), rows.len());
        assert_eq!(
            cache.clean_neighbor_recency.len() + cache.dirty_neighbor_recency.len(),
            rows.len()
        );
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
            let row = MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, node_id);
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
                MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, node_id)
            })
        );
        assert_eq!(
            cache.oldest_dirty_neighbor(),
            reference_oldest(rows, true).map(|node_id| {
                MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, node_id)
            })
        );
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
        let mut cache = MutationOpCache::<super::super::distance::Cosine>::default();
        let first = MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, 1);
        let second = MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, 2);
        let third = MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, 3);
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
        let mut cache = MutationOpCache::<super::super::distance::Cosine>::default();
        let first = MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, 1);
        let second = MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, 2);
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
        let mut cache = MutationOpCache::<super::super::distance::Cosine>::default();
        let row = MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, 1);
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
        let mut cache = MutationOpCache::<super::super::distance::Cosine>::default();
        let row = MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, 1);
        let present = neighbors(1, vec![2]);
        let neighbor_bytes = neighbor_payload_bytes(0, &present).unwrap();

        cache.put_item(0, 1, None, 11);
        cache.put_item(0, 1, None, 7);
        cache.put_simhash(1, Some(super::super::SimHash::from_bits(9)));
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
        let mut session = VectorBuildSession::<super::super::distance::Cosine>::new(
            NonZeroU64::new(1024).unwrap(),
        );

        let mut first_cache = session.take_cache(&first, 8, 4).unwrap();
        first_cache.put_item(0, 9, None, 0);
        first_cache.put_simhash(9, None);
        first_cache.install_loaded_neighbor(
            MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, 9),
            NeighborRowValue::KnownAbsent,
        );
        session.restore_cache(first.clone(), first_cache);

        let mut second_cache = session.take_cache(&second, 8, 4).unwrap();
        assert!(second_cache.item(0, 9).is_none());
        assert_eq!(second_cache.simhash(9), None);
        assert!(!second_cache.contains_neighbor(
            MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, 9)
        ));
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
        let mut session = VectorBuildSession::<super::super::distance::Cosine>::with_test_limits(
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
                MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, node_id),
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
        let row = MutationOpCache::<super::super::distance::Cosine>::node_row_id(0, 1);
        let mut session = VectorBuildSession::<super::super::distance::Cosine>::new(
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
        let handle = super::super::ValidatedVectorGenerationHandle::create_current::<
            super::super::distance::Cosine,
        >(identity)
        .unwrap();
        let index = VectorIndex::<super::super::distance::Cosine>::from_generation(&handle);
        let db = session_test_db("vector-build-session-traversal-reuse").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        index
            .stage_create(
                &measured,
                super::super::VectorIndexConfig::new(handle.physical_name(), "embedding", 2),
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

    proptest! {
        #[test]
        fn random_neighbor_operations_match_the_reference_model(
            operations in prop::collection::vec((0_u8..5, 1_u64..=4, 0_u8..=8), 0..128),
        ) {
            let mut cache = MutationOpCache::<super::super::distance::Cosine>::default();
            let mut reference = HashMap::<NodeId, ReferenceNeighbor>::new();
            let mut next_touch = 0_u64;

            for (operation, node_id, value_token) in operations {
                let row = MutationOpCache::<super::super::distance::Cosine>::node_row_id(
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
                    _ => unreachable!("generated operation is in the closed 0..5 range"),
                }
                assert_matches_reference(&cache, &reference);
            }
        }
    }
}
