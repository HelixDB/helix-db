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
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

#[cfg(any(test, feature = "production-coverage"))]
use slatedb::DbReadOps;
use slatedb::DbTransaction;

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
#[cfg(any(test, feature = "production-coverage"))]
use super::storage::ReverseSourcesForTarget;
use super::storage::{EntryCandidateLayerRow, VectorRows, VectorWriteRows};
use super::{
    encode_item, select_diverse, Connections, Layer0Connections, MeasuredVectorTransaction,
    VectorIndexMetadata, VectorIndexState,
};

const LAYER0_NEIGHBOR_PREFETCH_MAX_PER_STEP: usize = 2;
const LAYER0_NEIGHBOR_PREFETCH_MIN_TARGETS: usize = 2;
const LAYER0_NEIGHBOR_PREFETCH_MAX_PER_MUTATION: usize = 8;
const OPERATION_NEIGHBOR_CACHE_LIMIT: usize = 2_048;

/// Capability proving a vector insertion targets an empty physical generation.
///
/// Construction remains private to the vector module until the V2 lifecycle
/// repository can issue the capability from durable generation ownership.
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
    pub(super) async fn find_best_entry_candidate(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
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
            if self.get_item(txn, node_id).await?.is_some() {
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
    ) -> Result<bool, HelixDbError> {
        let Some(stale_entry_point) = metadata.entry_point else {
            return Ok(false);
        };
        if self.get_item(txn, stale_entry_point).await?.is_some() {
            return Ok(false);
        }

        let old_max_layer = metadata.max_layer;
        if let Some((replacement_entry_point, replacement_layer)) =
            self.find_best_entry_candidate(txn).await?
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
    ) -> Result<Option<(NodeId, Item<'static, D>)>, HelixDbError> {
        if let Some(item) = self.get_item_for_layer(txn, layer, entry_point).await? {
            return Ok(Some((entry_point, item)));
        }

        if let Some((replacement_entry_point, replacement_layer)) =
            self.find_best_entry_candidate(txn).await?
            && replacement_entry_point != entry_point
            && let Some(item) = self
                .get_item_for_layer(txn, layer, replacement_entry_point)
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
            return Ok(Some((replacement_entry_point, item)));
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
    /// layer makes planning and replay deterministic; otherwise the façade's
    /// configured selector chooses it exactly once.
    pub(super) async fn insert_with_measured_transaction(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        vector: &[f32],
        contract: VectorInsertContract,
        selected_layer: Option<u16>,
    ) -> Result<(), HelixDbError> {
        let mut metadata = self
            .get_metadata(txn)
            .await?
            .ok_or_else(|| HelixDbError::IndexNotFound(self.name().to_string()))?;

        if vector.len() != metadata.config.dimension {
            return Err(HelixDbError::InvalidDimension {
                expected: metadata.config.dimension,
                got: vector.len(),
            });
        }
        if let Some(index) = vector.iter().position(|component| !component.is_finite()) {
            return Err(HelixDbError::InvalidVectorComponent { index });
        }
        if matches!(
            ActiveVectorSemantics::for_distance::<D>().map(ActiveVectorSemantics::metric),
            Some(crate::encoding::v1::values::vector_generation::ActiveMetricKind::Cosine)
        ) && vector.iter().all(|component| *component == 0.0)
        {
            return Err(HelixDbError::ZeroNormCosineVector);
        }

        if matches!(contract, VectorInsertContract::Upsert)
            && self.get_item(txn, node_id).await?.is_some()
        {
            self.stage_delete(txn, node_id).await?;
            metadata = self
                .get_metadata(txn)
                .await?
                .ok_or_else(|| HelixDbError::IndexNotFound(self.name().to_string()))?;
        }

        let vector_unaligned = UnalignedVector::from_slice(vector);
        let item = Item::<D> {
            header: D::new_header(&vector_unaligned),
            vector: vector_unaligned,
        };

        let simhash_cache = self.simhash_cache(metadata.config.dimension)?;
        let simhash = simhash_cache.compute_and_cache_measured(txn, node_id, vector)?;
        self.mark_memory_node_dirty(node_id);

        let node_layer =
            selected_layer.unwrap_or_else(|| self.select_mutation_layer(metadata.config.ml));
        let canonical_key = self.canonical_vector_key_from_simhash(node_id, simhash);
        let encoded_item = encode_item(&item);
        let rows = VectorWriteRows::new(txn, self.row_keyspace());

        if node_layer > 0 {
            rows.put_canonical_vector(&canonical_key, encoded_item.clone())?;
            rows.put_upper_vector(node_id, encoded_item)?;
            self.mark_memory_node_dirty(node_id);
        } else {
            rows.put_canonical_vector(&canonical_key, encoded_item)?;
        }

        self.repair_stale_entry_point_for_write(txn, &mut metadata, "insert", node_id)
            .await?;

        let VectorIndexState::Populated {
            entry_point,
            max_layer: previous_max_layer,
        } = metadata.validated_state()?
        else {
            metadata.entry_point = Some(node_id);
            metadata.max_layer = node_layer;
            metadata.count = 1;

            self.store_neighbors_layer0(txn, node_id, &[]).await?;
            for layer in 1..=node_layer {
                self.store_upper_neighbors(txn, layer, node_id, &[])?;
            }
            self.upsert_entry_candidate(txn, node_id, node_layer)
                .await?;
            self.update_metadata(txn, &metadata).await?;
            return Ok(());
        };

        self.insert_hnsw(txn, node_id, &item, node_layer, entry_point, &metadata)
            .await?;
        self.upsert_entry_candidate(txn, node_id, node_layer)
            .await?;

        if node_layer > previous_max_layer {
            metadata.entry_point = Some(node_id);
            metadata.max_layer = node_layer;
            self.update_metadata(txn, &metadata).await?;
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
        node_id: NodeId,
        item: &Item<'_, D>,
        node_layer: u16,
        entry_point: NodeId,
        metadata: &VectorIndexMetadata,
    ) -> Result<(), HelixDbError> {
        let old_max_layer = metadata.max_layer;
        let maximum_upper_connections = metadata.config.m;
        let connections = Connections::try_new(maximum_upper_connections)
            .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?;
        let doubled_upper_connections = connections
            .checked_double()
            .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?
            .get();
        let maximum_layer0_connections =
            Layer0Connections::try_new(metadata.config.m0, connections)
                .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?
                .get()
                .max(doubled_upper_connections);
        let ef_construction = metadata.config.ef_construction;
        let mut mutation_cache = MutationOpCache::<D>::with_degree_limits(
            maximum_layer0_connections,
            maximum_upper_connections,
        )?;

        let mut current_entry_point = entry_point;
        if node_layer < old_max_layer {
            for layer in (node_layer + 1..=old_max_layer).rev() {
                current_entry_point = self
                    .search_layer_greedy(txn, item, current_entry_point, layer)
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
                    &mut mutation_cache,
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
                    &mut mutation_cache,
                )
                .await?;

            self.stage_new_neighbors_for_mutation(
                txn,
                layer,
                node_id,
                neighbors.clone(),
                &mut mutation_cache,
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
                    &mut mutation_cache,
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
                    &mut mutation_cache,
                )
                .await?;
            }
        }

        self.flush_mutation_cache(txn, &mut mutation_cache).await
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
            .resolve_beam_entry_point_for_insert(txn, entry_point, layer, inserting_node_id)
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

            let neighbors = if layer == 0 {
                self.load_neighbors_for_mutation(txn, layer, current.node_id, mutation_cache)
                    .await?
            } else {
                self.load_upper_neighbors(txn, layer, current.node_id)
                    .await?
                    .unwrap_or_default()
            };
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
    #[cfg(any(test, feature = "production-coverage"))]
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
                .neighbor(row)
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
        while mutation_cache.neighbor_count() > OPERATION_NEIGHBOR_CACHE_LIMIT {
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
        mutation_cache.items.remove(&row.storage_parts());
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
            mutation_cache.items.remove(&(layer, node_id));
        } else {
            mutation_cache
                .neighbor_mut(row)
                .expect("retained flushed vector neighbor row remains cached")
                .mark_flushed();
        }
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
    pub(crate) async fn stage_delete(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
    ) -> Result<(), HelixDbError> {
        let mut metadata = self
            .get_metadata(txn)
            .await?
            .ok_or_else(|| HelixDbError::IndexNotFound(self.name().to_string()))?;

        self.repair_stale_entry_point_for_write(txn, &mut metadata, "delete", node_id)
            .await?;

        let node_max_layer = self.get_node_max_layer(txn, node_id, &metadata).await?;
        let maximum_upper_connections = metadata.config.m;
        let connections = Connections::try_new(maximum_upper_connections)
            .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?;
        let doubled_upper_connections = connections
            .checked_double()
            .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?
            .get();
        let maximum_layer0_connections =
            Layer0Connections::try_new(metadata.config.m0, connections)
                .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?
                .get()
                .max(doubled_upper_connections);
        let rows = VectorWriteRows::new(txn, self.row_keyspace());
        let reverse_sources = rows.reverse_sources_for_target(node_id).await?;
        let mut layers_to_process = (0..=node_max_layer).collect::<BTreeSet<_>>();
        layers_to_process.extend(reverse_sources.sources_by_layer().keys().copied());

        let mut deleted_node_outgoing_by_layer = HashMap::<u16, Vec<NodeId>>::new();
        let mut mutation_cache = MutationOpCache::<D>::with_degree_limits(
            maximum_layer0_connections,
            maximum_upper_connections,
        )?;

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
                    &mut mutation_cache,
                )
                .await?;
            deleted_node_outgoing_by_layer.insert(layer, outgoing_neighbors);
        }

        self.flush_mutation_cache(txn, &mut mutation_cache).await?;

        for (layer, neighbors) in deleted_node_outgoing_by_layer {
            for target_node_id in neighbors {
                rows.delete_reverse_locator(target_node_id, layer, node_id)?;
            }
        }
        rows.delete_reverse_sources(&reverse_sources)?;

        let (canonical_key, _) = self
            .resolve_canonical_vector_key_counted::<true>(
                txn,
                node_id,
                "deleting canonical vector payload",
            )
            .await?;
        if let Some(canonical_key) = canonical_key {
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

        if metadata.entry_point == Some(node_id) {
            if let Some((new_entry, new_max_layer)) = self.find_best_entry_candidate(txn).await? {
                metadata.entry_point = Some(new_entry);
                metadata.max_layer = new_max_layer;
            } else {
                metadata.entry_point = None;
                metadata.max_layer = 0;
            }
            self.update_metadata(txn, &metadata).await?;
        }

        Ok(())
    }

    /// Finds the highest layer that still contains the node being deleted.
    ///
    /// The typed entry-candidate row is authoritative when present. Otherwise
    /// upper rows are probed from the metadata maximum downward, with layer zero
    /// as the deployed fallback.
    pub(super) async fn get_node_max_layer(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        node_id: NodeId,
        metadata: &VectorIndexMetadata,
    ) -> Result<u16, HelixDbError> {
        let Some(candidate_layer) = self.get_entry_candidate_layer(txn, node_id).await? else {
            for layer in (1..=metadata.max_layer).rev() {
                if self
                    .load_upper_neighbors(txn, layer, node_id)
                    .await?
                    .is_some()
                {
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug)]
pub(super) struct MutationOpCache<D: Distance> {
    neighbor_rows: HashMap<NeighborRowId, CachedNeighbor>,
    pub(super) items: HashMap<(u16, NodeId), Option<Arc<Item<'static, D>>>>,
    degree_limits: NeighborDegreeLimits,
    next_touch: CacheSequence,
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
            items: HashMap::new(),
            degree_limits: NeighborDegreeLimits::try_new(layer0, upper)
                .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?,
            next_touch: CacheSequence::initial(),
        })
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

    /// Returns mutable state for a previously loaded row.
    pub(super) fn neighbor_mut(&mut self, row: NeighborRowId) -> Option<&mut CachedNeighbor> {
        self.neighbor_rows.get_mut(&row)
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
        self.neighbor_rows
            .insert(row, CachedNeighbor::clean(value, touch));
        true
    }

    /// Stages a row that must already have storage-proven cache state.
    pub(super) fn stage_loaded_neighbor(
        &mut self,
        row: NeighborRowId,
        value: NeighborRowValue,
    ) -> Result<(), HelixDbError> {
        let touch = self.take_touch();
        let Some(cached) = self.neighbor_rows.get_mut(&row) else {
            return Err(HelixDbError::InvariantViolation(
                "cannot stage an unloaded vector neighbor row without new-row proof".to_string(),
            ));
        };
        cached.stage(value, touch);
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
        let touch = self.take_touch();
        let mut cached = CachedNeighbor::clean(NeighborRowValue::KnownAbsent, touch);
        cached.stage(value, touch);
        self.neighbor_rows.insert(proof.row, cached);
    }

    /// Removes one row after clean eviction or a successful evicting flush.
    pub(super) fn remove_neighbor(&mut self, row: NeighborRowId) -> Option<CachedNeighbor> {
        self.neighbor_rows.remove(&row)
    }

    /// Returns the number of loaded neighbor rows.
    pub(super) fn neighbor_count(&self) -> usize {
        self.neighbor_rows.len()
    }

    /// Returns the oldest dirty row using one bounded cache scan.
    pub(super) fn oldest_dirty_neighbor(&self) -> Option<NeighborRowId> {
        self.neighbor_rows
            .iter()
            .filter(|(_, cached)| cached.is_dirty())
            .min_by_key(|(row, cached)| (cached.last_touch(), row.storage_parts()))
            .map(|(row, _)| *row)
    }

    /// Returns the oldest clean row using one bounded cache scan.
    pub(super) fn oldest_clean_neighbor(&self) -> Option<NeighborRowId> {
        self.neighbor_rows
            .iter()
            .filter(|(_, cached)| !cached.is_dirty())
            .min_by_key(|(row, cached)| (cached.last_touch(), row.storage_parts()))
            .map(|(row, _)| *row)
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
        let mut rows = self
            .neighbor_rows
            .iter()
            .map(|(row, cached)| (*row, cached.last_touch()))
            .collect::<Vec<_>>();
        rows.sort_by_key(|(row, touch)| (*touch, row.storage_parts()));
        for (sequence, (row, _)) in rows.into_iter().enumerate() {
            let sequence =
                u64::try_from(sequence).expect("bounded vector mutation cache length fits in u64");
            self.neighbor_rows
                .get_mut(&row)
                .expect("renumbered vector cache row still exists")
                .last_touch = CacheSequence(sequence);
        }
        self.next_touch = CacheSequence(
            u64::try_from(self.neighbor_rows.len())
                .expect("bounded vector mutation cache length fits in u64"),
        );
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/mutation.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;

    use super::*;

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
        cache.items.insert((0, 1), None);

        cache.install_loaded_neighbor(row, neighbors(1, vec![2]));
        cache
            .stage_loaded_neighbor(row, neighbors(1, vec![3]))
            .unwrap();
        cache.neighbor_mut(row).unwrap().mark_flushed();
        cache.remove_neighbor(row);

        assert!(cache.items.contains_key(&(0, 1)));
        assert_eq!(cache.items.len(), 1);
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
                        cache
                            .neighbor_mut(row)
                            .into_iter()
                            .for_each(CachedNeighbor::mark_flushed);
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
