//! Transaction-owned Active V2 vector mutation session.
//!
//! The runtime keeps one typed HNSW handle, metadata value, and mutation cache
//! per complete physical generation. It never publishes resident-cache state:
//! storage commit remains the only durability boundary, and the existing
//! transaction-local cache write set retains post-commit eviction ownership.

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU64;

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::encoding::v2::keys::indexes::vector::{
    VectorKey, VectorLayer0NeighborsKey, VectorUpperNeighborsKey,
};
use crate::encoding::NodeId;
use crate::error::HelixDbError;

use super::super::distance::{Cosine, Distance, Euclidean, Manhattan};
use super::super::neighbor_set::NeighborSet;
use super::super::unaligned_vector::UnalignedVector;
use super::super::{
    managed_vector_write_index, MeasuredVectorTransaction, ValidatedMetricVector,
    ValidatedVectorGenerationHandle, VectorCacheWriteSet, VectorDistanceMetric,
    VectorGenerationIdentity, VectorIndex, VectorIndexConfig, VectorIndexMetadata,
};
use super::{
    CacheSequence, CachedNeighbor, MutationDegreeLimits, MutationOpCache, NeighborRowId,
    NeighborRowValue, SessionCacheKind, VectorGenerationAfterDeletion, VectorInsertContract,
    VECTOR_BUILD_ITEM_CACHE_LIMIT, VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT,
    VECTOR_BUILD_SIMHASH_CACHE_LIMIT,
};

/// Closed request-local lifecycle for Active vector mutation state.
pub(crate) struct ActiveVectorMutationRuntime {
    state: ActiveVectorMutationState,
}

enum ActiveVectorMutationState {
    Open(Box<OpenActiveVectorMutations>),
    Prepared,
}

struct OpenActiveVectorMutations {
    cosine: ActiveMetricSession<Cosine>,
    euclidean: ActiveMetricSession<Euclidean>,
    manhattan: ActiveMetricSession<Manhattan>,
    next_touch: CacheSequence,
    max_payload_bytes: usize,
    max_items: usize,
    max_neighbors: usize,
    max_simhashes: usize,
    benchmark_layers: Option<Vec<u16>>,
}

struct ActiveMetricSession<D: Distance> {
    entries: HashMap<VectorGenerationIdentity, ActiveMutationEntry<D>>,
}

struct ActiveMutationEntry<D: Distance> {
    index: VectorIndex<D>,
    metadata: VectorIndexMetadata,
    cache: MutationOpCache<D>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ActiveMetricOrder {
    Cosine,
    Euclidean,
    Manhattan,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ActiveEvictionOrder {
    touch: CacheSequence,
    kind: SessionCacheKind,
    metric: ActiveMetricOrder,
    identity: VectorGenerationIdentity,
    layer: u16,
    node_id: NodeId,
}

struct ActiveEvictionCandidate {
    order: ActiveEvictionOrder,
    target: ActiveEvictionTarget,
}

enum ActiveEvictionTarget {
    Item {
        metric: ActiveMetricOrder,
        identity: VectorGenerationIdentity,
        layer: u16,
        node_id: NodeId,
    },
    Neighbor {
        metric: ActiveMetricOrder,
        identity: VectorGenerationIdentity,
        row: NeighborRowId,
    },
    SimHash {
        metric: ActiveMetricOrder,
        identity: VectorGenerationIdentity,
        node_id: NodeId,
    },
}

impl fmt::Debug for ActiveVectorMutationRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            ActiveVectorMutationState::Open(open) => formatter
                .debug_struct("ActiveVectorMutationRuntime")
                .field("state", &"Open")
                .field("generations", &open.generation_count())
                .field("retained_payload_bytes", &open.retained_payload_bytes())
                .finish(),
            ActiveVectorMutationState::Prepared => formatter
                .debug_struct("ActiveVectorMutationRuntime")
                .field("state", &"Prepared")
                .finish(),
        }
    }
}

impl ActiveVectorMutationRuntime {
    /// Creates an empty transaction-owned session with the backfill input ceiling.
    pub(crate) fn new(max_input_bytes: NonZeroU64) -> Self {
        Self {
            state: ActiveVectorMutationState::Open(Box::new(OpenActiveVectorMutations {
                cosine: ActiveMetricSession::default(),
                euclidean: ActiveMetricSession::default(),
                manhattan: ActiveMetricSession::default(),
                next_touch: CacheSequence::initial(),
                max_payload_bytes: usize::try_from(max_input_bytes.get()).unwrap_or(usize::MAX),
                max_items: VECTOR_BUILD_ITEM_CACHE_LIMIT,
                max_neighbors: VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT,
                max_simhashes: VECTOR_BUILD_SIMHASH_CACHE_LIMIT,
                benchmark_layers: None,
            })),
        }
    }

    /// Installs the deterministic one-generation layer script used by the batch benchmark.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn with_batch_benchmark_layers(mut self, layers: Vec<u16>) -> Self {
        let ActiveVectorMutationState::Open(open) = &mut self.state else {
            unreachable!("a newly constructed Active vector runtime is open");
        };
        open.benchmark_layers = Some(layers);
        self
    }

    /// Creates deterministic small limits for cross-metric and dirty-eviction tests.
    #[cfg(test)]
    pub(crate) fn with_test_limits(
        mut self,
        max_items: usize,
        max_neighbors: usize,
        max_simhashes: usize,
    ) -> Self {
        assert!(max_items > 0);
        assert!(max_neighbors > 0);
        assert!(max_simhashes > 0);
        let ActiveVectorMutationState::Open(open) = &mut self.state else {
            unreachable!("a newly constructed Active vector runtime is open");
        };
        open.max_items = max_items;
        open.max_neighbors = max_neighbors;
        open.max_simhashes = max_simhashes;
        self
    }

    /// Overrides retained-cache limits for benchmark-only counterfactual measurements.
    #[cfg(feature = "production-coverage")]
    pub(crate) fn with_batch_benchmark_limits(
        mut self,
        max_items: usize,
        max_neighbors: usize,
        max_simhashes: usize,
    ) -> Self {
        assert!(max_items > 0);
        assert!(max_neighbors > 0);
        assert!(max_simhashes > 0);
        let ActiveVectorMutationState::Open(open) = &mut self.state else {
            unreachable!("a newly constructed Active vector runtime is open");
        };
        open.max_items = max_items;
        open.max_neighbors = max_neighbors;
        open.max_simhashes = max_simhashes;
        self
    }

    /// Applies one Active V2 upsert without flushing reusable neighbor state.
    pub(crate) async fn upsert(
        &mut self,
        transaction: &DbTransaction,
        generation: &ValidatedVectorGenerationHandle,
        cache_writes: &VectorCacheWriteSet,
        node_id: NodeId,
        vector: &[f32],
        create: bool,
    ) -> Result<(), HelixDbError> {
        let open = self.open_mut()?;
        let changed = match generation.metric() {
            VectorDistanceMetric::Cosine => {
                open.cosine
                    .upsert(
                        transaction,
                        generation,
                        cache_writes,
                        node_id,
                        vector,
                        create,
                        &mut open.next_touch,
                        &mut open.benchmark_layers,
                    )
                    .await?
            }
            VectorDistanceMetric::Euclidean => {
                open.euclidean
                    .upsert(
                        transaction,
                        generation,
                        cache_writes,
                        node_id,
                        vector,
                        create,
                        &mut open.next_touch,
                        &mut open.benchmark_layers,
                    )
                    .await?
            }
            VectorDistanceMetric::Manhattan => {
                open.manhattan
                    .upsert(
                        transaction,
                        generation,
                        cache_writes,
                        node_id,
                        vector,
                        create,
                        &mut open.next_touch,
                        &mut open.benchmark_layers,
                    )
                    .await?
            }
        };
        transaction.mark_read(changed)?;
        open.enforce_limits(transaction).await
    }

    /// Applies one Active V2 deletion and returns whether metadata is now empty.
    pub(crate) async fn delete(
        &mut self,
        transaction: &DbTransaction,
        generation: &ValidatedVectorGenerationHandle,
        cache_writes: &VectorCacheWriteSet,
        node_id: NodeId,
    ) -> Result<bool, HelixDbError> {
        let open = self.open_mut()?;
        let (empty, changed) = match generation.metric() {
            VectorDistanceMetric::Cosine => {
                open.cosine
                    .delete(
                        transaction,
                        generation,
                        cache_writes,
                        node_id,
                        &mut open.next_touch,
                        &mut open.benchmark_layers,
                    )
                    .await?
            }
            VectorDistanceMetric::Euclidean => {
                open.euclidean
                    .delete(
                        transaction,
                        generation,
                        cache_writes,
                        node_id,
                        &mut open.next_touch,
                        &mut open.benchmark_layers,
                    )
                    .await?
            }
            VectorDistanceMetric::Manhattan => {
                open.manhattan
                    .delete(
                        transaction,
                        generation,
                        cache_writes,
                        node_id,
                        &mut open.next_touch,
                        &mut open.benchmark_layers,
                    )
                    .await?
            }
        };
        transaction.mark_read(changed)?;
        open.enforce_limits(transaction).await?;
        Ok(empty)
    }

    /// Repairs and removes one sorted non-empty cohort in a single generation boundary.
    pub(crate) async fn delete_batch(
        &mut self,
        transaction: &DbTransaction,
        generation: &ValidatedVectorGenerationHandle,
        cache_writes: &VectorCacheWriteSet,
        node_ids: &[NodeId],
    ) -> Result<VectorGenerationAfterDeletion, HelixDbError> {
        if node_ids.is_empty() || node_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(HelixDbError::InvariantViolation(
                "Active vector deletion cohort must be non-empty, sorted, and unique".to_string(),
            ));
        }
        let open = self.open_mut()?;
        let (outcome, changed) = match generation.metric() {
            VectorDistanceMetric::Cosine => {
                open.cosine
                    .delete_batch(
                        transaction,
                        generation,
                        cache_writes,
                        node_ids,
                        &mut open.next_touch,
                        &mut open.benchmark_layers,
                    )
                    .await?
            }
            VectorDistanceMetric::Euclidean => {
                open.euclidean
                    .delete_batch(
                        transaction,
                        generation,
                        cache_writes,
                        node_ids,
                        &mut open.next_touch,
                        &mut open.benchmark_layers,
                    )
                    .await?
            }
            VectorDistanceMetric::Manhattan => {
                open.manhattan
                    .delete_batch(
                        transaction,
                        generation,
                        cache_writes,
                        node_ids,
                        &mut open.next_touch,
                        &mut open.benchmark_layers,
                    )
                    .await?
            }
        };
        transaction.mark_read(changed)?;
        open.enforce_limits(transaction).await?;
        Ok(outcome)
    }

    /// Flushes and removes one exact generation before physical-empty validation.
    pub(crate) async fn drain_generation(
        &mut self,
        transaction: &DbTransaction,
        generation: &ValidatedVectorGenerationHandle,
    ) -> Result<(), HelixDbError> {
        let open = self.open_mut()?;
        match generation.metric() {
            VectorDistanceMetric::Cosine => {
                open.cosine.drain(transaction, generation.identity()).await
            }
            VectorDistanceMetric::Euclidean => {
                open.euclidean
                    .drain(transaction, generation.identity())
                    .await
            }
            VectorDistanceMetric::Manhattan => {
                open.manhattan
                    .drain(transaction, generation.identity())
                    .await
            }
        }
    }

    /// Flushes dirty rows for read-your-writes while retaining clean entries.
    pub(crate) async fn flush(&mut self, transaction: &DbTransaction) -> Result<(), HelixDbError> {
        self.open_mut()?.flush(transaction).await
    }

    /// Performs the final deterministic flush and seals the runtime for commit.
    pub(crate) async fn prepare(
        &mut self,
        transaction: &DbTransaction,
    ) -> Result<(), HelixDbError> {
        let open = self.open_mut()?;
        open.flush(transaction).await?;
        #[cfg(feature = "production-coverage")]
        open.record_benchmark_stats();
        self.state = ActiveVectorMutationState::Prepared;
        Ok(())
    }

    /// Consumes the state proof required by the graph commit boundary.
    pub(crate) fn consume_prepared(self) -> Result<(), HelixDbError> {
        match self.state {
            ActiveVectorMutationState::Prepared => Ok(()),
            ActiveVectorMutationState::Open(_) => Err(HelixDbError::InvariantViolation(
                "Active vector mutation runtime reached commit while still open".to_string(),
            )),
        }
    }

    fn open_mut(&mut self) -> Result<&mut OpenActiveVectorMutations, HelixDbError> {
        match &mut self.state {
            ActiveVectorMutationState::Open(open) => Ok(open),
            ActiveVectorMutationState::Prepared => Err(HelixDbError::InvariantViolation(
                "prepared Active vector mutation runtime cannot accept more work".to_string(),
            )),
        }
    }
}

impl<D: Distance> Default for ActiveMetricSession<D> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<D: Distance> ActiveMetricSession<D> {
    /// Runs one upsert through the retained generation session entry.
    #[allow(
        clippy::too_many_arguments,
        reason = "the session binds exact generation, cache, entity, vector, and creation state"
    )]
    async fn upsert(
        &mut self,
        transaction: &DbTransaction,
        generation: &ValidatedVectorGenerationHandle,
        cache_writes: &VectorCacheWriteSet,
        node_id: NodeId,
        vector: &[f32],
        create: bool,
        next_touch: &mut CacheSequence,
        benchmark_layers: &mut Option<Vec<u16>>,
    ) -> Result<Vec<Bytes>, HelixDbError> {
        let vector = ValidatedMetricVector::try_new(
            UnalignedVector::<D::VectorCodec>::from_slice(vector),
            generation.metric(),
            generation.dimension(),
        )
        .map_err(HelixDbError::from)?;
        let identity = generation.identity().clone();
        let mut entry = self
            .take_or_load(
                transaction,
                generation,
                cache_writes,
                create,
                benchmark_layers,
            )
            .await?;
        entry.cache.next_touch = *next_touch;
        entry.cache.begin_entity();
        let measured = MeasuredVectorTransaction::new(transaction);
        let result = entry
            .index
            .insert_with_mutation_cache(
                &measured,
                node_id,
                &vector,
                VectorInsertContract::Upsert,
                None,
                &mut entry.metadata,
                &mut entry.cache,
                false,
            )
            .await;
        let changed = entry.cache.finish_entity_changes();
        *next_touch = entry.cache.next_touch;
        let boundary = result
            .is_ok()
            .then(|| entry.stage_boundary_reverse_updates(&measured, changed));
        assert!(
            self.entries.insert(identity, entry).is_none(),
            "detached Active vector generation cannot be restored twice"
        );
        result?;
        boundary.expect("successful Active vector insertion has a boundary result")
    }

    async fn delete(
        &mut self,
        transaction: &DbTransaction,
        generation: &ValidatedVectorGenerationHandle,
        cache_writes: &VectorCacheWriteSet,
        node_id: NodeId,
        next_touch: &mut CacheSequence,
        benchmark_layers: &mut Option<Vec<u16>>,
    ) -> Result<(bool, Vec<Bytes>), HelixDbError> {
        let identity = generation.identity().clone();
        let mut entry = self
            .take_or_load(
                transaction,
                generation,
                cache_writes,
                false,
                benchmark_layers,
            )
            .await?;
        entry.cache.next_touch = *next_touch;
        entry.cache.begin_entity();
        let measured = MeasuredVectorTransaction::new(transaction);
        let result = entry
            .index
            .stage_delete_with_metadata(&measured, node_id, &mut entry.metadata, &mut entry.cache)
            .await;
        if matches!(result, Ok(false)) {
            entry
                .index
                .update_metadata(&measured, &entry.metadata)
                .await?;
        }
        let changed = entry.cache.finish_entity_changes();
        *next_touch = entry.cache.next_touch;
        let empty = entry.metadata.count == 0;
        let boundary = result
            .is_ok()
            .then(|| entry.stage_boundary_reverse_updates(&measured, changed));
        assert!(
            self.entries.insert(identity, entry).is_none(),
            "detached Active vector generation cannot be restored twice"
        );
        result?;
        let changed = boundary.expect("successful Active vector deletion has a boundary result")?;
        Ok((empty, changed))
    }

    /// Runs one physical cohort through the retained generation session entry.
    #[allow(
        clippy::too_many_arguments,
        reason = "the cohort boundary binds generation, cache, IDs, and session clocks"
    )]
    async fn delete_batch(
        &mut self,
        transaction: &DbTransaction,
        generation: &ValidatedVectorGenerationHandle,
        cache_writes: &VectorCacheWriteSet,
        node_ids: &[NodeId],
        next_touch: &mut CacheSequence,
        benchmark_layers: &mut Option<Vec<u16>>,
    ) -> Result<(VectorGenerationAfterDeletion, Vec<Bytes>), HelixDbError> {
        let identity = generation.identity().clone();
        let mut entry = self
            .take_or_load(
                transaction,
                generation,
                cache_writes,
                false,
                benchmark_layers,
            )
            .await?;
        entry.cache.next_touch = *next_touch;
        entry.cache.begin_entity();
        let measured = MeasuredVectorTransaction::new(transaction);
        let result = entry
            .index
            .stage_delete_batch_with_metadata(
                &measured,
                node_ids,
                &mut entry.metadata,
                &mut entry.cache,
            )
            .await;
        let changed = entry.cache.finish_entity_changes();
        *next_touch = entry.cache.next_touch;
        let boundary = result
            .is_ok()
            .then(|| entry.stage_boundary_reverse_updates(&measured, changed));
        assert!(
            self.entries.insert(identity, entry).is_none(),
            "detached Active vector generation cannot be restored twice"
        );
        let outcome = result?;
        let changed = boundary.expect("successful vector cohort has a boundary result")?;
        Ok((outcome, changed))
    }

    async fn take_or_load(
        &mut self,
        transaction: &DbTransaction,
        generation: &ValidatedVectorGenerationHandle,
        cache_writes: &VectorCacheWriteSet,
        create: bool,
        benchmark_layers: &mut Option<Vec<u16>>,
    ) -> Result<ActiveMutationEntry<D>, HelixDbError> {
        if let Some(entry) = self.entries.remove(generation.identity()) {
            if create {
                return Err(HelixDbError::InvariantViolation(
                    "existing Active vector session entry cannot be recreated".to_string(),
                ));
            }
            return Ok(entry);
        }
        let index = managed_vector_write_index::<D>(
            generation,
            cache_writes.dirty_rows_for(generation),
            cache_writes.simhasher_registry(),
        )
        .map_err(|error| HelixDbError::IndexCatalogCorruption(error.to_string()))?;
        #[cfg(test)]
        let index = match benchmark_layers.take() {
            Some(layers) => index.with_scripted_layers(layers).map_err(|error| {
                HelixDbError::Config(format!(
                    "invalid Active vector benchmark layer script: {error:?}"
                ))
            })?,
            None => index,
        };
        #[cfg(all(not(test), feature = "production-coverage"))]
        let index = match benchmark_layers.take() {
            Some(layers) => index
                .with_batch_benchmark_contract(layers)
                .map_err(|error| {
                    HelixDbError::Config(format!(
                        "invalid Active vector benchmark layer script: {error:?}"
                    ))
                })?,
            None => index,
        };
        #[cfg(not(any(test, feature = "production-coverage")))]
        debug_assert!(benchmark_layers.is_none());
        let measured = MeasuredVectorTransaction::new(transaction);
        let expected = VectorIndexConfig::from_v2_definition(
            generation.definition(),
            generation.physical_name(),
        );
        if create {
            index.stage_create(&measured, expected.clone()).await?;
        }
        let Some(metadata) = index.get_metadata(&measured).await? else {
            return Err(HelixDbError::IndexNotFound(
                generation.physical_name().to_string(),
            ));
        };
        metadata.validated_state()?;
        if !metadata.config.has_same_physical_contract(&expected) {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "Active vector metadata for '{}' conflicts with its canonical generation",
                generation.physical_name()
            )));
        }
        let limits = MutationDegreeLimits::try_from_metadata(&metadata)?;
        let cache = MutationOpCache::with_degree_limits(limits.layer0.get(), limits.upper.get())?
            .into_build_session_cache();
        Ok(ActiveMutationEntry {
            index,
            metadata,
            cache,
        })
    }

    async fn flush(&mut self, transaction: &DbTransaction) -> Result<(), HelixDbError> {
        let mut identities = self.entries.keys().cloned().collect::<Vec<_>>();
        identities.sort();
        let measured = MeasuredVectorTransaction::new(transaction);
        for identity in identities {
            let entry = self
                .entries
                .get_mut(&identity)
                .expect("selected Active vector generation remains present");
            while let Some(row) = entry.cache.oldest_dirty_neighbor() {
                entry
                    .index
                    .flush_one_active_cached_neighbor(&measured, &mut entry.cache, row, false)
                    .await?;
            }
        }
        Ok(())
    }

    async fn drain(
        &mut self,
        transaction: &DbTransaction,
        identity: &VectorGenerationIdentity,
    ) -> Result<(), HelixDbError> {
        let Some(mut entry) = self.entries.remove(identity) else {
            return Err(HelixDbError::InvariantViolation(
                "Active vector generation was absent while draining for retirement".to_string(),
            ));
        };
        let measured = MeasuredVectorTransaction::new(transaction);
        while let Some(row) = entry.cache.oldest_dirty_neighbor() {
            entry
                .index
                .flush_one_active_cached_neighbor(&measured, &mut entry.cache, row, false)
                .await?;
        }
        #[cfg(feature = "production-coverage")]
        super::super::record_benchmark_cache_stats(entry.cache.stats);
        Ok(())
    }
}

impl<D: Distance> ActiveMutationEntry<D> {
    fn stage_boundary_reverse_updates(
        &self,
        transaction: &MeasuredVectorTransaction<'_>,
        rows: impl IntoIterator<Item = (NeighborRowId, NeighborRowValue)>,
    ) -> Result<Vec<Bytes>, HelixDbError> {
        let mut keys = Vec::new();
        for (row, original) in rows {
            let (layer, node_id) = row.storage_parts();
            let current = self
                .cache
                .neighbor(row)
                .map(|cached| cached.current().clone())
                .unwrap_or(NeighborRowValue::KnownAbsent);
            let previous_neighbors = match original {
                NeighborRowValue::KnownAbsent => {
                    NeighborSet::empty(node_id, self.cache.degree_limit(layer))
                }
                NeighborRowValue::Present(neighbors) => neighbors,
            };
            let current_neighbors = match current {
                NeighborRowValue::KnownAbsent => {
                    NeighborSet::empty(node_id, self.cache.degree_limit(layer))
                }
                NeighborRowValue::Present(neighbors) => neighbors,
            };
            self.index.update_reverse_edge_locator(
                transaction,
                layer,
                node_id,
                &previous_neighbors,
                &current_neighbors,
            )?;
            let key = if layer == 0 {
                VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                    self.index.row_keyspace().index_id(),
                    node_id,
                ))
            } else {
                VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
                    self.index.row_keyspace().index_id(),
                    layer,
                    node_id,
                ))
            };
            keys.push(self.index.row_keyspace().key(key));
        }
        Ok(keys)
    }
}

impl OpenActiveVectorMutations {
    async fn flush(&mut self, transaction: &DbTransaction) -> Result<(), HelixDbError> {
        self.cosine.flush(transaction).await?;
        self.euclidean.flush(transaction).await?;
        self.manhattan.flush(transaction).await
    }

    async fn enforce_limits(&mut self, transaction: &DbTransaction) -> Result<(), HelixDbError> {
        loop {
            let payload_bytes = self.retained_payload_bytes();
            let payload_pressure = payload_bytes > self.max_payload_bytes;
            let item_pressure = self.item_count() > self.max_items;
            let neighbor_pressure = self.neighbor_count() > self.max_neighbors;
            let simhash_pressure = self.simhash_count() > self.max_simhashes;
            if !payload_pressure && !item_pressure && !neighbor_pressure && !simhash_pressure {
                #[cfg(feature = "production-coverage")]
                super::super::observe_benchmark_retained_payload(
                    u64::try_from(payload_bytes).unwrap_or(u64::MAX),
                );
                return Ok(());
            }

            let mut selected = None;
            select_candidate(
                ActiveMetricOrder::Cosine,
                &self.cosine,
                payload_pressure,
                item_pressure,
                neighbor_pressure,
                simhash_pressure,
                &mut selected,
            );
            select_candidate(
                ActiveMetricOrder::Euclidean,
                &self.euclidean,
                payload_pressure,
                item_pressure,
                neighbor_pressure,
                simhash_pressure,
                &mut selected,
            );
            select_candidate(
                ActiveMetricOrder::Manhattan,
                &self.manhattan,
                payload_pressure,
                item_pressure,
                neighbor_pressure,
                simhash_pressure,
                &mut selected,
            );
            let Some(selected) = selected else {
                return Err(HelixDbError::InvariantViolation(
                    "Active vector session exceeded a limit without an evictable entry".to_string(),
                ));
            };
            match selected.target {
                ActiveEvictionTarget::Item {
                    metric,
                    identity,
                    layer,
                    node_id,
                } => match metric {
                    ActiveMetricOrder::Cosine => {
                        evict_item(&mut self.cosine, &identity, layer, node_id)
                    }
                    ActiveMetricOrder::Euclidean => {
                        evict_item(&mut self.euclidean, &identity, layer, node_id)
                    }
                    ActiveMetricOrder::Manhattan => {
                        evict_item(&mut self.manhattan, &identity, layer, node_id)
                    }
                },
                ActiveEvictionTarget::Neighbor {
                    metric,
                    identity,
                    row,
                } => match metric {
                    ActiveMetricOrder::Cosine => {
                        evict_neighbor(transaction, &mut self.cosine, &identity, row).await?
                    }
                    ActiveMetricOrder::Euclidean => {
                        evict_neighbor(transaction, &mut self.euclidean, &identity, row).await?
                    }
                    ActiveMetricOrder::Manhattan => {
                        evict_neighbor(transaction, &mut self.manhattan, &identity, row).await?
                    }
                },
                ActiveEvictionTarget::SimHash {
                    metric,
                    identity,
                    node_id,
                } => match metric {
                    ActiveMetricOrder::Cosine => {
                        evict_simhash(&mut self.cosine, &identity, node_id)
                    }
                    ActiveMetricOrder::Euclidean => {
                        evict_simhash(&mut self.euclidean, &identity, node_id)
                    }
                    ActiveMetricOrder::Manhattan => {
                        evict_simhash(&mut self.manhattan, &identity, node_id)
                    }
                },
            }
        }
    }

    fn generation_count(&self) -> usize {
        self.cosine.entries.len() + self.euclidean.entries.len() + self.manhattan.entries.len()
    }

    fn item_count(&self) -> usize {
        self.cosine
            .entries
            .values()
            .map(|entry| entry.cache.item_count())
            .sum::<usize>()
            .saturating_add(
                self.euclidean
                    .entries
                    .values()
                    .map(|entry| entry.cache.item_count())
                    .sum(),
            )
            .saturating_add(
                self.manhattan
                    .entries
                    .values()
                    .map(|entry| entry.cache.item_count())
                    .sum(),
            )
    }

    fn neighbor_count(&self) -> usize {
        self.cosine
            .entries
            .values()
            .map(|entry| entry.cache.neighbor_count())
            .sum::<usize>()
            .saturating_add(
                self.euclidean
                    .entries
                    .values()
                    .map(|entry| entry.cache.neighbor_count())
                    .sum(),
            )
            .saturating_add(
                self.manhattan
                    .entries
                    .values()
                    .map(|entry| entry.cache.neighbor_count())
                    .sum(),
            )
    }

    fn simhash_count(&self) -> usize {
        self.cosine
            .entries
            .values()
            .map(|entry| entry.cache.simhash_count())
            .sum::<usize>()
            .saturating_add(
                self.euclidean
                    .entries
                    .values()
                    .map(|entry| entry.cache.simhash_count())
                    .sum(),
            )
            .saturating_add(
                self.manhattan
                    .entries
                    .values()
                    .map(|entry| entry.cache.simhash_count())
                    .sum(),
            )
    }

    fn retained_payload_bytes(&self) -> usize {
        self.cosine
            .entries
            .values()
            .map(|entry| entry.cache.retained_payload_bytes)
            .chain(
                self.euclidean
                    .entries
                    .values()
                    .map(|entry| entry.cache.retained_payload_bytes),
            )
            .chain(
                self.manhattan
                    .entries
                    .values()
                    .map(|entry| entry.cache.retained_payload_bytes),
            )
            .fold(0_usize, usize::saturating_add)
    }

    #[cfg(feature = "production-coverage")]
    fn record_benchmark_stats(&self) {
        for stats in self
            .cosine
            .entries
            .values()
            .map(|entry| entry.cache.stats)
            .chain(
                self.euclidean
                    .entries
                    .values()
                    .map(|entry| entry.cache.stats),
            )
            .chain(
                self.manhattan
                    .entries
                    .values()
                    .map(|entry| entry.cache.stats),
            )
        {
            super::super::record_benchmark_cache_stats(stats);
        }
    }
}

fn select_candidate<D: Distance>(
    metric: ActiveMetricOrder,
    session: &ActiveMetricSession<D>,
    payload_pressure: bool,
    item_pressure: bool,
    neighbor_pressure: bool,
    simhash_pressure: bool,
    selected: &mut Option<ActiveEvictionCandidate>,
) {
    for (identity, entry) in &session.entries {
        let cache = &entry.cache;
        if (payload_pressure || item_pressure)
            && let Some((touch, layer, node_id)) = cache.item_recency.first().copied()
        {
            consider_candidate(
                ActiveEvictionCandidate {
                    order: ActiveEvictionOrder {
                        touch,
                        kind: SessionCacheKind::Item,
                        metric,
                        identity: identity.clone(),
                        layer,
                        node_id,
                    },
                    target: ActiveEvictionTarget::Item {
                        metric,
                        identity: identity.clone(),
                        layer,
                        node_id,
                    },
                },
                selected,
            );
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
            consider_candidate(
                ActiveEvictionCandidate {
                    order: ActiveEvictionOrder {
                        touch,
                        kind: SessionCacheKind::Neighbor,
                        metric,
                        identity: identity.clone(),
                        layer,
                        node_id,
                    },
                    target: ActiveEvictionTarget::Neighbor {
                        metric,
                        identity: identity.clone(),
                        row,
                    },
                },
                selected,
            );
        }
        if (payload_pressure || simhash_pressure)
            && let Some((touch, node_id)) = cache.simhash_recency.first().copied()
        {
            consider_candidate(
                ActiveEvictionCandidate {
                    order: ActiveEvictionOrder {
                        touch,
                        kind: SessionCacheKind::SimHash,
                        metric,
                        identity: identity.clone(),
                        layer: 0,
                        node_id,
                    },
                    target: ActiveEvictionTarget::SimHash {
                        metric,
                        identity: identity.clone(),
                        node_id,
                    },
                },
                selected,
            );
        }
    }
}

fn consider_candidate(
    candidate: ActiveEvictionCandidate,
    selected: &mut Option<ActiveEvictionCandidate>,
) {
    if selected
        .as_ref()
        .is_none_or(|current| candidate.order < current.order)
    {
        *selected = Some(candidate);
    }
}

fn evict_item<D: Distance>(
    session: &mut ActiveMetricSession<D>,
    identity: &VectorGenerationIdentity,
    layer: u16,
    node_id: NodeId,
) {
    let entry = session
        .entries
        .get_mut(identity)
        .expect("selected Active vector item namespace remains present");
    entry.cache.remove_item(layer, node_id);
    entry.cache.stats.item_evictions = entry.cache.stats.item_evictions.saturating_add(1);
}

async fn evict_neighbor<D: Distance>(
    transaction: &DbTransaction,
    session: &mut ActiveMetricSession<D>,
    identity: &VectorGenerationIdentity,
    row: NeighborRowId,
) -> Result<(), HelixDbError> {
    let entry = session
        .entries
        .get_mut(identity)
        .expect("selected Active vector neighbor namespace remains present");
    let dirty = entry
        .cache
        .neighbor(row)
        .is_some_and(CachedNeighbor::is_dirty);
    if dirty {
        let measured = MeasuredVectorTransaction::new(transaction);
        entry
            .index
            .flush_one_active_cached_neighbor(&measured, &mut entry.cache, row, true)
            .await?;
    } else {
        entry.cache.remove_neighbor(row);
        let (layer, node_id) = row.storage_parts();
        entry.cache.remove_item(layer, node_id);
    }
    entry.cache.stats.neighbor_evictions = entry.cache.stats.neighbor_evictions.saturating_add(1);
    Ok(())
}

fn evict_simhash<D: Distance>(
    session: &mut ActiveMetricSession<D>,
    identity: &VectorGenerationIdentity,
    node_id: NodeId,
) {
    let entry = session
        .entries
        .get_mut(identity)
        .expect("selected Active vector SimHash namespace remains present");
    entry.cache.invalidate_simhash(node_id);
    entry.cache.stats.simhash_evictions = entry.cache.stats.simhash_evictions.saturating_add(1);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::encoding::v2::keys::indexes::vector::VectorStorageLane;
    use crate::encoding::v2::keys::scope::DataScope;
    use crate::encoding::v2::keys::DataKey;
    use crate::index_lifecycle::IndexElementKind;
    use crate::search::vector::{SimHasherRegistry, VectorDimension, VectorIndexState};

    #[derive(Clone)]
    enum Operation {
        Upsert(NodeId, Vec<f32>),
        Delete(NodeId),
    }

    async fn exact_namespace_rows(db: &Db, physical_index_id: u64) -> Vec<(Bytes, Bytes)> {
        let mut rows = Vec::new();
        for lane in VectorStorageLane::ALL {
            let prefix = DataKey::data_prefix(
                DataScope::LegacyUnscoped,
                lane.prefix_key(physical_index_id).to_bytes(),
            );
            let mut scan = db.scan_prefix(prefix, ..).await.unwrap();
            while let Some(row) = scan.next().await.unwrap() {
                rows.push((row.key, row.value));
            }
        }
        rows.sort();
        rows
    }

    fn generation<D: Distance>(
        physical_name: &str,
        physical_index_id: u64,
    ) -> ValidatedVectorGenerationHandle {
        ValidatedVectorGenerationHandle::create_current::<D>(
            VectorGenerationIdentity::try_new(
                DataScope::LegacyUnscoped,
                physical_index_id + 1_000,
                physical_name.to_string(),
                physical_index_id,
                NonZeroU64::MIN,
                1,
                IndexElementKind::Node,
                VectorDimension::try_new(4).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    async fn differential_session_matches_per_entity_flush<D: Distance>(
        name: &str,
        small_limits: bool,
    ) {
        let old_db = Arc::new(
            Db::open(format!("{name}-old"), Arc::new(InMemory::new()))
                .await
                .unwrap(),
        );
        let new_db = Arc::new(
            Db::open(format!("{name}-new"), Arc::new(InMemory::new()))
                .await
                .unwrap(),
        );
        let handle = generation::<D>(name, 41);
        let layers = vec![0, 1, 0, 1, 0, 2];
        let old_index = VectorIndex::<D>::from_generation(&handle)
            .with_scripted_layers(layers.clone())
            .unwrap();
        let create_index = VectorIndex::<D>::from_generation(&handle);
        let config =
            VectorIndexConfig::from_v2_definition(handle.definition(), handle.physical_name());
        let old_create = old_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        old_index.create(&old_create, config.clone()).await.unwrap();
        old_create.commit().await.unwrap();
        let new_create = new_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        create_index.create(&new_create, config).await.unwrap();
        new_create.commit().await.unwrap();

        let operations = vec![
            Operation::Upsert(1, vec![1.0, 0.1, 0.2, 0.3]),
            Operation::Upsert(2, vec![0.2, 1.0, 0.3, 0.4]),
            Operation::Upsert(3, vec![0.3, 0.4, 1.0, 0.5]),
            Operation::Upsert(2, vec![0.7, 0.1, 0.8, 0.2]),
            Operation::Delete(1),
            Operation::Upsert(1, vec![0.4, 0.9, 0.2, 0.6]),
            Operation::Delete(3),
            Operation::Upsert(2, vec![0.8, 0.3, 0.1, 0.7]),
        ];
        let old_txn = old_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let new_txn = new_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let cache_writes = VectorCacheWriteSet::new(Arc::new(SimHasherRegistry::default()));
        let mut runtime =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap())
                .with_batch_benchmark_layers(layers);
        if small_limits {
            runtime = runtime.with_test_limits(2, 2, 2);
        }
        for operation in operations {
            match operation {
                Operation::Upsert(node_id, vector) => {
                    old_index.insert(&old_txn, node_id, &vector).await.unwrap();
                    runtime
                        .upsert(&new_txn, &handle, &cache_writes, node_id, &vector, false)
                        .await
                        .unwrap();
                }
                Operation::Delete(node_id) => {
                    old_index.delete(&old_txn, node_id).await.unwrap();
                    runtime
                        .delete(&new_txn, &handle, &cache_writes, node_id)
                        .await
                        .unwrap();
                }
            }
        }
        runtime.prepare(&new_txn).await.unwrap();
        old_txn.commit().await.unwrap();
        new_txn.commit().await.unwrap();

        assert_eq!(
            exact_namespace_rows(&old_db, handle.physical_index_id()).await,
            exact_namespace_rows(&new_db, handle.physical_index_id()).await
        );
        old_db.close().await.unwrap();
        new_db.close().await.unwrap();
    }

    async fn run_deletion_cohort(
        database: &str,
        batched: bool,
    ) -> (Vec<(Bytes, Bytes)>, Vec<NodeId>) {
        let db = Arc::new(Db::open(database, Arc::new(InMemory::new())).await.unwrap());
        let handle = generation::<Cosine>("active-deletion-cohort", 91);
        let index = VectorIndex::<Cosine>::from_generation(&handle);
        let seeded = VectorIndex::<Cosine>::from_generation(&handle)
            .with_scripted_layers(vec![2, 1, 0, 1, 0, 2, 0, 1])
            .unwrap();
        let seed = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        seeded
            .create(
                &seed,
                VectorIndexConfig::from_v2_definition(handle.definition(), handle.physical_name()),
            )
            .await
            .unwrap();
        for node_id in 1..=8 {
            let value = node_id as f32;
            seeded
                .insert(&seed, node_id, &[value, value + 1.0, 9.0 - value, 0.5])
                .await
                .unwrap();
        }
        seed.commit().await.unwrap();

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let cache_writes = VectorCacheWriteSet::new(Arc::new(SimHasherRegistry::default()));
        let mut runtime =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap());
        if batched {
            assert!(matches!(
                runtime
                    .delete_batch(&transaction, &handle, &cache_writes, &[])
                    .await,
                Err(HelixDbError::InvariantViolation(message))
                    if message.contains("non-empty, sorted, and unique")
            ));
            assert!(matches!(
                runtime
                    .delete_batch(&transaction, &handle, &cache_writes, &[3, 2])
                    .await,
                Err(HelixDbError::InvariantViolation(message))
                    if message.contains("non-empty, sorted, and unique")
            ));
            assert_eq!(
                runtime
                    .delete_batch(&transaction, &handle, &cache_writes, &[2, 3, 6])
                    .await
                    .unwrap(),
                VectorGenerationAfterDeletion::Retained
            );
        } else {
            for node_id in [2, 3, 6] {
                assert!(!runtime
                    .delete(&transaction, &handle, &cache_writes, node_id)
                    .await
                    .unwrap());
            }
        }
        runtime.prepare(&transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let metadata = index.get_metadata(db.as_ref()).await.unwrap().unwrap();
        assert_eq!(metadata.count, 5);
        let VectorIndexState::Populated { entry_point, .. } = metadata.validated_state().unwrap()
        else {
            panic!("a partial cohort must retain populated metadata");
        };
        assert!(![2, 3, 6].contains(&entry_point));
        for node_id in 1..=8 {
            assert_eq!(
                index
                    .get_item(db.as_ref(), node_id)
                    .await
                    .unwrap()
                    .is_some(),
                ![2, 3, 6].contains(&node_id)
            );
        }
        for target in [2, 3, 6] {
            let reverse = index
                .load_reverse_sources_for_target(db.as_ref(), target)
                .await
                .unwrap();
            assert!(reverse.sources_by_layer().values().all(Vec::is_empty));
        }
        let results = index
            .search(
                db.as_ref(),
                &[1.0, 2.0, 8.0, 0.5],
                &crate::search::vector::SearchParams::new(5)
                    .unwrap()
                    .with_ef(16)
                    .unwrap(),
            )
            .await
            .unwrap();
        let result_ids = results
            .into_iter()
            .map(|result| result.entity_id())
            .collect::<Vec<_>>();
        assert_eq!(result_ids.len(), 5);
        assert!(result_ids
            .iter()
            .all(|node_id| ![2, 3, 6].contains(node_id)));
        let rows = exact_namespace_rows(&db, handle.physical_index_id()).await;
        db.close().await.unwrap();
        (rows, result_ids)
    }

    #[tokio::test]
    async fn session_is_byte_exact_for_every_metric_and_repeated_transition() {
        differential_session_matches_per_entity_flush::<Cosine>(
            "active-session-cosine-differential",
            false,
        )
        .await;
        differential_session_matches_per_entity_flush::<Euclidean>(
            "active-session-euclidean-differential",
            false,
        )
        .await;
        differential_session_matches_per_entity_flush::<Manhattan>(
            "active-session-manhattan-differential",
            false,
        )
        .await;
    }

    #[tokio::test]
    async fn dirty_eviction_under_small_global_limits_is_byte_exact() {
        differential_session_matches_per_entity_flush::<Cosine>(
            "active-session-dirty-eviction-differential",
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn cohort_deletion_is_deterministic_and_semantically_matches_sequential_deletion() {
        let (first_rows, first_results) =
            run_deletion_cohort("active-deletion-cohort-first", true).await;
        let (second_rows, second_results) =
            run_deletion_cohort("active-deletion-cohort-second", true).await;
        let (_, sequential_results) =
            run_deletion_cohort("active-deletion-cohort-sequential", false).await;

        assert_eq!(first_rows, second_rows);
        assert_eq!(first_results, second_results);
        assert_eq!(
            first_results.into_iter().collect::<BTreeSet<_>>(),
            sequential_results.into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[tokio::test]
    async fn cohort_deletion_cleans_missing_rows_reverse_residue_and_stale_entry_metadata() {
        let db = Arc::new(
            Db::open("active-deletion-cohort-residue", Arc::new(InMemory::new()))
                .await
                .unwrap(),
        );
        let handle = generation::<Cosine>("active-deletion-cohort-residue", 92);
        let index = VectorIndex::<Cosine>::from_generation(&handle)
            .with_scripted_layers(vec![1, 0, 1])
            .unwrap();
        let seed = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        index
            .create(
                &seed,
                VectorIndexConfig::from_v2_definition(handle.definition(), handle.physical_name()),
            )
            .await
            .unwrap();
        for (node_id, vector) in [
            (1, [1.0, 0.1, 0.2, 0.3]),
            (2, [0.2, 1.0, 0.3, 0.4]),
            (3, [0.3, 0.4, 1.0, 0.5]),
        ] {
            index.insert(&seed, node_id, &vector).await.unwrap();
        }
        seed.commit().await.unwrap();

        let corrupt = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let (canonical_key, _) = index
            .resolve_required_canonical_vector_key_counted(
                &corrupt,
                2,
                "seeding batched deletion residue",
            )
            .await
            .unwrap();
        let measured = MeasuredVectorTransaction::new(&corrupt);
        let rows = super::super::VectorWriteRows::new(&measured, index.row_keyspace());
        rows.delete_canonical_vector(&canonical_key).unwrap();
        rows.delete_layer0_neighbors(2).unwrap();
        rows.put_reverse_locator(2, 0, 99).unwrap();
        let mut metadata = index.get_metadata(&measured).await.unwrap().unwrap();
        metadata.entry_point = Some(9_999);
        metadata.max_layer = metadata.max_layer.max(1);
        index.update_metadata(&measured, &metadata).await.unwrap();
        corrupt.commit().await.unwrap();

        let deletion = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let cache_writes = VectorCacheWriteSet::new(Arc::new(SimHasherRegistry::default()));
        let mut runtime =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap());
        assert_eq!(
            runtime
                .delete_batch(&deletion, &handle, &cache_writes, &[2])
                .await
                .unwrap(),
            VectorGenerationAfterDeletion::Retained
        );
        runtime.prepare(&deletion).await.unwrap();
        deletion.commit().await.unwrap();

        assert!(index.get_item(db.as_ref(), 2).await.unwrap().is_none());
        let reverse = index
            .load_reverse_sources_for_target(db.as_ref(), 2)
            .await
            .unwrap();
        assert!(reverse.sources_by_layer().values().all(Vec::is_empty));
        let metadata = index.get_metadata(db.as_ref()).await.unwrap().unwrap();
        let entry_point = metadata
            .entry_point
            .expect("survivors retain an entry point");
        assert_ne!(entry_point, 9_999);
        assert!(index
            .get_item(db.as_ref(), entry_point)
            .await
            .unwrap()
            .is_some());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_vector_aborts_without_durable_partial_graph() {
        enum ExpectedError {
            Dimension,
            Component(usize),
            ZeroNorm,
        }

        let db = Arc::new(
            Db::open(
                "active-session-invalid-vector-abort",
                Arc::new(InMemory::new()),
            )
            .await
            .unwrap(),
        );
        let handle = generation::<Cosine>("active-session-invalid-vector-abort", 73);
        let index = VectorIndex::<Cosine>::from_generation(&handle);
        let create = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        index
            .create(
                &create,
                VectorIndexConfig::from_v2_definition(handle.definition(), handle.physical_name()),
            )
            .await
            .unwrap();
        create.commit().await.unwrap();
        let before = exact_namespace_rows(&db, handle.physical_index_id()).await;

        for (invalid, expected) in [
            (vec![1.0, 0.2, 0.3], ExpectedError::Dimension),
            (vec![f32::NAN, 0.2, 0.3, 0.4], ExpectedError::Component(0)),
            (
                vec![1.0, 0.2, f32::INFINITY, 0.4],
                ExpectedError::Component(2),
            ),
            (vec![0.0; 4], ExpectedError::ZeroNorm),
        ] {
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            let cache_writes = VectorCacheWriteSet::new(Arc::new(SimHasherRegistry::default()));
            let mut runtime =
                ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap())
                    .with_batch_benchmark_layers(vec![0, 0]);
            runtime
                .upsert(
                    &transaction,
                    &handle,
                    &cache_writes,
                    1,
                    &[1.0, 0.2, 0.3, 0.4],
                    false,
                )
                .await
                .unwrap();
            let error = runtime
                .upsert(&transaction, &handle, &cache_writes, 2, &invalid, false)
                .await
                .unwrap_err();
            match expected {
                ExpectedError::Dimension => assert!(matches!(
                    error,
                    HelixDbError::InvalidDimension {
                        expected: 4,
                        got: 3
                    }
                )),
                ExpectedError::Component(expected_index) => assert!(matches!(
                    error,
                    HelixDbError::InvalidVectorComponent { index }
                        if index == expected_index
                )),
                ExpectedError::ZeroNorm => {
                    assert!(matches!(error, HelixDbError::ZeroNormCosineVector));
                }
            }
            transaction.rollback();

            assert_eq!(
                before,
                exact_namespace_rows(&db, handle.physical_index_id()).await
            );
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn magnitude_validation_precedes_active_generation_creation() {
        let db = Arc::new(
            Db::open(
                "active-session-magnitude-before-create",
                Arc::new(InMemory::new()),
            )
            .await
            .unwrap(),
        );
        let handle = generation::<Euclidean>("active-session-magnitude-before-create", 76);
        let index = VectorIndex::<Euclidean>::from_generation(&handle);
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let cache_writes = VectorCacheWriteSet::new(Arc::new(SimHasherRegistry::default()));
        let mut runtime =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap());

        let error = runtime
            .upsert(
                &transaction,
                &handle,
                &cache_writes,
                1,
                &[f32::MAX, 0.0, 0.0, 0.0],
                true,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            HelixDbError::VectorComponentMagnitudeExceeded {
                metric: VectorDistanceMetric::Euclidean,
                dimension: 4,
                component_index: 0,
                ..
            }
        ));
        assert!(index.get_metadata(&transaction).await.unwrap().is_none());

        transaction.rollback();
        assert!(exact_namespace_rows(&db, handle.physical_index_id())
            .await
            .is_empty());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn missing_and_mismatched_metadata_fail_without_durable_writes() {
        let db = Arc::new(
            Db::open("active-session-invalid-metadata", Arc::new(InMemory::new()))
                .await
                .unwrap(),
        );
        let missing = generation::<Cosine>("active-session-missing-metadata", 74);
        let mismatched = generation::<Cosine>("active-session-mismatched-metadata", 75);
        let mismatched_index = VectorIndex::<Cosine>::from_generation(&mismatched);
        let mut mismatched_config = VectorIndexConfig::from_v2_definition(
            mismatched.definition(),
            mismatched.physical_name(),
        );
        mismatched_config.property_name = "different-property".to_string();
        let create = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        mismatched_index
            .create(&create, mismatched_config)
            .await
            .unwrap();
        create.commit().await.unwrap();
        let mismatched_before = exact_namespace_rows(&db, mismatched.physical_index_id()).await;

        let missing_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let cache_writes = VectorCacheWriteSet::new(Arc::new(SimHasherRegistry::default()));
        let mut missing_runtime =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap());
        assert!(matches!(
            missing_runtime
                .upsert(
                    &missing_transaction,
                    &missing,
                    &cache_writes,
                    1,
                    &[1.0, 0.2, 0.3, 0.4],
                    false,
                )
                .await,
            Err(HelixDbError::IndexNotFound(name)) if name == missing.physical_name()
        ));
        missing_transaction.rollback();
        assert!(exact_namespace_rows(&db, missing.physical_index_id())
            .await
            .is_empty());

        let mismatched_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mut mismatched_runtime =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap());
        assert!(matches!(
            mismatched_runtime
                .upsert(
                    &mismatched_transaction,
                    &mismatched,
                    &cache_writes,
                    1,
                    &[1.0, 0.2, 0.3, 0.4],
                    false,
                )
                .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("conflicts with its canonical generation")
        ));
        mismatched_transaction.rollback();
        assert_eq!(
            mismatched_before,
            exact_namespace_rows(&db, mismatched.physical_index_id()).await
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_create_and_absent_drain_fail_closed() {
        let db = Arc::new(
            Db::open(
                "active-session-invalid-lifecycle",
                Arc::new(InMemory::new()),
            )
            .await
            .unwrap(),
        );
        let duplicate = generation::<Cosine>("active-session-duplicate-create", 76);
        let duplicate_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let cache_writes = VectorCacheWriteSet::new(Arc::new(SimHasherRegistry::default()));
        let mut duplicate_runtime =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap())
                .with_batch_benchmark_layers(vec![0, 0]);
        duplicate_runtime
            .upsert(
                &duplicate_transaction,
                &duplicate,
                &cache_writes,
                1,
                &[1.0, 0.2, 0.3, 0.4],
                true,
            )
            .await
            .unwrap();
        assert!(matches!(
            duplicate_runtime
                .upsert(
                    &duplicate_transaction,
                    &duplicate,
                    &cache_writes,
                    2,
                    &[0.2, 1.0, 0.3, 0.4],
                    true,
                )
                .await,
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("cannot be recreated")
        ));
        duplicate_transaction.rollback();
        assert!(exact_namespace_rows(&db, duplicate.physical_index_id())
            .await
            .is_empty());

        let drained = generation::<Cosine>("active-session-absent-drain", 77);
        let create = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        VectorIndex::<Cosine>::from_generation(&drained)
            .create(
                &create,
                VectorIndexConfig::from_v2_definition(
                    drained.definition(),
                    drained.physical_name(),
                ),
            )
            .await
            .unwrap();
        create.commit().await.unwrap();
        let drained_before = exact_namespace_rows(&db, drained.physical_index_id()).await;
        let drain_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mut drain_runtime =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap())
                .with_batch_benchmark_layers(vec![0, 0]);
        for (node_id, vector) in [(1, [1.0, 0.2, 0.3, 0.4]), (2, [0.2, 1.0, 0.3, 0.4])] {
            drain_runtime
                .upsert(
                    &drain_transaction,
                    &drained,
                    &cache_writes,
                    node_id,
                    &vector,
                    false,
                )
                .await
                .unwrap();
        }
        drain_runtime
            .drain_generation(&drain_transaction, &drained)
            .await
            .unwrap();
        assert!(matches!(
            drain_runtime
                .drain_generation(&drain_transaction, &drained)
                .await,
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("absent while draining")
        ));
        drain_transaction.rollback();
        assert_eq!(
            drained_before,
            exact_namespace_rows(&db, drained.physical_index_id()).await
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn prepared_state_rejects_work_and_open_state_cannot_commit() {
        let db = Arc::new(
            Db::open("active-session-closed-state", Arc::new(InMemory::new()))
                .await
                .unwrap(),
        );
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mut prepared =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap());
        prepared.prepare(&transaction).await.unwrap();
        assert!(matches!(
            prepared.flush(&transaction).await,
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("prepared Active vector mutation runtime")
        ));
        prepared.consume_prepared().unwrap();

        let open = ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap());
        assert!(matches!(
            open.consume_prepared(),
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("still open")
        ));
        transaction.rollback();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn coalesced_neighbor_return_still_conflicts_with_concurrent_write() {
        let db = Arc::new(
            Db::open(
                "active-session-coalesced-neighbor-conflict",
                Arc::new(InMemory::new()),
            )
            .await
            .unwrap(),
        );
        let handle = generation::<Cosine>("active-session-coalesced-neighbor-conflict", 79);
        let index = VectorIndex::<Cosine>::from_generation(&handle)
            .with_scripted_layers(vec![0, 0])
            .unwrap();
        let seed = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        index
            .create(
                &seed,
                VectorIndexConfig::from_v2_definition(handle.definition(), handle.physical_name()),
            )
            .await
            .unwrap();
        index.insert(&seed, 1, &[1.0, 0.1, 0.2, 0.3]).await.unwrap();
        index.insert(&seed, 2, &[0.2, 1.0, 0.3, 0.4]).await.unwrap();
        seed.commit().await.unwrap();
        let before = exact_namespace_rows(&db, handle.physical_index_id()).await;

        let returning = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let competing = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let cache_writes = VectorCacheWriteSet::new(Arc::new(SimHasherRegistry::default()));
        let mut runtime =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap())
                .with_batch_benchmark_layers(vec![0]);
        runtime
            .upsert(
                &returning,
                &handle,
                &cache_writes,
                3,
                &[0.9, 0.1, 0.2, 0.3],
                false,
            )
            .await
            .unwrap();
        runtime
            .delete(&returning, &handle, &cache_writes, 3)
            .await
            .unwrap();
        runtime.prepare(&returning).await.unwrap();

        let neighbor_key = VectorIndex::<Cosine>::from_generation(&handle)
            .row_keyspace()
            .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                handle.physical_index_id(),
                1,
            )));
        let original_neighbor = db.get(&neighbor_key).await.unwrap().unwrap();
        competing.put(neighbor_key, original_neighbor).unwrap();
        competing.commit().await.unwrap();

        let conflict = returning.commit().await.unwrap_err();
        assert_eq!(conflict.kind(), slatedb::ErrorKind::Transaction);
        assert_eq!(
            before,
            exact_namespace_rows(&db, handle.physical_index_id()).await
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn global_entry_caps_apply_across_all_metrics() {
        let db = Arc::new(
            Db::open(
                "active-session-global-metric-limits",
                Arc::new(InMemory::new()),
            )
            .await
            .unwrap(),
        );
        let cosine = generation::<Cosine>("active-session-global-cosine", 81);
        let euclidean = generation::<Euclidean>("active-session-global-euclidean", 82);
        let manhattan = generation::<Manhattan>("active-session-global-manhattan", 83);
        let create = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        VectorIndex::<Cosine>::from_generation(&cosine)
            .create(
                &create,
                VectorIndexConfig::from_v2_definition(cosine.definition(), cosine.physical_name()),
            )
            .await
            .unwrap();
        VectorIndex::<Euclidean>::from_generation(&euclidean)
            .create(
                &create,
                VectorIndexConfig::from_v2_definition(
                    euclidean.definition(),
                    euclidean.physical_name(),
                ),
            )
            .await
            .unwrap();
        VectorIndex::<Manhattan>::from_generation(&manhattan)
            .create(
                &create,
                VectorIndexConfig::from_v2_definition(
                    manhattan.definition(),
                    manhattan.physical_name(),
                ),
            )
            .await
            .unwrap();
        create.commit().await.unwrap();

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let cache_writes = VectorCacheWriteSet::new(Arc::new(SimHasherRegistry::default()));
        let mut runtime =
            ActiveVectorMutationRuntime::new(NonZeroU64::new(8 * 1024 * 1024).unwrap())
                .with_test_limits(3, 3, 3);
        for (node_id, generation, vector) in [
            (1, &cosine, [1.0, 0.1, 0.2, 0.3]),
            (2, &euclidean, [0.2, 1.0, 0.3, 0.4]),
            (3, &manhattan, [0.3, 0.4, 1.0, 0.5]),
            (4, &cosine, [0.4, 0.5, 0.6, 1.0]),
            (5, &euclidean, [0.5, 0.6, 1.0, 0.7]),
            (6, &manhattan, [0.6, 1.0, 0.7, 0.8]),
        ] {
            runtime
                .upsert(
                    &transaction,
                    generation,
                    &cache_writes,
                    node_id,
                    &vector,
                    false,
                )
                .await
                .unwrap();
            let ActiveVectorMutationState::Open(open) = &runtime.state else {
                panic!("mutation runtime remains open before preparation");
            };
            assert!(open.item_count() <= 3);
            assert!(open.neighbor_count() <= 3);
            assert!(open.simhash_count() <= 3);
            assert!(open.retained_payload_bytes() <= 8 * 1024 * 1024);
        }
        runtime.prepare(&transaction).await.unwrap();
        transaction.commit().await.unwrap();
        db.close().await.unwrap();
    }
}
