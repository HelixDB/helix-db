//! Production contracts for operation-local vector mutation state.
//!
//! This feature-gated child module verifies the closed neighbor-cache ADT,
//! fresh-row proof, bounded eviction, entry-candidate cleanup, stale-root
//! repair, and typed neighbor writes. Storage fixtures use only existing vector
//! keys and codecs in isolated databases, so no alternate format is introduced.

use std::collections;
use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::encoding::v2::keys::indexes::vector::{VectorEntryCandidateNodeKey, VectorKey};
use crate::encoding::v2::values::indexes::vector::entry_candidate::encode_entry_candidate_layer;
use crate::search::vector::distance::{Cosine, Distance, Euclidean, Manhattan};
use crate::search::vector::{self, VectorIndexConfig, VectorWriteMeasurement};

/// Distance type without an active persisted semantic binding.
#[derive(Debug, Clone)]
enum UnboundMutationDistance {}

impl Distance for UnboundMutationDistance {
    type Header = ();
    type VectorCodec = f32;

    fn name() -> &'static str {
        "production-unbound-mutation-distance"
    }

    fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {}

    fn distance(_left: &Item<Self>, _right: &Item<Self>) -> f32 {
        0.0
    }

    fn norm_no_header(_vector: &UnalignedVector<Self::VectorCodec>) -> f32 {
        0.0
    }
}

impl crate::search::vector::distance::sealed::Sealed for UnboundMutationDistance {}

/// Builds one canonical present row for concise transition fixtures.
fn neighbors(owner: NodeId, nodes: Vec<NodeId>) -> NeighborRowValue {
    NeighborRowValue::Present(
        NeighborSet::try_from_canonical(owner, NeighborDegreeLimit::try_new(8).unwrap(), nodes)
            .unwrap(),
    )
}

/// Verifies row identity, values, first-original retention, and flushing.
fn run_value_contracts() {
    let node = NeighborRowId::new(HnswLayer::from_deployed(3), VectorEntityId::Node(42));
    assert_eq!(node.storage_parts(), (3, 42));
    assert_eq!(node.layer().number(), 3);
    assert_eq!(node.entity(), VectorEntityId::Node(42));
    let edge = NeighborRowId::new(HnswLayer::from_deployed(4), VectorEntityId::Edge(7));
    assert_eq!(edge.storage_parts(), (4, 7));
    assert_eq!(CacheSequence(u64::MAX).checked_next(), None);

    let original = neighbors(1, vec![2]);
    let staged = neighbors(1, vec![3]);
    let mut cached = CachedNeighbor::clean(original.clone(), CacheSequence::initial());
    assert!(!cached.is_dirty());
    cached.stage(staged.clone(), CacheSequence(1));
    cached.stage(NeighborRowValue::KnownAbsent, CacheSequence(2));
    assert_eq!(cached.original(), Some(&original));
    assert_eq!(cached.current(), &NeighborRowValue::KnownAbsent);
    cached.mark_flushed();
    cached.mark_flushed();
    assert_eq!(cached.original(), None);
}

/// Verifies admission, proof, eviction ordering, rollover, and prefetch policy.
fn run_cache_contracts<D: Distance>() {
    assert!(MutationOpCache::<D>::with_degree_limits(0, 8).is_err());
    let mut cache = MutationOpCache::<D>::with_degree_limits(4, 2).unwrap();
    assert_eq!(cache.degree_limit(0).get(), 4);
    assert_eq!(cache.degree_limit(1).get(), 2);
    let first = MutationOpCache::<D>::node_row_id(0, 1);
    let second = MutationOpCache::<D>::node_row_id(0, 2);
    let third = MutationOpCache::<D>::node_row_id(1, 3);
    assert!(cache.install_loaded_neighbor(first, neighbors(1, vec![4])));
    assert!(!cache.install_loaded_neighbor(first, neighbors(1, vec![5])));
    assert!(cache.install_loaded_neighbor(second, NeighborRowValue::KnownAbsent));
    assert!(cache
        .stage_loaded_neighbor(third, neighbors(3, vec![4]))
        .is_err());
    assert!(cache.prove_new_neighbor_row(first).is_err());
    let proof = cache.prove_new_neighbor_row(third).unwrap();
    cache.stage_new_neighbor(proof, neighbors(3, vec![4]));
    cache
        .stage_loaded_neighbor(first, neighbors(1, vec![5]))
        .unwrap();
    assert_eq!(cache.oldest_clean_neighbor(), Some(second));
    assert_eq!(cache.oldest_dirty_neighbor(), Some(third));
    cache.mark_neighbor_flushed(third);
    assert!(cache.remove_neighbor(third).is_some());
    assert!(cache.remove_neighbor(third).is_none());

    cache.next_touch = CacheSequence(u64::MAX);
    cache
        .stage_loaded_neighbor(first, NeighborRowValue::KnownAbsent)
        .unwrap();
    cache
        .stage_loaded_neighbor(second, neighbors(2, vec![1]))
        .unwrap();
    assert!(
        cache.neighbor(first).unwrap().last_touch() < cache.neighbor(second).unwrap().last_touch()
    );
    assert_eq!(cache.neighbor_count(), 2);
    assert!(cache.contains_neighbor(first));

    assert!(select_layer0_neighbor_prefetch_targets::<D>(&[(4, 1.0)], &cache, 8).is_empty());
    assert!(
        select_layer0_neighbor_prefetch_targets::<D>(&[(4, 1.0), (5, 2.0)], &cache, 0).is_empty()
    );
    assert_eq!(
        select_layer0_neighbor_prefetch_targets::<D>(
            &[(2, 0.1), (4, 0.4), (4, 0.4), (5, 0.5), (6, 0.6)],
            &cache,
            2,
        ),
        vec![4, 5]
    );

    let missing_row = MutationOpCache::<D>::node_row_id(0, 999);
    cache.mark_neighbor_flushed(missing_row);
    let clean_row = MutationOpCache::<D>::node_row_id(0, 998);
    cache.install_loaded_neighbor(clean_row, NeighborRowValue::KnownAbsent);
    cache.mark_neighbor_flushed(clean_row);

    assert!(!cache.evict_oldest_item());
    cache.invalidate_simhash(999);
    assert!(!cache.evict_oldest_simhash());
    cache.put_item(0, 7, None, 3);
    cache.put_item(0, 7, None, 5);
    cache.put_simhash(7, Some(crate::search::vector::SimHash::from_bits(7)));
    cache.put_simhash(7, None);
    cache.next_touch = CacheSequence(u64::MAX);
    assert!(matches!(cache.item(0, 7), Some(None)));
    assert!(cache.evict_oldest_item());
    assert!(cache.evict_oldest_simhash());
}

/// Verifies entry-candidate cleanup and stale-root repair in one write view.
async fn run_entry_repair_contracts(db: &Db) {
    let index = VectorIndex::<Cosine>::new("production-vector-mutation-entry-repair");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .create(
            &txn,
            VectorIndexConfig::new(index.name(), "embedding", 3)
                .with_m(4)
                .with_m0(8)
                .with_ef_construction(16),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    for (node_id, vector, layer) in [(1, [1.0, 0.0, 0.0], 2), (2, [0.0, 1.0, 0.0], 0)] {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        index
            .insert_with_measured_transaction(
                &measured,
                node_id,
                &vector,
                VectorInsertContract::Upsert,
                Some(layer),
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    assert_eq!(
        index
            .get_entry_candidate_layer(&measured, 999)
            .await
            .unwrap(),
        None
    );
    let original_layer = index
        .get_entry_candidate_layer(&measured, 1)
        .await
        .unwrap()
        .unwrap();
    index
        .upsert_entry_candidate(&measured, 1, original_layer)
        .await
        .unwrap();
    index
        .upsert_entry_candidate(&measured, 1, original_layer.saturating_add(1))
        .await
        .unwrap();
    index
        .upsert_entry_candidate(&measured, 1, original_layer)
        .await
        .unwrap();

    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    rows.put_entry_candidate(96, 10).unwrap();
    measured
        .put(
            index.row_keyspace().key(VectorKey::EntryCandidateNode(
                VectorEntryCandidateNodeKey::new(index.id(), 96),
            )),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    assert_eq!(
        index
            .get_entry_candidate_layer(&measured, 96)
            .await
            .unwrap(),
        None
    );
    rows.put_entry_candidate(92, 11).unwrap();
    measured
        .put(
            index.row_keyspace().key(VectorKey::EntryCandidateNode(
                VectorEntryCandidateNodeKey::new(index.id(), 92),
            )),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();

    rows.put_entry_candidate(95, 9).unwrap();
    rows.delete_entry_candidate_node(95).unwrap();
    rows.put_entry_candidate(94, 8).unwrap();
    measured
        .put(
            index.row_keyspace().key(VectorKey::EntryCandidateNode(
                VectorEntryCandidateNodeKey::new(index.id(), 94),
            )),
            encode_entry_candidate_layer(7),
        )
        .unwrap();
    rows.put_entry_candidate(93, 6).unwrap();
    assert_eq!(
        index.find_best_entry_candidate(&measured).await.unwrap(),
        Some((1, original_layer))
    );

    let mut metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let mut mutation_cache = MutationOpCache::<Cosine>::default();
    assert!(!index
        .repair_stale_entry_point_for_write(
            &measured,
            &mut metadata,
            "live",
            10,
            &mut mutation_cache,
        )
        .await
        .unwrap());
    let live = index
        .resolve_beam_entry_point_for_insert(
            &measured,
            metadata.entry_point.unwrap(),
            0,
            10,
            &mut mutation_cache,
        )
        .await
        .unwrap();
    assert_eq!(live.unwrap().0, metadata.entry_point.unwrap());

    metadata.entry_point = Some(999);
    assert!(index
        .repair_stale_entry_point_for_write(
            &measured,
            &mut metadata,
            "replace",
            10,
            &mut mutation_cache,
        )
        .await
        .unwrap());
    assert_eq!(metadata.entry_point, Some(1));
    let replacement = index
        .resolve_beam_entry_point_for_insert(&measured, 999, 0, 10, &mut mutation_cache)
        .await
        .unwrap();
    assert_eq!(replacement.unwrap().0, 1);

    index.remove_entry_candidate(&measured, 1).await.unwrap();
    index.remove_entry_candidate(&measured, 1).await.unwrap();
    index.remove_entry_candidate(&measured, 2).await.unwrap();
    metadata.entry_point = Some(999);
    metadata.max_layer = 1;
    assert!(index
        .repair_stale_entry_point_for_write(
            &measured,
            &mut metadata,
            "clear",
            10,
            &mut mutation_cache,
        )
        .await
        .unwrap());
    assert_eq!(metadata.entry_point, None);
    assert_eq!(metadata.max_layer, 0);
    assert!(index
        .resolve_beam_entry_point_for_insert(&measured, 999, 0, 10, &mut mutation_cache)
        .await
        .unwrap()
        .is_none());

    let mut contradictory = metadata.clone();
    contradictory.max_layer = 1;
    assert!(index
        .update_metadata(&measured, &contradictory)
        .await
        .is_err());
    let mut changed_dimension = metadata;
    changed_dimension.config.dimension = 4;
    assert!(matches!(
        index.update_metadata(&measured, &changed_dimension).await,
        Err(HelixDbError::InvariantViolation(_))
    ));
    txn.rollback();
}

/// Verifies typed neighbor loading, staging, flushing, and bounded eviction.
async fn run_neighbor_write_contracts(db: &Db) {
    let missing = VectorIndex::<Cosine>::new("production-vector-mutation-insert-missing");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    assert!(matches!(
        missing
            .insert_with_measured_transaction(
                &measured,
                1,
                &[1.0, 0.0, 0.0],
                VectorInsertContract::Upsert,
                Some(0),
            )
            .await,
        Err(HelixDbError::IndexNotFound(_))
    ));
    txn.rollback();

    let index = VectorIndex::<Cosine>::new("production-vector-mutation-neighbors");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .create(
            &txn,
            VectorIndexConfig::new(index.name(), "embedding", 3)
                .with_m(4)
                .with_m0(8)
                .with_ef_construction(16),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    assert!(matches!(
        index
            .insert_with_measured_transaction(
                &measured,
                90,
                &[1.0, 0.0],
                VectorInsertContract::Upsert,
                Some(0),
            )
            .await,
        Err(HelixDbError::InvalidDimension { .. })
    ));
    assert!(matches!(
        index
            .insert_with_measured_transaction(
                &measured,
                91,
                &[1.0, f32::NAN, 0.0],
                VectorInsertContract::Upsert,
                Some(0),
            )
            .await,
        Err(HelixDbError::InvalidVectorComponent { index: 1 })
    ));
    assert!(matches!(
        index
            .insert_with_measured_transaction(
                &measured,
                92,
                &[0.0, 0.0, 0.0],
                VectorInsertContract::Upsert,
                Some(0),
            )
            .await,
        Err(HelixDbError::ZeroNormCosineVector)
    ));
    txn.rollback();

    for (node_id, vector) in [
        (1, [1.0, 0.0, 0.0]),
        (2, [0.9, 0.1, 0.0]),
        (3, [0.0, 1.0, 0.0]),
    ] {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        index
            .insert_with_measured_transaction(
                &measured,
                node_id,
                &vector,
                VectorInsertContract::Upsert,
                Some(0),
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    index
        .insert_with_measured_transaction(
            &measured,
            2,
            &[0.85, 0.15, 0.0],
            VectorInsertContract::Upsert,
            Some(1),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let item = Item::<Cosine>::new(vec![1.0, 0.0, 0.0]);
    let mut mutation_cache = MutationOpCache::<Cosine>::default();
    assert_eq!(
        index
            .search_layer_greedy_for_mutation(&measured, &item, 999_999, 0, &mut mutation_cache,)
            .await
            .unwrap(),
        999_999
    );

    let mut zero_connections = metadata.clone();
    zero_connections.config.m = 0;
    assert!(matches!(
        index
            .insert_hnsw(
                &measured,
                PopulatedHnswInsertion {
                    node_id: 600,
                    item: &item,
                    node_layer: 0,
                    entry_point: 1,
                    metadata: &zero_connections,
                },
                &mut mutation_cache,
            )
            .await,
        Err(HelixDbError::InvalidVectorConfig(_))
    ));

    let mut overflowing_connections = metadata.clone();
    overflowing_connections.config.m = usize::MAX;
    assert!(matches!(
        index
            .insert_hnsw(
                &measured,
                PopulatedHnswInsertion {
                    node_id: 601,
                    item: &item,
                    node_layer: 0,
                    entry_point: 1,
                    metadata: &overflowing_connections,
                },
                &mut mutation_cache,
            )
            .await,
        Err(HelixDbError::InvalidVectorConfig(_))
    ));

    let mut undersized_layer0 = metadata.clone();
    undersized_layer0.config.m0 = undersized_layer0.config.m.saturating_sub(1);
    assert!(matches!(
        index
            .insert_hnsw(
                &measured,
                PopulatedHnswInsertion {
                    node_id: 602,
                    item: &item,
                    node_layer: 0,
                    entry_point: 1,
                    metadata: &undersized_layer0,
                },
                &mut mutation_cache,
            )
            .await,
        Err(HelixDbError::InvalidVectorConfig(_))
    ));

    let mut cache = MutationOpCache::<Cosine>::with_degree_limits(8, 4).unwrap();
    let stored = index
        .load_neighbors_for_mutation(&measured, 0, 1, &mut cache)
        .await
        .unwrap();
    assert_eq!(
        index
            .load_neighbors_for_mutation(&measured, 0, 1, &mut cache)
            .await
            .unwrap(),
        stored
    );
    assert!(index
        .load_neighbors_for_mutation(&measured, 0, 999, &mut cache)
        .await
        .unwrap()
        .is_empty());
    assert!(index
        .load_neighbors_for_mutation(&measured, 0, 999, &mut cache)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        index
            .prefetch_layer0_neighbors_for_mutation(&measured, &[], &mut cache)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        index
            .prefetch_layer0_neighbors_for_mutation(&measured, &[1, 2, 2, 998], &mut cache)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        index
            .prefetch_layer0_neighbors_for_mutation(&measured, &[1, 2, 998, 999], &mut cache)
            .await
            .unwrap(),
        0
    );

    assert!(index
        .stage_neighbors_for_mutation(&measured, 0, 700, &[], &mut cache)
        .await
        .is_err());
    index
        .stage_new_neighbors_for_mutation(&measured, 0, 700, Vec::new(), &mut cache)
        .await
        .unwrap();
    assert!(index
        .stage_new_neighbors_for_mutation(&measured, 0, 700, Vec::new(), &mut cache)
        .await
        .is_err());
    assert!(index
        .stage_neighbors_vec_for_mutation(&measured, 0, 700, vec![700], &mut cache)
        .await
        .is_err());
    index
        .stage_neighbors_vec_for_mutation(&measured, 0, 700, vec![3, 2], &mut cache)
        .await
        .unwrap();

    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    rows.put_layer0_neighbors(701, &[701]).unwrap();
    let mut malformed_load = MutationOpCache::<Cosine>::with_degree_limits(8, 4).unwrap();
    assert!(index
        .load_neighbors_for_mutation(&measured, 0, 701, &mut malformed_load)
        .await
        .is_err());

    rows.put_layer0_neighbors(702, &[702]).unwrap();
    let mut malformed_prefetch = MutationOpCache::<Cosine>::with_degree_limits(8, 4).unwrap();
    assert!(index
        .prefetch_layer0_neighbors_for_mutation(&measured, &[702], &mut malformed_prefetch)
        .await
        .is_err());

    let mut malformed_new = MutationOpCache::<Cosine>::with_degree_limits(8, 4).unwrap();
    assert!(index
        .stage_new_neighbors_for_mutation(&measured, 0, 703, vec![703], &mut malformed_new)
        .await
        .is_err());

    let absent = MutationOpCache::<Cosine>::node_row_id(0, 999);
    cache
        .stage_loaded_neighbor(absent, NeighborRowValue::KnownAbsent)
        .unwrap();
    assert!(index
        .flush_one_cached_neighbor(&measured, &mut cache, absent, false)
        .await
        .is_err());
    index
        .stage_neighbors_for_mutation(&measured, 0, 999, &[], &mut cache)
        .await
        .unwrap();
    index
        .flush_one_cached_neighbor(
            &measured,
            &mut cache,
            MutationOpCache::<Cosine>::node_row_id(0, 123_456),
            false,
        )
        .await
        .unwrap();
    index
        .flush_mutation_cache(&measured, &mut cache)
        .await
        .unwrap();
    assert!(!cache.neighbor(absent).unwrap().is_dirty());

    index.store_upper_neighbors(&measured, 1, 1, &[2]).unwrap();
    let mut upper_cache = MutationOpCache::<Cosine>::with_degree_limits(8, 4).unwrap();
    assert_eq!(
        index
            .load_neighbors_for_mutation(&measured, 1, 1, &mut upper_cache)
            .await
            .unwrap(),
        vec![2]
    );
    index
        .flush_one_cached_neighbor(
            &measured,
            &mut upper_cache,
            MutationOpCache::<Cosine>::node_row_id(1, 1),
            false,
        )
        .await
        .unwrap();
    assert!(!index.evict_oldest_clean_neighbor(&mut MutationOpCache::default()));
    upper_cache.put_item(1, 1, None, 0);
    assert!(index.evict_oldest_clean_neighbor(&mut upper_cache));
    assert!(!upper_cache.item_is_known_absent(1, 1));

    let first = neighbors(800, vec![801]);
    let second = neighbors(800, vec![802]);
    let NeighborRowValue::Present(first) = first else {
        unreachable!("neighbor fixture is present")
    };
    let NeighborRowValue::Present(second) = second else {
        unreachable!("neighbor fixture is present")
    };
    assert!(VectorIndex::<Cosine>::neighbor_deltas(&first, &second).is_ok());
    let foreign =
        NeighborSet::try_from_canonical(900, NeighborDegreeLimit::try_new(8).unwrap(), vec![901])
            .unwrap();
    assert!(VectorIndex::<Cosine>::neighbor_deltas(&first, &foreign).is_err());
    assert!(index
        .load_reverse_sources_for_target(&measured, 2)
        .await
        .is_ok());

    let mut bounded = MutationOpCache::<Cosine>::with_degree_limits(8, 4).unwrap();
    for node_id in
        10_000_u64..10_000_u64 + u64::try_from(VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT + 1).unwrap()
    {
        bounded.install_loaded_neighbor(
            MutationOpCache::<Cosine>::node_row_id(0, node_id),
            neighbors(node_id, Vec::new()),
        );
    }
    let dirty = MutationOpCache::<Cosine>::node_row_id(0, 10_000);
    bounded
        .stage_loaded_neighbor(dirty, neighbors(10_000, vec![20_000]))
        .unwrap();
    bounded.put_item(0, 10_000, None, 0);
    index
        .enforce_mutation_cache_bounds(&measured, &mut bounded)
        .await
        .unwrap();
    assert_eq!(bounded.neighbor_count(), VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT);
    assert!(!bounded.contains_neighbor(dirty));
    assert!(!bounded.item_is_known_absent(0, 10_000));

    let mut clean_bounded = MutationOpCache::<Cosine>::with_degree_limits(8, 4).unwrap();
    for node_id in
        30_000_u64..30_000_u64 + u64::try_from(VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT + 1).unwrap()
    {
        clean_bounded.install_loaded_neighbor(
            MutationOpCache::<Cosine>::node_row_id(0, node_id),
            neighbors(node_id, Vec::new()),
        );
    }
    index
        .enforce_mutation_cache_bounds(&measured, &mut clean_bounded)
        .await
        .unwrap();
    assert_eq!(
        clean_bounded.neighbor_count(),
        VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT
    );
    txn.rollback();
}

/// Verifies deterministic graph deletion and asymmetric-residue repair.
async fn run_graph_delete_contracts(db: &Db) {
    let missing = VectorIndex::<Cosine>::new("production-vector-mutation-delete-missing");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    assert!(matches!(
        missing.stage_delete(&measured, 1).await,
        Err(HelixDbError::IndexNotFound(_))
    ));
    txn.rollback();

    let index = VectorIndex::<Cosine>::new("production-vector-mutation-delete");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .create(
            &txn,
            VectorIndexConfig::new(index.name(), "embedding", 3)
                .with_m(2)
                .with_m0(4)
                .with_ef_construction(8),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    for (node_id, vector, layer) in [
        (1, [1.0, 0.0, 0.0], 2),
        (2, [0.9, 0.1, 0.0], 1),
        (3, [0.8, 0.2, 0.0], 0),
        (4, [0.7, 0.3, 0.0], 0),
        (5, [0.6, 0.4, 0.0], 0),
        (6, [0.0, 1.0, 0.0], 0),
    ] {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        index
            .insert_with_measured_transaction(
                &measured,
                node_id,
                &vector,
                VectorInsertContract::Upsert,
                Some(layer),
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let mut insert_reached_success = false;
    for successful_writes in 0..128 {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        measured.fail_write_after(successful_writes);
        let result = index
            .insert_with_measured_transaction(
                &measured,
                777,
                &[0.5, 0.25, 0.25],
                VectorInsertContract::Upsert,
                Some(2),
            )
            .await;
        txn.rollback();
        if result.is_ok() {
            insert_reached_success = true;
            break;
        }
    }
    assert!(insert_reached_success);

    let mut insert_read_reached_success = false;
    for successful_reads in 0..128 {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        measured.fail_read_after(successful_reads);
        let result = index
            .insert_with_measured_transaction(
                &measured,
                778,
                &[0.5, 0.2, 0.3],
                VectorInsertContract::Upsert,
                Some(2),
            )
            .await;
        txn.rollback();
        if result.is_ok() {
            insert_read_reached_success = true;
            break;
        }
    }
    assert!(insert_read_reached_success);

    let mut delete_reached_success = false;
    for successful_writes in 0..128 {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        measured.fail_write_after(successful_writes);
        let result = index.stage_delete(&measured, 3).await;
        txn.rollback();
        if result.is_ok() {
            delete_reached_success = true;
            break;
        }
    }
    assert!(delete_reached_success);

    let mut delete_read_reached_success = false;
    for successful_reads in 0..128 {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        measured.fail_read_after(successful_reads);
        let result = index.stage_delete(&measured, 3).await;
        txn.rollback();
        if result.is_ok() {
            delete_read_reached_success = true;
            break;
        }
    }
    assert!(delete_read_reached_success);

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let entry = metadata.entry_point.unwrap();
    let query = Item::<Cosine>::new(vec![1.0, 0.0, 0.0]);
    let mut cache = MutationOpCache::<Cosine>::with_degree_limits(4, 2).unwrap();
    let layer0 = index
        .search_layer_beam(&measured, &query, entry, 0, 4, 999, &mut cache)
        .await
        .unwrap();
    assert!(!layer0.is_empty());
    assert!(!index
        .select_neighbors_heuristic(&txn, &query, &layer0, 2, 0, &mut cache)
        .await
        .unwrap()
        .is_empty());
    assert!(!index
        .search_layer_beam(&measured, &query, entry, 1, 4, 999, &mut cache)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        index
            .get_node_max_layer(&measured, 1, &metadata)
            .await
            .unwrap(),
        2
    );
    index.remove_entry_candidate(&measured, 2).await.unwrap();
    assert_eq!(
        index
            .get_node_max_layer(&measured, 2, &metadata)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        index
            .get_node_max_layer(&measured, 999, &metadata)
            .await
            .unwrap(),
        0
    );

    let empty = VectorIndex::<Cosine>::new("production-vector-mutation-empty-beam");
    empty
        .create(
            &txn,
            VectorIndexConfig::new(empty.name(), "embedding", 3)
                .with_m(2)
                .with_m0(4)
                .with_ef_construction(8),
        )
        .await
        .unwrap();
    let mut empty_cache = MutationOpCache::<Cosine>::with_degree_limits(4, 2).unwrap();
    assert!(empty
        .search_layer_beam(&measured, &query, 999, 0, 4, 1, &mut empty_cache)
        .await
        .unwrap()
        .is_empty());

    let missing_candidate = [Candidate::try_new(999, 0.5).unwrap()];
    assert!(index
        .select_neighbors_heuristic(&txn, &query, &missing_candidate, 2, 0, &mut cache,)
        .await
        .unwrap()
        .is_empty());

    let from_item = index.get_item(&measured, 1).await.unwrap().unwrap();
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    let simhash_cache = index.simhash_cache(3).unwrap();
    simhash_cache
        .set(&txn, 997, vector::SimHash::from_bits(997))
        .unwrap();
    rows.put_layer0_neighbors(997, &[2, 3]).unwrap();
    let mut missing_destination = MutationOpCache::<Cosine>::with_degree_limits(4, 2).unwrap();
    index
        .add_bidirectional_link(
            &measured,
            0,
            1,
            997,
            &from_item,
            2,
            &mut missing_destination,
        )
        .await
        .unwrap();
    index
        .flush_mutation_cache(&measured, &mut missing_destination)
        .await
        .unwrap();

    simhash_cache
        .set(&txn, 998, vector::SimHash::from_bits(998))
        .unwrap();
    rows.put_layer0_neighbors(998, &[1, 2]).unwrap();
    let mut existing_link = MutationOpCache::<Cosine>::with_degree_limits(4, 2).unwrap();
    index
        .add_bidirectional_link(&measured, 0, 1, 998, &from_item, 2, &mut existing_link)
        .await
        .unwrap();

    rows.put_layer0_neighbors(2, &[3, 997]).unwrap();
    let mut pruned_link = MutationOpCache::<Cosine>::with_degree_limits(4, 2).unwrap();
    index
        .add_bidirectional_link(&measured, 0, 1, 2, &from_item, 2, &mut pruned_link)
        .await
        .unwrap();

    rows.put_layer0_neighbors(6, &[]).unwrap();
    rows.put_layer0_neighbors(3, &[4, 997]).unwrap();
    let mut relink = MutationOpCache::<Cosine>::with_degree_limits(4, 2).unwrap();
    index
        .relink_neighbor(
            &measured,
            0,
            6,
            &collections::HashSet::from([3, 997]),
            2,
            &mut relink,
        )
        .await
        .unwrap();
    index
        .flush_mutation_cache(&measured, &mut relink)
        .await
        .unwrap();

    let mut empty_delete = MutationOpCache::<Cosine>::with_degree_limits(4, 2).unwrap();
    assert!(index
        .delete_from_layer(&measured, 995, 0, 4, &[], &mut empty_delete)
        .await
        .unwrap()
        .is_empty());
    assert!(index
        .delete_from_layer(&measured, 994, 0, 4, &[993], &mut empty_delete)
        .await
        .unwrap()
        .is_empty());
    index
        .relink_neighbor(
            &measured,
            0,
            992,
            &collections::HashSet::from([1]),
            4,
            &mut empty_delete,
        )
        .await
        .unwrap();
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    index.stage_delete(&measured, 3).await.unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(index.get_item(&txn, 3).await.unwrap().is_none());
    assert_ne!(
        index.get_metadata(&txn).await.unwrap().unwrap().entry_point,
        Some(3)
    );
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    rows.put_layer0_neighbors(98, &[6]).unwrap();
    rows.put_reverse_locator(4, 0, 98).unwrap();
    rows.put_layer0_neighbors(99, &[4]).unwrap();
    rows.put_reverse_locator(4, 0, 99).unwrap();
    let simhash_cache = index.simhash_cache(3).unwrap();
    simhash_cache
        .set(&txn, 98, vector::SimHash::from_bits(98))
        .unwrap();
    simhash_cache
        .set(&txn, 99, vector::SimHash::from_bits(99))
        .unwrap();
    rows.put_upper_neighbors(3, 97, &[4]).unwrap();
    rows.put_reverse_locator(4, 3, 97).unwrap();
    let (canonical, _) = index
        .resolve_required_canonical_vector_key_counted(
            &measured,
            4,
            "removing the canonical payload before residue cleanup",
        )
        .await
        .unwrap();
    rows.delete_canonical_vector(&canonical).unwrap();
    index.stage_delete(&measured, 4).await.unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let rows = VectorRows::new(&measured, index.row_keyspace());
    assert!(index.get_item(&measured, 4).await.unwrap().is_none());
    assert!(rows
        .reverse_sources_for_target(4)
        .await
        .unwrap()
        .sources_by_layer()
        .is_empty());
    assert!(!rows.layer0_neighbors(99).await.unwrap().contains(&4));
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let entry = index
        .get_metadata(&measured)
        .await
        .unwrap()
        .unwrap()
        .entry_point
        .unwrap();
    index.stage_delete(&measured, entry).await.unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let metadata = index.get_metadata(&txn).await.unwrap().unwrap();
    assert_ne!(metadata.entry_point, Some(entry));
    assert!(index.get_item(&txn, entry).await.unwrap().is_none());
    txn.rollback();

    for _ in 0..8 {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
        let Some(entry) = metadata.entry_point else {
            txn.rollback();
            break;
        };
        index.stage_delete(&measured, entry).await.unwrap();
        txn.commit().await.unwrap();
    }
    let metadata = index.get_metadata(db).await.unwrap().unwrap();
    assert_eq!(metadata.entry_point, None);
    assert_eq!(metadata.max_layer, 0);
}

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

async fn session_test_db(name: &str) -> Arc<Db> {
    Arc::new(Db::open(name, Arc::new(InMemory::new())).await.unwrap())
}

fn run_build_session_identity_contract<D: Distance>() {
    use crate::encoding::v2::keys::scope::{DataScope, TenantId};

    let first = session_identity(DataScope::Tenant(TenantId::from_u128(1)), 31);
    let second = session_identity(DataScope::Tenant(TenantId::from_u128(2)), 31);
    let mut session = VectorBuildSession::<D>::new(NonZeroU64::new(1024).unwrap());

    let mut first_cache = session.take_cache(&first, 8, 4).unwrap();
    first_cache.put_item(0, 9, None, 0);
    first_cache.put_simhash(9, None);
    first_cache.install_loaded_neighbor(
        MutationOpCache::<D>::node_row_id(0, 9),
        NeighborRowValue::KnownAbsent,
    );
    session.restore_cache(first.clone(), first_cache);

    let mut second_cache = session.take_cache(&second, 8, 4).unwrap();
    assert!(second_cache.item(0, 9).is_none());
    assert_eq!(second_cache.simhash(9), None);
    let missing_neighbor = MutationOpCache::<D>::node_row_id(0, 9);
    assert!(!second_cache.contains_neighbor(missing_neighbor));
    assert!(second_cache.touched_neighbor(missing_neighbor).is_none());
    second_cache.put_item(0, 9, None, 0);
    session.restore_cache(second, second_cache);

    let mut first_cache = session.take_cache(&first, 8, 4).unwrap();
    assert!(matches!(first_cache.item(0, 9), Some(None)));
    assert!(matches!(first_cache.simhash(9), Some(None)));
    session.restore_cache(first.clone(), first_cache);
    assert_eq!(session.item_count(), 2);
    let stats = session.stats();
    assert_eq!(stats.item_hits(), 1);
    assert!(stats.item_misses() >= 1);
    assert!(stats.neighbor_misses() >= 1);
    assert_eq!(stats.simhash_hits(), 1);
    assert!(stats.simhash_misses() >= 1);
    assert!(session.take_cache(&first, 0, 4).is_err());
    assert!(session.take_cache(&first, 7, 4).is_err());
}

async fn run_build_session_limit_contract<D: Distance>() {
    use crate::encoding::v2::keys::scope::DataScope;

    let identity = session_identity(DataScope::LegacyUnscoped, 41);
    let mut session =
        VectorBuildSession::<D>::with_test_limits(NonZeroU64::new(1).unwrap(), 1, 1, 1);
    let mut cache = session.take_cache(&identity, 8, 4).unwrap();
    cache.put_item(0, 1, None, 8);
    cache.put_item(0, 2, None, 8);
    cache.put_simhash(1, None);
    cache.put_simhash(2, None);
    for node_id in [1, 2] {
        cache.install_loaded_neighbor(
            MutationOpCache::<D>::node_row_id(0, node_id),
            NeighborRowValue::KnownAbsent,
        );
    }
    session.restore_cache(identity, cache);

    let db = session_test_db(&format!(
        "production-vector-build-session-limits-{}",
        D::name()
    ))
    .await;
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

    let dirty_identity = session_identity(DataScope::LegacyUnscoped, 42);
    let mut dirty_session =
        VectorBuildSession::<D>::with_test_limits(NonZeroU64::new(4096).unwrap(), 8, 1, 8);
    let mut dirty_cache = dirty_session.take_cache(&dirty_identity, 8, 4).unwrap();
    let dirty_row = MutationOpCache::<D>::node_row_id(0, 10);
    dirty_cache.install_loaded_neighbor(dirty_row, NeighborRowValue::KnownAbsent);
    dirty_cache
        .stage_loaded_neighbor(dirty_row, neighbors(10, vec![11]))
        .unwrap();
    dirty_cache.install_loaded_neighbor(
        MutationOpCache::<D>::node_row_id(0, 12),
        NeighborRowValue::KnownAbsent,
    );
    dirty_session.restore_cache(dirty_identity, dirty_cache);
    dirty_session.enforce_limits(&measured).unwrap();
    assert_eq!(dirty_session.neighbor_count(), 1);
    assert_eq!(dirty_session.stats().dirty_neighbor_flushes(), 1);
}

async fn run_build_session_flush_recovery_contract<D: Distance>() {
    use crate::encoding::v2::keys::scope::DataScope;

    let identity = session_identity(DataScope::LegacyUnscoped, 51);
    let row = MutationOpCache::<D>::node_row_id(0, 1);
    let mut session = VectorBuildSession::<D>::new(NonZeroU64::new(1024).unwrap());
    let mut cache = session.take_cache(&identity, 8, 4).unwrap();
    cache.install_loaded_neighbor(row, NeighborRowValue::KnownAbsent);
    cache
        .stage_loaded_neighbor(row, neighbors(1, vec![2]))
        .unwrap();
    session.restore_cache(identity.clone(), cache);

    let db = session_test_db(&format!(
        "production-vector-build-session-flush-failure-{}",
        D::name()
    ))
    .await;
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

async fn run_build_session_flush_edge_contract<D: Distance>() {
    use crate::encoding::v2::keys::scope::DataScope;

    let identity = session_identity(DataScope::LegacyUnscoped, 52);
    let db = session_test_db(&format!(
        "production-vector-build-session-flush-edges-{}",
        D::name()
    ))
    .await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    let mut cache = MutationOpCache::<D>::with_degree_limits(8, 4).unwrap();

    let missing = MutationOpCache::<D>::node_row_id(0, 1);
    flush_build_session_neighbor(&measured, &identity, &mut cache, missing).unwrap();

    let clean = MutationOpCache::<D>::node_row_id(0, 2);
    cache.install_loaded_neighbor(clean, neighbors(2, vec![3]));
    flush_build_session_neighbor(&measured, &identity, &mut cache, clean).unwrap();

    let deleted = MutationOpCache::<D>::node_row_id(0, 4);
    cache.install_loaded_neighbor(deleted, neighbors(4, vec![5]));
    cache
        .stage_loaded_neighbor(deleted, NeighborRowValue::KnownAbsent)
        .unwrap();
    assert!(flush_build_session_neighbor(&measured, &identity, &mut cache, deleted).is_err());

    let upper = MutationOpCache::<D>::node_row_id(1, 6);
    cache.install_loaded_neighbor(upper, neighbors(6, vec![7, 8]));
    cache
        .stage_loaded_neighbor(upper, neighbors(6, vec![8, 9]))
        .unwrap();
    flush_build_session_neighbor(&measured, &identity, &mut cache, upper).unwrap();
    assert!(!cache.neighbor(upper).unwrap().is_dirty());
}

async fn run_build_session_reuse_contract() {
    use crate::encoding::v2::keys::scope::DataScope;

    let identity = session_identity(DataScope::LegacyUnscoped, 61);
    let handle =
        crate::search::vector::ValidatedVectorGenerationHandle::create_current::<Cosine>(identity)
            .unwrap();
    let index = VectorIndex::<Cosine>::from_generation(&handle);
    let db = session_test_db("production-vector-build-session-traversal-reuse").await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    let mut session = VectorBuildSession::new(NonZeroU64::new(64 * 1024).unwrap());
    assert!(matches!(
        index
            .insert_with_build_session(
                &measured,
                1,
                &[1.0, 0.0],
                VectorInsertContract::Upsert,
                Some(0),
                &mut session,
            )
            .await,
        Err(HelixDbError::IndexNotFound(_))
    ));
    assert!(matches!(
        index
            .stage_delete_with_build_session(&measured, 1, &mut session)
            .await,
        Err(HelixDbError::IndexNotFound(_))
    ));
    index
        .stage_create(
            &measured,
            VectorIndexConfig::new(handle.physical_name(), "embedding", 2),
        )
        .await
        .unwrap();

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
    index
        .stage_delete_with_build_session(&measured, 2, &mut session)
        .await
        .unwrap();
    session.flush_all(&measured).unwrap();

    let stats = session.stats();
    assert!(stats.item_hits() > 0);
    assert!(stats.neighbor_hits() > 0);
    assert!(stats.simhash_hits() > 0);
}

async fn run_unbound_metric_rejection_contract() {
    let db = session_test_db("production-vector-unbound-mutation").await;
    let index_name = "production-vector-unbound-mutation-index";
    let bound = VectorIndex::<Cosine>::new(index_name);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    bound
        .create(
            &transaction,
            VectorIndexConfig::new(index_name, "embedding", 2),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let unbound = VectorIndex::<UnboundMutationDistance>::new(index_name);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    assert!(matches!(
        unbound
            .insert_with_measured_transaction(
                &measured,
                1,
                &[1.0, 0.0],
                VectorInsertContract::Upsert,
                Some(0),
            )
            .await,
        Err(HelixDbError::Config(_))
    ));
    let mut session = VectorBuildSession::new(NonZeroU64::new(1024).unwrap());
    assert!(matches!(
        unbound
            .insert_with_build_session(
                &measured,
                1,
                &[1.0, 0.0],
                VectorInsertContract::Upsert,
                Some(0),
                &mut session,
            )
            .await,
        Err(HelixDbError::Config(_))
    ));
    assert_eq!(
        measured.measurement().unwrap(),
        VectorWriteMeasurement::zero()
    );
    transaction.rollback();
}

/// Exercises every constructible cache, repair, and typed graph-write transition.
pub(crate) async fn run() {
    run_value_contracts();
    run_cache_contracts::<Cosine>();
    run_cache_contracts::<Euclidean>();
    run_cache_contracts::<Manhattan>();
    run_build_session_identity_contract::<Cosine>();
    run_build_session_identity_contract::<Euclidean>();
    run_build_session_identity_contract::<Manhattan>();
    run_build_session_limit_contract::<Cosine>().await;
    run_build_session_limit_contract::<Euclidean>().await;
    run_build_session_limit_contract::<Manhattan>().await;
    run_build_session_flush_recovery_contract::<Cosine>().await;
    run_build_session_flush_recovery_contract::<Euclidean>().await;
    run_build_session_flush_recovery_contract::<Manhattan>().await;
    run_build_session_flush_edge_contract::<Cosine>().await;
    run_build_session_reuse_contract().await;
    run_unbound_metric_rejection_contract().await;
    let db = Db::open(
        "production-vector-mutation-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    run_entry_repair_contracts(&db).await;
    run_neighbor_write_contracts(&db).await;
    run_graph_delete_contracts(&db).await;
}
