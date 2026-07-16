//! Production contracts for the vector-index facade.
//!
//! This feature-gated child module exercises descriptor-bound projection
//! identity, write-once dimension proof, DDL validation, current typed row
//! lookup, bounded operation-local caching, fail-closed corruption handling,
//! search diagnostics, and exhaustive drop. Fixtures use only the deployed
//! `encoding::v1` vector keys and values in isolated in-memory databases.

use std::collections::HashMap;
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::encoding::v1::keys::vectors::{VectorKey, VectorSimHashKey, VectorUpperNeighborsKey};
use crate::search::vector::distance::{self, Cosine};
use crate::search::vector::generation::{CURRENT_SIMHASH_ALGORITHM_VERSION, CURRENT_SIMHASH_SEED};
use crate::search::vector::read_fault_production_support::{FaultingRead, ReadFault};
use crate::search::vector::unaligned_vector::UnalignedVector;
use crate::search::vector::VectorReadView;
use crate::search::vector::{
    encode_item, Item, SimHashIdentity, SimHashMode, VectorConfigError, VectorDimensionError,
};

/// Distance type without reviewed durable semantics, used to prove DDL rejection.
#[derive(Debug, Clone)]
enum UnboundDistance {}

impl Distance for UnboundDistance {
    type Header = ();
    type VectorCodec = f32;

    fn name() -> &'static str {
        "production-unbound-index-distance"
    }

    fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {}

    fn distance(_p: &Item<Self>, _q: &Item<Self>) -> f32 {
        0.0
    }

    fn norm_no_header(_vector: &UnalignedVector<Self::VectorCodec>) -> f32 {
        0.0
    }
}

impl crate::search::vector::distance::sealed::Sealed for UnboundDistance {}

/// Verifies handle identity, managed projection binding, and dimension proof.
fn run_handle_contracts() {
    let index = VectorIndex::<Cosine>::new("production-vector-index-handle");
    assert_eq!(index.name(), "production-vector-index-handle");
    assert_eq!(index.id(), index.row_keyspace().index_id());
    assert_eq!(index.row_keyspace().physical_name(), index.name());

    assert!(matches!(
        index.remember_dimension(0),
        Err(HelixDbError::InvalidVectorConfig(
            VectorConfigError::Dimension(VectorDimensionError::ZeroDimension)
        ))
    ));
    assert_eq!(index.remember_dimension(3).unwrap().get(), 3);
    assert_eq!(index.remember_dimension(3).unwrap().get(), 3);
    assert!(matches!(
        index.remember_dimension(4),
        Err(HelixDbError::InvariantViolation(_))
    ));

    let legacy_cache = index.simhash_cache(3).unwrap();
    assert!(legacy_cache
        .simhasher()
        .hash_from_slice(&[1.0, 0.0, 0.0])
        .is_ok());
    let reused_cache = index.simhash_cache(3).unwrap();
    assert!(std::ptr::eq(legacy_cache, reused_cache));
    assert!(std::ptr::eq(
        legacy_cache.simhasher(),
        reused_cache.simhasher()
    ));
    assert!(index.simhash_cache(usize::MAX).is_err());

    let identity = SimHashIdentity::new(
        NonZeroUsize::new(3).unwrap(),
        CURRENT_SIMHASH_SEED,
        NonZeroU16::new(CURRENT_SIMHASH_ALGORITHM_VERSION).unwrap(),
    );
    let managed = VectorIndex::<Cosine>::new("production-vector-index-managed")
        .with_simhash_identity(identity);
    assert!(managed.simhash_cache(3).is_ok());
    assert!(matches!(
        managed.simhash_cache(4),
        Err(HelixDbError::InvariantViolation(_))
    ));

    let fetch = VectorFetchReadStats {
        simhash_reads: usize::MAX,
        vector_reads: 1,
        simhash_multi_get_calls: 2,
        simhash_fetch_ns: 3,
    };
    assert_eq!(fetch.total_reads(), usize::MAX);
    assert_eq!(fetch.simhash_multi_get_calls, 2);
    assert_eq!(fetch.simhash_fetch_ns, 3);
}

/// Verifies creation validation and exact current metadata accounting.
async fn run_ddl_contracts(db: &Db) {
    let missing = VectorIndex::<Cosine>::new("production-vector-index-missing");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        missing.expected_dimension(&txn).await,
        Err(HelixDbError::IndexNotFound(_))
    ));
    assert!(missing.measure_metadata_input(&txn).await.unwrap() > 0);
    txn.rollback();

    let invalid = VectorIndex::<Cosine>::new("production-vector-index-invalid");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        invalid
            .create(&txn, VectorIndexConfig::new(invalid.name(), "embedding", 0),)
            .await,
        Err(HelixDbError::InvalidVectorConfig(_))
    ));
    txn.rollback();

    let unsupported = VectorIndex::<UnboundDistance>::new("production-vector-index-unbound");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        unsupported
            .create(
                &txn,
                VectorIndexConfig::new(unsupported.name(), "embedding", 3),
            )
            .await,
        Err(HelixDbError::Config(message))
            if message.contains("no stable durable semantic identity")
    ));
    txn.rollback();

    let mismatch = VectorIndex::<Cosine>::new("production-vector-index-name");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        mismatch
            .create(
                &txn,
                VectorIndexConfig::new("different-vector-index-name", "embedding", 3),
            )
            .await,
        Err(HelixDbError::Config(message)) if message.contains("name mismatch")
    ));
    txn.rollback();
}

/// Verifies typed item reads, cache behavior, corruption rejection, and cleanup.
async fn run_row_contracts(db: &Db) {
    let index = VectorIndex::<Cosine>::new("production-vector-index-rows");
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
    assert!(matches!(
        index
            .create(&txn, VectorIndexConfig::new(index.name(), "embedding", 3),)
            .await,
        Err(HelixDbError::IndexAlreadyExists(_))
    ));
    assert_eq!(
        index
            .get_metadata(&txn)
            .await
            .unwrap()
            .unwrap()
            .config
            .dimension,
        3
    );
    assert!(index.measure_metadata_input(&txn).await.unwrap() > 0);
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(index.get_item(&txn, 99).await.unwrap().is_none());
    let (missing, missing_reads) = index
        .get_canonical_vector_bytes_counted::<true>(&txn, 99)
        .await
        .unwrap();
    assert!(missing.is_none());
    assert_eq!(missing_reads.vector_reads, 0);
    assert!(missing_reads.simhash_reads > 0);
    txn.rollback();

    for (node_id, vector) in [
        (1, [1.0, 0.0, 0.0]),
        (2, [0.9, 0.1, 0.0]),
        (3, [0.0, 1.0, 0.0]),
    ] {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.insert(&txn, node_id, &vector).await.unwrap();
        txn.commit().await.unwrap();
    }

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let uncached_handle = VectorIndex::<Cosine>::new(index.name());
    let point = FaultingRead::new(&txn, ReadFault::Point);
    assert!(uncached_handle.expected_dimension(&point).await.is_err());
    assert!(index
        .search_with_stats(&point, &[1.0, 0.0, 0.0], &SearchParams::new(1).unwrap(),)
        .await
        .is_err());

    let managed_store = Arc::new(VectorMemoryStore::new(
        index.row_keyspace().scope(),
        index.id(),
        1,
    ));
    let simhash = index
        .simhash_cache(3)
        .unwrap()
        .get(&txn, 1)
        .await
        .unwrap()
        .unwrap();
    managed_store.insert_simhash(1, simhash);
    let managed_read = VectorIndex::<Cosine>::new(index.name())
        .with_managed_read_cache(
            managed_store,
            Arc::new(VectorMemoryPendingDirtyRows::default()),
        )
        .unwrap();
    assert!(managed_read
        .get_canonical_vector_bytes_counted::<true>(&point, 1)
        .await
        .is_err());

    let item = index.get_item(&txn, 1).await.unwrap().unwrap();
    assert_eq!(item.vector.to_vec(), vec![1.0, 0.0, 0.0]);
    let (bytes, reads) = index
        .get_canonical_vector_bytes_counted::<true>(&txn, 1)
        .await
        .unwrap();
    assert!(bytes.is_some());
    assert_eq!(reads.vector_reads, 1);
    assert!(reads.total_reads() >= 2);

    let mut cache = MutationOpCache::<Cosine>::default();
    assert!(index
        .get_items_for_layer_cached_batch(&txn, 0, &[], &mut cache)
        .await
        .unwrap()
        .is_empty());
    let mut absent_only = MutationOpCache::<Cosine>::default();
    assert!(index
        .get_items_for_layer_cached_batch(&txn, 0, &[99], &mut absent_only)
        .await
        .unwrap()
        .is_empty());
    assert!(absent_only.items.contains_key(&(0, 99)));
    let loaded = index
        .get_items_for_layer_cached_batch(&txn, 0, &[1, 1, 99], &mut cache)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert!(loaded.contains_key(&1));
    assert!(index
        .get_item_for_layer_cached(&txn, 0, 1, &mut cache)
        .await
        .unwrap()
        .is_some());
    assert!(index
        .get_item_for_layer_cached(&txn, 0, 99, &mut cache)
        .await
        .unwrap()
        .is_none());
    for node_id in 10_000_u64..10_000_u64 + u64::try_from(OP_LOCAL_ITEM_CACHE_LIMIT).unwrap() {
        cache.items.insert((0, node_id), None);
    }
    assert!(index
        .get_item_for_layer_cached(&txn, 0, 100_000, &mut cache)
        .await
        .unwrap()
        .is_none());
    assert!(cache.items.len() < OP_LOCAL_ITEM_CACHE_LIMIT);

    let mut cached_batch = MutationOpCache::<Cosine>::default();
    cached_batch.items.insert(
        (0, 1),
        Some(Arc::new(Item::<Cosine>::new(vec![1.0, 0.0, 0.0]))),
    );
    cached_batch.items.insert((0, 99), None);
    let loaded = index
        .get_items_for_layer_cached_batch(&txn, 0, &[1, 99], &mut cached_batch)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert!(loaded.contains_key(&1));

    for node_id in 20_000_u64..20_000_u64 + u64::try_from(OP_LOCAL_ITEM_CACHE_LIMIT).unwrap() {
        cached_batch.items.insert((0, node_id), None);
    }
    assert!(index
        .get_items_for_layer_cached_batch(&txn, 0, &[1], &mut cached_batch)
        .await
        .unwrap()
        .contains_key(&1));
    assert!(cached_batch.items.len() < OP_LOCAL_ITEM_CACHE_LIMIT);

    let (results, stats) = index
        .search_with_stats(
            &txn,
            &[1.0, 0.0, 0.0],
            &SearchParams::new(3).unwrap().with_ef(8).unwrap(),
        )
        .await
        .unwrap();
    assert!(!results.is_empty());
    assert!(stats.expansion_steps > 0);
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    VectorWriteRows::new(&measured, index.row_keyspace())
        .put_upper_vector(1, encode_item(&Item::<Cosine>::new(vec![0.0, 0.0, 1.0])))
        .unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let upper = index.get_item_for_layer(&txn, 1, 1).await.unwrap().unwrap();
    assert_eq!(upper.vector.to_vec(), vec![0.0, 0.0, 1.0]);
    let mut upper_batch = MutationOpCache::<Cosine>::default();
    let loaded = index
        .get_items_for_layer_cached_batch(&txn, 1, &[1, 2, 99], &mut upper_batch)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 2);
    assert!(loaded.contains_key(&1));
    assert!(loaded.contains_key(&2));
    txn.rollback();

    let snapshot = db.snapshot().await.unwrap();
    let read_view = VectorReadView::snapshot(snapshot.as_ref());
    assert_eq!(
        index
            .search_layer_greedy(&read_view, &Item::<Cosine>::new(vec![0.0, 0.0, 1.0]), 1, 1,)
            .await
            .unwrap(),
        1
    );
    assert!(!index
        .search_with_stats(
            &read_view,
            &[1.0, 0.0, 0.0],
            &SearchParams::new(3).unwrap().with_ef(8).unwrap(),
        )
        .await
        .unwrap()
        .0
        .is_empty());

    // A corrupt typed upper-neighbor row must fail at the greedy traversal's
    // storage boundary after the current upper item has been resolved.
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    measured
        .put(
            index
                .row_keyspace()
                .key(VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
                    index.id(),
                    1,
                    1,
                ))),
            Bytes::from_static(b"corrupt-upper-neighbors"),
        )
        .unwrap();
    assert!(index
        .search_layer_greedy(&measured, &Item::<Cosine>::new(vec![0.0, 0.0, 1.0]), 1, 1,)
        .await
        .is_err());
    txn.rollback();

    let managed_store = Arc::new(VectorMemoryStore::new(
        index.row_keyspace().scope(),
        index.id(),
        1,
    ));
    managed_store.insert_upper_vector(88, Bytes::from_static(b"stale"));
    let managed = VectorIndex::<Cosine>::new(index.name())
        .with_managed_read_cache(
            Arc::clone(&managed_store),
            Arc::new(VectorMemoryPendingDirtyRows::default()),
        )
        .unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    managed
        .stage_known_fresh_at_layer(
            &measured,
            88,
            &[1.0, 0.0, 0.0],
            0,
            crate::search::vector::mutation::FreshVectorBuildProof::for_test(),
        )
        .await
        .unwrap();
    assert!(managed_store.get_upper_vector(88).is_some());
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    rows.put_layer0_neighbors(77, &[]).unwrap();
    let mut missing_simhash = MutationOpCache::<Cosine>::default();
    assert!(index
        .get_items_for_layer_cached_batch(&txn, 0, &[77], &mut missing_simhash)
        .await
        .is_err());
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .simhash_cache(3)
        .unwrap()
        .set(&txn, 78, crate::search::vector::SimHash::from_bits(0x77))
        .unwrap();
    let mut missing_payload = MutationOpCache::<Cosine>::default();
    assert!(index
        .get_items_for_layer_cached_batch(&txn, 0, &[78], &mut missing_payload)
        .await
        .unwrap()
        .is_empty());
    assert!(missing_payload.items.contains_key(&(0, 78)));
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let simhash_cache = index.simhash_cache(3).unwrap();
    let saved_simhash = simhash_cache.get(&txn, 2).await.unwrap().unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    VectorWriteRows::new(&measured, index.row_keyspace())
        .delete_simhash(2)
        .unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        index
            .resolve_canonical_vector_key_counted::<true>(&txn, 2, "checking layer-zero residue",)
            .await,
        Err(HelixDbError::InvariantViolation(_))
    ));
    assert!(matches!(
        index
            .resolve_required_canonical_vector_key_counted(&txn, 2, "requiring canonical identity",)
            .await,
        Err(HelixDbError::InvariantViolation(_))
    ));
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    simhash_cache.set(&txn, 2, saved_simhash).unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    measured
        .put(
            index
                .row_keyspace()
                .key(VectorKey::SimHash(VectorSimHashKey::new(index.id(), 77))),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let mut local = HashMap::new();
    assert!(index
        .fill_simhash_cache_for_nodes_counted::<true>(
            &txn,
            &[77],
            &mut local,
            "reading corrupt row",
        )
        .await
        .is_err());
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let (canonical, _) = index
        .resolve_required_canonical_vector_key_counted(&txn, 1, "locating dimension fixture")
        .await
        .unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    VectorWriteRows::new(&measured, index.row_keyspace())
        .put_canonical_vector(
            &canonical,
            encode_item(&Item::<Cosine>::new(vec![1.0, 0.0])),
        )
        .unwrap();
    assert!(matches!(
        index.get_item(&txn, 1).await,
        Err(HelixDbError::InvalidVectorItem(_))
    ));
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index.delete(&txn, 3).await.unwrap();
    index.drop(&txn).await.unwrap();
    txn.commit().await.unwrap();
    assert!(index.get_metadata(db).await.unwrap().is_none());
}

/// Runs the complete current-format facade lifecycle for another active metric.
async fn run_additional_metric_contract<D>(db: &Db, physical_name: &str)
where
    D: Distance + 'static,
{
    let index = VectorIndex::<D>::new(physical_name);
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
        (1, [1.0, 0.0, 0.0], 1),
        (2, [0.9, 0.1, 0.0], 0),
        (3, [0.0, 1.0, 0.0], 0),
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

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let (results, stats) = index
        .search_with_stats(
            &txn,
            &[1.0, 0.0, 0.0],
            &SearchParams::new(3)
                .unwrap()
                .with_ef(4)
                .unwrap()
                .with_simhash_mode(SimHashMode::Off)
                .with_pre_simhash_sampling_ratio(1.0)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert!(stats.distance_computations >= 3);
    txn.rollback();

    // Exercise greedy upper traversal through an explicit graph so coverage
    // does not depend on the production level selector used by public inserts.
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    rows.put_upper_vector(100, encode_item(&Item::<D>::new(vec![0.0, 1.0, 0.0])))
        .unwrap();
    rows.put_upper_vector(101, encode_item(&Item::<D>::new(vec![1.0, 0.0, 0.0])))
        .unwrap();
    rows.put_upper_neighbors(1, 100, &[99, 101]).unwrap();
    rows.put_upper_neighbors(1, 101, &[100]).unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(
        index
            .search_layer_greedy(&txn, &Item::<D>::new(vec![1.0, 0.0, 0.0]), 100, 1,)
            .await
            .unwrap(),
        101
    );
    txn.rollback();

    let snapshot = db.snapshot().await.unwrap();
    let read_view = VectorReadView::snapshot(snapshot.as_ref());
    assert_eq!(
        index
            .search_layer_greedy(&read_view, &Item::<D>::new(vec![1.0, 0.0, 0.0]), 100, 1,)
            .await
            .unwrap(),
        101
    );
    assert_eq!(
        index
            .search_with_stats(
                &read_view,
                &[1.0, 0.0, 0.0],
                &SearchParams::new(3)
                    .unwrap()
                    .with_ef(4)
                    .unwrap()
                    .with_simhash_mode(SimHashMode::Off)
                    .with_pre_simhash_sampling_ratio(1.0)
                    .unwrap(),
            )
            .await
            .unwrap()
            .0
            .len(),
        3
    );

    // Run the complete relink/prune path for every active metric. The fixed
    // rows make both forward diversity pruning and reciprocal overflow
    // pruning deterministic instead of relying on insertion history.
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    rows.put_layer0_neighbors(2, &[3]).unwrap();
    rows.put_layer0_neighbors(1, &[3]).unwrap();
    rows.put_layer0_neighbors(3, &[1]).unwrap();
    let mut relink_cache = MutationOpCache::<D>::with_degree_limits(4, 2).unwrap();
    index
        .relink_neighbor(
            &measured,
            0,
            2,
            &std::collections::HashSet::from([1]),
            1,
            &mut relink_cache,
        )
        .await
        .unwrap();
    index
        .flush_mutation_cache(&measured, &mut relink_cache)
        .await
        .unwrap();
    txn.rollback();

    // Growing beyond the current maximum covers metadata promotion, empty
    // upper-row staging, and upper-neighbor flush for every active metric.
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    index
        .insert_with_measured_transaction(
            &measured,
            4,
            &[0.0, 0.0, 1.0],
            VectorInsertContract::Upsert,
            Some(2),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    index
        .stage_known_fresh_at_layer(
            &measured,
            5,
            &[0.1, 0.1, 0.8],
            1,
            crate::search::vector::mutation::FreshVectorBuildProof::for_test(),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    index
        .stage_upsert_at_layer(&measured, 2, &[0.8, 0.2, 0.0], 0)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    index.stage_delete(&measured, 1).await.unwrap();
    txn.commit().await.unwrap();
    assert!(index.get_item(db, 1).await.unwrap().is_none());

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index.drop(&txn).await.unwrap();
    txn.commit().await.unwrap();
    assert!(index.get_metadata(db).await.unwrap().is_none());
}

/// Exercises vector facade identity, DDL, typed row, and corruption contracts.
pub(crate) async fn run() {
    run_handle_contracts();
    let db = Db::open(
        "production-vector-index-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    run_ddl_contracts(&db).await;
    run_row_contracts(&db).await;
    run_additional_metric_contract::<distance::Euclidean>(&db, "production-vector-index-euclidean")
        .await;
    run_additional_metric_contract::<distance::Manhattan>(&db, "production-vector-index-manhattan")
        .await;
}
