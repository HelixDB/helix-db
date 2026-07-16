//! Production contracts for the resident vector-memory boundary.
//!
//! This feature-gated child module verifies that cache capability variants,
//! transaction-local and commit-window fences, managed lookup, bounded
//! hydration, shutdown, and corruption handling use the real production key
//! and value codecs. It writes only isolated in-memory databases and does not
//! add or alter any persisted vector format.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use slatedb::config::DbReaderOptions;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::{Db, DbReader, IsolationLevel};
use tokio::sync::watch;

use super::*;
use crate::encoding::keys::tenant::TenantId;
use crate::encoding::v1::keys::vectors::{
    VectorMemoryPrefixKey, VectorSimHashKey, VectorUpperNeighborsKey, VectorUpperVectorKey,
};
use crate::encoding::v1::values::vectors::{
    neighbors::encode_upper_neighbors, simhash::encode_simhash,
};
use crate::search::vector::read_fault_production_support::{FaultingRead, ReadFault};
use crate::search::vector::storage::VectorRowKeyspace;

/// Opens an isolated in-memory SlateDB for cache and hydration contracts.
async fn test_db(name: &str) -> Arc<Db> {
    Arc::new(
        Db::open(name, Arc::new(InMemory::new()))
            .await
            .expect("vector-memory contract database opens"),
    )
}

/// Writes one current-format row through a keyspace-bound production key.
fn put_row(
    txn: &slatedb::DbTransaction,
    keyspace: &VectorRowKeyspace,
    key: VectorKey,
    value: Bytes,
) {
    txn.put(keyspace.key(key), value)
        .expect("vector-memory contract row stages");
}

/// Verifies every valid cache capability and dirty-row ownership transition.
async fn run_capability_and_fence_contracts() {
    let store = Arc::new(VectorMemoryStore::new(DataScope::LegacyUnscoped, 42, 10));
    assert_eq!(store.scope(), DataScope::LegacyUnscoped);
    assert_eq!(store.index_id(), 42);
    assert_eq!(store.visible_seq(), 10);
    assert!(store.is_visible_to_snapshot(10));
    assert!(!store.is_visible_to_snapshot(9));
    assert!(store.is_usable_for_writer_snapshot(10));
    assert!(store.is_usable_for_writer_snapshot(11));
    assert!(!store.is_usable_for_writer_snapshot(9));

    let uncached = VectorMemoryAccess::uncached();
    assert!(uncached.store().is_none());
    uncached.mark_node_dirty(1);
    uncached.mark_upper_neighbors_dirty(1, 1);
    assert!(!uncached.is_node_dirty(1));

    let dirty = Arc::new(VectorMemoryDirtyRows::default());
    assert!(dirty.is_empty());
    let local = VectorMemoryAccess::write_tracking(Arc::clone(&dirty));
    local.mark_node_dirty(7);
    local.mark_upper_neighbors_dirty(2, 9);
    assert!(local.is_node_dirty(7));
    assert!(local.is_upper_neighbors_dirty(4, 7));
    assert!(local.is_upper_neighbors_dirty(2, 9));
    assert_eq!(dirty.dirty_nodes(), vec![7]);
    assert_eq!(dirty.dirty_upper_neighbors(), vec![(2, 9)]);

    let tracking_dirty = Arc::new(VectorMemoryDirtyRows::default());
    let tracking = VectorMemoryAccess::write_tracking(Arc::clone(&tracking_dirty));
    tracking.mark_node_dirty(11);
    tracking.mark_upper_neighbors_dirty(3, 12);
    assert!(tracking.store().is_none());
    assert!(tracking.is_node_dirty(11));
    assert!(tracking.is_upper_neighbors_dirty(3, 12));

    let pending = Arc::new(VectorMemoryPendingDirtyRows::default());
    assert_eq!(pending.generation(), 0);
    pending.bump_generation();
    assert_eq!(pending.generation(), 1);
    drop(pending.lock_publish().await);

    let first = pending.acquire(&dirty);
    let second = pending.acquire(&dirty);
    let pending_access =
        VectorMemoryAccess::read_snapshot(Arc::clone(&store), Arc::clone(&pending));
    assert!(pending_access.store().is_some());
    assert!(pending_access.is_node_dirty(7));
    assert!(pending_access.is_upper_neighbors_dirty(2, 9));
    drop(first);
    assert!(pending.is_node_dirty(7));
    drop(second);
    assert!(!pending.is_node_dirty(7));
    VectorMemoryPendingDirtyRows::decrement(&pending.dirty_nodes, 999);

    store.insert_simhash(7, SimHash::from_bits(7));
    store.insert_upper_vector(7, Bytes::from_static(b"vector"));
    store
        .insert_upper_neighbors(2, 7, &[8, 9])
        .expect("upper neighbors encode");
    store.insert_upper_neighbors_bytes(3, 7, Bytes::from_static(b"raw"));
    store.insert_upper_neighbors_bytes(4, 7, Bytes::from_static(b"raw-occupied"));
    assert_eq!(store.get_simhash(7), Some(SimHash::from_bits(7)));
    assert_eq!(
        store.get_upper_vector(7).unwrap(),
        Bytes::from_static(b"vector")
    );
    assert!(store.get_upper_neighbors_bytes(2, 7).is_some());
    store.remove_upper_neighbors(9, 999);
    store.remove_upper_neighbors(2, 7);
    assert!(store.get_upper_neighbors_bytes(2, 7).is_none());
    store.remove_upper_neighbors(3, 7);
    assert!(store.get_upper_neighbors_bytes(4, 7).is_some());

    store
        .insert_upper_neighbors(5, 9, &[10])
        .expect("upper-neighbor-only eviction row encodes");
    assert!(store.get_upper_neighbors_bytes(5, 9).is_some());

    store.insert_simhash(8, SimHash::from_bits(8));
    store.insert_upper_vector(8, Bytes::from_static(b"other"));
    let first_all = pending.acquire_all();
    let second_all = pending.acquire_all();
    assert!(pending.is_all_dirty());
    drop(first_all);
    assert!(pending.is_all_dirty());
    drop(second_all);
    assert!(!pending.is_all_dirty());
}

/// Verifies managed cache hits, misses, dirty bypass, and corrupt SimHash rows.
async fn run_managed_lookup_contracts() {
    let db = test_db("production_vector_memory_managed_lookup").await;
    let keyspace = VectorRowKeyspace::new(
        "vector:memory:resident-snapshot".to_string(),
        DataScope::Tenant(TenantId::from_u128(7)),
    );
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    put_row(
        &txn,
        &keyspace,
        VectorKey::UpperVector(VectorUpperVectorKey::new(keyspace.index_id(), 1)),
        Bytes::from_static(b"durable-vector"),
    );
    put_row(
        &txn,
        &keyspace,
        VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(keyspace.index_id(), 2, 1)),
        encode_upper_neighbors(&[2, 3]).unwrap(),
    );
    put_row(
        &txn,
        &keyspace,
        VectorKey::SimHash(VectorSimHashKey::new(keyspace.index_id(), 1)),
        Bytes::copy_from_slice(&encode_simhash(0x1234)),
    );
    put_row(
        &txn,
        &keyspace,
        VectorKey::SimHash(VectorSimHashKey::new(keyspace.index_id(), 9)),
        Bytes::from_static(b"corrupt"),
    );
    txn.commit().await.unwrap();

    let store = Arc::new(VectorMemoryStore::new(
        keyspace.scope(),
        keyspace.index_id(),
        1,
    ));
    store.insert_upper_vector(1, Bytes::from_static(b"stale"));
    let pending = Arc::new(VectorMemoryPendingDirtyRows::default());
    let dirty = VectorMemoryDirtyRows::default();
    dirty.mark_node_dirty(1);
    let _guard = pending.acquire(&dirty);
    let bypass = VectorMemoryAccess::read_snapshot(Arc::clone(&store), Arc::clone(&pending));
    assert_eq!(
        bypass
            .read_upper_vector_row(db.as_ref(), &keyspace, 1)
            .await
            .unwrap(),
        Some(Bytes::from_static(b"durable-vector"))
    );
    assert_eq!(
        store.get_upper_vector(1),
        Some(Bytes::from_static(b"stale"))
    );

    let managed = VectorMemoryAccess::read_snapshot(
        Arc::clone(&store),
        Arc::new(VectorMemoryPendingDirtyRows::default()),
    );
    store.remove_upper_vector(1);
    assert_eq!(
        managed
            .read_upper_vector_rows(db.as_ref(), &keyspace, &[1, 2])
            .await
            .unwrap(),
        vec![Some(Bytes::from_static(b"durable-vector")), None]
    );
    assert_eq!(
        managed
            .read_upper_vector_row(db.as_ref(), &keyspace, 1)
            .await
            .unwrap(),
        Some(Bytes::from_static(b"durable-vector"))
    );
    assert_eq!(
        managed
            .read_upper_vector_rows(db.as_ref(), &keyspace, &[1])
            .await
            .unwrap(),
        vec![Some(Bytes::from_static(b"durable-vector"))]
    );
    assert_eq!(
        managed
            .read_upper_vector_row(db.as_ref(), &keyspace, 2)
            .await
            .unwrap(),
        None
    );
    assert!(store.get_upper_vector(2).is_none());
    assert_eq!(
        managed
            .read_upper_neighbors(db.as_ref(), &keyspace, 2, 1)
            .await
            .unwrap(),
        Some(vec![2, 3])
    );
    assert_eq!(
        managed
            .read_upper_neighbors(db.as_ref(), &keyspace, 2, 2)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        managed
            .read_upper_neighbors(db.as_ref(), &keyspace, 2, 1)
            .await
            .unwrap(),
        Some(vec![2, 3])
    );
    store.insert_upper_neighbors_bytes(4, 4, Bytes::from_static(b"corrupt-cache"));
    assert!(managed
        .read_upper_neighbors(db.as_ref(), &keyspace, 4, 4)
        .await
        .is_err());
    store
        .insert_upper_neighbors(5, 5, &[6])
        .expect("stale missing-neighbor row encodes");
    assert_eq!(
        managed
            .read_upper_neighbors(db.as_ref(), &keyspace, 5, 5)
            .await
            .unwrap(),
        Some(vec![6])
    );
    store.remove_upper_neighbors(5, 5);
    assert_eq!(
        managed
            .read_upper_neighbors(db.as_ref(), &keyspace, 5, 5)
            .await
            .unwrap(),
        None
    );

    let mut local_cache = HashMap::new();
    let stats = managed
        .fill_simhash_cache::<true, _>(
            db.as_ref(),
            &keyspace,
            &[1, 2],
            &mut local_cache,
            "contract",
        )
        .await
        .unwrap();
    assert_eq!(stats.reads, 2);
    assert_eq!(stats.multi_get_calls, 1);
    assert!(stats.fetch_ns > 0);
    assert_eq!(local_cache.get(&1), Some(&Some(SimHash::from_bits(0x1234))));
    assert_eq!(local_cache.get(&2), Some(&None));
    let cached = managed
        .fill_simhash_cache::<true, _>(
            db.as_ref(),
            &keyspace,
            &[1, 2],
            &mut local_cache,
            "contract",
        )
        .await
        .unwrap();
    assert_eq!(cached.reads, 0);
    assert_eq!(cached.multi_get_calls, 0);

    store.insert_simhash(2, SimHash::from_bits(99));
    let resident_hit = managed
        .fill_simhash_cache::<true, _>(
            db.as_ref(),
            &keyspace,
            &[2],
            &mut HashMap::new(),
            "contract",
        )
        .await
        .unwrap();
    assert_eq!(resident_hit.reads, 0);
    store.remove_simhash(2);
    let storage_fallback = managed
        .fill_simhash_cache::<true, _>(
            db.as_ref(),
            &keyspace,
            &[2],
            &mut HashMap::new(),
            "contract",
        )
        .await
        .unwrap();
    assert_eq!(storage_fallback.reads, 1);
    assert!(store.get_simhash(2).is_none());

    assert!(managed
        .fill_simhash_cache::<true, _>(
            db.as_ref(),
            &keyspace,
            &[9],
            &mut HashMap::new(),
            "contract",
        )
        .await
        .is_err());

    let point = FaultingRead::new(db.as_ref(), ReadFault::Point);
    assert!(managed
        .read_upper_neighbors(&point, &keyspace, 2, 777)
        .await
        .is_err());

    let multi_get = FaultingRead::new(db.as_ref(), ReadFault::MultiGet);
    assert!(managed
        .read_upper_vector_rows(&multi_get, &keyspace, &[777])
        .await
        .is_err());
    assert!(managed
        .read_upper_vector_row(&multi_get, &keyspace, 778)
        .await
        .is_err());
    assert!(managed
        .fill_simhash_cache::<true, _>(
            &multi_get,
            &keyspace,
            &[777],
            &mut HashMap::new(),
            "injected read failure",
        )
        .await
        .is_err());
}

/// Verifies strict, bounded, and shutdown hydration outcomes.
async fn run_hydration_contracts() {
    let db = test_db("production_vector_memory_hydration").await;
    let keyspace = VectorRowKeyspace::new(
        "vector:memory:hydration".to_string(),
        DataScope::LegacyUnscoped,
    );
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    put_row(
        &txn,
        &keyspace,
        VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(keyspace.index_id(), 2, 1)),
        Bytes::from_static(b"neighbors"),
    );
    put_row(
        &txn,
        &keyspace,
        VectorKey::SimHash(VectorSimHashKey::new(keyspace.index_id(), 1)),
        Bytes::copy_from_slice(&encode_simhash(0x55)),
    );
    put_row(
        &txn,
        &keyspace,
        VectorKey::UpperVector(VectorUpperVectorKey::new(keyspace.index_id(), 1)),
        Bytes::from_static(b"vector"),
    );
    txn.commit().await.unwrap();

    let descriptor_bound = VectorMemoryStore::new(keyspace.scope(), keyspace.index_id(), 1);
    let summary = descriptor_bound
        .load_descriptor_bound_with_budget(
            db.as_ref(),
            VectorMemoryAdmissionBudget::Unbounded,
            None,
        )
        .await
        .unwrap();
    assert_eq!(summary.loaded_entries, 3);
    assert_eq!(
        summary.completion,
        VectorMemoryStoreLoadCompletion::Complete
    );
    assert_eq!(descriptor_bound.estimated_bytes(), summary.estimated_bytes);
    assert!(descriptor_bound.get_upper_neighbors_bytes(2, 1).is_some());
    assert_eq!(
        descriptor_bound.get_simhash(1),
        Some(SimHash::from_bits(0x55))
    );
    assert_eq!(
        descriptor_bound.get_upper_vector(1),
        Some(Bytes::from_static(b"vector"))
    );

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    put_row(
        &txn,
        &keyspace,
        VectorKey::SimHash(VectorSimHashKey::new(keyspace.index_id(), 99)),
        Bytes::from_static(b"bad"),
    );
    txn.commit().await.unwrap();
    let strict = VectorMemoryStore::new(keyspace.scope(), keyspace.index_id(), 1);
    assert!(strict
        .load_descriptor_bound_with_budget(
            db.as_ref(),
            VectorMemoryAdmissionBudget::Unbounded,
            None,
        )
        .await
        .is_err());

    let bounded_db = test_db("production_vector_memory_bounded").await;
    let bounded_keyspace = VectorRowKeyspace::new(
        "vector:memory:bounded".to_string(),
        DataScope::LegacyUnscoped,
    );
    let first_key = bounded_keyspace.key(VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
        bounded_keyspace.index_id(),
        1,
        7,
    )));
    let txn = bounded_db.begin(IsolationLevel::Snapshot).await.unwrap();
    put_row(
        &txn,
        &bounded_keyspace,
        VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
            bounded_keyspace.index_id(),
            1,
            7,
        )),
        Bytes::from_static(b"first"),
    );
    put_row(
        &txn,
        &bounded_keyspace,
        VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
            bounded_keyspace.index_id(),
            1,
            8,
        )),
        Bytes::from_static(b"second"),
    );
    txn.commit().await.unwrap();
    let first_row_bytes = estimated_entry_bytes(first_key.len(), b"first".len()).unwrap();
    let bounded = VectorMemoryStore::new(bounded_keyspace.scope(), bounded_keyspace.index_id(), 1);
    let summary = bounded
        .load_descriptor_bound_with_budget(
            bounded_db.as_ref(),
            VectorMemoryAdmissionBudget::Bounded(first_row_bytes),
            None,
        )
        .await
        .unwrap();
    assert_eq!(summary.loaded_entries, 1);
    assert_eq!(
        summary.completion,
        VectorMemoryStoreLoadCompletion::BudgetExhausted
    );

    let snapshot = bounded_db.snapshot().await.unwrap();
    let snapshot_summary =
        VectorMemoryStore::new(bounded_keyspace.scope(), bounded_keyspace.index_id(), 1)
            .load_descriptor_bound_with_budget(
                snapshot.as_ref(),
                VectorMemoryAdmissionBudget::Unbounded,
                None,
            )
            .await
            .unwrap();
    assert_eq!(snapshot_summary.loaded_entries, 2);

    // Reader-mode hydration is a distinct production monomorphization used by
    // read-only HelixDB instances. Build its fixture inline so the reader sees
    // the same committed manifest without widening the general test helper.
    let reader_object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reader_path = "production-vector-memory-reader";
    let reader_writer = Db::open(reader_path, Arc::clone(&reader_object_store))
        .await
        .unwrap();
    let reader_keyspace = VectorRowKeyspace::new(
        "vector:memory:reader".to_string(),
        DataScope::LegacyUnscoped,
    );
    let txn = reader_writer.begin(IsolationLevel::Snapshot).await.unwrap();
    put_row(
        &txn,
        &reader_keyspace,
        VectorKey::SimHash(VectorSimHashKey::new(reader_keyspace.index_id(), 1)),
        Bytes::copy_from_slice(&encode_simhash(0x66)),
    );
    txn.commit().await.unwrap();
    reader_writer.close().await.unwrap();
    let reader = DbReader::open(
        reader_path,
        reader_object_store,
        None,
        DbReaderOptions::default(),
    )
    .await
    .unwrap();
    let reader_summary =
        VectorMemoryStore::new(reader_keyspace.scope(), reader_keyspace.index_id(), 1)
            .load_descriptor_bound_with_budget(
                &reader,
                VectorMemoryAdmissionBudget::Unbounded,
                None,
            )
            .await
            .unwrap();
    assert_eq!(reader_summary.loaded_entries, 1);
    let (_reader_shutdown_tx, mut reader_shutdown_rx) = watch::channel(false);
    let reader_summary =
        VectorMemoryStore::new(reader_keyspace.scope(), reader_keyspace.index_id(), 1)
            .load_descriptor_bound_with_budget(
                &reader,
                VectorMemoryAdmissionBudget::Unbounded,
                Some(&mut reader_shutdown_rx),
            )
            .await
            .unwrap();
    assert_eq!(reader_summary.loaded_entries, 1);

    let shutdown = VectorMemoryStore::new(bounded_keyspace.scope(), bounded_keyspace.index_id(), 1);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let summary = shutdown
        .load_descriptor_bound_with_budget(
            bounded_db.as_ref(),
            VectorMemoryAdmissionBudget::Unbounded,
            Some(&mut shutdown_rx),
        )
        .await
        .unwrap();
    assert_eq!(summary.loaded_entries, 0);
    assert_eq!(
        summary.completion,
        VectorMemoryStoreLoadCompletion::Shutdown
    );

    let (_open_tx, mut open_rx) = watch::channel(false);
    let summary = VectorMemoryStore::new(bounded_keyspace.scope(), bounded_keyspace.index_id(), 1)
        .load_descriptor_bound_with_budget(
            bounded_db.as_ref(),
            VectorMemoryAdmissionBudget::Unbounded,
            Some(&mut open_rx),
        )
        .await
        .unwrap();
    assert_eq!(summary.loaded_entries, 2);
    assert_eq!(
        summary.completion,
        VectorMemoryStoreLoadCompletion::Complete
    );

    let (unchanged_tx, mut unchanged_rx) = watch::channel(false);
    unchanged_tx.send(false).unwrap();
    let summary = VectorMemoryStore::new(bounded_keyspace.scope(), bounded_keyspace.index_id(), 1)
        .load_descriptor_bound_with_budget(
            bounded_db.as_ref(),
            VectorMemoryAdmissionBudget::Unbounded,
            Some(&mut unchanged_rx),
        )
        .await
        .unwrap();
    assert_eq!(summary.loaded_entries, 2);
    assert_eq!(
        summary.completion,
        VectorMemoryStoreLoadCompletion::Complete
    );

    let (closed_tx, mut closed_rx) = watch::channel(false);
    drop(closed_tx);
    let summary = VectorMemoryStore::new(bounded_keyspace.scope(), bounded_keyspace.index_id(), 1)
        .load_descriptor_bound_with_budget(
            bounded_db.as_ref(),
            VectorMemoryAdmissionBudget::Unbounded,
            Some(&mut closed_rx),
        )
        .await
        .unwrap();
    assert_eq!(summary.loaded_entries, 0);
    assert_eq!(
        summary.completion,
        VectorMemoryStoreLoadCompletion::Shutdown
    );

    let zero_budget =
        VectorMemoryStore::new(bounded_keyspace.scope(), bounded_keyspace.index_id(), 1);
    let summary = zero_budget
        .load_descriptor_bound_with_budget(
            bounded_db.as_ref(),
            VectorMemoryAdmissionBudget::Bounded(0),
            None,
        )
        .await
        .unwrap();
    assert_eq!(summary.loaded_entries, 0);
    assert_eq!(
        summary.completion,
        VectorMemoryStoreLoadCompletion::BudgetExhausted
    );

    let scan = FaultingRead::new(bounded_db.as_ref(), ReadFault::Scan);
    assert!(
        VectorMemoryStore::new(bounded_keyspace.scope(), bounded_keyspace.index_id(), 1,)
            .load_descriptor_bound_with_budget(&scan, VectorMemoryAdmissionBudget::Unbounded, None,)
            .await
            .is_err()
    );

    let malformed_db = test_db("production_vector_memory_malformed_key").await;
    let malformed_keyspace = VectorRowKeyspace::new(
        "vector:memory:malformed-key".to_string(),
        DataScope::LegacyUnscoped,
    );
    let mut malformed_key = malformed_keyspace
        .key(VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(
            malformed_keyspace.index_id(),
        )))
        .to_vec();
    malformed_key.push(0xFE);
    let txn = malformed_db.begin(IsolationLevel::Snapshot).await.unwrap();
    txn.put(malformed_key, Bytes::from_static(b"invalid-hot-row"))
        .unwrap();
    txn.commit().await.unwrap();
    let descriptor_bound =
        VectorMemoryStore::new(malformed_keyspace.scope(), malformed_keyspace.index_id(), 1);
    assert!(descriptor_bound
        .load_descriptor_bound_with_budget(
            malformed_db.as_ref(),
            VectorMemoryAdmissionBudget::Unbounded,
            None,
        )
        .await
        .is_err());

    assert!(estimated_entry_bytes(usize::MAX, usize::MAX).is_err());
}

/// Exercises resident-cache ownership, reads, hydration, and fail-closed edges.
pub(crate) async fn run() {
    run_capability_and_fence_contracts().await;
    run_managed_lookup_contracts().await;
    run_hydration_contracts().await;
}
