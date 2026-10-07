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
use crate::encoding::keys::scope::TenantId;
use crate::encoding::v2::keys::indexes::vector::{
    VectorMemoryPrefixKey, VectorSimHashKey, VectorUpperNeighborsKey, VectorUpperVectorKey,
};
use crate::encoding::v2::values::indexes::vector::{
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
    assert!(!uncached.is_node_dirty(1));
    assert!(!uncached.is_upper_neighbors_dirty(1, 1));

    let dirty = Arc::new(VectorMemoryDirtyRows::default());
    assert!(dirty.is_empty());
    dirty.mark_node_dirty(7);
    dirty.mark_upper_neighbors_dirty(2, 9);
    assert_eq!(dirty.dirty_nodes(), vec![7]);
    assert_eq!(dirty.dirty_upper_neighbors(), vec![(2, 9)]);

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
    store.insert_upper_vector(3, Bytes::from_static(b"resident-vector"));
    assert_eq!(
        managed
            .read_upper_vector_rows(db.as_ref(), &keyspace, &[3])
            .await
            .unwrap(),
        vec![Some(Bytes::from_static(b"resident-vector"))]
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
    let (ordered, ordered_stats) = managed
        .read_simhash_rows_counted::<true, _>(
            db.as_ref(),
            &keyspace,
            &[2, 1],
            "ordered resident and storage rows",
        )
        .await
        .unwrap();
    assert_eq!(
        ordered,
        vec![
            Some(SimHash::from_bits(99)),
            Some(SimHash::from_bits(0x1234))
        ]
    );
    assert_eq!(ordered_stats.reads, 1);
    assert_eq!(ordered_stats.multi_get_calls, 1);
    assert!(ordered_stats.fetch_ns > 0);
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
    assert!(managed
        .read_simhash_rows_counted::<true, _>(db.as_ref(), &keyspace, &[9], "corrupt ordered row",)
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
    let first_row_bytes = isolated_upper_row_admission_bytes(b"first".len() as u64);
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
}

/// Verifies the slab layout: inline and spilled neighbor layers, open and
/// sealed chunk reads, bounded chunk sizing, released ids, and exact charges.
async fn run_slab_layout_contracts() {
    let row = |chunk| SlabRow {
        chunk,
        offset: 0,
        len: 1,
    };
    let mut layers = UpperNeighborRows::Empty;
    assert_eq!(layers.get(1), None);
    assert_eq!(layers.growth_bytes(1), 0);
    layers.remove(1);
    layers.upsert(3, row(3));
    assert_eq!(layers.growth_bytes(3), 0);
    assert_eq!(layers.growth_bytes(1), 2 * UPPER_LAYER_SLOT_BYTES);
    layers.upsert(3, row(30));
    layers.remove(1);
    assert_eq!(layers.get(3), Some(row(30)));
    layers.upsert(1, row(1));
    layers.upsert(5, row(5));
    layers.upsert(5, row(50));
    assert_eq!(layers.growth_bytes(5), 0);
    assert_eq!(layers.growth_bytes(2), UPPER_LAYER_SLOT_BYTES);
    assert_eq!(layers.get(5), Some(row(50)));
    assert_eq!(layers.get(2), None);
    layers.remove(2);
    layers.remove(5);
    assert!(matches!(layers, UpperNeighborRows::Many(ref rows) if rows.len() == 2));
    layers.remove(3);
    assert!(matches!(layers, UpperNeighborRows::One(1, _)));
    layers.remove(1);
    assert!(layers.is_empty());
    let node = UpperNodeRows {
        vector: None,
        neighbors: UpperNeighborRows::One(1, row(1)),
    };
    assert_eq!(node.index_growth_bytes(UpperRowKind::Vector), 0);
    assert_eq!(
        node.index_growth_bytes(UpperRowKind::Neighbors(2)),
        2 * UPPER_LAYER_SLOT_BYTES
    );

    let slab = VectorMemorySlab::default();
    assert_eq!(slab.seal(), 0);
    let empty = slab
        .append(&[], VectorMemoryAdmissionBudget::Bounded(0))
        .unwrap();
    assert_eq!(slab.read(empty.row), Some(Bytes::new()));
    let handle = SLAB_CHUNK_HANDLE_BYTES;
    assert_eq!(
        slab.append(b"row", VectorMemoryAdmissionBudget::Bounded(1)),
        None
    );
    assert_eq!(
        slab.append(b"row", VectorMemoryAdmissionBudget::Bounded(handle + 2)),
        None
    );
    let exact = slab
        .append(b"row", VectorMemoryAdmissionBudget::Bounded(handle + 3))
        .unwrap();
    assert_eq!(exact.allocated, handle + 3);
    let halved = slab
        .append(b"next", VectorMemoryAdmissionBudget::Bounded(handle + 64))
        .unwrap();
    assert_eq!(halved.allocated, handle + 32);
    let fits = slab
        .append(b"fits", VectorMemoryAdmissionBudget::Bounded(0))
        .unwrap();
    assert_eq!(fits.allocated, 0);
    assert_eq!(slab.read(fits.row), Some(Bytes::from_static(b"fits")));
    assert_eq!(slab.seal(), 24);
    assert_eq!(slab.read(exact.row), Some(Bytes::from_static(b"row")));
    assert_eq!(slab.read(fits.row), Some(Bytes::from_static(b"fits")));
    let open = slab
        .append(b"open", VectorMemoryAdmissionBudget::Unbounded)
        .unwrap();
    assert_eq!(
        open.allocated,
        VECTOR_MEMORY_SLAB_CHUNK_BYTES as u64 + handle
    );
    let past_open = SlabRow {
        chunk: open.row.chunk + 1,
        ..open.row
    };
    assert_eq!(slab.read(past_open), None);
    slab.clear();
    slab.clear();
    assert_eq!(slab.read(open.row), None);
    assert_eq!(slab.read(exact.row), Some(Bytes::from_static(b"row")));
    let after = slab
        .append(b"after", VectorMemoryAdmissionBudget::Unbounded)
        .unwrap();
    assert!(after.row.chunk > open.row.chunk);
    assert_eq!(slab.read(after.row), Some(Bytes::from_static(b"after")));

    // Exact load charges, including rows that update fixture entries and
    // budgets that refuse an index entry or a SimHash.
    let db = test_db("production_vector_memory_slab_charges").await;
    let keyspace = VectorRowKeyspace::new(
        "vector:memory:slab-charges".to_string(),
        DataScope::LegacyUnscoped,
    );
    let index_id = keyspace.index_id();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    put_row(
        &txn,
        &keyspace,
        VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 1, 5)),
        Bytes::from_static(&[1; 10]),
    );
    put_row(
        &txn,
        &keyspace,
        VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 2, 5)),
        Bytes::from_static(&[2; 20]),
    );
    put_row(
        &txn,
        &keyspace,
        VectorKey::SimHash(VectorSimHashKey::new(index_id, 5)),
        Bytes::copy_from_slice(&encode_simhash(5)),
    );
    put_row(
        &txn,
        &keyspace,
        VectorKey::UpperVector(VectorUpperVectorKey::new(index_id, 5)),
        Bytes::from_static(&[3; 30]),
    );
    txn.commit().await.unwrap();

    let unbounded = VectorMemoryStore::new(keyspace.scope(), index_id, 1);
    let summary = unbounded
        .load_descriptor_bound_with_budget(
            db.as_ref(),
            VectorMemoryAdmissionBudget::Unbounded,
            None,
        )
        .await
        .unwrap();
    let full_charge = UPPER_NODE_INDEX_ENTRY_BYTES
        + 2 * UPPER_LAYER_SLOT_BYTES
        + SIMHASH_INDEX_ENTRY_BYTES
        + handle
        + 60;
    assert_eq!(summary.estimated_bytes, full_charge);

    let refreshed = VectorMemoryStore::new(keyspace.scope(), index_id, 1);
    refreshed.insert_upper_neighbors_bytes(1, 5, Bytes::from_static(b"stale-1"));
    refreshed.insert_upper_neighbors_bytes(2, 5, Bytes::from_static(b"stale-2"));
    refreshed.insert_simhash(5, SimHash::from_bits(0));
    let summary = refreshed
        .load_descriptor_bound_with_budget(
            db.as_ref(),
            VectorMemoryAdmissionBudget::Unbounded,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        summary.estimated_bytes,
        handle + 60,
        "updated entries charge only their chunk"
    );
    assert_eq!(refreshed.get_simhash(5), Some(SimHash::from_bits(5)));
    assert_eq!(
        refreshed.get_upper_neighbors_bytes(2, 5),
        Some(Bytes::from_static(&[2; 20]))
    );

    for (budget, loaded) in [
        (UPPER_NODE_INDEX_ENTRY_BYTES - 1, 0),
        (
            UPPER_NODE_INDEX_ENTRY_BYTES + handle + 10 + 2 * UPPER_LAYER_SLOT_BYTES - 1,
            1,
        ),
        (full_charge - 31, 2),
    ] {
        let bounded = VectorMemoryStore::new(keyspace.scope(), index_id, 1);
        let summary = bounded
            .load_descriptor_bound_with_budget(
                db.as_ref(),
                VectorMemoryAdmissionBudget::Bounded(budget),
                None,
            )
            .await
            .unwrap();
        assert_eq!(summary.loaded_entries, loaded, "budget {budget}");
        assert!(summary.estimated_bytes <= budget);
    }
}

/// Exercises resident-cache ownership, reads, hydration, and fail-closed edges.
pub(crate) async fn run() {
    run_capability_and_fence_contracts().await;
    run_managed_lookup_contracts().await;
    run_hydration_contracts().await;
    run_slab_layout_contracts().await;
}
