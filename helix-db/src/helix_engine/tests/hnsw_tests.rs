#[cfg(feature = "rocks")]
use std::sync::Arc;

use bumpalo::Bump;
#[cfg(feature = "lmdb")]
use heed3::RwTxn;
#[cfg(feature = "lmdb")]
use heed3::{Env, EnvOpenOptions, RoTxn, WithTls};
use rand::Rng;
use tempfile::TempDir;

use crate::helix_engine::storage_core::txn::{ReadTransaction, WriteTransaction};
use crate::helix_engine::traversal_core::RTxn;
use crate::helix_engine::vector_core::{HNSW, HNSWConfig, VectorCore, vector::HVector};

type Filter = for<'a> fn(&HVector, &RTxn<'a>) -> bool;

#[cfg(feature = "lmdb")]
type DB = Env;

#[cfg(feature = "rocks")]
type DB = Arc<rocksdb::TransactionDB<rocksdb::MultiThreaded>>;

fn setup_env() -> (DB, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path();

    #[cfg(feature = "lmdb")]
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(512 * 1024 * 1024)
            .max_dbs(32)
            .open(path)
            .unwrap()
    };

    #[cfg(feature = "rocks")]
    let env = {
        use crate::helix_engine::storage_core::{HelixGraphStorage, default_helix_rocksdb_options};

        let mut cf_descriptors = vec![
            rocksdb::ColumnFamilyDescriptor::new("nodes", HelixGraphStorage::nodes_cf_options()),
            rocksdb::ColumnFamilyDescriptor::new("edges", HelixGraphStorage::edges_cf_options()),
            rocksdb::ColumnFamilyDescriptor::new(
                "out_edges",
                HelixGraphStorage::edges_index_cf_options(),
            ),
            rocksdb::ColumnFamilyDescriptor::new(
                "in_edges",
                HelixGraphStorage::edges_index_cf_options(),
            ),
            rocksdb::ColumnFamilyDescriptor::new("metadata", rocksdb::Options::default()),
        ];

        let vector_cf_descriptors = vec![
            rocksdb::ColumnFamilyDescriptor::new("vectors", VectorCore::vector_cf_options()),
            rocksdb::ColumnFamilyDescriptor::new(
                "vector_data",
                VectorCore::vector_properties_cf_options(),
            ),
            rocksdb::ColumnFamilyDescriptor::new(
                "hnsw_edges",
                VectorCore::vector_edges_cf_options(),
            ),
            rocksdb::ColumnFamilyDescriptor::new("ep", rocksdb::Options::default()),
        ];
        cf_descriptors.extend(vector_cf_descriptors);
        let db_opts = default_helix_rocksdb_options();
        let txn_db_opts = rocksdb::TransactionDBOptions::new();
        let db = Arc::new(
            rocksdb::TransactionDB::<rocksdb::MultiThreaded>::open_cf_descriptors(
                &db_opts,
                &txn_db_opts,
                path,
                cf_descriptors,
            )
            .unwrap(),
        );
        db
    };

    (env, temp_dir)
}

#[cfg(feature = "rocks")]
fn index(env: &DB) -> VectorCore {
    VectorCore::new(Arc::clone(env), HNSWConfig::new(None, None, None)).unwrap()
}

#[cfg(feature = "lmdb")]
fn index(env: &DB, txn: &mut RwTxn) -> VectorCore {
    VectorCore::new(env, txn, HNSWConfig::new(None, None, None)).unwrap()
}

#[test]
fn test_hnsw_insert_and_count() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    #[cfg(feature = "lmdb")]
    let index = index(&env, &mut txn);
    #[cfg(feature = "rocks")]
    let index = index(&env);

    let vector: Vec<f64> = (0..4).map(|_| rand::rng().random_range(0.0..1.0)).collect();
    for _ in 0..10 {
        let arena = Bump::new();
        let data = arena.alloc_slice_copy(&vector);
        let _ = index
            .insert::<Filter>(&mut txn, "vector", data, None, &arena)
            .unwrap();
    }

    txn.commit().unwrap();
    #[cfg(feature = "rocks")]
    assert!(
        env.iterator_cf(&index.cf_vectors(), rocksdb::IteratorMode::Start)
            .count()
            >= 10
    );

    #[cfg(feature = "lmdb")]
    assert!(index.vectors_db.len(&txn).unwrap() >= 10);
}

#[test]
fn test_hnsw_search_returns_results() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    #[cfg(feature = "lmdb")]
    let index = index(&env, &mut txn);
    #[cfg(feature = "rocks")]
    let index = index(&env);

    let mut rng = rand::rng();
    for _ in 0..128 {
        let arena = Bump::new();
        let vector: Vec<f64> = (0..4).map(|_| rng.random_range(0.0..1.0)).collect();
        let data = arena.alloc_slice_copy(&vector);
        let _ = index
            .insert::<Filter>(&mut txn, "vector", data, None, &arena)
            .unwrap();
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let query = [0.5, 0.5, 0.5, 0.5];
    let results = index
        .search::<Filter>(&txn, &query, 5, "vector", None, false, &arena)
        .unwrap();
    assert!(!results.is_empty());
}
