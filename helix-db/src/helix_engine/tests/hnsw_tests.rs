use bumpalo::Bump;
use heed3::{Env, EnvOpenOptions, RoTxn};
use rand::Rng;
use tempfile::TempDir;

use crate::helix_engine::vector_core::{HNSWConfig, HVector, VectorCore};

type Filter = fn(&HVector, &RoTxn) -> bool;

fn setup_env() -> (Env, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path();

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(512 * 1024 * 1024)
            .max_dbs(32)
            .open(path)
            .unwrap()
    };
    (env, temp_dir)
}

#[test]
fn test_hnsw_insert_and_count() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let mut index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let vector: Vec<f32> = (0..4).map(|_| rand::rng().random_range(0.0..1.0)).collect();
    for _ in 0..10 {
        let arena = Bump::new();
        let _ = index
            .insert(&mut txn, "vector", vector.as_slice(), None, &arena)
            .unwrap();
    }

    txn.commit().unwrap();
    assert!(index.num_inserted_vectors() >= 10);
}

#[test]
fn test_hnsw_search_returns_results() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let mut index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let mut rng = rand::rng();
    for _ in 0..128 {
        let arena = Bump::new();
        let vector: Vec<f32> = (0..4).map(|_| rng.random_range(0.0..1.0)).collect();
        let _ = index
            .insert(&mut txn, "vector", vector.as_slice(), None, &arena)
            .unwrap();
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let query = [0.5, 0.5, 0.5, 0.5];
    let results = index
        .search(&txn, query.to_vec(), 5, "vector", false, &arena)
        .unwrap();
    assert!(!results.nns.is_empty());
}
