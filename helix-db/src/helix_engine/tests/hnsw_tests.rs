use bumpalo::Bump;
use heed3::{Env, EnvOpenOptions, RoTxn};
use rand::Rng;
use tempfile::TempDir;

use crate::helix_engine::{
    types::VectorError,
    vector_core::{
        hnsw::HNSW,
        vector::HVector,
        vector_core::{HNSWConfig, VectorCore},
    },
};

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

// ============================================================================
// HNSWConfig Validation Tests
// ============================================================================

#[test]
fn test_hnsw_config_defaults() {
    let config = HNSWConfig::new(None, None, None);
    assert_eq!(config.m, 16);
    assert_eq!(config.ef_construct, 128);
    // ef defaults to 768 but is clamped to max of 512
    assert_eq!(config.ef, 512);
}

#[test]
fn test_hnsw_config_clamp_below_min() {
    // m < 5 should clamp to 5
    let config = HNSWConfig::new(Some(1), Some(10), Some(1));
    assert_eq!(config.m, 5);
    // ef_construct < 40 should clamp to 40
    assert_eq!(config.ef_construct, 40);
    // ef < 10 should clamp to 10
    assert_eq!(config.ef, 10);
}

#[test]
fn test_hnsw_config_clamp_above_max() {
    // m > 48 should clamp to 48
    let config = HNSWConfig::new(Some(100), Some(1000), Some(1000));
    assert_eq!(config.m, 48);
    // ef_construct > 512 should clamp to 512
    assert_eq!(config.ef_construct, 512);
    // ef > 512 should clamp to 512
    assert_eq!(config.ef, 512);
}

#[test]
fn test_hnsw_config_m_l_calculation() {
    let config = HNSWConfig::new(Some(16), None, None);
    // m_l = 1.0 / ln(m) = 1.0 / ln(16)
    let expected_m_l = 1.0 / (16.0_f64).ln();
    assert!((config.m_l - expected_m_l).abs() < f64::EPSILON);

    // Test with a different m value
    let config = HNSWConfig::new(Some(32), None, None);
    let expected_m_l = 1.0 / (32.0_f64).ln();
    assert!((config.m_l - expected_m_l).abs() < f64::EPSILON);
}

#[test]
fn test_hnsw_config_m_max_0_calculation() {
    let config = HNSWConfig::new(Some(16), None, None);
    // m_max_0 = 2 * m
    assert_eq!(config.m_max_0, 32);

    let config = HNSWConfig::new(Some(24), None, None);
    assert_eq!(config.m_max_0, 48);
}

// ============================================================================
// VectorCore Delete Tests
// ============================================================================

#[test]
fn test_delete_existing_vector() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let arena = Bump::new();
    let vector: Vec<f64> = vec![0.1, 0.2, 0.3, 0.4];
    let data = arena.alloc_slice_copy(&vector);
    let inserted = index
        .insert::<Filter>(&mut txn, "vector", data, None, &arena)
        .unwrap();
    let inserted_id = inserted.id;

    // Delete the vector
    let delete_arena = Bump::new();
    let result = index.delete(&mut txn, inserted_id, &delete_arena);
    assert!(result.is_ok());
    txn.commit().unwrap();

    // Verify it's marked as deleted (get_vector_properties returns VectorDeleted error)
    let read_arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let props_result = index.get_vector_properties(&txn, inserted_id, &read_arena);
    assert!(matches!(props_result, Err(VectorError::VectorDeleted)));
}

#[test]
fn test_deleted_vector_excluded_from_search() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let arena = Bump::new();
    let target_vector: Vec<f64> = vec![1.0, 0.0, 0.0, 0.0];
    let data = arena.alloc_slice_copy(&target_vector);
    let target = index
        .insert::<Filter>(&mut txn, "vector", data, None, &arena)
        .unwrap();

    // Insert some other vectors
    for i in 0..5 {
        let arena = Bump::new();
        let vector: Vec<f64> = vec![0.1 * i as f64, 0.2, 0.3, 0.4];
        let data = arena.alloc_slice_copy(&vector);
        let _ = index
            .insert::<Filter>(&mut txn, "vector", data, None, &arena)
            .unwrap();
    }

    // Delete the target vector
    let delete_arena = Bump::new();
    index.delete(&mut txn, target.id, &delete_arena).unwrap();
    txn.commit().unwrap();

    // Search for the deleted vector's pattern - it should not appear in results
    let search_arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let query = [1.0, 0.0, 0.0, 0.0];
    let results = index
        .search::<Filter>(&txn, &query, 10, "vector", None, false, &search_arena)
        .unwrap();

    // Verify the deleted vector is not in results
    for result in &results {
        assert_ne!(result.id, target.id);
    }
}

#[test]
fn test_delete_non_existent_vector() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let arena = Bump::new();
    // Try to delete a vector that doesn't exist
    let fake_id: u128 = 12345678901234567890;
    let result = index.delete(&mut txn, fake_id, &arena);

    assert!(matches!(result, Err(VectorError::VectorNotFound(_))));
}

#[test]
fn test_delete_already_deleted_vector() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let arena = Bump::new();
    let vector: Vec<f64> = vec![0.1, 0.2, 0.3, 0.4];
    let data = arena.alloc_slice_copy(&vector);
    let inserted = index
        .insert::<Filter>(&mut txn, "vector", data, None, &arena)
        .unwrap();
    let inserted_id = inserted.id;

    // Delete once - should succeed
    let delete_arena = Bump::new();
    index.delete(&mut txn, inserted_id, &delete_arena).unwrap();

    // Delete again - should fail with VectorDeleted
    // Note: get_vector_properties returns VectorDeleted for deleted vectors,
    // which gets propagated before the VectorAlreadyDeleted check
    let delete_arena2 = Bump::new();
    let result = index.delete(&mut txn, inserted_id, &delete_arena2);
    assert!(matches!(result, Err(VectorError::VectorDeleted)));

    // Commit transaction to ensure proper cleanup
    txn.commit().unwrap();
}

// ============================================================================
// VectorCore Retrieval Tests
// ============================================================================

#[test]
fn test_get_vector_properties_existing() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let arena = Bump::new();
    let vector: Vec<f64> = vec![0.1, 0.2, 0.3, 0.4];
    let data = arena.alloc_slice_copy(&vector);
    let inserted = index
        .insert::<Filter>(&mut txn, "vector", data, None, &arena)
        .unwrap();
    txn.commit().unwrap();

    let txn = env.read_txn().unwrap();
    let props = index
        .get_vector_properties(&txn, inserted.id, &arena)
        .unwrap();
    assert!(props.is_some());
    let props = props.unwrap();
    assert_eq!(props.id, inserted.id);
    assert!(!props.deleted);
}

#[test]
fn test_get_vector_properties_deleted() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let arena = Bump::new();
    let vector: Vec<f64> = vec![0.1, 0.2, 0.3, 0.4];
    let data = arena.alloc_slice_copy(&vector);
    let inserted = index
        .insert::<Filter>(&mut txn, "vector", data, None, &arena)
        .unwrap();

    // Delete the vector
    index.delete(&mut txn, inserted.id, &arena).unwrap();
    txn.commit().unwrap();

    // Getting properties of deleted vector should return error
    let txn = env.read_txn().unwrap();
    let result = index.get_vector_properties(&txn, inserted.id, &arena);
    assert!(matches!(result, Err(VectorError::VectorDeleted)));
}

#[test]
fn test_get_full_vector_existing() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let arena = Bump::new();
    let vector: Vec<f64> = vec![0.1, 0.2, 0.3, 0.4];
    let data = arena.alloc_slice_copy(&vector);
    let inserted = index
        .insert::<Filter>(&mut txn, "vector", data, None, &arena)
        .unwrap();
    txn.commit().unwrap();

    let txn = env.read_txn().unwrap();
    let full_vector = index.get_full_vector(&txn, inserted.id, &arena).unwrap();
    assert_eq!(full_vector.id, inserted.id);
    assert!(!full_vector.deleted);
    // Verify vector data matches
    assert_eq!(full_vector.data.len(), 4);
}

#[test]
fn test_get_full_vector_non_existent() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let fake_id: u128 = 12345678901234567890;
    let result = index.get_full_vector(&txn, fake_id, &arena);
    assert!(matches!(result, Err(VectorError::VectorNotFound(_))));
}

#[test]
fn test_get_all_vectors_with_level_filter() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    // Insert multiple vectors
    for i in 0..10 {
        let arena = Bump::new();
        let vector: Vec<f64> = vec![0.1 * i as f64, 0.2, 0.3, 0.4];
        let data = arena.alloc_slice_copy(&vector);
        let _ = index
            .insert::<Filter>(&mut txn, "vector", data, None, &arena)
            .unwrap();
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = env.read_txn().unwrap();

    // Get all vectors without level filter
    let all_vectors = index.get_all_vectors(&txn, None, &arena).unwrap();
    assert_eq!(all_vectors.len(), 10);

    // Get vectors at level 0 (all vectors are stored at level 0)
    let level_0_vectors = index.get_all_vectors(&txn, Some(0), &arena).unwrap();
    assert_eq!(level_0_vectors.len(), 10);
}

// ============================================================================
// Search Edge Case Tests
// ============================================================================

#[test]
fn test_search_k_zero() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    // Insert some vectors
    for i in 0..5 {
        let arena = Bump::new();
        let vector: Vec<f64> = vec![0.1 * i as f64, 0.2, 0.3, 0.4];
        let data = arena.alloc_slice_copy(&vector);
        let _ = index
            .insert::<Filter>(&mut txn, "vector", data, None, &arena)
            .unwrap();
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let query = [0.5, 0.5, 0.5, 0.5];

    // Search with k=0 should return empty results
    let results = index
        .search::<Filter>(&txn, &query, 0, "vector", None, false, &arena)
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_search_k_exceeds_total() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    // Insert exactly 5 vectors
    for i in 0..5 {
        let arena = Bump::new();
        let vector: Vec<f64> = vec![0.1 * i as f64, 0.2, 0.3, 0.4];
        let data = arena.alloc_slice_copy(&vector);
        let _ = index
            .insert::<Filter>(&mut txn, "vector", data, None, &arena)
            .unwrap();
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let query = [0.5, 0.5, 0.5, 0.5];

    // Search with k=100, more than total vectors
    let results = index
        .search::<Filter>(&txn, &query, 100, "vector", None, false, &arena)
        .unwrap();
    // Should return at most 5 (all available vectors)
    assert!(results.len() <= 5);
}

#[test]
fn test_search_empty_index() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let query = [0.5, 0.5, 0.5, 0.5];

    // Search on empty index should return EntryPointNotFound error
    let result = index.search::<Filter>(&txn, &query, 5, "vector", None, false, &arena);
    assert!(matches!(result, Err(VectorError::EntryPointNotFound)));
}

#[test]
fn test_search_after_deletions() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let mut inserted_ids = Vec::new();

    // Insert 10 vectors
    for i in 0..10 {
        let arena = Bump::new();
        let vector: Vec<f64> = vec![0.1 * i as f64, 0.2, 0.3, 0.4];
        let data = arena.alloc_slice_copy(&vector);
        let inserted = index
            .insert::<Filter>(&mut txn, "vector", data, None, &arena)
            .unwrap();
        inserted_ids.push(inserted.id);
    }

    // Delete first 5 vectors
    for i in 0..5 {
        let arena = Bump::new();
        index.delete(&mut txn, inserted_ids[i], &arena).unwrap();
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let query = [0.5, 0.5, 0.5, 0.5];

    // Search should only return non-deleted vectors
    let results = index
        .search::<Filter>(&txn, &query, 10, "vector", None, false, &arena)
        .unwrap();

    // Should only find up to 5 vectors (the non-deleted ones)
    assert!(results.len() <= 5);

    // Verify none of the deleted vectors appear in results
    for result in &results {
        for i in 0..5 {
            assert_ne!(result.id, inserted_ids[i]);
        }
    }
}

#[test]
fn test_search_with_filter_predicate() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    // Insert vectors
    for i in 0..10 {
        let arena = Bump::new();
        let vector: Vec<f64> = vec![0.1 * i as f64, 0.2, 0.3, 0.4];
        let data = arena.alloc_slice_copy(&vector);
        let _ = index
            .insert::<Filter>(&mut txn, "vector", data, None, &arena)
            .unwrap();
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let query = [0.5, 0.5, 0.5, 0.5];

    // Filter that always returns false - should find no results
    let filter: &[fn(&HVector, &RoTxn) -> bool] = &[|_, _| false];
    let results = index
        .search(&txn, &query, 10, "vector", Some(filter), false, &arena)
        .unwrap();
    assert!(results.is_empty());

    // Filter that always returns true - should find results
    let filter: &[fn(&HVector, &RoTxn) -> bool] = &[|_, _| true];
    let results = index
        .search(&txn, &query, 10, "vector", Some(filter), false, &arena)
        .unwrap();
    assert!(!results.is_empty());
}

#[test]
fn test_search_label_filtering() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    // Insert vectors with label "vector_a"
    for i in 0..5 {
        let arena = Bump::new();
        let vector: Vec<f64> = vec![0.1 * i as f64, 0.2, 0.3, 0.4];
        let data = arena.alloc_slice_copy(&vector);
        let _ = index
            .insert::<Filter>(&mut txn, "vector_a", data, None, &arena)
            .unwrap();
    }

    // Insert vectors with label "vector_b"
    for i in 0..5 {
        let arena = Bump::new();
        let vector: Vec<f64> = vec![0.5 + 0.1 * i as f64, 0.2, 0.3, 0.4];
        let data = arena.alloc_slice_copy(&vector);
        let _ = index
            .insert::<Filter>(&mut txn, "vector_b", data, None, &arena)
            .unwrap();
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = env.read_txn().unwrap();
    let query = [0.5, 0.5, 0.5, 0.5];

    // Search for label "vector_a"
    let results = index
        .search::<Filter>(&txn, &query, 10, "vector_a", None, false, &arena)
        .unwrap();
    // All results should have label "vector_a"
    for result in &results {
        assert_eq!(result.label, "vector_a");
    }

    // Search for label "vector_b"
    let results = index
        .search::<Filter>(&txn, &query, 10, "vector_b", None, false, &arena)
        .unwrap();
    // All results should have label "vector_b"
    for result in &results {
        assert_eq!(result.label, "vector_b");
    }
}

// ============================================================================
// Original Tests (kept for backwards compatibility)
// ============================================================================

#[test]
fn test_hnsw_insert_and_count() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

    let vector: Vec<f64> = (0..4).map(|_| rand::rng().random_range(0.0..1.0)).collect();
    for _ in 0..10 {
        let arena = Bump::new();
        let data = arena.alloc_slice_copy(&vector);
        let _ = index
            .insert::<Filter>(&mut txn, "vector", data, None, &arena)
            .unwrap();
    }

    txn.commit().unwrap();
    let txn = env.read_txn().unwrap();
    assert!(index.num_inserted_vectors(&txn).unwrap() >= 10);
}

#[test]
fn test_hnsw_search_returns_results() {
    let (env, _temp_dir) = setup_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

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
