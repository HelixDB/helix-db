use std::path::Path;
use rand::{rngs::StdRng, SeedableRng, Rng};
use heed3::{EnvOpenOptions, Env};

use super::hnsw::{HNSW, HNSWConfig};
use super::vectors::HVector;
use crate::helix_engine::types::GraphError;

fn setup_temp_env() -> Env {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    
    unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(10)
            .open(path)
            .unwrap()
    }
}

fn generate_random_vectors(count: usize, dim: usize, seed: u64) -> Vec<(String, Vec<f64>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut vectors = Vec::with_capacity(count);
    
    for i in 0..count {
        let id = format!("vec_{}", i);
        let data: Vec<f64> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        vectors.push((id, data));
    }
    
    vectors
}

#[test]
fn test_hnsw_creation() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    
    let hnsw = HNSW::new(&env, &mut txn, None);
    assert!(hnsw.is_ok());
    
    let hnsw = HNSW::new(&env, &mut txn, Some(Default::default()));
    assert!(hnsw.is_ok());
    
    txn.commit().unwrap();
}

#[test]
fn test_hnsw_insert_single() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
    
    let id = "test_vec";
    let data = vec![1.0, 2.0, 3.0];
    
    let result = hnsw.insert(&mut txn, id, &data);
    assert!(result.is_ok());
    
    txn.commit().unwrap();
}

#[test]
fn test_hnsw_search_empty() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let hnsw = HNSW::new(&env, &mut txn, None).unwrap();
    
    let query = HVector::new("query".to_string(), vec![1.0, 2.0, 3.0]);
    let results = hnsw.search(&txn, &query, 5);
    
    assert!(results.is_ok());
    assert!(results.unwrap().is_empty());
    
    txn.commit().unwrap();
}

#[test]
fn test_hnsw_insert_and_search() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
    
    let vectors = generate_random_vectors(100, 10, 42);
    
    for (id, data) in &vectors {
        let result = hnsw.insert(&mut txn, id, data);
        assert!(result.is_ok());
    }
    
    let query_id = &vectors[0].0;
    let query_data = &vectors[0].1;
    let query = HVector::new(query_id.clone(), query_data.clone());
    
    let results = hnsw.search(&txn, &query, 5).unwrap();
    
    assert!(!results.is_empty());
    assert_eq!(results[0].0, *query_id);
    assert!(results[0].1 < 0.001);
    
    txn.commit().unwrap();
}

#[test]
fn test_hnsw_accuracy() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    
    let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
    
    let mut vectors = Vec::new();
    
    for cluster in 0..3 {
        let mut base = vec![0.0; 5];
        base[cluster] = 10000.0;
        
        for i in 0..5 {
            let id = format!("cluster_{}_vec_{}", cluster, i);
            let mut data = base.clone();
            
            for j in 0..data.len() {
                data[j] += i as f64 * 0.01;
            }
            
            vectors.push((id, data));
        }
    }
    
    for (id, data) in &vectors {
        let result = hnsw.insert(&mut txn, id, data);
        assert!(result.is_ok());
    }
    
    for cluster in 0..3 {
        let query_idx = cluster * 5;
        let query_id = &vectors[query_idx].0;
        let query_data = &vectors[query_idx].1;
        let query = HVector::new(query_id.clone(), query_data.clone());
        
        let results = hnsw.search(&txn, &query, 5).unwrap();
        
        assert!(!results.is_empty(), "No results found for query: {}", query_id);
        assert_eq!(results[0].0, *query_id);
    }
    
    txn.commit().unwrap();
}

#[test]
fn test_hnsw_persistence() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    
    let vectors = generate_random_vectors(50, 10, 42);
    let query_id = vectors[0].0.clone();
    let query_data = vectors[0].1.clone();
    
    {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(10)
                .open(path)
                .unwrap()
        };
        
        let mut txn = env.write_txn().unwrap();
        let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
        
        for (id, data) in &vectors {
            let result = hnsw.insert(&mut txn, id, data);
            assert!(result.is_ok());
        }
        
        txn.commit().unwrap();
    }
    
    {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(10)
                .open(path)
                .unwrap()
        };
        
        let mut init_txn = env.write_txn().unwrap();
        let hnsw = HNSW::new(&env, &mut init_txn, None).unwrap();
        init_txn.commit().unwrap();
        
        let txn = env.read_txn().unwrap();
        
        let query = HVector::new(query_id.clone(), query_data);
        let results = hnsw.search(&txn, &query, 5).unwrap();
        
        assert!(!results.is_empty());
        assert_eq!(results[0].0, query_id);
        assert!(results[0].1 < 0.001);
    }
}

#[test]
fn test_hnsw_large_scale() {
    let env = setup_temp_env();
    
    let config = HNSWConfig {
        m: 16,
        m_max: 32,
        ef_construction: 500,
        max_elements: 10_000,
        ml_factor: 1.0 / std::f64::consts::LN_2,
        distance_multiplier: 1.0,
    };
    
    let mut txn = env.write_txn().unwrap();
    let mut hnsw = HNSW::new(&env, &mut txn, Some(config)).unwrap();
    
    let vectors = generate_random_vectors(100, 10, 42);
    
    for (id, data) in &vectors {
        let result = hnsw.insert(&mut txn, id, data);
        assert!(result.is_ok());
    }
    
    let query_indices = vec![0, 10, 20]; 
    
    for &idx in &query_indices {
        let query_id = &vectors[idx].0;
        let query_data = &vectors[idx].1;
        let query = HVector::new(query_id.clone(), query_data.clone());
        
        let results = hnsw.search(&txn, &query, 10).unwrap();
        
        assert!(!results.is_empty(), 
                "No results found for query vector {}", query_id);
                
        println!("Query: {}, Found {} results. First result: {} with distance {}", 
                 query_id, results.len(), results[0].0, results[0].1);
    }
    
    txn.commit().unwrap();
}

#[test]
fn test_hnsw_edge_cases() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
    
    let result = hnsw.insert(&mut txn, "empty", &[]);
    assert!(result.is_ok());
    
    let large_vec: Vec<f64> = (0..1000).map(|i| i as f64).collect();
    let result = hnsw.insert(&mut txn, "large", &large_vec);
    assert!(result.is_ok());
    
    let result1 = hnsw.insert(&mut txn, "duplicate", &[1.0, 2.0, 3.0]);
    assert!(result1.is_ok());
    
    let result2 = hnsw.insert(&mut txn, "duplicate", &[4.0, 5.0, 6.0]);
    assert!(result2.is_ok());
    
    let query = HVector::new("query".to_string(), vec![4.0, 5.0, 6.0]);
    let results = hnsw.search(&txn, &query, 1).unwrap();
    
    assert!(!results.is_empty());
    
    txn.commit().unwrap();
}

#[test]
fn test_hnsw_different_dimensions() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
    
    hnsw.insert(&mut txn, "vec_2d", &[1.0, 2.0]).unwrap();
    hnsw.insert(&mut txn, "vec_3d", &[1.0, 2.0, 3.0]).unwrap();
    hnsw.insert(&mut txn, "vec_4d", &[1.0, 2.0, 3.0, 4.0]).unwrap();
    
    let query_2d = HVector::new("query".to_string(), vec![1.0, 2.0]);
    let results_2d = hnsw.search(&txn, &query_2d, 3).unwrap();
    
    let query_3d = HVector::new("query".to_string(), vec![1.0, 2.0, 3.0]);
    let results_3d = hnsw.search(&txn, &query_3d, 3).unwrap();
    
    assert!(!results_2d.is_empty());
    assert!(!results_3d.is_empty());
    
    txn.commit().unwrap();
} 