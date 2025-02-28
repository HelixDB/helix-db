use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use helixdb::helix_engine::{
    storage_core::{
        hnsw::{HNSW, HNSWConfig},
        vectors::HVector,
    },
    types::GraphError,
};
use heed3::{EnvOpenOptions, Env};
use rand::{rngs::StdRng, SeedableRng, Rng};
use std::{path::Path, time::Duration};
use tempfile::TempDir;

/// Creates a temporary environment for testing
fn setup_temp_env() -> (Env, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024) // 1GB
            .max_dbs(10)
            .open(path)
            .unwrap()
    };
    
    (env, temp_dir)
}

/// Generates random vectors with specified dimensions
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

/// Benchmark insertion of vectors with different dimensions
fn bench_vector_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_insertion");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    
    // Test with different vector dimensions
    for &dim in &[128, 1024, 4096, 8192] {
        // Number of vectors to insert per iteration
        let vectors_per_iter = 10;
        
        let id = BenchmarkId::new(format!("insert_{}vecs", vectors_per_iter), dim);
        group.bench_with_input(id, &dim, |b, &dim| {
            eprintln!("Benchmarking insertion of {} vectors with {} dimensions", vectors_per_iter, dim);
            
            b.iter_with_setup(
                || {
                    let (env, _temp_dir) = setup_temp_env();
                    let mut txn = env.write_txn().unwrap();
                    let hnsw = HNSW::new(&env, &mut txn, None).unwrap();
                    txn.commit().unwrap();
                    
                    let vectors = generate_random_vectors(100, dim, 42);
                    (env, hnsw, vectors)
                },
                |(env, mut hnsw, vectors)| {
                    let mut txn = env.write_txn().unwrap();
                    for (id, data) in vectors.iter().take(vectors_per_iter) {
                        hnsw.insert(&mut txn, id, data).unwrap();
                    }
                    txn.commit().unwrap();
                },
            );
        });
    }
    
    group.finish();
}

/// Benchmark search with different dimensions
fn bench_vector_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_search");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    
    // Test with different vector dimensions
    for &dim in &[128, 1024, 4096, 8192] {
        // Number of vectors in the index
        let index_size = 1000;
        // Number of queries per iteration
        let queries_per_iter = 10;
        
        let id = BenchmarkId::new(format!("search_{}q_{}idx", queries_per_iter, index_size), dim);
        group.bench_with_input(id, &dim, |b, &dim| {
            eprintln!("Benchmarking {} queries against index of {} vectors with {} dimensions", 
                     queries_per_iter, index_size, dim);
            
            // Setup: Create index with vectors
            let (env, _temp_dir) = setup_temp_env();
            let mut txn = env.write_txn().unwrap();
            let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
            
            let vectors = generate_random_vectors(index_size, dim, 42);
            eprintln!("Building index with {} vectors of {} dimensions...", vectors.len(), dim);
            
            for (id, data) in &vectors {
                hnsw.insert(&mut txn, id, data).unwrap();
            }
            txn.commit().unwrap();
            eprintln!("Index built successfully");
            
            // Create query vectors
            let query_vectors = generate_random_vectors(queries_per_iter, dim, 100);
            
            b.iter(|| {
                let txn = env.read_txn().unwrap();
                for (_, data) in &query_vectors {
                    let query = HVector::new("query".to_string(), data.clone());
                    let results = hnsw.search(&txn, &query, 10).unwrap();
                    black_box(results);
                }
            });
        });
    }
    
    group.finish();
}

/// Benchmark high throughput operations
fn bench_high_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_throughput");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);
    
    // Test with different numbers of vectors
    for &count in &[1000, 10000, 50000] {
        // Fixed dimension for throughput test
        let dim = 128;
        
        group.bench_with_input(BenchmarkId::new("build_index", count), &count, |b, &count| {
            eprintln!("Benchmarking building index with {} vectors of {} dimensions", count, dim);
            
            b.iter_with_setup(
                || {
                    let (env, _temp_dir) = setup_temp_env();
                    let vectors = generate_random_vectors(count, dim, 42);
                    (env, vectors)
                },
                |(env, vectors)| {
                    let mut txn = env.write_txn().unwrap();
                    let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
                    
                    for (id, data) in &vectors {
                        hnsw.insert(&mut txn, id, data).unwrap();
                    }
                    txn.commit().unwrap();
                },
            );
        });
        
        // Benchmark batch search operations
        // Number of queries per iteration
        let queries_per_iter = 100;
        
        let id = BenchmarkId::new(format!("batch_search_{}q", queries_per_iter), count);
        group.bench_with_input(id, &count, |b, &count| {
            eprintln!("Benchmarking {} queries against index of {} vectors with {} dimensions", 
                     queries_per_iter, count, dim);
            
            // Setup: Create index with vectors
            let (env, _temp_dir) = setup_temp_env();
            let mut txn = env.write_txn().unwrap();
            let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
            
            let vectors = generate_random_vectors(count, dim, 42);
            eprintln!("Building index with {} vectors of {} dimensions...", vectors.len(), dim);
            
            for (id, data) in &vectors {
                hnsw.insert(&mut txn, id, data).unwrap();
            }
            txn.commit().unwrap();
            eprintln!("Index built successfully");
            
            // Create query vectors
            let query_vectors = generate_random_vectors(queries_per_iter, dim, 100);
            
            b.iter(|| {
                let txn = env.read_txn().unwrap();
                for (_, data) in &query_vectors {
                    let query = HVector::new("query".to_string(), data.clone());
                    let results = hnsw.search(&txn, &query, 10).unwrap();
                    black_box(results);
                }
            });
        });
    }
    
    group.finish();
}

/// Benchmark different HNSW configurations
fn bench_hnsw_configs(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_configs");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    
    // Test different ef_construction values
    let configs = vec![
        ("default", HNSWConfig::default()),
        ("high_precision", HNSWConfig {
            m: 32,
            m_max: 64,
            ef_construction: 500,
            max_elements: 1_000_000,
            ml_factor: 1.0 / std::f64::consts::LN_2,
            distance_multiplier: 1.0,
        }),
        ("high_performance", HNSWConfig {
            m: 8,
            m_max: 16,
            ef_construction: 100,
            max_elements: 1_000_000,
            ml_factor: 1.0 / std::f64::consts::LN_2,
            distance_multiplier: 1.0,
        }),
    ];
    
    // Fixed parameters for this benchmark
    let index_size = 1000;
    let dim = 1024;
    let queries_per_iter = 10;
    
    for (name, config) in configs {
        let id = BenchmarkId::new(format!("build_search_{}q_{}idx", queries_per_iter, index_size), name);
        group.bench_with_input(id, &config, |b, config| {
            eprintln!("Benchmarking config '{}' with {} vectors of {} dimensions and {} queries", 
                     name, index_size, dim, queries_per_iter);
            eprintln!("Config details: m={}, m_max={}, ef_construction={}", 
                     config.m, config.m_max, config.ef_construction);
            
            b.iter_with_setup(
                || {
                    let (env, _temp_dir) = setup_temp_env();
                    let vectors = generate_random_vectors(index_size, dim, 42);
                    let query_vectors = generate_random_vectors(queries_per_iter, dim, 100);
                    (env, vectors, query_vectors, config.clone())
                },
                |(env, vectors, query_vectors, config)| {
                    // Build index
                    let mut txn = env.write_txn().unwrap();
                    let mut hnsw = HNSW::new(&env, &mut txn, Some(config)).unwrap();
                    
                    for (id, data) in &vectors {
                        hnsw.insert(&mut txn, id, data).unwrap();
                    }
                    txn.commit().unwrap();
                    
                    // Search
                    let txn = env.read_txn().unwrap();
                    for (_, data) in &query_vectors {
                        let query = HVector::new("query".to_string(), data.clone());
                        let results = hnsw.search(&txn, &query, 10).unwrap();
                        black_box(results);
                    }
                },
            );
        });
    }
    
    group.finish();
}

/// Benchmark memory usage with high-dimensional vectors
fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(5));
    
    // Test with extremely high dimensions
    for &dim in &[4096, 8192] {
        // Fixed parameters for this benchmark
        let index_size = 100;
        let queries_per_iter = 1;
        
        let id = BenchmarkId::new(format!("high_dim_{}vecs_{}q", index_size, queries_per_iter), dim);
        group.bench_with_input(id, &dim, |b, &dim| {
            eprintln!("Benchmarking memory usage with {} vectors of {} dimensions", index_size, dim);
            let bytes_per_vector = dim * std::mem::size_of::<f64>();
            let estimated_raw_data_mb = (index_size * bytes_per_vector) as f64 / (1024.0 * 1024.0);
            eprintln!("Estimated raw vector data size: {:.2} MB (excluding index overhead)", estimated_raw_data_mb);
            
            b.iter_with_setup(
                || {
                    let (env, _temp_dir) = setup_temp_env();
                    let vectors = generate_random_vectors(index_size, dim, 42);
                    (env, vectors)
                },
                |(env, vectors)| {
                    let mut txn = env.write_txn().unwrap();
                    let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
                    
                    for (id, data) in &vectors {
                        hnsw.insert(&mut txn, id, data).unwrap();
                    }
                    txn.commit().unwrap();
                    
                    // Perform some searches to measure memory during active use
                    let txn = env.read_txn().unwrap();
                    let query = HVector::new("query".to_string(), vectors[0].1.clone());
                    let results = hnsw.search(&txn, &query, 10).unwrap();
                    black_box(results);
                },
            );
        });
    }
    
    group.finish();
}

/// Benchmark scaling with increasing vector counts
fn bench_vector_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_scaling");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    
    // Fixed dimension for this benchmark
    let dim = 256;
    
    // Test with exponentially increasing vector counts
    for &count in &[100, 1000, 10000, 50000] {
        let id = BenchmarkId::new("search_scaling", count);
        group.bench_with_input(id, &count, |b, &count| {
            eprintln!("Benchmarking search scaling with {} vectors of {} dimensions", count, dim);
            
            // Setup: Create index with vectors
            let (env, _temp_dir) = setup_temp_env();
            let mut txn = env.write_txn().unwrap();
            let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
            
            let vectors = generate_random_vectors(count, dim, 42);
            eprintln!("Building index with {} vectors...", vectors.len());
            
            for (id, data) in &vectors {
                hnsw.insert(&mut txn, id, data).unwrap();
            }
            txn.commit().unwrap();
            eprintln!("Index built successfully");
            
            // Create 10 query vectors
            let query_vectors = generate_random_vectors(10, dim, 100);
            
            b.iter(|| {
                let txn = env.read_txn().unwrap();
                for (_, data) in &query_vectors {
                    let query = HVector::new("query".to_string(), data.clone());
                    let results = hnsw.search(&txn, &query, 10).unwrap();
                    black_box(results);
                }
            });
        });
    }
    
    group.finish();
}

/// Benchmark operations on a very large dataset
fn bench_large_dataset(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_dataset");
    // Longer measurement time for more stable results with large datasets
    group.measurement_time(Duration::from_secs(60));
    // Criterion requires at least 10 samples
    group.sample_size(10);
    // Reduce the number of resamples to speed up the benchmark
    group.warm_up_time(Duration::from_secs(5));
    
    // Parameters for the large dataset benchmark
    let base_count = 100_000; // Base dataset size (100K vectors)
    let dim = 128;           // Moderate dimension to balance memory usage and realism
    let batch_size = 1000;   // Number of vectors to insert in each benchmark iteration
    let query_count = 50;    // Number of queries to run in each benchmark iteration
    
    eprintln!("=== LARGE DATASET BENCHMARK ===");
    eprintln!("Preparing base dataset with {} vectors of {} dimensions", base_count, dim);
    eprintln!("This may take a while...");
    
    // Setup: Create a large base dataset
    let (env, _temp_dir) = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
    
    // Use a more efficient config for large datasets
    let large_dataset_config = HNSWConfig {
        m: 16,
        m_max: 32,
        ef_construction: 100, // Lower for faster construction
        max_elements: 1_000_000,
        ml_factor: 1.0 / std::f64::consts::LN_2,
        distance_multiplier: 1.0,
    };
    
    // Generate and insert the base dataset
    let base_vectors = generate_random_vectors(base_count, dim, 42);
    
    // Insert in batches to avoid excessive memory usage
    let batch_size_build = 10_000;
    let num_batches = (base_count + batch_size_build - 1) / batch_size_build;
    
    for i in 0..num_batches {
        let start = i * batch_size_build;
        let end = (start + batch_size_build).min(base_count);
        eprintln!("Building index batch {}/{}: vectors {}-{}", i+1, num_batches, start, end-1);
        
        for j in start..end {
            let (id, data) = &base_vectors[j];
            hnsw.insert(&mut txn, id, data).unwrap();
        }
    }
    
    txn.commit().unwrap();
    eprintln!("Base dataset of {} vectors built successfully", base_count);
    
    // 1. Benchmark: Insertion into existing large dataset
    group.bench_function(format!("insert_{}_into_{}", batch_size, base_count), |b| {
        eprintln!("Benchmarking insertion of {} new vectors into existing dataset of {} vectors", 
                 batch_size, base_count);
        
        // Generate new vectors for insertion
        let new_vectors = generate_random_vectors(batch_size, dim, 100);
        
        b.iter(|| {
            let mut txn = env.write_txn().unwrap();
            for (id, data) in &new_vectors {
                // Add a unique suffix to avoid ID conflicts with base dataset
                let unique_id = format!("{}_new", id);
                hnsw.insert(&mut txn, &unique_id, data).unwrap();
            }
            txn.commit().unwrap();
        });
    });
    
    // 2. Benchmark: Querying the large dataset
    group.bench_function(format!("query_{}_against_{}", query_count, base_count), |b| {
        eprintln!("Benchmarking {} queries against dataset of {} vectors", 
                 query_count, base_count);
        
        // Generate query vectors
        let query_vectors = generate_random_vectors(query_count, dim, 200);
        
        b.iter(|| {
            let txn = env.read_txn().unwrap();
            for (_, data) in &query_vectors {
                let query = HVector::new("query".to_string(), data.clone());
                let results = hnsw.search(&txn, &query, 10).unwrap();
                black_box(results);
            }
        });
    });
    
    // 3. Benchmark: Mixed workload (both insertions and queries)
    group.bench_function(format!("mixed_{}q_{}i_on_{}", query_count/2, batch_size/10, base_count), |b| {
        eprintln!("Benchmarking mixed workload: {} queries and {} insertions on dataset of {} vectors", 
                 query_count/2, batch_size/10, base_count);
        
        // Generate vectors for the mixed workload
        let insert_vectors = generate_random_vectors(batch_size/10, dim, 300);
        let query_vectors = generate_random_vectors(query_count/2, dim, 400);
        
        b.iter(|| {
            // First do some insertions
            let mut txn = env.write_txn().unwrap();
            for (id, data) in &insert_vectors {
                // Add a unique suffix to avoid ID conflicts
                let unique_id = format!("{}_mixed", id);
                hnsw.insert(&mut txn, &unique_id, data).unwrap();
            }
            txn.commit().unwrap();
            
            // Then do some queries
            let txn = env.read_txn().unwrap();
            for (_, data) in &query_vectors {
                let query = HVector::new("query".to_string(), data.clone());
                let results = hnsw.search(&txn, &query, 10).unwrap();
                black_box(results);
            }
        });
    });
    
    // 4. Benchmark: Bulk query performance (many queries in a single transaction)
    group.bench_function(format!("bulk_query_{}_against_{}", query_count*5, base_count), |b| {
        eprintln!("Benchmarking bulk query performance: {} queries against dataset of {} vectors", 
                 query_count*5, base_count);
        
        // Generate a larger set of query vectors for bulk testing
        let bulk_query_vectors = generate_random_vectors(query_count*5, dim, 500);
        
        b.iter(|| {
            let txn = env.read_txn().unwrap();
            for (_, data) in &bulk_query_vectors {
                let query = HVector::new("query".to_string(), data.clone());
                let results = hnsw.search(&txn, &query, 10).unwrap();
                black_box(results);
            }
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_vector_insertion,
    bench_vector_search,
    bench_high_throughput,
    bench_hnsw_configs,
    bench_memory_usage,
    bench_vector_scaling,
    bench_large_dataset
);
criterion_main!(benches); 