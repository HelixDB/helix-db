/// Performance benchmarks for Vec::with_capacity() optimizations
///
/// Run with: cargo test --test capacity_optimization_benches --release -- --nocapture
///
/// These are performance tests that measure actual execution time
/// to demonstrate the improvements from Vec::with_capacity() optimizations

#[cfg(test)]
mod tests {
    use helix_db::{
        helix_engine::{
            bm25::bm25::BM25,
            storage_core::HelixGraphStorage,
            traversal_core::{
                config::Config,
                ops::{
                    g::G,
                    source::{add_n::AddNAdapter, n_from_type::NFromTypeAdapter},
                    util::{
                        aggregate::AggregateAdapter, group_by::GroupByAdapter,
                        update::UpdateAdapter,
                    },
                },
            },
        },
        props,
        utils::id::v6_uuid,
    };
    use std::sync::Arc;
    use std::time::Instant;
    use tempfile::TempDir;

    fn setup_test_db() -> (Arc<HelixGraphStorage>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let mut config = Config::default();
        config.bm25 = Some(true);

        let storage = HelixGraphStorage::new(db_path, config, Default::default()).unwrap();
        (Arc::new(storage), temp_dir)
    }

    fn setup_db_with_nodes(count: usize) -> (Arc<HelixGraphStorage>, TempDir) {
        let (storage, temp_dir) = setup_test_db();
        let mut txn = storage.graph_env.write_txn().unwrap();

        for i in 0..count {
            let _ = G::new_mut(Arc::clone(&storage), &mut txn)
                .add_n(
                    "User",
                    Some(props! {
                        "name" => format!("User{}", i),
                        "age" => (20 + (i % 50)) as i64,
                        "department" => format!("Dept{}", i % 5),
                        "city" => format!("City{}", i % 10),
                        "role" => format!("Role{}", i % 3),
                        "score" => (i % 100) as i64,
                    }),
                    None,
                )
                .collect_to_obj();
        }

        txn.commit().unwrap();
        (storage, temp_dir)
    }

    #[test]
    fn bench_aggregate_small() {
        println!("\n=== Aggregate Performance (100 rows) ===");

        for prop_count in [1, 3, 5] {
            let (storage, _temp_dir) = setup_db_with_nodes(100);

            let properties: Vec<String> = match prop_count {
                1 => vec!["department".to_string()],
                3 => vec![
                    "department".to_string(),
                    "age".to_string(),
                    "city".to_string(),
                ],
                5 => vec![
                    "department".to_string(),
                    "age".to_string(),
                    "city".to_string(),
                    "role".to_string(),
                    "score".to_string(),
                ],
                _ => vec![],
            };

            let start = Instant::now();
            let txn = storage.graph_env.read_txn().unwrap();
            let _result = G::new(Arc::clone(&storage), &txn)
                .n_from_type("User")
                .aggregate_by(&properties, false);
            let elapsed = start.elapsed();

            println!("  {} properties: {:?}", prop_count, elapsed);
        }
    }

    #[test]
    fn bench_aggregate_medium() {
        println!("\n=== Aggregate Performance (1,000 rows) ===");

        for prop_count in [1, 3, 5] {
            let (storage, _temp_dir) = setup_db_with_nodes(1000);

            let properties: Vec<String> = match prop_count {
                1 => vec!["department".to_string()],
                3 => vec![
                    "department".to_string(),
                    "age".to_string(),
                    "city".to_string(),
                ],
                5 => vec![
                    "department".to_string(),
                    "age".to_string(),
                    "city".to_string(),
                    "role".to_string(),
                    "score".to_string(),
                ],
                _ => vec![],
            };

            let start = Instant::now();
            let txn = storage.graph_env.read_txn().unwrap();
            let _result = G::new(Arc::clone(&storage), &txn)
                .n_from_type("User")
                .aggregate_by(&properties, false);
            let elapsed = start.elapsed();

            println!("  {} properties: {:?}", prop_count, elapsed);
        }
    }

    #[test]
    fn bench_aggregate_large() {
        println!("\n=== Aggregate Performance (10,000 rows) ===");

        let (storage, _temp_dir) = setup_db_with_nodes(10000);

        for prop_count in [1, 3, 5] {
            let properties: Vec<String> = match prop_count {
                1 => vec!["department".to_string()],
                3 => vec![
                    "department".to_string(),
                    "age".to_string(),
                    "city".to_string(),
                ],
                5 => vec![
                    "department".to_string(),
                    "age".to_string(),
                    "city".to_string(),
                    "role".to_string(),
                    "score".to_string(),
                ],
                _ => vec![],
            };

            let start = Instant::now();
            let txn = storage.graph_env.read_txn().unwrap();
            let _result = G::new(Arc::clone(&storage), &txn)
                .n_from_type("User")
                .aggregate_by(&properties, false);
            let elapsed = start.elapsed();

            println!("  {} properties: {:?}", prop_count, elapsed);
        }
    }

    #[test]
    fn bench_group_by() {
        println!("\n=== Group By Performance ===");

        for size in [100, 1000] {
            let (storage, _temp_dir) = setup_db_with_nodes(size);
            let properties = vec!["department".to_string(), "city".to_string()];

            let start = Instant::now();
            let txn = storage.graph_env.read_txn().unwrap();
            let _result = G::new(Arc::clone(&storage), &txn)
                .n_from_type("User")
                .group_by(&properties, false);
            let elapsed = start.elapsed();

            println!("  {} rows: {:?}", size, elapsed);
        }
    }

    #[test]
    fn bench_update_operations() {
        println!("\n=== Update Performance ===");

        for size in [10, 100, 1000] {
            let (storage, _temp_dir) = setup_db_with_nodes(size);
            let mut txn = storage.graph_env.write_txn().unwrap();

            // Get nodes to update
            let update_tr = {
                let rtxn = storage.graph_env.read_txn().unwrap();
                G::new(Arc::clone(&storage), &rtxn)
                    .n_from_type("User")
                    .collect_to::<Vec<_>>()
            };

            let start = Instant::now();
            let _result = G::new_mut_from(Arc::clone(&storage), &mut txn, update_tr)
                .update(Some(vec![("score".to_string(), 999.into())]))
                .collect_to::<Vec<_>>();
            let elapsed = start.elapsed();

            txn.commit().unwrap();
            println!("  {} nodes: {:?}", size, elapsed);
        }
    }

    #[test]
    fn bench_bm25_search() {
        println!("\n=== BM25 Search Performance ===");

        let (storage, _temp_dir) = setup_test_db();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");

        // Insert 10,000 documents
        for i in 0..10000 {
            let doc = format!(
                "Document {} contains various search terms keywords database performance optimization testing benchmark",
                i
            );
            bm25.insert_doc(&mut wtxn, v6_uuid(), &doc).unwrap();
        }

        wtxn.commit().unwrap();

        let rtxn = storage.graph_env.read_txn().unwrap();

        for limit in [10, 100, 1000] {
            let start = Instant::now();
            let _results = bm25.search(&rtxn, "database optimization performance", limit);
            let elapsed = start.elapsed();

            println!("  limit={}: {:?}", limit, elapsed);
        }
    }

    #[test]
    fn bench_vector_allocation_patterns() {
        println!("\n=== Vector Allocation Patterns ===");

        // Pattern 1: Vec::new() in loop (old way - slow)
        let start = Instant::now();
        for _ in 0..1000 {
            let properties_count = 5;
            for _ in 0..100 {
                let mut vec1 = Vec::new();
                let mut vec2 = Vec::new();
                for i in 0..properties_count {
                    vec1.push(i);
                    vec2.push(format!("value_{}", i));
                }
            }
        }
        let vec_new_time = start.elapsed();
        println!("  Vec::new() in loop: {:?}", vec_new_time);

        // Pattern 2: Vec::with_capacity() in loop (new way - fast)
        let start = Instant::now();
        for _ in 0..1000 {
            let properties_count = 5;
            for _ in 0..100 {
                let mut vec1 = Vec::with_capacity(properties_count);
                let mut vec2 = Vec::with_capacity(properties_count);
                for i in 0..properties_count {
                    vec1.push(i);
                    vec2.push(format!("value_{}", i));
                }
            }
        }
        let vec_capacity_time = start.elapsed();
        println!("  Vec::with_capacity() in loop: {:?}", vec_capacity_time);

        let improvement =
            (1.0 - (vec_capacity_time.as_secs_f64() / vec_new_time.as_secs_f64())) * 100.0;
        println!("  Improvement: {:.1}% faster", improvement);
    }
}
