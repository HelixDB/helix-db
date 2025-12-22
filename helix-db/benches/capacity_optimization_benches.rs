///
/// Run with: cargo test --test capacity_optimization_benches --release -- --nocapture
///
/// this test demonstrate the improvements from arena and Vec::with_capacity() optimizations

#[cfg(test)]
mod tests {
    use bumpalo::Bump;
    use heed3::RoTxn;
    use helix_db::{
        helix_engine::{
            bm25::bm25::{BM25, HBM25Config},
            storage_core::{HelixGraphStorage, storage_methods::StorageMethods},
            traversal_core::{
                config::Config,
                ops::{
                    g::G,
                    source::{add_e::AddEAdapter, add_n::AddNAdapter},
                },
                traversal_value::TraversalValue,
            },
            types::GraphError,
        },
        utils::id::v6_uuid,
    };
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Instant;
    use tempfile::TempDir;

    fn search_without_arena(
        bm25: &HBM25Config,
        txn: &RoTxn,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(u128, f32)>, GraphError> {
        let arena = Bump::new();
        bm25.search(txn, query, limit, &arena)
    }

    fn search_with_arena(
        bm25: &HBM25Config,
        txn: &RoTxn,
        query: &str,
        limit: usize,
        arena: &Bump,
    ) -> Result<Vec<(u128, f32)>, GraphError> {
        bm25.search(txn, query, limit, arena)
    }

    fn setup_test_db(temp_dir: &TempDir) -> Arc<HelixGraphStorage> {
        let db_path = temp_dir.path().to_str().unwrap();

        let mut config = Config::default();
        config.bm25 = Some(true);

        let storage = HelixGraphStorage::new(db_path, config, Default::default()).unwrap();
        Arc::new(storage)
    }

    // fn setup_db_with_nodes(count: usize) -> (Arc<HelixGraphStorage>, TempDir) {
    //     let (storage, temp_dir) = setup_test_db();
    //     let mut txn = storage.graph_env.write_txn().unwrap();
    //     let arena = Bump::new();

    //     for i in 0..count {
    //         let props_vec = props! {
    //             "name" => format!("User{}", i),
    //             "age" => (20 + (i % 50)) as i64,
    //             "department" => format!("Dept{}", i % 5),
    //             "city" => format!("City{}", i % 10),
    //             "role" => format!("Role{}", i % 3),
    //             "score" => (i % 100) as i64,
    //         };
    //         let props_map = ImmutablePropertiesMap::new(
    //             props_vec.len(),
    //             props_vec
    //                 .iter()
    //                 .map(|(k, v): &(String, Value)| (arena.alloc_str(k) as &str, v.clone())),
    //             &arena,
    //         );
    //         let _ = G::new_mut(&storage, &arena, &mut txn)
    //             .add_n(arena.alloc_str("User"), Some(props_map), None)
    //             .collect_to_obj();
    //     }

    //     txn.commit().unwrap();
    //     (storage, temp_dir)
    // }
    #[test]
    fn bench_node_connections_arena_vs_capacity() {
        let temp_dir = &TempDir::new().unwrap();
        let storage = setup_test_db(&temp_dir);
        let mut txn = storage.graph_env.write_txn().unwrap();
        let arena_setup = bumpalo::Bump::new();

        let hub_node = G::new_mut(&storage, &arena_setup, &mut txn)
            .add_n(arena_setup.alloc_str("hub"), None, None)
            .collect_to_obj()
            .unwrap();

        // Create 50 connected nodes
        let mut connected_node_ids = Vec::new();
        for _i in 0..50 {
            let node = G::new_mut(&storage, &arena_setup, &mut txn)
                .add_n(arena_setup.alloc_str("person"), None, None)
                .collect_to_obj()
                .unwrap();

            G::new_mut(&storage, &arena_setup, &mut txn)
                .add_edge(
                    arena_setup.alloc_str("knows"),
                    None,
                    hub_node.id(),
                    node.id(),
                    false,
                    false,
                )
                .collect_to_obj()
                .unwrap();

            connected_node_ids.push(node.id());
        }

        txn.commit().unwrap();

        // arena without with_capacity

        for _ in 0..20 {
            let rtxn = storage.graph_env.read_txn().unwrap();
            let arena = bumpalo::Bump::new();
            let mut connected_node_ids = HashSet::new();
            let mut connected_nodes = Vec::new();

            let _ = storage
                .out_edges_db
                .prefix_iter(&rtxn, &hub_node.id().to_be_bytes())
                .unwrap()
                .filter_map(|result| match result {
                    Ok((_, value)) => match HelixGraphStorage::unpack_adj_edge_data(value) {
                        Ok((edge_id, to_node)) => {
                            if connected_node_ids.insert(to_node) {
                                if let Ok(node) = storage.get_node(&rtxn, &to_node, &arena) {
                                    connected_nodes.push(TraversalValue::Node(node));
                                }
                            }
                            match storage.get_edge(&rtxn, &edge_id, &arena) {
                                Ok(edge) => Some(TraversalValue::Edge(edge)),
                                Err(_) => None,
                            }
                        }
                        Err(_) => None,
                    },
                    Err(_) => None,
                })
                .collect::<Vec<_>>();
        }

        // arena without with_capacity
        let mut times_arena_no_capacity = Vec::new();
        for _ in 0..100 {
            let rtxn = storage.graph_env.read_txn().unwrap();
            let arena = bumpalo::Bump::new();

            let start = Instant::now();

            let mut connected_node_ids = HashSet::new();
            let mut connected_nodes = Vec::new();

            let _edges = storage
                .out_edges_db
                .prefix_iter(&rtxn, &hub_node.id().to_be_bytes())
                .unwrap()
                .filter_map(|result| match result {
                    Ok((_, value)) => match HelixGraphStorage::unpack_adj_edge_data(value) {
                        Ok((edge_id, to_node)) => {
                            if connected_node_ids.insert(to_node) {
                                if let Ok(node) = storage.get_node(&rtxn, &to_node, &arena) {
                                    connected_nodes.push(TraversalValue::Node(node));
                                }
                            }
                            match storage.get_edge(&rtxn, &edge_id, &arena) {
                                Ok(edge) => Some(TraversalValue::Edge(edge)),
                                Err(_) => None,
                            }
                        }
                        Err(_) => None,
                    },
                    Err(_) => None,
                })
                .collect::<Vec<_>>();

            times_arena_no_capacity.push(start.elapsed().as_micros());
        }

        // arena with capacity
        for _ in 0..20 {
            let rtxn = storage.graph_env.read_txn().unwrap();
            let arena = bumpalo::Bump::new();
            let mut connected_node_ids = HashSet::with_capacity(32);
            let mut connected_nodes = Vec::with_capacity(32);

            let _ = storage
                .out_edges_db
                .prefix_iter(&rtxn, &hub_node.id().to_be_bytes())
                .unwrap()
                .filter_map(|result| match result {
                    Ok((_, value)) => match HelixGraphStorage::unpack_adj_edge_data(value) {
                        Ok((edge_id, to_node)) => {
                            if connected_node_ids.insert(to_node) {
                                if let Ok(node) = storage.get_node(&rtxn, &to_node, &arena) {
                                    connected_nodes.push(TraversalValue::Node(node));
                                }
                            }
                            match storage.get_edge(&rtxn, &edge_id, &arena) {
                                Ok(edge) => Some(TraversalValue::Edge(edge)),
                                Err(_) => None,
                            }
                        }
                        Err(_) => None,
                    },
                    Err(_) => None,
                })
                .collect::<Vec<_>>();
        }

        // arena with with_capacity
        let mut times_arena_with_capacity = Vec::new();
        for _ in 0..100 {
            let rtxn = storage.graph_env.read_txn().unwrap();
            let arena = bumpalo::Bump::new();

            let start = Instant::now();

            let mut connected_node_ids = HashSet::with_capacity(32);
            let mut connected_nodes = Vec::with_capacity(32);

            let _edges = storage
                .out_edges_db
                .prefix_iter(&rtxn, &hub_node.id().to_be_bytes())
                .unwrap()
                .filter_map(|result| match result {
                    Ok((_, value)) => match HelixGraphStorage::unpack_adj_edge_data(value) {
                        Ok((edge_id, to_node)) => {
                            if connected_node_ids.insert(to_node) {
                                if let Ok(node) = storage.get_node(&rtxn, &to_node, &arena) {
                                    connected_nodes.push(TraversalValue::Node(node));
                                }
                            }
                            match storage.get_edge(&rtxn, &edge_id, &arena) {
                                Ok(edge) => Some(TraversalValue::Edge(edge)),
                                Err(_) => None,
                            }
                        }
                        Err(_) => None,
                    },
                    Err(_) => None,
                })
                .collect::<Vec<_>>();

            times_arena_with_capacity.push(start.elapsed().as_micros());
        }

        times_arena_no_capacity.sort_unstable();
        times_arena_with_capacity.sort_unstable();

        let no_capacity_median = times_arena_no_capacity[times_arena_no_capacity.len() / 2];
        let with_capacity_median = times_arena_with_capacity[times_arena_with_capacity.len() / 2];

        let no_capacity_avg: u128 =
            times_arena_no_capacity.iter().sum::<u128>() / times_arena_no_capacity.len() as u128;
        let with_capacity_avg: u128 = times_arena_with_capacity.iter().sum::<u128>()
            / times_arena_with_capacity.len() as u128;

        let improvement_median = ((no_capacity_median as f64 - with_capacity_median as f64)
            / no_capacity_median as f64)
            * 100.0;
        let improvement_avg =
            ((no_capacity_avg as f64 - with_capacity_avg as f64) / no_capacity_avg as f64) * 100.0;

        println!("   Arena WITHOUT with_capacity:");
        println!("     • Average: {}μs", no_capacity_avg);
        println!("     • Median:  {}μs\n", no_capacity_median);

        println!("   Arena WITH with_capacity(32):");
        println!("     • Average: {}μs", with_capacity_avg);
        println!("     • Median:  {}μs\n", with_capacity_median);

        println!("    Improvement:");
        println!("     • Average: {:.1}% faster", improvement_avg);
        println!("     • Median:  {:.1}% faster\n", improvement_median);
    }

    #[test]
    fn bench_nodes_by_label_with_excessive_limit() {
        use helix_db::utils::items::Node;

        let temp_dir = &TempDir::new().unwrap();
        let storage = setup_test_db(&temp_dir);
        let mut txn = storage.graph_env.write_txn().unwrap();
        let arena = bumpalo::Bump::new();

        // Insert only 100 nodes
        println!("\nInserting 100 person nodes...");
        for _i in 0..100 {
            let _node = G::new_mut(&storage, &arena, &mut txn)
                .add_n(arena.alloc_str("person"), None, None)
                .collect_to_obj()
                .unwrap();
        }
        txn.commit().unwrap();

        println!("test capacity optimization with excessive limit (10M) on 100 nodes...");

        const MAX_PREALLOCATE_CAPACITY: usize = 100_000;

        // Test without capacity optimization
        let mut times_no_capacity = Vec::new();
        for _ in 0..50 {
            let rtxn = storage.graph_env.read_txn().unwrap();
            let arena = bumpalo::Bump::new();
            let start = Instant::now();

            let mut nodes = Vec::new();
            for result in storage.nodes_db.iter(&rtxn).unwrap() {
                let (id, node_data) = result.unwrap();
                if let Ok(node) = Node::from_bincode_bytes(id, node_data, &arena) {
                    if node.label == "person" {
                        nodes.push(node);
                    }
                }
            }
            times_no_capacity.push(start.elapsed().as_micros());
        }

        // test with capacity optimization
        let mut times_with_capacity = Vec::new();
        for _ in 0..50 {
            let rtxn = storage.graph_env.read_txn().unwrap();
            let arena = bumpalo::Bump::new();
            let start = Instant::now();

            let limit = 10_000_000;
            let initial_capacity = if limit <= MAX_PREALLOCATE_CAPACITY {
                limit
            } else {
                MAX_PREALLOCATE_CAPACITY
            };

            let mut nodes = Vec::with_capacity(initial_capacity);
            for result in storage.nodes_db.iter(&rtxn).unwrap() {
                let (id, node_data) = result.unwrap();
                if let Ok(node) = Node::from_bincode_bytes(id, node_data, &arena) {
                    if node.label == "person" {
                        nodes.push(node);
                    }
                }
            }
            times_with_capacity.push(start.elapsed().as_micros());
        }

        times_no_capacity.sort_unstable();
        times_with_capacity.sort_unstable();

        let no_cap_median = times_no_capacity[times_no_capacity.len() / 2];
        let with_cap_median = times_with_capacity[times_with_capacity.len() / 2];

        let improvement =
            ((no_cap_median as f64 - with_cap_median as f64) / no_cap_median as f64) * 100.0;

        println!("   Without capacity: {}μs (median)", no_cap_median);
        println!(
            "   With capacity({}): {}μs (median)",
            MAX_PREALLOCATE_CAPACITY, with_cap_median
        );
        println!("   Improvement: {:.1}% faster\n", improvement);
    }

    #[test]
    fn bench_bm25_search_before_and_after() {
        let temp_dir = &TempDir::new().unwrap();
        let storage = setup_test_db(&temp_dir);
        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let bm25 = storage.bm25.as_ref().unwrap();

        // insert 10,000 documents
        for i in 0..10_000 {
            let doc = format!(
                "Document {} database search optimization performance query index benchmark test {}",
                i,
                i % 100
            );
            bm25.insert_doc(&mut wtxn, v6_uuid(), &doc).unwrap();
        }
        wtxn.commit().unwrap();

        let test_case = vec![
            ("Simple (1 term)", "database", 100),
            ("Medium (3 terms)", "database search optimization", 100),
            (
                "Complex (5 terms)",
                "database search optimization performance benchmark",
                100,
            ),
        ];

        println!("\n Running benchmarks (100 iterations each)...\n");

        for (name, query, limit) in test_case {
            let rtxn = storage.graph_env.read_txn().unwrap();

            // without arena implementation
            for _ in 0..50 {
                let _results = search_without_arena(bm25, &rtxn, query, limit).unwrap();
            }

            //  without arena implementation
            let mut before_times = Vec::new();
            for _ in 0..500 {
                let start = Instant::now();
                let _results = search_without_arena(bm25, &rtxn, query, limit).unwrap();
                before_times.push(start.elapsed().as_micros());
            }

            for _ in 0..50 {
                let arena = Bump::new();
                let _ = bm25.search(&rtxn, query, limit, &arena).unwrap();
            }

            //  with arena implementation
            let mut after_times = Vec::new();
            for _ in 0..500 {
                let arena = Bump::new();
                let start = Instant::now();
                let _results = search_with_arena(bm25, &rtxn, query, limit, &arena).unwrap();
                after_times.push(start.elapsed().as_micros());
            }

            let before_avg = before_times.iter().sum::<u128>() / before_times.len() as u128;
            let after_avg = after_times.iter().sum::<u128>() / after_times.len() as u128;

            before_times.sort_unstable();
            after_times.sort_unstable();
            let before_median = before_times[before_times.len() / 2];
            let after_median = after_times[after_times.len() / 2];

            let before_stddev = calculate_stddev(&before_times, before_avg);
            let after_stddev = calculate_stddev(&after_times, after_avg);

            let improvement_avg =
                ((before_avg as f64 - after_avg as f64) / before_avg as f64) * 100.0;
            let improvement_median =
                ((before_median as f64 - after_median as f64) / before_median as f64) * 100.0;

            println!(" {}", name);
            println!(
                "   Before: {}μs avg (±{}μs) | {}μs median",
                before_avg, before_stddev, before_median
            );
            println!(
                "   After:  {}μs avg (±{}μs) | {}μs median",
                after_avg, after_stddev, after_median
            );
            println!(
                "   Improvement: {:.1}% (avg) | {:.1}% (median)\n",
                improvement_avg, improvement_median
            );
        }

        println!("Impact of Result Limit: ");
        let rtxn = storage.graph_env.read_txn().unwrap();

        for limit in [10, 100, 1000] {
            // Warmup
            for _ in 0..50 {
                let _ = search_without_arena(bm25, &rtxn, "database optimization", limit).unwrap();
                let arena = Bump::new();
                let _ = bm25
                    .search(&rtxn, "database optimization", limit, &arena)
                    .unwrap();
            }

            let mut before_times = Vec::new();
            for _ in 0..500 {
                let start = Instant::now();
                let _ = search_without_arena(bm25, &rtxn, "database optimization", limit).unwrap();
                before_times.push(start.elapsed().as_micros());
            }
            let before_median = {
                before_times.sort_unstable();
                before_times[before_times.len() / 2]
            };

            let mut after_times = Vec::new();
            for _ in 0..500 {
                let arena = Bump::new();
                let start = Instant::now();
                let _ = bm25
                    .search(&rtxn, "database optimization", limit, &arena)
                    .unwrap();
                after_times.push(start.elapsed().as_micros());
            }
            let after_median = {
                after_times.sort_unstable();
                after_times[after_times.len() / 2]
            };

            let improvement =
                ((before_median as f64 - after_median as f64) / before_median as f64) * 100.0;

            println!(
                "   limit={:4} → Before: {}μs, After: {}μs ({:.1}% faster)",
                limit, before_median, after_median, improvement
            );
        }
    }
    fn calculate_stddev(times: &[u128], mean: u128) -> u128 {
        let variance = times
            .iter()
            .map(|&t| {
                let diff = if t > mean { t - mean } else { mean - t };
                diff * diff
            })
            .sum::<u128>()
            / times.len() as u128;
        (variance as f64).sqrt() as u128
    }
}
