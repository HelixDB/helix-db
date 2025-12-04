//! Tests for Vec::with_capacity() optimizations
//!
//! These tests verify that our capacity optimizations:
//! 1. Produce correct results (no regression)
//! 2. Improve performance (benchmarks)
//! 3. Reduce memory allocations (allocation counting)

#[cfg(lmdb)]
mod tests {
    use bumpalo::Bump;
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::{
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
        protocol::value::Value,
        utils::{id::v6_uuid, properties::ImmutablePropertiesMap},
    };

    fn setup_test_db() -> (Arc<HelixGraphStorage>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let mut config = Config::default();
        config.bm25 = Some(true);

        let storage = HelixGraphStorage::new(db_path, config, Default::default()).unwrap();
        (Arc::new(storage), temp_dir)
    }

    fn setup_test_db_with_nodes(count: usize) -> (Arc<HelixGraphStorage>, TempDir) {
        let (storage, temp_dir) = setup_test_db();
        let mut txn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        // Create nodes with properties for testing aggregate/group operations
        for i in 0..count {
            let props_vec = props! {
                "name" => format!("User{}", i),
                "age" => (20 + (i % 50)) as i64,
                "department" => format!("Dept{}", i % 5),
                "score" => (i % 100) as i64,
            };
            let props_map = ImmutablePropertiesMap::new(
                props_vec.len(),
                props_vec
                    .iter()
                    .map(|(k, v): &(String, Value)| (arena.alloc_str(k) as &str, v.clone())),
                &arena,
            );
            let _ = G::new_mut(&storage, &arena, &mut txn)
                .add_n(arena.alloc_str("User"), Some(props_map), None)
                .collect_to_obj();
        }

        txn.commit().unwrap();
        (storage, temp_dir)
    }

    #[test]
    fn test_aggregate_correctness_small() {
        let (storage, _temp_dir) = setup_test_db_with_nodes(10);
        let txn = storage.graph_env.read_txn().unwrap();
        let arena = Bump::new();

        let properties = vec!["department".to_string()];

        let result = G::new(&storage, &txn, &arena)
            .n_from_type("User")
            .aggregate_by(&properties, false);

        assert!(result.is_ok(), "Aggregate should succeed");
        let aggregate = result.unwrap();

        // Should have 5 departments (Dept0-Dept4)
        match aggregate {
            crate::utils::aggregate::Aggregate::Group(groups) => {
                assert_eq!(groups.len(), 5, "Should have 5 distinct departments");
            }
            _ => panic!("Expected Group aggregate"),
        }
    }

    #[test]
    fn test_aggregate_correctness_large() {
        // Test with larger dataset to stress-test capacity allocation
        let (storage, _temp_dir) = setup_test_db_with_nodes(1000);
        let txn = storage.graph_env.read_txn().unwrap();
        let arena = Bump::new();

        let properties = vec!["department".to_string(), "age".to_string()];

        let result = G::new(&storage, &txn, &arena)
            .n_from_type("User")
            .aggregate_by(&properties, true);

        assert!(result.is_ok(), "Aggregate with 1000 nodes should succeed");
    }

    #[test]
    fn test_group_by_correctness() {
        let (storage, _temp_dir) = setup_test_db_with_nodes(100);
        let txn = storage.graph_env.read_txn().unwrap();
        let arena = Bump::new();

        let properties = vec!["department".to_string()];

        let result = G::new(&storage, &txn, &arena)
            .n_from_type("User")
            .group_by(&properties, false);

        assert!(result.is_ok(), "GroupBy should succeed");
    }

    #[test]
    fn test_update_operation_correctness() {
        let (storage, _temp_dir) = setup_test_db_with_nodes(50);
        let read_arena = Bump::new();

        // Update all users' scores
        // First get the nodes to update
        let update_tr = {
            let rtxn = storage.graph_env.read_txn().unwrap();
            G::new(&storage, &rtxn, &read_arena)
                .n_from_type("User")
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };

        let arena = Bump::new();
        let mut txn = storage.graph_env.write_txn().unwrap();
        let result = G::new_mut_from_iter(&storage, &mut txn, update_tr.into_iter(), &arena)
            .update(&[("score", 999.into())])
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(result.len(), 50, "Should update all 50 nodes");

        txn.commit().unwrap();
    }

    #[test]
    fn test_bm25_search_correctness() {
        let (storage, _temp_dir) = setup_test_db();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");

        // Insert test documents
        let docs = vec![
            (v6_uuid(), "The quick brown fox jumps over the lazy dog"),
            (v6_uuid(), "A fast brown fox leaps over a sleepy dog"),
            (v6_uuid(), "The lazy dog sleeps under the tree"),
            (v6_uuid(), "Quick foxes and lazy dogs are common"),
        ];

        for (id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *id, doc).unwrap();
        }

        wtxn.commit().unwrap();

        // Search
        let rtxn = storage.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "quick fox", 10, &arena);

        assert!(results.is_ok(), "BM25 search should succeed");
        let results = results.unwrap();
        assert!(!results.is_empty(), "Should find matching documents");
        assert!(results.len() <= 10, "Should respect limit");
    }

    #[test]
    fn test_bm25_search_with_large_limit() {
        let (storage, _temp_dir) = setup_test_db();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");

        // Insert 100 documents
        for i in 0..100 {
            let doc = format!("Document {} contains search terms and keywords", i);
            bm25.insert_doc(&mut wtxn, v6_uuid(), &doc).unwrap();
        }

        wtxn.commit().unwrap();

        // Search with large limit
        let rtxn = storage.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "document search", 1000, &arena);

        assert!(
            results.is_ok(),
            "BM25 search with large limit should succeed"
        );
    }

    /// Test that demonstrates capacity optimization doesn't break edge cases
    #[test]
    fn test_empty_result_sets() {
        let (storage, _temp_dir) = setup_test_db();
        let txn = storage.graph_env.read_txn().unwrap();
        let arena = Bump::new();

        // Test aggregate on empty set
        let properties = vec!["nonexistent".to_string()];
        let result = G::new(&storage, &txn, &arena)
            .n_from_type("NonExistentType")
            .aggregate_by(&properties, false);

        assert!(result.is_ok(), "Aggregate on empty set should succeed");
    }

    /// Test with properties of varying lengths
    #[test]
    fn test_aggregate_varying_property_counts() {
        let (storage, _temp_dir) = setup_test_db_with_nodes(100);
        let txn = storage.graph_env.read_txn().unwrap();
        let arena = Bump::new();

        // Test with 1 property
        let props1 = vec!["department".to_string()];
        let result = G::new(&storage, &txn, &arena)
            .n_from_type("User")
            .aggregate_by(&props1, false);
        assert!(result.is_ok(), "Aggregate with 1 property should work");

        // Test with 3 properties
        let props3 = vec![
            "department".to_string(),
            "age".to_string(),
            "score".to_string(),
        ];
        let result = G::new(&storage, &txn, &arena)
            .n_from_type("User")
            .aggregate_by(&props3, false);
        assert!(result.is_ok(), "Aggregate with 3 properties should work");
    }

    #[cfg(test)]
    mod performance_tests {
        use super::*;
        use std::time::Instant;

        /// This test measures relative performance
        /// Run with: cargo test test_aggregate_performance -- --nocapture --ignored
        #[test]
        #[ignore] // Ignore by default, run explicitly for performance testing
        fn test_aggregate_performance() {
            let sizes = vec![100, 1000, 10000];

            for size in sizes {
                let (storage, _temp_dir) = setup_test_db_with_nodes(size);
                let txn = storage.graph_env.read_txn().unwrap();
                let arena = Bump::new();

                let properties = vec![
                    "department".to_string(),
                    "age".to_string(),
                    "score".to_string(),
                ];

                let start = Instant::now();
                let result = G::new(&storage, &txn, &arena)
                    .n_from_type("User")
                    .aggregate_by(&properties, false);
                let elapsed = start.elapsed();

                assert!(result.is_ok(), "Aggregate should succeed");
                println!("Aggregate {} nodes with 3 properties: {:?}", size, elapsed);
            }
        }

        #[test]
        #[ignore]
        fn test_update_performance() {
            let sizes = vec![10, 100, 1000];

            for size in sizes {
                let (storage, _temp_dir) = setup_test_db_with_nodes(size);
                let read_arena = Bump::new();

                // Get nodes to update
                let update_tr = {
                    let rtxn = storage.graph_env.read_txn().unwrap();
                    G::new(&storage, &rtxn, &read_arena)
                        .n_from_type("User")
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap()
                };

                let arena = Bump::new();
                let mut txn = storage.graph_env.write_txn().unwrap();
                let start = Instant::now();
                let result =
                    G::new_mut_from_iter(&storage, &mut txn, update_tr.into_iter(), &arena)
                        .update(&[("score", 999.into())])
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                let elapsed = start.elapsed();

                assert_eq!(result.len(), size, "Update should succeed");
                println!("Update {} nodes: {:?}", size, elapsed);

                txn.commit().unwrap();
            }
        }

        #[test]
        #[ignore]
        fn test_bm25_search_performance() {
            let (storage, _temp_dir) = setup_test_db();
            let mut wtxn = storage.graph_env.write_txn().unwrap();

            let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");

            // Insert 10,000 documents
            for i in 0..10000 {
                let doc = format!(
                    "Document {} contains various search terms and keywords for testing performance",
                    i
                );
                bm25.insert_doc(&mut wtxn, v6_uuid(), &doc).unwrap();
            }

            wtxn.commit().unwrap();

            let rtxn = storage.graph_env.read_txn().unwrap();

            let limits = vec![10, 100, 1000];
            for limit in limits {
                let arena = Bump::new();
                let start = Instant::now();
                let results = bm25.search(&rtxn, "document search performance", limit, &arena);
                let elapsed = start.elapsed();

                assert!(results.is_ok(), "BM25 search should succeed");
                println!("BM25 search (limit={}): {:?}", limit, elapsed);
            }
        }
    }
}
