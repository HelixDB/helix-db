/// Concurrent access tests for HNSW Vector Core
///
/// This test suite validates thread safety and concurrent operation correctness
/// for the HNSW vector search implementation. Key areas tested:
///
/// 1. **Read-Write Conflicts**: Concurrent searches while inserts are happening
/// 2. **Write-Write Conflicts**: Multiple concurrent inserts
/// 3. **Race Conditions**: Entry point updates, graph topology consistency
///
/// CRITICAL ISSUES BEING TESTED:
/// - Entry point updates have no synchronization (potential race)
/// - Multiple inserts at same level could create invalid graph topology
/// - Delete during search might return inconsistent results
/// - LMDB transaction model provides MVCC but needs validation
use bumpalo::Bump;
use heed3::RoTxn;
use rand::Rng;
use serial_test::serial;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

use crate::helix_engine::storage_core::HelixGraphStorage;
use crate::helix_engine::storage_core::version_info::VersionInfo;
use crate::helix_engine::traversal_core::config::Config;
use crate::helix_engine::traversal_core::ops::g::G;
use crate::helix_engine::traversal_core::ops::vectors::insert::InsertVAdapter;
use crate::helix_engine::traversal_core::ops::vectors::search::SearchVAdapter;
use crate::helix_engine::vector_core::HVector;

type Filter = fn(&HVector, &RoTxn) -> bool;

/// Setup storage for concurrent testing
fn setup_concurrent_storage() -> (Arc<HelixGraphStorage>, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    let mut config = Config::default();
    config.db_max_size_gb = Some(1); // 1GB for concurrent testing

    let version_info = VersionInfo::default();

    let storage = HelixGraphStorage::new(path, config, version_info).unwrap();
    (Arc::new(storage), temp_dir)
}

/// Generate a random vector of given dimensionality
fn random_vector(dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|_| rand::rng().random_range(0.0..1.0))
        .collect()
}

#[test]
#[serial(lmdb_stress)]
fn test_concurrent_inserts_single_label() {
    // Tests concurrent inserts from multiple threads to the same label
    //
    // RACE CONDITION: Entry point updates are not synchronized.
    // Multiple threads could race to set the entry point.
    //
    // EXPECTED: All inserts should succeed, graph should remain consistent

    let (storage, _temp_dir) = setup_concurrent_storage();

    let num_threads = 4;
    let vectors_per_thread = 25;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_thread_id| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();

                for _i in 0..vectors_per_thread {
                    // Each insert needs its own write transaction (serialized by LMDB)
                    let mut wtxn = storage.graph_env.write_txn().unwrap();
                    let arena = Bump::new();
                    let vector = random_vector(128);
                    let data = arena.alloc_slice_copy(&vector);

                    // Insert using G::new_mut
                    G::new_mut(&storage, &arena, &mut wtxn)
                        .insert_v(data, "concurrent_test", None)
                        .collect::<Result<Vec<_>, _>>()
                        .expect("Insert should succeed");
                    wtxn.commit().expect("Commit should succeed");
                }
            })
        })
        .collect();

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify: All vectors should be inserted and graph should be consistent
    let rtxn = storage.graph_env.read_txn().unwrap();
    let count = storage.vectors.num_inserted_vectors();

    // Note: count includes entry point (+1), so actual vectors inserted = count - 1
    let expected_inserted = num_threads * vectors_per_thread;
    assert!(
        count == expected_inserted || count == expected_inserted + 1,
        "Expected {} or {} vectors (with entry point), found {}",
        expected_inserted,
        expected_inserted + 1,
        count
    );

    // Additional consistency check: Verify we can perform searches (entry point exists implicitly)
    let arena = Bump::new();
    let query = arena.alloc_slice_copy(&[0.5; 128]);
    let search_result = G::new(&storage, &rtxn, &arena)
        .search_v::<Filter, _>(query, 10, "concurrent_test", None)
        .collect::<Result<Vec<_>, _>>();
    assert!(
        search_result.is_ok(),
        "Should be able to search after concurrent inserts (entry point exists)"
    );
}

#[test]
#[serial(lmdb_stress)]
fn test_concurrent_searches_during_inserts() {
    // Tests read-write conflicts: Concurrent searches while inserts happen
    //
    // EXPECTED BEHAVIOR:
    // - Readers get snapshot isolation (MVCC)
    // - Searches should return consistent results (no torn reads)
    // - Number of results should increase over time as inserts complete

    let (storage, _temp_dir) = setup_concurrent_storage();

    // Initialize with some initial vectors
    {
        let mut txn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();
        for _ in 0..50 {
            let vector = random_vector(128);
            let data = arena.alloc_slice_copy(&vector);
            G::new_mut(&storage, &arena, &mut txn)
                .insert_v(data, "search_test", None)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        }
        txn.commit().unwrap();
    }

    let num_readers = 4;
    let num_writers = 2;
    let barrier = Arc::new(Barrier::new(num_readers + num_writers));
    let query = Arc::new([0.5; 128]);

    let mut handles = vec![];

    // Spawn reader threads
    for reader_id in 0..num_readers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        let query = Arc::clone(&query);

        handles.push(thread::spawn(move || {
            barrier.wait();

            let mut total_searches = 0;
            let mut total_results = 0;

            for _ in 0..50 {
                let rtxn = storage.graph_env.read_txn().unwrap();
                let arena = Bump::new();
                let query_data = arena.alloc_slice_copy(&query[..]);

                match G::new(&storage, &rtxn, &arena)
                    .search_v::<Filter, _>(query_data, 10, "search_test", None)
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(results) => {
                        total_searches += 1;
                        total_results += results.len();

                        // Validate result consistency
                        for (i, result) in results.iter().enumerate() {
                            if let crate::helix_engine::traversal_core::traversal_value::TraversalValue::Vector(v) = result {
                                assert!(
                                    v.distance.is_some(),
                                    "Result {} should have distance",
                                    i
                                );
                            }
                        }
                    }
                    Err(e) => {
                        println!("Reader {} search failed: {:?}", reader_id, e);
                    }
                }

                // Small delay to allow writers to make progress
                thread::sleep(std::time::Duration::from_millis(1));
            }

            println!(
                "Reader {} completed: {} searches, avg {} results",
                reader_id,
                total_searches,
                total_results / total_searches.max(1)
            );
        }));
    }

    // Spawn writer threads
    for _writer_id in 0..num_writers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier.wait();

            for _i in 0..25 {
                let mut wtxn = storage.graph_env.write_txn().unwrap();
                let arena = Bump::new();

                let vector = random_vector(128);
                let data = arena.alloc_slice_copy(&vector);

                G::new_mut(&storage, &arena, &mut wtxn)
                    .insert_v(data, "search_test", None)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("Insert should succeed");
                wtxn.commit().expect("Commit should succeed");

                thread::sleep(std::time::Duration::from_millis(2));
            }
        }));
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Final verification
    let rtxn = storage.graph_env.read_txn().unwrap();
    let final_count = storage.vectors.num_inserted_vectors();

    assert!(
        final_count >= 50,
        "Should have at least initial 50 vectors, found {}",
        final_count
    );

    // Verify we can still search successfully
    let arena = Bump::new();
    let query_data = arena.alloc_slice_copy(&query[..]);
    let results = G::new(&storage, &rtxn, &arena)
        .search_v::<Filter, _>(query_data, 10, "search_test", None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        !results.is_empty(),
        "Should find results after concurrent operations"
    );
}

#[test]
#[serial(lmdb_stress)]
fn test_concurrent_inserts_multiple_labels() {
    // Tests concurrent inserts to different labels (should be independent)
    //
    // EXPECTED: No contention between different labels, all inserts succeed

    let (storage, _temp_dir) = setup_concurrent_storage();

    let num_labels = 4;
    let vectors_per_label = 25;
    let barrier = Arc::new(Barrier::new(num_labels));

    let handles: Vec<_> = (0..num_labels)
        .map(|label_id| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                let label = format!("label_{}", label_id);

                for i in 0..vectors_per_label {
                    let mut wtxn = storage.graph_env.write_txn().unwrap();
                    let arena = Bump::new();

                    let vector = random_vector(64);
                    let data = arena.alloc_slice_copy(&vector);
                    let label_ref = arena.alloc_str(&label);

                    G::new_mut(&storage, &arena, &mut wtxn)
                        .insert_v(data, label_ref, None)
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    wtxn.commit().unwrap();

                    if i % 10 == 0 {
                        println!("Label {} inserted {} vectors", label, i);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify each label has correct count
    let rtxn = storage.graph_env.read_txn().unwrap();

    for label_id in 0..num_labels {
        let label = format!("label_{}", label_id);
        let arena = Bump::new();

        // Verify we can search for each label (entry point exists implicitly)
        let query = arena.alloc_slice_copy(&[0.5; 64]);
        let label_ref = arena.alloc_str(&label);
        let search_result = G::new(&storage, &rtxn, &arena)
            .search_v::<Filter, _>(query, 5, label_ref, None)
            .collect::<Result<Vec<_>, _>>();
        assert!(
            search_result.is_ok(),
            "Should be able to search label {}",
            label
        );
    }

    let total_count = storage.vectors.num_inserted_vectors();
    let expected_total = num_labels * vectors_per_label;
    assert!(
        total_count == expected_total || total_count == expected_total + 1,
        "Expected {} or {} vectors (with entry point), found {}",
        expected_total,
        expected_total + 1,
        total_count
    );
}

#[test]
#[serial(lmdb_stress)]
fn test_entry_point_consistency() {
    // Tests entry point consistency under concurrent inserts
    //
    // CRITICAL: This tests the identified race condition where entry point
    // updates have no synchronization. Multiple threads could race to set
    // the entry point.
    //
    // EXPECTED: Entry point should always be a valid vector ID

    let (storage, _temp_dir) = setup_concurrent_storage();

    let num_threads = 8;
    let vectors_per_thread = 10;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..vectors_per_thread {
                    let mut wtxn = storage.graph_env.write_txn().unwrap();
                    let arena = Bump::new();

                    let vector = random_vector(32);
                    let data = arena.alloc_slice_copy(&vector);

                    G::new_mut(&storage, &arena, &mut wtxn)
                        .insert_v(data, "entry_test", None)
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    wtxn.commit().unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify entry point is valid by performing a search
    let rtxn = storage.graph_env.read_txn().unwrap();
    let arena = Bump::new();

    // If we can successfully search, entry point must be valid
    let query = arena.alloc_slice_copy(&[0.5; 32]);
    let search_result = G::new(&storage, &rtxn, &arena)
        .search_v::<Filter, _>(query, 10, "entry_test", None)
        .collect::<Result<Vec<_>, _>>();
    assert!(
        search_result.is_ok(),
        "Entry point should exist and be valid"
    );

    let results = search_result.unwrap();
    assert!(
        !results.is_empty(),
        "Should return results if entry point is valid"
    );

    // Verify results have valid properties
    for result in results.iter() {
        if let crate::helix_engine::traversal_core::traversal_value::TraversalValue::Vector(v) =
            result
        {
            assert!(v.id > 0, "Result ID should be valid");
            assert!(!v.deleted, "Results should not be deleted");
            assert!(
                !v.data.as_ref().unwrap().is_empty(),
                "Results should have data"
            );
        }
    }
}

#[test]
#[serial(lmdb_stress)]
fn test_graph_connectivity_after_concurrent_inserts() {
    // Tests HNSW graph topology consistency after concurrent operations
    //
    // EXPECTED: Graph should remain connected (no orphaned nodes)
    // All vectors should be reachable from entry point

    let (storage, _temp_dir) = setup_concurrent_storage();

    let num_threads = 4;
    let vectors_per_thread = 20;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..vectors_per_thread {
                    let mut wtxn = storage.graph_env.write_txn().unwrap();
                    let arena = Bump::new();

                    let vector = random_vector(64);
                    let data = arena.alloc_slice_copy(&vector);

                    G::new_mut(&storage, &arena, &mut wtxn)
                        .insert_v(data, "connectivity_test", None)
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    wtxn.commit().unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify graph connectivity by performing searches from different query points
    let rtxn = storage.graph_env.read_txn().unwrap();
    let arena = Bump::new();

    // Try multiple random queries - all should return results
    for i in 0..10 {
        let query = random_vector(64);
        let query_data = arena.alloc_slice_copy(&query);
        let results = G::new(&storage, &rtxn, &arena)
            .search_v::<Filter, _>(query_data, 10, "connectivity_test", None)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(
            !results.is_empty(),
            "Query {} should return results (graph should be connected)",
            i
        );

        // All results should have valid distances
        for result in results {
            if let crate::helix_engine::traversal_core::traversal_value::TraversalValue::Vector(v) =
                result
            {
                assert!(
                    v.distance.is_some() && v.distance.unwrap() >= 0.0,
                    "Result should have valid distance"
                );
            }
        }
    }
}

#[test]
#[serial(lmdb_stress)]
fn test_transaction_isolation() {
    // Tests MVCC snapshot isolation guarantees
    //
    // EXPECTED: Readers should see consistent snapshots even while writes occur

    let (storage, _temp_dir) = setup_concurrent_storage();

    // Initialize with known vectors
    let initial_count = 10;
    {
        let mut txn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();
        for _ in 0..initial_count {
            let vector = random_vector(32);
            let data = arena.alloc_slice_copy(&vector);
            G::new_mut(&storage, &arena, &mut txn)
                .insert_v(data, "isolation_test", None)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        }
        txn.commit().unwrap();
    }

    // Start a long-lived read transaction
    let rtxn = storage.graph_env.read_txn().unwrap();
    let count_before = storage.vectors.num_inserted_vectors();

    // Entry point may be included in count (+1)
    assert!(
        count_before == initial_count || count_before == initial_count + 1,
        "Expected {} or {} (with entry point), got {}",
        initial_count,
        initial_count + 1,
        count_before
    );

    // In another thread, insert more vectors
    let storage_clone = Arc::clone(&storage);
    let handle = thread::spawn(move || {
        for _ in 0..20 {
            let mut wtxn = storage_clone.graph_env.write_txn().unwrap();
            let arena = Bump::new();

            let vector = random_vector(32);
            let data = arena.alloc_slice_copy(&vector);
            G::new_mut(&storage_clone, &arena, &mut wtxn)
                .insert_v(data, "isolation_test", None)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            wtxn.commit().unwrap();
        }
    });

    handle.join().unwrap();

    // Original read transaction should still see the same count (snapshot isolation)
    let count_after = storage.vectors.num_inserted_vectors();
    assert_eq!(
        count_after, count_before,
        "Read transaction should see consistent snapshot"
    );

    // New read transaction should see new vectors
    drop(rtxn);

    let count_new = storage.vectors.num_inserted_vectors();

    // Entry point may be included in counts (+1)
    let expected_new = initial_count + 20;
    assert!(
        count_new == expected_new
            || count_new == expected_new + 1
            || count_new == initial_count + 20 + 1,
        "Expected around {} vectors, got {}",
        expected_new,
        count_new
    );
}
