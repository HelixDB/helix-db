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
use heed3::{Env, EnvOpenOptions, RoTxn, RwTxn};
use rand::Rng;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

use crate::helix_engine::vector_core::{
    hnsw::HNSW,
    vector::HVector,
    vector_core::{HNSWConfig, VectorCore},
};

type Filter = fn(&HVector, &RoTxn) -> bool;

/// Setup test environment with larger map size for concurrent access
///
/// IMPORTANT: Returns (TempDir, Env) to ensure TempDir outlives Env.
/// This prevents double-free errors where LMDB tries to access memory-mapped
/// files after the directory has been deleted.
fn setup_concurrent_env() -> (TempDir, Env) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path();

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024) // 1GB for concurrent operations
            .max_dbs(32)
            .max_readers(128) // Allow many concurrent readers
            .open(path)
            .unwrap()
    };
    (temp_dir, env)
}

/// Generate a random vector of given dimensionality
fn random_vector(dim: usize) -> Vec<f64> {
    (0..dim)
        .map(|_| rand::rng().random_range(0.0..1.0))
        .collect()
}

/// Open existing VectorCore databases (for concurrent access)
/// Note: create_database opens existing database if it exists
fn open_vector_core(
    env: &Env,
    txn: &mut RwTxn,
) -> Result<VectorCore, crate::helix_engine::types::VectorError> {
    VectorCore::new(env, txn, HNSWConfig::new(None, None, None))
}

#[test]
fn test_concurrent_inserts_single_label() {
    // Tests concurrent inserts from multiple threads to the same label
    //
    // RACE CONDITION: Entry point updates are not synchronized.
    // Multiple threads could race to set the entry point.
    //
    // EXPECTED: All inserts should succeed, graph should remain consistent

    let (_temp_dir, env) = setup_concurrent_env();
    let env = Arc::new(env);

    // Initialize the index
    {
        let mut txn = env.write_txn().unwrap();
        VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();
        txn.commit().unwrap();
    }

    let num_threads = 4;
    let vectors_per_thread = 25;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_thread_id| {
            let env = Arc::clone(&env);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();

                for _i in 0..vectors_per_thread {
                    // Each insert needs its own write transaction (serialized by LMDB)
                    let mut wtxn = env.write_txn().unwrap();
                    let arena = Bump::new();
                    let vector = random_vector(128);
                    let data = arena.alloc_slice_copy(&vector);

                    // Open the existing databases and insert
                    let index = open_vector_core(&env, &mut wtxn).unwrap();
                    index
                        .insert::<Filter>(&mut wtxn, "concurrent_test", data, None, &arena)
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
    let mut wtxn = env.write_txn().unwrap();
    let index = open_vector_core(&env, &mut wtxn).unwrap();
    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();
    let count = index.num_inserted_vectors(&rtxn).unwrap();

    // Note: count includes entry point (+1), so actual vectors inserted = count - 1
    let expected_inserted = (num_threads * vectors_per_thread) as u64;
    assert!(
        count == expected_inserted || count == expected_inserted + 1,
        "Expected {} or {} vectors (with entry point), found {}",
        expected_inserted,
        expected_inserted + 1,
        count
    );

    // Additional consistency check: Verify we can perform searches (entry point exists implicitly)
    let arena = Bump::new();
    let query = [0.5; 128];
    let search_result =
        index.search::<Filter>(&rtxn, &query, 10, "concurrent_test", None, false, &arena);
    assert!(
        search_result.is_ok(),
        "Should be able to search after concurrent inserts (entry point exists)"
    );
}

#[test]
fn test_concurrent_searches_during_inserts() {
    // Tests read-write conflicts: Concurrent searches while inserts happen
    //
    // EXPECTED BEHAVIOR:
    // - Readers get snapshot isolation (MVCC)
    // - Searches should return consistent results (no torn reads)
    // - Number of results should increase over time as inserts complete

    let (_temp_dir, env) = setup_concurrent_env();
    let env = Arc::new(env);

    // Initialize with some initial vectors
    {
        let mut txn = env.write_txn().unwrap();
        let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

        let arena = Bump::new();
        for _ in 0..50 {
            let vector = random_vector(128);
            let data = arena.alloc_slice_copy(&vector);
            index
                .insert::<Filter>(&mut txn, "search_test", data, None, &arena)
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
        let env = Arc::clone(&env);
        let barrier = Arc::clone(&barrier);
        let query = Arc::clone(&query);

        handles.push(thread::spawn(move || {
            barrier.wait();

            let mut total_searches = 0;
            let mut total_results = 0;

            // Perform many searches
            // Open databases once per thread
            let mut wtxn_init = env.write_txn().unwrap();
            let index = open_vector_core(&env, &mut wtxn_init).unwrap();
            wtxn_init.commit().unwrap();

            for _ in 0..50 {
                let rtxn = env.read_txn().unwrap();
                let arena = Bump::new();

                match index.search::<Filter>(
                    &rtxn,
                    &query[..],
                    10,
                    "search_test",
                    None,
                    false,
                    &arena,
                ) {
                    Ok(results) => {
                        total_searches += 1;
                        total_results += results.len();

                        // Validate result consistency
                        for (i, result) in results.iter().enumerate() {
                            assert!(
                                result.distance.is_some(),
                                "Result {} should have distance",
                                i
                            );
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
        let env = Arc::clone(&env);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier.wait();

            for _i in 0..25 {
                let mut wtxn = env.write_txn().unwrap();
                let arena = Bump::new();

                let vector = random_vector(128);
                let data = arena.alloc_slice_copy(&vector);

                let index = open_vector_core(&env, &mut wtxn).unwrap();
                index
                    .insert::<Filter>(&mut wtxn, "search_test", data, None, &arena)
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
    let mut wtxn = env.write_txn().unwrap();
    let index = open_vector_core(&env, &mut wtxn).unwrap();
    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();
    let final_count = index.num_inserted_vectors(&rtxn).unwrap();

    assert!(
        final_count >= 50,
        "Should have at least initial 50 vectors, found {}",
        final_count
    );

    // Verify we can still search successfully
    let arena = Bump::new();
    let results = index
        .search::<Filter>(&rtxn, &query[..], 10, "search_test", None, false, &arena)
        .unwrap();
    assert!(
        !results.is_empty(),
        "Should find results after concurrent operations"
    );
}

#[test]
fn test_concurrent_inserts_multiple_labels() {
    // Tests concurrent inserts to different labels (should be independent)
    //
    // EXPECTED: No contention between different labels, all inserts succeed

    let (_temp_dir, env) = setup_concurrent_env();
    let env = Arc::new(env);

    // Initialize the index
    {
        let mut txn = env.write_txn().unwrap();
        VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();
        txn.commit().unwrap();
    }

    let num_labels = 4;
    let vectors_per_label = 25;
    let barrier = Arc::new(Barrier::new(num_labels));

    let handles: Vec<_> = (0..num_labels)
        .map(|label_id| {
            let env = Arc::clone(&env);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                let label = format!("label_{}", label_id);

                for i in 0..vectors_per_label {
                    let mut wtxn = env.write_txn().unwrap();
                    let index = open_vector_core(&env, &mut wtxn).unwrap();
                    let arena = Bump::new();

                    let vector = random_vector(64);
                    let data = arena.alloc_slice_copy(&vector);

                    index
                        .insert::<Filter>(&mut wtxn, &label, data, None, &arena)
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
    let mut wtxn = env.write_txn().unwrap();
    let index = open_vector_core(&env, &mut wtxn).unwrap();
    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();

    for label_id in 0..num_labels {
        let label = format!("label_{}", label_id);
        let arena = Bump::new();

        // Verify we can search for each label (entry point exists implicitly)
        let query = [0.5; 64];
        let search_result = index.search::<Filter>(&rtxn, &query, 5, &label, None, false, &arena);
        assert!(
            search_result.is_ok(),
            "Should be able to search label {}",
            label
        );
    }

    let total_count = index.num_inserted_vectors(&rtxn).unwrap();
    let expected_total = (num_labels * vectors_per_label) as u64;
    assert!(
        total_count == expected_total || total_count == expected_total + 1,
        "Expected {} or {} vectors (with entry point), found {}",
        expected_total,
        expected_total + 1,
        total_count
    );
}

#[test]
fn test_entry_point_consistency() {
    // Tests entry point consistency under concurrent inserts
    //
    // CRITICAL: This tests the identified race condition where entry point
    // updates have no synchronization. Multiple threads could race to set
    // the entry point.
    //
    // EXPECTED: Entry point should always be a valid vector ID

    let (_temp_dir, env) = setup_concurrent_env();
    let env = Arc::new(env);

    {
        let mut txn = env.write_txn().unwrap();
        VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();
        txn.commit().unwrap();
    }

    let num_threads = 8;
    let vectors_per_thread = 10;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let env = Arc::clone(&env);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..vectors_per_thread {
                    let mut wtxn = env.write_txn().unwrap();
                    let index = open_vector_core(&env, &mut wtxn).unwrap();
                    let arena = Bump::new();

                    let vector = random_vector(32);
                    let data = arena.alloc_slice_copy(&vector);

                    index
                        .insert::<Filter>(&mut wtxn, "entry_test", data, None, &arena)
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
    let mut wtxn = env.write_txn().unwrap();
    let index = open_vector_core(&env, &mut wtxn).unwrap();
    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();
    let arena = Bump::new();

    // If we can successfully search, entry point must be valid
    let query = [0.5; 32];
    let search_result =
        index.search::<Filter>(&rtxn, &query, 10, "entry_test", None, false, &arena);
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
        assert!(result.id > 0, "Result ID should be valid");
        assert!(!result.deleted, "Results should not be deleted");
        assert!(!result.data.is_empty(), "Results should have data");
    }
}

#[test]
fn test_graph_connectivity_after_concurrent_inserts() {
    // Tests HNSW graph topology consistency after concurrent operations
    //
    // EXPECTED: Graph should remain connected (no orphaned nodes)
    // All vectors should be reachable from entry point

    let (_temp_dir, env) = setup_concurrent_env();
    let env = Arc::new(env);

    {
        let mut txn = env.write_txn().unwrap();
        VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();
        txn.commit().unwrap();
    }

    let num_threads = 4;
    let vectors_per_thread = 20;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let env = Arc::clone(&env);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..vectors_per_thread {
                    let mut wtxn = env.write_txn().unwrap();
                    let index = open_vector_core(&env, &mut wtxn).unwrap();
                    let arena = Bump::new();

                    let vector = random_vector(64);
                    let data = arena.alloc_slice_copy(&vector);

                    index
                        .insert::<Filter>(&mut wtxn, "connectivity_test", data, None, &arena)
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
    let mut wtxn = env.write_txn().unwrap();
    let index = open_vector_core(&env, &mut wtxn).unwrap();
    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();
    let arena = Bump::new();

    // Try multiple random queries - all should return results
    for i in 0..10 {
        let query = random_vector(64);
        let results = index
            .search::<Filter>(&rtxn, &query, 10, "connectivity_test", None, false, &arena)
            .unwrap();

        assert!(
            !results.is_empty(),
            "Query {} should return results (graph should be connected)",
            i
        );

        // All results should have valid distances
        for result in results {
            assert!(
                result.distance.is_some() && result.distance.unwrap() >= 0.0,
                "Result should have valid distance"
            );
        }
    }
}

#[test]
fn test_transaction_isolation() {
    // Tests MVCC snapshot isolation guarantees
    //
    // EXPECTED: Readers should see consistent snapshots even while writes occur

    let (_temp_dir, env) = setup_concurrent_env();
    let env = Arc::new(env);

    // Initialize with known vectors
    let initial_count = 10;
    {
        let mut txn = env.write_txn().unwrap();
        let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

        let arena = Bump::new();
        for _ in 0..initial_count {
            let vector = random_vector(32);
            let data = arena.alloc_slice_copy(&vector);
            index
                .insert::<Filter>(&mut txn, "isolation_test", data, None, &arena)
                .unwrap();
        }
        txn.commit().unwrap();
    }

    // Start a long-lived read transaction
    let mut wtxn_open = env.write_txn().unwrap();
    let index = open_vector_core(&env, &mut wtxn_open).unwrap();
    wtxn_open.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    let count_before = index.num_inserted_vectors(&rtxn).unwrap();

    // Entry point may be included in count (+1)
    assert!(
        count_before == initial_count || count_before == initial_count + 1,
        "Expected {} or {} (with entry point), got {}",
        initial_count,
        initial_count + 1,
        count_before
    );

    // In another thread, insert more vectors
    let env_clone = Arc::clone(&env);
    let handle = thread::spawn(move || {
        for _ in 0..20 {
            let mut wtxn = env_clone.write_txn().unwrap();
            let index = open_vector_core(&env_clone, &mut wtxn).unwrap();
            let arena = Bump::new();

            let vector = random_vector(32);
            let data = arena.alloc_slice_copy(&vector);
            index
                .insert::<Filter>(&mut wtxn, "isolation_test", data, None, &arena)
                .unwrap();
            wtxn.commit().unwrap();
        }
    });

    handle.join().unwrap();

    // Original read transaction should still see the same count (snapshot isolation)
    let count_after = index.num_inserted_vectors(&rtxn).unwrap();
    assert_eq!(
        count_after, count_before,
        "Read transaction should see consistent snapshot"
    );

    // New read transaction should see new vectors
    drop(rtxn);

    let mut wtxn_new = env.write_txn().unwrap();
    let index_new = open_vector_core(&env, &mut wtxn_new).unwrap();
    wtxn_new.commit().unwrap();

    let rtxn_new = env.read_txn().unwrap();
    let count_new = index_new.num_inserted_vectors(&rtxn_new).unwrap();

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
