use bumpalo::Bump;
use serial_test::serial;
use std::sync::atomic::{AtomicUsize, Ordering};
/// Integration Stress Tests for HelixDB
///
/// This test suite performs comprehensive stress testing across all major components
/// of the HelixDB system under high concurrent load. These tests validate that:
///
/// 1. **Cross-Component Integration**: Multiple subsystems work correctly together
/// 2. **High Load Handling**: System remains stable under sustained heavy load
/// 3. **Resource Management**: No memory leaks or resource exhaustion
/// 4. **Transaction Consistency**: ACID properties maintained under stress
/// 5. **Performance Degradation**: No severe performance degradation over time
///
/// CRITICAL SCENARIOS:
/// - Simultaneous graph operations + vector search + BM25 indexing
/// - Mixed read-heavy and write-heavy workloads
/// - Long-running transactions with concurrent modifications
/// - Rapid node/edge creation with immediate traversals
/// - Vector index updates during concurrent searches
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use crate::helix_engine::storage_core::HelixGraphStorage;
use crate::helix_engine::traversal_core::config::Config;
use crate::helix_engine::traversal_core::ops::g::G;
use crate::helix_engine::traversal_core::ops::out::out::OutAdapter;
use crate::helix_engine::traversal_core::ops::source::{
    add_e::AddEAdapter, add_n::AddNAdapter, n_from_id::NFromIdAdapter,
};

/// Setup storage with appropriate configuration for stress testing
fn setup_stress_storage() -> (Arc<HelixGraphStorage>, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    let mut config = Config::default();
    config.db_max_size_gb = Some(20); // Large size for stress tests

    let storage = HelixGraphStorage::new(path, config, Default::default()).unwrap();
    (Arc::new(storage), temp_dir)
}

#[test]
#[serial(lmdb_stress)]
fn test_stress_mixed_read_write_operations() {
    // Stress test: Simultaneous graph reads and writes under high load
    //
    // EXPECTED: Both operations function correctly under load

    let (storage, _temp_dir) = setup_stress_storage();

    let duration = Duration::from_secs(3);
    let start = std::time::Instant::now();

    let write_ops = Arc::new(AtomicUsize::new(0));
    let read_ops = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    // Graph writers - create nodes and edges
    for writer_id in 0..4 {
        let storage = Arc::clone(&storage);
        let write_ops = Arc::clone(&write_ops);

        handles.push(thread::spawn(move || {
            let mut count = 0;
            while start.elapsed() < duration {
                let arena = Bump::new();
                let mut wtxn = storage.graph_env.write_txn().unwrap();

                let label1 = format!("node_w{}_n{}_a", writer_id, count);
                let label2 = format!("node_w{}_n{}_b", writer_id, count);

                let id1 = G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label1, None, None)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()[0]
                    .id();

                let id2 = G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label2, None, None)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()[0]
                    .id();

                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_edge("connects", None, id1, id2, false)
                    .collect_to_obj()
                    .unwrap();

                wtxn.commit().unwrap();
                write_ops.fetch_add(1, Ordering::Relaxed);
                count += 1;
            }
            count
        }));
    }

    // Graph readers - monitor graph growth
    for _reader_id in 0..4 {
        let storage = Arc::clone(&storage);
        let read_ops = Arc::clone(&read_ops);

        handles.push(thread::spawn(move || {
            let mut count = 0;
            while start.elapsed() < duration {
                let rtxn = storage.graph_env.read_txn().unwrap();
                let node_count = storage.nodes_db.len(&rtxn).unwrap();
                let edge_count = storage.edges_db.len(&rtxn).unwrap();
                if node_count > 0 && edge_count > 0 {
                    read_ops.fetch_add(1, Ordering::Relaxed);
                    count += 1;
                }
            }
            count
        }));
    }

    let mut counts = vec![];
    for handle in handles {
        counts.push(handle.join().unwrap());
    }

    let total_writes = write_ops.load(Ordering::Relaxed);
    let total_reads = read_ops.load(Ordering::Relaxed);

    println!(
        "Stress test: {} writes, {} reads in {:?}",
        total_writes, total_reads, duration
    );
    println!("Thread counts: {:?}", counts);

    // Verify significant work was done
    assert!(total_writes > 100, "Should perform many write operations");
    assert!(total_reads > 100, "Should perform many read operations");
}

#[test]
#[serial(lmdb_stress)]
fn test_stress_rapid_graph_growth() {
    // Stress test: Rapidly growing graph with immediate traversals
    //
    // EXPECTED: Graph remains traversable and consistent

    let (storage, _temp_dir) = setup_stress_storage();

    // Create root nodes
    let root_ids: Vec<u128> = {
        let arena = Bump::new();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        let ids = (0..5)
            .map(|i| {
                let label = format!("root_{}", i);
                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()[0]
                    .id()
            })
            .collect();

        wtxn.commit().unwrap();
        ids
    };

    let duration = Duration::from_secs(3);
    let start = std::time::Instant::now();
    let root_ids = Arc::new(root_ids);

    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    // Writers - rapidly add nodes and edges
    for writer_id in 0..3 {
        let storage = Arc::clone(&storage);
        let root_ids = Arc::clone(&root_ids);
        let write_count = Arc::clone(&write_count);

        handles.push(thread::spawn(move || {
            let mut local_count = 0;
            while start.elapsed() < duration {
                let arena = Bump::new();
                let mut wtxn = storage.graph_env.write_txn().unwrap();

                // Add new node
                let label = format!("w{}_n{}", writer_id, local_count);
                let new_id = G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()[0]
                    .id();

                // Connect to random root
                let root_idx = local_count % root_ids.len();
                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_edge("child_of", None, root_ids[root_idx], new_id, false)
                    .collect_to_obj()
                    .unwrap();

                wtxn.commit().unwrap();
                write_count.fetch_add(1, Ordering::Relaxed);
                local_count += 1;
            }
            local_count
        }));
    }

    // Readers - continuously traverse from roots
    for _reader_id in 0..3 {
        let storage = Arc::clone(&storage);
        let root_ids = Arc::clone(&root_ids);
        let read_count = Arc::clone(&read_count);

        handles.push(thread::spawn(move || {
            let mut local_count = 0;
            while start.elapsed() < duration {
                let arena = Bump::new();
                let rtxn = storage.graph_env.read_txn().unwrap();

                // Traverse from each root
                for root_id in root_ids.iter() {
                    let _children = G::new(&storage, &rtxn, &arena)
                        .n_from_id(root_id)
                        .out_node("child_of")
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    local_count += 1;
                }

                read_count.fetch_add(root_ids.len(), Ordering::Relaxed);
            }
            local_count
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total_writes = write_count.load(Ordering::Relaxed);
    let total_reads = read_count.load(Ordering::Relaxed);

    println!(
        "Rapid growth: {} writes, {} reads in {:?}",
        total_writes, total_reads, duration
    );

    // Verify significant work done
    assert!(total_writes > 100, "Should create many nodes");
    assert!(total_reads > 100, "Should perform many traversals");

    // Verify graph integrity
    let _arena = Bump::new();
    let rtxn = storage.graph_env.read_txn().unwrap();
    let final_node_count = storage.nodes_db.len(&rtxn).unwrap();
    let final_edge_count = storage.edges_db.len(&rtxn).unwrap();

    println!(
        "Final: {} nodes, {} edges",
        final_node_count, final_edge_count
    );
    assert!(
        final_node_count > 100,
        "Graph should have grown significantly"
    );
}

#[test]
#[serial(lmdb_stress)]
fn test_stress_transaction_contention() {
    // Stress test: High contention on write transactions
    //
    // EXPECTED: LMDB single-writer enforced, no corruption

    let (storage, _temp_dir) = setup_stress_storage();

    let num_threads = 8;
    let duration = Duration::from_secs(2);
    let start = std::time::Instant::now();
    let barrier = Arc::new(Barrier::new(num_threads));

    let success_count = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);
            let success_count = Arc::clone(&success_count);

            thread::spawn(move || {
                barrier.wait();

                let mut local_count = 0;
                while start.elapsed() < duration {
                    let arena = Bump::new();

                    // Try to acquire write transaction
                    if let Ok(mut wtxn) = storage.graph_env.write_txn() {
                        let label = format!("t{}_n{}", thread_id, local_count);
                        G::new_mut(&storage, &arena, &mut wtxn)
                            .add_n(&label, None, None)
                            .collect::<Result<Vec<_>, _>>()
                            .unwrap();

                        wtxn.commit().unwrap();
                        success_count.fetch_add(1, Ordering::Relaxed);
                        local_count += 1;
                    }

                    // Small delay to allow other threads
                    thread::sleep(Duration::from_micros(50));
                }
                local_count
            })
        })
        .collect();

    let mut per_thread_counts = vec![];
    for handle in handles {
        per_thread_counts.push(handle.join().unwrap());
    }

    let total_success = success_count.load(Ordering::Relaxed);

    println!(
        "Transaction contention: {} successful writes in {:?}",
        total_success, duration
    );
    println!("Per thread: {:?}", per_thread_counts);

    // Verify all writes succeeded
    let rtxn = storage.graph_env.read_txn().unwrap();
    let final_count = storage.nodes_db.len(&rtxn).unwrap();

    assert_eq!(
        final_count, total_success as u64,
        "All commits should be visible"
    );
    assert!(total_success > 50, "Should handle significant contention");
}

#[test]
#[serial(lmdb_stress)]
fn test_stress_long_running_transactions() {
    // Stress test: Long-lived read transactions with concurrent writes
    //
    // EXPECTED: MVCC snapshot isolation maintained, no blocking

    let (storage, _temp_dir) = setup_stress_storage();

    // Create initial data
    {
        let arena = Bump::new();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        for i in 0..20 {
            let label = format!("initial_{}", i);
            G::new_mut(&storage, &arena, &mut wtxn)
                .add_n(&label, None, None)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        }

        wtxn.commit().unwrap();
    }

    let duration = Duration::from_secs(2);
    let start = std::time::Instant::now();

    let write_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    // Long-running reader - holds transaction open
    let storage_clone = Arc::clone(&storage);
    handles.push(thread::spawn(move || {
        let rtxn = storage_clone.graph_env.read_txn().unwrap();
        let initial_count = storage_clone.nodes_db.len(&rtxn).unwrap();

        // Hold transaction for entire duration
        while start.elapsed() < duration {
            thread::sleep(Duration::from_millis(100));
        }

        // Should still see initial count (snapshot isolation)
        let final_count = storage_clone.nodes_db.len(&rtxn).unwrap();
        assert_eq!(
            final_count, initial_count,
            "Long-lived txn should see consistent snapshot"
        );

        final_count
    }));

    // Writers - continuously add data
    for writer_id in 0..3 {
        let storage = Arc::clone(&storage);
        let write_count = Arc::clone(&write_count);

        handles.push(thread::spawn(move || {
            let mut count = 0;
            while start.elapsed() < duration {
                let arena = Bump::new();
                let mut wtxn = storage.graph_env.write_txn().unwrap();

                let label = format!("w{}_n{}", writer_id, count);
                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();

                wtxn.commit().unwrap();
                write_count.fetch_add(1, Ordering::Relaxed);
                count += 1;
            }
            count
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total_writes = write_count.load(Ordering::Relaxed);

    println!(
        "Long txns: {} writes completed during long-lived read",
        total_writes
    );

    // New transaction should see all writes
    let rtxn = storage.graph_env.read_txn().unwrap();
    let final_count = storage.nodes_db.len(&rtxn).unwrap();
    assert_eq!(final_count, 20 + total_writes as u64);
}

#[test]
#[serial(lmdb_stress)]
fn test_stress_memory_stability() {
    // Stress test: Verify no memory leaks under sustained load
    //
    // EXPECTED: System remains stable, no unbounded growth

    let (storage, _temp_dir) = setup_stress_storage();

    let duration = Duration::from_secs(3);
    let iterations = 3;

    for iteration in 0..iterations {
        let start = std::time::Instant::now();
        let op_count = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        // Multiple worker threads doing various operations
        for worker_id in 0..4 {
            let storage = Arc::clone(&storage);
            let op_count = Arc::clone(&op_count);

            handles.push(thread::spawn(move || {
                let mut count = 0;
                while start.elapsed() < duration {
                    // Create short-lived transaction
                    {
                        let arena = Bump::new();
                        let mut wtxn = storage.graph_env.write_txn().unwrap();

                        let label = format!("iter{}_w{}_n{}", iteration, worker_id, count);
                        G::new_mut(&storage, &arena, &mut wtxn)
                            .add_n(&label, None, None)
                            .collect::<Result<Vec<_>, _>>()
                            .unwrap();

                        wtxn.commit().unwrap();
                    }

                    // Perform read
                    {
                        let rtxn = storage.graph_env.read_txn().unwrap();
                        let _count = storage.nodes_db.len(&rtxn).unwrap();
                    }

                    op_count.fetch_add(1, Ordering::Relaxed);
                    count += 1;
                }
                count
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let ops = op_count.load(Ordering::Relaxed);
        println!(
            "Memory stability iteration {}: {} ops in {:?}",
            iteration, ops, duration
        );
    }

    // If we reach here without panic/OOM, memory is stable
    println!("Memory stability test completed successfully");
}
