/// Concurrent access tests for Traversal Operations
///
/// This test suite validates thread safety and concurrent operation correctness
/// for graph traversal operations. Key areas tested:
///
/// 1. **Concurrent Graph Modifications**: Multiple writers adding nodes/edges
/// 2. **Read-Write Conflicts**: Readers traversing while writers modify graph
/// 3. **Transaction Isolation**: MVCC snapshot isolation during traversals
/// 4. **Topology Consistency**: Graph structure remains valid under concurrent operations
///
/// CRITICAL ISSUES BEING TESTED:
/// - Traversal iterators maintain consistent view during concurrent writes
/// - MVCC ensures readers see consistent graph snapshots
/// - Edge creation/deletion doesn't corrupt graph topology
/// - No race conditions in neighbor list updates

use std::sync::{Arc, Barrier};
use std::thread;
use bumpalo::Bump;
use serial_test::serial;
use tempfile::TempDir;

use crate::helix_engine::storage_core::HelixGraphStorage;
use crate::helix_engine::traversal_core::config::Config;
use crate::helix_engine::traversal_core::ops::g::G;
use crate::helix_engine::traversal_core::ops::source::{
    add_n::AddNAdapter,
    add_e::AddEAdapter,
    n_from_id::NFromIdAdapter,
};
use crate::helix_engine::traversal_core::ops::out::out::OutAdapter;
use crate::helix_engine::traversal_core::ops::in_::in_::InAdapter;

/// Setup storage for concurrent testing
fn setup_concurrent_storage() -> (TempDir, Arc<HelixGraphStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    let mut config = Config::default();
    config.db_max_size_gb = Some(10);

    let storage = HelixGraphStorage::new(path, config, Default::default()).unwrap();
    (temp_dir, Arc::new(storage))
}

#[test]
#[serial(lmdb_stress)]
fn test_concurrent_node_additions() {
    // Tests multiple threads adding nodes concurrently
    //
    // EXPECTED: All nodes created successfully, no ID collisions

    let (_temp_dir, storage) = setup_concurrent_storage();

    let num_threads = 4;
    let nodes_per_thread = 25;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                for i in 0..nodes_per_thread {
                    let arena = Bump::new();
                    let mut wtxn = storage.graph_env.write_txn().unwrap();

                    let label = format!("person_t{}_n{}", thread_id, i);
                    G::new_mut(&storage, &arena, &mut wtxn)
                        .add_n(&label, None, None)
                        .collect::<Result<Vec<_>,_>>().unwrap();

                    wtxn.commit().unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify: All nodes created
    let _arena = Bump::new();
    let rtxn = storage.graph_env.read_txn().unwrap();
    let count = storage.nodes_db.len(&rtxn).unwrap();
    assert_eq!(
        count,
        (num_threads * nodes_per_thread) as u64,
        "Expected {} nodes, found {}",
        num_threads * nodes_per_thread,
        count
    );
}

#[test]
#[serial(lmdb_stress)]
fn test_concurrent_edge_additions() {
    // Tests multiple threads adding edges between nodes
    //
    // EXPECTED: All edges created, proper serialization

    let (_temp_dir, storage) = setup_concurrent_storage();

    // Create nodes first
    let node_ids: Vec<u128> = {
        let arena = Bump::new();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        let ids: Vec<u128> = (0..10)
            .map(|i| {
                let label = format!("node_{}", i);
                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>,_>>().unwrap()[0]
                    .id()
            })
            .collect();

        wtxn.commit().unwrap();
        ids
    };

    let num_threads = 4;
    let edges_per_thread = 10;
    let barrier = Arc::new(Barrier::new(num_threads));
    let node_ids = Arc::new(node_ids);

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);
            let node_ids = Arc::clone(&node_ids);

            thread::spawn(move || {
                barrier.wait();

                for i in 0..edges_per_thread {
                    let arena = Bump::new();
                    let mut wtxn = storage.graph_env.write_txn().unwrap();

                    let source_idx = (thread_id * 2) % node_ids.len();
                    let target_idx = (thread_id * 2 + 1) % node_ids.len();

                    let label = format!("knows_t{}_e{}", thread_id, i);
                    G::new_mut(&storage, &arena, &mut wtxn)
                        .add_edge(&label, None, node_ids[source_idx], node_ids[target_idx], false)
                        .collect_to_obj().unwrap();

                    wtxn.commit().unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify: All edges created
    let rtxn = storage.graph_env.read_txn().unwrap();
    let count = storage.edges_db.len(&rtxn).unwrap();
    assert_eq!(
        count,
        (num_threads * edges_per_thread) as u64,
        "Expected {} edges, found {}",
        num_threads * edges_per_thread,
        count
    );
}

#[test]
#[serial(lmdb_stress)]
fn test_concurrent_reads_during_writes() {
    // Tests concurrent traversals while writes are happening
    //
    // EXPECTED: Readers see consistent snapshots (MVCC)

    let (_temp_dir, storage) = setup_concurrent_storage();

    // Create initial graph structure
    let root_id = {
        let arena = Bump::new();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        let root = G::new_mut(&storage, &arena, &mut wtxn)
            .add_n("root", None, None)
            .collect::<Result<Vec<_>,_>>().unwrap()[0]
            .id();

        // Add initial neighbors
        for i in 0..5 {
            let label = format!("initial_{}", i);
            let neighbor_id = G::new_mut(&storage, &arena, &mut wtxn)
                .add_n(&label, None, None)
                .collect::<Result<Vec<_>,_>>().unwrap()[0]
                .id();

            G::new_mut(&storage, &arena, &mut wtxn)
                .add_edge("connects", None, root, neighbor_id, false)
                .collect_to_obj().unwrap();
        }

        wtxn.commit().unwrap();
        root
    };

    let num_readers = 4;
    let num_writers = 2;
    let barrier = Arc::new(Barrier::new(num_readers + num_writers));

    let mut handles = vec![];

    // Spawn readers - traverse graph repeatedly
    for reader_id in 0..num_readers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier.wait();

            let mut total_reads = 0;

            for _ in 0..20 {
                let arena = Bump::new();
                let rtxn = storage.graph_env.read_txn().unwrap();

                // Traverse from root
                let neighbors = G::new(&storage, &rtxn, &arena)
                    .n_from_id(&root_id)
                    .out_node("connects")
                    .collect::<Result<Vec<_>,_>>().unwrap();

                // Should see at least initial neighbors
                assert!(
                    neighbors.len() >= 5,
                    "Reader {} saw only {} neighbors",
                    reader_id,
                    neighbors.len()
                );

                total_reads += 1;
                thread::sleep(std::time::Duration::from_millis(1));
            }

            total_reads
        }));
    }

    // Spawn writers - add more neighbors
    for writer_id in 0..num_writers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier.wait();

            for i in 0..10 {
                let arena = Bump::new();
                let mut wtxn = storage.graph_env.write_txn().unwrap();

                let label = format!("writer_{}_node_{}", writer_id, i);
                let new_node_id = G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>,_>>().unwrap()[0]
                    .id();

                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_edge("connects", None, root_id, new_node_id, false)
                    .collect_to_obj().unwrap();

                wtxn.commit().unwrap();

                thread::sleep(std::time::Duration::from_millis(2));
            }
            0 // Return value to match reader threads
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Final verification - should see all neighbors
    let arena = Bump::new();
    let rtxn = storage.graph_env.read_txn().unwrap();
    let final_neighbors = G::new(&storage, &rtxn, &arena)
        .n_from_id(&root_id)
        .out_node("connects")
        .collect::<Result<Vec<_>,_>>().unwrap();

    let expected_count = 5 + (num_writers * 10);
    assert_eq!(
        final_neighbors.len(),
        expected_count,
        "Expected {} neighbors, found {}",
        expected_count,
        final_neighbors.len()
    );
}

#[test]
#[serial(lmdb_stress)]
fn test_traversal_snapshot_isolation() {
    // Tests that long-lived read transaction sees consistent snapshot
    //
    // EXPECTED: Traversal results don't change during transaction lifetime

    let (_temp_dir, storage) = setup_concurrent_storage();

    // Create initial graph
    let root_id = {
        let arena = Bump::new();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        let root = G::new_mut(&storage, &arena, &mut wtxn)
            .add_n("root", None, None)
            .collect::<Result<Vec<_>,_>>().unwrap()[0]
            .id();

        for i in 0..5 {
            let label = format!("node_{}", i);
            let node_id = G::new_mut(&storage, &arena, &mut wtxn)
                .add_n(&label, None, None)
                .collect::<Result<Vec<_>,_>>().unwrap()[0]
                .id();

            G::new_mut(&storage, &arena, &mut wtxn)
                .add_edge("links", None, root, node_id, false)
                .collect_to_obj().unwrap();
        }

        wtxn.commit().unwrap();
        root
    };

    // Start long-lived read transaction
    let arena = Bump::new();
    let rtxn = storage.graph_env.read_txn().unwrap();
    let initial_neighbors = G::new(&storage, &rtxn, &arena)
        .n_from_id(&root_id)
        .out_node("links")
        .collect::<Result<Vec<_>,_>>().unwrap();
    let initial_count = initial_neighbors.len();
    assert_eq!(initial_count, 5);

    // In another thread, add more neighbors
    let storage_clone = Arc::clone(&storage);
    let handle = thread::spawn(move || {
        for i in 0..10 {
            let arena = Bump::new();
            let mut wtxn = storage_clone.graph_env.write_txn().unwrap();

            let label = format!("new_node_{}", i);
            let new_id = G::new_mut(&storage_clone, &arena, &mut wtxn)
                .add_n(&label, None, None)
                .collect::<Result<Vec<_>,_>>().unwrap()[0]
                .id();

            G::new_mut(&storage_clone, &arena, &mut wtxn)
                .add_edge("links", None, root_id, new_id, false)
                .collect_to_obj().unwrap();

            wtxn.commit().unwrap();
        }
    });

    handle.join().unwrap();

    // Original transaction should still see same count (snapshot isolation)
    let arena2 = Bump::new();
    let current_neighbors = G::new(&storage, &rtxn, &arena2)
        .n_from_id(&root_id)
        .out_node("links")
        .collect::<Result<Vec<_>,_>>().unwrap();

    assert_eq!(
        current_neighbors.len(),
        initial_count,
        "Snapshot isolation violated: count changed from {} to {}",
        initial_count,
        current_neighbors.len()
    );

    drop(rtxn);

    // New transaction should see all neighbors
    let arena3 = Bump::new();
    let rtxn_new = storage.graph_env.read_txn().unwrap();
    let final_neighbors = G::new(&storage, &rtxn_new, &arena3)
        .n_from_id(&root_id)
        .out_node("links")
        .collect::<Result<Vec<_>,_>>().unwrap();
    assert_eq!(final_neighbors.len(), 15);
}

#[test]
#[serial(lmdb_stress)]
fn test_concurrent_bidirectional_traversals() {
    // Tests concurrent out() and in() traversals
    //
    // EXPECTED: Both directions remain consistent

    let (_temp_dir, storage) = setup_concurrent_storage();

    // Create bidirectional graph structure
    let (source_ids, target_ids) = {
        let arena = Bump::new();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        let sources: Vec<u128> = (0..5)
            .map(|i| {
                let label = format!("source_{}", i);
                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>,_>>().unwrap()[0]
                    .id()
            })
            .collect();

        let targets: Vec<u128> = (0..5)
            .map(|i| {
                let label = format!("target_{}", i);
                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>,_>>().unwrap()[0]
                    .id()
            })
            .collect();

        // Create edges: each source -> all targets
        for source_id in &sources {
            for target_id in &targets {
                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_edge("points_to", None, *source_id, *target_id, false)
                    .collect_to_obj().unwrap();
            }
        }

        wtxn.commit().unwrap();
        (sources, targets)
    };

    let num_threads = 4;
    let barrier = Arc::new(Barrier::new(num_threads));
    let source_ids = Arc::new(source_ids);
    let target_ids = Arc::new(target_ids);

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);
            let source_ids = Arc::clone(&source_ids);
            let target_ids = Arc::clone(&target_ids);

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..20 {
                    let arena = Bump::new();
                    let rtxn = storage.graph_env.read_txn().unwrap();

                    if thread_id % 2 == 0 {
                        // Test out() traversals
                        for source_id in source_ids.iter() {
                            let neighbors = G::new(&storage, &rtxn, &arena)
                                .n_from_id(source_id)
                                .out_node("points_to")
                                .collect::<Result<Vec<_>,_>>().unwrap();
                            assert_eq!(neighbors.len(), 5, "Source should have 5 outgoing edges");
                        }
                    } else {
                        // Test in() traversals
                        for target_id in target_ids.iter() {
                            let neighbors = G::new(&storage, &rtxn, &arena)
                                .n_from_id(target_id)
                                .in_node("points_to")
                                .collect::<Result<Vec<_>,_>>().unwrap();
                            assert_eq!(neighbors.len(), 5, "Target should have 5 incoming edges");
                        }
                    }

                    thread::sleep(std::time::Duration::from_micros(100));
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
#[serial(lmdb_stress)]
fn test_concurrent_multi_hop_traversals() {
    // Tests concurrent traversals across multiple hops
    //
    // EXPECTED: Multi-hop paths remain consistent

    let (_temp_dir, storage) = setup_concurrent_storage();

    // Create chain: root -> level1 nodes -> level2 nodes
    let root_id = {
        let arena = Bump::new();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        let root = G::new_mut(&storage, &arena, &mut wtxn)
            .add_n("root", None, None)
            .collect::<Result<Vec<_>,_>>().unwrap()[0]
            .id();

        // Create level 1 nodes
        let level1_ids: Vec<u128> = (0..3)
            .map(|i| {
                let label = format!("level1_{}", i);
                let id = G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>,_>>().unwrap()[0]
                    .id();

                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_edge("to_l1", None, root, id, false)
                    .collect_to_obj().unwrap();

                id
            })
            .collect();

        // Create level 2 nodes
        for l1_id in level1_ids {
            for i in 0..2 {
                let label = format!("level2_{}", i);
                let l2_id = G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>,_>>().unwrap()[0]
                    .id();

                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_edge("to_l2", None, l1_id, l2_id, false)
                    .collect_to_obj().unwrap();
            }
        }

        wtxn.commit().unwrap();
        root
    };

    let num_threads = 4;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_thread_id| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..15 {
                    let arena = Bump::new();
                    let rtxn = storage.graph_env.read_txn().unwrap();

                    // Traverse: root -> level1
                    let level1 = G::new(&storage, &rtxn, &arena)
                        .n_from_id(&root_id)
                        .out_node("to_l1")
                        .collect::<Result<Vec<_>,_>>().unwrap();
                    assert_eq!(level1.len(), 3, "Should have 3 level1 nodes");

                    // For each level1, traverse to level2
                    for l1_node in level1 {
                        let arena2 = Bump::new();
                        let level2 = G::new(&storage, &rtxn, &arena2)
                            .n_from_id(&l1_node.id())
                            .out_node("to_l2")
                            .collect::<Result<Vec<_>,_>>().unwrap();
                        assert_eq!(level2.len(), 2, "Each level1 should have 2 level2 nodes");
                    }

                    thread::sleep(std::time::Duration::from_micros(200));
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
#[serial(lmdb_stress)]
fn test_concurrent_graph_topology_consistency() {
    // Tests that graph topology remains valid under concurrent operations
    //
    // EXPECTED: No broken edges, all edges point to valid nodes

    let (_temp_dir, storage) = setup_concurrent_storage();

    let num_writers = 4;
    let nodes_per_writer = 10;
    let barrier = Arc::new(Barrier::new(num_writers));

    let handles: Vec<_> = (0..num_writers)
        .map(|writer_id| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                // Each writer creates nodes and edges
                for i in 0..nodes_per_writer {
                    let arena = Bump::new();
                    let mut wtxn = storage.graph_env.write_txn().unwrap();

                    let label1 = format!("w{}_n{}_a", writer_id, i);
                    let label2 = format!("w{}_n{}_b", writer_id, i);

                    let node1_id = G::new_mut(&storage, &arena, &mut wtxn)
                        .add_n(&label1, None, None)
                        .collect::<Result<Vec<_>,_>>().unwrap()[0]
                        .id();

                    let node2_id = G::new_mut(&storage, &arena, &mut wtxn)
                        .add_n(&label2, None, None)
                        .collect::<Result<Vec<_>,_>>().unwrap()[0]
                        .id();

                    G::new_mut(&storage, &arena, &mut wtxn)
                        .add_edge("connects", None, node1_id, node2_id, false)
                        .collect_to_obj().unwrap();

                    wtxn.commit().unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify topology: all edges should be valid
    let arena = Bump::new();
    let rtxn = storage.graph_env.read_txn().unwrap();

    // Count nodes and edges
    let node_count = storage.nodes_db.len(&rtxn).unwrap();
    let edge_count = storage.edges_db.len(&rtxn).unwrap();

    let expected_nodes = (num_writers * nodes_per_writer * 2) as u64; // 2 nodes per iteration
    let expected_edges = (num_writers * nodes_per_writer) as u64;

    assert_eq!(node_count, expected_nodes, "Node count mismatch");
    assert_eq!(edge_count, expected_edges, "Edge count mismatch");

    // Verify all edges point to valid nodes
    for result in storage.edges_db.iter(&rtxn).unwrap() {
        let (edge_id, edge_bytes) = result.unwrap();
        let edge = crate::utils::items::Edge::from_bincode_bytes(edge_id, &edge_bytes, &arena).unwrap();

        // Verify source exists
        assert!(
            storage.nodes_db.get(&rtxn, &edge.from_node).unwrap().is_some(),
            "Edge source node not found"
        );

        // Verify target exists
        assert!(
            storage.nodes_db.get(&rtxn, &edge.to_node).unwrap().is_some(),
            "Edge target node not found"
        );
    }
}

#[test]
#[serial(lmdb_stress)]
fn test_stress_concurrent_mixed_operations() {
    // Stress test: sustained mixed read/write operations
    //
    // EXPECTED: No panics, deadlocks, or corruption

    let (_temp_dir, storage) = setup_concurrent_storage();

    // Create initial graph
    let root_ids: Vec<u128> = {
        let arena = Bump::new();
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        let ids = (0..3)
            .map(|i| {
                let label = format!("root_{}", i);
                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>,_>>().unwrap()[0]
                    .id()
            })
            .collect();

        wtxn.commit().unwrap();
        ids
    };

    let duration = std::time::Duration::from_secs(2);
    let start = std::time::Instant::now();
    let root_ids = Arc::new(root_ids);

    let mut handles = vec![];

    // Spawn writers
    for writer_id in 0..2 {
        let storage = Arc::clone(&storage);
        let root_ids = Arc::clone(&root_ids);

        handles.push(thread::spawn(move || {
            let mut write_count = 0;
            while start.elapsed() < duration {
                let arena = Bump::new();
                let mut wtxn = storage.graph_env.write_txn().unwrap();

                let label = format!("w{}_n{}", writer_id, write_count);
                let new_id = G::new_mut(&storage, &arena, &mut wtxn)
                    .add_n(&label, None, None)
                    .collect::<Result<Vec<_>,_>>().unwrap()[0]
                    .id();

                let root_idx = write_count % root_ids.len();
                G::new_mut(&storage, &arena, &mut wtxn)
                    .add_edge("links", None, root_ids[root_idx], new_id, false)
                    .collect_to_obj().unwrap();

                wtxn.commit().unwrap();
                write_count += 1;
            }
            write_count
        }));
    }

    // Spawn readers
    for _reader_id in 0..4 {
        let storage = Arc::clone(&storage);
        let root_ids = Arc::clone(&root_ids);

        handles.push(thread::spawn(move || {
            let mut read_count = 0;
            while start.elapsed() < duration {
                let arena = Bump::new();
                let rtxn = storage.graph_env.read_txn().unwrap();

                for root_id in root_ids.iter() {
                    let _neighbors = G::new(&storage, &rtxn, &arena)
                        .n_from_id(root_id)
                        .out_node("links")
                        .collect::<Result<Vec<_>,_>>().unwrap();
                    read_count += 1;
                }
            }
            read_count
        }));
    }

    let mut write_counts = vec![];
    let mut read_counts = vec![];

    for (idx, handle) in handles.into_iter().enumerate() {
        let count = handle.join().unwrap();
        if idx < 2 {
            write_counts.push(count);
        } else {
            read_counts.push(count);
        }
    }

    let total_writes: usize = write_counts.iter().sum();
    let total_reads: usize = read_counts.iter().sum();

    println!("Stress test: {} writes, {} reads in {:?}", total_writes, total_reads, duration);

    // Should process many operations
    assert!(total_writes > 50, "Should perform many writes, got {}", total_writes);
    assert!(total_reads > 100, "Should perform many reads, got {}", total_reads);
}
