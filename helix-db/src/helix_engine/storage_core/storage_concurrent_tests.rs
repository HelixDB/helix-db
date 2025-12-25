/// Concurrent access tests for Storage Layer
///
/// This test suite validates thread safety and concurrent operation correctness
/// for the HelixGraphStorage implementation. Key areas tested:
///
/// 1. **Concurrent Node/Edge Operations**: Multiple threads creating/dropping nodes and edges
/// 2. **Transaction Isolation**: MVCC snapshot isolation during concurrent operations
/// 3. **Write Serialization**: LMDB single-writer guarantee validation
///
/// CRITICAL ISSUES BEING TESTED:
/// - Drop operations are multi-step (not atomic) - could leave orphans
/// - LMDB provides single-writer guarantee but needs validation
/// - MVCC snapshot isolation needs verification

use serial_test::serial;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

use crate::helix_engine::storage_core::HelixGraphStorage;
use crate::helix_engine::traversal_core::config::Config;
use crate::helix_engine::storage_core::version_info::VersionInfo;
use crate::utils::items::{Node, Edge};
use bumpalo::Bump;
use uuid::Uuid;

/// Setup storage for concurrent testing
fn setup_concurrent_storage() -> (Arc<HelixGraphStorage>, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    let mut config = Config::default();
    config.db_max_size_gb = Some(10); // 10GB for concurrent testing

    let version_info = VersionInfo::default();

    let storage = HelixGraphStorage::new(path, config, version_info).unwrap();
    (Arc::new(storage), temp_dir)
}

#[test]
#[serial(lmdb_stress)]
fn test_concurrent_node_creation() {
    // Tests concurrent node creation from multiple threads
    //
    // EXPECTED: All nodes created successfully, no ID collisions

    let (storage, _temp_dir) = setup_concurrent_storage();

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
                    let mut wtxn = storage.graph_env.write_txn().unwrap();
                    let arena = Bump::new();

                    // Create node with struct literal
                    let label = arena.alloc_str(&format!("node_t{}_i{}", thread_id, i));
                    let node = Node {
                        id: Uuid::new_v4().as_u128(),
                        label,
                        version: 1,
                        properties: None,
                    };

                    storage.nodes_db.put(&mut wtxn, &node.id, &node.to_bincode_bytes().unwrap()).unwrap();
                    wtxn.commit().unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify: All nodes created
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
fn test_concurrent_edge_creation() {
    // Tests concurrent edge creation between nodes
    //
    // EXPECTED: All edges created, proper serialization

    let (storage, _temp_dir) = setup_concurrent_storage();

    // Create source and sink nodes first
    {
        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        for i in 0..10 {
            let label = arena.alloc_str(&format!("node_{}", i));
            let node = Node {
                id: Uuid::new_v4().as_u128(),
                label,
                version: 1,
                properties: None,
            };
            storage.nodes_db.put(&mut wtxn, &node.id, &node.to_bincode_bytes().unwrap()).unwrap();
        }
        wtxn.commit().unwrap();
    }

    // Get node IDs
    let node_ids: Vec<u128> = {
        let rtxn = storage.graph_env.read_txn().unwrap();
        storage.nodes_db
            .iter(&rtxn)
            .unwrap()
            .map(|result| {
                let (id, _) = result.unwrap();
                id
            })
            .collect()
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
                    let mut wtxn = storage.graph_env.write_txn().unwrap();
                    let arena = Bump::new();

                    // Create edge between nodes
                    let source_idx = (thread_id * 2) % node_ids.len();
                    let sink_idx = (thread_id * 2 + 1) % node_ids.len();

                    let label = arena.alloc_str(&format!("edge_t{}_i{}", thread_id, i));
                    let edge = Edge {
                        id: Uuid::new_v4().as_u128(),
                        from_node: node_ids[source_idx],
                        to_node: node_ids[sink_idx],
                        label,
                        version: 1,
                        properties: None,
                    };

                    storage.edges_db.put(&mut wtxn, &edge.id, &edge.to_bincode_bytes().unwrap()).unwrap();
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
fn test_concurrent_node_reads() {
    // Tests concurrent reads while writes are happening
    //
    // EXPECTED: Readers see consistent snapshots (MVCC)

    let (storage, _temp_dir) = setup_concurrent_storage();

    // Create initial nodes
    let initial_count = 20u64;
    {
        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        for i in 0..initial_count {
            let label = arena.alloc_str(&format!("initial_{}", i));
            let node = Node {
                id: Uuid::new_v4().as_u128(),
                label,
                version: 1,
                properties: None,
            };
            storage.nodes_db.put(&mut wtxn, &node.id, &node.to_bincode_bytes().unwrap()).unwrap();
        }
        wtxn.commit().unwrap();
    }

    let num_readers = 4;
    let num_writers = 2;
    let barrier = Arc::new(Barrier::new(num_readers + num_writers));

    let mut handles = vec![];

    // Spawn readers
    for reader_id in 0..num_readers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier.wait();

            let mut total_reads = 0;

            for _ in 0..20 {
                let rtxn = storage.graph_env.read_txn().unwrap();
                let count = storage.nodes_db.len(&rtxn).unwrap();
                total_reads += 1;

                // Count should be at least initial_count
                assert!(
                    count >= initial_count,
                    "Reader {} saw only {} nodes",
                    reader_id,
                    count
                );

                thread::sleep(std::time::Duration::from_millis(1));
            }

            total_reads
        }));
    }

    // Spawn writers
    for writer_id in 0..num_writers {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier.wait();

            for i in 0..10 {
                let mut wtxn = storage.graph_env.write_txn().unwrap();
                let arena = Bump::new();

                let label = arena.alloc_str(&format!("writer_{}_node_{}", writer_id, i));
                let node = Node {
                    id: Uuid::new_v4().as_u128(),
                    label,
                    version: 1,
                    properties: None,
                };
                storage.nodes_db.put(&mut wtxn, &node.id, &node.to_bincode_bytes().unwrap()).unwrap();
                wtxn.commit().unwrap();

                thread::sleep(std::time::Duration::from_millis(2));
            }
            0 // Return value to match reader threads
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Final verification
    let rtxn = storage.graph_env.read_txn().unwrap();
    let final_count = storage.nodes_db.len(&rtxn).unwrap();
    assert_eq!(
        final_count,
        initial_count + (num_writers * 10) as u64,
        "Final count mismatch"
    );
}

#[test]
#[serial(lmdb_stress)]
fn test_transaction_isolation_storage() {
    // Tests MVCC snapshot isolation at storage layer
    //
    // EXPECTED: Long-lived read transactions see consistent snapshot

    let (storage, _temp_dir) = setup_concurrent_storage();

    // Create initial nodes
    let initial_count = 10u64;
    {
        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        for i in 0..initial_count {
            let label = arena.alloc_str(&format!("node_{}", i));
            let node = Node {
                id: Uuid::new_v4().as_u128(),
                label,
                version: 1,
                properties: None,
            };
            storage.nodes_db.put(&mut wtxn, &node.id, &node.to_bincode_bytes().unwrap()).unwrap();
        }
        wtxn.commit().unwrap();
    }

    // Start long-lived read transaction
    let rtxn = storage.graph_env.read_txn().unwrap();
    let count_before = storage.nodes_db.len(&rtxn).unwrap();
    assert_eq!(count_before, initial_count);

    // In another thread, add more nodes
    let storage_clone = Arc::clone(&storage);
    let handle = thread::spawn(move || {
        for i in 0..15 {
            let mut wtxn = storage_clone.graph_env.write_txn().unwrap();
            let arena = Bump::new();

            let label = arena.alloc_str(&format!("new_node_{}", i));
            let node = Node {
                id: Uuid::new_v4().as_u128(),
                label,
                version: 1,
                properties: None,
            };
            storage_clone.nodes_db.put(&mut wtxn, &node.id, &node.to_bincode_bytes().unwrap()).unwrap();
            wtxn.commit().unwrap();
        }
    });

    handle.join().unwrap();

    // Original transaction should still see same count (snapshot isolation)
    let count_after = storage.nodes_db.len(&rtxn).unwrap();
    assert_eq!(
        count_after, count_before,
        "Transaction isolation violated: count changed from {} to {}",
        count_before, count_after
    );

    drop(rtxn);

    // New transaction should see all nodes
    let rtxn_new = storage.graph_env.read_txn().unwrap();
    let count_new = storage.nodes_db.len(&rtxn_new).unwrap();
    assert_eq!(count_new, initial_count + 15);
}

#[test]
#[serial(lmdb_stress)]
fn test_write_transaction_serialization() {
    // Tests that write transactions are properly serialized
    //
    // EXPECTED: Only one write transaction at a time (enforced by LMDB)

    let (storage, _temp_dir) = setup_concurrent_storage();

    let num_threads = 4;
    let writes_per_thread = 25;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                for i in 0..writes_per_thread {
                    // Each write transaction should be serialized
                    let mut wtxn = storage.graph_env.write_txn().unwrap();
                    let arena = Bump::new();

                    let label = arena.alloc_str(&format!("serial_t{}_i{}", thread_id, i));
                    let node = Node {
                        id: Uuid::new_v4().as_u128(),
                        label,
                        version: 1,
                        properties: None,
                    };

                    storage.nodes_db.put(&mut wtxn, &node.id, &node.to_bincode_bytes().unwrap()).unwrap();

                    // Simulate some work during transaction
                    thread::sleep(std::time::Duration::from_micros(100));

                    wtxn.commit().unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all writes completed
    let rtxn = storage.graph_env.read_txn().unwrap();
    let count = storage.nodes_db.len(&rtxn).unwrap();
    assert_eq!(
        count,
        (num_threads * writes_per_thread) as u64,
        "Write serialization failed: expected {} nodes, found {}",
        num_threads * writes_per_thread,
        count
    );
}
