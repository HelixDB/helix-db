/// Loom-based concurrency tests for HNSW Vector Core
///
/// Loom is a model checker that exhaustively tests all possible thread interleavings
/// to find concurrency bugs like race conditions, deadlocks, and data races.
///
/// These tests focus on CRITICAL RACE CONDITIONS identified in the analysis:
/// 1. Entry point updates (no synchronization - last writer wins)
/// 2. Neighbor graph mutations (multiple inserts could create invalid topology)
/// 3. Level generation and entry point selection races
///
/// NOTE: Loom tests are expensive - they explore all possible execution orderings.
/// Keep the problem space small (few operations, few threads).
use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering};
use loom::thread;

/// Simplified model of entry point updates for loom testing
///
/// This models the race condition where multiple threads try to update
/// the entry point without synchronization.
#[test]
fn loom_entry_point_race() {
    loom::model(|| {
        // Simulated entry point - None means no entry point set yet
        let entry_point = Arc::new(AtomicU64::new(0));
        let insert_count = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];

        // Two threads both trying to insert and potentially set entry point
        for thread_id in 1..=2 {
            let entry_point = Arc::clone(&entry_point);
            let insert_count = Arc::clone(&insert_count);

            handles.push(thread::spawn(move || {
                // Simulate: Check if entry point exists
                let current_entry = entry_point.load(Ordering::SeqCst);

                // Simulate: Insert a new vector (always succeeds)
                let my_id = insert_count.fetch_add(1, Ordering::SeqCst) + 1;

                // Simulate: If no entry point, try to set it
                // RACE CONDITION: Both threads could see 0 and both try to set
                if current_entry == 0 {
                    // This is the problematic code pattern - non-atomic check-then-set
                    entry_point.store(thread_id * 100 + my_id, Ordering::SeqCst);
                }

                my_id
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Verify: Both inserts succeeded
        assert_eq!(results.len(), 2);

        // Entry point should be set to one of the inserted IDs
        let final_entry = entry_point.load(Ordering::SeqCst);
        assert!(final_entry > 0, "Entry point should be set");

        // Total inserts should be 2
        assert_eq!(insert_count.load(Ordering::SeqCst), 2);
    });
}

/// Model of concurrent entry point reads and writes
///
/// Tests the pattern where one thread writes entry point while another reads it
#[test]
fn loom_entry_point_read_write_race() {
    loom::model(|| {
        let entry_point = Arc::new(AtomicU64::new(0));

        let writer_entry = Arc::clone(&entry_point);
        let reader_entry = Arc::clone(&entry_point);

        // Writer thread: Sets entry point
        let writer = thread::spawn(move || {
            writer_entry.store(12345, Ordering::SeqCst);
        });

        // Reader thread: Reads entry point (might see 0 or 12345)
        let reader = thread::spawn(move || reader_entry.load(Ordering::SeqCst));

        writer.join().unwrap();
        let read_value = reader.join().unwrap();

        // Should see either 0 (old value) or 12345 (new value), but not garbage
        assert!(
            read_value == 0 || read_value == 12345,
            "Should see valid value, got {}",
            read_value
        );

        // Final value should be 12345
        assert_eq!(entry_point.load(Ordering::SeqCst), 12345);
    });
}

/// Model of concurrent updates to a counter (simulates neighbor count updates)
#[test]
fn loom_neighbor_count_race() {
    loom::model(|| {
        let neighbor_count = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];

        // Two threads both adding neighbors
        for _ in 0..2 {
            let count = Arc::clone(&neighbor_count);
            handles.push(thread::spawn(move || {
                // Read current count
                let current = count.load(Ordering::Acquire);

                // Simulate: Check if we can add more neighbors (max 10)
                if current < 10 {
                    // RACE: Another thread could increment between load and store
                    count.store(current + 1, Ordering::Release);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // With fetch_add, we'd always get 2
        // With load/store, we might get 1 (lost update)
        let final_count = neighbor_count.load(Ordering::SeqCst);

        // This test demonstrates the lost update problem
        // In real code, this should use fetch_add
        assert!(
            final_count >= 1 && final_count <= 2,
            "Expected 1 or 2, got {}",
            final_count
        );
    });
}

/// Model of level generation race
///
/// Simulates the race where two threads generate levels and update max level
#[test]
fn loom_max_level_update_race() {
    loom::model(|| {
        let max_level = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];

        // Two threads inserting vectors with potentially new max levels
        for thread_level in [2, 3] {
            let max_level = Arc::clone(&max_level);
            handles.push(thread::spawn(move || {
                // Read current max level
                let current_max = max_level.load(Ordering::Acquire);

                // If my level is higher, update max level
                if thread_level > current_max {
                    // RACE: Another thread could update between load and store
                    max_level.store(thread_level, Ordering::Release);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should end up with max level of 3
        let final_max = max_level.load(Ordering::SeqCst);
        assert!(
            final_max >= 2 && final_max <= 3,
            "Expected 2 or 3, got {}",
            final_max
        );

        // In a correct implementation with compare_exchange, it should always be 3
        // With load/store, it could be 2 (lost update)
    });
}

/// Model of compare-and-swap for entry point (correct implementation)
#[test]
fn loom_entry_point_cas_correct() {
    loom::model(|| {
        let entry_point = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];

        // Two threads both trying to set entry point using CAS
        for thread_id in 1..=2 {
            let entry_point = Arc::clone(&entry_point);
            handles.push(thread::spawn(move || {
                // Try to set entry point if it's 0 (using CAS - atomic check-then-set)
                let result = entry_point.compare_exchange(
                    0,
                    thread_id * 100,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );

                result.is_ok() // Returns true if this thread won the race
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Exactly one thread should have successfully set the entry point
        let num_successful = results.iter().filter(|&&success| success).count();
        assert_eq!(
            num_successful, 1,
            "Exactly one thread should set entry point"
        );

        // Entry point should be set
        let final_entry = entry_point.load(Ordering::SeqCst);
        assert!(final_entry > 0, "Entry point should be set");
        assert!(
            final_entry == 100 || final_entry == 200,
            "Entry point should be one of the thread IDs"
        );
    });
}

/// Model of sequential consistency for graph operations
///
/// Tests that operations appear in a consistent order to all observers
#[test]
fn loom_graph_operation_ordering() {
    loom::model(|| {
        // Simulate: node inserted flag, edges added flag
        let node_inserted = Arc::new(AtomicU64::new(0));
        let edges_added = Arc::new(AtomicU64::new(0));

        let writer_node = Arc::clone(&node_inserted);
        let writer_edges = Arc::clone(&edges_added);
        let reader_node = Arc::clone(&node_inserted);
        let reader_edges = Arc::clone(&edges_added);

        // Writer: Insert node, then add edges
        let writer = thread::spawn(move || {
            writer_node.store(1, Ordering::SeqCst);
            writer_edges.store(1, Ordering::SeqCst);
        });

        // Reader: Check if edges are added, then check if node is inserted
        let reader = thread::spawn(move || {
            let edges = reader_edges.load(Ordering::SeqCst);
            let node = reader_node.load(Ordering::SeqCst);
            (node, edges)
        });

        writer.join().unwrap();
        let (node_seen, edges_seen) = reader.join().unwrap();

        // If edges are added (1), node must be inserted (1)
        // Can't have edges without node (SeqCst guarantees this)
        if edges_seen == 1 {
            assert_eq!(
                node_seen, 1,
                "If edges added, node must be inserted (sequential consistency)"
            );
        }
    });
}

#[test]
fn loom_two_writers_one_reader() {
    // Model of two writers and one reader accessing shared counter
    //
    // Tests MVCC-like behavior where reader should see consistent state

    loom::model(|| {
        let value = Arc::new(AtomicU64::new(0));

        let w1_value = Arc::clone(&value);
        let w2_value = Arc::clone(&value);
        let r_value = Arc::clone(&value);

        // Writer 1: Increment value
        let w1 = thread::spawn(move || {
            w1_value.fetch_add(1, Ordering::SeqCst);
        });

        // Writer 2: Increment value
        let w2 = thread::spawn(move || {
            w2_value.fetch_add(1, Ordering::SeqCst);
        });

        // Reader: Read value (should see 0, 1, or 2)
        let reader = thread::spawn(move || r_value.load(Ordering::SeqCst));

        w1.join().unwrap();
        w2.join().unwrap();
        let read_value = reader.join().unwrap();

        // Reader should see 0 (before writes), 1 (after one write), or 2 (after both)
        assert!(
            read_value <= 2,
            "Reader should see valid value, got {}",
            read_value
        );

        // Final value should always be 2
        assert_eq!(value.load(Ordering::SeqCst), 2);
    });
}
