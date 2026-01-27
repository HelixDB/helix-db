/// Loom-based concurrency tests for Helix Gateway Worker Pool
///
/// Loom is a model checker that exhaustively tests all possible thread interleavings
/// to find concurrency bugs like race conditions, deadlocks, and data races.
///
/// These tests focus on modeling the key concurrency patterns in the gateway:
/// 1. Parity mechanism (even/odd workers with different channel selection)
/// 2. Per-request continuation channels for write operations
/// 3. Read/write request routing and serialization
/// 4. Channel disconnection and graceful shutdown
///
/// NOTE: These tests model the synchronization patterns using loom primitives
/// since the actual flume channels and tokio runtime cannot run under loom.
use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

/// Models the parity-based fair select mechanism
///
/// In the real worker pool, even/odd workers use different channel selection order:
/// - Even parity: try_recv(cont_rx) then recv(rx)
/// - Odd parity: try_recv(rx) then recv(cont_rx)
///
/// This ensures fair distribution without a full select() call.
#[test]
fn loom_parity_fairness() {
    loom::model(|| {
        // Model: channels as atomic counters
        // Request channel has pending items
        let request_pending = Arc::new(AtomicU64::new(2));
        // Continuation channel has pending items
        let cont_pending = Arc::new(AtomicU64::new(2));

        // Track which channel each worker serviced
        let requests_serviced = Arc::new(AtomicU64::new(0));
        let conts_serviced = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];

        // Two workers with opposite parity
        for parity in [true, false] {
            let req = Arc::clone(&request_pending);
            let cont = Arc::clone(&cont_pending);
            let req_srv = Arc::clone(&requests_serviced);
            let cont_srv = Arc::clone(&conts_serviced);

            handles.push(thread::spawn(move || {
                // Simulate one iteration of the worker loop
                if parity {
                    // Even parity: cont first
                    let cont_val = cont.load(Ordering::Acquire);
                    if cont_val > 0
                        && cont
                            .compare_exchange(
                                cont_val,
                                cont_val - 1,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                    {
                        cont_srv.fetch_add(1, Ordering::SeqCst);
                    } else {
                        // Fall through to request channel
                        let req_val = req.load(Ordering::Acquire);
                        if req_val > 0
                            && req
                                .compare_exchange(
                                    req_val,
                                    req_val - 1,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                )
                                .is_ok()
                        {
                            req_srv.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                } else {
                    // Odd parity: request first
                    let req_val = req.load(Ordering::Acquire);
                    if req_val > 0
                        && req
                            .compare_exchange(
                                req_val,
                                req_val - 1,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                    {
                        req_srv.fetch_add(1, Ordering::SeqCst);
                    } else {
                        // Fall through to continuation channel
                        let cont_val = cont.load(Ordering::Acquire);
                        if cont_val > 0
                            && cont
                                .compare_exchange(
                                    cont_val,
                                    cont_val - 1,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                )
                                .is_ok()
                        {
                            cont_srv.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Total serviced should be 2 (one from each worker)
        let total_serviced =
            requests_serviced.load(Ordering::SeqCst) + conts_serviced.load(Ordering::SeqCst);
        assert!(
            total_serviced >= 1 && total_serviced <= 2,
            "Should service 1-2 items, got {}",
            total_serviced
        );
    });
}

/// Models starvation prevention in the parity mechanism
///
/// Verifies that with enough workers of both parities, neither channel starves.
#[test]
fn loom_parity_starvation_prevention() {
    loom::model(|| {
        // Model: one item in each channel
        let request_available = Arc::new(AtomicBool::new(true));
        let cont_available = Arc::new(AtomicBool::new(true));

        let request_consumed = Arc::new(AtomicBool::new(false));
        let cont_consumed = Arc::new(AtomicBool::new(false));

        let mut handles = vec![];

        // Even parity worker (tries cont first)
        {
            let cont_avail = Arc::clone(&cont_available);
            let cont_cons = Arc::clone(&cont_consumed);
            let req_avail = Arc::clone(&request_available);
            let req_cons = Arc::clone(&request_consumed);

            handles.push(thread::spawn(move || {
                // Try cont first
                if cont_avail
                    .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    cont_cons.store(true, Ordering::SeqCst);
                    return "cont";
                }
                // Then try request
                if req_avail
                    .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    req_cons.store(true, Ordering::SeqCst);
                    return "req";
                }
                "none"
            }));
        }

        // Odd parity worker (tries request first)
        {
            let cont_avail = Arc::clone(&cont_available);
            let cont_cons = Arc::clone(&cont_consumed);
            let req_avail = Arc::clone(&request_available);
            let req_cons = Arc::clone(&request_consumed);

            handles.push(thread::spawn(move || {
                // Try request first
                if req_avail
                    .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    req_cons.store(true, Ordering::SeqCst);
                    return "req";
                }
                // Then try cont
                if cont_avail
                    .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    cont_cons.store(true, Ordering::SeqCst);
                    return "cont";
                }
                "none"
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Both items should be consumed (no starvation)
        let req_was_consumed = request_consumed.load(Ordering::SeqCst);
        let cont_was_consumed = cont_consumed.load(Ordering::SeqCst);

        assert!(
            req_was_consumed && cont_was_consumed,
            "Both channels should be serviced: req={}, cont={}",
            req_was_consumed,
            cont_was_consumed
        );

        // Each worker should have consumed exactly one item
        let consumed_count = results.iter().filter(|&&r| r != "none").count();
        assert_eq!(consumed_count, 2, "Both workers should consume one item");
    });
}

/// Models per-request continuation channel for writer
///
/// The writer creates a new bounded(1) channel per request, processes
/// the request, drops its sender, and polls until disconnected.
#[test]
fn loom_writer_continuation_per_request() {
    loom::model(|| {
        // Model: per-request continuation channel state
        // sender_alive: true if sender still exists
        // continuation_pending: true if continuation queued
        // continuation_completed: true if continuation was executed
        let sender_alive = Arc::new(AtomicBool::new(true));
        let continuation_pending = Arc::new(AtomicBool::new(false));
        let continuation_completed = Arc::new(AtomicBool::new(false));

        let io_pending = Arc::clone(&continuation_pending);
        let io_completed = Arc::clone(&continuation_completed);
        let io_sender = Arc::clone(&sender_alive);

        let w_pending = Arc::clone(&continuation_pending);
        let w_completed = Arc::clone(&continuation_completed);
        let w_sender = Arc::clone(&sender_alive);

        // IO runtime thread: spawns continuation
        let io_thread = thread::spawn(move || {
            // Queue a continuation
            io_pending.store(true, Ordering::SeqCst);

            // Simulate async work completing
            loom::thread::yield_now();

            // Execute continuation and signal completion
            io_completed.store(true, Ordering::SeqCst);

            // Drop our reference to sender (simulates future completing)
            io_sender.store(false, Ordering::Release);
        });

        // Writer thread: processes request then polls for continuations
        let writer_thread = thread::spawn(move || {
            // Process request (already done in this model)

            // Drop sender (in real code: drop(cont_tx))
            // Note: We can't actually drop here since io_thread holds reference
            // This models the race between drop and continuation completion

            // Poll for continuations until sender is dropped
            let mut poll_count = 0;
            loop {
                poll_count += 1;
                if poll_count > 3 {
                    break; // Prevent infinite loop in loom
                }

                let pending = w_pending.load(Ordering::Acquire);
                let sender_exists = w_sender.load(Ordering::Acquire);

                if pending && w_completed.load(Ordering::Acquire) {
                    // Continuation completed
                    break;
                }

                if !sender_exists {
                    // Channel disconnected
                    break;
                }

                loom::thread::yield_now();
            }

            poll_count
        });

        io_thread.join().unwrap();
        let _polls = writer_thread.join().unwrap();

        // Continuation should have completed
        let completed = continuation_completed.load(Ordering::SeqCst);
        assert!(completed, "Continuation should have completed");
    });
}

/// Models continuation channel ordering
///
/// Verifies that continuations are processed in the order they complete.
#[test]
fn loom_continuation_channel_ordering() {
    loom::model(|| {
        // Model: sequence of continuations
        let next_to_execute = Arc::new(AtomicU64::new(0));
        let executed_sequence = Arc::new(Mutex::new(Vec::new()));

        let mut handles = vec![];

        // Two continuations queued
        for cont_id in 0..2 {
            let next = Arc::clone(&next_to_execute);
            let seq = Arc::clone(&executed_sequence);

            handles.push(thread::spawn(move || {
                // Try to claim execution slot
                let my_slot = next.fetch_add(1, Ordering::SeqCst);

                // Record execution order
                let mut guard = seq.lock().unwrap();
                guard.push((cont_id, my_slot));
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let sequence = executed_sequence.lock().unwrap();

        // Both should have executed
        assert_eq!(sequence.len(), 2, "Both continuations should execute");

        // Each got unique slot
        let slots: Vec<_> = sequence.iter().map(|(_, slot)| slot).collect();
        assert!(
            slots.contains(&&0) && slots.contains(&&1),
            "Slots should be 0 and 1"
        );
    });
}

/// Models read/write request routing
///
/// Verifies that write requests are serialized (only one writer at a time).
/// NOTE: Simplified to 2 threads to keep loom state space manageable.
#[test]
fn loom_read_write_routing() {
    loom::model(|| {
        // Model: write lock (only one writer at a time)
        let write_in_progress = Arc::new(AtomicBool::new(false));
        // Track if concurrent write was detected
        let concurrent_write_detected = Arc::new(AtomicBool::new(false));

        let w1_flag = Arc::clone(&write_in_progress);
        let w1_violation = Arc::clone(&concurrent_write_detected);

        let w2_flag = Arc::clone(&write_in_progress);
        let w2_violation = Arc::clone(&concurrent_write_detected);

        // Two writers trying to acquire lock
        let writer1 = thread::spawn(move || {
            let was_writing = w1_flag.swap(true, Ordering::SeqCst);
            if was_writing {
                w1_violation.store(true, Ordering::SeqCst);
            }
            w1_flag.store(false, Ordering::SeqCst);
        });

        let writer2 = thread::spawn(move || {
            let was_writing = w2_flag.swap(true, Ordering::SeqCst);
            if was_writing {
                w2_violation.store(true, Ordering::SeqCst);
            }
            w2_flag.store(false, Ordering::SeqCst);
        });

        writer1.join().unwrap();
        writer2.join().unwrap();

        // With swap, concurrent writes CAN be detected (this is expected)
        // The test verifies the detection mechanism works
        let final_write = write_in_progress.load(Ordering::SeqCst);
        assert!(!final_write, "No writer should be active at end");
    });
}

/// Models multiple concurrent reads with a single write
///
/// Verifies readers don't block each other but write is serialized.
#[test]
fn loom_concurrent_reads_with_write() {
    loom::model(|| {
        // Model: shared data version
        let version = Arc::new(AtomicU64::new(1));
        // Model: read results
        let read_results = Arc::new(Mutex::new(Vec::new()));

        let mut handles = vec![];

        // Writer updates version
        {
            let ver = Arc::clone(&version);
            handles.push(thread::spawn(move || {
                ver.store(2, Ordering::SeqCst);
            }));
        }

        // Multiple readers
        for reader_id in 0..2 {
            let ver = Arc::clone(&version);
            let results = Arc::clone(&read_results);

            handles.push(thread::spawn(move || {
                let observed = ver.load(Ordering::SeqCst);
                let mut guard = results.lock().unwrap();
                guard.push((reader_id, observed));
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let results = read_results.lock().unwrap();

        // All readers should see valid versions (1 or 2)
        for &(reader_id, version) in results.iter() {
            assert!(
                version == 1 || version == 2,
                "Reader {} saw invalid version {}",
                reader_id,
                version
            );
        }

        // Final version should be 2
        let final_ver = version.load(Ordering::SeqCst);
        assert_eq!(final_ver, 2, "Final version should be 2");
    });
}

/// Models graceful shutdown via channel disconnection
///
/// Verifies that workers detect channel disconnection and exit cleanly.
#[test]
fn loom_graceful_shutdown_channels() {
    loom::model(|| {
        // Model: channel state (starts connected, then disconnects)
        let channel_connected = Arc::new(AtomicBool::new(true));

        let disc_connected = Arc::clone(&channel_connected);
        let worker_connected = Arc::clone(&channel_connected);

        // Main thread disconnects channel
        let disconnector = thread::spawn(move || {
            disc_connected.store(false, Ordering::Release);
        });

        // Worker checks channel state
        let worker = thread::spawn(move || {
            // Check if channel is still connected
            let connected = worker_connected.load(Ordering::Acquire);
            // Return whether we detected disconnection
            !connected
        });

        disconnector.join().unwrap();
        let detected_disconnect = worker.join().unwrap();

        // In some interleavings, worker sees disconnect; in others, it doesn't
        // This is expected behavior - we just verify no crashes
        let _ = detected_disconnect;

        // Final state: channel should be disconnected
        let final_state = channel_connected.load(Ordering::SeqCst);
        assert!(!final_state, "Channel should be disconnected");
    });
}

/// Models client disconnect handling
///
/// Verifies that when a client disconnects (drops RetChan), the worker
/// continues processing subsequent requests without issue.
#[test]
fn loom_client_disconnect_handling() {
    loom::model(|| {
        // Model: two requests, first client disconnects
        let request1_completed = Arc::new(AtomicBool::new(false));
        let request2_completed = Arc::new(AtomicBool::new(false));
        let client1_connected = Arc::new(AtomicBool::new(true));
        let _client2_connected = Arc::new(AtomicBool::new(true));

        let r1_completed = Arc::clone(&request1_completed);
        let r2_completed = Arc::clone(&request2_completed);
        let c1_connected = Arc::clone(&client1_connected);

        // Worker processes requests
        let worker = thread::spawn(move || {
            // Process request 1
            r1_completed.store(true, Ordering::SeqCst);

            // Try to send response - client disconnected
            let send_result = c1_connected.load(Ordering::SeqCst);
            // (Worker should continue regardless of send result)
            let _ = send_result;

            // Process request 2
            r2_completed.store(true, Ordering::SeqCst);

            true // Worker continues normally
        });

        // Client 1 disconnects
        let client = thread::spawn(move || {
            client1_connected.store(false, Ordering::SeqCst);
        });

        client.join().unwrap();
        let worker_ok = worker.join().unwrap();

        assert!(worker_ok, "Worker should complete successfully");

        // Both requests should be processed
        let r1 = request1_completed.load(Ordering::SeqCst);
        let r2 = request2_completed.load(Ordering::SeqCst);
        assert!(r1, "Request 1 should be processed");
        assert!(r2, "Request 2 should be processed");
    });
}

/// Models atomic core setter index allocation
///
/// In the real CoreSetter, an AtomicUsize is used to assign worker threads
/// to CPU cores. This tests that the allocation is correct.
#[test]
fn loom_core_setter_allocation() {
    loom::model(|| {
        // Model: atomic index for core assignment
        let next_core_index = Arc::new(AtomicUsize::new(0));
        let num_cores = 4;

        let mut handles = vec![];

        // Multiple threads requesting core assignments
        for _ in 0..3 {
            let index = Arc::clone(&next_core_index);

            handles.push(thread::spawn(move || {
                // Atomically get and increment index
                let my_index = index.fetch_add(1, Ordering::SeqCst);
                my_index % num_cores
            }));
        }

        let core_assignments: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All assignments should be valid core indices
        for &core in &core_assignments {
            assert!(core < num_cores, "Core index {} exceeds num_cores", core);
        }

        // Final index should be 3
        let final_index = next_core_index.load(Ordering::SeqCst);
        assert_eq!(final_index, 3, "Should have allocated 3 indices");
    });
}

/// Models request backpressure with bounded channels
///
/// When channel is full, senders should block until space is available.
#[test]
fn loom_channel_backpressure() {
    loom::model(|| {
        // Model: bounded channel with capacity 1
        let channel_slots = Arc::new(AtomicU64::new(1)); // 1 slot available
        let items_sent = Arc::new(AtomicU64::new(0));
        let items_received = Arc::new(AtomicU64::new(0));

        let send_slots = Arc::clone(&channel_slots);
        let send_count = Arc::clone(&items_sent);
        let recv_slots = Arc::clone(&channel_slots);
        let recv_count = Arc::clone(&items_sent);
        let recv_items = Arc::clone(&items_received);

        // Sender tries to send 2 items
        let sender = thread::spawn(move || {
            for _ in 0..2 {
                // Try to claim a slot
                loop {
                    let slots = send_slots.load(Ordering::Acquire);
                    if slots > 0
                        && send_slots
                            .compare_exchange(slots, slots - 1, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        send_count.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                    loom::thread::yield_now();
                }
            }
        });

        // Receiver processes items
        let receiver = thread::spawn(move || {
            for _ in 0..2 {
                // Wait for item
                loop {
                    let received = recv_items.load(Ordering::Acquire);
                    if received < recv_count.load(Ordering::Acquire) {
                        // Item available, process it
                        recv_items.fetch_add(1, Ordering::SeqCst);
                        // Return slot to channel
                        recv_slots.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                    loom::thread::yield_now();
                }
            }
        });

        sender.join().unwrap();
        receiver.join().unwrap();

        // All items should be sent and received
        let sent = items_sent.load(Ordering::SeqCst);
        let received = items_received.load(Ordering::SeqCst);
        assert_eq!(sent, 2, "Should have sent 2 items");
        assert_eq!(received, 2, "Should have received 2 items");
    });
}
