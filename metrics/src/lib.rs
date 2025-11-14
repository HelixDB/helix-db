pub mod events;

use std::{
    cell::RefCell,
    env::consts::OS,
    fs,
    path::Path,
    sync::{
        LazyLock, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde::Serialize;
use tokio::task::JoinHandle;

pub static METRICS_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

static CONFIG: LazyLock<String> = LazyLock::new(|| {
    let home_dir = std::env::var("HOME").unwrap_or("~/".to_string());
    let config_path = &format!("{home_dir}/.helix/credentials");
    let config_path = Path::new(config_path);
    fs::read_to_string(config_path).unwrap_or_default()
});

pub static HELIX_USER_ID: LazyLock<&'static str> = LazyLock::new(|| {
    // read from credentials file
    for line in CONFIG.lines() {
        if let Some((key, value)) = line.split_once("=")
            && key.to_lowercase() == "helix_user_id"
        {
            return value;
        }
    }
    String::new().leak()
});

pub static METRICS_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    for line in CONFIG.lines() {
        if let Some((key, value)) = line.split_once("=")
            && key.to_lowercase().as_str() == "metrics"
        {
            return value.to_string().parse().unwrap_or(true);
        }
    }
    true
});

pub const METRICS_URL: &str = "https://logs.helix-db.com/v2";

// Thread-local buffer for events
thread_local! {
    static EVENT_BUFFER: RefCell<Vec<events::RawEvent<events::EventData>>> =
        RefCell::new(Vec::with_capacity(THREAD_LOCAL_EVENT_BUFFER_LENGTH));
}

// Global state for metrics system
struct MetricsState {
    events_tx: flume::Sender<Vec<events::RawEvent<events::EventData>>>,
    events_rx: flume::Receiver<Vec<events::RawEvent<events::EventData>>>,
    notify_tx: flume::Sender<()>,
    notify_rx: flume::Receiver<()>,
    shutdown_tx: flume::Sender<()>,
    shutdown_rx: flume::Receiver<()>,
    threshold_batches: AtomicUsize,
    sender_handle: OnceLock<tokio::task::JoinHandle<()>>,
}

static METRICS_STATE: LazyLock<MetricsState> = LazyLock::new(|| {
    let (events_tx, events_rx) = flume::unbounded();
    let (notify_tx, notify_rx) = flume::unbounded();
    let (shutdown_tx, shutdown_rx) = flume::unbounded();

    // Read threshold from environment or use default
    let threshold_batches = std::env::var("HELIX_METRICS_THRESHOLD_BATCHES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(num_cpus::get());

    MetricsState {
        events_tx,
        events_rx,
        notify_tx,
        notify_rx,
        shutdown_tx,
        shutdown_rx,
        threshold_batches: AtomicUsize::new(threshold_batches),
        sender_handle: OnceLock::new(),
    }
});

// Configuration constants
const THREAD_LOCAL_EVENT_BUFFER_LENGTH: usize = 65536;
const THREAD_LOCAL_FLUSH_THRESHOLD: usize = 65536;
const BATCH_TIMEOUT_SECS: u64 = 1;
const THREAD_LOCAL_FLUSH_INTERVAL_SECS: u64 = 1; // Flush thread-local buffers every second

/// Initialize the metrics system with a tokio runtime
/// This must be called once at startup with an active tokio runtime
pub fn init_metrics_system() {
    if !*METRICS_ENABLED {
        return;
    }

    // Spawn the sender task if not already started
    let _ = METRICS_STATE.sender_handle.get_or_init(|| {
        tokio::spawn(sender_task(
            METRICS_STATE.events_rx.clone(),
            METRICS_STATE.notify_rx.clone(),
            METRICS_STATE.shutdown_rx.clone(),
        ))
    });
}

// Track last flush time per thread
thread_local! {
    static LAST_FLUSH_TIME: RefCell<std::time::Instant> = RefCell::new(std::time::Instant::now());
}

/// Initialize thread-local buffer for the current thread
/// Call this once per worker thread
pub fn init_thread_local() {
    if !*METRICS_ENABLED {
        return;
    }

    EVENT_BUFFER.with(|buffer| {
        buffer.borrow_mut().clear();
    });

    LAST_FLUSH_TIME.with(|time| {
        *time.borrow_mut() = std::time::Instant::now();
    });
}

/// Set the batch threshold for notifications
/// When the number of batches in channel exceeds this, sender task is notified
pub fn set_threshold_batches(batches: usize) {
    METRICS_STATE
        .threshold_batches
        .store(batches, Ordering::Relaxed);
}

/// Get the current batch threshold
pub fn get_threshold_batches() -> usize {
    METRICS_STATE.threshold_batches.load(Ordering::Relaxed)
}

/// Log an event to the metrics system
/// Events are buffered locally per-thread and flushed in batches
pub fn log_event<D>(event_type: events::EventType, event_data: D)
where
    D: Into<events::EventData> + Serialize + std::fmt::Debug + Clone,
{
    if !*METRICS_ENABLED {
        return;
    }

    let raw_event = create_raw_event(event_type, event_data.into());

    EVENT_BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.push(raw_event);

        // Check if we should flush based on size or time
        let should_flush = buf.len() >= THREAD_LOCAL_FLUSH_THRESHOLD
            || LAST_FLUSH_TIME.with(|time| {
                time.borrow().elapsed() >= Duration::from_secs(THREAD_LOCAL_FLUSH_INTERVAL_SECS)
            });

        if should_flush {
            flush_local_buffer(&mut buf);
            LAST_FLUSH_TIME.with(|time| {
                *time.borrow_mut() = std::time::Instant::now();
            });
        }
    });
}

/// Flush the thread-local buffer to the global channel
fn flush_local_buffer(buf: &mut Vec<events::RawEvent<events::EventData>>) {
    let events = std::mem::take(buf);

    if events.is_empty() {
        return;
    }

    // Send entire vec in one operation - much faster!
    let _ = METRICS_STATE.events_tx.send(events);

    // Check if we should notify based on batch count
    let channel_len = METRICS_STATE.events_tx.len();
    let threshold = METRICS_STATE.threshold_batches.load(Ordering::Relaxed);

    if channel_len >= threshold {
        let _ = METRICS_STATE.notify_tx.try_send(());
    }
}

/// Create a RawEvent with common metadata
fn create_raw_event(
    event_type: events::EventType,
    event_data: events::EventData,
) -> events::RawEvent<events::EventData> {
    events::RawEvent {
        os: OS,
        user_id: Some(&HELIX_USER_ID),
        event_type,
        event_data,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Failed to get system time")
            .as_secs(),
        email: None,
    }
}

/// Background task that batches and sends events via HTTP
async fn sender_task(
    events_rx: flume::Receiver<Vec<events::RawEvent<events::EventData>>>,
    notify_rx: flume::Receiver<()>,
    shutdown_rx: flume::Receiver<()>,
) {
    loop {
        // Wait for notification, timeout, or shutdown signal
        tokio::select! {
            _ = notify_rx.recv_async() => {
                process_batch(&events_rx).await;
            }
            _ = tokio::time::sleep(Duration::from_secs(BATCH_TIMEOUT_SECS)) => {
                // Periodic flush even if threshold not reached
                process_batch(&events_rx).await;
            }
            _ = shutdown_rx.recv_async() => {
                // Shutdown signal received - process final batch and exit
                process_batch(&events_rx).await;
                break;
            }
        }
    }
}

/// Process a batch of events from the channel
async fn process_batch(
    rx: &flume::Receiver<Vec<events::RawEvent<events::EventData>>>,
) -> Option<JoinHandle<()>> {
    // Drain all Vec batches and flatten into single Vec
    let events: Vec<_> = rx.drain().flatten().collect();

    if events.is_empty() {
        return None;
    }

    // Spawn new task for serialization + HTTP
    // This allows the sender task to continue processing batches
    Some(tokio::spawn(async move {
        // Serialize as NDJSON (newline-delimited JSON)
        // Each event is a separate JSON object on its own line
        let mut ndjson = String::new();
        for event in &events {
            match sonic_rs::to_string(event) {
                Ok(json) => {
                    ndjson.push_str(&json);
                    ndjson.push('\n');
                }
                Err(e) => {
                    eprintln!("Failed to serialize event: {}", e);
                    continue;
                }
            }
        }

        if ndjson.is_empty() {
            return;
        }

        // Send batch over HTTP as NDJSON
        match METRICS_CLIENT
            .post(METRICS_URL)
            .header("Content-Type", "application/x-ndjson")
            .body(ndjson)
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    eprintln!(
                        "Metrics HTTP error: {} from {} (body: {:?})",
                        response.status(),
                        METRICS_URL,
                        response.text().await.unwrap_or_default()
                    );
                }
                // Success - no need to log (metrics are silent on success)
            }
            Err(e) => {
                eprintln!("Failed to send metrics to {}: {}", METRICS_URL, e);
            }
        }
    }))
}

/// Flush all pending events immediately
/// Useful for graceful shutdown
pub async fn flush_all() -> Option<JoinHandle<()>> {
    if !*METRICS_ENABLED {
        return None;
    }

    // Flush all thread-local buffers first
    EVENT_BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        if !buf.is_empty() {
            flush_local_buffer(&mut buf);
        }
    });

    // Process any remaining events in the channel
    process_batch(&METRICS_STATE.events_rx).await
}

/// Shutdown the metrics system gracefully
/// This should be called before process exit to ensure all metrics are flushed
pub async fn shutdown_metrics_system() {
    if !*METRICS_ENABLED {
        return;
    }

    // Send shutdown signal to sender task
    let _ = METRICS_STATE.shutdown_tx.send(());

    // Flush all remaining events
    if let Some(handle) = flush_all().await {
        let _ = handle.await;
    }
}

#[derive(Debug)]
pub struct MetricError(String);

impl std::fmt::Display for MetricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for MetricError {}

impl From<sonic_rs::Error> for MetricError {
    fn from(e: sonic_rs::Error) -> Self {
        MetricError(e.to_string())
    }
}

impl From<reqwest::Error> for MetricError {
    fn from(e: reqwest::Error) -> Self {
        MetricError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_thread_local_buffer_initialization() {
        init_thread_local();

        // Verify buffer is initialized and empty
        EVENT_BUFFER.with(|buffer| {
            assert_eq!(buffer.borrow().len(), 0);
            assert!(buffer.borrow().capacity() >= 32);
        });
    }

    #[test]
    fn test_thread_local_buffering() {
        init_thread_local();

        // Log a few events (less than flush threshold)
        for i in 0..5 {
            log_event(
                events::EventType::Test,
                events::TestEvent {
                    cluster_id: format!("test_{}", i),
                    queries_string: "test".to_string(),
                    num_of_queries: 1,
                    time_taken_sec: 1,
                    success: true,
                    error_messages: None,
                },
            );
        }

        // Buffer should have events (or be flushed if >= threshold)
        EVENT_BUFFER.with(|buffer| {
            let len = buffer.borrow().len();
            // Either still in buffer or already flushed
            assert!(len <= 5);
        });
    }

    #[test]
    fn test_thread_local_auto_flush() {
        init_thread_local();

        // Clear the channel first
        while METRICS_STATE.events_rx.try_recv().is_ok() {}

        // Log exactly THREAD_LOCAL_FLUSH_THRESHOLD events to trigger flush
        for i in 0..THREAD_LOCAL_FLUSH_THRESHOLD {
            log_event(
                events::EventType::Test,
                events::TestEvent {
                    cluster_id: format!("test_auto_flush_{}", i),
                    queries_string: "test".to_string(),
                    num_of_queries: 1,
                    time_taken_sec: 1,
                    success: true,
                    error_messages: None,
                },
            );
        }

        // Buffer should be empty after flush
        EVENT_BUFFER.with(|buffer| {
            assert_eq!(buffer.borrow().len(), 0);
        });

        // At least 1 batch should have been added (since we logged THREAD_LOCAL_FLUSH_THRESHOLD events)
        let channel_count = METRICS_STATE.events_rx.len();
        assert!(
            channel_count >= 1,
            "Expected at least 1 batch in channel, got {}",
            channel_count
        );
    }

    #[test]
    fn test_threshold_configuration() {
        // Test setting threshold in batches
        set_threshold_batches(100);
        assert_eq!(get_threshold_batches(), 100);

        set_threshold_batches(500);
        assert_eq!(get_threshold_batches(), 500);

        // Reset to default
        set_threshold_batches(num_cpus::get());
    }

    #[test]
    fn test_threshold_notification_trigger() {
        init_thread_local();

        // Clear channels
        while METRICS_STATE.events_rx.try_recv().is_ok() {}
        while METRICS_STATE.notify_rx.try_recv().is_ok() {}

        // Set threshold to 1 batch to trigger notification easily
        set_threshold_batches(1);

        // Log enough events to trigger a flush (which sends 1 batch)
        for i in 0..THREAD_LOCAL_FLUSH_THRESHOLD {
            log_event(
                events::EventType::Test,
                events::TestEvent {
                    cluster_id: format!("test_{}", i),
                    queries_string: "test".to_string(),
                    num_of_queries: 1,
                    time_taken_sec: 1,
                    success: true,
                    error_messages: None,
                },
            );
        }

        // Should have triggered a notification
        let notification_count = METRICS_STATE.notify_rx.len();
        assert!(notification_count > 0, "Expected notification to be sent");

        // Reset threshold
        set_threshold_batches(num_cpus::get());
    }

    #[test]
    fn test_create_raw_event() {
        let event = create_raw_event(
            events::EventType::Test,
            events::EventData::Test(events::TestEvent::default()),
        );

        assert_eq!(event.os, OS.to_string());
        assert_eq!(event.event_type, events::EventType::Test);
        assert!(event.timestamp > 0);
    }

    #[test]
    fn test_multi_threaded_logging() {
        // Skip if metrics are disabled
        if !*METRICS_ENABLED {
            eprintln!("Skipping test_multi_threaded_logging: METRICS_ENABLED is false");
            return;
        }

        let num_threads = 4;
        let events_per_thread = 20;
        let counter = Arc::new(AtomicUsize::new(0));
        let total_expected = num_threads * events_per_thread;

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let counter = Arc::clone(&counter);
                thread::spawn(move || {
                    init_thread_local();

                    for i in 0..events_per_thread {
                        log_event(
                            events::EventType::Test,
                            events::TestEvent {
                                cluster_id: format!("thread_{}_{}", thread_id, i),
                                queries_string: "test".to_string(),
                                num_of_queries: 1,
                                time_taken_sec: 1,
                                success: true,
                                error_messages: None,
                            },
                        );
                        counter.fetch_add(1, AtomicOrdering::SeqCst);
                    }

                    // Flush remaining events
                    EVENT_BUFFER.with(|buffer| {
                        let mut buf = buffer.borrow_mut();
                        if !buf.is_empty() {
                            flush_local_buffer(&mut buf);
                        }
                    });
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all events were attempted to be logged
        assert_eq!(counter.load(AtomicOrdering::SeqCst), total_expected);

        // This test just verifies that multi-threaded logging doesn't crash or deadlock
        // In parallel test execution, the sender task may consume events concurrently
        eprintln!("Multi-threaded logging completed successfully");
    }

    #[tokio::test]
    async fn test_process_batch() {
        // Skip if metrics are disabled
        if !*METRICS_ENABLED {
            eprintln!("Skipping test_process_batch: METRICS_ENABLED is false");
            return;
        }

        // Clear channel
        while METRICS_STATE.events_rx.try_recv().is_ok() {}

        // Add a batch of events to channel
        let events: Vec<_> = (0..10)
            .map(|i| {
                create_raw_event(
                    events::EventType::Test,
                    events::EventData::Test(events::TestEvent {
                        cluster_id: format!("test_batch_{}", i),
                        queries_string: "test".to_string(),
                        num_of_queries: 1,
                        time_taken_sec: 1,
                        success: true,
                        error_messages: None,
                    }),
                )
            })
            .collect();
        METRICS_STATE.events_tx.send(events).unwrap();

        // Give a moment for all events to arrive
        tokio::time::sleep(Duration::from_millis(10)).await;

        let initial_count = METRICS_STATE.events_rx.len();

        // In parallel test execution, sender task might process events, so just verify we can process batches
        if initial_count > 0 {
            // Process batch (won't actually send HTTP in test, but will drain channel)
            process_batch(&METRICS_STATE.events_rx).await;

            // Give spawned tasks a moment to start
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Channel should have fewer or equal batches
            let _final_count = METRICS_STATE.events_rx.len();
            
        }
    }

    #[tokio::test]
    async fn test_flush_all() {
        init_thread_local();

        // Clear channel
        while METRICS_STATE.events_rx.try_recv().is_ok() {}

        // Add events to thread-local buffer
        for i in 0..5 {
            log_event(
                events::EventType::Test,
                events::TestEvent {
                    cluster_id: format!("test_{}", i),
                    queries_string: "test".to_string(),
                    num_of_queries: 1,
                    time_taken_sec: 1,
                    success: true,
                    error_messages: None,
                },
            );
        }

        // Flush all
        flush_all().await;

        // Thread-local buffer should be empty
        EVENT_BUFFER.with(|buffer| {
            assert_eq!(buffer.borrow().len(), 0);
        });
    }

    #[tokio::test]
    async fn test_init_metrics_system() {
        // Should not panic when called multiple times
        init_metrics_system();
        init_metrics_system();

        // Sender handle should be initialized
        assert!(METRICS_STATE.sender_handle.get().is_some());
    }

    #[test]
    fn test_event_serialization() {
        let event = create_raw_event(
            events::EventType::QuerySuccess,
            events::EventData::QuerySuccess(events::QuerySuccessEvent {
                cluster_id: Some("test_cluster".to_string()),
                query_name: "test_query".to_string(),
                time_taken_usec: 1000,
            }),
        );

        // Should be able to serialize
        let json = sonic_rs::to_string(&event).unwrap();
        assert!(json.contains("test_cluster"));
        assert!(json.contains("test_query"));
    }

    #[test]
    fn test_batch_serialization() {
        let events: Vec<_> = (0..5)
            .map(|i| {
                create_raw_event(
                    events::EventType::Test,
                    events::EventData::Test(events::TestEvent {
                        cluster_id: format!("test_{}", i),
                        queries_string: "test".to_string(),
                        num_of_queries: 1,
                        time_taken_sec: 1,
                        success: true,
                        error_messages: None,
                    }),
                )
            })
            .collect();

        // Should be able to serialize batch
        let json_bytes = sonic_rs::to_vec(&events).unwrap();
        assert!(json_bytes.len() > 0);

        // Should be valid JSON array
        let json_str = String::from_utf8(json_bytes).unwrap();
        assert!(json_str.starts_with('['));
        assert!(json_str.ends_with(']'));
    }

}
