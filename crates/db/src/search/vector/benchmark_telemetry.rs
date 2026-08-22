//! Feature-gated counters for the vector batch benchmark.
//!
//! The benchmark runs the unchanged typed vector transaction and mutation
//! paths. These process-local counters observe only call counts, encoded write
//! bytes, and existing cache statistics. They are excluded from normal builds
//! and never participate in a storage or graph decision.

use std::sync::atomic::{AtomicU64, Ordering};

use super::mutation::VectorBuildSessionStats;

#[derive(Default)]
struct Counters {
    point_get_calls: AtomicU64,
    multi_get_calls: AtomicU64,
    multi_get_keys: AtomicU64,
    scan_calls: AtomicU64,
    put_calls: AtomicU64,
    delete_calls: AtomicU64,
    staged_write_bytes: AtomicU64,
    item_hits: AtomicU64,
    item_misses: AtomicU64,
    neighbor_hits: AtomicU64,
    neighbor_misses: AtomicU64,
    simhash_hits: AtomicU64,
    simhash_misses: AtomicU64,
    item_evictions: AtomicU64,
    neighbor_evictions: AtomicU64,
    simhash_evictions: AtomicU64,
    dirty_neighbor_flushes: AtomicU64,
    peak_retained_payload_bytes: AtomicU64,
}

static COUNTERS: Counters = Counters {
    point_get_calls: AtomicU64::new(0),
    multi_get_calls: AtomicU64::new(0),
    multi_get_keys: AtomicU64::new(0),
    scan_calls: AtomicU64::new(0),
    put_calls: AtomicU64::new(0),
    delete_calls: AtomicU64::new(0),
    staged_write_bytes: AtomicU64::new(0),
    item_hits: AtomicU64::new(0),
    item_misses: AtomicU64::new(0),
    neighbor_hits: AtomicU64::new(0),
    neighbor_misses: AtomicU64::new(0),
    simhash_hits: AtomicU64::new(0),
    simhash_misses: AtomicU64::new(0),
    item_evictions: AtomicU64::new(0),
    neighbor_evictions: AtomicU64::new(0),
    simhash_evictions: AtomicU64::new(0),
    dirty_neighbor_flushes: AtomicU64::new(0),
    peak_retained_payload_bytes: AtomicU64::new(0),
};

/// One complete benchmark observation for the typed vector mutation boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VectorMutationBenchmarkTelemetry {
    pub point_get_calls: u64,
    pub multi_get_calls: u64,
    pub multi_get_keys: u64,
    pub scan_calls: u64,
    pub put_calls: u64,
    pub delete_calls: u64,
    pub staged_write_bytes: u64,
    pub item_hits: u64,
    pub item_misses: u64,
    pub neighbor_hits: u64,
    pub neighbor_misses: u64,
    pub simhash_hits: u64,
    pub simhash_misses: u64,
    pub item_evictions: u64,
    pub neighbor_evictions: u64,
    pub simhash_evictions: u64,
    pub dirty_neighbor_flushes: u64,
    pub peak_retained_payload_bytes: u64,
}

pub(crate) fn reset() {
    for counter in [
        &COUNTERS.point_get_calls,
        &COUNTERS.multi_get_calls,
        &COUNTERS.multi_get_keys,
        &COUNTERS.scan_calls,
        &COUNTERS.put_calls,
        &COUNTERS.delete_calls,
        &COUNTERS.staged_write_bytes,
        &COUNTERS.item_hits,
        &COUNTERS.item_misses,
        &COUNTERS.neighbor_hits,
        &COUNTERS.neighbor_misses,
        &COUNTERS.simhash_hits,
        &COUNTERS.simhash_misses,
        &COUNTERS.item_evictions,
        &COUNTERS.neighbor_evictions,
        &COUNTERS.simhash_evictions,
        &COUNTERS.dirty_neighbor_flushes,
        &COUNTERS.peak_retained_payload_bytes,
    ] {
        counter.store(0, Ordering::Relaxed);
    }
}

pub(crate) fn snapshot() -> VectorMutationBenchmarkTelemetry {
    VectorMutationBenchmarkTelemetry {
        point_get_calls: COUNTERS.point_get_calls.load(Ordering::Relaxed),
        multi_get_calls: COUNTERS.multi_get_calls.load(Ordering::Relaxed),
        multi_get_keys: COUNTERS.multi_get_keys.load(Ordering::Relaxed),
        scan_calls: COUNTERS.scan_calls.load(Ordering::Relaxed),
        put_calls: COUNTERS.put_calls.load(Ordering::Relaxed),
        delete_calls: COUNTERS.delete_calls.load(Ordering::Relaxed),
        staged_write_bytes: COUNTERS.staged_write_bytes.load(Ordering::Relaxed),
        item_hits: COUNTERS.item_hits.load(Ordering::Relaxed),
        item_misses: COUNTERS.item_misses.load(Ordering::Relaxed),
        neighbor_hits: COUNTERS.neighbor_hits.load(Ordering::Relaxed),
        neighbor_misses: COUNTERS.neighbor_misses.load(Ordering::Relaxed),
        simhash_hits: COUNTERS.simhash_hits.load(Ordering::Relaxed),
        simhash_misses: COUNTERS.simhash_misses.load(Ordering::Relaxed),
        item_evictions: COUNTERS.item_evictions.load(Ordering::Relaxed),
        neighbor_evictions: COUNTERS.neighbor_evictions.load(Ordering::Relaxed),
        simhash_evictions: COUNTERS.simhash_evictions.load(Ordering::Relaxed),
        dirty_neighbor_flushes: COUNTERS.dirty_neighbor_flushes.load(Ordering::Relaxed),
        peak_retained_payload_bytes: COUNTERS.peak_retained_payload_bytes.load(Ordering::Relaxed),
    }
}

pub(crate) fn record_point_get() {
    COUNTERS.point_get_calls.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_multi_get(keys: usize) {
    COUNTERS.multi_get_calls.fetch_add(1, Ordering::Relaxed);
    COUNTERS
        .multi_get_keys
        .fetch_add(u64::try_from(keys).unwrap_or(u64::MAX), Ordering::Relaxed);
}

pub(crate) fn record_scan() {
    COUNTERS.scan_calls.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_put(key_bytes: usize, value_bytes: usize) {
    COUNTERS.put_calls.fetch_add(1, Ordering::Relaxed);
    COUNTERS.staged_write_bytes.fetch_add(
        u64::try_from(key_bytes.saturating_add(value_bytes)).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
}

pub(crate) fn record_delete(key_bytes: usize) {
    COUNTERS.delete_calls.fetch_add(1, Ordering::Relaxed);
    COUNTERS.staged_write_bytes.fetch_add(
        u64::try_from(key_bytes).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
}

pub(crate) fn record_cache_stats(stats: VectorBuildSessionStats) {
    COUNTERS
        .item_hits
        .fetch_add(stats.item_hits(), Ordering::Relaxed);
    COUNTERS
        .item_misses
        .fetch_add(stats.item_misses(), Ordering::Relaxed);
    COUNTERS
        .neighbor_hits
        .fetch_add(stats.neighbor_hits(), Ordering::Relaxed);
    COUNTERS
        .neighbor_misses
        .fetch_add(stats.neighbor_misses(), Ordering::Relaxed);
    COUNTERS
        .simhash_hits
        .fetch_add(stats.simhash_hits(), Ordering::Relaxed);
    COUNTERS
        .simhash_misses
        .fetch_add(stats.simhash_misses(), Ordering::Relaxed);
    COUNTERS
        .item_evictions
        .fetch_add(stats.item_evictions(), Ordering::Relaxed);
    COUNTERS
        .neighbor_evictions
        .fetch_add(stats.neighbor_evictions(), Ordering::Relaxed);
    COUNTERS
        .simhash_evictions
        .fetch_add(stats.simhash_evictions(), Ordering::Relaxed);
    observe_retained_payload(stats.max_retained_payload_bytes());
}

pub(crate) fn record_dirty_neighbor_flush() {
    COUNTERS
        .dirty_neighbor_flushes
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn observe_retained_payload(bytes: u64) {
    COUNTERS
        .peak_retained_payload_bytes
        .fetch_max(bytes, Ordering::Relaxed);
}
