//! Request-local work counters used by regression tests and release benchmarks.
//! These count logical visits and API reads, not SlateDB block/cache read-ahead.
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub(in crate::execution::interpreter) struct WorkCounters {
    pub raw_gets: AtomicUsize,
    pub multi_get_keys: AtomicUsize,
    pub source_visits: AtomicUsize,
    pub expansion_parents: AtomicUsize,
    pub pair_reads: AtomicUsize,
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::execution::interpreter) struct WorkSnapshot {
    pub raw_gets: usize,
    pub multi_get_keys: usize,
    pub source_visits: usize,
    pub expansion_parents: usize,
    pub pair_reads: usize,
}

impl WorkCounters {
    pub fn snapshot(&self) -> WorkSnapshot {
        WorkSnapshot {
            raw_gets: self.raw_gets.load(Ordering::Relaxed),
            multi_get_keys: self.multi_get_keys.load(Ordering::Relaxed),
            source_visits: self.source_visits.load(Ordering::Relaxed),
            expansion_parents: self.expansion_parents.load(Ordering::Relaxed),
            pair_reads: self.pair_reads.load(Ordering::Relaxed),
        }
    }
}
