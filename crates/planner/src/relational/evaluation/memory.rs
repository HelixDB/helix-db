//! Borrowed scalar admission with optional request-wide peak observation.
use super::{QueryError, Result};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A scalar allowance tracks retained siblings while evaluating a child.
/// Copies describe alternate evaluation branches; they do not reserve heap
/// ownership. The caller retains admission for inputs and completed outputs.
/// Each allowance describes one synchronous evaluation tree; concurrently
/// evaluated trees require separately admitted budgets.
///
/// ```
/// use helix_planner::relational::EvaluationMemory;
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// let peak = AtomicUsize::new(0);
/// let memory = EvaluationMemory::observed(1024, 128, &peak).unwrap();
/// assert_eq!(memory.available(), 896);
/// assert_eq!(memory.capped(256).available(), 256);
/// assert_eq!(peak.load(Ordering::Relaxed), 128);
/// assert!(EvaluationMemory::observed(100, 101, &peak).is_err());
/// ```
#[derive(Clone, Copy)]
pub struct Memory<'a> {
    available: usize,
    admitted: usize,
    peak: Option<&'a AtomicUsize>,
}

impl<'a> Memory<'a> {
    /// A standalone allowance with no shared peak observer.
    pub fn new(bytes: usize) -> Self {
        Self {
            available: bytes,
            admitted: 0,
            peak: None,
        }
    }

    /// Include already-admitted request owners in every scalar peak. The
    /// request retains those owners and this counter through evaluation.
    pub fn observed(limit: usize, used: usize, peak: &'a AtomicUsize) -> Result<Self> {
        let available = limit.checked_sub(used).ok_or_else(exhausted)?;
        peak.fetch_max(used, Ordering::Relaxed);
        Ok(Self {
            available,
            admitted: used,
            peak: Some(peak),
        })
    }

    pub fn available(self) -> usize {
        self.available
    }

    /// Keep headroom for a caller's later conversion without pretending
    /// that unused headroom is a live scalar allocation.
    pub fn capped(self, bytes: usize) -> Self {
        Self {
            available: self.available.min(bytes),
            ..self
        }
    }

    pub(super) fn remaining(self, bytes: usize) -> Result<Self> {
        let available = self
            .available
            .checked_sub(bytes)
            .filter(|_| bytes <= isize::MAX as usize)
            .ok_or_else(exhausted)?;
        let admitted = self
            .admitted
            .checked_add(bytes)
            .expect("child admission fits the validated allowance");
        let remaining = Self {
            available,
            admitted,
            ..self
        };
        let Some(peak) = self.peak else {
            return Ok(remaining);
        };
        peak.fetch_max(admitted, Ordering::Relaxed);
        Ok(remaining)
    }
}

fn exhausted() -> QueryError {
    QueryError::runtime(
        "ResourceLimit",
        "MemoryLimit",
        "expression temporaries exceed the query memory budget",
    )
}

#[cfg(test)]
#[path = "tests/memory.rs"]
mod tests;
