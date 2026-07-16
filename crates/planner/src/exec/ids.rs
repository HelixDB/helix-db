//! Step identifiers and scheduling contracts for executable DAG steps.

use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use crate::properties;

/// Stable executable DAG step ID.
///
/// ```
/// use helix_planner::exec::ExecStepId;
///
/// assert!(ExecStepId::new(0).is_none());
/// assert_eq!(ExecStepId::new(1).unwrap().get(), 1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExecStepId(NonZeroUsize);

impl ExecStepId {
    /// Build an executable step ID, rejecting zero.
    pub fn new(value: usize) -> Option<Self> {
        NonZeroUsize::new(value).map(Self)
    }

    /// First stable executable DAG step ID.
    ///
    /// ```
    /// use helix_planner::exec::ExecStepId;
    ///
    /// assert_eq!(ExecStepId::first().get(), 1);
    /// assert_eq!(ExecStepId::first().next().unwrap().get(), 2);
    /// ```
    pub const fn first() -> Self {
        Self(NonZeroUsize::MIN)
    }

    /// Next stable executable DAG step ID, returning `None` only if the
    /// `usize` ID space is exhausted.
    pub fn next(self) -> Option<Self> {
        self.get().checked_add(1).and_then(Self::new)
    }

    /// Return the positive integer ID.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Execution schedule for a DAG step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecSchedule {
    /// Pipeline after dependencies complete.
    Pipeline,
    /// Explicit materialization or side-effect barrier.
    Barrier,
    /// Dependencies may run in parallel.
    Parallel {
        /// Positive concurrency bound.
        max_concurrency: properties::PositiveUsize,
        /// Whether dependency output order must be restored.
        preserve_order: bool,
    },
}
