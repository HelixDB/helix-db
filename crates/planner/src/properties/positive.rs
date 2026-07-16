use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

/// Positive `usize` planner value.
///
/// Use this instead of plain `usize` for limits, IDs, and concurrency knobs
/// where zero would be an invalid state.
///
/// ```
/// use helix_planner::properties::PositiveUsize;
///
/// assert!(PositiveUsize::new(0).is_none());
/// assert_eq!(PositiveUsize::new(8).unwrap().get(), 8);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PositiveUsize(NonZeroUsize);

impl PositiveUsize {
    /// Build a positive value, returning `None` for zero.
    pub fn new(value: usize) -> Option<Self> {
        NonZeroUsize::new(value).map(Self)
    }

    /// Build a positive value, replacing zero with one.
    ///
    /// ```
    /// use helix_planner::properties::PositiveUsize;
    ///
    /// assert_eq!(PositiveUsize::at_least_one(0).get(), 1);
    /// assert_eq!(PositiveUsize::at_least_one(8).get(), 8);
    /// ```
    pub fn at_least_one(value: usize) -> Self {
        Self(NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN))
    }

    /// Return the underlying positive integer.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl From<NonZeroUsize> for PositiveUsize {
    fn from(value: NonZeroUsize) -> Self {
        Self(value)
    }
}
