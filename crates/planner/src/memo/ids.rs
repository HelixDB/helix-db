//! Non-zero memo identifier contracts.

use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

/// Cascades memo group ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemoGroupId(NonZeroUsize);

impl MemoGroupId {
    /// Build a memo group ID, rejecting zero.
    pub fn new(value: usize) -> Option<Self> {
        NonZeroUsize::new(value).map(Self)
    }

    /// First stable memo group ID.
    ///
    /// ```
    /// use helix_planner::memo::MemoGroupId;
    ///
    /// assert_eq!(MemoGroupId::first().get(), 1);
    /// assert_eq!(MemoGroupId::first().next().unwrap().get(), 2);
    /// ```
    pub const fn first() -> Self {
        Self(NonZeroUsize::MIN)
    }

    /// Next stable memo group ID, returning `None` only if the `usize` ID
    /// space is exhausted.
    pub fn next(self) -> Option<Self> {
        self.get().checked_add(1).and_then(Self::new)
    }

    /// Return the positive integer ID.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Cascades memo expression ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemoExprId(NonZeroUsize);

impl MemoExprId {
    /// Build a memo expression ID, rejecting zero.
    pub fn new(value: usize) -> Option<Self> {
        NonZeroUsize::new(value).map(Self)
    }

    /// First stable memo expression ID.
    ///
    /// ```
    /// use helix_planner::memo::MemoExprId;
    ///
    /// assert_eq!(MemoExprId::first().get(), 1);
    /// assert_eq!(MemoExprId::first().next().unwrap().get(), 2);
    /// ```
    pub const fn first() -> Self {
        Self(NonZeroUsize::MIN)
    }

    /// Next stable memo expression ID, returning `None` only if the `usize` ID
    /// space is exhausted.
    pub fn next(self) -> Option<Self> {
        self.get().checked_add(1).and_then(Self::new)
    }

    /// Return the positive integer ID.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Physical alternative ID retained for a memo group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PhysicalAlternativeId(NonZeroUsize);

impl PhysicalAlternativeId {
    /// Build a physical alternative ID, rejecting zero.
    pub fn new(value: usize) -> Option<Self> {
        NonZeroUsize::new(value).map(Self)
    }

    /// Yield stable one-based physical alternative IDs.
    ///
    /// ```
    /// use helix_planner::memo::PhysicalAlternativeId;
    ///
    /// let ids = PhysicalAlternativeId::sequential()
    ///     .take(3)
    ///     .map(PhysicalAlternativeId::get)
    ///     .collect::<Vec<_>>();
    ///
    /// assert_eq!(ids, vec![1, 2, 3]);
    /// ```
    pub fn sequential() -> impl Iterator<Item = Self> {
        let mut next = Some(NonZeroUsize::MIN);
        std::iter::from_fn(move || {
            let current = next?;
            next = current.get().checked_add(1).and_then(NonZeroUsize::new);
            Some(Self(current))
        })
    }

    /// Return the positive integer ID.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}
