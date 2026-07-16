//! Ordered memo-child lineage contracts.

use serde::{Deserialize, Serialize};

use super::ids::MemoGroupId;

/// Ordered child memo groups referenced by one memo expression.
///
/// Empty child lists are valid for leaf expressions. Duplicate group IDs are
/// deliberately preserved because future recursive expressions may use the
/// same child group more than once in different argument positions.
///
/// ```
/// use helix_planner::memo::{MemoChildGroups, MemoGroupId};
///
/// let first = MemoGroupId::new(1).unwrap();
/// let children = MemoChildGroups::new(vec![first, first]);
///
/// assert_eq!(children.len(), 2);
/// assert_eq!(children.summary(), "[1,1]");
/// assert!(MemoChildGroups::empty().is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemoChildGroups {
    children: Vec<MemoGroupId>,
}

impl MemoChildGroups {
    /// Build ordered child groups.
    pub fn new(children: Vec<MemoGroupId>) -> Self {
        Self { children }
    }

    /// Empty child-group list.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Borrow child groups.
    pub fn as_slice(&self) -> &[MemoGroupId] {
        &self.children
    }

    /// Iterate over child groups.
    pub fn iter(&self) -> impl Iterator<Item = &MemoGroupId> {
        self.children.iter()
    }

    /// Number of child groups.
    pub fn len(&self) -> usize {
        self.children.len()
    }

    /// True when no child groups are referenced.
    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    /// Consume into the underlying ordered group list.
    pub fn into_vec(self) -> Vec<MemoGroupId> {
        self.children
    }

    /// Stable compact trace summary.
    pub fn summary(&self) -> String {
        let children = self
            .children
            .iter()
            .map(|child| child.get().to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!("[{children}]")
    }
}

impl From<Vec<MemoGroupId>> for MemoChildGroups {
    fn from(children: Vec<MemoGroupId>) -> Self {
        Self::new(children)
    }
}
