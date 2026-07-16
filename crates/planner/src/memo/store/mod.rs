//! Mutable Cascades memo container.

mod insert;
mod serialization;

use serde::Serialize;

use super::ids::{MemoExprId, MemoGroupId};
use super::index::MemoIndexes;
use super::records::{MemoError, MemoExpr, MemoGroup};

/// Minimal Cascades memo container.
///
/// The serialized surface is only the ordered memo records. Dense ID indexes
/// are rebuilt on deserialization, making hot-path lookups direct while keeping
/// invalid sparse or duplicate IDs outside the runtime representation.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Memo {
    groups: Vec<MemoGroup>,
    #[serde(skip)]
    next_group_id: Option<MemoGroupId>,
    #[serde(skip)]
    next_expr_id: Option<MemoExprId>,
    #[serde(skip)]
    indexes: MemoIndexes,
}

impl Default for Memo {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            next_group_id: Some(MemoGroupId::first()),
            next_expr_id: Some(MemoExprId::first()),
            indexes: MemoIndexes::default(),
        }
    }
}

impl Memo {
    /// Number of memo groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Number of memo expressions.
    pub fn expression_count(&self) -> usize {
        self.indexes.expr_count()
    }

    /// Borrow groups.
    pub fn groups(&self) -> &[MemoGroup] {
        &self.groups
    }

    /// True when the memo owns the group ID.
    pub(crate) fn contains_group(&self, group: MemoGroupId) -> bool {
        self.group_index(group).is_ok()
    }

    /// Borrow a memo expression by stable ID.
    pub fn expression(&self, id: MemoExprId) -> Option<&MemoExpr> {
        let location = self.indexes.expr_location(id)?;
        self.groups
            .get(location.group_index())?
            .expressions
            .get(location.expr_index())
            .filter(|expr| expr.id == id)
    }

    pub(super) fn group_index(&self, group: MemoGroupId) -> Result<usize, MemoError> {
        let index = MemoIndexes::group_index(group);
        self.groups
            .get(index)
            .filter(|candidate| candidate.id == group)
            .map(|_| index)
            .ok_or(MemoError::MissingGroup { group })
    }
}

#[cfg(test)]
impl Memo {
    pub(super) fn with_next_ids(
        next_group_id: Option<MemoGroupId>,
        next_expr_id: Option<MemoExprId>,
    ) -> Self {
        Self {
            next_group_id,
            next_expr_id,
            ..Self::default()
        }
    }

    pub(super) fn set_next_expr_id(&mut self, next_expr_id: Option<MemoExprId>) {
        self.next_expr_id = next_expr_id;
    }
}
