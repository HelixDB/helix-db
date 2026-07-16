//! Typed recursive physical-selection failures.

use serde::{Deserialize, Serialize};

use crate::{memo, properties};

/// Reason recursive physical selection failed for a memo group.
///
/// Selection is a contract boundary between exploration and selected executable
/// lowering, so failures stay typed instead of collapsing to `None`.
///
/// # Examples
///
/// ```
/// use helix_planner::{memo, optimizer};
///
/// let error = optimizer::SelectionError::NoPhysicalAlternatives {
///     group: memo::MemoGroupId::first(),
/// };
///
/// assert_eq!(error.to_string(), "memo group 1 has no physical alternatives");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectionError {
    /// The requested group does not exist in the memo.
    MissingMemoGroup {
        /// Requested memo group.
        group: memo::MemoGroupId,
    },
    /// The requested group has no retained implementation alternatives.
    NoPhysicalAlternatives {
        /// Requested memo group.
        group: memo::MemoGroupId,
    },
    /// The group has alternatives, but none satisfy the required properties.
    UnsatisfiedRequiredProperties {
        /// Requested memo group.
        group: memo::MemoGroupId,
        /// Required physical properties.
        required: properties::RequiredProperties,
    },
    /// A retained alternative points at a memo expression that is not present.
    MissingSourceExpression {
        /// Group being selected.
        group: memo::MemoGroupId,
        /// Retained physical alternative ID.
        alternative: memo::PhysicalAlternativeId,
        /// Missing memo expression ID.
        source_expr: memo::MemoExprId,
    },
    /// Recursive child selection re-entered a group already being selected.
    RecursiveSelectionCycle {
        /// Re-entered memo group.
        group: memo::MemoGroupId,
    },
    /// A parent alternative cannot be selected because a selected child group
    /// has no selectable implementation.
    ChildSelectionFailed {
        /// Parent memo group.
        parent_group: memo::MemoGroupId,
        /// Child memo group.
        child_group: memo::MemoGroupId,
        /// Child failure reason.
        reason: Box<SelectionError>,
    },
}

impl std::fmt::Display for SelectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingMemoGroup { group } => {
                write!(f, "memo group {} is missing", group.get())
            }
            Self::NoPhysicalAlternatives { group } => {
                write!(f, "memo group {} has no physical alternatives", group.get())
            }
            Self::UnsatisfiedRequiredProperties { group, required } => write!(
                f,
                "memo group {} has no alternative satisfying {:?}",
                group.get(),
                required
            ),
            Self::MissingSourceExpression {
                group,
                alternative,
                source_expr,
            } => write!(
                f,
                "memo group {} alternative {} references missing expression {}",
                group.get(),
                alternative.get(),
                source_expr.get()
            ),
            Self::RecursiveSelectionCycle { group } => write!(
                f,
                "recursive physical selection cycle at memo group {}",
                group.get()
            ),
            Self::ChildSelectionFailed {
                parent_group,
                child_group,
                reason,
            } => write!(
                f,
                "memo group {} child group {} selection failed: {}",
                parent_group.get(),
                child_group.get(),
                reason
            ),
        }
    }
}

impl std::error::Error for SelectionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_selection_failure_display_preserves_nested_reason() {
        let root = memo::MemoGroupId::first();
        let child = memo::MemoGroupId::new(2).unwrap();
        let error = SelectionError::ChildSelectionFailed {
            parent_group: root,
            child_group: child,
            reason: Box::new(SelectionError::NoPhysicalAlternatives { group: child }),
        };

        assert_eq!(
            error.to_string(),
            "memo group 1 child group 2 selection failed: memo group 2 has no physical alternatives"
        );
    }
}
