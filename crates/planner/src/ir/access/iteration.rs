//! Physical iteration is independent of the persisted range-index direction.

use helix_ast::index::RangeIndexDirection;
use serde::{Deserialize, Serialize};

/// Traversal direction within an existing physical range lane.
///
/// Reversing iteration also reverses entity IDs within equal-value groups;
/// execution must restore ascending ID ties before advertising this ordering.
///
/// ```
/// use helix_ast::index::RangeIndexDirection::{Asc, Desc};
/// use helix_planner::ir::RangeScanIteration::{Forward, Reverse};
/// assert_eq!(Forward.effective_direction(Asc), Asc);
/// assert_eq!(Forward.effective_direction(Desc), Desc);
/// assert_eq!(Reverse.effective_direction(Asc), Desc);
/// assert_eq!(Reverse.effective_direction(Desc), Asc);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeScanIteration {
    /// Visit physical keys in ascending byte order.
    #[default]
    Forward,
    /// Visit physical keys in descending byte order.
    Reverse,
}

impl RangeScanIteration {
    /// Value ordering produced by this iteration over the physical lane.
    pub const fn effective_direction(self, physical: RangeIndexDirection) -> RangeIndexDirection {
        match (self, physical) {
            (Self::Forward, direction) => direction,
            (Self::Reverse, RangeIndexDirection::Asc) => RangeIndexDirection::Desc,
            (Self::Reverse, RangeIndexDirection::Desc) => RangeIndexDirection::Asc,
        }
    }
}
