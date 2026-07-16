use serde::{Deserialize, Serialize};

use super::{edge::ExecEdgeAccessPlan, node::ExecNodeAccessPlan};
use crate::properties;

/// Native executable graph access.
///
/// The outer node/edge split keeps element-kind mismatches unrepresentable for
/// index and search payloads.
///
/// ```
/// use helix_planner::exec::{ExecAccessPlan, ExecNodeAccessPlan};
///
/// let access = ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan);
/// assert!(matches!(access, ExecAccessPlan::Node(_)));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecAccessPlan {
    /// Node-producing access.
    Node(ExecNodeAccessPlan),
    /// Edge-producing access.
    Edge(ExecEdgeAccessPlan),
    /// Access with a positive read limit.
    Limited(ExecLimitedAccessPlan),
}

impl ExecAccessPlan {
    /// Return this access with a positive read limit.
    pub fn limited(self, limit: properties::PositiveUsize) -> Self {
        Self::Limited(ExecLimitedAccessPlan::new(self, limit))
    }
}

/// Optional executable access read limit.
///
/// Keeping the limit as an ADT instead of an `Option` makes the unbounded case
/// explicit at lowering boundaries, while [`properties::PositiveUsize`] keeps
/// zero unrepresentable.
///
/// ```
/// use helix_planner::exec::{ExecAccessPlan, ExecAccessReadLimit, ExecNodeAccessPlan};
/// use helix_planner::properties::PositiveUsize;
///
/// let access = ExecAccessPlan::Node(ExecNodeAccessPlan::AllScan);
/// let limited = ExecAccessReadLimit::bounded(PositiveUsize::at_least_one(4)).apply_to(access);
///
/// assert!(matches!(limited, ExecAccessPlan::Limited(_)));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExecAccessReadLimit {
    /// No static read limit is available.
    #[default]
    Unbounded,
    /// A positive static read limit is available.
    Bounded(properties::PositiveUsize),
}

impl ExecAccessReadLimit {
    /// Build a bounded read limit.
    pub const fn bounded(limit: properties::PositiveUsize) -> Self {
        Self::Bounded(limit)
    }

    /// Remove this explicit read limit when the access itself already proves an
    /// equal-or-tighter hard upper bound.
    ///
    /// ```
    /// use helix_planner::exec::ExecAccessReadLimit;
    /// use helix_planner::properties::PositiveUsize;
    ///
    /// let limit = ExecAccessReadLimit::bounded(PositiveUsize::at_least_one(5));
    ///
    /// assert_eq!(
    ///     limit.elide_if_covered_by_hard_upper(Some(1)),
    ///     ExecAccessReadLimit::Unbounded,
    /// );
    /// assert_eq!(
    ///     limit.elide_if_covered_by_hard_upper(Some(8)),
    ///     limit,
    /// );
    /// ```
    pub const fn elide_if_covered_by_hard_upper(self, hard_upper: Option<usize>) -> Self {
        match (self, hard_upper) {
            (Self::Bounded(limit), Some(upper)) if upper <= limit.get() => Self::Unbounded,
            _ => self,
        }
    }

    /// Apply this read-limit contract to native executable access.
    pub fn apply_to(self, access: ExecAccessPlan) -> ExecAccessPlan {
        match self {
            Self::Unbounded => access,
            Self::Bounded(limit) => access.limited(limit),
        }
    }
}

/// Native executable access plus a positive read limit.
///
/// Nested limits are flattened by construction, and zero is unrepresentable
/// because the limit is a [`properties::PositiveUsize`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecLimitedAccessPlan {
    source: Box<ExecAccessPlan>,
    limit: properties::PositiveUsize,
}

impl ExecLimitedAccessPlan {
    /// Build a limited access plan, preserving the tightest nested bound.
    pub fn new(source: ExecAccessPlan, limit: properties::PositiveUsize) -> Self {
        match source {
            ExecAccessPlan::Limited(existing) => Self {
                source: existing.source,
                limit: tightest_limit(existing.limit, limit),
            },
            source @ (ExecAccessPlan::Node(_) | ExecAccessPlan::Edge(_)) => Self {
                source: Box::new(source),
                limit,
            },
        }
    }

    /// Source access.
    pub fn source(&self) -> &ExecAccessPlan {
        &self.source
    }

    /// Positive read limit.
    pub const fn limit(&self) -> properties::PositiveUsize {
        self.limit
    }
}

fn tightest_limit(
    left: properties::PositiveUsize,
    right: properties::PositiveUsize,
) -> properties::PositiveUsize {
    if left <= right {
        left
    } else {
        right
    }
}
