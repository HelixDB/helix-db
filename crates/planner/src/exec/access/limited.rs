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
    /// Access with a literal or runtime read limit.
    Limited(ExecLimitedAccessPlan),
}

impl ExecAccessPlan {
    /// Apply an access bound without discarding nested runtime validation.
    pub fn limited_by(self, limit: ExecAccessLimit) -> Self {
        Self::Limited(ExecLimitedAccessPlan::new_bound(self, limit))
    }

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

/// Native executable access plus a typed read limit.
///
/// Adjacent positive literals are tightened during construction. Runtime layers
/// remain nested and are validated from inner to outer before scanning, even
/// when an outer bound is zero.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecLimitedAccessPlan {
    source: Box<ExecAccessPlan>,
    limit: ExecAccessLimit,
}

impl ExecLimitedAccessPlan {
    /// Build a limited access plan, preserving the tightest nested bound.
    pub fn new(source: ExecAccessPlan, limit: properties::PositiveUsize) -> Self {
        Self::new_bound(source, ExecAccessLimit::Static(limit))
    }

    /// Keep runtime layers nested so evaluation retains inner-to-outer order.
    pub fn new_bound(source: ExecAccessPlan, limit: ExecAccessLimit) -> Self {
        match (source, limit) {
            (ExecAccessPlan::Limited(existing), ExecAccessLimit::Static(outer)) => {
                let ExecAccessLimit::Static(inner) = &existing.limit else {
                    return Self {
                        source: Box::new(ExecAccessPlan::Limited(existing)),
                        limit: ExecAccessLimit::Static(outer),
                    };
                };
                Self {
                    limit: ExecAccessLimit::Static(tightest_limit(*inner, outer)),
                    source: existing.source,
                }
            }
            (source, limit) => Self {
                source: Box::new(source),
                limit,
            },
        }
    }

    /// Source access.
    pub fn source(&self) -> &ExecAccessPlan {
        &self.source
    }

    /// Bound evaluated before access.
    pub const fn limit(&self) -> &ExecAccessLimit {
        &self.limit
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

/// A planner-selected access bound. Dynamic bounds must be evaluated before
/// access and must never be removed using a static cardinality estimate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecAccessLimit {
    /// A literal zero; inner runtime bounds still require validation.
    Zero,
    /// A positive literal bound.
    Static(properties::PositiveUsize),
    /// A runtime expression, including a bound that may resolve to zero.
    Dynamic(crate::ir::StreamBoundExprPlan),
}

impl ExecAccessLimit {
    /// Preserve the logical bound without resolving runtime parameters.
    pub fn from_stream_bound(bound: &crate::ir::StreamBoundPlan) -> Self {
        match bound {
            crate::ir::StreamBoundPlan::Literal(0) => Self::Zero,
            crate::ir::StreamBoundPlan::Literal(value) => {
                Self::Static(properties::PositiveUsize::at_least_one(*value))
            }
            crate::ir::StreamBoundPlan::Expr(expr) => Self::Dynamic(expr.clone()),
        }
    }

    /// Known bound, if available. Runtime expressions are never cardinality facts.
    pub const fn literal(&self) -> Option<usize> {
        match self {
            Self::Zero => Some(0),
            Self::Static(value) => Some(value.get()),
            Self::Dynamic(_) => None,
        }
    }
}
