//! Pure logical rewrite contracts.
//!
//! These ADTs represent rule candidates that cannot carry observable effects,
//! such as pure operator pipelines, adjacent filter chains, and safe filter
//! pushdown pairs.

use serde::{Deserialize, Serialize};

use super::{PureLogicalOp, PureLogicalOpKind};
use crate::{ir, properties};

/// Non-empty side-effect-free operator pipeline.
///
/// The pipeline contains only [`PureLogicalOp`] values, so effectful barriers
/// cannot be embedded in a stream rewrite candidate.
///
/// ```
/// use helix_planner::ir::AtLeast;
/// use helix_planner::logical::{PureLogicalOp, PurePipeline};
/// use helix_planner::properties::ElementKind;
///
/// let pipeline = PurePipeline::new(AtLeast::<_, 1>::from_one(PureLogicalOp::Source {
///     element: ElementKind::Node,
/// }));
///
/// assert_eq!(pipeline.ops().len(), 1);
/// assert_eq!(pipeline.ops_at_least().as_ref().len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PurePipeline {
    ops: ir::AtLeast<PureLogicalOp, 1>,
}

impl PurePipeline {
    /// Build a pure pipeline from one or more pure operators.
    pub fn new(ops: ir::AtLeast<PureLogicalOp, 1>) -> Self {
        Self { ops }
    }

    /// Pipeline operators in execution order.
    pub fn ops(&self) -> &[PureLogicalOp] {
        self.ops.as_ref()
    }

    /// Typed pipeline operators preserving the non-empty invariant.
    pub const fn ops_at_least(&self) -> &ir::AtLeast<PureLogicalOp, 1> {
        &self.ops
    }

    /// True when local pure-pipeline simplification should inspect this
    /// pipeline.
    ///
    /// This is a conservative optimizer-scheduling predicate: `true` means the
    /// simplification rule may rewrite the pipeline, while `false` means the
    /// rule cannot remove or collapse any operator.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, StreamBoundPlan};
    /// use helix_planner::logical::{PureLogicalOp, PurePipeline};
    ///
    /// let pipeline = PurePipeline::new(AtLeast::<_, 1>::from_one(
    ///     PureLogicalOp::Skip {
    ///         count: StreamBoundPlan::Literal(0),
    ///     },
    /// ));
    ///
    /// assert!(pipeline.has_local_simplification_candidate());
    /// ```
    pub fn has_local_simplification_candidate(&self) -> bool {
        self.ops.iter().any(is_local_simplification_op)
            || has_adjacent_ops(self.ops(), PureLogicalOpKind::Distinct)
    }

    /// True when static stream-window composition should inspect this
    /// pipeline.
    ///
    /// Adjacent literal limit/skip/range operators can compose into a single
    /// window. Literal `skip(0)` is also a candidate because it can become an
    /// identity when composed with surrounding non-window operators.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, StreamBoundPlan};
    /// use helix_planner::logical::{PureLogicalOp, PurePipeline};
    ///
    /// let pipeline = PurePipeline::new(AtLeast::<_, 1>::from_one_and_rest(
    ///     PureLogicalOp::Skip {
    ///         count: StreamBoundPlan::Literal(2),
    ///     },
    ///     vec![PureLogicalOp::Limit {
    ///         count: StreamBoundPlan::Literal(5),
    ///     }],
    /// ));
    ///
    /// assert!(pipeline.has_static_window_composition_candidate());
    /// ```
    pub fn has_static_window_composition_candidate(&self) -> bool {
        self.ops.iter().any(is_identity_static_window_op)
            || self
                .ops
                .as_ref()
                .windows(2)
                .any(|window| is_static_window_op(&window[0]) && is_static_window_op(&window[1]))
    }
}

fn is_local_simplification_op(op: &PureLogicalOp) -> bool {
    matches!(
        op,
        PureLogicalOp::NoOp
            | PureLogicalOp::Empty
            | PureLogicalOp::Skip {
                count: ir::StreamBoundPlan::Literal(0)
            }
    )
}

fn has_adjacent_ops(ops: &[PureLogicalOp], kind: PureLogicalOpKind) -> bool {
    ops.windows(2)
        .any(|window| window[0].kind() == kind && window[1].kind() == kind)
}

fn is_static_window_op(op: &PureLogicalOp) -> bool {
    matches!(
        op,
        PureLogicalOp::Limit {
            count: ir::StreamBoundPlan::Literal(_)
        } | PureLogicalOp::Skip {
            count: ir::StreamBoundPlan::Literal(_)
        } | PureLogicalOp::Range {
            range: ir::StreamRangePlan::Literal(_)
        }
    )
}

fn is_identity_static_window_op(op: &PureLogicalOp) -> bool {
    matches!(
        op,
        PureLogicalOp::Skip {
            count: ir::StreamBoundPlan::Literal(0)
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::properties;

    fn pipeline(first: PureLogicalOp, rest: Vec<PureLogicalOp>) -> PurePipeline {
        PurePipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(first, rest))
    }

    fn source() -> PureLogicalOp {
        PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        }
    }

    fn limit(count: usize) -> PureLogicalOp {
        PureLogicalOp::Limit {
            count: ir::StreamBoundPlan::Literal(count),
        }
    }

    fn skip(count: usize) -> PureLogicalOp {
        PureLogicalOp::Skip {
            count: ir::StreamBoundPlan::Literal(count),
        }
    }

    #[test]
    fn pure_pipeline_local_simplification_candidate_is_conservative() {
        assert!(pipeline(PureLogicalOp::NoOp, vec![source()]).has_local_simplification_candidate());
        assert!(pipeline(skip(0), vec![source()]).has_local_simplification_candidate());
        assert!(
            pipeline(PureLogicalOp::Distinct, vec![PureLogicalOp::Distinct])
                .has_local_simplification_candidate()
        );
        assert!(pipeline(PureLogicalOp::Empty, vec![limit(3)]).has_local_simplification_candidate());
        assert!(!pipeline(source(), vec![limit(3)]).has_local_simplification_candidate());
    }

    #[test]
    fn pure_pipeline_static_window_candidate_tracks_composable_literal_windows() {
        assert!(pipeline(skip(2), vec![limit(5)]).has_static_window_composition_candidate());
        assert!(pipeline(source(), vec![skip(0)]).has_static_window_composition_candidate());
        assert!(!pipeline(source(), vec![limit(5)]).has_static_window_composition_candidate());
        assert!(!pipeline(source(), vec![PureLogicalOp::Distinct])
            .has_static_window_composition_candidate());
    }
}

/// Two or more adjacent residual filters.
///
/// The chain stores only validated predicates and has cardinality encoded in
/// the type, so the merge rule never has to handle an empty or singleton
/// candidate.
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_planner::ir::{AtLeast, PredicatePlan};
/// use helix_planner::logical::FilterChain;
///
/// let first = PredicatePlan::new(Predicate::eq("active", true)).unwrap();
/// let second = PredicatePlan::new(Predicate::eq("tenant", "acme")).unwrap();
/// let chain = FilterChain::new(AtLeast::<_, 2>::from_pair(first, second));
///
/// assert_eq!(chain.predicates().len(), 2);
/// assert!(matches!(
///     chain.merged_predicate().predicate(),
///     Predicate::And { predicates } if predicates.len() == 2
/// ));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterChain {
    predicates: ir::AtLeast<ir::PredicatePlan, 2>,
}

impl FilterChain {
    /// Build an adjacent filter chain from two or more predicates.
    pub fn new(predicates: ir::AtLeast<ir::PredicatePlan, 2>) -> Self {
        Self { predicates }
    }

    /// Chain predicates in execution order.
    pub fn predicates(&self) -> &[ir::PredicatePlan] {
        self.predicates.as_ref()
    }

    /// Merge the adjacent filters into one validated conjunctive predicate.
    pub fn merged_predicate(&self) -> ir::PredicatePlan {
        ir::PredicatePlan::conjunction(&self.predicates)
    }
}

/// Pure operators that preserve filter semantics when transposed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterPushdownOp {
    /// Explicit or logical ordering.
    Order {
        /// Required order to preserve after filtering.
        ordering: properties::RequiredOrdering,
    },
    /// Stream deduplication.
    Distinct,
}

impl FilterPushdownOp {
    fn into_pure_op(self) -> PureLogicalOp {
        match self {
            Self::Order { ordering } => PureLogicalOp::Order { ordering },
            Self::Distinct => PureLogicalOp::Distinct,
        }
    }
}

/// A filter immediately above a safe pure operator.
///
/// Only [`FilterPushdownOp`] targets can be represented, so unsafe cases such
/// as `limit -> filter`, `project -> filter`, and barriers are excluded before
/// the optimizer rule can run.
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_planner::ir::PredicatePlan;
/// use helix_planner::logical::{FilterPushdown, FilterPushdownOp};
///
/// let predicate = PredicatePlan::new(Predicate::eq("active", true)).unwrap();
/// let candidate = FilterPushdown::new(FilterPushdownOp::Distinct, predicate);
///
/// assert!(matches!(candidate.op(), FilterPushdownOp::Distinct));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterPushdown {
    op: FilterPushdownOp,
    predicate: ir::PredicatePlan,
}

impl FilterPushdown {
    /// Build a safe filter-pushdown candidate.
    pub fn new(op: FilterPushdownOp, predicate: ir::PredicatePlan) -> Self {
        Self { op, predicate }
    }

    /// Operator that the filter may commute through.
    pub const fn op(&self) -> &FilterPushdownOp {
        &self.op
    }

    /// Predicate to push below the operator.
    pub const fn predicate(&self) -> &ir::PredicatePlan {
        &self.predicate
    }

    pub(crate) fn into_pipeline_ops(self) -> ir::AtLeast<PureLogicalOp, 1> {
        ir::AtLeast::<_, 1>::from_one_and_rest(
            PureLogicalOp::Filter {
                predicate: self.predicate,
            },
            vec![self.op.into_pure_op()],
        )
    }
}
