//! Runtime expression matching for rule applicability contracts.
//!
//! Rule metadata and schedule compilation use the same matching semantics. This
//! module keeps the expression dispatch separate from the applicability enum
//! and from the candidate predicate helpers.

use super::{candidates, RuleApplicability};
use crate::logical;

impl RuleApplicability {
    /// True when a rule with this applicability may inspect `expr`.
    pub fn matches(&self, expr: &logical::LogicalExpr) -> bool {
        match self {
            Self::Any => true,
            Self::LogicalKinds(kinds) => kinds.contains(expr.kind()),
            Self::PureOpKinds(kinds) => match expr {
                logical::LogicalExpr::Pure(op) => kinds.contains(op.kind()),
                _ => false,
            },
            Self::PurePipelineLocalSimplification => match expr {
                logical::LogicalExpr::PurePipeline(pipeline) => {
                    pipeline.has_local_simplification_candidate()
                }
                _ => false,
            },
            Self::PurePipelineStaticWindowComposition => match expr {
                logical::LogicalExpr::PurePipeline(pipeline) => {
                    pipeline.has_static_window_composition_candidate()
                }
                _ => false,
            },
            Self::AccessPipelineHeadOpKinds(kinds) => match expr {
                logical::LogicalExpr::AccessPipeline(pipeline) => {
                    kinds.contains(pipeline.head_op_kind())
                }
                _ => false,
            },
            Self::AccessPipelineLocalSimplification => match expr {
                logical::LogicalExpr::AccessPipeline(pipeline) => {
                    pipeline.has_local_simplification_candidate()
                }
                _ => false,
            },
            Self::AccessWindowRewriteCandidate => match expr {
                logical::LogicalExpr::AccessWindow(window) => window.has_rewrite_candidate(),
                _ => false,
            },
            Self::AccessFilterSimplificationCandidate => match expr {
                logical::LogicalExpr::AccessFilter(filter) => {
                    candidates::access_filter_has_simplification_candidate(filter)
                }
                _ => false,
            },
            Self::AccessFilterIndexCandidate => match expr {
                logical::LogicalExpr::AccessFilter(filter) => {
                    candidates::access_filter_has_index_candidate(filter)
                }
                _ => false,
            },
            Self::AccessOrderElisionCandidate => match expr {
                logical::LogicalExpr::AccessOrder(order) => order.has_order_elision_candidate(),
                _ => false,
            },
            Self::AccessOrderRangeDirectionCandidate => match expr {
                logical::LogicalExpr::AccessOrder(order) => order.has_range_direction_candidate(),
                _ => false,
            },
            Self::AccessDistinctNoopCandidate => match expr {
                logical::LogicalExpr::AccessDistinct(distinct) => distinct.has_noop_candidate(),
                _ => false,
            },
            Self::AccessSetCanonicalizationCandidate => match expr {
                logical::LogicalExpr::AccessPath(access) => {
                    candidates::access_path_has_set_canonicalization_candidate(access)
                }
                _ => false,
            },
            Self::AccessSetSubsumptionCandidate => match expr {
                logical::LogicalExpr::AccessPath(access) => {
                    candidates::access_path_has_set_subsumption_candidate(access)
                }
                _ => false,
            },
            Self::AccessRangeIntersectionCandidate => match expr {
                logical::LogicalExpr::AccessPath(access) => {
                    candidates::access_path_has_range_intersection_candidate(access)
                }
                _ => false,
            },
            Self::AccessEqualityRangeIntersectionCandidate => match expr {
                logical::LogicalExpr::AccessPath(access) => {
                    candidates::access_path_has_equality_range_intersection_candidate(access)
                }
                _ => false,
            },
            Self::AccessEqualityRangeUnionCandidate => match expr {
                logical::LogicalExpr::AccessPath(access) => {
                    candidates::access_path_has_equality_range_union_candidate(access)
                }
                _ => false,
            },
            Self::AccessContradictionCandidate => match expr {
                logical::LogicalExpr::AccessPath(access) => {
                    candidates::access_path_has_contradiction_candidate(access)
                }
                _ => false,
            },
            Self::RootControlFlowEmptyInputCandidate => match expr {
                logical::LogicalExpr::RootBranch(branch) => {
                    candidates::root_branch_has_empty_input(branch)
                }
                logical::LogicalExpr::RootRepeat(repeat) => {
                    candidates::root_repeat_has_empty_input(repeat)
                }
                _ => false,
            },
            Self::RootBranchImplementationCandidate => match expr {
                logical::LogicalExpr::RootBranch(branch) => {
                    !candidates::root_branch_has_empty_input(branch)
                }
                _ => false,
            },
            Self::RootRepeatImplementationCandidate => match expr {
                logical::LogicalExpr::RootRepeat(repeat) => {
                    !candidates::root_repeat_has_empty_input(repeat)
                }
                _ => false,
            },
            Self::AccessSourceKinds(kinds) => match expr {
                logical::LogicalExpr::AccessPath(access) => kinds.contains(access.source_kind()),
                _ => false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ir, properties};

    fn pure_noop() -> logical::LogicalExpr {
        logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp)
    }

    fn node_access(plan: ir::NodeAccessPlan) -> logical::LogicalExpr {
        logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(plan).unwrap(),
        )))
    }

    #[test]
    fn access_source_kind_matching_requires_access_path_with_matching_source_kind() {
        let scan = node_access(ir::NodeAccessPlan::AllScan);

        assert!(
            RuleApplicability::access_source_only(logical::AccessSourceKind::Scan).matches(&scan)
        );
        assert!(
            !RuleApplicability::access_source_only(logical::AccessSourceKind::Empty).matches(&scan)
        );
        assert!(
            !RuleApplicability::access_source_only(logical::AccessSourceKind::Scan)
                .matches(&pure_noop())
        );
    }

    #[test]
    fn shape_specific_matching_rejects_unrelated_expression_families() {
        let pure = pure_noop();
        let access = node_access(ir::NodeAccessPlan::AllScan);

        assert!(!RuleApplicability::pure_only(logical::PureLogicalOpKind::Filter).matches(&access));
        assert!(!RuleApplicability::pure_pipeline_local_simplification().matches(&pure));
        assert!(!RuleApplicability::pure_pipeline_static_window_composition().matches(&pure));
        assert!(!RuleApplicability::access_pipeline_head_only(
            logical::StreamPipelineOpKind::Filter,
        )
        .matches(&pure));
        assert!(!RuleApplicability::access_pipeline_local_simplification().matches(&pure));
        assert!(!RuleApplicability::access_window_rewrite_candidate().matches(&pure));
        assert!(!RuleApplicability::access_filter_simplification_candidate().matches(&pure));
        assert!(!RuleApplicability::access_filter_index_candidate().matches(&pure));
        assert!(!RuleApplicability::access_order_elision_candidate().matches(&pure));
        assert!(!RuleApplicability::access_order_range_direction_candidate().matches(&pure));
        assert!(!RuleApplicability::access_distinct_noop_candidate().matches(&pure));
        assert!(!RuleApplicability::access_set_canonicalization_candidate().matches(&pure));
        assert!(!RuleApplicability::access_set_subsumption_candidate().matches(&pure));
        assert!(!RuleApplicability::access_range_intersection_candidate().matches(&pure));
        assert!(!RuleApplicability::access_equality_range_intersection_candidate().matches(&pure));
        assert!(!RuleApplicability::access_equality_range_union_candidate().matches(&pure));
        assert!(!RuleApplicability::access_contradiction_candidate().matches(&pure));
        assert!(!RuleApplicability::root_control_flow_empty_input_candidate().matches(&pure));
        assert!(!RuleApplicability::root_branch_implementation_candidate().matches(&pure));
        assert!(!RuleApplicability::root_repeat_implementation_candidate().matches(&pure));
        assert!(!RuleApplicability::only(logical::LogicalExprKind::AccessPipeline).matches(&pure));
        assert!(RuleApplicability::any().matches(&pure));

        let source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        });
        assert!(RuleApplicability::pure_only(logical::PureLogicalOpKind::Source).matches(&source));
    }
}
