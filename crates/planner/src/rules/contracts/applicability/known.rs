//! Known-rule applicability inventory.

use super::RuleApplicability;
use crate::{ir, logical, rules::KnownRuleId};

pub(super) fn for_known_rule(id: KnownRuleId) -> RuleApplicability {
    use logical::LogicalExprKind as Kind;
    use logical::PureLogicalOpKind as PureKind;
    match id {
        KnownRuleId::FilterPushdown => RuleApplicability::only(Kind::FilterPushdown),
        KnownRuleId::PurePipelineSimplification => {
            RuleApplicability::pure_pipeline_local_simplification()
        }
        KnownRuleId::StaticPredicateSimplification => {
            RuleApplicability::pure_only(PureKind::Filter)
        }
        KnownRuleId::SeedSimplifiedPredicate => RuleApplicability::pure_any_of(
            ir::AtLeast::<_, 1>::from_one_and_rest(PureKind::NoOp, vec![PureKind::Empty]),
        ),
        KnownRuleId::SeedSourceAccess => RuleApplicability::pure_only(PureKind::Source),
        KnownRuleId::SeedFilter => RuleApplicability::pure_only(PureKind::Filter),
        KnownRuleId::SeedOrder => RuleApplicability::pure_only(PureKind::Order),
        KnownRuleId::SeedStream => {
            RuleApplicability::pure_any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
                PureKind::Limit,
                vec![
                    PureKind::Skip,
                    PureKind::Range,
                    PureKind::Distinct,
                    PureKind::Expand,
                    PureKind::Project,
                    PureKind::Aggregate,
                    PureKind::Variable,
                    PureKind::Reserved,
                ],
            ))
        }
        KnownRuleId::FilterMerge => RuleApplicability::only(Kind::FilterChain),
        KnownRuleId::SeedPurePipeline => RuleApplicability::only(Kind::PurePipeline),
        KnownRuleId::SeedVariableSource => RuleApplicability::only(Kind::VariableSource),
        KnownRuleId::SeedBarrier => RuleApplicability::only(Kind::Barrier),
        KnownRuleId::SeedAccessPath => RuleApplicability::only(Kind::AccessPath),
        KnownRuleId::AccessSetSimplification => {
            RuleApplicability::access_set_canonicalization_candidate()
        }
        KnownRuleId::AccessSubsumption => RuleApplicability::access_set_subsumption_candidate(),
        KnownRuleId::AccessRangeIntersection => {
            RuleApplicability::access_range_intersection_candidate()
        }
        KnownRuleId::AccessEqualityRangeIntersection => {
            RuleApplicability::access_equality_range_intersection_candidate()
        }
        KnownRuleId::AccessEqualityRangeUnion => {
            RuleApplicability::access_equality_range_union_candidate()
        }
        KnownRuleId::AccessContradiction => RuleApplicability::access_contradiction_candidate(),
        KnownRuleId::AccessWindow => RuleApplicability::access_window_rewrite_candidate(),
        KnownRuleId::SeedAccessWindow => RuleApplicability::only(Kind::AccessWindow),
        KnownRuleId::AccessFilterSimplification => {
            RuleApplicability::access_filter_simplification_candidate()
        }
        KnownRuleId::AccessFilterIndex => RuleApplicability::access_filter_index_candidate(),
        KnownRuleId::SeedAccessFilter => RuleApplicability::only(Kind::AccessFilter),
        KnownRuleId::AccessOrderRangeDirection => {
            RuleApplicability::access_order_range_direction_candidate()
        }
        KnownRuleId::AccessOrder => RuleApplicability::access_order_elision_candidate(),
        KnownRuleId::SeedAccessOrder => RuleApplicability::only(Kind::AccessOrder),
        KnownRuleId::AccessDistinct => RuleApplicability::access_distinct_noop_candidate(),
        KnownRuleId::SeedAccessDistinct => RuleApplicability::only(Kind::AccessDistinct),
        KnownRuleId::AccessPipelineFilter => {
            RuleApplicability::access_pipeline_head_only(logical::StreamPipelineOpKind::Filter)
        }
        KnownRuleId::AccessPipelineOrder => {
            RuleApplicability::access_pipeline_head_any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOpKind::Order,
                vec![logical::StreamPipelineOpKind::Filter],
            ))
        }
        KnownRuleId::AccessPipelineSimplification => {
            RuleApplicability::access_pipeline_local_simplification()
        }
        KnownRuleId::SeedAccessPipeline => RuleApplicability::only(Kind::AccessPipeline),
        KnownRuleId::SeedRootPipeline => RuleApplicability::only(Kind::RootPipeline),
        KnownRuleId::SeedRootMutation => RuleApplicability::only(Kind::RootMutation),
        KnownRuleId::SeedRootIndexDdl => RuleApplicability::only(Kind::RootIndexDdl),
        KnownRuleId::SeedRootShortestPath => RuleApplicability::only(Kind::RootShortestPath),
        KnownRuleId::RootControlFlowEmpty => {
            RuleApplicability::root_control_flow_empty_input_candidate()
        }
        KnownRuleId::RootStreamAccessRewrite => {
            RuleApplicability::any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
                Kind::RootPipeline,
                vec![
                    Kind::StreamReserved,
                    Kind::StreamProject,
                    Kind::StreamAggregate,
                    Kind::StreamVariableWrite,
                ],
            ))
        }
        KnownRuleId::SeedRootBranch => RuleApplicability::root_branch_implementation_candidate(),
        KnownRuleId::SeedRootRepeat => RuleApplicability::root_repeat_implementation_candidate(),
        KnownRuleId::SeedStreamReserved => RuleApplicability::only(Kind::StreamReserved),
        KnownRuleId::SeedStreamProject => RuleApplicability::only(Kind::StreamProject),
        KnownRuleId::SeedStreamAggregate => RuleApplicability::only(Kind::StreamAggregate),
        KnownRuleId::SeedStreamVariableWrite => RuleApplicability::only(Kind::StreamVariableWrite),
        KnownRuleId::StreamWindowComposition => {
            RuleApplicability::pure_pipeline_static_window_composition()
        }
    }
}
