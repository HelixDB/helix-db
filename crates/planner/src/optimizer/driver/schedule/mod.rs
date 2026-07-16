//! Compiled rule schedule for request exploration.
//!
//! The schedule is compiled once from the ordered registry into dense closed
//! enum tables. Exploration then reads candidate slices without scanning the
//! full registry or performing map lookups for every memo expression.

mod candidates;
mod compile;
mod routing;
mod table;

use super::super::registry;
use crate::logical;

use self::candidates::CandidateList;
use self::table::CandidateTable;

#[cfg(test)]
mod tests;

/// Rule registry compiled by logical and operation family.
pub(super) struct RuleSchedule<'a> {
    rules: registry::OptimizerRuleRegistry<'a>,
    candidates_by_kind: CandidateTable<logical::LogicalExprKind>,
    pure_candidates_by_kind: CandidateTable<logical::PureLogicalOpKind>,
    pure_pipeline_local_simplification_candidates: CandidateList,
    pure_pipeline_static_window_composition_candidates: CandidateList,
    access_pipeline_head_candidates_by_kind: CandidateTable<logical::StreamPipelineOpKind>,
    access_pipeline_local_simplification_candidates: CandidateList,
    access_window_rewrite_candidates: CandidateList,
    access_filter_simplification_candidates: CandidateList,
    access_filter_index_candidates: CandidateList,
    access_order_elision_candidates: CandidateList,
    access_order_range_direction_candidates: CandidateList,
    access_distinct_noop_candidates: CandidateList,
    access_set_canonicalization_candidates: CandidateList,
    access_set_subsumption_candidates: CandidateList,
    access_range_intersection_candidates: CandidateList,
    access_equality_range_intersection_candidates: CandidateList,
    access_equality_range_union_candidates: CandidateList,
    access_contradiction_candidates: CandidateList,
    access_source_candidates_by_kind: CandidateTable<logical::AccessSourceKind>,
    root_control_flow_empty_candidates: CandidateList,
    root_branch_implementation_candidates: CandidateList,
    root_repeat_implementation_candidates: CandidateList,
}

impl<'a> RuleSchedule<'a> {
    /// Compile an ordered rule registry into per-family candidate lists.
    pub(super) fn new(rules: registry::OptimizerRuleRegistry<'a>) -> Self {
        let mut schedule = Self {
            rules,
            candidates_by_kind: CandidateTable::empty(),
            pure_candidates_by_kind: CandidateTable::empty(),
            pure_pipeline_local_simplification_candidates: CandidateList::default(),
            pure_pipeline_static_window_composition_candidates: CandidateList::default(),
            access_pipeline_head_candidates_by_kind: CandidateTable::empty(),
            access_pipeline_local_simplification_candidates: CandidateList::default(),
            access_window_rewrite_candidates: CandidateList::default(),
            access_filter_simplification_candidates: CandidateList::default(),
            access_filter_index_candidates: CandidateList::default(),
            access_order_elision_candidates: CandidateList::default(),
            access_order_range_direction_candidates: CandidateList::default(),
            access_distinct_noop_candidates: CandidateList::default(),
            access_set_canonicalization_candidates: CandidateList::default(),
            access_set_subsumption_candidates: CandidateList::default(),
            access_range_intersection_candidates: CandidateList::default(),
            access_equality_range_intersection_candidates: CandidateList::default(),
            access_equality_range_union_candidates: CandidateList::default(),
            access_contradiction_candidates: CandidateList::default(),
            access_source_candidates_by_kind: CandidateTable::empty(),
            root_control_flow_empty_candidates: CandidateList::default(),
            root_branch_implementation_candidates: CandidateList::default(),
            root_repeat_implementation_candidates: CandidateList::default(),
        };
        schedule.compile_applicability();
        schedule
    }
}
