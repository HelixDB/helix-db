//! Registry-to-candidate schedule compilation.
//!
//! This module owns the one-time conversion from rule applicability metadata
//! into dense scheduler candidate tables. Request-time routing should only read
//! these precompiled lists.

use crate::{logical, rules};

use super::candidates::RuleIndex;
use super::RuleSchedule;

impl<'a> RuleSchedule<'a> {
    pub(super) fn compile_applicability(&mut self) {
        for (index, optimizer_rule) in self.rules.iter().enumerate() {
            let rule_index = RuleIndex::from_enumerated_registry_position(index, self.rules.len());
            match &optimizer_rule.metadata().applicability {
                rules::RuleApplicability::Any => {
                    for kind in logical::LogicalExprKind::ALL {
                        self.candidates_by_kind.push(kind, rule_index);
                    }
                }
                rules::RuleApplicability::LogicalKinds(kinds) => {
                    for kind in kinds.as_slice() {
                        self.candidates_by_kind.push(*kind, rule_index);
                    }
                }
                rules::RuleApplicability::PureOpKinds(kinds) => {
                    for kind in kinds.as_slice() {
                        self.pure_candidates_by_kind.push(*kind, rule_index);
                    }
                }
                rules::RuleApplicability::PurePipelineLocalSimplification => {
                    self.pure_pipeline_local_simplification_candidates
                        .push(rule_index);
                }
                rules::RuleApplicability::PurePipelineStaticWindowComposition => {
                    self.pure_pipeline_static_window_composition_candidates
                        .push(rule_index);
                }
                rules::RuleApplicability::AccessPipelineHeadOpKinds(kinds) => {
                    for kind in kinds.as_slice() {
                        self.access_pipeline_head_candidates_by_kind
                            .push(*kind, rule_index);
                    }
                }
                rules::RuleApplicability::AccessPipelineLocalSimplification => {
                    self.access_pipeline_local_simplification_candidates
                        .push(rule_index);
                }
                rules::RuleApplicability::AccessWindowRewriteCandidate => {
                    self.access_window_rewrite_candidates.push(rule_index);
                }
                rules::RuleApplicability::AccessFilterSimplificationCandidate => {
                    self.access_filter_simplification_candidates
                        .push(rule_index);
                }
                rules::RuleApplicability::AccessFilterIndexCandidate => {
                    self.access_filter_index_candidates.push(rule_index);
                }
                rules::RuleApplicability::AccessOrderElisionCandidate => {
                    self.access_order_elision_candidates.push(rule_index);
                }
                rules::RuleApplicability::AccessOrderRangeDirectionCandidate => {
                    self.access_order_range_direction_candidates
                        .push(rule_index);
                }
                rules::RuleApplicability::AccessDistinctNoopCandidate => {
                    self.access_distinct_noop_candidates.push(rule_index);
                }
                rules::RuleApplicability::AccessSetCanonicalizationCandidate => {
                    self.access_set_canonicalization_candidates.push(rule_index);
                }
                rules::RuleApplicability::AccessSetSubsumptionCandidate => {
                    self.access_set_subsumption_candidates.push(rule_index);
                }
                rules::RuleApplicability::AccessRangeIntersectionCandidate => {
                    self.access_range_intersection_candidates.push(rule_index);
                }
                rules::RuleApplicability::AccessEqualityRangeIntersectionCandidate => {
                    self.access_equality_range_intersection_candidates
                        .push(rule_index);
                }
                rules::RuleApplicability::AccessEqualityRangeUnionCandidate => {
                    self.access_equality_range_union_candidates.push(rule_index);
                }
                rules::RuleApplicability::AccessContradictionCandidate => {
                    self.access_contradiction_candidates.push(rule_index);
                }
                rules::RuleApplicability::AccessSourceKinds(kinds) => {
                    for kind in kinds.as_slice() {
                        self.access_source_candidates_by_kind
                            .push(*kind, rule_index);
                    }
                }
                rules::RuleApplicability::RootControlFlowEmptyInputCandidate => {
                    self.root_control_flow_empty_candidates.push(rule_index);
                }
                rules::RuleApplicability::RootBranchImplementationCandidate => {
                    self.root_branch_implementation_candidates.push(rule_index);
                }
                rules::RuleApplicability::RootRepeatImplementationCandidate => {
                    self.root_repeat_implementation_candidates.push(rule_index);
                }
            }
        }
    }
}
