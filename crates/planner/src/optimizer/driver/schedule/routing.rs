//! Request-time rule candidate routing.
//!
//! Routing reads the compiled candidate tables without mutating scheduler
//! state. It owns the expression-shape predicates that decide which narrow
//! feature buckets should be merged with broad logical-family candidates.

use crate::{logical, rules};

use super::candidates::{CandidateSlice, FeatureCandidates, RuleCandidates};
use super::RuleSchedule;

impl<'a> RuleSchedule<'a> {
    /// Return candidate rules for one logical expression.
    pub(in crate::optimizer::driver) fn rules_for_expr(
        &self,
        expr: &logical::LogicalExpr,
    ) -> RuleCandidates<'_, 'a> {
        let broad = self.candidates_by_kind.get(expr.kind());
        let (narrow, features) = match expr {
            logical::LogicalExpr::Pure(op) => (
                self.pure_candidates_by_kind.get(op.kind()),
                FeatureCandidates::empty(),
            ),
            logical::LogicalExpr::PurePipeline(pipeline) => {
                let local_simplification = if pipeline.has_local_simplification_candidate() {
                    self.pure_pipeline_local_simplification_candidates
                        .as_slice()
                } else {
                    CandidateSlice::empty()
                };
                let static_window_composition =
                    if pipeline.has_static_window_composition_candidate() {
                        self.pure_pipeline_static_window_composition_candidates
                            .as_slice()
                    } else {
                        CandidateSlice::empty()
                    };
                (
                    CandidateSlice::empty(),
                    FeatureCandidates::two(local_simplification, static_window_composition),
                )
            }
            logical::LogicalExpr::AccessPipeline(pipeline) => {
                let local_simplification = if pipeline.has_local_simplification_candidate() {
                    self.access_pipeline_local_simplification_candidates
                        .as_slice()
                } else {
                    CandidateSlice::empty()
                };
                (
                    self.access_pipeline_head_candidates_by_kind
                        .get(pipeline.head_op_kind()),
                    FeatureCandidates::one(local_simplification),
                )
            }
            logical::LogicalExpr::AccessPath(access) => {
                let canonicalization =
                    if rules::access_path_has_set_canonicalization_candidate(access) {
                        self.access_set_canonicalization_candidates.as_slice()
                    } else {
                        CandidateSlice::empty()
                    };
                let subsumption = if rules::access_path_has_set_subsumption_candidate(access) {
                    self.access_set_subsumption_candidates.as_slice()
                } else {
                    CandidateSlice::empty()
                };
                let range_intersection =
                    if rules::access_path_has_range_intersection_candidate(access) {
                        self.access_range_intersection_candidates.as_slice()
                    } else {
                        CandidateSlice::empty()
                    };
                let equality_range_intersection =
                    if rules::access_path_has_equality_range_intersection_candidate(access) {
                        self.access_equality_range_intersection_candidates
                            .as_slice()
                    } else {
                        CandidateSlice::empty()
                    };
                let equality_range_union =
                    if rules::access_path_has_equality_range_union_candidate(access) {
                        self.access_equality_range_union_candidates.as_slice()
                    } else {
                        CandidateSlice::empty()
                    };
                let contradiction = if rules::access_path_has_contradiction_candidate(access) {
                    self.access_contradiction_candidates.as_slice()
                } else {
                    CandidateSlice::empty()
                };
                (
                    self.access_source_candidates_by_kind
                        .get(access.source_kind()),
                    FeatureCandidates::six(
                        canonicalization,
                        subsumption,
                        range_intersection,
                        equality_range_intersection,
                        equality_range_union,
                        contradiction,
                    ),
                )
            }
            logical::LogicalExpr::AccessWindow(window) => {
                let rewrite = if window.has_rewrite_candidate() {
                    self.access_window_rewrite_candidates.as_slice()
                } else {
                    CandidateSlice::empty()
                };
                (CandidateSlice::empty(), FeatureCandidates::one(rewrite))
            }
            logical::LogicalExpr::AccessFilter(filter) => {
                let simplification = if rules::access_filter_has_simplification_candidate(filter) {
                    self.access_filter_simplification_candidates.as_slice()
                } else {
                    CandidateSlice::empty()
                };
                let index = if rules::access_filter_has_index_candidate(filter) {
                    self.access_filter_index_candidates.as_slice()
                } else {
                    CandidateSlice::empty()
                };
                (
                    CandidateSlice::empty(),
                    FeatureCandidates::two(simplification, index),
                )
            }
            logical::LogicalExpr::AccessOrder(order) => {
                let elision = if order.has_order_elision_candidate() {
                    self.access_order_elision_candidates.as_slice()
                } else {
                    CandidateSlice::empty()
                };
                let range_direction = if order.has_range_direction_candidate() {
                    self.access_order_range_direction_candidates.as_slice()
                } else {
                    CandidateSlice::empty()
                };
                (
                    CandidateSlice::empty(),
                    FeatureCandidates::two(elision, range_direction),
                )
            }
            logical::LogicalExpr::AccessDistinct(distinct) => {
                let noop = if distinct.has_noop_candidate() {
                    self.access_distinct_noop_candidates.as_slice()
                } else {
                    CandidateSlice::empty()
                };
                (CandidateSlice::empty(), FeatureCandidates::one(noop))
            }
            logical::LogicalExpr::RootBranch(branch) => {
                let empty = rules::root_branch_has_empty_input(branch);
                let empty_candidate = if empty {
                    self.root_control_flow_empty_candidates.as_slice()
                } else {
                    CandidateSlice::empty()
                };
                let implementation = if empty {
                    CandidateSlice::empty()
                } else {
                    self.root_branch_implementation_candidates.as_slice()
                };
                (
                    CandidateSlice::empty(),
                    FeatureCandidates::two(empty_candidate, implementation),
                )
            }
            logical::LogicalExpr::RootRepeat(repeat) => {
                let empty = rules::root_repeat_has_empty_input(repeat);
                let empty_candidate = if empty {
                    self.root_control_flow_empty_candidates.as_slice()
                } else {
                    CandidateSlice::empty()
                };
                let implementation = if empty {
                    CandidateSlice::empty()
                } else {
                    self.root_repeat_implementation_candidates.as_slice()
                };
                (
                    CandidateSlice::empty(),
                    FeatureCandidates::two(empty_candidate, implementation),
                )
            }
            _ => (CandidateSlice::empty(), FeatureCandidates::empty()),
        };
        RuleCandidates::new(self.rules.as_slice(), broad, narrow, features)
    }

    #[cfg(test)]
    pub(super) fn rules_for_kind(&self, kind: logical::LogicalExprKind) -> RuleCandidates<'_, 'a> {
        RuleCandidates::new(
            self.rules.as_slice(),
            self.candidates_by_kind.get(kind),
            CandidateSlice::empty(),
            FeatureCandidates::empty(),
        )
    }
}
