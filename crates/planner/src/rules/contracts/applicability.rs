//! Rule applicability contracts used by the optimizer scheduler.

mod candidates;
mod kind_sets;
mod known;
mod matching;

use serde::{Deserialize, Serialize};

use super::KnownRuleId;
use crate::{ir, logical};

pub(crate) use candidates::{
    access_filter_has_index_candidate, access_filter_has_simplification_candidate,
    access_path_has_contradiction_candidate, access_path_has_equality_range_intersection_candidate,
    access_path_has_equality_range_union_candidate, access_path_has_range_intersection_candidate,
    access_path_has_set_canonicalization_candidate, access_path_has_set_subsumption_candidate,
    root_branch_has_empty_input, root_repeat_has_empty_input,
};
pub use kind_sets::{
    RuleAccessSourceKinds, RuleLogicalKinds, RulePureOpKinds, RuleStreamPipelineOpKinds,
};

/// Logical-expression families a rule can possibly match.
///
/// Custom rules default to `Any`, while production rules infer a non-empty
/// family set from the closed `KnownRuleId` inventory. This lets the optimizer
/// skip impossible rule calls before entering rule-specific logic.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleApplicability {
    /// The rule may inspect any logical expression.
    #[default]
    Any,
    /// The rule can only match the listed top-level logical expression kinds.
    LogicalKinds(RuleLogicalKinds),
    /// The rule can only match the listed `LogicalExpr::Pure` operation kinds.
    PureOpKinds(RulePureOpKinds),
    /// The rule can only match `LogicalExpr::PurePipeline` values that have a
    /// local simplification candidate.
    PurePipelineLocalSimplification,
    /// The rule can only match `LogicalExpr::PurePipeline` values that have a
    /// static stream-window composition candidate.
    PurePipelineStaticWindowComposition,
    /// The rule can only match `LogicalExpr::AccessPipeline` values whose first
    /// stream operator is in the listed family set.
    AccessPipelineHeadOpKinds(RuleStreamPipelineOpKinds),
    /// The rule can only match `LogicalExpr::AccessPipeline` values that have
    /// a local simplification candidate.
    AccessPipelineLocalSimplification,
    /// The rule can only match `LogicalExpr::AccessWindow` values that have a
    /// statically recognizable rewrite candidate.
    AccessWindowRewriteCandidate,
    /// The rule can only match `LogicalExpr::AccessFilter` values whose access
    /// source or predicate shape can be simplified without catalog lookup.
    AccessFilterSimplificationCandidate,
    /// The rule can only match `LogicalExpr::AccessFilter` values whose
    /// predicate has a known label scope plus a possible secondary-index atom.
    AccessFilterIndexCandidate,
    /// The rule can only match `LogicalExpr::AccessOrder` values that may be
    /// elided because the access already satisfies the order.
    AccessOrderElisionCandidate,
    /// The rule can only match `LogicalExpr::AccessOrder` values that may be
    /// satisfied by switching to an opposite-direction range index.
    AccessOrderRangeDirectionCandidate,
    /// The rule can only match `LogicalExpr::AccessDistinct` values that may
    /// be elided because uniqueness is statically provable.
    AccessDistinctNoopCandidate,
    /// The rule can only match `LogicalExpr::AccessPath` values whose
    /// residual-free set source has a canonicalization rewrite candidate.
    AccessSetCanonicalizationCandidate,
    /// The rule can only match `LogicalExpr::AccessPath` values whose
    /// residual-free set source has a subsumed child candidate.
    AccessSetSubsumptionCandidate,
    /// The rule can only match `LogicalExpr::AccessPath` values whose
    /// same-key range intersections can be tightened.
    AccessRangeIntersectionCandidate,
    /// The rule can only match `LogicalExpr::AccessPath` values whose
    /// equality union can be intersected with a compatible range source.
    AccessEqualityRangeIntersectionCandidate,
    /// The rule can only match `LogicalExpr::AccessPath` values whose
    /// equality source is covered by an existing range union sibling.
    AccessEqualityRangeUnionCandidate,
    /// The rule can only match `LogicalExpr::AccessPath` values whose
    /// residual-free source has a static contradiction.
    AccessContradictionCandidate,
    /// The rule can only match root branch/repeat values whose input is a
    /// direct empty access path.
    RootControlFlowEmptyInputCandidate,
    /// The rule can only implement root branch values whose input is not a
    /// direct empty access path.
    RootBranchImplementationCandidate,
    /// The rule can only implement root repeat values whose input is not a
    /// direct empty access path.
    RootRepeatImplementationCandidate,
    /// The rule can only match `LogicalExpr::AccessPath` values whose
    /// top-level residual-free source is in the listed family set.
    AccessSourceKinds(RuleAccessSourceKinds),
}

impl RuleApplicability {
    /// Match every logical expression.
    pub const fn any() -> Self {
        Self::Any
    }

    /// Match one top-level logical expression kind.
    pub fn only(kind: logical::LogicalExprKind) -> Self {
        Self::LogicalKinds(RuleLogicalKinds::one(kind))
    }

    /// Match a non-empty set of top-level logical expression kinds.
    pub fn any_of(kinds: ir::AtLeast<logical::LogicalExprKind, 1>) -> Self {
        Self::LogicalKinds(RuleLogicalKinds::new(kinds))
    }

    /// Match one `LogicalExpr::Pure` operation kind.
    pub fn pure_only(kind: logical::PureLogicalOpKind) -> Self {
        Self::PureOpKinds(RulePureOpKinds::one(kind))
    }

    /// Match a non-empty set of `LogicalExpr::Pure` operation kinds.
    pub fn pure_any_of(kinds: ir::AtLeast<logical::PureLogicalOpKind, 1>) -> Self {
        Self::PureOpKinds(RulePureOpKinds::new(kinds))
    }

    /// Match pure pipelines that may be locally simplified.
    pub const fn pure_pipeline_local_simplification() -> Self {
        Self::PurePipelineLocalSimplification
    }

    /// Match pure pipelines that may compose static stream windows.
    pub const fn pure_pipeline_static_window_composition() -> Self {
        Self::PurePipelineStaticWindowComposition
    }

    /// Match one access-pipeline head stream operator kind.
    pub fn access_pipeline_head_only(kind: logical::StreamPipelineOpKind) -> Self {
        Self::AccessPipelineHeadOpKinds(RuleStreamPipelineOpKinds::one(kind))
    }

    /// Match a non-empty set of access-pipeline head stream operator kinds.
    pub fn access_pipeline_head_any_of(
        kinds: ir::AtLeast<logical::StreamPipelineOpKind, 1>,
    ) -> Self {
        Self::AccessPipelineHeadOpKinds(RuleStreamPipelineOpKinds::new(kinds))
    }

    /// Match access pipelines that may be locally simplified.
    pub const fn access_pipeline_local_simplification() -> Self {
        Self::AccessPipelineLocalSimplification
    }

    /// Match access windows that may be rewritten by exploration rules.
    pub const fn access_window_rewrite_candidate() -> Self {
        Self::AccessWindowRewriteCandidate
    }

    /// Match access filters that may simplify without catalog lookup.
    pub const fn access_filter_simplification_candidate() -> Self {
        Self::AccessFilterSimplificationCandidate
    }

    /// Match access filters that may be replaced by catalog-backed indexes.
    pub const fn access_filter_index_candidate() -> Self {
        Self::AccessFilterIndexCandidate
    }

    /// Match access-order requests that may be elided.
    pub const fn access_order_elision_candidate() -> Self {
        Self::AccessOrderElisionCandidate
    }

    /// Match access-order requests that may use an opposite range direction.
    pub const fn access_order_range_direction_candidate() -> Self {
        Self::AccessOrderRangeDirectionCandidate
    }

    /// Match access-distinct requests that may be no-ops.
    pub const fn access_distinct_noop_candidate() -> Self {
        Self::AccessDistinctNoopCandidate
    }

    /// Match access sets that may canonicalize.
    pub const fn access_set_canonicalization_candidate() -> Self {
        Self::AccessSetCanonicalizationCandidate
    }

    /// Match access sets that may remove a subsumed child source.
    pub const fn access_set_subsumption_candidate() -> Self {
        Self::AccessSetSubsumptionCandidate
    }

    /// Match access sets that may tighten same-key range intersections.
    pub const fn access_range_intersection_candidate() -> Self {
        Self::AccessRangeIntersectionCandidate
    }

    /// Match access sets that may intersect equality unions with ranges.
    pub const fn access_equality_range_intersection_candidate() -> Self {
        Self::AccessEqualityRangeIntersectionCandidate
    }

    /// Match access sets that may drop equality sources covered by ranges.
    pub const fn access_equality_range_union_candidate() -> Self {
        Self::AccessEqualityRangeUnionCandidate
    }

    /// Match access sets that are statically contradictory.
    pub const fn access_contradiction_candidate() -> Self {
        Self::AccessContradictionCandidate
    }

    /// Match root branch/repeat expressions that collapse to empty access.
    pub const fn root_control_flow_empty_input_candidate() -> Self {
        Self::RootControlFlowEmptyInputCandidate
    }

    /// Match implementable non-empty root branches.
    pub const fn root_branch_implementation_candidate() -> Self {
        Self::RootBranchImplementationCandidate
    }

    /// Match implementable non-empty root repeats.
    pub const fn root_repeat_implementation_candidate() -> Self {
        Self::RootRepeatImplementationCandidate
    }

    /// Match one access source kind.
    pub fn access_source_only(kind: logical::AccessSourceKind) -> Self {
        Self::AccessSourceKinds(RuleAccessSourceKinds::one(kind))
    }

    /// Match a non-empty set of access source kinds.
    pub fn access_source_any_of(kinds: ir::AtLeast<logical::AccessSourceKind, 1>) -> Self {
        Self::AccessSourceKinds(RuleAccessSourceKinds::new(kinds))
    }

    pub(super) fn for_known_rule(id: KnownRuleId) -> Self {
        known::for_known_rule(id)
    }
}
