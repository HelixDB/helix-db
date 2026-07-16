//! Residual-free access set algebra rewrite facade.
//!
//! Proof modules own access-set algebra. Rule wrapper modules only bind those
//! proofs to optimizer metadata and `RuleResult` construction.

mod canonical;
mod contradiction;
mod equality_range;
mod range;
mod rules;
mod subsumption;

use super::sources::{
    dedupe_edge_sources, dedupe_node_sources, edge_access_path_from_plan,
    edge_intersection_from_sources, edge_union_from_sources, empty_access_path_like,
    node_access_path_from_plan, node_intersection_from_sources, node_union_from_sources,
    AccessPathFromPlan,
};
use crate::{catalog, digest, ir, logical, optimizer};

/// Access-set rewrite outcome at the optimizer rule boundary.
///
/// Local proof helpers may use `Option` for small "proof did not fire" checks,
/// but rule facades return this ADT so callers cannot confuse an unchanged set
/// with a rewritten access path.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access) enum AccessSetRewrite {
    /// The rule family did not produce a replacement.
    NotApplicable,
    /// The rule family produced a validated replacement access path.
    Rewritten(logical::AccessPath),
}

impl AccessSetRewrite {
    pub(in crate::rules::access) fn from_node_plan(
        rewrite: AccessSetPlanRewrite<ir::NodeAccessPlan>,
    ) -> Self {
        match rewrite {
            AccessSetPlanRewrite::NotApplicable => Self::NotApplicable,
            AccessSetPlanRewrite::Rewritten(plan) => match node_access_path_from_plan(plan) {
                AccessPathFromPlan::NotResidualFree => Self::NotApplicable,
                AccessPathFromPlan::Access(access) => Self::Rewritten(access),
            },
        }
    }

    pub(in crate::rules::access) fn from_edge_plan(
        rewrite: AccessSetPlanRewrite<ir::EdgeAccessPlan>,
    ) -> Self {
        match rewrite {
            AccessSetPlanRewrite::NotApplicable => Self::NotApplicable,
            AccessSetPlanRewrite::Rewritten(plan) => match edge_access_path_from_plan(plan) {
                AccessPathFromPlan::NotResidualFree => Self::NotApplicable,
                AccessPathFromPlan::Access(access) => Self::Rewritten(access),
            },
        }
    }

    pub(in crate::rules::access) fn rewritten_empty_like(access: &logical::AccessPath) -> Self {
        Self::Rewritten(empty_access_path_like(access))
    }

    pub(in crate::rules::access) fn into_rule_result(self) -> optimizer::RuleResult {
        match self {
            Self::NotApplicable => optimizer::RuleResult::NotApplicable,
            Self::Rewritten(access) => super::super::access_path_result(access),
        }
    }
}

/// Access-set rewrite outcome before node/edge plans are revalidated as
/// residual-free logical access paths.
///
/// This keeps proof modules from leaking nullable plan rewrites while still
/// preserving the fail-closed validation boundary in `AccessSetRewrite`.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access) enum AccessSetPlanRewrite<P> {
    /// The proof family did not produce a replacement plan.
    NotApplicable,
    /// The proof family produced a replacement plan for the same element kind.
    Rewritten(P),
}

pub use self::rules::{
    AccessContradictionRule, AccessEqualityRangeIntersectionRule, AccessEqualityRangeUnionRule,
    AccessRangeIntersectionRule, AccessSetSimplificationRule, AccessSubsumptionRule,
};

pub(in crate::rules) use self::{
    contradiction::access_path_has_contradiction_candidate,
    equality_range::{
        access_path_has_equality_range_intersection_candidate,
        access_path_has_equality_range_union_candidate,
    },
    range::access_path_has_range_intersection_candidate,
};

pub(super) use self::{
    canonical::simplify_access_set,
    contradiction::simplify_access_contradiction,
    equality_range::{
        simplify_access_equality_range_intersection, simplify_access_equality_range_union,
    },
    range::simplify_access_range_intersection,
    subsumption::simplify_access_subsumption,
};

#[cfg(test)]
mod tests {
    use super::*;

    fn residual_predicate() -> ir::PredicatePlan {
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap()
    }

    #[test]
    fn access_set_rewrite_rejects_invalid_node_replacements() {
        let source = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap();

        assert_eq!(
            AccessSetRewrite::from_node_plan(AccessSetPlanRewrite::Rewritten(
                ir::NodeAccessPlan::ScanThenFilter {
                    source,
                    residual: residual_predicate(),
                }
            )),
            AccessSetRewrite::NotApplicable
        );
    }

    #[test]
    fn access_set_rewrite_converts_valid_edge_replacements_to_rule_results() {
        let rewrite = AccessSetRewrite::from_edge_plan(AccessSetPlanRewrite::Rewritten(
            ir::EdgeAccessPlan::Empty,
        ));

        assert!(matches!(
            rewrite.into_rule_result(),
            optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(exprs))
                if matches!(
                    exprs.as_ref(),
                    [logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(path))]
                        if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
                )
        ));
    }

    #[test]
    fn access_set_rewrite_preserves_not_applicable_plan_rewrites() {
        assert_eq!(
            AccessSetRewrite::from_node_plan(AccessSetPlanRewrite::NotApplicable),
            AccessSetRewrite::NotApplicable
        );
    }
}
