//! Rule input and result contracts.

use serde::{Deserialize, Serialize};

use crate::{catalog, context, cost, ir, logical, physical, rules};

/// Rule input.
pub struct RuleInput<'a> {
    /// Logical expression currently being explored.
    pub expr: &'a logical::LogicalExpr,
    /// Planner-level shape guardrails for exploration rules.
    pub planner_limits: &'a context::PlannerLimits,
    /// Immutable cardinality estimates for implementation rules.
    pub stats: &'a context::StatsSnapshot,
    /// Tunable storage costs for implementation rules.
    pub storage: &'a cost::StorageCostProfile,
    /// Immutable index catalog snapshot for catalog-dependent exploration.
    pub indexes: &'a catalog::IndexCatalogSnapshot,
}

impl<'a> RuleInput<'a> {
    /// Request bindings and scoped late-bound names carried by a logical
    /// cardinality expression.
    pub fn cardinality_bindings(
        &self,
    ) -> Option<(
        &'a context::ParamBindings,
        &'a std::collections::BTreeSet<crate::ir::NonEmptyString>,
    )> {
        let logical::LogicalExpr::StreamCardinality(cardinality) = self.expr else {
            return None;
        };
        Some((cardinality.params(), cardinality.late_bound_params()))
    }
}

/// Non-empty rule effect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleEffect {
    /// Equivalent logical expressions to add to the same memo group.
    Logical(ir::AtLeast<logical::LogicalExpr, 1>),
    /// Physical implementations for the current memo group.
    Physical(ir::AtLeast<physical::PhysicalAlternative, 1>),
}

/// Result of applying one optimizer rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleResult {
    /// Preconditions did not match.
    NotApplicable,
    /// Rule matched but rejected the candidate with a stable reason.
    Rejected(rules::RuleRejection),
    /// Rule produced a non-empty effect.
    Applied(RuleEffect),
}

impl RuleResult {
    /// Return the coarse rule outcome for metrics and tracing.
    pub const fn outcome(&self) -> rules::RuleOutcome {
        match self {
            Self::NotApplicable => rules::RuleOutcome::NotApplicable,
            Self::Rejected(_) => rules::RuleOutcome::Rejected,
            Self::Applied(_) => rules::RuleOutcome::Applied,
        }
    }
}

/// Optimizer rule contract.
pub trait OptimizerRule {
    /// Stable rule metadata.
    fn metadata(&self) -> &rules::RuleMetadata;

    /// Apply this rule to one logical expression.
    fn apply(&self, input: RuleInput<'_>) -> RuleResult;
}
