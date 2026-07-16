//! Optimizer configuration contract.

use serde::{Deserialize, Serialize};

use crate::{catalog, context, cost};

/// Cascades optimizer configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptimizerConfig {
    /// Exploration guardrails.
    pub limits: context::OptimizerLimits,
    /// Planner-level shape guardrails visible to exploration rules.
    pub planner_limits: context::PlannerLimits,
    /// Immutable cardinality snapshot visible to costing rules.
    pub stats: context::StatsSnapshot,
    /// Tunable storage cost profile visible to implementation rules.
    pub storage: cost::StorageCostProfile,
    /// Immutable index catalog snapshot visible to exploration rules.
    pub indexes: catalog::IndexCatalogSnapshot,
}

impl OptimizerConfig {
    /// Build optimizer configuration from a planner context.
    pub fn from_context(ctx: &context::PlannerContext) -> Self {
        Self {
            limits: ctx.optimizer_limits.clone(),
            planner_limits: ctx.limits.clone(),
            stats: ctx.effective_stats(),
            storage: ctx.storage.clone(),
            indexes: ctx.indexes.clone(),
        }
    }
}
