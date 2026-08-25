//! Optimizer configuration contract.

use serde::{Deserialize, Serialize};

use std::collections::BTreeSet;

use crate::{catalog, context, cost, ir};

/// Cascades optimizer configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Immutable request bindings used to specialize ordinary parameters.
    pub params: context::ParamBindings,
    /// Active scopes whose object fields keep enclosed parameters runtime-dependent.
    pub late_bound_params: BTreeSet<ir::NonEmptyString>,
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
            params: ctx.params.clone(),
            late_bound_params: ctx.late_bound_params.clone(),
        }
    }
}
