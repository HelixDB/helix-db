//! Optimizer configuration contract.

use serde::{Deserialize, Serialize};

use std::borrow::Cow;
use std::collections::BTreeSet;

use crate::{catalog, context, cost, ir};

/// Cascades optimizer configuration.
///
/// The index catalog and statistics are borrowed from the planner context
/// (statistics are owned only when runtime feedback rewrites them), so a
/// planning session does not copy a catalog that may hold thousands of
/// indexes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OptimizerConfig<'ctx> {
    /// Exploration guardrails.
    pub limits: context::OptimizerLimits,
    /// Planner-level shape guardrails visible to exploration rules.
    pub planner_limits: context::PlannerLimits,
    /// Immutable cardinality snapshot visible to costing rules.
    pub stats: Cow<'ctx, context::StatsSnapshot>,
    /// Tunable storage cost profile visible to implementation rules.
    pub storage: cost::StorageCostProfile,
    /// Immutable index catalog snapshot visible to exploration rules.
    pub indexes: Cow<'ctx, catalog::IndexCatalogSnapshot>,
    /// Immutable request bindings used to specialize ordinary parameters,
    /// shared with the planner context.
    pub params: context::SharedParamBindings,
    /// Active scopes whose object fields keep enclosed parameters runtime-dependent.
    pub late_bound_params: BTreeSet<ir::NonEmptyString>,
}

impl<'ctx> OptimizerConfig<'ctx> {
    /// Build optimizer configuration from a planner context.
    pub fn from_context(ctx: &'ctx context::PlannerContext) -> Self {
        Self {
            limits: ctx.optimizer_limits.clone(),
            planner_limits: ctx.limits.clone(),
            stats: match ctx.runtime_feedback.is_empty() {
                true => Cow::Borrowed(&ctx.stats),
                false => Cow::Owned(ctx.effective_stats()),
            },
            storage: ctx.storage.clone(),
            indexes: Cow::Borrowed(&ctx.indexes),
            params: ctx.params.clone(),
            late_bound_params: ctx.late_bound_params.clone(),
        }
    }

    /// Own the borrowed catalog and statistics, for a configuration that
    /// outlives its planner context.
    ///
    /// ```
    /// use helix_planner::{context::PlannerContext, optimizer::OptimizerConfig};
    ///
    /// let config: OptimizerConfig<'static> =
    ///     OptimizerConfig::from_context(&PlannerContext::default()).into_owned();
    /// assert!(config.indexes.node_eq.is_empty());
    /// ```
    pub fn into_owned(self) -> OptimizerConfig<'static> {
        OptimizerConfig {
            limits: self.limits,
            planner_limits: self.planner_limits,
            stats: Cow::Owned(self.stats.into_owned()),
            storage: self.storage,
            indexes: Cow::Owned(self.indexes.into_owned()),
            params: self.params,
            late_bound_params: self.late_bound_params,
        }
    }
}
