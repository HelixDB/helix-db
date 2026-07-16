//! Cascades exploration driver facade.
//!
//! The facade owns public construction and entry points only. Request-scoped
//! exploration, queue adaptation, guardrail checks, and result finalization are
//! split into child modules so each contract can be tested independently.

mod exploration;
mod finish;
mod guardrails;
mod queue;
mod schedule;

use super::{config, registry, result, rule};
use crate::{ir, logical, memo};

/// Cascades exploration driver.
pub struct CascadesOptimizer<'a> {
    rules: schedule::RuleSchedule<'a>,
}

impl<'a> CascadesOptimizer<'a> {
    /// Build an optimizer with an ordered rule registry.
    pub fn new(rules: registry::OptimizerRuleRegistry<'a>) -> Self {
        Self {
            rules: schedule::RuleSchedule::new(rules),
        }
    }

    /// Build an optimizer from raw ordered rule references, rejecting invalid
    /// registries before they can reach scheduling.
    pub fn try_from_rules(
        rules: Vec<&'a dyn rule::OptimizerRule>,
    ) -> Result<Self, registry::OptimizerRuleRegistryError> {
        registry::OptimizerRuleRegistry::try_from_rules(rules).map(Self::new)
    }

    /// Explore one logical root under the configured guardrails.
    pub fn optimize(
        &self,
        root_expr: logical::LogicalExpr,
        config: &config::OptimizerConfig,
    ) -> Result<result::OptimizationResult, memo::MemoError> {
        self.optimize_many(ir::AtLeast::<_, 1>::from_one(root_expr), config)
    }

    /// Explore multiple logical roots under one shared guardrail budget.
    ///
    /// This keeps related top-level and nested logical roots in one optimizer
    /// request so memo sharing, metrics, and guardrails stay request-scoped
    /// rather than root-scoped.
    pub fn optimize_many(
        &self,
        root_exprs: ir::AtLeast<logical::LogicalExpr, 1>,
        config: &config::OptimizerConfig,
    ) -> Result<result::OptimizationResult, memo::MemoError> {
        exploration::optimize_many(self, root_exprs, config)
    }
}
