//! Request-scoped Cascades exploration loop.

mod state;

use super::CascadesOptimizer;
use crate::{ir, logical, memo, optimizer, rules};

use self::state::{ExplorationRun, ExplorationSeed};

/// Explore all seeded logical roots under one shared guardrail budget.
pub(super) fn optimize_many(
    optimizer: &CascadesOptimizer<'_>,
    root_exprs: ir::AtLeast<logical::LogicalExpr, 1>,
    config: &optimizer::OptimizerConfig,
) -> Result<optimizer::OptimizationResult, memo::MemoError> {
    let mut run = match ExplorationRun::seed(root_exprs, config)? {
        ExplorationSeed::Ready(run) => run,
        ExplorationSeed::Finished(result) => return Ok(result),
    };

    // The wall-clock budget bounds optional exploration only. Once it expires
    // no further optional logical alternatives are explored, but required
    // rewrites and implementation rules still run for every expression already
    // queued so each memo group keeps the physical alternatives selection
    // needs. A required rewrite is one whose paired implementation rule
    // rejects the shapes the rewrite matches (see
    // `RuleApplicability::is_required_rewrite`), so it is the only route to a
    // physical alternative; skipping it, or stopping outright, could leave a
    // root group with no physical alternative and fail an otherwise plannable
    // request. Rewritten expressions are re-queued into the same group and the
    // loop keeps draining, so multi-step simplifications such as collapsing
    // several adjacent distinct operators still reach an implementable form.
    let mut time_guardrail = None;
    while let Some(task) = run.pop_task() {
        if time_guardrail.is_none() {
            time_guardrail = run.time_guardrail(config);
        }

        for optimizer_rule in optimizer.rules.rules_for_expr(&task.expr) {
            let metadata = optimizer_rule.metadata();
            if time_guardrail.is_some()
                && metadata.kind != rules::RuleKind::Implementation
                && !metadata.applicability.is_required_rewrite()
            {
                continue;
            }
            if let Some(guardrail) = run.rule_budget_guardrail(config) {
                return Ok(run.finish(Some(guardrail)));
            }
            run.record_rule_fire();
            let rule_result = optimizer_rule.apply(optimizer::RuleInput {
                expr: &task.expr,
                planner_limits: &config.planner_limits,
                stats: &config.stats,
                storage: &config.storage,
                indexes: &config.indexes,
            });

            match rule_result {
                optimizer::RuleResult::NotApplicable => continue,
                optimizer::RuleResult::Rejected(_) => {
                    run.record_rejection();
                }
                optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(expressions)) => {
                    if let Some(guardrail) =
                        run.apply_logical_effect(task.group, expressions, config)?
                    {
                        return Ok(run.finish(Some(guardrail)));
                    }
                }
                optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(alternatives)) => {
                    let provenance = optimizer::RuleProvenance::from_metadata(metadata);
                    if let Some(guardrail) = run.apply_physical_effect(
                        task.group,
                        task.source_expr,
                        provenance,
                        alternatives,
                        config,
                    ) {
                        return Ok(run.finish(Some(guardrail)));
                    }
                }
            }
        }
    }

    Ok(run.finish(time_guardrail))
}
