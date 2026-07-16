//! Optimizer result finalization contract.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::{exec, ir, memo, optimizer};

/// Build the public optimization result and finish request-scoped metrics.
pub(super) fn finish(
    memo: memo::Memo,
    root: memo::MemoGroupId,
    roots: ir::AtLeast<memo::MemoGroupId, 1>,
    physical: BTreeMap<memo::MemoGroupId, Vec<optimizer::result::PendingPhysicalAlternative>>,
    mut metrics: exec::PlannerMetrics,
    guardrail: Option<optimizer::OptimizerGuardrail>,
    started: Instant,
) -> optimizer::OptimizationResult {
    metrics.memo_groups = memo.group_count();
    metrics.memo_exprs = memo.expression_count();
    metrics.alternatives_considered = physical.values().map(Vec::len).sum();
    metrics.optimization_micros = started.elapsed().as_micros() as u64;
    metrics.guardrail_hit = guardrail.is_some();
    optimizer::OptimizationResult::new(memo, root, roots, physical, metrics, guardrail)
}
