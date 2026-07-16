//! Selected planner metrics accounting.
//!
//! Top-level selected roots carry optimizer-selected recursive execution cost.
//! Batch entries compose independent optimizer results. Child roots reconstructed
//! from a selected parent's memo provenance are part of that same optimizer
//! result, so selected lowering does not add child metrics a second time.

use crate::exec;

pub(super) fn merge_planner_metrics(total: &mut exec::PlannerMetrics, next: exec::PlannerMetrics) {
    merge_planner_work_metrics(total, &next);
    total.selected_cost = total.selected_cost.serial(next.selected_cost);
}

pub(super) fn merge_planner_work_metrics(
    total: &mut exec::PlannerMetrics,
    next: &exec::PlannerMetrics,
) {
    total.memo_groups = total.memo_groups.saturating_add(next.memo_groups);
    total.memo_exprs = total.memo_exprs.saturating_add(next.memo_exprs);
    total.rule_fires = total.rule_fires.saturating_add(next.rule_fires);
    total.rejected_alternatives = total
        .rejected_alternatives
        .saturating_add(next.rejected_alternatives);
    total.alternatives_considered = total
        .alternatives_considered
        .saturating_add(next.alternatives_considered);
    total.optimization_micros = total
        .optimization_micros
        .saturating_add(next.optimization_micros);
    total.guardrail_hit |= next.guardrail_hit;
}
