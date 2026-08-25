//! Selected-root provenance trace events.

use crate::{exec, ir, trace};

use super::event;

pub(super) fn push_run_root(
    prefix: &str,
    index: usize,
    root: &exec::SelectedExecutableRunRoot,
    trace: &mut trace::PlanningTrace,
) {
    push_run_root_at(&format!("{prefix}[{index}].root"), root, trace);
}

fn push_run_root_at(
    path: &str,
    root: &exec::SelectedExecutableRunRoot,
    trace: &mut trace::PlanningTrace,
) {
    event::push(
        trace,
        path,
        trace::TraceDecision::SelectedRunRoot,
        trace::TraceReason::SelectedRootFamily(ir::NonEmptyString::from_static(
            selected_root_family(root),
        )),
    );
    push_run_root_rule(path, selected_root_provenance(root), trace);
    push_run_root_inputs(path, root, trace);
}

fn push_run_root_rule(
    path: &str,
    provenance: &exec::SelectedRootProvenance,
    trace: &mut trace::PlanningTrace,
) {
    let rule_id = provenance.optimizer_rule_id();
    event::push(
        trace,
        format!("{path}.rule"),
        trace::TraceDecision::SelectedOptimizerRule,
        trace::TraceReason::SelectedOptimizerRule(rule_id.to_non_empty_string()),
    );
    push_run_root_memo(path, provenance, trace);
}

fn push_run_root_memo(
    path: &str,
    provenance: &exec::SelectedRootProvenance,
    trace: &mut trace::PlanningTrace,
) {
    let optimizer = provenance.optimizer();
    if let Some(summary) = event::non_empty(optimizer.memo_summary()) {
        event::push(
            trace,
            format!("{path}.memo"),
            trace::TraceDecision::SelectedMemoExpression,
            trace::TraceReason::SelectedMemoExpression(summary),
        );
    }
    optimizer
        .source_children()
        .iter()
        .enumerate()
        .for_each(|(child_index, child)| {
            if let Some(summary) =
                event::non_empty(format!("index={child_index} group={}", child.get()))
            {
                event::push(
                    trace,
                    format!("{path}.memo.child[{child_index}]"),
                    trace::TraceDecision::SelectedMemoChild,
                    trace::TraceReason::SelectedMemoChild(summary),
                );
            }
        });
}

fn push_run_root_inputs(
    path: &str,
    root: &exec::SelectedExecutableRunRoot,
    trace: &mut trace::PlanningTrace,
) {
    match root {
        exec::SelectedExecutableRunRoot::Mutation(root) => {
            push_mutation_inputs(path, root.plan(), trace);
        }
        exec::SelectedExecutableRunRoot::Branch(root) => {
            push_run_root_at(&format!("{path}.input[0].root"), root.input(), trace);
            push_branch_plan_roots(path, root.plan(), trace);
        }
        exec::SelectedExecutableRunRoot::Repeat(root) => {
            push_run_root_at(&format!("{path}.input[0].root"), root.input(), trace);
            push_run_root_at(
                &format!("{path}.body.root"),
                root.plan().body.as_ref(),
                trace,
            );
        }
        _ => {}
    }
}

fn push_mutation_inputs(
    path: &str,
    plan: &exec::SelectedMutationPlan,
    trace: &mut trace::PlanningTrace,
) {
    match plan {
        exec::SelectedMutationPlan::AddNode {
            input: exec::SelectedMutationInput::FromInput(input),
            ..
        }
        | exec::SelectedMutationPlan::DropEdgeById {
            input: exec::SelectedMutationInput::FromInput(input),
            ..
        } => push_run_root_at(&format!("{path}.input[0].root"), input, trace),
        exec::SelectedMutationPlan::AddEdge { input, .. }
        | exec::SelectedMutationPlan::SetProperty { input, .. }
        | exec::SelectedMutationPlan::RemoveProperty { input, .. }
        | exec::SelectedMutationPlan::Drop { input }
        | exec::SelectedMutationPlan::DropEdge { input, .. }
        | exec::SelectedMutationPlan::DropEdgeLabeled { input, .. } => {
            push_run_root_at(&format!("{path}.input[0].root"), input, trace);
        }
        exec::SelectedMutationPlan::AddNode {
            input: exec::SelectedMutationInput::Source,
            ..
        }
        | exec::SelectedMutationPlan::DropEdgeById {
            input: exec::SelectedMutationInput::Source,
            ..
        } => {}
    }
}

fn push_branch_plan_roots(
    path: &str,
    plan: &exec::SelectedBranchPlan,
    trace: &mut trace::PlanningTrace,
) {
    match plan {
        exec::SelectedBranchPlan::Union(branches) => {
            branches
                .as_ref()
                .iter()
                .enumerate()
                .for_each(|(index, branch)| {
                    push_run_root_at(&format!("{path}.branch[{index}].root"), branch, trace);
                })
        }
        exec::SelectedBranchPlan::Choose { then_plan, .. } => {
            push_run_root_at(&format!("{path}.then.root"), then_plan, trace);
        }
        exec::SelectedBranchPlan::ChooseElse {
            then_plan,
            else_plan,
            ..
        } => {
            push_run_root_at(&format!("{path}.then.root"), then_plan, trace);
            push_run_root_at(&format!("{path}.else.root"), else_plan, trace);
        }
        exec::SelectedBranchPlan::Coalesce(branches) => branches
            .as_ref()
            .iter()
            .enumerate()
            .for_each(|(index, branch)| {
                push_run_root_at(&format!("{path}.branch[{index}].root"), branch, trace);
            }),
        exec::SelectedBranchPlan::Optional(branch) => {
            push_run_root_at(&format!("{path}.optional.root"), branch, trace);
        }
    }
}

fn selected_root_family(root: &exec::SelectedExecutableRunRoot) -> &'static str {
    match root {
        exec::SelectedExecutableRunRoot::Alternative(_) => "alternative",
        exec::SelectedExecutableRunRoot::Mutation(_) => "mutation",
        exec::SelectedExecutableRunRoot::IndexDdl(_) => "index_ddl",
        exec::SelectedExecutableRunRoot::Branch(_) => "branch",
        exec::SelectedExecutableRunRoot::Repeat(_) => "repeat",
        exec::SelectedExecutableRunRoot::ShortestPath(_) => "shortest_path",
        exec::SelectedExecutableRunRoot::Pipeline(_) => "pipeline",
        exec::SelectedExecutableRunRoot::Terminal(_) => "terminal",
        exec::SelectedExecutableRunRoot::Count(_) => "count",
    }
}

fn selected_root_provenance(
    root: &exec::SelectedExecutableRunRoot,
) -> &exec::SelectedRootProvenance {
    match root {
        exec::SelectedExecutableRunRoot::Alternative(root) => root.provenance(),
        exec::SelectedExecutableRunRoot::Mutation(root) => root.provenance(),
        exec::SelectedExecutableRunRoot::IndexDdl(root) => root.provenance(),
        exec::SelectedExecutableRunRoot::Branch(root) => root.provenance(),
        exec::SelectedExecutableRunRoot::Repeat(root) => root.provenance(),
        exec::SelectedExecutableRunRoot::ShortestPath(root) => root.provenance(),
        exec::SelectedExecutableRunRoot::Pipeline(root) => root.provenance(),
        exec::SelectedExecutableRunRoot::Terminal(root) => root.provenance(),
        exec::SelectedExecutableRunRoot::Count(root) => root.provenance(),
    }
}
