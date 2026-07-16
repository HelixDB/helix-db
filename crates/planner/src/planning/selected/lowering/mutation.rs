//! Mutation selected executable reconstruction.

use super::super::{rejection, SelectedCascadesPlanner};
use super::memo_children;
use crate::{error, exec, ir, logical};

impl SelectedCascadesPlanner<'_> {
    pub(super) fn selected_mutation_plan(
        &mut self,
        plan: &ir::MutationPlan<logical::LogicalExpr>,
        child_plans: memo_children::MemoChildPlanAvailability<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedMutationPlan, error::PlannerError> {
        let mut child_plans = mutation_child_plans(plan, child_plans)?;
        match plan {
            ir::MutationPlan::AddNode {
                input,
                label,
                properties,
            } => Ok(exec::SelectedMutationPlan::AddNode {
                input: self.selected_mutation_input(input, &mut child_plans, metrics)?,
                label: label.clone(),
                properties: properties.clone(),
            }),
            ir::MutationPlan::AddEdge {
                label,
                to,
                properties,
                ..
            } => Ok(exec::SelectedMutationPlan::AddEdge {
                input: Box::new(self.selected_mutation_child(&mut child_plans, metrics)?),
                label: label.clone(),
                to: to.clone(),
                properties: properties.clone(),
            }),
            ir::MutationPlan::SetProperty { name, value, .. } => {
                Ok(exec::SelectedMutationPlan::SetProperty {
                    input: Box::new(self.selected_mutation_child(&mut child_plans, metrics)?),
                    name: name.clone(),
                    value: value.clone(),
                })
            }
            ir::MutationPlan::RemoveProperty { name, .. } => {
                Ok(exec::SelectedMutationPlan::RemoveProperty {
                    input: Box::new(self.selected_mutation_child(&mut child_plans, metrics)?),
                    name: name.clone(),
                })
            }
            ir::MutationPlan::Drop { .. } => Ok(exec::SelectedMutationPlan::Drop {
                input: Box::new(self.selected_mutation_child(&mut child_plans, metrics)?),
            }),
            ir::MutationPlan::DropEdge { to, .. } => Ok(exec::SelectedMutationPlan::DropEdge {
                input: Box::new(self.selected_mutation_child(&mut child_plans, metrics)?),
                to: to.clone(),
            }),
            ir::MutationPlan::DropEdgeLabeled { to, label, .. } => {
                Ok(exec::SelectedMutationPlan::DropEdgeLabeled {
                    input: Box::new(self.selected_mutation_child(&mut child_plans, metrics)?),
                    to: to.clone(),
                    label: label.clone(),
                })
            }
            ir::MutationPlan::DropEdgeById { input, edges } => {
                Ok(exec::SelectedMutationPlan::DropEdgeById {
                    input: self.selected_mutation_input(input, &mut child_plans, metrics)?,
                    edges: edges.clone(),
                })
            }
        }
    }

    fn selected_mutation_input(
        &mut self,
        input: &ir::MutationInput<logical::LogicalExpr>,
        child_plans: &mut MutationChildPlans<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedMutationInput, error::PlannerError> {
        match input {
            ir::MutationInput::Source => Ok(exec::SelectedMutationInput::Source),
            ir::MutationInput::FromInput { .. } => Ok(exec::SelectedMutationInput::FromInput(
                Box::new(self.selected_mutation_child(child_plans, metrics)?),
            )),
        }
    }

    fn selected_mutation_child(
        &mut self,
        child_plans: &mut MutationChildPlans<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        match child_plans {
            MutationChildPlans::Memo(cursor) => {
                let child = cursor.next()?;
                self.selected_run_root_from_memo_child(child, metrics)
            }
            MutationChildPlans::NoChildren => Err(rejection::unsupported(
                rejection::Reason::MemoChildPlanMissing,
            )),
        }
    }
}

enum MutationChildPlans<'result, 'selection> {
    Memo(memo_children::MemoChildPlanCursor<'result, 'selection>),
    NoChildren,
}

fn mutation_child_plans<'result, 'selection>(
    plan: &ir::MutationPlan<logical::LogicalExpr>,
    child_plans: memo_children::MemoChildPlanAvailability<'result, 'selection>,
) -> Result<MutationChildPlans<'result, 'selection>, error::PlannerError> {
    let expected = mutation_child_count(plan);
    if expected == 0 {
        return Ok(MutationChildPlans::NoChildren);
    }
    let child_plans = child_plans.require()?;
    Ok(MutationChildPlans::Memo(
        child_plans
            .exactly(expected, rejection::Reason::MemoChildArityMismatch)?
            .cursor(),
    ))
}

fn mutation_child_count(plan: &ir::MutationPlan<logical::LogicalExpr>) -> usize {
    match plan {
        ir::MutationPlan::AddNode { input, .. } | ir::MutationPlan::DropEdgeById { input, .. } => {
            mutation_input_child_count(input)
        }
        ir::MutationPlan::AddEdge { .. }
        | ir::MutationPlan::SetProperty { .. }
        | ir::MutationPlan::RemoveProperty { .. }
        | ir::MutationPlan::Drop { .. }
        | ir::MutationPlan::DropEdge { .. }
        | ir::MutationPlan::DropEdgeLabeled { .. } => 1,
    }
}

fn mutation_input_child_count(input: &ir::MutationInput<logical::LogicalExpr>) -> usize {
    match input {
        ir::MutationInput::Source => 0,
        ir::MutationInput::FromInput { .. } => 1,
    }
}
