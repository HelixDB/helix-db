//! Memo-child extraction for root mutation expressions.

use super::*;

pub(super) fn children(mutation: &RootMutation) -> Vec<LogicalExpr> {
    match mutation.plan() {
        ir::MutationPlan::AddNode { input, .. } | ir::MutationPlan::DropEdgeById { input, .. } => {
            mutation_input_child(input).into_iter().collect()
        }
        ir::MutationPlan::AddEdge { input, .. }
        | ir::MutationPlan::SetProperty { input, .. }
        | ir::MutationPlan::RemoveProperty { input, .. }
        | ir::MutationPlan::Drop { input }
        | ir::MutationPlan::DropEdge { input, .. }
        | ir::MutationPlan::DropEdgeLabeled { input, .. } => {
            vec![input.as_ref().clone()]
        }
    }
}

fn mutation_input_child(input: &ir::MutationInput<LogicalExpr>) -> Option<LogicalExpr> {
    match input {
        ir::MutationInput::Source => None,
        ir::MutationInput::FromInput { input } => Some(input.as_ref().clone()),
    }
}
