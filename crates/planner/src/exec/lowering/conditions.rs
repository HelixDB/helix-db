//! Batch run-condition lowering.
//!
//! This module is deliberately pure: it translates typed planner batch
//! conditions into executable DAG conditions without touching step allocation.

use super::*;
use crate::ir;

pub(in crate::exec) fn initial_exec_condition(
    condition: ir::RunConditionPlan<ir::BatchVariableConditionPlan>,
) -> ExecCondition {
    match condition {
        ir::RunConditionPlan::Always => ExecCondition::Always,
        ir::RunConditionPlan::If(condition) => ExecCondition::Variable(condition),
    }
}

pub(in crate::exec) fn followup_exec_condition(
    condition: ir::RunConditionPlan<ir::BatchConditionPlan>,
    previous: ExecStepId,
) -> ExecCondition {
    match condition {
        ir::RunConditionPlan::Always => ExecCondition::Always,
        ir::RunConditionPlan::If(condition) => match condition {
            ir::BatchConditionPlan::VarNotEmpty(variable) => {
                ExecCondition::Variable(ir::BatchVariableConditionPlan::VarNotEmpty(variable))
            }
            ir::BatchConditionPlan::VarEmpty(variable) => {
                ExecCondition::Variable(ir::BatchVariableConditionPlan::VarEmpty(variable))
            }
            ir::BatchConditionPlan::VarMinSize(variable, min_size) => ExecCondition::Variable(
                ir::BatchVariableConditionPlan::VarMinSize(variable, min_size),
            ),
            ir::BatchConditionPlan::PrevNotEmpty => ExecCondition::PreviousStepNotEmpty {
                dependency: previous,
            },
        },
    }
}
