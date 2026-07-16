//! Executable repeat control-flow interpretation.

use helix_planner::{exec, ir};

use super::super::{ExecutionContext, ExecutionRow, ExecutionValue};
use super::support::{context_variable, restore_variable};
use crate::error::Result;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn execute_repeat(
        &mut self,
        input: ExecutionValue,
        plan: &exec::ExecRepeatPlan,
    ) -> Result<ExecutionValue> {
        let context = context_variable();
        let previous_context = self.variables.get(&context).cloned();
        let result = self
            .execute_repeat_with_context(input, plan, &context)
            .await;
        restore_variable(&mut self.variables, context, previous_context);
        result
    }

    async fn execute_repeat_with_context(
        &mut self,
        input: ExecutionValue,
        plan: &exec::ExecRepeatPlan,
        context: &ir::NonEmptyString,
    ) -> Result<ExecutionValue> {
        let mut current_rows = self.stream_rows(input, "repeat")?;
        let mut emitted = Vec::new();
        for _depth in 0..repeat_max_depth(plan) {
            if matches!(
                plan.emit,
                ir::RepeatEmitPlan::Before | ir::RepeatEmitPlan::All
            ) {
                emitted.extend(current_rows.iter().cloned());
            }

            self.variables.insert(
                context.clone(),
                ExecutionValue::Stream(current_rows.clone()),
            );
            let next_rows = self.subplan_stream_rows(&plan.body, "repeat").await?;

            match &plan.emit {
                ir::RepeatEmitPlan::After | ir::RepeatEmitPlan::All => {
                    emitted.extend(next_rows.iter().cloned());
                }
                ir::RepeatEmitPlan::AfterIf { predicate } => {
                    emitted.extend(
                        self.filter_rows(next_rows.clone(), predicate, "repeat.after_if")
                            .await?,
                    );
                }
                ir::RepeatEmitPlan::None | ir::RepeatEmitPlan::Before => {}
            }

            let stop = self.repeat_should_stop(&next_rows, &plan.stop).await?;
            current_rows = next_rows;
            if current_rows.is_empty() || stop {
                break;
            }
        }

        match &plan.emit {
            ir::RepeatEmitPlan::None => Ok(ExecutionValue::Stream(current_rows)),
            ir::RepeatEmitPlan::Before
            | ir::RepeatEmitPlan::After
            | ir::RepeatEmitPlan::AfterIf { .. }
            | ir::RepeatEmitPlan::All => Ok(ExecutionValue::Stream(emitted)),
        }
    }

    async fn repeat_should_stop(
        &mut self,
        rows: &[ExecutionRow],
        stop: &ir::RepeatStopPlan,
    ) -> Result<bool> {
        match stop {
            ir::RepeatStopPlan::MaxDepthOnly | ir::RepeatStopPlan::Times { .. } => Ok(false),
            ir::RepeatStopPlan::Until { predicate }
            | ir::RepeatStopPlan::TimesOrUntil { predicate, .. } => Ok(!self
                .filter_rows(rows.to_vec(), predicate, "repeat.until")
                .await?
                .is_empty()),
        }
    }
}

fn repeat_max_depth(plan: &exec::ExecRepeatPlan) -> usize {
    match &plan.stop {
        ir::RepeatStopPlan::MaxDepthOnly | ir::RepeatStopPlan::Until { .. } => plan.max_depth.get(),
        ir::RepeatStopPlan::Times { count } | ir::RepeatStopPlan::TimesOrUntil { count, .. } => {
            core::cmp::min(plan.max_depth.get(), count.get())
        }
    }
}
