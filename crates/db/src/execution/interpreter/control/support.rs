//! Shared executable control-flow helpers.

use std::collections::BTreeMap;

use helix_planner::{exec, ir};

use super::super::{ExecutionContext, ExecutionRow, ExecutionValue};
use crate::error::Result;

pub(super) fn context_variable() -> ir::NonEmptyString {
    ir::NonEmptyString::new("$context").expect("$context is a non-empty planner variable")
}

pub(super) fn restore_variable(
    variables: &mut BTreeMap<ir::NonEmptyString, ExecutionValue>,
    variable: ir::NonEmptyString,
    previous: Option<ExecutionValue>,
) {
    match previous {
        Some(value) => {
            variables.insert(variable, value);
        }
        None => {
            variables.remove(&variable);
        }
    }
}

impl<'db> ExecutionContext<'db> {
    pub(super) async fn filter_rows(
        &mut self,
        rows: Vec<ExecutionRow>,
        predicate: &ir::PredicatePlan,
        op: &'static str,
    ) -> Result<Vec<ExecutionRow>> {
        let filtered = self.filter(ExecutionValue::Stream(rows), predicate).await?;
        self.stream_rows(filtered, op)
    }

    pub(super) async fn subplan_stream_rows(
        &mut self,
        plan: &exec::ExecutableSubplan,
        op: &'static str,
    ) -> Result<Vec<ExecutionRow>> {
        let value = self.execute_subplan(plan).await?;
        self.stream_rows(value, op)
    }

    pub(super) async fn subplan_stream_rows_for_context_row(
        &mut self,
        plan: &exec::ExecutableSubplan,
        context: &ir::NonEmptyString,
        row: ExecutionRow,
        op: &'static str,
    ) -> Result<Vec<ExecutionRow>> {
        self.variables
            .insert(context.clone(), ExecutionValue::Stream(vec![row]));
        self.subplan_stream_rows(plan, op).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_variable_reinstates_previous_value() {
        let variable = context_variable();
        let mut variables = BTreeMap::from([(variable.clone(), ExecutionValue::Count(1))]);

        restore_variable(
            &mut variables,
            variable.clone(),
            Some(ExecutionValue::Count(2)),
        );

        assert_eq!(variables.get(&variable), Some(&ExecutionValue::Count(2)));
    }
}
