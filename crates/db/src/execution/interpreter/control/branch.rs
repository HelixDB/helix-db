//! Executable branch control-flow interpretation.

use std::collections::BTreeSet;

use helix_planner::{exec, ir};

use super::super::{ElementRef, ExecutionContext, ExecutionRow, ExecutionValue};
use super::support::{context_variable, restore_variable};
use crate::error::{HelixDbError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CurrentElementKind {
    Node,
    Edge,
}

impl From<&ElementRef> for CurrentElementKind {
    fn from(value: &ElementRef) -> Self {
        match value {
            ElementRef::Node(_) => Self::Node,
            ElementRef::Edge(_) => Self::Edge,
        }
    }
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn execute_branch(
        &mut self,
        input: ExecutionValue,
        plan: &exec::ExecBranchPlan,
    ) -> Result<ExecutionValue> {
        let context = context_variable();
        let previous_context = self.variables.insert(context.clone(), input.clone());
        let result = self
            .execute_branch_with_context(input, plan, &context)
            .await;
        restore_variable(&mut self.variables, context, previous_context);
        result
    }

    async fn execute_branch_with_context(
        &mut self,
        input: ExecutionValue,
        plan: &exec::ExecBranchPlan,
        context: &ir::NonEmptyString,
    ) -> Result<ExecutionValue> {
        match plan {
            exec::ExecBranchPlan::Union(branches) => {
                let rows = self.stream_rows(input, "branch.union")?;
                let mut output = Vec::new();
                for row in rows {
                    for branch in branches.as_ref() {
                        output.extend(
                            self.subplan_stream_rows_for_context_row(
                                branch,
                                context,
                                row.clone(),
                                "branch.union",
                            )
                            .await?,
                        );
                    }
                }
                validate_bound_union_current_types(&output)?;
                Ok(ExecutionValue::Stream(output))
            }
            exec::ExecBranchPlan::Choose {
                condition,
                then_plan,
            } => {
                let input_rows = self.stream_rows(input, "branch.choose")?;
                let passing = self
                    .filter_rows(input_rows, condition, "branch.choose")
                    .await?;
                if passing.is_empty() {
                    Ok(ExecutionValue::Stream(Vec::new()))
                } else {
                    self.variables
                        .insert(context.clone(), ExecutionValue::Stream(passing));
                    self.execute_subplan(then_plan).await
                }
            }
            exec::ExecBranchPlan::ChooseElse {
                condition,
                then_plan,
                else_plan,
            } => {
                let input_rows = self.stream_rows(input, "branch.choose_else")?;
                let input_set = input_rows.iter().cloned().collect::<BTreeSet<_>>();
                let passing = self
                    .filter_rows(input_rows.clone(), condition, "branch.choose_else")
                    .await?;
                if passing.is_empty() {
                    self.variables
                        .insert(context.clone(), ExecutionValue::Stream(input_rows));
                    return self.execute_subplan(else_plan).await;
                }

                let passing_set = passing.iter().cloned().collect::<BTreeSet<_>>();
                if passing_set.len() == input_set.len() && passing_set == input_set {
                    self.variables
                        .insert(context.clone(), ExecutionValue::Stream(passing));
                    return self.execute_subplan(then_plan).await;
                }

                let failing = input_rows
                    .into_iter()
                    .filter(|row| !passing_set.contains(row))
                    .collect::<Vec<_>>();
                let mut output = Vec::new();
                self.variables
                    .insert(context.clone(), ExecutionValue::Stream(passing));
                output.extend(
                    self.subplan_stream_rows(then_plan, "branch.choose_else.then")
                        .await?,
                );
                self.variables
                    .insert(context.clone(), ExecutionValue::Stream(failing));
                output.extend(
                    self.subplan_stream_rows(else_plan, "branch.choose_else.else")
                        .await?,
                );
                Ok(ExecutionValue::Stream(output))
            }
            exec::ExecBranchPlan::Coalesce(branches) => {
                let rows = self.stream_rows(input, "branch.coalesce")?;
                let mut output = Vec::new();
                for row in rows {
                    for branch in branches.as_ref() {
                        let branch_rows = self
                            .subplan_stream_rows_for_context_row(
                                branch,
                                context,
                                row.clone(),
                                "branch.coalesce",
                            )
                            .await?;
                        if !branch_rows.is_empty() {
                            output.extend(branch_rows);
                            break;
                        }
                    }
                }
                Ok(ExecutionValue::Stream(output))
            }
            exec::ExecBranchPlan::Optional(branch) => {
                let rows = self.stream_rows(input, "branch.optional")?;
                let mut output = Vec::new();
                for row in rows {
                    let branch_rows = self
                        .subplan_stream_rows_for_context_row(
                            branch,
                            context,
                            row.clone(),
                            "branch.optional",
                        )
                        .await?;
                    if branch_rows.is_empty() {
                        output.push(row);
                    } else {
                        output.extend(branch_rows);
                    }
                }
                Ok(ExecutionValue::Stream(output))
            }
        }
    }
}

fn validate_bound_union_current_types(rows: &[ExecutionRow]) -> Result<()> {
    if !rows.iter().any(|row| !row.bindings.is_empty()) {
        return Ok(());
    }

    let mut current_kind = None;
    for kind in rows
        .iter()
        .filter_map(|row| row.current.as_ref().map(CurrentElementKind::from))
    {
        match current_kind {
            Some(existing) if existing != kind => {
                return Err(HelixDbError::Query(
                    "union row branches produced mixed current element types".to_string(),
                ));
            }
            Some(_) => {}
            None => current_kind = Some(kind),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding_name() -> ir::NonEmptyString {
        ir::NonEmptyString::new("bound").expect("test binding name is non-empty")
    }

    fn row(element: ElementRef, bound: bool) -> ExecutionRow {
        let mut row = ExecutionRow::current(element);
        if bound {
            row.bindings.insert(binding_name(), ElementRef::Node(1));
        }
        row
    }

    #[test]
    fn bound_union_current_type_validation_rejects_mixed_nodes_and_edges() {
        let err = validate_bound_union_current_types(&[
            row(ElementRef::Node(1), true),
            row(ElementRef::Edge(2), true),
        ])
        .expect_err("bound mixed union rows are rejected");
        assert!(matches!(
            err,
            HelixDbError::Query(message)
                if message.contains("union row branches produced mixed current element types")
        ));

        validate_bound_union_current_types(&[
            row(ElementRef::Node(1), false),
            row(ElementRef::Edge(2), false),
        ])
        .expect("unbound mixed union rows remain valid");
        validate_bound_union_current_types(&[
            row(ElementRef::Node(1), true),
            row(ElementRef::Node(2), true),
        ])
        .expect("bound same-kind union rows remain valid");
    }
}
