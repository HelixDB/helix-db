//! Execution state, output binding, and condition contracts.
//!
//! The scheduler owns step order. This module owns the interpreter-visible
//! state transitions that make named variables, root outputs, and conditional
//! execution observable.

use std::collections::BTreeMap;

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn finish(
        self,
        root: exec::ExecStepId,
        returns: &ir::ReturnPlan,
    ) -> Result<ExecutionResult> {
        let last = self.step_outputs.get(&root).cloned();
        let returns = match returns {
            ir::ReturnPlan::None => BTreeMap::new(),
            ir::ReturnPlan::Variables(names) => names
                .as_ref()
                .iter()
                .map(|name| {
                    self.variables
                        .get(name)
                        .cloned()
                        .map(|value| (name.clone(), value))
                        .ok_or_else(|| missing_variable("return variable", name))
                })
                .collect::<Result<BTreeMap<_, _>>>()?,
        };
        Ok(ExecutionResult {
            last,
            variables: self.variables,
            returns,
        })
    }

    pub(in crate::execution::interpreter) fn bind_output(
        &mut self,
        output: &ir::BatchOutputPlan,
        value: &ExecutionValue,
    ) {
        match output {
            ir::BatchOutputPlan::Discard => {}
            ir::BatchOutputPlan::Bind(name) => {
                self.variables.insert(name.clone(), value.clone());
            }
        }
    }

    pub(in crate::execution::interpreter) fn condition_allows(
        &self,
        condition: &exec::ExecCondition,
    ) -> Result<bool> {
        match condition {
            exec::ExecCondition::Always => Ok(true),
            exec::ExecCondition::Variable(condition) => self.variable_condition_allows(condition),
            exec::ExecCondition::PreviousStepNotEmpty { dependency } => Ok(self
                .step_outputs
                .get(dependency)
                .is_some_and(|value| !value.is_empty())),
        }
    }

    pub(in crate::execution::interpreter) fn variable_value(
        &self,
        name: &ir::NonEmptyString,
    ) -> Result<&ExecutionValue> {
        self.variables
            .get(name)
            .ok_or_else(|| missing_variable("variable", name))
    }

    fn variable_condition_allows(
        &self,
        condition: &ir::BatchVariableConditionPlan,
    ) -> Result<bool> {
        match condition {
            ir::BatchVariableConditionPlan::VarNotEmpty(name) => {
                Ok(!self.variable_value(name)?.is_empty())
            }
            ir::BatchVariableConditionPlan::VarEmpty(name) => {
                Ok(self.variable_value(name)?.is_empty())
            }
            ir::BatchVariableConditionPlan::VarMinSize(name, size) => {
                Ok(self.variable_value(name)?.len() >= size.get())
            }
        }
    }
}

fn missing_variable(kind: &str, name: &ir::NonEmptyString) -> HelixDbError {
    HelixDbError::Query(format!("{kind} `{name}` is not bound"))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use helix_planner::context;

    use super::test_support;
    use super::*;

    fn step_id(id: usize) -> exec::ExecStepId {
        exec::ExecStepId::new(id).expect("positive test step id")
    }

    fn row(id: u64) -> ExecutionRow {
        ExecutionRow::current(ElementRef::Node(id))
    }

    fn name(value: &str) -> ir::NonEmptyString {
        test_support::name(value)
    }

    fn return_variables(names: Vec<&str>) -> ir::ReturnPlan {
        ir::ReturnPlan::Variables(
            ir::ReturnVariables::new(
                ir::AtLeast::<_, 1>::try_from_vec(names.into_iter().map(name).collect())
                    .expect("non-empty return variable list"),
            )
            .expect("unique return variables"),
        )
    }

    #[tokio::test]
    async fn finish_returns_root_output_all_variables_and_requested_returns() {
        let db = test_support::open_db("state-finish-returns").await;
        let root = step_id(2);
        let all = name("all");
        let selected = name("selected");
        let ignored = name("ignored");
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.step_outputs
            .insert(root, ExecutionValue::Stream(vec![row(7)]));
        ctx.variables.insert(all.clone(), ExecutionValue::Count(11));
        ctx.variables
            .insert(selected.clone(), ExecutionValue::Bool(true));
        ctx.variables
            .insert(ignored.clone(), ExecutionValue::Stream(Vec::new()));

        let result = ctx
            .finish(root, &return_variables(vec!["all", "selected"]))
            .unwrap();

        assert_eq!(result.last, Some(ExecutionValue::Stream(vec![row(7)])));
        assert_eq!(result.variables.len(), 3);
        assert_eq!(
            result.variables.get(&ignored),
            Some(&ExecutionValue::Stream(Vec::new()))
        );
        assert_eq!(result.returns.len(), 2);
        assert_eq!(result.returns.get(&all), Some(&ExecutionValue::Count(11)));
        assert_eq!(
            result.returns.get(&selected),
            Some(&ExecutionValue::Bool(true))
        );
        assert!(!result.returns.contains_key(&ignored));
    }

    #[tokio::test]
    async fn finish_without_return_variables_preserves_last_and_variable_state() {
        let db = test_support::open_db("state-finish-no-returns").await;
        let root = step_id(1);
        let bound = name("bound");
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.step_outputs.insert(root, ExecutionValue::Count(3));
        ctx.variables
            .insert(bound.clone(), ExecutionValue::Stream(vec![row(1), row(2)]));

        let result = ctx.finish(root, &ir::ReturnPlan::None).unwrap();

        assert_eq!(result.last, Some(ExecutionValue::Count(3)));
        assert!(result.returns.is_empty());
        assert_eq!(
            result.variables.get(&bound),
            Some(&ExecutionValue::Stream(vec![row(1), row(2)]))
        );
    }

    #[tokio::test]
    async fn finish_rejects_missing_return_variable_by_name() {
        let db = test_support::open_db("state-finish-missing-return").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        let err = ctx
            .finish(step_id(1), &return_variables(vec!["missing"]))
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("return variable `missing` is not bound"));
    }

    #[tokio::test]
    async fn bind_output_discards_or_clones_values_into_variables() {
        let db = test_support::open_db("state-bind-output").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let bound = name("result");
        let value = ExecutionValue::Stream(vec![row(9)]);

        ctx.bind_output(&ir::BatchOutputPlan::Discard, &value);
        assert!(ctx.variables.is_empty());

        ctx.bind_output(&ir::BatchOutputPlan::Bind(bound.clone()), &value);
        assert_eq!(ctx.variables.get(&bound), Some(&value));
    }

    #[tokio::test]
    async fn previous_step_condition_depends_on_bound_output_emptiness() {
        let db = test_support::open_db("state-previous-step-condition").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let empty = step_id(1);
        let non_empty_stream = step_id(2);
        let non_empty_count = step_id(3);
        let false_bool = step_id(4);
        ctx.step_outputs
            .insert(empty, ExecutionValue::Stream(Vec::new()));
        ctx.step_outputs
            .insert(non_empty_stream, ExecutionValue::Stream(vec![row(1)]));
        ctx.step_outputs
            .insert(non_empty_count, ExecutionValue::Count(2));
        ctx.step_outputs
            .insert(false_bool, ExecutionValue::Bool(false));

        assert!(ctx.condition_allows(&exec::ExecCondition::Always).unwrap());
        assert!(!ctx
            .condition_allows(&exec::ExecCondition::PreviousStepNotEmpty {
                dependency: step_id(99),
            })
            .unwrap());
        assert!(!ctx
            .condition_allows(&exec::ExecCondition::PreviousStepNotEmpty { dependency: empty })
            .unwrap());
        assert!(ctx
            .condition_allows(&exec::ExecCondition::PreviousStepNotEmpty {
                dependency: non_empty_stream,
            })
            .unwrap());
        assert!(ctx
            .condition_allows(&exec::ExecCondition::PreviousStepNotEmpty {
                dependency: non_empty_count,
            })
            .unwrap());
        assert!(!ctx
            .condition_allows(&exec::ExecCondition::PreviousStepNotEmpty {
                dependency: false_bool,
            })
            .unwrap());
    }

    #[tokio::test]
    async fn variable_conditions_use_execution_value_cardinality() {
        let db = test_support::open_db("state-variable-conditions").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let empty = name("empty");
        let stream = name("stream");
        let folded = name("folded");
        let bool_false = name("bool_false");
        ctx.variables
            .insert(empty.clone(), ExecutionValue::Stream(Vec::new()));
        ctx.variables
            .insert(stream.clone(), ExecutionValue::Stream(vec![row(1), row(2)]));
        ctx.variables.insert(
            folded.clone(),
            ExecutionValue::FoldedStream(FoldedStream::new(vec![row(7), row(8)])),
        );
        ctx.variables
            .insert(bool_false.clone(), ExecutionValue::Bool(false));

        assert!(ctx
            .condition_allows(&exec::ExecCondition::Variable(
                ir::BatchVariableConditionPlan::VarEmpty(empty.clone()),
            ))
            .unwrap());
        assert!(!ctx
            .condition_allows(&exec::ExecCondition::Variable(
                ir::BatchVariableConditionPlan::VarNotEmpty(empty),
            ))
            .unwrap());
        assert!(ctx
            .condition_allows(&exec::ExecCondition::Variable(
                ir::BatchVariableConditionPlan::VarNotEmpty(stream.clone()),
            ))
            .unwrap());
        assert!(ctx
            .condition_allows(&exec::ExecCondition::Variable(
                ir::BatchVariableConditionPlan::VarMinSize(
                    stream,
                    NonZeroUsize::new(2).expect("positive size"),
                ),
            ))
            .unwrap());
        assert!(!ctx
            .condition_allows(&exec::ExecCondition::Variable(
                ir::BatchVariableConditionPlan::VarMinSize(
                    folded,
                    NonZeroUsize::new(2).expect("positive size"),
                ),
            ))
            .unwrap());
        assert!(ctx
            .condition_allows(&exec::ExecCondition::Variable(
                ir::BatchVariableConditionPlan::VarEmpty(bool_false),
            ))
            .unwrap());
    }

    #[tokio::test]
    async fn variable_lookup_reports_unbound_variables() {
        let db = test_support::open_db("state-variable-missing").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        let err = ctx.variable_value(&name("missing")).unwrap_err();

        assert!(err.to_string().contains("variable `missing` is not bound"));
    }
}
