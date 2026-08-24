//! Execution state, output binding, and condition contracts.
//!
//! The scheduler owns step order. This module owns the interpreter-visible
//! state transitions that make named variables, root outputs, and conditional
//! execution observable.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn finish(
        &mut self,
        root: exec::ExecStepId,
        returns: &exec::ExecutableReturns,
    ) -> Result<ExecutionResult> {
        self.step_output_uses.remove(&root);
        let last = self.step_outputs.remove(&root);
        let returns = match returns {
            exec::ExecutableReturns::None => BTreeMap::new(),
            exec::ExecutableReturns::Variables(returns) => returns
                .as_ref()
                .iter()
                .map(|planned| {
                    let shape = self
                        .variable_return_shapes
                        .get(planned.name())
                        .copied()
                        .unwrap_or_else(|| planned.shape());
                    let value = match self.variables.get(planned.name()).cloned() {
                        Some(value) if value.is_empty() => match shape {
                            exec::ReturnShape::List => ReturnedValue::EmptyList,
                            exec::ReturnShape::Object => ReturnedValue::EmptyObject,
                            exec::ReturnShape::Scalar => ReturnedValue::Present(value),
                        },
                        Some(value) => ReturnedValue::Present(value),
                        None => match planned.shape() {
                            exec::ReturnShape::List => ReturnedValue::EmptyList,
                            exec::ReturnShape::Object => ReturnedValue::EmptyObject,
                            exec::ReturnShape::Scalar => {
                                return Err(missing_variable("return variable", planned.name()));
                            }
                        },
                    };
                    Ok((planned.name().clone(), value))
                })
                .collect::<Result<BTreeMap<_, _>>>()?,
        };
        Ok(ExecutionResult {
            last,
            variables: std::mem::take(&mut self.variables).into_values(),
            returns,
        })
    }

    #[cfg(test)]
    pub(in crate::execution::interpreter) fn bind_output(
        &mut self,
        output: &ir::BatchOutputPlan,
        value: ExecutionValue,
    ) {
        match output {
            ir::BatchOutputPlan::Discard => {}
            ir::BatchOutputPlan::Bind(name) => {
                self.variables.insert(name.clone(), value);
            }
        }
    }

    /// Retains a completed step only where the validated plan can observe it.
    pub(in crate::execution::interpreter) fn record_step_value(
        &mut self,
        step: &exec::ExecStep,
        value: ExecutionValue,
    ) {
        let step_is_observed = self.step_output_uses.contains_key(&step.id);
        match (&step.output, step_is_observed) {
            (ir::BatchOutputPlan::Discard, false) => {}
            (ir::BatchOutputPlan::Discard, true) => {
                self.step_outputs.insert(step.id, value);
            }
            (ir::BatchOutputPlan::Bind(name), false) => {
                Arc::make_mut(&mut self.variable_return_shapes)
                    .insert(name.clone(), step.inferred_return_shape());
                self.variables.insert(name.clone(), value);
            }
            (ir::BatchOutputPlan::Bind(name), true) => {
                Arc::make_mut(&mut self.variable_return_shapes)
                    .insert(name.clone(), step.inferred_return_shape());
                let (variable, step_output) = ExecutionValueSlot::from(value).fork();
                self.variables.insert_slot(name.clone(), variable);
                self.step_outputs.insert_slot(step.id, step_output);
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

    use helix_planner::{context, properties};

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

    fn return_variables(returns: Vec<(&str, exec::ReturnShape)>) -> exec::ExecutableReturns {
        exec::ExecutableReturns::Variables(
            exec::ExecutableReturnVariables::new(
                ir::AtLeast::<_, 1>::try_from_vec(
                    returns
                        .into_iter()
                        .map(|(variable, shape)| exec::ExecutableReturn::new(name(variable), shape))
                        .collect(),
                )
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
            .finish(
                root,
                &return_variables(vec![
                    ("all", exec::ReturnShape::Scalar),
                    ("selected", exec::ReturnShape::Scalar),
                ]),
            )
            .unwrap();

        assert_eq!(result.last, Some(ExecutionValue::Stream(vec![row(7)])));
        assert_eq!(result.variables.len(), 3);
        assert_eq!(
            result.variables.get(&ignored),
            Some(&ExecutionValue::Stream(Vec::new()))
        );
        assert_eq!(result.returns.len(), 2);
        assert_eq!(
            result.returns.get(&all),
            Some(&ReturnedValue::Present(ExecutionValue::Count(11)))
        );
        assert_eq!(
            result.returns.get(&selected),
            Some(&ReturnedValue::Present(ExecutionValue::Bool(true)))
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

        let result = ctx.finish(root, &exec::ExecutableReturns::None).unwrap();

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
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        let err = ctx
            .finish(
                step_id(1),
                &return_variables(vec![("missing", exec::ReturnShape::Scalar)]),
            )
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("return variable `missing` is not bound"));
    }

    #[tokio::test]
    async fn finish_normalizes_only_empty_list_and_object_returns() {
        let db = test_support::open_db("state-finish-empty-shapes").await;
        let root = step_id(1);
        let list = name("list");
        let object = name("object");
        let scalar = name("scalar");
        let missing_list = name("missing_list");
        let missing_object = name("missing_object");
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.variables
            .insert(list.clone(), ExecutionValue::Stream(Vec::new()));
        ctx.variables
            .insert(object.clone(), ExecutionValue::Scalars(Vec::new()));
        ctx.variables
            .insert(scalar.clone(), ExecutionValue::Count(0));

        let result = ctx
            .finish(
                root,
                &return_variables(vec![
                    ("list", exec::ReturnShape::List),
                    ("object", exec::ReturnShape::Object),
                    ("scalar", exec::ReturnShape::Scalar),
                    ("missing_list", exec::ReturnShape::List),
                    ("missing_object", exec::ReturnShape::Object),
                ]),
            )
            .unwrap();

        assert_eq!(result.returns.get(&list), Some(&ReturnedValue::EmptyList));
        assert_eq!(
            result.returns.get(&object),
            Some(&ReturnedValue::EmptyObject)
        );
        assert_eq!(
            result.returns.get(&scalar),
            Some(&ReturnedValue::Present(ExecutionValue::Count(0)))
        );
        assert_eq!(
            result.returns.get(&missing_list),
            Some(&ReturnedValue::EmptyList)
        );
        assert_eq!(
            result.returns.get(&missing_object),
            Some(&ReturnedValue::EmptyObject)
        );
    }

    #[tokio::test]
    async fn finish_uses_the_executed_shape_only_for_empty_values() {
        let db = test_support::open_db("state-finish-executed-shapes").await;
        let empty_scalar = name("empty_scalar");
        let present_scalar = name("present_scalar");
        let empty_object = name("empty_object");
        let present_object = name("present_object");
        let empty_list = name("empty_list");
        let present_list = name("present_list");
        let mut empty_scalar_step = test_support::step(
            1,
            Vec::new(),
            exec::ExecOp::Count {
                plan: Box::new(exec::ExecCountPlan::Constant(0)),
            },
        );
        empty_scalar_step.output = ir::BatchOutputPlan::Bind(empty_scalar.clone());
        let mut present_scalar_step = empty_scalar_step.clone();
        present_scalar_step.id = step_id(2);
        present_scalar_step.output = ir::BatchOutputPlan::Bind(present_scalar.clone());
        let mut empty_object_step = test_support::step(3, Vec::new(), exec::ExecOp::Noop);
        empty_object_step.delivered.cardinality = properties::CardinalityBounds::zero_to(Some(1));
        empty_object_step.output = ir::BatchOutputPlan::Bind(empty_object.clone());
        let mut present_object_step = empty_object_step.clone();
        present_object_step.id = step_id(4);
        present_object_step.output = ir::BatchOutputPlan::Bind(present_object.clone());
        let mut empty_list_step = test_support::step(5, Vec::new(), exec::ExecOp::Noop);
        empty_list_step.output = ir::BatchOutputPlan::Bind(empty_list.clone());
        let mut present_list_step = empty_list_step.clone();
        present_list_step.id = step_id(6);
        present_list_step.output = ir::BatchOutputPlan::Bind(present_list.clone());
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.record_step_value(&empty_scalar_step, ExecutionValue::Count(0));
        ctx.record_step_value(&present_scalar_step, ExecutionValue::Count(7));
        ctx.record_step_value(&empty_object_step, ExecutionValue::Stream(Vec::new()));
        ctx.record_step_value(&present_object_step, ExecutionValue::Stream(vec![row(1)]));
        ctx.record_step_value(&empty_list_step, ExecutionValue::Stream(Vec::new()));
        ctx.record_step_value(
            &present_list_step,
            ExecutionValue::Stream(vec![row(2), row(3)]),
        );

        let execution = ctx
            .finish(
                present_list_step.id,
                &return_variables(vec![
                    ("empty_scalar", exec::ReturnShape::Object),
                    ("present_scalar", exec::ReturnShape::Object),
                    ("empty_object", exec::ReturnShape::List),
                    ("present_object", exec::ReturnShape::List),
                    ("empty_list", exec::ReturnShape::Object),
                    ("present_list", exec::ReturnShape::Object),
                ]),
            )
            .unwrap();

        assert_eq!(
            execution.returns.get(&empty_scalar),
            Some(&ReturnedValue::Present(ExecutionValue::Count(0)))
        );
        assert_eq!(
            execution.returns.get(&present_scalar),
            Some(&ReturnedValue::Present(ExecutionValue::Count(7)))
        );
        assert_eq!(
            execution.returns.get(&empty_object),
            Some(&ReturnedValue::EmptyObject)
        );
        assert_eq!(
            execution.returns.get(&present_object),
            Some(&ReturnedValue::Present(ExecutionValue::Stream(vec![row(
                1
            )])))
        );
        assert_eq!(
            execution.returns.get(&empty_list),
            Some(&ReturnedValue::EmptyList)
        );
        assert_eq!(
            execution.returns.get(&present_list),
            Some(&ReturnedValue::Present(ExecutionValue::Stream(vec![
                row(2),
                row(3),
            ])))
        );
    }

    #[tokio::test]
    async fn bind_output_discards_or_moves_values_into_variables() {
        let db = test_support::open_db("state-bind-output").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let bound = name("result");
        let value = ExecutionValue::Stream(vec![row(9)]);

        ctx.bind_output(&ir::BatchOutputPlan::Discard, value.clone());
        assert!(ctx.variables.is_empty());

        ctx.bind_output(&ir::BatchOutputPlan::Bind(bound.clone()), value.clone());
        assert_eq!(ctx.variables.get(&bound), Some(&value));
    }

    #[tokio::test]
    async fn observed_bound_output_shares_one_allocation_until_consumed() {
        let db = test_support::open_db("state-shared-bound-output").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let step = exec::ExecStep {
            output: ir::BatchOutputPlan::Bind(name("result")),
            ..test_support::step(1, Vec::new(), exec::ExecOp::Noop)
        };
        ctx.step_output_uses
            .insert(step.id, std::num::NonZeroUsize::MIN);
        let rows = vec![row(9)];
        let original_rows = rows.as_ptr();

        ctx.record_step_value(&step, ExecutionValue::Stream(rows));

        let Some(ExecutionValue::Stream(variable_rows)) = ctx.variables.get(&name("result")) else {
            panic!("bound variable should retain the stream");
        };
        let Some(ExecutionValue::Stream(step_rows)) = ctx.step_outputs.get(&step.id) else {
            panic!("observed step should retain the stream");
        };
        assert_eq!(variable_rows.as_ptr(), original_rows);
        assert_eq!(step_rows.as_ptr(), original_rows);
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
