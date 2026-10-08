//! Execution state, output binding, and condition contracts.
//!
//! The scheduler owns step order. This module owns the interpreter-visible
//! state transitions that make named variables, root outputs, and conditional
//! execution observable.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;

impl<'db> ExecutionContext<'db> {
    /// Finishes execution keeping the root output, unreturned variables and
    /// requested returns, for callers that inspect the whole result.
    ///
    /// When the root step binds a returned variable, both `last` and the
    /// return own one copy of that value: two owned results cannot share it.
    /// Callers that only need the returns use [`Self::finish_returns`].
    pub(in crate::execution::interpreter) fn finish(
        &mut self,
        root: exec::ExecStepId,
        returns: &exec::ExecutableReturns,
    ) -> Result<ExecutionResult> {
        self.step_output_uses.remove(&root);
        let last = self.step_outputs.remove(&root);
        let returns = self.take_returns(returns)?;
        Ok(ExecutionResult {
            last,
            variables: std::mem::take(&mut self.variables).into_values(),
            returns,
        })
    }

    /// Finishes execution keeping only the requested returns.
    ///
    /// Every retained step output is released first, so a returned value
    /// whose other owner was a step output (the root step binding the
    /// returned variable, or any observed bound step) moves out without a
    /// copy.
    pub(in crate::execution::interpreter) fn finish_returns(
        &mut self,
        returns: &exec::ExecutableReturns,
    ) -> Result<ReturnedValues> {
        drop(std::mem::take(&mut self.step_outputs));
        self.take_returns(returns)
    }

    /// Moves requested returns out of the variable table. A value is copied
    /// only while another live interpreter location still shares it.
    fn take_returns(&mut self, returns: &exec::ExecutableReturns) -> Result<ReturnedValues> {
        match returns {
            exec::ExecutableReturns::None => Ok(BTreeMap::new()),
            exec::ExecutableReturns::Variables(returns) => returns
                .as_ref()
                .iter()
                .map(|planned| {
                    let shape = self
                        .variable_return_shapes
                        .get(planned.name())
                        .copied()
                        .unwrap_or_else(|| planned.shape());
                    let value = match self.variables.remove(planned.name()) {
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
                .collect(),
        }
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
    async fn finish_returns_root_output_unreturned_variables_and_requested_returns() {
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
        assert_eq!(
            result.variables,
            BTreeMap::from([(ignored.clone(), ExecutionValue::Stream(Vec::new()))])
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
    async fn finish_returns_unique_and_shared_values_unchanged() {
        let db = test_support::open_db("state-finish-shared-returns").await;
        let unique = name("unique");
        let forked = name("forked");
        let retained = step_id(5);
        let rows = ExecutionValue::Stream(vec![row(1), row(2)]);
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.variables.insert(unique.clone(), rows.clone());
        let (variable, step_output) = ExecutionValueSlot::from(rows.clone()).fork();
        ctx.variables.insert_slot(forked.clone(), variable);
        ctx.step_outputs.insert_slot(retained, step_output);
        let snapshot = ctx.variables.shallow_snapshot();

        let result = ctx
            .finish(
                step_id(9),
                &return_variables(vec![
                    ("unique", exec::ReturnShape::List),
                    ("forked", exec::ReturnShape::List),
                ]),
            )
            .unwrap();

        assert_eq!(
            result.returns,
            BTreeMap::from([
                (unique, ReturnedValue::Present(rows.clone())),
                (forked.clone(), ReturnedValue::Present(rows.clone())),
            ])
        );
        assert_eq!(ctx.step_outputs.get(&retained), Some(&rows));
        assert_eq!(snapshot.get(&forked), Some(&rows));
    }

    #[tokio::test]
    async fn finish_moves_uniquely_owned_returns_without_copying() {
        let db = test_support::open_db("state-finish-moves-returns").await;
        let returned = name("returned");
        let rows = vec![row(1), row(2)];
        let allocation = rows.as_ptr();
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.variables
            .insert(returned.clone(), ExecutionValue::Stream(rows));

        let result = ctx
            .finish(
                step_id(1),
                &return_variables(vec![("returned", exec::ReturnShape::List)]),
            )
            .unwrap();

        let Some(ReturnedValue::Present(ExecutionValue::Stream(rows))) =
            result.returns.get(&returned)
        else {
            panic!("returned stream is present");
        };
        assert_eq!(rows.as_ptr(), allocation);
        assert!(result.variables.is_empty());
    }

    fn allocation(value: &ExecutionValue) -> *const () {
        match value {
            ExecutionValue::Stream(rows) => rows.as_ptr().cast(),
            ExecutionValue::Scalars(values) => values.as_ptr().cast(),
            other @ (ExecutionValue::FoldedStream(_)
            | ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::IndexDdlReceipt(_)
            | ExecutionValue::IndexOperationStatus(_)) => {
                panic!("expected a row or scalar collection, got {other:?}")
            }
        }
    }

    fn returned_allocation(returns: &ReturnedValues, variable: &str) -> *const () {
        let Some(ReturnedValue::Present(value)) = returns.get(&name(variable)) else {
            panic!("`{variable}` is returned with a value");
        };
        allocation(value)
    }

    #[tokio::test]
    async fn finish_returns_moves_returns_shared_with_observed_step_outputs() {
        let db = test_support::open_db("state-finish-returns-shared").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let bound = |id, variable| exec::ExecStep {
            output: ir::BatchOutputPlan::Bind(name(variable)),
            ..test_support::step(id, Vec::new(), exec::ExecOp::Noop)
        };
        let earlier = bound(1, "earlier");
        let root = bound(2, "rows");
        for step in [&earlier, &root] {
            ctx.step_output_uses
                .insert(step.id, std::num::NonZeroUsize::MIN);
        }
        let earlier_rows = vec![row(1)];
        let root_rows = vec![row(2), row(3)];
        let (earlier_allocation, root_allocation) = (
            earlier_rows.as_ptr().cast::<()>(),
            root_rows.as_ptr().cast::<()>(),
        );
        ctx.record_step_value(&earlier, ExecutionValue::Stream(earlier_rows));
        ctx.record_step_value(&root, ExecutionValue::Stream(root_rows));

        let returns = ctx
            .finish_returns(&return_variables(vec![
                ("earlier", exec::ReturnShape::List),
                ("rows", exec::ReturnShape::List),
            ]))
            .unwrap();

        assert_eq!(returned_allocation(&returns, "earlier"), earlier_allocation);
        assert_eq!(returned_allocation(&returns, "rows"), root_allocation);
        assert_eq!(
            returns.get(&name("rows")),
            Some(&ReturnedValue::Present(ExecutionValue::Stream(vec![
                row(2),
                row(3)
            ])))
        );
        assert!(ctx.step_outputs.is_empty());
    }

    #[tokio::test]
    async fn finish_returns_matches_finish_and_rejects_missing_returns() {
        let db = test_support::open_db("state-finish-returns-errors").await;
        let returns = return_variables(vec![
            ("list", exec::ReturnShape::List),
            ("missing_object", exec::ReturnShape::Object),
            ("count", exec::ReturnShape::Scalar),
        ]);
        let context = || {
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.variables
                .insert(name("list"), ExecutionValue::Stream(Vec::new()));
            ctx.variables
                .insert(name("count"), ExecutionValue::Count(4));
            ctx
        };

        assert_eq!(
            context().finish_returns(&returns).unwrap(),
            context().finish(step_id(1), &returns).unwrap().returns
        );
        assert!(context()
            .finish_returns(&exec::ExecutableReturns::None)
            .unwrap()
            .is_empty());
        let err = context()
            .finish_returns(&return_variables(vec![(
                "missing",
                exec::ReturnShape::Scalar,
            )]))
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("return variable `missing` is not bound"));
    }

    /// Pins the production shape: the planner binds the returned variable on
    /// the root step, which the scheduler always observes, so the variable and
    /// the root output share one allocation until execution finishes.
    #[tokio::test]
    async fn planned_root_bound_returns_move_out_without_copying() {
        use helix_ast::batch::read_batch;
        use helix_ast::traversal::g;

        let db = test_support::open_db("state-planned-root-returns").await;
        for user in ["ada", "grace", "linus"] {
            test_support::add_user(&db, user).await;
        }
        let batches = [
            read_batch()
                .var_as("rows", g().n_with_label("User"))
                .returning(["rows"]),
            read_batch()
                .var_as("rows", g().n_with_label("User").values(vec!["name"]))
                .returning(["rows"]),
        ];
        for batch in batches {
            let plan = helix_planner::planning::plan_read_batch(
                &batch,
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap();
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.enable_request_read_view().await.unwrap();
            ctx.execute_steps(
                plan.steps(),
                plan.execution_order(),
                plan.root(),
                plan.execution_program(),
            )
            .await
            .unwrap();
            let variable = ctx.variables.get(&name("rows")).expect("rows are bound");
            let root_output = ctx
                .step_outputs
                .get(&plan.root())
                .expect("root is observed");
            let shared = allocation(variable);
            assert_eq!(allocation(root_output), shared);
            assert_eq!(variable.len(), 3);

            let returns = ctx.finish_returns(plan.executable_returns()).unwrap();

            assert_eq!(returned_allocation(&returns, "rows"), shared);
            let full = Interpreter::new(&db, context::ParamBindings::default())
                .execute(&plan)
                .await
                .unwrap();
            assert_eq!(full.returns, returns);
            let Some(ReturnedValue::Present(returned)) = returns.get(&name("rows")) else {
                panic!("rows are returned");
            };
            assert_eq!(full.last.as_ref(), Some(returned));
            assert_eq!(
                Interpreter::new(&db, context::ParamBindings::default())
                    .execute_returns(&plan)
                    .await
                    .unwrap(),
                returns
            );
        }
        db.close().await.unwrap();
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
