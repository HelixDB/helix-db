//! Nested executable subplan execution contracts.
//!
//! Control-flow operators execute validated executable DAGs inside the current
//! request context. Subplans share variables and parameters with the caller but
//! must isolate temporary step outputs from the outer DAG.

use futures::future::BoxFuture;

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn execute_subplan<'a>(
        &'a mut self,
        plan: &'a exec::ExecutableSubplan,
    ) -> BoxFuture<'a, Result<ExecutionValue>> {
        Box::pin(async move {
            let outer_outputs = std::mem::take(&mut self.step_outputs);
            let result = async {
                self.execute_steps(plan.steps(), plan.execution_order())
                    .await?;
                self.subplan_root_output(plan.root())
            }
            .await;
            self.step_outputs = outer_outputs;
            result
        })
    }

    fn subplan_root_output(&self, root: exec::ExecStepId) -> Result<ExecutionValue> {
        self.step_outputs.get(&root).cloned().ok_or_else(|| {
            HelixDbError::InvariantViolation(format!(
                "subplan root step {} did not produce output",
                root.get()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use helix_planner::context;

    use super::test_support;
    use super::*;

    fn step_id(id: usize) -> exec::ExecStepId {
        exec::ExecStepId::new(id).expect("positive test step id")
    }

    fn name(value: &str) -> ir::NonEmptyString {
        test_support::name(value)
    }

    fn row(id: u64) -> ExecutionRow {
        ExecutionRow::current(ElementRef::Node(id))
    }

    fn source_inject_step(id: usize, variable: ir::NonEmptyString) -> exec::ExecStep {
        test_support::step(
            id,
            Vec::new(),
            exec::ExecOp::Variable {
                op: exec::ExecVariableOp::SourceInject { variable },
            },
        )
    }

    #[tokio::test]
    async fn subplan_returns_root_output_and_restores_outer_step_outputs() {
        let db = test_support::open_db("subplan-success-isolation").await;
        let seed = name("seed");
        let bound = name("bound");
        let outer_step = step_id(9);
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.step_outputs
            .insert(outer_step, ExecutionValue::Count(42));
        ctx.variables
            .insert(seed.clone(), ExecutionValue::Stream(vec![row(1), row(2)]));
        let mut step = source_inject_step(1, seed);
        step.output = ir::BatchOutputPlan::Bind(bound.clone());
        let subplan = test_support::subplan(vec![step], 1);

        let result = ctx.execute_subplan(&subplan).await.unwrap();

        let expected = ExecutionValue::Stream(vec![row(1), row(2)]);
        assert_eq!(result, expected);
        assert_eq!(ctx.step_outputs.len(), 1);
        assert_eq!(
            ctx.step_outputs.get(&outer_step),
            Some(&ExecutionValue::Count(42))
        );
        assert!(!ctx.step_outputs.contains_key(&step_id(1)));
        assert_eq!(ctx.variables.get(&bound), Some(&expected));
    }

    #[tokio::test]
    async fn subplan_restores_outer_step_outputs_after_failure() {
        let db = test_support::open_db("subplan-failure-isolation").await;
        let outer_step = step_id(7);
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.step_outputs
            .insert(outer_step, ExecutionValue::Stream(vec![row(99)]));
        let subplan = test_support::subplan(vec![source_inject_step(1, name("missing"))], 1);

        let err = ctx.execute_subplan(&subplan).await.unwrap_err();

        assert!(err.to_string().contains("variable `missing` is not bound"));
        assert_eq!(ctx.step_outputs.len(), 1);
        assert_eq!(
            ctx.step_outputs.get(&outer_step),
            Some(&ExecutionValue::Stream(vec![row(99)]))
        );
        assert!(!ctx.step_outputs.contains_key(&step_id(1)));
    }

    #[tokio::test]
    async fn subplan_root_output_reports_missing_roots_by_id() {
        let db = test_support::open_db("subplan-missing-root-output").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        let err = ctx.subplan_root_output(step_id(11)).unwrap_err();

        assert!(err
            .to_string()
            .contains("subplan root step 11 did not produce output"));
    }
}
