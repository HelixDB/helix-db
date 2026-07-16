//! Executable-step dependency input contracts.
//!
//! The scheduler validates ordering; this module owns how already-produced
//! dependency values become the next operation's input.

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn dependency_input(
        &self,
        dependencies: &[exec::ExecStepId],
    ) -> Result<ExecutionValue> {
        match dependencies {
            [] => Ok(ExecutionValue::Stream(Vec::new())),
            [dependency] => self.step_output(*dependency),
            dependencies => self.concat_values(self.dependency_values(dependencies)?),
        }
    }

    pub(in crate::execution::interpreter) fn dependency_values(
        &self,
        dependencies: &[exec::ExecStepId],
    ) -> Result<Vec<ExecutionValue>> {
        dependencies
            .iter()
            .copied()
            .map(|dependency| self.step_output(dependency))
            .collect()
    }

    fn step_output(&self, dependency: exec::ExecStepId) -> Result<ExecutionValue> {
        self.step_outputs
            .get(&dependency)
            .cloned()
            .ok_or_else(|| missing_dependency_output(dependency))
    }

    fn concat_values(&self, values: Vec<ExecutionValue>) -> Result<ExecutionValue> {
        let mut rows = Vec::new();
        let mut scalars = Vec::new();
        let mut saw_stream = false;
        let mut saw_scalar = false;
        for value in values {
            match value {
                ExecutionValue::Stream(mut value_rows) => {
                    saw_stream = true;
                    rows.append(&mut value_rows);
                }
                ExecutionValue::FoldedStream(_) => {
                    return Err(HelixDbError::Query(
                        "cannot concatenate folded stream dependency output; unfold it first"
                            .to_string(),
                    ));
                }
                ExecutionValue::Scalars(mut value_scalars) => {
                    saw_scalar = true;
                    scalars.append(&mut value_scalars);
                }
                ExecutionValue::Count(count) => {
                    saw_scalar = true;
                    scalars.push(ExecutionScalar::Value(DbPropertyValue::I64(
                        count.try_into().unwrap_or(i64::MAX),
                    )));
                }
                ExecutionValue::Bool(value) => {
                    saw_scalar = true;
                    scalars.push(ExecutionScalar::Value(DbPropertyValue::Bool(value)));
                }
                ExecutionValue::IndexDdlReceipt(_) | ExecutionValue::IndexOperationStatus(_) => {
                    return Err(HelixDbError::Query(
                        "cannot concatenate index lifecycle dependency outputs".to_string(),
                    ));
                }
            }
        }
        match (saw_stream, saw_scalar) {
            (true, false) => Ok(ExecutionValue::Stream(rows)),
            (false, true) => Ok(ExecutionValue::Scalars(scalars)),
            (false, false) => Ok(ExecutionValue::Stream(Vec::new())),
            (true, true) => Err(HelixDbError::Query(
                "cannot concatenate mixed stream and scalar dependency outputs".to_string(),
            )),
        }
    }
}

fn missing_dependency_output(dependency: exec::ExecStepId) -> HelixDbError {
    HelixDbError::InvariantViolation(format!(
        "dependency step {} has not executed",
        dependency.get()
    ))
}

#[cfg(test)]
mod tests {
    use helix_planner::context;

    use super::test_support;
    use super::*;

    fn step_id(id: usize) -> exec::ExecStepId {
        exec::ExecStepId::new(id).expect("positive test step id")
    }

    fn row(id: u64) -> ExecutionRow {
        ExecutionRow::current(ElementRef::Node(id))
    }

    #[tokio::test]
    async fn dependency_input_preserves_empty_single_and_ordered_stream_shapes() {
        let db = test_support::open_db("dependency-input-stream-shapes").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.step_outputs
            .insert(step_id(1), ExecutionValue::Stream(vec![row(2), row(1)]));
        ctx.step_outputs
            .insert(step_id(2), ExecutionValue::Stream(vec![row(3)]));

        assert_eq!(
            ctx.dependency_input(&[]).unwrap(),
            ExecutionValue::Stream(Vec::new())
        );
        assert_eq!(
            ctx.concat_values(Vec::new()).unwrap(),
            ExecutionValue::Stream(Vec::new())
        );
        assert_eq!(
            ctx.dependency_input(&[step_id(1)]).unwrap(),
            ExecutionValue::Stream(vec![row(2), row(1)])
        );
        assert_eq!(
            ctx.dependency_input(&[step_id(1), step_id(2)]).unwrap(),
            ExecutionValue::Stream(vec![row(2), row(1), row(3)])
        );
    }

    #[tokio::test]
    async fn dependency_values_reject_missing_outputs_with_step_identity() {
        let db = test_support::open_db("dependency-input-missing-output").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        let err = ctx.dependency_values(&[step_id(7)]).unwrap_err();

        assert!(err
            .to_string()
            .contains("dependency step 7 has not executed"));
    }

    #[tokio::test]
    async fn dependency_concat_coerces_scalar_terminal_shapes_in_order() {
        let db = test_support::open_db("dependency-input-scalar-shapes").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.step_outputs
            .insert(step_id(1), ExecutionValue::Count(2));
        ctx.step_outputs
            .insert(step_id(2), ExecutionValue::Bool(true));
        ctx.step_outputs.insert(
            step_id(3),
            ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(9)]),
        );

        assert_eq!(
            ctx.dependency_input(&[step_id(1), step_id(2), step_id(3)])
                .unwrap(),
            ExecutionValue::Scalars(vec![
                ExecutionScalar::Value(DbPropertyValue::I64(2)),
                ExecutionScalar::Value(DbPropertyValue::Bool(true)),
                ExecutionScalar::NodeId(9),
            ])
        );
    }

    #[tokio::test]
    async fn dependency_concat_rejects_folded_and_mixed_dependency_shapes() {
        let folded_db = test_support::open_db("dependency-input-folded-shape").await;
        let mut folded = ExecutionContext::new(&folded_db, context::ParamBindings::default());
        folded.step_outputs.insert(
            step_id(1),
            ExecutionValue::FoldedStream(FoldedStream::new(vec![row(1)])),
        );
        let err = folded
            .dependency_input(&[step_id(1), step_id(1)])
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("cannot concatenate folded stream dependency output"));

        let mixed_db = test_support::open_db("dependency-input-mixed-shape").await;
        let mut mixed = ExecutionContext::new(&mixed_db, context::ParamBindings::default());
        mixed
            .step_outputs
            .insert(step_id(1), ExecutionValue::Stream(vec![row(1)]));
        mixed
            .step_outputs
            .insert(step_id(2), ExecutionValue::Count(1));
        let err = mixed
            .dependency_input(&[step_id(1), step_id(2)])
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("cannot concatenate mixed stream and scalar dependency outputs"));
    }
}
