//! Executable DAG stage scheduling.
//!
//! The planner validates dependency order and labels independent ready stages.
//! The interpreter still owns the runtime safety decision: only context-isolated
//! read/stream operations run concurrently, and their outputs are merged back in
//! stable stage order.

use std::collections::BTreeMap;

use futures::future;

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(super) async fn execute_steps(
        &mut self,
        steps: &[exec::ExecStep],
        order: exec::ExecExecutionOrder,
    ) -> Result<()> {
        let by_id = steps
            .iter()
            .map(|step| (step.id, step))
            .collect::<BTreeMap<_, _>>();

        for stage in order.stages() {
            match StageExecutionMode::for_stage(stage, &by_id)? {
                StageExecutionMode::Serial => {
                    self.execute_serial_stage(stage, &by_id).await?;
                }
                StageExecutionMode::ParallelIsolated(policy) => {
                    if self.has_active_write_tx() {
                        self.execute_serial_stage(stage, &by_id).await?;
                    } else {
                        self.execute_parallel_isolated_stage(stage, &by_id, policy)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn execute_serial_stage(
        &mut self,
        stage: &exec::ExecExecutionStage,
        by_id: &BTreeMap<exec::ExecStepId, &exec::ExecStep>,
    ) -> Result<()> {
        for id in stage.iter() {
            let step = step_by_id(by_id, id)?;
            let value = self.execute_step(step).await?;
            self.record_step_output(step, value);
        }
        Ok(())
    }

    async fn execute_parallel_isolated_stage(
        &mut self,
        stage: &exec::ExecExecutionStage,
        by_id: &BTreeMap<exec::ExecStepId, &exec::ExecStep>,
        policy: exec::ExecParallelStagePolicy,
    ) -> Result<()> {
        let ids = stage.iter().collect::<Vec<_>>();
        for chunk in ids.chunks(policy.max_concurrency().get()) {
            let futures = chunk
                .iter()
                .map(|id| {
                    let step = step_by_id(by_id, *id)?;
                    let mut context = self.parallel_step_context();
                    Ok(async move {
                        context
                            .execute_step(step)
                            .await
                            .map(|value| CompletedStep::new(step, value))
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            for completed in future::try_join_all(futures).await? {
                self.record_step_output(completed.step, completed.value);
            }
        }
        Ok(())
    }

    fn parallel_step_context(&self) -> Self {
        Self {
            db: self.db,
            tenant_scope: self.tenant_scope,
            params: self.params.clone(),
            variables: self.variables.clone(),
            step_outputs: self.step_outputs.clone(),
            request_read_view: self.request_read_view.clone(),
            index_read_leases: self.index_read_leases.clone(),
            request_write_scope: runtime_context::RequestWriteScopeState::Disabled,
            row_mode_max_rows: self.row_mode_max_rows,
        }
    }

    fn record_step_output(&mut self, step: &exec::ExecStep, value: ExecutionValue) {
        self.bind_output(&step.output, &value);
        self.step_outputs.insert(step.id, value);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StageExecutionMode {
    Serial,
    ParallelIsolated(exec::ExecParallelStagePolicy),
}

impl StageExecutionMode {
    fn for_stage(
        stage: &exec::ExecExecutionStage,
        by_id: &BTreeMap<exec::ExecStepId, &exec::ExecStep>,
    ) -> Result<Self> {
        match stage {
            exec::ExecExecutionStage::Single(_) => Ok(Self::Serial),
            exec::ExecExecutionStage::Parallel(parallel) => {
                for id in parallel.iter() {
                    if !is_parallel_isolated_step(step_by_id(by_id, id)?) {
                        return Ok(Self::Serial);
                    }
                }
                Ok(Self::ParallelIsolated(parallel.policy()))
            }
        }
    }
}

#[derive(Debug)]
struct CompletedStep<'a> {
    step: &'a exec::ExecStep,
    value: ExecutionValue,
}

impl<'a> CompletedStep<'a> {
    fn new(step: &'a exec::ExecStep, value: ExecutionValue) -> Self {
        Self { step, value }
    }
}

fn step_by_id<'a>(
    by_id: &'a BTreeMap<exec::ExecStepId, &'a exec::ExecStep>,
    id: exec::ExecStepId,
) -> Result<&'a exec::ExecStep> {
    by_id.get(&id).copied().ok_or_else(|| {
        HelixDbError::InvariantViolation(format!(
            "execution order referenced missing step {}",
            id.get()
        ))
    })
}

fn is_parallel_isolated_step(step: &exec::ExecStep) -> bool {
    !matches!(step.schedule, exec::ExecSchedule::Barrier)
        && matches!(
            &step.op,
            exec::ExecOp::Access { .. }
                | exec::ExecOp::KvRead(_)
                | exec::ExecOp::Expand { .. }
                | exec::ExecOp::Filter { .. }
                | exec::ExecOp::Limit { .. }
                | exec::ExecOp::Skip { .. }
                | exec::ExecOp::Range { .. }
                | exec::ExecOp::Distinct
                | exec::ExecOp::Order { .. }
                | exec::ExecOp::Project { .. }
                | exec::ExecOp::Aggregate { .. }
                | exec::ExecOp::Merge { .. }
                | exec::ExecOp::Reserved { .. }
                | exec::ExecOp::Noop
        )
}

#[cfg(test)]
mod tests {
    use helix_planner::{context, exec, ir, properties, trace};
    use slatedb::IsolationLevel;

    use super::super::test_support;
    use super::super::{runtime_context, search_index};
    use super::*;

    fn id(value: usize) -> exec::ExecStepId {
        exec::ExecStepId::new(value).expect("positive step ID")
    }

    fn by_id(steps: &[exec::ExecStep]) -> BTreeMap<exec::ExecStepId, &exec::ExecStep> {
        steps.iter().map(|step| (step.id, step)).collect()
    }

    fn named(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).expect("valid name")
    }

    #[test]
    fn stage_mode_parallelizes_only_context_isolated_steps() {
        let isolated = vec![
            test_support::step(1, Vec::new(), exec::ExecOp::Noop),
            test_support::step(2, Vec::new(), exec::ExecOp::Noop),
        ];
        let policy =
            exec::ExecParallelStagePolicy::new(properties::PositiveUsize::new(1).unwrap(), false);
        let stage = exec::ExecExecutionStage::Parallel(exec::ExecParallelStage::new(
            ir::AtLeast::<_, 2>::from_pair(id(1), id(2)),
            policy,
        ));
        assert_eq!(
            StageExecutionMode::for_stage(&stage, &by_id(&isolated)).unwrap(),
            StageExecutionMode::ParallelIsolated(policy)
        );

        let stateful = vec![
            test_support::step(1, Vec::new(), exec::ExecOp::Noop),
            test_support::step(
                2,
                Vec::new(),
                exec::ExecOp::Variable {
                    op: exec::ExecVariableOp::Stream(ir::StreamVariableOp::Store(named("x"))),
                },
            ),
        ];
        assert_eq!(
            StageExecutionMode::for_stage(&stage, &by_id(&stateful)).unwrap(),
            StageExecutionMode::Serial
        );

        let barrier = vec![
            test_support::step(1, Vec::new(), exec::ExecOp::Noop),
            exec::ExecStep {
                schedule: exec::ExecSchedule::Barrier,
                ..test_support::step(2, Vec::new(), exec::ExecOp::Noop)
            },
        ];
        assert_eq!(
            StageExecutionMode::for_stage(&stage, &by_id(&barrier)).unwrap(),
            StageExecutionMode::Serial
        );
    }

    #[tokio::test]
    async fn parallel_stage_records_outputs_in_stable_stage_order() {
        let db = test_support::open_db("parallel-stage-output-order").await;
        test_support::add_user(&db, "alice").await;
        let output = named("seen");
        let first = exec::ExecStep {
            output: ir::BatchOutputPlan::Bind(output.clone()),
            op: exec::ExecOp::Access {
                plan: Box::new(exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Empty)),
            },
            ..test_support::step(1, Vec::new(), exec::ExecOp::Noop)
        };
        let second = exec::ExecStep {
            output: ir::BatchOutputPlan::Bind(output.clone()),
            op: exec::ExecOp::Access {
                plan: Box::new(exec::ExecAccessPlan::Node(
                    exec::ExecNodeAccessPlan::AllScan,
                )),
            },
            ..test_support::step(2, Vec::new(), exec::ExecOp::Noop)
        };
        let root = exec::ExecStep {
            dependencies: vec![id(1), id(2)],
            op: exec::ExecOp::Variable {
                op: exec::ExecVariableOp::SourceInject {
                    variable: output.clone(),
                },
            },
            ..test_support::step(3, Vec::new(), exec::ExecOp::Noop)
        };
        let plan = exec::ExecutablePlan::new(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            ir::AtLeast::<_, 1>::from_one_and_rest(first, vec![second, root]),
            id(3),
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("parallel read plan is valid");

        let order = plan.execution_order();
        let exec::ExecExecutionStage::Parallel(stage) = &order.stages()[0] else {
            panic!("first stage should be parallel");
        };
        assert_eq!(stage.max_concurrency().get(), 2);

        let result = db
            .execute(&plan, context::ParamBindings::default())
            .await
            .expect("parallel stage executes");
        let Some(ExecutionValue::Stream(rows)) = result.last else {
            panic!("root should expose the stage-order-winning variable stream");
        };
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn active_write_transaction_forces_parallel_stage_to_execute_serially() {
        let db = test_support::open_db("parallel-stage-active-write-transaction").await;
        let first = test_support::step(1, Vec::new(), exec::ExecOp::Noop);
        let second = test_support::step(2, Vec::new(), exec::ExecOp::Noop);
        let root = test_support::step(3, vec![id(1), id(2)], exec::ExecOp::Noop);
        let plan = exec::ExecutablePlan::new(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            ir::AtLeast::<_, 1>::from_one_and_rest(first, vec![second, root]),
            id(3),
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("parallel read plan is valid");
        assert!(matches!(
            &plan.execution_order().stages()[0],
            exec::ExecExecutionStage::Parallel(_)
        ));

        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.request_write_scope = runtime_context::RequestWriteScopeState::Active(Box::new(
            runtime_context::ActiveWriteTx {
                txn,
                text_indexes: search_index::TextIndexMaintenanceOutcome::default(),
                configured_indexes: crate::index_v2::ConfiguredIndexCatalog::default(),
                index_context: mutation::MutationIndexContext::for_configured_index_test(
                    std::sync::Arc::clone(db.simhasher_registry()),
                ),
            },
        ));

        context
            .execute_steps(plan.steps(), plan.execution_order())
            .await
            .expect("active transaction executes the parallel stage serially");

        assert_eq!(context.step_outputs.len(), 3);
    }

    #[tokio::test]
    async fn parallel_task_context_is_a_snapshot() {
        let db = test_support::open_db("parallel-context-snapshot").await;
        let variable = named("x");
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context
            .variables
            .insert(variable.clone(), ExecutionValue::Stream(Vec::new()));

        let mut fork = context.parallel_step_context();
        fork.variables
            .insert(variable.clone(), ExecutionValue::Bool(true));

        assert_eq!(
            context.variables.get(&variable),
            Some(&ExecutionValue::Stream(Vec::new()))
        );
    }

    #[test]
    fn isolated_step_contract_rejects_stateful_and_barrier_operations() {
        let mutation = exec::ExecStep {
            op: exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::Drop,
            },
            ..test_support::step(1, Vec::new(), exec::ExecOp::Noop)
        };
        assert!(!is_parallel_isolated_step(&mutation));

        let read = test_support::step(
            1,
            Vec::new(),
            exec::ExecOp::KvRead(exec::KvReadPlan::RangeScan {
                keyspace: exec::ElementKeyspace::NodeProperty,
                start: exec::KvKeyBound::Unbounded,
                end: exec::KvKeyBound::Unbounded,
                limit: properties::PositiveUsize::new(1),
            }),
        );
        assert!(is_parallel_isolated_step(&read));

        let barrier = exec::ExecStep {
            schedule: exec::ExecSchedule::Barrier,
            ..read
        };
        assert!(!is_parallel_isolated_step(&barrier));
    }

    #[test]
    fn missing_stage_step_is_reported_as_invariant_violation() {
        let stage = exec::ExecExecutionStage::Parallel(exec::ExecParallelStage::new(
            ir::AtLeast::<_, 2>::from_pair(id(1), id(2)),
            exec::ExecParallelStagePolicy::for_ready_width(2),
        ));
        let steps = vec![test_support::step(1, Vec::new(), exec::ExecOp::Noop)];

        assert!(matches!(
            StageExecutionMode::for_stage(&stage, &by_id(&steps)),
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("missing step 2")
        ));
    }
}
