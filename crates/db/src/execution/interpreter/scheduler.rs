//! Executable DAG stage scheduling.
//!
//! The planner validates dependency order and labels independent ready stages.
//! The interpreter still owns the runtime safety decision: only context-isolated
//! read/stream operations run concurrently, and their outputs are merged back in
//! stable stage order.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use futures::future;

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(super) async fn execute_steps(
        &mut self,
        steps: &[exec::ExecStep],
        order: exec::ExecExecutionOrder,
        root: exec::ExecStepId,
    ) -> Result<()> {
        self.initialize_step_output_uses(steps, root)?;
        let by_id = steps
            .iter()
            .map(|step| (step.id, step))
            .collect::<BTreeMap<_, _>>();

        for stage in order.stages() {
            self.check_execution_deadline()?;
            match StageExecutionMode::for_stage(stage, &by_id)? {
                StageExecutionMode::Serial => {
                    self.execute_serial_stage(stage, &by_id).await?;
                }
                StageExecutionMode::ParallelIsolated(policy) => {
                    if self.has_active_write_tx() || self.request_read_view_requires_serial_stages()
                    {
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
            self.check_execution_deadline()?;
            let step = step_by_id(by_id, id)?;
            let value = self.execute_step(step).await?;
            self.check_execution_deadline()?;
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
                    let mut context = self.parallel_step_context(step)?;
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

    fn parallel_step_context(&mut self, step: &exec::ExecStep) -> Result<Self> {
        let step_output_uses = step_output_references(step)?;
        let mut step_outputs = ExecutionValueStore::default();
        for (dependency, consumed) in step_output_uses.iter() {
            let remaining = self.step_output_uses.get_mut(dependency).ok_or_else(|| {
                HelixDbError::InvariantViolation(format!(
                    "parallel step {} references unplanned dependency {}",
                    step.id.get(),
                    dependency.get(),
                ))
            })?;
            if remaining.get() < consumed.get() {
                return Err(HelixDbError::InvariantViolation(format!(
                    "parallel step {} over-consumes dependency {}",
                    step.id.get(),
                    dependency.get(),
                )));
            }
            let final_use = remaining.get() == consumed.get();
            if final_use {
                self.step_output_uses.remove(dependency);
            } else {
                *remaining = NonZeroUsize::new(remaining.get() - consumed.get())
                    .expect("a non-final parallel transfer stays non-zero");
            }
            let value = if final_use {
                self.step_outputs.take_slot(dependency)
            } else {
                self.step_outputs.fork_slot(dependency)
            };
            if let Some(value) = value {
                step_outputs.insert_slot(*dependency, value);
            }
        }
        Ok(Self {
            db: self.db,
            tenant_scope: self.tenant_scope,
            params: self.params.shallow_snapshot(),
            variables: self.variables.shallow_snapshot(),
            step_outputs,
            step_output_uses,
            request_read_scope: self.clone_parallel_request_read_scope(),
            request_write_scope: runtime_context::RequestWriteScopeState::Disabled,
            pending_catalog_freshness: runtime_context::PendingCatalogFreshness::Consumed,
            row_mode_max_rows: self.row_mode_max_rows,
            execution_control: self.execution_control,
            #[cfg(test)]
            projection_reads: std::sync::Arc::clone(&self.projection_reads),
            #[cfg(test)]
            deadline_checks_remaining: std::sync::atomic::AtomicUsize::new(usize::MAX),
        })
    }

    fn record_step_output(&mut self, step: &exec::ExecStep, value: ExecutionValue) {
        self.record_step_value(step, value);
    }

    fn initialize_step_output_uses(
        &mut self,
        steps: &[exec::ExecStep],
        root: exec::ExecStepId,
    ) -> Result<()> {
        if !self.step_output_uses.is_empty() {
            return Err(HelixDbError::InvariantViolation(
                "step-output use plan was not isolated from its enclosing plan".to_string(),
            ));
        }
        for step in steps {
            let references = step_output_references(step)?;
            for (dependency, count) in references.iter() {
                add_output_uses(&mut self.step_output_uses, *dependency, *count)?;
            }
        }
        increment_output_use(&mut self.step_output_uses, root)
    }
}

fn step_output_references(step: &exec::ExecStep) -> Result<runtime_context::StepOutputUsePlan> {
    let mut uses = runtime_context::StepOutputUsePlan::default();
    for dependency in &step.dependencies {
        increment_output_use(&mut uses, *dependency)?;
    }
    if let exec::ExecCondition::PreviousStepNotEmpty { dependency } = &step.condition {
        increment_output_use(&mut uses, *dependency)?;
    }
    Ok(uses)
}

fn increment_output_use(
    uses: &mut runtime_context::StepOutputUsePlan,
    step: exec::ExecStepId,
) -> Result<()> {
    add_output_uses(uses, step, NonZeroUsize::MIN)
}

fn add_output_uses(
    uses: &mut runtime_context::StepOutputUsePlan,
    step: exec::ExecStepId,
    added: NonZeroUsize,
) -> Result<()> {
    let count = match uses.get(&step) {
        Some(count) => count.get().checked_add(added.get()).ok_or_else(|| {
            HelixDbError::InvariantViolation(format!(
                "step {} has more consumers than usize can represent",
                step.get()
            ))
        })?,
        None => added.get(),
    };
    uses.insert(
        step,
        NonZeroUsize::new(count).expect("positive consumers remain positive"),
    );
    Ok(())
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
                | exec::ExecOp::VectorSearch { .. }
                | exec::ExecOp::TextSearch { .. }
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

    use super::super::runtime_context;
    use super::super::test_support;
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
    fn output_use_plan_counts_repeated_dependencies_and_conditions() {
        let dependency = id(1);
        let step = exec::ExecStep {
            dependencies: vec![dependency, dependency],
            condition: exec::ExecCondition::PreviousStepNotEmpty { dependency },
            ..test_support::step(2, Vec::new(), exec::ExecOp::Noop)
        };

        let uses = step_output_references(&step).expect("output use plan is valid");

        assert_eq!(uses.iter().count(), 1);
        assert_eq!(uses.get(&dependency).map(|count| count.get()), Some(3));
    }

    #[tokio::test]
    async fn parallel_context_releases_every_repeated_reference_when_condition_skips() {
        let db = test_support::open_db("parallel-context-repeated-references").await;
        let dependency = id(1);
        let step = exec::ExecStep {
            dependencies: vec![dependency, dependency],
            condition: exec::ExecCondition::PreviousStepNotEmpty { dependency },
            ..test_support::step(2, Vec::new(), exec::ExecOp::Noop)
        };
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context
            .step_outputs
            .insert(dependency, ExecutionValue::Stream(Vec::new()));
        context.step_output_uses.insert(
            dependency,
            NonZeroUsize::new(4).expect("three task references and one later reference"),
        );

        let mut parallel = context
            .parallel_step_context(&step)
            .expect("parallel context transfer is valid");

        assert_eq!(
            context
                .step_output_uses
                .get(&dependency)
                .map(|count| count.get()),
            Some(1)
        );
        assert!(context.step_outputs.contains_key(&dependency));
        assert_eq!(
            parallel
                .step_output_uses
                .get(&dependency)
                .map(|count| count.get()),
            Some(3)
        );
        assert_eq!(
            parallel.execute_step(&step).await.unwrap(),
            ExecutionValue::Stream(Vec::new())
        );
        assert!(!parallel.step_output_uses.contains_key(&dependency));
        assert!(!parallel.step_outputs.contains_key(&dependency));
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
        let config = test_support::in_memory_config("parallel-stage-output-order");
        let writer = test_support::open_db_with_config(config.clone()).await;
        test_support::add_user(&writer, "alice").await;
        drop(writer);
        let db = test_support::open_reader_with_config(config).await;
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
                index_context: mutation::MutationIndexContext::for_configured_index_test(
                    std::sync::Arc::clone(db.simhasher_registry()),
                ),
            },
        ));

        context
            .execute_steps(plan.steps(), plan.execution_order(), plan.root())
            .await
            .expect("active transaction executes the parallel stage serially");

        assert_eq!(context.step_outputs.len(), 1);
    }

    #[tokio::test]
    async fn writer_read_transaction_forces_parallel_stage_to_execute_serially() {
        let db = test_support::open_db("parallel-stage-writer-read-transaction").await;
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

        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context
            .enable_request_read_view()
            .await
            .expect("writer read view opens");
        assert!(context.request_read_view_requires_serial_stages());

        context
            .execute_steps(plan.steps(), plan.execution_order(), plan.root())
            .await
            .expect("writer transaction executes the parallel stage serially");

        assert_eq!(context.step_outputs.len(), 1);
        context
            .close_request_read_view()
            .expect("writer read transaction rolls back");
    }

    #[tokio::test]
    async fn parallel_task_context_is_a_snapshot() {
        let db = test_support::open_db("parallel-context-snapshot").await;
        let variable = named("x");
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context
            .variables
            .insert(variable.clone(), ExecutionValue::Stream(Vec::new()));

        let step = test_support::step(1, Vec::new(), exec::ExecOp::Noop);
        let mut fork = context
            .parallel_step_context(&step)
            .expect("parallel snapshot is valid");
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
