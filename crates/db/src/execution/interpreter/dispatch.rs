//! Executable operation dispatch contracts.
//!
//! The scheduler decides when a step is ready. This module owns the final
//! mapping from one validated executable step to the interpreter contract that
//! implements its operation, including the merge special case that requires
//! dependency provenance.

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn execute_step(
        &mut self,
        step: &exec::ExecStep,
    ) -> Result<ExecutionValue> {
        self.check_execution_deadline()?;
        let allowed = self.condition_allows(&step.condition)?;
        self.release_condition_reference(&step.condition);
        if !allowed {
            self.release_dependency_references(&step.dependencies);
            return Ok(ExecutionValue::Stream(Vec::new()));
        }
        self.flush_required_mutations(mutation::visibility::required_for(&step.op))
            .await?;
        if let exec::ExecOp::Merge { mode } = &step.op {
            let dependencies = self.dependency_values(&step.dependencies)?;
            let value = self.merge_values(dependencies, *mode)?;
            self.enforce_row_mode_cap(row_mode::op_name(&step.op), &value)?;
            return Ok(value);
        }
        let input = self.dependency_input(&step.dependencies)?;
        let value = self.execute_op(&step.op, input).await?;
        self.check_execution_deadline()?;
        Ok(value)
    }

    async fn execute_op(
        &mut self,
        op: &exec::ExecOp,
        input: ExecutionValue,
    ) -> Result<ExecutionValue> {
        let execution_control = self.execution_control.clone();
        let value = match op {
            exec::ExecOp::Access { plan } => execution_control.run(self.execute_access(plan)).await,
            exec::ExecOp::Count { plan } => {
                execution_control.run(self.execute_count(input, plan)).await
            }
            exec::ExecOp::KvRead(read) => execution_control.run(self.execute_kv_read(read)).await,
            exec::ExecOp::Expand { plan } => execution_control.run(self.expand(input, plan)).await,
            exec::ExecOp::VectorSearch { plan } => {
                execution_control
                    .run(self.restricted_vector_search(input, plan))
                    .await
            }
            exec::ExecOp::TextSearch { plan } => {
                execution_control
                    .run(self.restricted_text_search(input, plan))
                    .await
            }
            exec::ExecOp::Filter { predicate } => {
                execution_control.run(self.filter(input, predicate)).await
            }
            exec::ExecOp::Limit { count } => self.limit(input, count),
            exec::ExecOp::Skip { count } => self.skip(input, count),
            exec::ExecOp::Range { range } => self.range(input, range),
            exec::ExecOp::Distinct => self.distinct(input),
            exec::ExecOp::Order { plan } => execution_control.run(self.order(input, plan)).await,
            exec::ExecOp::Project { projection } => {
                execution_control.run(self.project(input, projection)).await
            }
            exec::ExecOp::Aggregate { aggregate } => {
                execution_control
                    .run(self.aggregate(input, aggregate))
                    .await
            }
            exec::ExecOp::Variable { op } => self.variable(input, op),
            exec::ExecOp::Branch { plan } => {
                execution_control
                    .run(self.execute_branch(input, plan))
                    .await
            }
            exec::ExecOp::Repeat { plan } => {
                execution_control
                    .run(self.execute_repeat(input, plan))
                    .await
            }
            exec::ExecOp::ShortestPath { plan } => {
                execution_control
                    .run(self.execute_shortest_path(plan))
                    .await
            }
            exec::ExecOp::Merge { .. } => Err(HelixDbError::InvariantViolation(
                "merge operations must be executed with dependency provenance".to_string(),
            )),
            exec::ExecOp::Mutation { plan } => self.execute_mutation(input, plan).await,
            exec::ExecOp::IndexDdl { plan } => {
                let resume_request_scope = self.has_request_write_scope();
                self.check_execution_deadline()?;
                self.commit_request_write_scope().await?;
                let result = self.execute_index_ddl(input, plan).await;
                if resume_request_scope && result.is_ok() {
                    self.enable_request_write_scope().await?;
                }
                result
            }
            exec::ExecOp::Noop | exec::ExecOp::Barrier { .. } => Ok(input),
            exec::ExecOp::Reserved { op } => self.reserved(input, op).await,
            exec::ExecOp::ForEach { param, body } => self.execute_foreach(param, body).await,
        }?;
        self.enforce_row_mode_cap(row_mode::op_name(op), &value)?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use helix_planner::context;

    use super::test_support;
    use super::*;
    use crate::execution::interpreter::runtime_context::RequestWriteScopeState;

    fn step_id(id: usize) -> exec::ExecStepId {
        exec::ExecStepId::new(id).expect("positive test step id")
    }

    fn row(id: u64) -> ExecutionRow {
        ExecutionRow::current(ElementRef::Node(id))
    }

    #[tokio::test]
    async fn execute_step_skips_conditioned_steps_without_reading_missing_variables() {
        let db = test_support::open_db("dispatch-skip-condition").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let empty = test_support::name("empty");
        ctx.variables
            .insert(empty.clone(), ExecutionValue::Stream(Vec::new()));
        let step = exec::ExecStep {
            condition: exec::ExecCondition::Variable(ir::BatchVariableConditionPlan::VarNotEmpty(
                empty,
            )),
            op: exec::ExecOp::Variable {
                op: exec::ExecVariableOp::SourceInject {
                    variable: test_support::name("would_error_if_executed"),
                },
            },
            ..test_support::step(1, Vec::new(), exec::ExecOp::Noop)
        };

        assert_eq!(
            ctx.execute_step(&step).await.unwrap(),
            ExecutionValue::Stream(Vec::new())
        );
    }

    #[tokio::test]
    async fn execute_step_merges_dependency_values_with_dependency_provenance() {
        let db = test_support::open_db("dispatch-merge-provenance").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.step_outputs
            .insert(step_id(1), ExecutionValue::Stream(vec![row(1), row(2)]));
        ctx.step_outputs
            .insert(step_id(2), ExecutionValue::Stream(vec![row(2), row(3)]));
        let step = test_support::step(
            3,
            vec![step_id(1), step_id(2)],
            exec::ExecOp::Merge {
                mode: exec::ExecMergeMode::Union,
            },
        );

        assert_eq!(
            ctx.execute_step(&step).await.unwrap(),
            ExecutionValue::Stream(vec![row(1), row(2), row(3)])
        );
    }

    #[tokio::test]
    async fn execute_op_preserves_input_for_noop_and_barrier() {
        let db = test_support::open_db("dispatch-pass-through").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let input = ExecutionValue::Stream(vec![row(7)]);

        assert_eq!(
            ctx.execute_op(&exec::ExecOp::Noop, input.clone())
                .await
                .unwrap(),
            input
        );
        assert_eq!(
            ctx.execute_op(
                &exec::ExecOp::Barrier {
                    name: test_support::name("optimization")
                },
                ExecutionValue::Count(3)
            )
            .await
            .unwrap(),
            ExecutionValue::Count(3)
        );
    }

    #[tokio::test]
    async fn execute_op_routes_simple_stream_contracts() {
        let db = test_support::open_db("dispatch-simple-stream").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let input = ExecutionValue::Stream(vec![row(1), row(2), row(3), row(2)]);

        assert_eq!(
            ctx.execute_op(
                &exec::ExecOp::Filter {
                    predicate: ir::PredicatePlan::new(Predicate::eq("name", "alice"))
                        .expect("valid predicate"),
                },
                ExecutionValue::Stream(vec![row(alice), row(bob)]),
            )
            .await
            .unwrap(),
            ExecutionValue::Stream(vec![row(alice)])
        );

        assert_eq!(
            ctx.execute_op(
                &exec::ExecOp::Limit {
                    count: ir::StreamBoundPlan::Literal(2)
                },
                input.clone()
            )
            .await
            .unwrap(),
            ExecutionValue::Stream(vec![row(1), row(2)])
        );
        assert_eq!(
            ctx.execute_op(
                &exec::ExecOp::Skip {
                    count: ir::StreamBoundPlan::Literal(1)
                },
                input.clone()
            )
            .await
            .unwrap(),
            ExecutionValue::Stream(vec![row(2), row(3), row(2)])
        );
        assert_eq!(
            ctx.execute_op(
                &exec::ExecOp::Range {
                    range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(1, 3).unwrap())
                },
                input.clone()
            )
            .await
            .unwrap(),
            ExecutionValue::Stream(vec![row(2), row(3)])
        );
        assert_eq!(
            ctx.execute_op(&exec::ExecOp::Distinct, input)
                .await
                .unwrap(),
            ExecutionValue::Stream(vec![row(1), row(2), row(3)])
        );
    }

    #[tokio::test]
    async fn execute_op_routes_variable_dispatch() {
        let db = test_support::open_db("dispatch-variable").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let input = ExecutionValue::Stream(vec![row(4)]);
        let variable = test_support::name("saved");

        assert_eq!(
            ctx.execute_op(
                &exec::ExecOp::Variable {
                    op: exec::ExecVariableOp::Stream(ir::StreamVariableOp::Store(variable.clone()))
                },
                input.clone()
            )
            .await
            .unwrap(),
            input
        );
        assert_eq!(
            ctx.execute_op(
                &exec::ExecOp::Variable {
                    op: exec::ExecVariableOp::SourceInject { variable }
                },
                ExecutionValue::Stream(Vec::new())
            )
            .await
            .unwrap(),
            ExecutionValue::Stream(vec![row(4)])
        );
    }

    #[tokio::test]
    async fn execute_op_rejects_direct_merge_without_dependency_provenance() {
        let db = test_support::open_db("dispatch-direct-merge").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        let err = ctx
            .execute_op(
                &exec::ExecOp::Merge {
                    mode: exec::ExecMergeMode::Concat,
                },
                ExecutionValue::Stream(Vec::new()),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, HelixDbError::InvariantViolation(message) if message.contains("dependency provenance"))
        );
    }

    #[tokio::test]
    async fn every_fully_ready_index_family_enqueues_create_and_drop() {
        let db = test_support::open_db("dispatch-index-ddl-v2-ready").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        let key = || helix_planner::catalog::ScopedPropertyKey::try_new("User", "indexed").unwrap();
        let plans = [
            ir::IndexDdlPlan::Create {
                spec: ir::IndexDdlCreateSpec::NodeEquality {
                    key: key(),
                    uniqueness: helix_planner::catalog::IndexUniqueness::NonUnique,
                },
                mode: ir::IndexCreateMode::ErrorIfExists,
            },
            ir::IndexDdlPlan::Drop {
                spec: ir::IndexDdlDropSpec::NodeEquality {
                    key: key(),
                    uniqueness: helix_planner::catalog::IndexUniqueness::NonUnique,
                },
            },
            ir::IndexDdlPlan::Create {
                spec: ir::IndexDdlCreateSpec::NodeVector {
                    key: key(),
                    dimension: ir::VectorIndexDimension::new(3).unwrap(),
                    metric: ir::VectorIndexMetric::Cosine,
                    scope: helix_planner::catalog::SearchIndexScope::Unscoped,
                },
                mode: ir::IndexCreateMode::ErrorIfExists,
            },
            ir::IndexDdlPlan::Drop {
                spec: ir::IndexDdlDropSpec::NodeVector { key: key() },
            },
            ir::IndexDdlPlan::Create {
                spec: ir::IndexDdlCreateSpec::NodeText {
                    key: key(),
                    scope: helix_planner::catalog::SearchIndexScope::Unscoped,
                },
                mode: ir::IndexCreateMode::ErrorIfExists,
            },
            ir::IndexDdlPlan::Drop {
                spec: ir::IndexDdlDropSpec::NodeText { key: key() },
            },
        ];

        for plan in plans {
            let value = context
                .execute_op(
                    &exec::ExecOp::IndexDdl { plan },
                    ExecutionValue::Stream(Vec::new()),
                )
                .await
                .expect("fully ready family DDL returns a durable receipt");
            assert!(matches!(value, ExecutionValue::IndexDdlReceipt(_)));
        }
        db.close().await.expect("writer closes");
    }

    #[tokio::test]
    async fn object_store_index_ddl_needs_no_external_runtime_authority() {
        let db = test_support::open_db_with_object_store(
            "dispatch-index-ddl-shared-unavailable",
            std::sync::Arc::new(slatedb::object_store::memory::InMemory::new()),
        )
        .await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        let key = || helix_planner::catalog::ScopedPropertyKey::try_new("User", "indexed").unwrap();
        for plan in [
            ir::IndexDdlPlan::Create {
                spec: ir::IndexDdlCreateSpec::NodeEquality {
                    key: key(),
                    uniqueness: helix_planner::catalog::IndexUniqueness::NonUnique,
                },
                mode: ir::IndexCreateMode::ErrorIfExists,
            },
            ir::IndexDdlPlan::Create {
                spec: ir::IndexDdlCreateSpec::NodeVector {
                    key: key(),
                    dimension: ir::VectorIndexDimension::new(3).unwrap(),
                    metric: ir::VectorIndexMetric::Cosine,
                    scope: helix_planner::catalog::SearchIndexScope::Unscoped,
                },
                mode: ir::IndexCreateMode::ErrorIfExists,
            },
        ] {
            let value = context
                .execute_op(
                    &exec::ExecOp::IndexDdl { plan },
                    ExecutionValue::Stream(Vec::new()),
                )
                .await
                .expect("equality and vector DDL need no external runtime");
            assert!(matches!(value, ExecutionValue::IndexDdlReceipt(_)));
        }

        let value = context
            .execute_op(
                &exec::ExecOp::IndexDdl {
                    plan: ir::IndexDdlPlan::Create {
                        spec: ir::IndexDdlCreateSpec::NodeText {
                            key: key(),
                            scope: helix_planner::catalog::SearchIndexScope::Unscoped,
                        },
                        mode: ir::IndexCreateMode::ErrorIfExists,
                    },
                },
                ExecutionValue::Stream(Vec::new()),
            )
            .await
            .expect("text DDL uses the opened object store directly");
        assert!(matches!(value, ExecutionValue::IndexDdlReceipt(_)));
        db.close().await.expect("writer closes");
    }

    #[tokio::test]
    async fn non_mutation_step_flushes_active_vectors_and_retains_session_for_more_mutations() {
        let db = test_support::open_db("dispatch-active-vector-flush-barrier").await;
        let generation = crate::search::vector::ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Cosine,
        >(
            crate::search::vector::VectorGenerationIdentity::try_new(
                crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
                901,
                "dispatch-active-vector-flush-barrier".to_string(),
                902,
                std::num::NonZeroU64::MIN,
                1,
                crate::index_lifecycle::IndexElementKind::Node,
                crate::search::vector::VectorDimension::try_new(4).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        let RequestWriteScopeState::Active(active) = &mut context.request_write_scope else {
            panic!("write request owns its transaction and index context");
        };
        active
            .index_context
            .stage_active_vector_for_test(&active.txn, &generation, 1, &[1.0, 0.1, 0.2, 0.3], true)
            .await
            .unwrap();
        active
            .index_context
            .stage_active_vector_for_test(&active.txn, &generation, 2, &[0.2, 1.0, 0.3, 0.4], false)
            .await
            .unwrap();

        context
            .execute_step(&test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Barrier {
                    name: test_support::name("vector visibility"),
                },
            ))
            .await
            .unwrap();
        let RequestWriteScopeState::Active(active) = &mut context.request_write_scope else {
            panic!("flush barrier retains the write transaction");
        };
        active
            .index_context
            .stage_active_vector_for_test(&active.txn, &generation, 3, &[0.3, 0.4, 1.0, 0.5], false)
            .await
            .unwrap();

        context
            .execute_step(&test_support::step(
                2,
                Vec::new(),
                exec::ExecOp::Barrier {
                    name: test_support::name("vector visibility"),
                },
            ))
            .await
            .unwrap();
        let RequestWriteScopeState::Active(active) = &context.request_write_scope else {
            panic!("second flush barrier retains the write transaction");
        };
        let results = crate::search::vector::VectorIndex::<
            crate::search::vector::distance::Cosine,
        >::from_generation(&generation)
        .search(
            &active.txn,
            &[0.3, 0.4, 1.0, 0.5],
            &crate::search::vector::SearchParams::new(3).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].entity_id(), 3);

        context.commit_request_write_scope().await.unwrap();
        let raw = db.inner_db();
        let persisted = crate::search::vector::VectorIndex::<
            crate::search::vector::distance::Cosine,
        >::from_generation(&generation)
        .search(
            raw.as_ref(),
            &[0.3, 0.4, 1.0, 0.5],
            &crate::search::vector::SearchParams::new(3).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(persisted.len(), 3);
        assert_eq!(persisted[0].entity_id(), 3);
        db.close().await.unwrap();
    }
}
