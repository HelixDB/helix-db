//! Executable operation dispatch contracts.
//!
//! The scheduler decides when a step is ready. This module owns the final
//! mapping from one validated executable step to the interpreter contract that
//! implements its operation, including the merge special case that requires
//! dependency provenance.

use super::*;

impl<'db> ExecutionContext<'db> {
    /// Nested plans recurse through each step, so its state is boxed here and
    /// callers hold only a pointer across the await.
    pub(in crate::execution::interpreter) fn execute_step<'a>(
        &'a mut self,
        step: &'a exec::ExecStep,
    ) -> futures::future::BoxFuture<'a, Result<ExecutionValue>> {
        Box::pin(async move {
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
            let started = std::time::Instant::now();
            let value = self.execute_op(&step.op, input).await?;
            trace_step(&step.op, &value, started);
            self.check_execution_deadline()?;
            Ok(value)
        })
    }

    /// Run one operation. Nested plans recurse through here, so each
    /// operation kind polls in its own boxed future: a level of recursion
    /// costs this small frame and one kind's frame, never the temporaries of
    /// every operation kind.
    pub(in crate::execution::interpreter) fn execute_op<'a>(
        &'a mut self,
        op: &'a exec::ExecOp,
        input: ExecutionValue,
    ) -> futures::future::BoxFuture<'a, Result<ExecutionValue>> {
        Box::pin(async move {
            // A failed operation may have staged writes it never reported to
            // the membership cache, or, when a deadline dropped a body, left
            // `ForEach` bindings mid-frame.
            let value = self
                .operation(op, input)
                .await
                .inspect_err(|_| self.prepared_memberships.clear())?;
            self.enforce_row_mode_cap(row_mode::op_name(op), &value)?;
            Ok(value)
        })
    }

    /// Never inlined: building an operation's future stages it on the stack
    /// before boxing, and inlined into its caller's poll that stack space
    /// would stay reserved at every level of a nested plan.
    #[inline(never)]
    fn operation<'a>(
        &'a mut self,
        op: &'a exec::ExecOp,
        input: ExecutionValue,
    ) -> futures::future::BoxFuture<'a, Result<ExecutionValue>> {
        let execution_control = self.execution_control.clone();
        match op {
            exec::ExecOp::Access { plan } => {
                Box::pin(async move { execution_control.run(self.execute_access(plan)).await })
            }
            exec::ExecOp::Count { plan } => {
                Box::pin(
                    async move { execution_control.run(self.execute_count(input, plan)).await },
                )
            }
            exec::ExecOp::KvRead(read) => {
                Box::pin(async move { execution_control.run(self.execute_kv_read(read)).await })
            }
            exec::ExecOp::Expand { plan } => {
                Box::pin(async move { execution_control.run(self.expand(input, plan)).await })
            }
            exec::ExecOp::VectorSearch { plan } => Box::pin(async move {
                execution_control
                    .run(self.restricted_vector_search(input, plan))
                    .await
            }),
            exec::ExecOp::TextSearch { plan } => Box::pin(async move {
                execution_control
                    .run(self.restricted_text_search(input, plan))
                    .await
            }),
            exec::ExecOp::Filter { predicate } => {
                Box::pin(async move { execution_control.run(self.filter(input, predicate)).await })
            }
            exec::ExecOp::IndexMembership { plan } => Box::pin(async move {
                execution_control
                    .run(self.index_membership(input, plan))
                    .await
            }),
            exec::ExecOp::Limit { count } => Box::pin(std::future::ready(self.limit(input, count))),
            exec::ExecOp::Skip { count } => Box::pin(std::future::ready(self.skip(input, count))),
            exec::ExecOp::Range { range } => Box::pin(std::future::ready(self.range(input, range))),
            exec::ExecOp::Distinct => Box::pin(std::future::ready(self.distinct(input))),
            exec::ExecOp::Order { plan } => {
                Box::pin(async move { execution_control.run(self.order(input, plan)).await })
            }
            exec::ExecOp::Project { projection } => {
                Box::pin(
                    async move { execution_control.run(self.project(input, projection)).await },
                )
            }
            exec::ExecOp::Aggregate { aggregate } => Box::pin(async move {
                execution_control
                    .run(self.aggregate(input, aggregate))
                    .await
            }),
            exec::ExecOp::Variable { op } => Box::pin(std::future::ready(self.variable(input, op))),
            exec::ExecOp::Branch { plan } => Box::pin(async move {
                execution_control
                    .run(self.execute_branch(input, plan))
                    .await
            }),
            exec::ExecOp::Repeat { plan } => Box::pin(async move {
                execution_control
                    .run(self.execute_repeat(input, plan))
                    .await
            }),
            exec::ExecOp::ShortestPath { plan } => Box::pin(async move {
                execution_control
                    .run(self.execute_shortest_path(plan))
                    .await
            }),
            exec::ExecOp::Merge { .. } => {
                Box::pin(std::future::ready(Err(HelixDbError::InvariantViolation(
                    "merge operations must be executed with dependency provenance".to_string(),
                ))))
            }
            // Mutation futures contain large transaction/index-maintenance state.
            exec::ExecOp::Mutation { plan } => Box::pin(self.execute_mutation(input, plan)),
            exec::ExecOp::IndexDdl { plan } => Box::pin(async move {
                // DDL changes which indexes serve a set and may start a newer
                // request snapshot.
                self.prepared_memberships.clear();
                self.pending_sets = std::sync::Arc::default();
                if !plan.requires_isolated_catalog_transaction() {
                    return self.execute_index_ddl(input, plan).await;
                }
                let resume_request_scope = self.has_request_write_scope();
                self.check_execution_deadline()?;
                self.commit_request_write_scope().await?;
                let result = self.execute_index_ddl(input, plan).await;
                if resume_request_scope && result.is_ok() {
                    self.enable_request_write_scope().await?;
                }
                result
            }),
            exec::ExecOp::Noop | exec::ExecOp::Barrier { .. } => {
                Box::pin(std::future::ready(Ok(input)))
            }
            exec::ExecOp::Reserved { op } => Box::pin(self.reserved(input, op)),
            exec::ExecOp::ForEach { param, body } => Box::pin(self.execute_foreach(param, body)),
        }
    }
}

/// Kept out of a step's async body: unoptimized builds would otherwise hold the
/// event's temporaries on the stack at every level of a nested plan.
fn trace_step(op: &exec::ExecOp, value: &ExecutionValue, started: std::time::Instant) {
    tracing::debug!(
        target: "helix::query::step",
        op = row_mode::op_name(op),
        rows = match value {
            ExecutionValue::Stream(rows) => rows.len(),
            ExecutionValue::FoldedStream(_)
            | ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::Scalars(_)
            | ExecutionValue::IndexDdlReceipt(_)
            | ExecutionValue::IndexOperationStatus(_) => 0,
        },
        elapsed_us = started.elapsed().as_micros() as u64,
        "query step"
    );
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
    async fn get_operation_does_not_commit_an_open_graph_write() {
        let db = test_support::open_db("dispatch-get-op-does-not-commit").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        let create = context
            .execute_op(
                &exec::ExecOp::IndexDdl {
                    plan: ir::IndexDdlPlan::Create {
                        spec: ir::IndexDdlCreateSpec::NodeEquality {
                            key: helix_planner::catalog::ScopedPropertyKey::try_new(
                                "User", "email",
                            )
                            .unwrap(),
                            uniqueness: helix_planner::catalog::IndexUniqueness::NonUnique,
                        },
                        mode: ir::IndexCreateMode::ErrorIfExists,
                    },
                },
                ExecutionValue::Stream(Vec::new()),
            )
            .await
            .expect("create enqueues a durable operation");
        let ExecutionValue::IndexDdlReceipt(crate::index_lifecycle::IndexDdlReceipt::Accepted {
            operation_id,
            ..
        }) = create
        else {
            panic!("new index create must be accepted");
        };
        let operation_id = ir::IndexOperationId::try_new(operation_id.as_uuid().to_string())
            .expect("receipt UUID is a canonical lowercase operation ID");

        context.enable_request_write_scope().await.unwrap();
        let added = context
            .execute_op(
                &exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: test_support::name("User"),
                        properties: test_support::assignments(vec![(
                            "email",
                            helix_ast::value::PropertyValue::from("uncommitted@example.com"),
                        )]),
                    },
                },
                ExecutionValue::Stream(Vec::new()),
            )
            .await
            .expect("graph write stays on the request transaction");
        let ExecutionValue::Stream(rows) = added else {
            panic!("node write should return a stream");
        };
        let Some(ExecutionRow {
            current: Some(ElementRef::Node(id)),
            ..
        }) = rows.first()
        else {
            panic!("node write should return a node row");
        };
        let node_key = crate::encoding::keys::DataKey::Data {
            scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
            kind: crate::encoding::keys::DataKeyKind::NodeProperty(
                crate::encoding::keys::NodePropertyKey::new(*id),
            ),
        }
        .to_bytes();
        assert!(
            db.inner_db().get(&node_key).await.unwrap().is_none(),
            "uncommitted graph write must not be visible on a snapshot"
        );

        let status = context
            .execute_op(
                &exec::ExecOp::IndexDdl {
                    plan: ir::IndexDdlPlan::GetOperation { operation_id },
                },
                ExecutionValue::Stream(Vec::new()),
            )
            .await
            .expect("status read stays on the open graph write");
        assert!(matches!(status, ExecutionValue::IndexOperationStatus(_)));
        assert!(
            context.has_request_write_scope(),
            "status read must not disable the request write"
        );
        assert!(
            db.inner_db().get(&node_key).await.unwrap().is_none(),
            "successful GetOperation must not publish the open graph write"
        );

        let missing = context
            .execute_op(
                &exec::ExecOp::IndexDdl {
                    plan: ir::IndexDdlPlan::GetOperation {
                        operation_id: ir::IndexOperationId::try_new(
                            "07070707-0707-0707-0707-070707070707",
                        )
                        .unwrap(),
                    },
                },
                ExecutionValue::Stream(Vec::new()),
            )
            .await
            .expect_err("unknown operation ID is a status miss, not a commit");
        assert!(matches!(
            missing,
            HelixDbError::IndexOperationNotFound { .. }
        ));
        assert!(context.has_request_write_scope());
        assert!(db.inner_db().get(&node_key).await.unwrap().is_none());

        context.commit_request_write_scope().await.unwrap();
        assert!(db.inner_db().get(&node_key).await.unwrap().is_some());
        db.close().await.expect("writer closes");
    }

    /// Unique maintenance is deferred until a step observes it, so two
    /// conflicting writes in one request fail the observing step with the
    /// violation before that step runs.
    #[tokio::test]
    async fn step_flush_surfaces_a_pending_unique_conflict() {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("dispatch-flush-unique-conflict")
                .with_unique_equality_index("User", "email"),
        )
        .await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        let add = exec::ExecOp::Mutation {
            plan: exec::ExecMutationPlan::AddNodeSource {
                label: test_support::name("User"),
                properties: test_support::assignments(vec![(
                    "email",
                    helix_ast::value::PropertyValue::from("taken@example.com"),
                )]),
            },
        };
        for _ in 0..2 {
            context
                .execute_op(&add, ExecutionValue::Stream(Vec::new()))
                .await
                .expect("conflicting writes stage without flushing");
        }

        assert!(matches!(
            context
                .execute_step(&test_support::step(
                    1,
                    Vec::new(),
                    exec::ExecOp::Barrier {
                        name: test_support::name("unique visibility"),
                    },
                ))
                .await,
            Err(HelixDbError::UniqueConstraintViolation { .. })
        ));
        context.abort_request_write_scope();
        db.close().await.expect("writer closes");
    }

    /// Isolated DDL commits the open write before changing the catalog, then
    /// reopens it. A deadline that expires before the reopen fails the
    /// request without a write scope, while the DDL stays durably accepted.
    #[tokio::test]
    async fn expired_deadline_after_isolated_ddl_does_not_reopen_the_write() {
        let db = test_support::open_db("dispatch-ddl-reopen-deadline").await;
        let create = exec::ExecOp::IndexDdl {
            plan: ir::IndexDdlPlan::Create {
                spec: ir::IndexDdlCreateSpec::NodeEquality {
                    key: helix_planner::catalog::ScopedPropertyKey::try_new("User", "email")
                        .unwrap(),
                    uniqueness: helix_planner::catalog::IndexUniqueness::NonUnique,
                },
                mode: ir::IndexCreateMode::ErrorIfExists,
            },
        };
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        // Only the check before the DDL commits passes.
        context.fail_deadline_after(1);

        assert!(matches!(
            context
                .execute_op(&create, ExecutionValue::Stream(Vec::new()))
                .await,
            Err(HelixDbError::QueryDeadlineExceeded)
        ));
        assert!(!context.has_request_write_scope());
        assert!(matches!(
            ExecutionContext::new(&db, context::ParamBindings::default())
                .execute_op(&create, ExecutionValue::Stream(Vec::new()))
                .await,
            Err(HelixDbError::IndexAlreadyExists(_))
        ));
        db.close().await.expect("writer closes");
    }
}
