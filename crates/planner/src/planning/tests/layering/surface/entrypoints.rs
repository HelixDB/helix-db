use super::*;

#[test]
fn plan_entrypoints_preserve_read_and_write_kind() {
    let read = BatchQuery::Read(
        read_batch()
            .var_as("users", g().n(NodeRef::all()))
            .returning(["users"]),
    );
    let read_plan = crate::planning::plan(&read, &PlannerContext::default()).unwrap();
    assert_eq!(read_plan.kind(), PlanKind::Read);
    assert_eq!(
        read_plan.returns(),
        &ReturnPlan::Variables(
            ReturnVariables::new(AtLeast::<_, 1>::from_one_and_rest(
                NonEmptyString::new("users").unwrap(),
                Vec::new()
            ))
            .unwrap()
        )
    );
    assert_eq!(read_plan.steps().len(), 1);
    assert!(matches!(
        first_kv_read(&read_plan),
        KvReadPlan::RangeScan { keyspace, .. }
            if *keyspace == ElementKeyspace::NodeProperty
    ));

    let write = BatchQuery::Write(
        write_batch()
            .var_as("created", g().add_n("User", vec![("name", "alice")]))
            .returning(["created"]),
    );
    let write_plan = crate::planning::plan(&write, &PlannerContext::default()).unwrap();
    assert_eq!(write_plan.kind(), PlanKind::Write);
    assert_eq!(
        write_plan.returns(),
        &ReturnPlan::Variables(
            ReturnVariables::new(AtLeast::<_, 1>::from_one_and_rest(
                NonEmptyString::new("created").unwrap(),
                Vec::new()
            ))
            .unwrap()
        )
    );
    assert_eq!(write_plan.steps().len(), 1);
    assert!(matches!(
        &write_plan.steps()[0].op,
        ExecOp::Mutation {
            plan: ExecMutationPlan::AddNodeSource { .. },
        }
    ));
}

#[test]
fn executable_entrypoint_makes_batch_order_and_returns_explicit() {
    let batch = read_batch()
        .var_as("seed", g().n(NodeRef::all()))
        .var_as_if("users", BatchCondition::PrevNotEmpty, g().n(NodeRef::all()))
        .returning(["users"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Read);
    assert_eq!(
        plan.returns(),
        &ReturnPlan::Variables(
            ReturnVariables::new(AtLeast::<_, 1>::from_one(
                NonEmptyString::new("users").unwrap()
            ))
            .unwrap()
        )
    );
    assert_eq!(plan.steps().len(), 2);
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[1].condition,
        crate::exec::ExecCondition::PreviousStepNotEmpty { dependency } if dependency.get() == 1
    ));
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().memo_exprs, 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert_eq!(plan.metrics().selected_cost.range_seeks, 2);
}

#[test]
fn executable_entrypoint_applies_cascades_guardrails_to_run_batch_request() {
    fn two_run_batch(first: AstNode, second: AstNode) -> ReadBatch {
        ReadBatch {
            entries: vec![
                BatchEntry::Query(Box::new(NamedQuery {
                    name: None,
                    root: first,
                    condition: None,
                })),
                BatchEntry::Query(Box::new(NamedQuery {
                    name: None,
                    root: second,
                    condition: None,
                })),
            ],
            returns: Vec::new(),
        }
    }

    let ctx = PlannerContext {
        optimizer_limits: crate::context::OptimizerLimits {
            memo_groups: crate::properties::PositiveUsize::new(1).unwrap(),
            ..crate::context::OptimizerLimits::default()
        },
        ..PlannerContext::default()
    };
    let duplicate =
        crate::planning::plan_read_batch(&two_run_batch(nodes_root(), nodes_root()), &ctx).unwrap();
    assert!(duplicate.metrics().memo_groups >= 1);
    assert_eq!(duplicate.metrics().selected_cost.range_seeks, 2);

    let distinct = crate::planning::plan_read_batch(
        &two_run_batch(
            nodes_root(),
            AstNode::Edges {
                reference: EdgeRef::Ids(Vec::new()),
            },
        ),
        &ctx,
    )
    .unwrap_err();
    assert!(matches!(
        distinct,
        PlannerError::UnsupportedCascadesPlan { .. }
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_native_access() {
    let batch = read_batch()
        .var_as("users", g().n(NodeRef::all()))
        .returning(["users"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 1);
    assert_eq!(plan.trace().events.len(), 4);
    assert_eq!(plan.trace().events[0].path.as_ref(), "entry[0].root");
    assert_eq!(plan.trace().events[0].pass, TracePass::NativeHandoff);
    assert_eq!(
        plan.trace().events[0].decision,
        TraceDecision::NativeQueryRoot
    );
    assert_eq!(
        plan.trace().events[0].reason,
        TraceReason::NativeAstRoot(NonEmptyString::new("nodes").unwrap())
    );
    assert_eq!(
        plan.trace().events[1].path.as_ref(),
        "selected.entry[0].root"
    );
    assert_eq!(plan.trace().events[1].pass, TracePass::SelectedHandoff);
    assert_eq!(
        plan.trace().events[1].decision,
        TraceDecision::SelectedRunRoot
    );
    assert_eq!(
        plan.trace().events[1].reason,
        TraceReason::SelectedRootFamily(NonEmptyString::new("alternative").unwrap())
    );
    assert_eq!(
        plan.trace().events[2].path.as_ref(),
        "selected.entry[0].root.rule"
    );
    assert_eq!(plan.trace().events[2].pass, TracePass::SelectedHandoff);
    assert_eq!(
        plan.trace().events[2].decision,
        TraceDecision::SelectedOptimizerRule
    );
    assert_eq!(
        plan.trace().events[2].reason,
        TraceReason::SelectedOptimizerRule(NonEmptyString::new("seed_access_path").unwrap())
    );
    assert_eq!(
        plan.trace().events[3].path.as_ref(),
        "selected.entry[0].root.memo"
    );
    assert_eq!(plan.trace().events[3].pass, TracePass::SelectedHandoff);
    assert_eq!(
        plan.trace().events[3].decision,
        TraceDecision::SelectedMemoExpression
    );
    assert_eq!(
        plan.trace().events[3].reason,
        TraceReason::SelectedMemoExpression(
            NonEmptyString::new("group=1 expr=1 alternative=1 children=[]").unwrap()
        )
    );
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "users"
    ));
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_source_mutation() {
    let batch = write_batch()
        .var_as("created", g().add_n("User", vec![("name", "alice")]))
        .returning(["created"]);

    let plan = crate::planning::plan_write_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Write);
    assert_eq!(plan.steps().len(), 1);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert_eq!(plan.steps()[0].schedule, crate::exec::ExecSchedule::Barrier);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::Mutation {
            plan: crate::exec::ExecMutationPlan::AddNodeSource { label, .. }
        } if label.as_ref() == "User"
    ));
    assert!(matches!(
        &plan.steps()[0].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "created"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_index_ddl() {
    let batch = helix_ast::batch::WriteBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some("ddl".to_string()),
            root: AstNode::CreateIndex {
                spec: IndexSpec::node_equality("User", "email"),
                if_not_exists: false,
            },
            condition: None,
        }))],
        returns: vec!["ddl".to_string()],
    };

    let plan = crate::planning::plan_write_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Write);
    assert_eq!(plan.steps().len(), 1);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert_eq!(plan.steps()[0].schedule, crate::exec::ExecSchedule::Barrier);
    assert_eq!(
        plan.steps()[0].delivered.cardinality,
        crate::properties::CardinalityBounds::exact(1)
    );
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::IndexDdl {
            plan: IndexDdlPlan::Create {
                spec: IndexDdlCreateSpec::NodeEquality { key, uniqueness },
                mode: IndexCreateMode::ErrorIfExists,
            }
        } if key.label.as_ref() == "User"
            && key.property.as_ref() == "email"
            && matches!(uniqueness, IndexUniqueness::NonUnique)
    ));
    assert!(matches!(
        &plan.steps()[0].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "ddl"
    ));
}
