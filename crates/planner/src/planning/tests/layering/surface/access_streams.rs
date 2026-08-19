use super::*;

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_stream_project() {
    let batch = read_batch()
        .var_as("count", g().n(NodeRef::all()).count())
        .returning(["count"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 1);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::Count { plan }
            if matches!(plan.as_ref(), crate::exec::ExecCountPlan::NodeFullScan { .. })
    ));
    assert!(plan.steps()[0].dependencies.is_empty());
    assert!(matches!(
        &plan.steps()[0].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "count"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_composed_stream_project() {
    let batch = read_batch()
        .var_as(
            "count",
            g().n(NodeRef::all())
                .has("active", true)
                .range(1usize, 3usize)
                .count(),
        )
        .returning(["count"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 1);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::Count { plan }
            if matches!(
                plan.as_ref(),
                crate::exec::ExecCountPlan::Stream(crate::exec::ExecCountStreamPlan {
                    cursor: crate::exec::ExecCountCursorPlan::Filter { .. },
                    window: crate::exec::ExecCountWindowPlan {
                        skip: crate::exec::ExecUsizeExpr::Literal(1),
                        take: crate::exec::ExecCountTake::AtMost(
                            crate::exec::ExecUsizeExpr::Literal(2)
                        ),
                    },
                })
            )
    ));
    assert!(plan.steps()[0].dependencies.is_empty());
    assert!(matches!(
        &plan.steps()[0].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "count"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_access_expansion() {
    let batch = read_batch()
        .var_as("likes", g().n(NodeRef::all()).out_e(Some("LIKES")))
        .returning(["likes"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 2);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Expand {
            plan: ExpandPlan {
                direction: ExpandDirection::Out,
                output: ExpandOutput::Edges,
                label: ExpandLabelPlan::Label(label),
            },
        } if label.as_ref() == "LIKES"
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "likes"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_stream_aggregate() {
    let batch = read_batch()
        .var_as("groups", g().n(NodeRef::all()).group("kind"))
        .returning(["groups"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 2);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Aggregate {
            aggregate: AggregatePlan::Group(property),
        } if property.as_ref() == "kind"
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "groups"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_access_filter() {
    let batch = read_batch()
        .var_as("users", g().n(NodeRef::all()).has("active", true))
        .returning(["users"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 2);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Filter { predicate }
            if matches!(predicate.as_ref(), Predicate::Eq { .. })
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "users"
    ));
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_access_stream_shapes() {
    let range_batch = read_batch()
        .var_as("users", g().n(NodeRef::all()).range(2usize, 5usize))
        .returning(["users"]);
    let range_plan =
        crate::planning::plan_read_batch(&range_batch, &PlannerContext::default()).unwrap();

    assert_eq!(range_plan.steps().len(), 2);
    assert!(matches!(
        &range_plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &range_plan.steps()[1].op,
        crate::exec::ExecOp::Range {
            range: StreamRangePlan::Literal(range),
        } if range.start() == 2 && range.end() == 5
    ));
    assert_eq!(
        range_plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );

    let expected_order = OrderKeys::from(OrderKey {
        property: NonEmptyString::new("age").unwrap(),
        order: Order::Asc,
    });
    let order_batch = read_batch()
        .var_as("users", g().n(NodeRef::all()).order_by("age", Order::Asc))
        .returning(["users"]);
    let order_plan =
        crate::planning::plan_read_batch(&order_batch, &PlannerContext::default()).unwrap();

    assert_eq!(order_plan.steps().len(), 2);
    assert!(matches!(
        &order_plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &order_plan.steps()[1].op,
        crate::exec::ExecOp::Order {
            plan: OrderPlan::ExplicitSort(keys),
        } if keys == &expected_order
    ));
    assert_eq!(
        order_plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );

    let distinct_batch = read_batch()
        .var_as("users", g().n(NodeRef::all()).dedup())
        .returning(["users"]);
    let distinct_plan =
        crate::planning::plan_read_batch(&distinct_batch, &PlannerContext::default()).unwrap();

    assert_eq!(distinct_plan.steps().len(), 2);
    assert!(matches!(
        &distinct_plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &distinct_plan.steps()[1].op,
        crate::exec::ExecOp::Distinct
    ));
    assert_eq!(
        distinct_plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
}

#[test]
fn single_run_executable_entrypoint_uses_cascades_selected_dynamic_access_stream_bounds() {
    let batch = read_batch()
        .var_as(
            "users",
            g().n(NodeRef::all())
                .limit(StreamBound::expr(Expr::param("limit")))
                .skip(StreamBound::expr(Expr::param("offset")))
                .range(
                    StreamBound::expr(Expr::param("start")),
                    StreamBound::expr(Expr::param("end")),
                ),
        )
        .returning(["users"]);

    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 4);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Limit {
            count: StreamBoundPlan::Expr(_),
        }
    ));
    assert_eq!(
        plan.steps()[1].dependencies,
        vec![crate::exec::ExecStepId::new(1).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[2].op,
        crate::exec::ExecOp::Skip {
            count: StreamBoundPlan::Expr(_),
        }
    ));
    assert_eq!(
        plan.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
    assert!(matches!(
        &plan.steps()[3].op,
        crate::exec::ExecOp::Range {
            range: StreamRangePlan::Dynamic(_),
        }
    ));
    assert_eq!(
        plan.steps()[3].dependencies,
        vec![crate::exec::ExecStepId::new(3).unwrap()]
    );
}
