use crate::planning::tests::support::*;

#[test]
fn direct_node_sources_keep_scan_point_and_runtime_reference_plans() {
    let all_nodes = executable_traversal(g().n(NodeRef::all()), PlannerContext::default());
    let point_nodes = executable_traversal(g().n([7u64, 9]), PlannerContext::default());
    let empty_nodes = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new())),
        PlannerContext::default(),
    );
    let param_nodes =
        executable_traversal(g().n(NodeRef::param("node_ids")), PlannerContext::default());

    assert!(matches!(
        first_kv_read(&all_nodes),
        KvReadPlan::RangeScan { keyspace, .. }
            if *keyspace == ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        first_kv_read(&point_nodes),
        KvReadPlan::MultiGet(plan)
            if plan.keyspace() == ElementKeyspace::NodeProperty && plan.len() == 2
    ));
    assert!(matches!(
        first_exec_access(&empty_nodes),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert!(matches!(
        first_exec_access(&param_nodes),
        ExecAccessPlan::Node(ExecNodeAccessPlan::FromParam { param })
            if param.as_ref() == "node_ids"
    ));
}

#[test]
fn node_source_predicates_without_indexes_use_label_scan_with_residual_filter() {
    let plan = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice")),
        PlannerContext::default(),
    );

    assert!(matches!(
        first_exec_access(&plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::LabelScan { label })
            if label.as_ref() == "User"
    ));
    assert!(plan
        .steps()
        .iter()
        .any(|step| matches!(&step.op, ExecOp::Filter { .. })));
}

#[test]
fn node_source_predicates_use_cascades_equality_indexes_in_executable_plan() {
    let plan = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice")),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );

    assert!(matches!(
        first_exec_access(&plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, value, .. })
            if key.label == "User"
                && key.property == "username"
                && matches!(value, IndexValue::Literal(_))
    ));
    assert!(plan
        .steps()
        .iter()
        .all(|step| !matches!(&step.op, ExecOp::Filter { .. })));
    assert!(plan.metrics().memo_groups >= 1);
    assert!(plan.metrics().alternatives_considered >= 1);
}

#[test]
fn node_source_predicates_use_cascades_range_indexes_in_executable_plan() {
    let plan = executable_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21)),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    assert!(matches!(
        first_exec_access(&plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::RangeIndex { key, range, .. })
            if key.label == "User"
                && key.property == "age"
                && key.direction == RangeIndexDirection::Asc
                && matches!(
                    range,
                    IndexRange::Lower {
                        lower: IndexBound::Inclusive(_)
                    }
                )
    ));
    assert!(plan
        .steps()
        .iter()
        .all(|step| !matches!(&step.op, ExecOp::Filter { .. })));
}

#[test]
fn node_order_uses_cascades_range_direction_without_explicit_sort() {
    let indexes = builtin_label_indexes()
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc).unwrap(),
        );
    let plan = executable_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Desc),
        ctx(indexes),
    );

    assert!(matches!(
        first_exec_access(&plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::RangeIndex { key, .. })
            if key.label == "User"
                && key.property == "age"
                && key.direction == RangeIndexDirection::Desc
    ));
    assert!(plan
        .steps()
        .iter()
        .all(|step| !matches!(&step.op, ExecOp::Order { .. })));
}

#[test]
fn unscoped_node_source_predicates_remain_residual_filters_after_cascades() {
    let plan = executable_traversal(
        g().n_where(Predicate::eq("username", "alice")),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );

    assert!(plan.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
                if *keyspace == ElementKeyspace::NodeProperty
        )
    }));
    assert!(plan
        .steps()
        .iter()
        .any(|step| matches!(&step.op, ExecOp::Filter { .. })));
}
