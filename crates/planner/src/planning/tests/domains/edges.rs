use crate::planning::tests::support::*;

#[test]
fn direct_edge_sources_keep_point_and_runtime_reference_plans() {
    let all_edges = executable_traversal(g().e(EdgeRef::all()), PlannerContext::default());
    let point_edges = executable_traversal(g().e([3u64, 4]), PlannerContext::default());
    let empty_edges = executable_traversal(
        g().e(EdgeRef::ids(Vec::<u64>::new())),
        PlannerContext::default(),
    );
    let param_edges =
        executable_traversal(g().e(EdgeRef::param("edge_ids")), PlannerContext::default());

    assert!(matches!(
        first_kv_read(&all_edges),
        KvReadPlan::RangeScan { keyspace, .. }
            if *keyspace == ElementKeyspace::EdgeEndpoints
    ));
    assert!(matches!(
        first_kv_read(&point_edges),
        KvReadPlan::MultiGet(plan)
            if plan.keyspace() == ElementKeyspace::EdgeEndpoints && plan.len() == 2
    ));
    assert!(matches!(
        first_exec_access(&empty_edges),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::Empty)
    ));
    assert!(matches!(
        first_exec_access(&param_edges),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::FromParam { param })
            if param.as_ref() == "edge_ids"
    ));
}

#[test]
fn edge_source_predicates_without_indexes_use_label_scan_with_residual_filter() {
    let plan = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active")),
        PlannerContext::default(),
    );

    assert!(matches!(
        first_exec_access(&plan),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::LabelScan { label })
            if label.as_ref() == "FOLLOWS"
    ));
    assert!(plan
        .steps()
        .iter()
        .any(|step| matches!(&step.op, ExecOp::Filter { .. })));
}

#[test]
fn edge_source_predicates_use_cascades_equality_indexes_in_executable_plan() {
    let plan = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active")),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    assert!(matches!(
        first_exec_access(&plan),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::Bitmap { bitmap: crate::exec::ExecEdgeBitmapExpr::PointRead { key, value, .. } })
            if key.label == "FOLLOWS"
                && key.property == "status"
                && value.literal().as_property_value().as_str() == Some("active")
    ));
    assert!(plan
        .steps()
        .iter()
        .all(|step| !matches!(&step.op, ExecOp::Filter { .. })));
    assert!(plan.metrics().memo_groups >= 1);
    assert!(plan.metrics().alternatives_considered >= 1);
}

#[test]
fn edge_source_predicates_use_cascades_range_indexes_in_executable_plan() {
    let plan = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::lt("weight", 50)),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    assert!(matches!(
        first_exec_access(&plan),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::RangeIndex { key, range, .. })
            if key.label == "FOLLOWS"
                && key.property == "weight"
                && key.direction == RangeIndexDirection::Asc
                && matches!(
                    range,
                    IndexRange::Upper {
                        upper: IndexBound::Exclusive(_)
                    }
                )
    ));
    assert!(plan
        .steps()
        .iter()
        .all(|step| !matches!(&step.op, ExecOp::Filter { .. })));
}

#[test]
fn edge_order_uses_cascades_range_direction_without_explicit_sort() {
    let indexes = builtin_label_indexes()
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Desc)
                .unwrap(),
        );
    let plan = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::lt("weight", 50))
            .order_by("weight", Order::Desc),
        ctx(indexes),
    );

    assert!(matches!(
        first_exec_access(&plan),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::RangeIndex { key, .. })
            if key.label == "FOLLOWS"
                && key.property == "weight"
                && key.direction == RangeIndexDirection::Desc
    ));
    assert!(plan
        .steps()
        .iter()
        .all(|step| !matches!(&step.op, ExecOp::Order { .. })));
}

#[test]
fn unscoped_edge_source_predicates_remain_residual_filters_after_cascades() {
    let plan = executable_traversal(
        g().e_where(Predicate::eq("status", "active")),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    assert!(plan.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
                if *keyspace == ElementKeyspace::EdgeEndpoints
        )
    }));
    assert!(plan
        .steps()
        .iter()
        .any(|step| matches!(&step.op, ExecOp::Filter { .. })));
}
