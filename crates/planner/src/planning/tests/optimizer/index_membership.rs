//! End-to-end selection of node index membership for post-expansion filters.

use crate::planning::tests::support::*;

fn membership_indexes() -> IndexCatalogSnapshot {
    IndexCatalogSnapshot::default()
        .with_node_eq(ScopedPropertyKey::try_new("Group", "name").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("Attribute", "kind").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("Attribute", "status").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("Attribute", "rank", RangeIndexDirection::Asc)
                .unwrap(),
        )
}

fn attributes_where(predicate: Predicate) -> Traversal<helix_ast::traversal::OnNodes, ReadOnly> {
    g().n_with_label_where("Group", Predicate::eq("name", "g3"))
        .in_(Some("IN_GROUP"))
        .out(Some("HAS_ATTRIBUTE"))
        .where_(predicate)
}

fn memberships(plan: &ExecutablePlan) -> Vec<&crate::exec::ExecNodeIndexMembershipPlan> {
    plan.steps()
        .iter()
        .filter_map(|step| match &step.op {
            ExecOp::IndexMembership { plan } => Some(plan.as_ref()),
            _ => None,
        })
        .collect()
}

fn only_membership(plan: &ExecutablePlan) -> &crate::exec::ExecNodeIndexMembershipPlan {
    let [membership] = memberships(plan)[..] else {
        panic!("expected one index membership step: {:#?}", plan.steps());
    };
    membership
}

fn filter_predicates(plan: &ExecutablePlan) -> Vec<&Predicate> {
    plan.steps()
        .iter()
        .filter_map(|step| match &step.op {
            ExecOp::Filter { predicate } => Some(predicate.predicate()),
            _ => None,
        })
        .collect()
}

#[test]
fn post_expansion_equality_filter_uses_index_membership_after_out_and_in() {
    for traversal in [
        attributes_where(Predicate::eq("kind", "B")),
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out(Some("HAS_ATTRIBUTE"))
            .in_(Some("LINKS"))
            .where_(Predicate::eq("kind", "B")),
    ] {
        let plan = executable_traversal(traversal.values(vec!["kind"]), ctx(membership_indexes()));
        let membership = only_membership(&plan);

        assert_eq!(membership.label.as_ref(), "Attribute");
        assert_eq!(
            membership.outside_label,
            crate::ir::NodeMembershipOutsideLabel::Evaluate
        );
        assert_eq!(
            membership.predicate.predicate(),
            &Predicate::eq("kind", "B")
        );
        assert!(matches!(
            &membership.set,
            crate::exec::ExecNodeSecondarySetPlan::Bitmap(
                crate::exec::ExecNodeBitmapExpr::PointRead { key, .. }
            ) if key.label == "Attribute" && key.property == "kind"
        ));
        assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
        assert!(has_exec_op_family(&plan, ExecOpFamily::Expand));
        assert_eq!(
            crate::diagnostics::analyze(&plan, &ctx(membership_indexes()))
                .statistics
                .residual_filters,
            0
        );
    }
}

#[test]
fn post_expansion_membership_keeps_unindexed_conjuncts_as_a_residual_filter() {
    let plan = executable_traversal(
        attributes_where(Predicate::and(vec![
            Predicate::eq("kind", "B"),
            Predicate::contains("title", "x"),
        ]))
        .values(vec!["kind"]),
        ctx(membership_indexes()),
    );

    assert_eq!(
        only_membership(&plan).predicate.predicate(),
        &Predicate::eq("kind", "B")
    );
    assert_eq!(
        filter_predicates(&plan),
        [&Predicate::contains("title", "x")]
    );
    let membership_position = plan
        .steps()
        .iter()
        .position(|step| matches!(step.op, ExecOp::IndexMembership { .. }))
        .unwrap();
    let filter_position = plan
        .steps()
        .iter()
        .position(|step| matches!(step.op, ExecOp::Filter { .. }))
        .unwrap();
    assert!(membership_position < filter_position);
}

#[test]
fn post_expansion_membership_covers_in_range_and_multiple_indexed_conjuncts() {
    let is_in = executable_traversal(
        attributes_where(Predicate::is_in(
            "kind",
            PropertyValue::StringArray(vec!["A".to_owned(), "B".to_owned()]),
        ))
        .values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert!(matches!(
        &only_membership(&is_in).set,
        crate::exec::ExecNodeSecondarySetPlan::Bitmap(
            crate::exec::ExecNodeBitmapExpr::BatchedUnionRead { values, .. }
        ) if values.len() == 2
    ));

    let range = executable_traversal(
        attributes_where(Predicate::gte("rank", 3)).values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert!(matches!(
        &only_membership(&range).set,
        crate::exec::ExecNodeSecondarySetPlan::Range(range)
            if range.key.label == "Attribute" && range.key.property == "rank"
    ));

    // Two serial bitmap reads only pay off once the stream is known to be
    // larger than the unknown-fan-out default.
    let both_predicate = Predicate::and(vec![
        Predicate::eq("kind", "B"),
        Predicate::eq("status", "live"),
    ]);
    let default_stream = executable_traversal(
        attributes_where(both_predicate.clone()).values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert!(memberships(&default_stream).is_empty());
    let mut large_stream = ctx(membership_indexes());
    large_stream.stats = large_stream
        .stats
        .with_node_eq_cardinality(ScopedPropertyKey::try_new("Group", "name").unwrap(), 5_000);
    let both = executable_traversal(
        attributes_where(both_predicate).values(vec!["kind"]),
        large_stream,
    );
    assert!(matches!(
        &only_membership(&both).set,
        crate::exec::ExecNodeSecondarySetPlan::Bitmap(
            crate::exec::ExecNodeBitmapExpr::Intersect { .. }
        ) | crate::exec::ExecNodeSecondarySetPlan::Intersect { .. }
    ));
    assert!(filter_predicates(&both).is_empty());
}

#[test]
fn label_scoped_post_expansion_membership_rejects_other_labels_without_reads() {
    let plan = executable_traversal(
        attributes_where(Predicate::eq("kind", "B"))
            .has_label("Attribute")
            .values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    let membership = only_membership(&plan);

    assert_eq!(
        membership.outside_label,
        crate::ir::NodeMembershipOutsideLabel::Reject
    );
    assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
}

#[test]
fn null_unindexed_and_edge_stream_filters_keep_the_per_row_filter() {
    for traversal in [
        attributes_where(Predicate::eq("kind", PropertyValue::Null)),
        attributes_where(Predicate::eq("color", "red")),
        attributes_where(Predicate::contains("kind", "B")),
    ] {
        let plan = executable_traversal(traversal.values(vec!["kind"]), ctx(membership_indexes()));
        assert!(memberships(&plan).is_empty(), "{:#?}", plan.steps());
        assert_eq!(filter_predicates(&plan).len(), 1);
    }

    let edges = executable_traversal(
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out_e(Some("HAS_ATTRIBUTE"))
            .where_(Predicate::eq("kind", "B"))
            .values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert!(memberships(&edges).is_empty(), "{:#?}", edges.steps());
    assert_eq!(filter_predicates(&edges).len(), 1);
}

#[test]
fn late_bound_parameters_keep_runtime_classified_membership() {
    let mut planner_ctx = ctx(membership_indexes());
    planner_ctx.late_bound_params = [
        NonEmptyString::new("kind").unwrap(),
        NonEmptyString::new("kinds").unwrap(),
    ]
    .into_iter()
    .collect();
    let equality = executable_traversal(
        attributes_where(Predicate::eq_param("kind", "kind")).values(vec!["kind"]),
        planner_ctx.clone(),
    );
    assert!(matches!(
        &only_membership(&equality).set,
        crate::exec::ExecNodeSecondarySetPlan::DynamicEquality { param, .. }
            if param.as_ref() == "kind"
    ));

    let set = executable_traversal(
        attributes_where(Predicate::is_in_param("kind", "kinds")).values(vec!["kind"]),
        planner_ctx,
    );
    assert!(matches!(
        &only_membership(&set).set,
        crate::exec::ExecNodeSecondarySetPlan::DynamicMembership { values, .. }
            if values.param().as_ref() == "kinds"
    ));
}

#[test]
fn source_filters_still_use_index_access_instead_of_membership() {
    let plan = executable_traversal(
        g().n_with_label_where("Attribute", Predicate::eq("kind", "B"))
            .out(Some("LINKS"))
            .values(vec!["kind"]),
        ctx(membership_indexes()),
    );

    assert!(memberships(&plan).is_empty());
    assert!(matches!(
        unwrapped_first_exec_access(&plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Bitmap { .. })
    ));
}

#[test]
fn tiny_bounded_inputs_keep_the_per_row_filter() {
    let plan = executable_traversal(
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out(Some("HAS_ATTRIBUTE"))
            .limit(2)
            .where_(Predicate::eq("kind", "B"))
            .values(vec!["kind"]),
        ctx(membership_indexes()),
    );

    assert!(memberships(&plan).is_empty(), "{:#?}", plan.steps());
    assert_eq!(filter_predicates(&plan), [&Predicate::eq("kind", "B")]);
}

#[test]
fn post_expansion_membership_feeds_counts_and_variable_pipelines() {
    let count = executable_traversal(
        attributes_where(Predicate::eq("kind", "B")).count(),
        ctx(membership_indexes()),
    );
    let counted = count
        .steps()
        .iter()
        .find_map(|step| match &step.op {
            ExecOp::Count { plan } => Some(plan.as_ref()),
            _ => None,
        })
        .expect("count step");
    assert!(
        matches!(
            counted,
            ExecCountPlan::Stream(crate::exec::ExecCountStreamPlan {
                cursor: crate::exec::ExecCountCursorPlan::IndexMembership { .. },
                ..
            })
        ),
        "{counted:#?}"
    );

    let batch = read_batch()
        .var_as(
            "groups",
            g().n_with_label_where("Group", Predicate::eq("name", "g3")),
        )
        .var_as(
            "result",
            g().n(NodeRef::var("groups"))
                .in_(Some("IN_GROUP"))
                .out(Some("HAS_ATTRIBUTE"))
                .where_(Predicate::eq("kind", "B"))
                .values(vec!["kind"]),
        )
        .returning(["result"]);
    let plan = crate::planning::plan_read_batch(&batch, &ctx(membership_indexes())).unwrap();
    assert_eq!(only_membership(&plan).label.as_ref(), "Attribute");
    assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
}
