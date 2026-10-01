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

/// Statistics proving that `name = g3` selects 5,000 of 50,000 groups, so
/// the source stays an index read and the expanded stream is large.
fn large_ctx() -> PlannerContext {
    let mut large = ctx(membership_indexes());
    large.stats = large
        .stats
        .with_node_eq_cardinality(ScopedPropertyKey::try_new("Group", "name").unwrap(), 5_000)
        .with_node_label_cardinality(NonEmptyString::new("Group").unwrap(), 50_000);
    large
}

fn attributes_where(predicate: Predicate) -> Traversal<helix_ast::traversal::OnNodes, ReadOnly> {
    g().n_with_label_where("Group", Predicate::eq("name", "g3"))
        .in_(Some("IN_GROUP"))
        .out(Some("HAS_ATTRIBUTE"))
        .where_(predicate)
}

/// The benchmark's `kind == B` predicate, unscoped and label-scoped, with the
/// decision its membership makes for nodes of other labels.
fn kind_b_policies() -> [(Predicate, crate::ir::NodeMembershipOutsideLabel); 2] {
    [
        (
            Predicate::eq("kind", "B"),
            crate::ir::NodeMembershipOutsideLabel::Evaluate,
        ),
        (
            Predicate::and(vec![
                Predicate::eq("$label", "Attribute"),
                Predicate::eq("kind", "B"),
            ]),
            crate::ir::NodeMembershipOutsideLabel::Reject,
        ),
    ]
}

/// Index-set fields of a membership. Every membership planned here decides
/// its predicate from a secondary-index set.
trait IndexSetFields {
    fn index_set(&self) -> &crate::exec::ExecNodeSecondarySetPlan;
    fn label(&self) -> &NonEmptyString;
    fn outside_label(&self) -> crate::ir::NodeMembershipOutsideLabel;
}

impl IndexSetFields for crate::exec::ExecNodeIndexMembershipPlan {
    fn index_set(&self) -> &crate::exec::ExecNodeSecondarySetPlan {
        let crate::exec::ExecNodeMembershipSet::Index { set, .. } = &self.set else {
            panic!("expected an index membership set: {:?}", self.set);
        };
        set
    }

    fn label(&self) -> &NonEmptyString {
        let crate::exec::ExecNodeMembershipSet::Index { label, .. } = &self.set else {
            panic!("expected an index membership set: {:?}", self.set);
        };
        label
    }

    fn outside_label(&self) -> crate::ir::NodeMembershipOutsideLabel {
        let crate::exec::ExecNodeMembershipSet::Index { outside_label, .. } = &self.set else {
            panic!("expected an index membership set: {:?}", self.set);
        };
        *outside_label
    }
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

fn count_cursor(plan: &ExecutablePlan) -> &crate::exec::ExecCountCursorPlan {
    let counted = plan
        .steps()
        .iter()
        .find_map(|step| match &step.op {
            ExecOp::Count { plan } => Some(plan.as_ref()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected a count step: {:#?}", plan.steps()));
    let ExecCountPlan::Stream(stream) = counted else {
        panic!("expected a count cursor: {counted:#?}");
    };
    &stream.cursor
}

#[test]
fn post_expansion_equality_filter_uses_index_membership_after_out_and_in() {
    // Without statistics this is the benchmark shape.
    for planner_ctx in [ctx(membership_indexes()), large_ctx()] {
        for traversal in [
            attributes_where(Predicate::eq("kind", "B")),
            g().n_with_label_where("Group", Predicate::eq("name", "g3"))
                .out(Some("HAS_ATTRIBUTE"))
                .in_(Some("LINKS"))
                .where_(Predicate::eq("kind", "B")),
        ] {
            let plan = executable_traversal(traversal.values(vec!["kind"]), planner_ctx.clone());
            let membership = only_membership(&plan);

            assert_eq!(membership.label().as_ref(), "Attribute");
            assert_eq!(
                membership.outside_label(),
                crate::ir::NodeMembershipOutsideLabel::Evaluate
            );
            assert_eq!(
                membership.predicate.predicate(),
                &Predicate::eq("kind", "B")
            );
            assert!(matches!(
                &membership.index_set(),
                crate::exec::ExecNodeSecondarySetPlan::Bitmap(
                    crate::exec::ExecNodeBitmapExpr::PointRead { key, .. }
                ) if key.label == "Attribute" && key.property == "kind"
            ));
            assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
            assert!(has_exec_op_family(&plan, ExecOpFamily::Expand));
            assert_eq!(
                crate::diagnostics::analyze(&plan, &planner_ctx)
                    .statistics
                    .residual_filters,
                0
            );
        }
    }
}

#[test]
fn post_expansion_membership_fuses_unindexed_conjuncts_as_its_residual() {
    let predicate = Predicate::and(vec![
        Predicate::eq("kind", "B"),
        Predicate::contains("title", "x"),
    ]);
    let plan = executable_traversal(
        attributes_where(predicate.clone()).values(vec!["kind"]),
        large_ctx(),
    );

    let membership = only_membership(&plan);
    assert_eq!(membership.predicate.predicate(), &predicate);
    assert_eq!(
        membership
            .residual
            .as_ref()
            .map(|residual| residual.predicate()),
        Some(&Predicate::contains("title", "x"))
    );
    assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
}

#[test]
fn post_expansion_membership_covers_in_and_multiple_indexed_conjuncts_but_not_ranges() {
    let is_in = executable_traversal(
        attributes_where(Predicate::is_in(
            "kind",
            PropertyValue::StringArray(vec!["A".to_owned(), "B".to_owned()]),
        ))
        .values(vec!["kind"]),
        large_ctx(),
    );
    assert!(matches!(
        &only_membership(&is_in).index_set(),
        crate::exec::ExecNodeSecondarySetPlan::Bitmap(
            crate::exec::ExecNodeBitmapExpr::BatchedUnionRead { values, .. }
        ) if values.len() == 2
    ));

    // A range set would verify every in-range record of the label, so a lone
    // range keeps the per-row filter, and behind an equality set it is the
    // residual that only set matches evaluate.
    let range = executable_traversal(
        attributes_where(Predicate::gte("rank", 3)).values(vec!["kind"]),
        large_ctx(),
    );
    assert!(memberships(&range).is_empty(), "{:#?}", range.steps());
    assert_eq!(filter_predicates(&range), [&Predicate::gte("rank", 3)]);
    let ranged = executable_traversal(
        attributes_where(Predicate::and(vec![
            Predicate::eq("kind", "B"),
            Predicate::gte("rank", 3),
        ]))
        .values(vec!["kind"]),
        large_ctx(),
    );
    assert_eq!(
        only_membership(&ranged)
            .residual
            .as_ref()
            .map(|residual| residual.predicate()),
        Some(&Predicate::gte("rank", 3))
    );
    assert!(
        filter_predicates(&ranged).is_empty(),
        "{:#?}",
        ranged.steps()
    );

    let both = executable_traversal(
        attributes_where(Predicate::and(vec![
            Predicate::eq("kind", "B"),
            Predicate::eq("status", "live"),
        ]))
        .values(vec!["kind"]),
        large_ctx(),
    );
    assert!(matches!(
        &only_membership(&both).index_set(),
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
        large_ctx(),
    );
    let membership = only_membership(&plan);

    assert_eq!(
        membership.outside_label(),
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
        let plan = executable_traversal(traversal.values(vec!["kind"]), large_ctx());
        assert!(memberships(&plan).is_empty(), "{:#?}", plan.steps());
        assert_eq!(filter_predicates(&plan).len(), 1);
    }

    let edges = executable_traversal(
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out_e(Some("HAS_ATTRIBUTE"))
            .where_(Predicate::eq("kind", "B"))
            .values(vec!["kind"]),
        large_ctx(),
    );
    assert!(memberships(&edges).is_empty(), "{:#?}", edges.steps());
    assert_eq!(filter_predicates(&edges).len(), 1);
}

#[test]
fn late_bound_parameters_keep_runtime_classified_membership() {
    let mut planner_ctx = large_ctx();
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
        &only_membership(&equality).index_set(),
        crate::exec::ExecNodeSecondarySetPlan::DynamicEquality { param, .. }
            if param.as_ref() == "kind"
    ));

    let set = executable_traversal(
        attributes_where(Predicate::is_in_param("kind", "kinds")).values(vec!["kind"]),
        planner_ctx.clone(),
    );
    assert!(matches!(
        &only_membership(&set).index_set(),
        crate::exec::ExecNodeSecondarySetPlan::DynamicMembership { values, .. }
            if values.param().as_ref() == "kinds"
    ));

    // Late-bound range bounds may bind values without a range encoding, so
    // they keep the per-row filter instead of a runtime range scan.
    planner_ctx.late_bound_params = [NonEmptyString::new("min").unwrap()].into_iter().collect();
    let range = executable_traversal(
        attributes_where(Predicate::gte_param("rank", "min")).values(vec!["kind"]),
        planner_ctx,
    );
    assert!(memberships(&range).is_empty(), "{:#?}", range.steps());
    assert_eq!(
        filter_predicates(&range),
        [&Predicate::gte_param("rank", "min")]
    );
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
fn bounded_inputs_still_use_membership() {
    let expanded = || {
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out(Some("HAS_ATTRIBUTE"))
    };
    for planner_ctx in [ctx(membership_indexes()), large_ctx()] {
        for (predicate, outside_label) in kind_b_policies() {
            for bounded in [expanded().limit(2), expanded().range(0usize, 3usize)] {
                let plan = executable_traversal(
                    bounded.where_(predicate.clone()).values(vec!["kind"]),
                    planner_ctx.clone(),
                );

                assert_eq!(only_membership(&plan).outside_label(), outside_label);
                assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
            }
        }
    }
}

#[test]
fn point_sources_plan_membership_without_statistics() {
    // An indexed filter is never decided row by row, whatever the stream's
    // estimate.
    for (predicate, outside_label) in kind_b_policies() {
        let plan = executable_traversal(
            attributes_where(predicate).values(vec!["kind"]),
            ctx(membership_indexes()),
        );
        let membership = only_membership(&plan);
        assert_eq!(membership.outside_label(), outside_label);
        assert_eq!(membership.label().as_ref(), "Attribute");
        assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
    }

    let both = executable_traversal(
        attributes_where(Predicate::and(vec![
            Predicate::eq("kind", "B"),
            Predicate::eq("status", "live"),
        ]))
        .values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert!(matches!(
        &only_membership(&both).index_set(),
        crate::exec::ExecNodeSecondarySetPlan::Bitmap(
            crate::exec::ExecNodeBitmapExpr::Intersect { .. }
        ) | crate::exec::ExecNodeSecondarySetPlan::Intersect { .. }
    ));
    assert!(filter_predicates(&both).is_empty(), "{:#?}", both.steps());
}

#[test]
fn point_source_membership_covers_in_late_bound_and_one_hop_shapes() {
    let is_in = executable_traversal(
        attributes_where(Predicate::is_in(
            "kind",
            PropertyValue::StringArray(vec!["A".to_owned(), "B".to_owned()]),
        ))
        .values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert!(matches!(
        &only_membership(&is_in).index_set(),
        crate::exec::ExecNodeSecondarySetPlan::Bitmap(
            crate::exec::ExecNodeBitmapExpr::BatchedUnionRead { values, .. }
        ) if values.len() == 2
    ));

    let mut late_bound = ctx(membership_indexes());
    late_bound.late_bound_params = [NonEmptyString::new("kind").unwrap()].into_iter().collect();
    let equality = executable_traversal(
        attributes_where(Predicate::eq_param("kind", "kind")).values(vec!["kind"]),
        late_bound,
    );
    assert!(matches!(
        &only_membership(&equality).index_set(),
        crate::exec::ExecNodeSecondarySetPlan::DynamicEquality { param, .. }
            if param.as_ref() == "kind"
    ));

    let one_hop = executable_traversal(
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out(Some("HAS_ATTRIBUTE"))
            .where_(Predicate::eq("kind", "B"))
            .values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert_eq!(only_membership(&one_hop).label().as_ref(), "Attribute");
    assert!(
        filter_predicates(&one_hop).is_empty(),
        "{:#?}",
        one_hop.steps()
    );

    // A unique source proves one row, but its expansions are unbounded.
    let mut unique = ctx(membership_indexes());
    unique.indexes.node_eq.insert(
        ScopedPropertyKey::try_new("Group", "name").unwrap(),
        NodeEqualityIndexMeta::try_new("group-name")
            .unwrap()
            .with_uniqueness(IndexUniqueness::Unique),
    );
    let plan = executable_traversal(
        attributes_where(Predicate::eq("kind", "B")).values(vec!["kind"]),
        unique,
    );
    assert_eq!(only_membership(&plan).label().as_ref(), "Attribute");
    assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
}

#[test]
fn point_source_membership_feeds_pull_exists_count_and_variable_pipelines() {
    for (predicate, outside_label) in kind_b_policies() {
        let limited = executable_traversal(
            attributes_where(predicate.clone())
                .limit(5)
                .values(vec!["kind"]),
            ctx(membership_indexes()),
        );
        assert_eq!(only_membership(&limited).outside_label(), outside_label);
        let position = |family: fn(&ExecOp) -> bool| {
            limited
                .steps()
                .iter()
                .position(|step| family(&step.op))
                .unwrap_or_else(|| panic!("missing step: {:#?}", limited.steps()))
        };
        assert!(
            position(|op| matches!(op, ExecOp::IndexMembership { .. }))
                < position(|op| matches!(op, ExecOp::Limit { .. })),
            "{:#?}",
            limited.steps()
        );

        let exists = executable_traversal(
            attributes_where(predicate.clone()).exists(),
            ctx(membership_indexes()),
        );
        assert_eq!(only_membership(&exists).outside_label(), outside_label);
        assert!(
            filter_predicates(&exists).is_empty(),
            "{:#?}",
            exists.steps()
        );

        let count = executable_traversal(
            attributes_where(predicate.clone()).count(),
            ctx(membership_indexes()),
        );
        let crate::exec::ExecCountCursorPlan::IndexMembership { plan, .. } = count_cursor(&count)
        else {
            panic!("expected a membership count cursor: {:#?}", count.steps());
        };
        assert_eq!(plan.outside_label(), outside_label);

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
                    .where_(predicate)
                    .values(vec!["kind"]),
            )
            .returning(["result"]);
        let plan = crate::planning::plan_read_batch(&batch, &ctx(membership_indexes())).unwrap();
        assert_eq!(only_membership(&plan).outside_label(), outside_label);
        assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
    }
}

#[test]
fn post_expansion_membership_feeds_counts_and_variable_pipelines() {
    for planner_ctx in [ctx(membership_indexes()), large_ctx()] {
        for (predicate, outside_label) in kind_b_policies() {
            let count = executable_traversal(
                attributes_where(predicate.clone()).count(),
                planner_ctx.clone(),
            );
            let crate::exec::ExecCountCursorPlan::IndexMembership { plan, .. } =
                count_cursor(&count)
            else {
                panic!("expected a membership count cursor: {:#?}", count.steps());
            };
            assert_eq!(plan.outside_label(), outside_label);

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
                        .where_(predicate)
                        .values(vec!["kind"]),
                )
                .returning(["result"]);
            let plan = crate::planning::plan_read_batch(&batch, &planner_ctx).unwrap();
            let membership = only_membership(&plan);
            assert_eq!(membership.label().as_ref(), "Attribute");
            assert_eq!(membership.outside_label(), outside_label);
            assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
        }
    }
}

#[test]
fn unscoped_membership_needs_exactly_one_indexed_label() {
    let label_scan = |predicate: Predicate| {
        g().n_with_label("Group")
            .in_(Some("IN_GROUP"))
            .out(Some("HAS_ATTRIBUTE"))
            .where_(predicate)
            .values(vec!["kind"])
    };
    for (predicate, outside_label) in kind_b_policies() {
        let plan = executable_traversal(label_scan(predicate), ctx(membership_indexes()));
        assert_eq!(only_membership(&plan).outside_label(), outside_label);
    }

    // A second label indexing `kind` leaves the expansion's label ambiguous,
    // so eligibility, not cost, keeps the per-row filter unless it is scoped.
    for mut ambiguous in [ctx(membership_indexes()), large_ctx()] {
        ambiguous.indexes =
            membership_indexes().with_node_eq(ScopedPropertyKey::try_new("Note", "kind").unwrap());
        let plan = executable_traversal(
            attributes_where(Predicate::eq("kind", "B")).values(vec!["kind"]),
            ambiguous.clone(),
        );
        assert!(memberships(&plan).is_empty(), "{:#?}", plan.steps());
        assert_eq!(filter_predicates(&plan), [&Predicate::eq("kind", "B")]);
        let plan = executable_traversal(
            attributes_where(Predicate::and(vec![
                Predicate::eq("$label", "Note"),
                Predicate::eq("kind", "B"),
            ]))
            .values(vec!["kind"]),
            ambiguous,
        );
        assert_eq!(only_membership(&plan).label().as_ref(), "Note");
    }
}

#[test]
fn proven_bounds_never_change_the_membership_choice() {
    let expanded = || {
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out(Some("HAS_ATTRIBUTE"))
    };
    for (predicate, outside_label) in kind_b_policies() {
        for count in [256, 257, 100_000] {
            let plan = executable_traversal(
                expanded()
                    .limit(count)
                    .where_(predicate.clone())
                    .values(vec!["kind"]),
                ctx(membership_indexes()),
            );
            assert_eq!(only_membership(&plan).outside_label(), outside_label);
            assert!(
                filter_predicates(&plan).is_empty(),
                "{count}: {:#?}",
                plan.steps()
            );
        }

        // A limit after the filter.
        let plan = executable_traversal(
            expanded()
                .where_(predicate.clone())
                .limit(2)
                .values(vec!["kind"]),
            ctx(membership_indexes()),
        );
        assert_eq!(only_membership(&plan).outside_label(), outside_label);
    }
}

#[test]
fn range_predicates_keep_the_per_row_filter_behind_point_sources() {
    let range = executable_traversal(
        attributes_where(Predicate::gte("rank", 3)).values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert!(memberships(&range).is_empty(), "{:#?}", range.steps());
    assert_eq!(filter_predicates(&range), [&Predicate::gte("rank", 3)]);

    // Behind a membership on `kind`, the range conjunct is the residual only
    // set matches evaluate, so each kept record is read once.
    let conjunction = Predicate::and(vec![Predicate::eq("kind", "B"), Predicate::gte("rank", 3)]);
    for planner_ctx in [ctx(membership_indexes()), large_ctx()] {
        let ranged = executable_traversal(
            attributes_where(conjunction.clone()).values(vec!["kind"]),
            planner_ctx,
        );
        let membership = only_membership(&ranged);
        assert_eq!(membership.predicate.predicate(), &conjunction);
        assert_eq!(
            membership
                .residual
                .as_ref()
                .map(|residual| residual.predicate()),
            Some(&Predicate::gte("rank", 3))
        );
        assert!(
            filter_predicates(&ranged).is_empty(),
            "{:#?}",
            ranged.steps()
        );
    }

    let count = executable_traversal(
        attributes_where(Predicate::gte("rank", 3)).count(),
        ctx(membership_indexes()),
    );
    assert!(
        matches!(
            count_cursor(&count),
            crate::exec::ExecCountCursorPlan::Filter { .. }
        ),
        "{:#?}",
        count.steps()
    );

    let mut late_bound = ctx(membership_indexes());
    late_bound.late_bound_params = [NonEmptyString::new("min").unwrap()].into_iter().collect();
    let late = executable_traversal(
        attributes_where(Predicate::gte_param("rank", "min")).values(vec!["kind"]),
        late_bound,
    );
    assert!(memberships(&late).is_empty(), "{:#?}", late.steps());
    assert_eq!(
        filter_predicates(&late),
        [&Predicate::gte_param("rank", "min")]
    );
}

#[test]
fn ambiguous_unscoped_labels_keep_the_filter_behind_point_sources() {
    let mut ambiguous = ctx(membership_indexes());
    ambiguous.indexes =
        membership_indexes().with_node_eq(ScopedPropertyKey::try_new("Note", "kind").unwrap());

    let plan = executable_traversal(
        attributes_where(Predicate::eq("kind", "B")).values(vec!["kind"]),
        ambiguous.clone(),
    );
    assert!(memberships(&plan).is_empty(), "{:#?}", plan.steps());
    assert_eq!(filter_predicates(&plan), [&Predicate::eq("kind", "B")]);

    let count = executable_traversal(
        attributes_where(Predicate::eq("kind", "B")).count(),
        ambiguous.clone(),
    );
    assert!(
        matches!(
            count_cursor(&count),
            crate::exec::ExecCountCursorPlan::Filter { .. }
        ),
        "{:#?}",
        count.steps()
    );

    let plan = executable_traversal(
        attributes_where(Predicate::and(vec![
            Predicate::eq("$label", "Note"),
            Predicate::eq("kind", "B"),
        ]))
        .values(vec!["kind"]),
        ambiguous,
    );
    assert_eq!(only_membership(&plan).label().as_ref(), "Note");
}

/// `large_ctx` with a second, broad `Group` equality index. `name = g3`
/// selects 5,000 of 5,000,000 groups while `region = west` selects
/// 4,900,000. Both conjuncts are index-served, so the source intersects
/// both sets rather than reading `name` and evaluating `region` per row.
fn two_index_ctx() -> PlannerContext {
    let mut planner_ctx =
        ctx(membership_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("Group", "region").unwrap()));
    planner_ctx.stats = planner_ctx
        .stats
        .with_node_eq_cardinality(ScopedPropertyKey::try_new("Group", "name").unwrap(), 5_000)
        .with_node_eq_cardinality(
            ScopedPropertyKey::try_new("Group", "region").unwrap(),
            4_900_000,
        )
        .with_node_label_cardinality(NonEmptyString::new("Group").unwrap(), 5_000_000);
    planner_ctx
}

/// Attributes behind the groups named `g3` in region `west`.
fn two_index_attributes_where(
    predicate: Predicate,
) -> Traversal<helix_ast::traversal::OnNodes, ReadOnly> {
    g().n_with_label_where(
        "Group",
        Predicate::and(vec![
            Predicate::eq("name", "g3"),
            Predicate::eq("region", "west"),
        ]),
    )
    .in_(Some("IN_GROUP"))
    .out(Some("HAS_ATTRIBUTE"))
    .where_(predicate)
}

/// Assert the source intersects the `name` and `region` sets with no
/// per-row filter, and answers the post-expansion `predicate` with membership
/// after the expansions, fusing its unindexed conjuncts as `residual`.
fn assert_indexed_source_then_membership(
    plan: &ExecutablePlan,
    predicate: &Predicate,
    residual: Option<&Predicate>,
) {
    assert_eq!(
        intersected_equality_properties(plan),
        Some(vec!["name".to_string(), "region".to_string()]),
        "{:#?}",
        plan.steps()
    );
    let membership = only_membership(plan);
    assert_eq!(membership.label().as_ref(), "Attribute");
    assert_eq!(membership.predicate.predicate(), predicate);
    assert_eq!(
        membership
            .residual
            .as_ref()
            .map(|residual| residual.predicate()),
        residual
    );
    assert!(filter_predicates(plan).is_empty(), "{:#?}", plan.steps());
    let position = |matches: &dyn Fn(&ExecOp) -> bool| {
        plan.steps()
            .iter()
            .position(|step| matches(&step.op))
            .unwrap_or_else(|| panic!("missing step: {:#?}", plan.steps()))
    };
    let expand = position(&|op| ExecOpFamily::Expand.matches(op));
    let membership = position(&|op| matches!(op, ExecOp::IndexMembership { .. }));
    assert!(expand < membership, "{:#?}", plan.steps());
}

#[test]
fn indexed_source_intersection_precedes_membership() {
    // Plain and projected streams reach the source through the access-pipeline
    // and root-stream filter rules. Either way the source reads both indexes
    // and the membership rule answers the post-expansion filter.
    let kind = Predicate::eq("kind", "B");
    for plan in [
        executable_traversal(two_index_attributes_where(kind.clone()), two_index_ctx()),
        executable_traversal(
            two_index_attributes_where(kind.clone()).values(vec!["kind"]),
            two_index_ctx(),
        ),
    ] {
        assert_indexed_source_then_membership(&plan, &kind, None);
    }

    // An unindexed post-expansion conjunct is the membership's fused residual.
    let title = Predicate::contains("title", "x");
    let predicate = Predicate::and(vec![kind, title.clone()]);
    let plan = executable_traversal(
        two_index_attributes_where(predicate.clone()).values(vec!["kind"]),
        two_index_ctx(),
    );
    assert_indexed_source_then_membership(&plan, &predicate, Some(&title));
}

#[test]
fn partial_conjunctions_fuse_their_residual() {
    let title = Predicate::contains("title", "x");
    let predicate = Predicate::and(vec![Predicate::eq("kind", "B"), title.clone()]);
    for planner_ctx in [ctx(membership_indexes()), large_ctx()] {
        let plan = executable_traversal(
            attributes_where(predicate.clone()).values(vec!["kind"]),
            planner_ctx,
        );
        let membership = only_membership(&plan);
        assert_eq!(membership.predicate.predicate(), &predicate);
        assert_eq!(
            membership
                .residual
                .as_ref()
                .map(|residual| residual.predicate()),
            Some(&title)
        );
        assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
    }
}

#[test]
fn partial_conjunctions_behind_a_unique_source_use_membership() {
    // A unique source proves one row and its expansions keep that estimate,
    // yet an indexed conjunct is still decided by its set.
    let mut unique = ctx(membership_indexes());
    unique.indexes.node_eq.insert(
        ScopedPropertyKey::try_new("Group", "name").unwrap(),
        NodeEqualityIndexMeta::try_new("group-name")
            .unwrap()
            .with_uniqueness(IndexUniqueness::Unique),
    );
    let title = Predicate::contains("title", "x");
    for (decided, partial) in [
        (
            Predicate::eq("kind", "B"),
            Predicate::and(vec![Predicate::eq("kind", "B"), title.clone()]),
        ),
        (
            Predicate::and(vec![
                Predicate::eq("$label", "Attribute"),
                Predicate::eq("kind", "B"),
            ]),
            Predicate::and(vec![
                Predicate::eq("$label", "Attribute"),
                Predicate::eq("kind", "B"),
                title.clone(),
            ]),
        ),
    ] {
        let plan = executable_traversal(
            attributes_where(decided.clone()).values(vec!["kind"]),
            unique.clone(),
        );
        assert_eq!(only_membership(&plan).predicate.predicate(), &decided);
        assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());

        let plan = executable_traversal(
            attributes_where(partial.clone()).values(vec!["kind"]),
            unique.clone(),
        );
        let membership = only_membership(&plan);
        assert_eq!(membership.predicate.predicate(), &partial);
        assert_eq!(
            membership
                .residual
                .as_ref()
                .map(|residual| residual.predicate()),
            Some(&title)
        );
        assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
    }
}

#[test]
fn indexed_source_intersection_precedes_membership_under_a_count() {
    for (counted, outside_label) in [
        (
            two_index_attributes_where(Predicate::eq("kind", "B"))
                .has_label("Attribute")
                .count(),
            crate::ir::NodeMembershipOutsideLabel::Reject,
        ),
        (
            two_index_attributes_where(Predicate::eq("kind", "B")).count(),
            crate::ir::NodeMembershipOutsideLabel::Evaluate,
        ),
    ] {
        let plan = executable_traversal(counted, two_index_ctx());
        let counted = plan
            .steps()
            .iter()
            .find_map(|step| match &step.op {
                ExecOp::Count { plan } => Some(plan.as_ref()),
                _ => None,
            })
            .expect("count step");
        let ExecCountPlan::Stream(crate::exec::ExecCountStreamPlan { cursor, .. }) = counted else {
            panic!("expected a streamed count: {counted:#?}");
        };
        // The cursor chain from the count down to its source.
        let chain = core::iter::successors(Some(cursor), |cursor| match cursor {
            crate::exec::ExecCountCursorPlan::IndexMembership { input, .. }
            | crate::exec::ExecCountCursorPlan::Expand { input, .. }
            | crate::exec::ExecCountCursorPlan::Filter { input, .. } => Some(input.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
        assert!(
            matches!(
                chain[..],
                [
                    crate::exec::ExecCountCursorPlan::IndexMembership { plan: membership, .. },
                    crate::exec::ExecCountCursorPlan::Expand { .. },
                    crate::exec::ExecCountCursorPlan::Expand { .. },
                    crate::exec::ExecCountCursorPlan::NodeBitmap(
                        crate::exec::ExecNodeBitmapExpr::Intersect { rest, .. }
                    ),
                ] if membership.label().as_ref() == "Attribute"
                    && membership.outside_label() == outside_label
                    && rest.len() == 1
            ),
            "{counted:#?}"
        );
    }
}

#[test]
fn statistics_never_veto_membership() {
    for group_rows in [Some(300), Some(5_000), None] {
        let planner_ctx = group_rows.map_or(ctx(membership_indexes()), |rows| {
            let mut planner_ctx = ctx(membership_indexes());
            planner_ctx.stats = planner_ctx
                .stats
                .with_node_eq_cardinality(
                    ScopedPropertyKey::try_new("Group", "name").unwrap(),
                    rows,
                )
                .with_node_label_cardinality(NonEmptyString::new("Group").unwrap(), 50_000);
            planner_ctx
        });
        for (predicate, _) in kind_b_policies() {
            let plan = executable_traversal(
                attributes_where(predicate).values(vec!["kind"]),
                planner_ctx.clone(),
            );
            assert_eq!(
                memberships(&plan).len(),
                1,
                "{group_rows:?}: {:#?}",
                plan.steps()
            );
            assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
        }
    }
}

#[test]
fn membership_choice_is_deterministic() {
    let planner_ctx = ctx(membership_indexes().with_vector(
        SearchIndexKey::try_new(ElementKind::Node, "Attribute", "embedding").unwrap(),
        SearchIndexScope::Unscoped,
    ));
    // The optimizer's wall-clock duration is the only non-semantic field.
    let semantic = |plan: &ExecutablePlan| {
        let mut value = serde_json::to_value(plan).unwrap();
        let Some(_) = value["metrics"]
            .as_object_mut()
            .and_then(|metrics| metrics.remove("optimization_micros"))
        else {
            panic!("serialized plan omitted its optimization duration: {value:#}");
        };
        value
    };
    for (predicate, outside_label) in kind_b_policies() {
        let within = || {
            attributes_where(predicate.clone())
                .vector_search("Attribute", "embedding", vec![1.0, 0.0], 50, None)
                .project(vec![Projection::property("$id", "id")])
        };
        let plan = executable_traversal(within(), planner_ctx.clone());
        assert_eq!(only_membership(&plan).outside_label(), outside_label);

        for shape in [
            attributes_where(predicate.clone()).values(vec!["kind"]),
            within(),
            attributes_where(predicate.clone()).count(),
            attributes_where(predicate.clone()).exists(),
        ] {
            let first = executable_traversal(shape.clone(), planner_ctx.clone());
            let second = executable_traversal(shape, planner_ctx.clone());
            assert_eq!(first.steps(), second.steps());
            assert_eq!(semantic(&first), semantic(&second));
        }
    }
}

/// `planner_ctx` with the optional exploration budget exhausted after one rule
/// fire.
fn exhausted_exploration(mut planner_ctx: PlannerContext) -> PlannerContext {
    planner_ctx.optimizer_limits.exploration_rule_fires =
        crate::properties::PositiveUsize::at_least_one(1);
    planner_ctx
}

/// Assert `plan` decides its one post-expansion filter by membership, with
/// `residual` fused, and evaluates no per-row filter.
fn assert_only_membership(plan: &ExecutablePlan, residual: Option<&Predicate>) {
    let membership = only_membership(plan);
    assert_eq!(
        membership
            .residual
            .as_ref()
            .map(|residual| residual.predicate()),
        residual,
        "{:#?}",
        plan.steps()
    );
    assert!(filter_predicates(plan).is_empty(), "{:#?}", plan.steps());
}

#[test]
fn every_indexed_conjunct_count_after_expansion_plans_membership() {
    let indexes = (0..8).fold(membership_indexes(), |indexes, property| {
        indexes
            .with_node_eq(ScopedPropertyKey::try_new("Attribute", format!("p{property}")).unwrap())
    });
    let mut unique = ctx(indexes.clone());
    unique.indexes.node_eq.insert(
        ScopedPropertyKey::try_new("Group", "name").unwrap(),
        NodeEqualityIndexMeta::try_new("group-name")
            .unwrap()
            .with_uniqueness(IndexUniqueness::Unique),
    );
    let mut large = ctx(indexes.clone());
    large.stats = large_ctx().stats;
    let mut huge_label = ctx(indexes.clone());
    huge_label.stats = huge_label
        .stats
        .with_node_label_cardinality(NonEmptyString::new("Attribute").unwrap(), 10_000_000);
    let contexts = [
        ctx(indexes.clone()),
        large,
        unique,
        huge_label,
        exhausted_exploration(ctx(indexes)),
    ];
    let title = Predicate::contains("title", "x");
    for conjuncts in 1..=8 {
        let indexed = (0..conjuncts)
            .map(|property| Predicate::eq(format!("p{property}"), "v"))
            .collect::<Vec<_>>();
        for (predicate, residual) in [
            (Predicate::and(indexed.clone()), None),
            (
                Predicate::and(indexed.into_iter().chain([title.clone()]).collect()),
                Some(&title),
            ),
        ] {
            for planner_ctx in &contexts {
                let label = format!("{conjuncts} {predicate:?}");
                let plan = executable_traversal(
                    attributes_where(predicate.clone()).values(vec!["kind"]),
                    planner_ctx.clone(),
                );
                assert_only_membership(&plan, residual);

                let exists = executable_traversal(
                    attributes_where(predicate.clone()).exists(),
                    planner_ctx.clone(),
                );
                assert_only_membership(&exists, residual);

                let limited = executable_traversal(
                    g().n_with_label_where("Group", Predicate::eq("name", "g3"))
                        .out(Some("HAS_ATTRIBUTE"))
                        .limit(2)
                        .where_(predicate.clone())
                        .values(vec!["kind"]),
                    planner_ctx.clone(),
                );
                assert_only_membership(&limited, residual);

                let count = executable_traversal(
                    attributes_where(predicate.clone()).count(),
                    planner_ctx.clone(),
                );
                let crate::exec::ExecCountCursorPlan::IndexMembership { plan, .. } =
                    count_cursor(&count)
                else {
                    panic!("{label}: expected a membership count cursor: {count:#?}");
                };
                assert_eq!(
                    plan.residual.as_ref().map(|residual| residual.predicate()),
                    residual,
                    "{label}"
                );
            }
        }
    }
}

#[test]
fn every_eligible_filter_in_a_pipeline_becomes_membership() {
    let title = Predicate::contains("title", "x");
    let later = Predicate::and(vec![Predicate::eq("status", "live"), title.clone()]);
    for planner_ctx in [
        ctx(membership_indexes()),
        large_ctx(),
        exhausted_exploration(ctx(membership_indexes())),
    ] {
        let plan = executable_traversal(
            g().n_with_label_where("Group", Predicate::eq("name", "g3"))
                .out(Some("HAS_ATTRIBUTE"))
                .where_(Predicate::eq("kind", "B"))
                .out(Some("LINKS"))
                .where_(later.clone())
                .values(vec!["kind"]),
            planner_ctx,
        );
        let [first, second] = memberships(&plan)[..] else {
            panic!("expected two memberships: {:#?}", plan.steps());
        };
        assert_eq!(first.predicate.predicate(), &Predicate::eq("kind", "B"));
        assert_eq!(first.residual, None);
        assert_eq!(second.predicate.predicate(), &later);
        assert_eq!(
            second
                .residual
                .as_ref()
                .map(|residual| residual.predicate()),
            Some(&title)
        );
        assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
    }
}

#[test]
fn label_less_sources_and_unknown_streams_use_membership() {
    let kind = Predicate::eq("kind", "B");
    let with_items = |result: Traversal<helix_ast::traversal::OnNodes, ReadOnly>| {
        read_batch()
            .var_as("items", g().n_with_label("Attribute"))
            .var_as("result", result.values(vec!["kind"]))
            .returning(["result"])
    };
    // Without an exploration budget a labeled source's own filter may stay
    // per row, but the filters these sources feed never do.
    for (planner_ctx, source_filters) in [
        (ctx(membership_indexes()), 0),
        (exhausted_exploration(ctx(membership_indexes())), 1),
    ] {
        // Point IDs, a parameter and an all-node scan carry no label for an
        // index access, so their leading filter is decided by membership.
        for source in [
            g().n(NodeRef::ids([1u64, 2])),
            g().n(NodeRef::param("ids")),
            g().n(NodeRef::all()),
        ] {
            let plan = executable_traversal(
                source.clone().where_(kind.clone()).values(vec!["kind"]),
                planner_ctx.clone(),
            );
            assert_only_membership(&plan, None);
            let count =
                executable_traversal(source.where_(kind.clone()).count(), planner_ctx.clone());
            assert!(
                matches!(
                    count_cursor(&count),
                    crate::exec::ExecCountCursorPlan::IndexMembership { .. }
                ),
                "{count:#?}"
            );
        }

        // A variable source, a root pipeline over an injected variable, and
        // a stream whose rows a variable read replaced.
        for result in [
            g().n(NodeRef::var("items")).where_(kind.clone()),
            g().inject("items").where_(kind.clone()),
            g().n_with_label_where("Group", Predicate::eq("name", "g3"))
                .out(Some("HAS_ATTRIBUTE"))
                .inject("items")
                .where_(kind.clone()),
            g().n_with_label_where("Group", Predicate::eq("name", "g3"))
                .out(Some("HAS_ATTRIBUTE"))
                .store("seen")
                .out(Some("LINKS"))
                .select("seen")
                .where_(kind.clone()),
        ] {
            let plan = crate::planning::plan_read_batch(&with_items(result), &planner_ctx).unwrap();
            assert_eq!(only_membership(&plan).predicate.predicate(), &kind);
            assert!(
                !filter_predicates(&plan).contains(&&kind),
                "{:#?}",
                plan.steps()
            );
            assert!(filter_predicates(&plan).len() <= source_filters);
        }
    }
}

#[test]
fn label_only_filters_after_expansion_use_label_bitmaps() {
    let expanded = || {
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out(Some("HAS_ATTRIBUTE"))
    };
    let labels = |plan: &ExecutablePlan| {
        let crate::exec::ExecNodeMembershipSet::Labels(labels) = &only_membership(plan).set else {
            panic!("expected a label membership: {:#?}", plan.steps());
        };
        labels
            .iter()
            .map(|label| label.as_ref().to_owned())
            .collect::<Vec<_>>()
    };
    let title = Predicate::contains("title", "x");
    let domain = Predicate::is_in(
        "$label",
        PropertyValue::StringArray(vec!["Attribute".to_owned(), "Note".to_owned()]),
    );
    for planner_ctx in [
        ctx(membership_indexes()),
        large_ctx(),
        exhausted_exploration(ctx(membership_indexes())),
    ] {
        for (traversal, expected, residual) in [
            (expanded().has_label("Note"), vec!["Note"], None),
            (
                expanded().where_(Predicate::eq("$label", "Note")),
                vec!["Note"],
                None,
            ),
            (
                expanded().where_(domain.clone()),
                vec!["Attribute", "Note"],
                None,
            ),
            (
                expanded().where_(Predicate::and(vec![
                    Predicate::eq("$label", "Note"),
                    title.clone(),
                ])),
                vec!["Note"],
                Some(&title),
            ),
        ] {
            let plan = executable_traversal(traversal.values(vec!["kind"]), planner_ctx.clone());
            assert_eq!(labels(&plan), expected, "{:#?}", plan.steps());
            assert_only_membership(&plan, residual);
        }
    }

    // A label domain wider than the union branch limit keeps the filter, the
    // same bound a label-domain access path obeys.
    let mut limited = ctx(membership_indexes());
    limited.limits.max_index_union_branches = IndexUnionBranchLimit::limited(1).unwrap();
    let plan = executable_traversal(
        expanded().where_(domain.clone()).values(vec!["kind"]),
        limited,
    );
    assert!(memberships(&plan).is_empty(), "{:#?}", plan.steps());
    assert_eq!(filter_predicates(&plan), [&domain]);

    // Edge streams keep the per-row filter.
    let edges = executable_traversal(
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out_e(None::<String>)
            .has_label("HAS_ATTRIBUTE")
            .values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert!(memberships(&edges).is_empty(), "{:#?}", edges.steps());
    assert_eq!(filter_predicates(&edges).len(), 1, "{:#?}", edges.steps());
}

#[test]
fn membership_survives_an_exhausted_exploration_budget() {
    let exhausted = exhausted_exploration(ctx(membership_indexes()));
    for (predicate, outside_label) in kind_b_policies() {
        let plan = executable_traversal(
            attributes_where(predicate.clone()).values(vec!["kind"]),
            exhausted.clone(),
        );
        assert_eq!(only_membership(&plan).outside_label(), outside_label);
        assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());

        // Every branch body decides its post-expansion filter by membership.
        let group = || g().n_with_label_where("Group", Predicate::eq("name", "g3"));
        let arm = || sub().out(Some("HAS_ATTRIBUTE")).where_(predicate.clone());
        for (traversal, bodies) in [
            (group().optional(arm()).values(vec!["kind"]), 1),
            (group().union(vec![arm(), arm()]).values(vec!["kind"]), 2),
        ] {
            let plan = executable_traversal(traversal, exhausted.clone());
            let memberships = branch_memberships(&plan);
            assert_eq!(memberships.len(), bodies, "{:#?}", plan.steps());
            assert!(memberships
                .iter()
                .all(|membership| membership.outside_label() == outside_label));
        }
    }
}

/// Memberships inside the optional and union branch bodies of `plan`.
fn branch_memberships(plan: &ExecutablePlan) -> Vec<&crate::exec::ExecNodeIndexMembershipPlan> {
    plan.steps()
        .iter()
        .flat_map(|step| match &step.op {
            ExecOp::Branch {
                plan: ExecBranchPlan::Optional(body),
            } => vec![body.as_ref()],
            ExecOp::Branch {
                plan: ExecBranchPlan::Union(bodies),
            } => bodies.iter().collect(),
            _ => Vec::new(),
        })
        .flat_map(|body| body.steps())
        .filter_map(|step| match &step.op {
            ExecOp::IndexMembership { plan } => Some(plan.as_ref()),
            _ => None,
        })
        .collect()
}
