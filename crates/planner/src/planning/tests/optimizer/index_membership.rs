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
/// the source stays an index read and the expanded stream is large enough to
/// pay for reading the membership bitmaps. Statistics-backed large streams
/// still choose membership past one record batch.
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
                crate::diagnostics::analyze(&plan, &planner_ctx)
                    .statistics
                    .residual_filters,
                0
            );
        }
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
        large_ctx(),
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
        &only_membership(&is_in).set,
        crate::exec::ExecNodeSecondarySetPlan::Bitmap(
            crate::exec::ExecNodeBitmapExpr::BatchedUnionRead { values, .. }
        ) if values.len() == 2
    ));

    // A range set would verify every in-range record of the label, so range
    // conjuncts keep the per-row filter, alone or behind an equality set.
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
        only_membership(&ranged).predicate.predicate(),
        &Predicate::eq("kind", "B")
    );
    assert_eq!(filter_predicates(&ranged), [&Predicate::gte("rank", 3)]);

    let both = executable_traversal(
        attributes_where(Predicate::and(vec![
            Predicate::eq("kind", "B"),
            Predicate::eq("status", "live"),
        ]))
        .values(vec!["kind"]),
        large_ctx(),
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
        large_ctx(),
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
        &only_membership(&equality).set,
        crate::exec::ExecNodeSecondarySetPlan::DynamicEquality { param, .. }
            if param.as_ref() == "kind"
    ));

    let set = executable_traversal(
        attributes_where(Predicate::is_in_param("kind", "kinds")).values(vec!["kind"]),
        planner_ctx.clone(),
    );
    assert!(matches!(
        &only_membership(&set).set,
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
fn tiny_bounded_inputs_keep_the_per_row_filter() {
    let expanded = || {
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out(Some("HAS_ATTRIBUTE"))
    };
    for planner_ctx in [ctx(membership_indexes()), large_ctx()] {
        for (predicate, _) in kind_b_policies() {
            for bounded in [expanded().limit(2), expanded().range(0usize, 3usize)] {
                let plan = executable_traversal(
                    bounded.where_(predicate.clone()).values(vec!["kind"]),
                    planner_ctx.clone(),
                );

                assert!(memberships(&plan).is_empty(), "{:#?}", plan.steps());
                assert_eq!(filter_predicates(&plan), [&predicate]);
            }
        }
    }
}

#[test]
fn point_sources_plan_membership_without_statistics() {
    // A stream of at most one record batch reads no set at runtime, so the
    // membership only matches the filter's work there and stops reading
    // records if the unbounded stream outgrows its estimate.
    for (predicate, outside_label) in kind_b_policies() {
        let plan = executable_traversal(
            attributes_where(predicate).values(vec!["kind"]),
            ctx(membership_indexes()),
        );
        let membership = only_membership(&plan);
        assert_eq!(membership.outside_label, outside_label);
        assert_eq!(membership.label.as_ref(), "Attribute");
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
        &only_membership(&both).set,
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
        &only_membership(&is_in).set,
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
        &only_membership(&equality).set,
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
    assert_eq!(only_membership(&one_hop).label.as_ref(), "Attribute");
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
    assert_eq!(only_membership(&plan).label.as_ref(), "Attribute");
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
        assert_eq!(only_membership(&limited).outside_label, outside_label);
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
        assert_eq!(only_membership(&exists).outside_label, outside_label);
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
        assert_eq!(plan.outside_label, outside_label);

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
        assert_eq!(only_membership(&plan).outside_label, outside_label);
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
            assert_eq!(plan.outside_label, outside_label);

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
            assert_eq!(membership.label.as_ref(), "Attribute");
            assert_eq!(membership.outside_label, outside_label);
            assert!(filter_predicates(&plan).is_empty(), "{:#?}", plan.steps());
        }
    }
}

#[test]
fn unscoped_membership_needs_exactly_one_indexed_label() {
    // Without statistics a label scan keeps the unknown-scan estimate, past
    // one record batch, where both policies amortize their bitmap reads.
    let label_scan = |predicate: Predicate| {
        g().n_with_label("Group")
            .in_(Some("IN_GROUP"))
            .out(Some("HAS_ATTRIBUTE"))
            .where_(predicate)
            .values(vec!["kind"])
    };
    for (predicate, outside_label) in kind_b_policies() {
        let plan = executable_traversal(label_scan(predicate), ctx(membership_indexes()));
        assert_eq!(only_membership(&plan).outside_label, outside_label);
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
        assert_eq!(only_membership(&plan).label.as_ref(), "Note");
    }
}

#[test]
fn proven_bounds_decide_at_the_bound() {
    let expanded = || {
        g().n_with_label_where("Group", Predicate::eq("name", "g3"))
            .out(Some("HAS_ATTRIBUTE"))
    };
    for (predicate, outside_label) in kind_b_policies() {
        // Exactly one batch never reads the set. Just past it the profile
        // charges the set read above the record reads it saves.
        for count in [256, 257] {
            let plan = executable_traversal(
                expanded()
                    .limit(count)
                    .where_(predicate.clone())
                    .values(vec!["kind"]),
                ctx(membership_indexes()),
            );
            assert!(
                memberships(&plan).is_empty(),
                "{count}: {:#?}",
                plan.steps()
            );
            assert_eq!(filter_predicates(&plan), [&predicate]);
        }

        // A proven bound becomes the estimate, and a large one amortizes the
        // set read.
        let plan = executable_traversal(
            expanded()
                .limit(100_000)
                .where_(predicate.clone())
                .values(vec!["kind"]),
            ctx(membership_indexes()),
        );
        assert_eq!(only_membership(&plan).outside_label, outside_label);

        // A limit after the filter leaves the filter's input unbounded.
        let plan = executable_traversal(
            expanded()
                .where_(predicate.clone())
                .limit(2)
                .values(vec!["kind"]),
            ctx(membership_indexes()),
        );
        assert_eq!(only_membership(&plan).outside_label, outside_label);
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

    // Within one batch a membership on `kind` plus the residual range filter
    // would read the kept records twice, so the whole conjunction stays one
    // per-row filter. Past one batch the range conjunct becomes the residual
    // behind the membership.
    let conjunction = Predicate::and(vec![Predicate::eq("kind", "B"), Predicate::gte("rank", 3)]);
    let ranged = executable_traversal(
        attributes_where(conjunction.clone()).values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert!(memberships(&ranged).is_empty(), "{:#?}", ranged.steps());
    assert_eq!(filter_predicates(&ranged), [&conjunction]);
    let ranged = executable_traversal(
        attributes_where(conjunction).values(vec!["kind"]),
        large_ctx(),
    );
    assert_eq!(
        only_membership(&ranged).predicate.predicate(),
        &Predicate::eq("kind", "B")
    );
    assert_eq!(filter_predicates(&ranged), [&Predicate::gte("rank", 3)]);
    let membership_position = ranged
        .steps()
        .iter()
        .position(|step| matches!(step.op, ExecOp::IndexMembership { .. }))
        .unwrap();
    let filter_position = ranged
        .steps()
        .iter()
        .position(|step| matches!(step.op, ExecOp::Filter { .. }))
        .unwrap();
    assert!(membership_position < filter_position);

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
    assert_eq!(only_membership(&plan).label.as_ref(), "Note");
}

/// `large_ctx` with a second, broad `Group` equality index. `name = g3`
/// still selects 5,000 of 5,000,000 groups while `region = west` selects
/// 4,900,000, so reading the region bitmap costs far more than verifying
/// the seed's candidates: the source keeps `name` as its only index seed
/// and evaluates `region` as a per-row residual filter.
fn seeded_ctx() -> PlannerContext {
    let mut seeded =
        ctx(membership_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("Group", "region").unwrap()));
    seeded.stats = seeded
        .stats
        .with_node_eq_cardinality(ScopedPropertyKey::try_new("Group", "name").unwrap(), 5_000)
        .with_node_eq_cardinality(
            ScopedPropertyKey::try_new("Group", "region").unwrap(),
            4_900_000,
        )
        .with_node_label_cardinality(NonEmptyString::new("Group").unwrap(), 5_000_000);
    seeded
}

/// Attributes behind the groups named `g3` in region `west`.
fn seeded_attributes_where(
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

/// Assert the plan reads the `name` seed, keeps `region` as a per-row filter
/// ahead of every expansion, and answers the post-expansion `kind` equality
/// with membership after the expansions.
fn assert_seed_residual_then_membership(plan: &ExecutablePlan) {
    assert!(
        matches!(
            unwrapped_first_exec_access(plan),
            ExecAccessPlan::Node(ExecNodeAccessPlan::Bitmap {
                bitmap: crate::exec::ExecNodeBitmapExpr::PointRead { key, .. },
            }) if key.label == "Group" && key.property == "name"
        ),
        "{:#?}",
        plan.steps()
    );
    let membership = only_membership(plan);
    assert_eq!(membership.label.as_ref(), "Attribute");
    assert_eq!(
        membership.predicate.predicate(),
        &Predicate::eq("kind", "B")
    );
    let position = |matches: &dyn Fn(&ExecOp) -> bool| {
        plan.steps()
            .iter()
            .position(|step| matches(&step.op))
            .unwrap_or_else(|| panic!("missing step: {:#?}", plan.steps()))
    };
    // A seed keeps every other conjunct inside one residual conjunction.
    let residual = position(&|op| {
        matches!(op, ExecOp::Filter { predicate }
            if predicate.predicate() == &Predicate::and(vec![Predicate::eq("region", "west")]))
    });
    let expand = position(&|op| ExecOpFamily::Expand.matches(op));
    let membership = position(&|op| matches!(op, ExecOp::IndexMembership { .. }));
    assert!(
        residual < expand && expand < membership,
        "{:#?}",
        plan.steps()
    );
}

#[test]
fn equality_seed_residual_stays_a_filter_while_later_filters_use_membership() {
    // Plain and projected streams reach the seed through the access-pipeline
    // and root-stream filter rules. Either way the membership rule starts
    // after the leading residual, so the `Group` region index is never read.
    let residual = Predicate::and(vec![Predicate::eq("region", "west")]);
    for plan in [
        executable_traversal(
            seeded_attributes_where(Predicate::eq("kind", "B")),
            seeded_ctx(),
        ),
        executable_traversal(
            seeded_attributes_where(Predicate::eq("kind", "B")).values(vec!["kind"]),
            seeded_ctx(),
        ),
    ] {
        assert_seed_residual_then_membership(&plan);
        assert_eq!(filter_predicates(&plan), [&residual]);
    }

    // An unindexed post-expansion conjunct stays a residual behind the
    // membership, separate from the seed's residual.
    let plan = executable_traversal(
        seeded_attributes_where(Predicate::and(vec![
            Predicate::eq("kind", "B"),
            Predicate::contains("title", "x"),
        ]))
        .values(vec!["kind"]),
        seeded_ctx(),
    );
    assert_seed_residual_then_membership(&plan);
    assert_eq!(
        filter_predicates(&plan),
        [&residual, &Predicate::contains("title", "x")]
    );
}

#[test]
fn partial_conjunctions_within_one_batch_keep_the_filter() {
    // Membership plus its residual would read the kept records twice within
    // one batch, so the whole predicate stays one per-row filter.
    let predicate = Predicate::and(vec![
        Predicate::eq("kind", "B"),
        Predicate::contains("title", "x"),
    ]);
    let plan = executable_traversal(
        attributes_where(predicate.clone()).values(vec!["kind"]),
        ctx(membership_indexes()),
    );
    assert!(memberships(&plan).is_empty(), "{:#?}", plan.steps());
    assert_eq!(filter_predicates(&plan), [&predicate]);

    let plan = executable_traversal(
        attributes_where(predicate).values(vec!["kind"]),
        large_ctx(),
    );
    assert_eq!(
        only_membership(&plan).predicate.predicate(),
        &Predicate::eq("kind", "B")
    );
    assert_eq!(
        filter_predicates(&plan),
        [&Predicate::contains("title", "x")]
    );
}

#[test]
fn partial_conjunctions_behind_a_unique_source_keep_the_filter() {
    // A unique source proves one row and its expansions keep that estimate.
    // Every leaf of a predicate is priced on both sides of the choice, so at
    // one row the residual's second record read is the only difference, and
    // the membership's one-leaf credit never outweighs it.
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
        // Membership alone still wins by its credit, whatever its leaf count.
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
        assert!(memberships(&plan).is_empty(), "{:#?}", plan.steps());
        assert_eq!(filter_predicates(&plan), [&partial]);
    }
}

#[test]
fn equality_seed_residual_stays_a_filter_under_a_count_with_membership() {
    // Count cursors price their operators at the unknown-input default, past
    // one record batch, where both the label-scoped and the unscoped predicate
    // pay for their set reads.
    for (counted, outside_label) in [
        (
            seeded_attributes_where(Predicate::eq("kind", "B"))
                .has_label("Attribute")
                .count(),
            crate::ir::NodeMembershipOutsideLabel::Reject,
        ),
        (
            seeded_attributes_where(Predicate::eq("kind", "B")).count(),
            crate::ir::NodeMembershipOutsideLabel::Evaluate,
        ),
    ] {
        let plan = executable_traversal(counted, seeded_ctx());
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
                    crate::exec::ExecCountCursorPlan::Filter { predicate, .. },
                    crate::exec::ExecCountCursorPlan::NodeBitmap(
                        crate::exec::ExecNodeBitmapExpr::PointRead { key, .. }
                    ),
                ] if membership.label.as_ref() == "Attribute"
                    && membership.outside_label == outside_label
                    && predicate.predicate()
                        == &Predicate::and(vec![Predicate::eq("region", "west")])
                    && key.property == "name"
            ),
            "{counted:#?}"
        );
    }
}

#[test]
fn statistics_price_membership_by_what_the_runtime_reads() {
    // 300 rows past one batch pay the set reads (6,400 or 5,360 us) above the
    // filter's 3,300 or 3,600 us (one or two predicate leaves); 5,000 rows
    // amortize them; no statistics keeps the unbounded point-source estimate
    // within one batch.
    for (group_rows, chooses_membership) in [(Some(300), false), (Some(5_000), true), (None, true)]
    {
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
                usize::from(chooses_membership),
                "{group_rows:?}: {:#?}",
                plan.steps()
            );
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
        assert_eq!(only_membership(&plan).outside_label, outside_label);

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
