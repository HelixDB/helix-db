use crate::planning::tests::support::*;

#[test]
fn ordered_range_queries_use_reverse_range_and_runtime_limit() {
    let indexes = IndexCatalogSnapshot::default()
        .with_node_eq(ScopedPropertyKey::try_new("Resource", "tenant").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("Resource", "type").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("Resource", "last_seen", RangeIndexDirection::Asc)
                .unwrap(),
        );
    for request in [
        include_str!(
            "../../../../../../docker-image/tests/fixtures/ordered-range-wide-projection.json"
        ),
        include_str!(
            "../../../../../../docker-image/tests/fixtures/ordered-range-narrow-projection.json"
        ),
    ] {
        let json: serde_json::Value = serde_json::from_str(request).unwrap();
        let root: AstNode =
            serde_json::from_value(json["query"]["read"]["entries"][0]["query"]["root"].clone())
                .unwrap();
        for stats in [
            StatsSnapshot::default(),
            StatsSnapshot::default()
                .with_node_label_cardinality(NonEmptyString::new("Resource").unwrap(), 100_000)
                .with_node_eq_cardinality(
                    ScopedPropertyKey::try_new("Resource", "tenant").unwrap(),
                    20_000,
                )
                .with_node_eq_cardinality(
                    ScopedPropertyKey::try_new("Resource", "type").unwrap(),
                    10_000,
                )
                .with_node_range_cardinality(
                    ScopedPropertyDirectionKey::try_new(
                        "Resource",
                        "last_seen",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                    100_000,
                ),
        ] {
            let context = PlannerContext {
                stats,
                ..ctx(indexes.clone())
            };
            let plan = executable_ast(root.clone(), context.clone());
            let diagnostics = crate::diagnostics::analyze(&plan, &context);
            assert_eq!(
                diagnostics
                    .statistics
                    .node_accesses
                    .reverse_range_index_scans,
                1,
                "{plan:#?}"
            );
            assert_eq!(
                diagnostics
                    .statistics
                    .node_accesses
                    .dynamic_bounded_accesses,
                1
            );
            assert_no_exec_op_family(&plan, ExecOpFamily::Order);
            let access = first_exec_access(&plan);
            assert!(
                matches!(access, ExecAccessPlan::Limited(limited) if matches!(limited.limit(), crate::exec::ExecAccessLimit::Dynamic(_))),
                "{access:#?}"
            );
            assert!(
                matches!(unwrapped_first_exec_access(&plan), ExecAccessPlan::Node(ExecNodeAccessPlan::SecondarySet { set: crate::exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, filters } })
            if driver.key.property == "last_seen" && driver.key.direction == RangeIndexDirection::Asc
                && driver.iteration == crate::ir::RangeScanIteration::Reverse && filters.len() == 2),
                "{plan:#?}"
            );
        }
    }
}

#[test]
fn ordered_access_preserves_nested_runtime_validation_even_with_zero() {
    let indexes = IndexCatalogSnapshot::default().with_node_range(
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
    );
    for outer in [
        StreamBound::Literal(0),
        StreamBound::Literal(10),
        StreamBound::expr(Expr::param("outer")),
    ] {
        let plan = executable_traversal(
            g().n_with_label_where("User", Predicate::gt("age", 0))
                .order_by("age", Order::Desc)
                .limit(StreamBound::expr(Expr::param("inner")))
                .limit(outer),
            ctx(indexes.clone()),
        );
        let mut source = first_exec_access(&plan);
        let mut dynamic = 0;
        while let ExecAccessPlan::Limited(limited) = source {
            dynamic += usize::from(matches!(
                limited.limit(),
                crate::exec::ExecAccessLimit::Dynamic(_)
            ));
            source = limited.source();
        }
        assert!(dynamic >= 1, "inner runtime bound must remain: {plan:#?}");
    }
}

#[test]
fn ordered_edge_intersection_pushes_dynamic_limit_for_both_physical_directions() {
    for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
        let indexes = IndexCatalogSnapshot::default()
            .with_edge_eq(ScopedPropertyKey::try_new("LINK", "tenant").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("LINK", "last_seen", direction).unwrap(),
            );
        let context = ctx(indexes);
        let plan = executable_traversal(
            g().e_with_label_where(
                "LINK",
                Predicate::and(vec![
                    Predicate::eq("tenant", "one"),
                    Predicate::gt("last_seen", 0),
                ]),
            )
            .order_by("last_seen", Order::Desc)
            .limit(StreamBound::expr(Expr::param("limit"))),
            context.clone(),
        );
        assert_no_exec_op_family(&plan, ExecOpFamily::Order);
        let diagnostics = crate::diagnostics::analyze(&plan, &context);
        assert_eq!(
            diagnostics
                .statistics
                .edge_accesses
                .dynamic_bounded_accesses,
            1
        );
        assert_eq!(
            diagnostics
                .statistics
                .edge_accesses
                .reverse_range_index_scans,
            usize::from(direction == RangeIndexDirection::Asc)
        );
    }
}
