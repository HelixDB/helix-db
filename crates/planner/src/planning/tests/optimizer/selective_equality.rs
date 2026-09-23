//! End-to-end coverage of empty-statistics equality plan selection.

use crate::planning::tests::support::*;

#[test]
fn unique_membership_keeps_bounded_index_access_without_statistics() {
    for count in [1, 2, 3, 4, 5, 8, 16, 64] {
        for label_rows in [None, Some(1_000), Some(100_000)] {
            let mut context = PlannerContext::default();
            context.indexes.node_eq.insert(
                ScopedPropertyKey::try_new("Fixture", "external_key").unwrap(),
                crate::catalog::NodeEqualityIndexMeta::try_new("fixture-key")
                    .unwrap()
                    .with_uniqueness(crate::catalog::IndexUniqueness::Unique),
            );
            if let Some(rows) = label_rows {
                context.stats = context
                    .stats
                    .with_node_label_cardinality(NonEmptyString::new("Fixture").unwrap(), rows);
            }
            for parameterized in [false, true] {
                for nested in [false, true] {
                    let values = PropertyValue::StringArray(
                        (0..count)
                            .chain(0..1)
                            .map(|n| format!("value-{n}"))
                            .collect(),
                    );
                    let predicate = if parameterized {
                        context.params = ParamBindings::default()
                            .with_value(NonEmptyString::new("keys").unwrap(), values);
                        Predicate::is_in_param("external_key", "keys")
                    } else {
                        Predicate::is_in("external_key", values)
                    };
                    let source = if nested {
                        g().n_where(Predicate::and(vec![
                            Predicate::eq("$label", "Fixture"),
                            Predicate::and(vec![predicate]),
                        ]))
                    } else {
                        g().n_with_label_where("Fixture", predicate)
                    };
                    let plan =
                        executable_traversal(source.values(vec!["external_key"]), context.clone());
                    let diagnostics = crate::diagnostics::analyze(&plan, &context);
                    assert_eq!(diagnostics.statistics.node_accesses.label_scans, 0,
                        "count={count}, label_rows={label_rows:?}, parameterized={parameterized}, nested={nested}");
                    assert_eq!(
                        diagnostics.statistics.node_accesses.equality_index_lookups,
                        count
                    );
                    if count > 1 {
                        assert_eq!(plan.metrics().selected_cost.multi_get_calls, 1);
                        assert_eq!(
                            plan.metrics().selected_cost.authoritative_graph_reads,
                            count as u64
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn selective_equality_type_union_preserves_unindexed_residuals() {
    for indexed_property in ["tenant", "type"] {
        let key = ScopedPropertyKey::try_new("Resource", indexed_property).unwrap();
        let context = ctx(IndexCatalogSnapshot::default()
            .with_node_eq(key.clone())
            .with_edge_eq(key));
        let predicate = Predicate::and(vec![
            Predicate::eq("tenant", "one"),
            Predicate::or(vec![
                Predicate::eq("type", "pod"),
                Predicate::eq("type", "service"),
            ]),
        ]);
        for traversal in [
            g().n_with_label_where("Resource", predicate.clone())
                .values(vec!["id"]),
            g().e_with_label_where("Resource", predicate)
                .values(vec!["id"]),
        ] {
            let plan = executable_traversal(traversal, context.clone());
            assert!(has_exec_op_family(&plan, ExecOpFamily::Filter));
            assert_eq!(
                plan.metrics().selected_cost.object_reads,
                if indexed_property == "tenant" { 1 } else { 2 }
            );
            assert_eq!(plan.metrics().selected_cost.authoritative_graph_reads, 0);
        }
    }
}

#[test]
fn unique_membership_respects_union_limits_and_small_label_costs() {
    let mut context = PlannerContext::default();
    context.indexes.node_eq.insert(
        ScopedPropertyKey::try_new("Fixture", "key").unwrap(),
        crate::catalog::NodeEqualityIndexMeta::try_new("fixture-key")
            .unwrap()
            .with_uniqueness(crate::catalog::IndexUniqueness::Unique),
    );
    for (count, known_rows) in [(65, None), (64, Some(1))] {
        context.stats = known_rows.map_or_else(StatsSnapshot::default, |rows| {
            StatsSnapshot::default()
                .with_node_label_cardinality(NonEmptyString::new("Fixture").unwrap(), rows)
        });
        let plan = executable_traversal(
            g().n_with_label_where(
                "Fixture",
                Predicate::is_in("key", PropertyValue::I64Array((0..count).collect())),
            )
            .values(vec!["key"]),
            context.clone(),
        );
        assert_eq!(
            crate::diagnostics::analyze(&plan, &context)
                .statistics
                .node_accesses
                .label_scans,
            1
        );
    }
}

#[test]
fn selective_equality_type_union_keeps_the_tenant_intersection() {
    let indexes = ["tenant", "type"].into_iter().fold(
        IndexCatalogSnapshot::default(),
        |indexes, property| {
            let key = ScopedPropertyKey::try_new("Resource", property).unwrap();
            indexes.with_node_eq(key.clone()).with_edge_eq(key)
        },
    );
    for type_count in [2, 3, 8] {
        for parameterized in [false, true] {
            let mut params = ParamBindings::default().with_value(
                NonEmptyString::new("tenant").unwrap(),
                PropertyValue::from("one"),
            );
            let alternatives = (0..type_count)
                .map(|index| {
                    let value = format!("workload-{index}");
                    let parameter = format!("type-{index}");
                    params = params.clone().with_value(
                        NonEmptyString::new(&parameter).unwrap(),
                        PropertyValue::from(value.clone()),
                    );
                    if parameterized {
                        Predicate::eq_param("type", parameter)
                    } else {
                        Predicate::eq("type", value)
                    }
                })
                .collect::<Vec<_>>();
            let context = PlannerContext {
                params,
                ..ctx(indexes.clone())
            };
            for union_first in [false, true] {
                let tenant = if parameterized {
                    Predicate::eq_param("tenant", "tenant")
                } else {
                    Predicate::eq("tenant", "one")
                };
                let types = Predicate::or(alternatives.clone());
                let predicate = Predicate::and(if union_first {
                    vec![types, tenant]
                } else {
                    vec![tenant, types]
                });
                for traversal in [
                    g().n_with_label_where("Resource", predicate.clone())
                        .values(vec!["id"]),
                    g().e_with_label_where("Resource", predicate)
                        .values(vec!["id"]),
                ] {
                    let plan = executable_traversal(traversal, context.clone());
                    assert!(
                        matches!(
                            first_exec_access(&plan),
                            ExecAccessPlan::Node(ExecNodeAccessPlan::SecondarySet {
                                set: crate::exec::ExecNodeSecondarySetPlan::Intersect { .. }
                            }) | ExecAccessPlan::Edge(ExecEdgeAccessPlan::SecondarySet {
                                set: crate::exec::ExecEdgeSecondarySetPlan::Intersect { .. }
                            })
                        ),
                        "type_count={type_count}, parameterized={parameterized}: {:#?}",
                        plan.steps()
                    );
                    assert_no_exec_op_family(&plan, ExecOpFamily::Filter);
                    assert_eq!(plan.metrics().selected_cost.range_nexts, 0);
                    assert_eq!(plan.metrics().selected_cost.object_reads, type_count + 1);
                    assert_eq!(plan.metrics().selected_cost.multi_get_calls, 1);
                    let diagnostics = crate::diagnostics::analyze(&plan, &context);
                    assert!(diagnostics.insights.iter().all(|insight| !matches!(
                        insight,
                        crate::diagnostics::PlannerInsight::UnboundedScan(_)
                    )));
                }
            }
        }
    }
}

#[test]
fn selective_equality_uses_three_bitmaps_with_empty_or_populated_statistics() {
    let indexes = ["tenant", "type", "deleted"].into_iter().fold(
        IndexCatalogSnapshot::default(),
        |indexes, property| {
            let key = ScopedPropertyKey::try_new("Resource", property).unwrap();
            indexes.with_node_eq(key.clone()).with_edge_eq(key)
        },
    );
    // An estimate of zero is not a proof of emptiness: stale statistics must
    // never suppress the actual lookup. Include rare and unknown matches.
    for deletion_rows in [None, Some(0), Some(1), Some(10)] {
        let stats = deletion_rows.map_or_else(StatsSnapshot::default, |rows| {
            let mut stats = StatsSnapshot::default();
            stats = stats
                .with_node_label_cardinality(NonEmptyString::new("Resource").unwrap(), 100_000)
                .with_edge_label_cardinality(NonEmptyString::new("Resource").unwrap(), 100_000);
            for (property, count) in [("tenant", 1000), ("type", 2000), ("deleted", rows)] {
                let key = ScopedPropertyKey::try_new("Resource", property).unwrap();
                stats = stats
                    .with_node_eq_cardinality(key.clone(), count)
                    .with_edge_eq_cardinality(key, count);
            }
            stats
        });
        for parameterized in [false, true] {
            for nested in [false, true] {
                let terms = vec![
                    Predicate::eq("tenant", "one"),
                    Predicate::eq("type", "pod"),
                    if parameterized {
                        Predicate::eq_param("deleted", "deleted")
                    } else {
                        Predicate::eq("deleted", true)
                    },
                ];
                let predicate = if nested {
                    Predicate::and(vec![Predicate::and(terms)])
                } else {
                    Predicate::and(terms)
                };
                let context = PlannerContext {
                    stats: stats.clone(),
                    params: ParamBindings::default().with_value(
                        NonEmptyString::new("deleted").unwrap(),
                        PropertyValue::Bool(true),
                    ),
                    ..ctx(indexes.clone())
                };
                for traversal in [
                    g().n_with_label_where("Resource", predicate.clone())
                        .values(vec!["id"]),
                    g().e_with_label_where("Resource", predicate)
                        .values(vec!["id"]),
                ] {
                    let plan = executable_traversal(traversal, context.clone());
                    assert!(
                        matches!(first_exec_access(&plan),
                            ExecAccessPlan::Node(ExecNodeAccessPlan::SecondarySet { set: crate::exec::ExecNodeSecondarySetPlan::Intersect { rest, .. } }) if rest.len() == 2
                        ) || matches!(first_exec_access(&plan),
                            ExecAccessPlan::Edge(ExecEdgeAccessPlan::SecondarySet { set: crate::exec::ExecEdgeSecondarySetPlan::Intersect { rest, .. } }) if rest.len() == 2
                        ),
                        "{deletion_rows:?}, parameterized={parameterized}, nested={nested}: {:#?}",
                        plan.steps()
                    );
                    assert_no_exec_op_family(&plan, ExecOpFamily::Filter);
                    assert_eq!(plan.metrics().selected_cost.object_reads, 3);
                    assert_eq!(plan.metrics().selected_cost.range_nexts, 0);
                    assert_eq!(plan.metrics().selected_cost.parallel_width, 1);
                    if deletion_rows.is_none() {
                        // Full selected plan: serial memberships plus projection.
                        assert_eq!(plan.metrics().selected_cost.latency.as_micros(), 16_220);
                    }
                    let diagnostics = crate::diagnostics::analyze(&plan, &context);
                    assert!(diagnostics.insights.iter().all(|insight| !matches!(
                        insight,
                        crate::diagnostics::PlannerInsight::UnboundedScan(_)
                    )));
                }
            }
        }
    }
}

#[test]
fn selective_equality_costing_still_allows_measurably_small_label_scans() {
    let mut context = PlannerContext::default();
    for property in ["tenant", "type", "deleted"] {
        let key = ScopedPropertyKey::try_new("Resource", property).unwrap();
        context.indexes = context
            .indexes
            .with_node_eq(key.clone())
            .with_edge_eq(key.clone());
        context.stats = context
            .stats
            .with_node_eq_cardinality(key.clone(), 1)
            .with_edge_eq_cardinality(key, 1);
    }
    context.stats = context
        .stats
        .with_node_label_cardinality(NonEmptyString::new("Resource").unwrap(), 1)
        .with_edge_label_cardinality(NonEmptyString::new("Resource").unwrap(), 1);
    let predicate = Predicate::and(vec![
        Predicate::eq("tenant", "one"),
        Predicate::eq("type", "pod"),
        Predicate::eq("deleted", true),
    ]);
    for traversal in [
        g().n_with_label_where("Resource", predicate.clone())
            .values(vec!["id"]),
        g().e_with_label_where("Resource", predicate)
            .values(vec!["id"]),
    ] {
        let plan = executable_traversal(traversal, context.clone());
        assert!(
            matches!(
                first_exec_access(&plan),
                ExecAccessPlan::Node(ExecNodeAccessPlan::LabelScan { .. })
                    | ExecAccessPlan::Edge(ExecEdgeAccessPlan::LabelScan { .. })
            ),
            "{:#?}",
            plan.steps()
        );
        assert_eq!(plan.metrics().selected_cost.authoritative_graph_reads, 1);
        assert!(has_exec_op_family(&plan, ExecOpFamily::Filter));
    }
}

#[test]
fn indexed_conjunction_main_head_probe() {
    let properties = ["kind", "name", "namespace", "group_id", "tenant_id"];
    let indexes =
        properties
            .into_iter()
            .fold(IndexCatalogSnapshot::default(), |indexes, property| {
                indexes.with_node_eq(ScopedPropertyKey::try_new("Fixture", property).unwrap())
            });
    let mut unbounded_cases = Vec::new();
    for count in [1, 2, 3, 4, 5] {
        for populated_stats in [false, true] {
            for parameterized in [false, true] {
                for nested in [false, true] {
                    let mut context = ctx(indexes.clone());
                    if populated_stats {
                        context.stats = context.stats.with_node_label_cardinality(
                            NonEmptyString::new("Fixture").unwrap(),
                            100_000,
                        );
                        for (property, rows) in properties
                            .into_iter()
                            .zip([3_000, 1, 5_000, 10_000, 50_000])
                        {
                            context.stats = context.stats.with_node_eq_cardinality(
                                ScopedPropertyKey::try_new("Fixture", property).unwrap(),
                                rows,
                            );
                        }
                    }
                    let terms = properties
                        .iter()
                        .take(count)
                        .map(|property| {
                            context.params = context.params.clone().with_value(
                                NonEmptyString::new(*property).unwrap(),
                                PropertyValue::from("fixture-value"),
                            );
                            if parameterized {
                                Predicate::eq_param(*property, *property)
                            } else {
                                Predicate::eq(*property, "fixture-value")
                            }
                        })
                        .collect::<Vec<_>>();
                    let predicate = if nested {
                        Predicate::and(vec![Predicate::and(terms)])
                    } else {
                        Predicate::and(terms)
                    };
                    let plan = executable_traversal(
                        g().n_with_label_where("Fixture", predicate)
                            .values(vec!["name"]),
                        context.clone(),
                    );
                    let diagnostics = crate::diagnostics::analyze(&plan, &context);
                    let unbounded = diagnostics.insights.iter().any(|insight| {
                        matches!(
                            insight,
                            crate::diagnostics::PlannerInsight::UnboundedScan(_)
                        )
                    });
                    println!("PROBE count={count} stats={populated_stats} params={parameterized} nested={nested} unbounded={unbounded} label_scans={} equality_lookups={} estimated_us={}",
                        diagnostics.statistics.node_accesses.label_scans,
                        diagnostics.statistics.node_accesses.equality_index_lookups,
                        plan.metrics().selected_cost.latency.as_micros());
                    if unbounded {
                        unbounded_cases.push((count, populated_stats, parameterized, nested));
                    }
                }
            }
        }
    }
    assert!(
        unbounded_cases.is_empty(),
        "indexed conjunctions selected unbounded scans: {unbounded_cases:?}"
    );
}
