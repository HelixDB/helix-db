//! Source filters whose conjuncts a property index serves never scan.
//!
//! If a field has an index, a filter on it must always be decided by that
//! index, never by reading records row by row. This matrix pins that for
//! source filters (`N<L>.where`, `E<L>.where`, and a leading pipeline
//! filter) under every terminal, with and without statistics, and with the
//! optional exploration budget exhausted: the selected plan never scans, and
//! no per-row filter mentions an indexed property. Only a predicate with no
//! index-served conjunct scans.

use crate::planning::tests::support::*;

const PROPERTIES: [&str; 5] = ["p0", "p1", "p2", "p3", "p4"];

fn indexes() -> IndexCatalogSnapshot {
    PROPERTIES
        .into_iter()
        .fold(IndexCatalogSnapshot::default(), |indexes, property| {
            indexes
                .with_node_eq(ScopedPropertyKey::try_new("Item", property).unwrap())
                .with_edge_eq(ScopedPropertyKey::try_new("Link", property).unwrap())
        })
}

/// Planner contexts: no statistics, populated statistics, and stale
/// statistics claiming zero matches, each with the default and an exhausted
/// exploration budget.
fn contexts() -> Vec<(&'static str, PlannerContext)> {
    let params = PROPERTIES
        .into_iter()
        .enumerate()
        .fold(ParamBindings::default(), |params, (value, property)| {
            params.with_value(
                NonEmptyString::new(format!("v{}", &property[1..])).unwrap(),
                PropertyValue::from(value as i64),
            )
        })
        .with_value(
            NonEmptyString::new("values").unwrap(),
            PropertyValue::I64Array(vec![1, 2, 3]),
        );
    let with_stats = |matches: u64| {
        PROPERTIES.into_iter().fold(
            StatsSnapshot::default()
                .with_node_label_cardinality(NonEmptyString::new("Item").unwrap(), 100_000)
                .with_edge_label_cardinality(NonEmptyString::new("Link").unwrap(), 100_000),
            |stats, property| {
                stats
                    .with_node_eq_cardinality(
                        ScopedPropertyKey::try_new("Item", property).unwrap(),
                        matches,
                    )
                    .with_edge_eq_cardinality(
                        ScopedPropertyKey::try_new("Link", property).unwrap(),
                        matches,
                    )
            },
        )
    };
    let base = PlannerContext {
        params,
        ..ctx(indexes())
    };
    [
        ("no stats", StatsSnapshot::default()),
        ("populated stats", with_stats(20_000)),
        ("stale zero stats", with_stats(0)),
    ]
    .into_iter()
    .flat_map(|(name, stats)| {
        let planner_ctx = PlannerContext {
            stats,
            ..base.clone()
        };
        let mut exhausted = planner_ctx.clone();
        exhausted.optimizer_limits.exploration_rule_fires =
            crate::properties::PositiveUsize::at_least_one(1);
        [(name, planner_ctx), (name, exhausted)]
    })
    .collect()
}

/// Predicates every one of whose conjuncts is index-served, or whose
/// unindexed conjuncts (`rank`, `title`) must stay a residual over the
/// index-narrowed source.
fn indexed_predicates() -> Vec<Predicate> {
    let equalities = |count: usize, parameterized: bool| {
        PROPERTIES
            .into_iter()
            .take(count)
            .enumerate()
            .map(|(value, property)| {
                if parameterized {
                    Predicate::eq_param(property, format!("v{value}"))
                } else {
                    Predicate::eq(property, value as i64)
                }
            })
            .collect::<Vec<_>>()
    };
    let mut predicates = Vec::new();
    for count in 1..=PROPERTIES.len() {
        for parameterized in [false, true] {
            let terms = equalities(count, parameterized);
            predicates.push(Predicate::and(terms.clone()));
            if count > 1 {
                let (head, tail) = terms.split_at(1);
                predicates.push(Predicate::and(vec![
                    Predicate::and(head.to_vec()),
                    Predicate::and(tail.to_vec()),
                ]));
            }
        }
    }
    predicates.extend([
        Predicate::and(vec![Predicate::eq("p0", 1), Predicate::gte("rank", 3)]),
        Predicate::and(vec![
            Predicate::eq("p0", 1),
            Predicate::eq_param("p1", "v1"),
            Predicate::contains("title", "x"),
        ]),
        Predicate::is_in("p0", PropertyValue::I64Array(vec![1, 2, 3])),
        Predicate::is_in_param("p0", "values"),
        Predicate::or(vec![Predicate::eq("p0", 1), Predicate::eq("p1", 2)]),
        Predicate::and(vec![
            Predicate::eq("p2", 4),
            Predicate::or(vec![Predicate::eq("p0", 1), Predicate::eq("p1", 2)]),
        ]),
        Predicate::eq("p0", PropertyValue::Null),
        // Literal lists and same-property disjunctions past the union limit
        // are one batched index read, with or without null.
        Predicate::is_in("p0", PropertyValue::I64Array((0..100).collect())),
        Predicate::is_in(
            "p0",
            PropertyValue::Array(
                core::iter::once(PropertyValue::Null)
                    .chain((0..100).map(PropertyValue::I64))
                    .collect(),
            ),
        ),
        Predicate::or((0..100).map(|value| Predicate::eq("p0", value)).collect()),
        Predicate::and(vec![
            Predicate::eq("p1", 1),
            Predicate::is_in("p0", PropertyValue::I64Array((0..100).collect())),
        ]),
        // Partly indexed disjunctions: each branch reads its own index and
        // evaluates only its unindexed conjuncts.
        Predicate::or(vec![
            Predicate::and(vec![Predicate::eq("p0", 0), Predicate::gte("rank", 3)]),
            Predicate::eq("p1", 0),
        ]),
        Predicate::and(vec![
            Predicate::eq("p2", 4),
            Predicate::or(vec![
                Predicate::and(vec![
                    Predicate::eq("p0", 1),
                    Predicate::contains("title", "x"),
                ]),
                Predicate::eq_param("p1", "v1"),
            ]),
        ]),
    ]);
    predicates
}

/// Every stream shape a source filter reaches: a labeled source filter, a
/// leading pipeline filter, and each terminal over them.
fn node_shapes(predicate: &Predicate) -> Vec<Traversal<helix_ast::traversal::Terminal, ReadOnly>> {
    let source = || g().n_with_label_where("Item", predicate.clone());
    let leading = || g().n_with_label("Item").where_(predicate.clone());
    vec![
        source().values(vec!["p0"]),
        leading().values(vec!["p0"]),
        leading()
            .where_(Predicate::gte("rank", 3))
            .values(vec!["p0"]),
        source().count(),
        leading().count(),
        source().as_("saved").count(),
        leading().store("saved").count(),
        source().exists(),
        source().project(vec![Projection::property("$id", "id")]),
        source().group_count("rank"),
    ]
}

fn edge_shapes(predicate: &Predicate) -> Vec<Traversal<helix_ast::traversal::Terminal, ReadOnly>> {
    let source = || g().e_with_label_where("Link", predicate.clone());
    let leading = || g().e_with_label("Link").where_(predicate.clone());
    vec![
        source().values(vec!["p0"]),
        leading().values(vec!["p0"]),
        leading()
            .where_(Predicate::gte("rank", 3))
            .values(vec!["p0"]),
        source().count(),
        leading().count(),
        source().as_("saved").count(),
        leading().store("saved").count(),
        source().exists(),
        source().project(vec![Projection::property("$id", "id")]),
    ]
}

/// Visit every JSON value inside `value`, depth first.
fn visit<'a>(value: &'a serde_json::Value, f: &mut impl FnMut(&'a serde_json::Value)) {
    f(value);
    match value {
        serde_json::Value::Array(items) => items.iter().for_each(|item| visit(item, f)),
        serde_json::Value::Object(fields) => fields.values().for_each(|field| visit(field, f)),
        _ => {}
    }
}

/// Serialized scans anywhere in `plan`, including count programs.
fn scans(plan: &serde_json::Value) -> Vec<String> {
    const SCANS: [&str; 6] = [
        "label_scan",
        "all_scan",
        "node_full_scan",
        "edge_full_scan",
        "node_label_bitmap",
        "edge_label_bitmap",
    ];
    let mut found = Vec::new();
    visit(plan, &mut |value| match value {
        serde_json::Value::Object(fields) => found.extend(
            fields
                .keys()
                .filter(|key| SCANS.contains(&key.as_str()))
                .cloned(),
        ),
        serde_json::Value::String(text) if SCANS.contains(&text.as_str()) => {
            found.push(text.clone())
        }
        _ => {}
    });
    found
}

/// Strings inside every serialized per-row filter predicate of `plan`,
/// including count-cursor filters.
fn filter_strings(plan: &serde_json::Value) -> Vec<String> {
    let mut strings = Vec::new();
    visit(plan, &mut |value| {
        let Some(filter) = value.get("filter") else {
            return;
        };
        let Some(predicate) = filter.get("predicate") else {
            return;
        };
        visit(predicate, &mut |value| {
            if let serde_json::Value::String(text) = value {
                strings.push(text.clone());
            }
        });
    });
    strings
}

/// The plan without its wall-clock optimization duration.
fn semantic(plan: &ExecutablePlan) -> serde_json::Value {
    let mut value = serde_json::to_value(plan).unwrap();
    let Some(_) = value["metrics"]
        .as_object_mut()
        .and_then(|metrics| metrics.remove("optimization_micros"))
    else {
        panic!("serialized plan omitted its optimization duration: {value:#}");
    };
    value
}

#[test]
fn index_served_source_filters_never_scan() {
    let mut violations = Vec::new();
    for (context_name, planner_ctx) in contexts() {
        for predicate in indexed_predicates() {
            for shape in node_shapes(&predicate)
                .into_iter()
                .chain(edge_shapes(&predicate))
            {
                let plan = executable_traversal(shape.clone(), planner_ctx.clone());
                let diagnostics = crate::diagnostics::analyze(&plan, &planner_ctx);
                let json = semantic(&plan);
                let statistics = &diagnostics.statistics;
                let indexed_filters = filter_strings(&json)
                    .into_iter()
                    .filter(|text| PROPERTIES.contains(&text.as_str()))
                    .collect::<Vec<_>>();
                let scans = scans(&json);
                if statistics.node_accesses.label_scans
                    + statistics.node_accesses.all_scans
                    + statistics.edge_accesses.label_scans
                    + statistics.edge_accesses.all_scans
                    != 0
                    || !scans.is_empty()
                    || !indexed_filters.is_empty()
                {
                    violations.push(format!(
                        "{context_name} {predicate:?} {:?}: scans {scans:?}, \
                         indexed filters {indexed_filters:?}",
                        shape
                    ));
                }
                // The same input always plans the same way.
                let again = executable_traversal(shape, planner_ctx.clone());
                assert_eq!(plan.steps(), again.steps());
                assert_eq!(json, semantic(&again));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "{} index-served source filters scanned or filtered per row:\n{}",
        violations.len(),
        violations.join("\n")
    );
}

#[test]
fn source_filters_without_an_index_served_conjunct_scan() {
    for (context_name, planner_ctx) in contexts() {
        for predicate in [
            Predicate::gte("rank", 3),
            Predicate::and(vec![
                Predicate::gte("rank", 3),
                Predicate::contains("title", "x"),
            ]),
            // A branch no index serves leaves every row to be read anyway.
            Predicate::or(vec![Predicate::eq("p0", 1), Predicate::gte("rank", 3)]),
        ] {
            for (shape, element) in [
                (
                    g().n_with_label_where("Item", predicate.clone())
                        .values(vec!["p0"]),
                    ElementKind::Node,
                ),
                (
                    g().e_with_label_where("Link", predicate.clone())
                        .values(vec!["p0"]),
                    ElementKind::Edge,
                ),
            ] {
                let plan = executable_traversal(shape, planner_ctx.clone());
                let statistics = crate::diagnostics::analyze(&plan, &planner_ctx).statistics;
                let accesses = match element {
                    ElementKind::Node => statistics.node_accesses,
                    ElementKind::Edge => statistics.edge_accesses,
                };
                assert_eq!(accesses.label_scans, 1, "{context_name} {predicate:?}");
                assert!(
                    has_exec_op_family(&plan, ExecOpFamily::Filter),
                    "{context_name} {predicate:?}"
                );
                // The matrix's detectors see this scan and its filter.
                let json = semantic(&plan);
                assert!(!scans(&json).is_empty(), "{json:#}");
                assert!(filter_strings(&json).contains(&"rank".to_string()));
            }
            for shape in [
                g().n_with_label_where("Item", predicate.clone()).count(),
                g().e_with_label_where("Link", predicate.clone()).count(),
            ] {
                let json = semantic(&executable_traversal(shape, planner_ctx.clone()));
                assert!(!scans(&json).is_empty(), "{json:#}");
                assert!(filter_strings(&json).contains(&"rank".to_string()));
            }
        }
    }
}

#[test]
fn wide_disjunctions_over_many_indexes_plan_one_union() {
    // A thousand branches over a thousand indexed properties stay one index
    // union, and planning them stays bounded.
    let properties = (0..1_000).map(|n| format!("q{n}")).collect::<Vec<_>>();
    let planner_ctx =
        ctx(properties
            .iter()
            .fold(IndexCatalogSnapshot::default(), |indexes, property| {
                indexes.with_node_eq(ScopedPropertyKey::try_new("Item", property).unwrap())
            }));
    let predicate = Predicate::or(
        properties
            .iter()
            .map(|property| Predicate::eq(property.as_str(), 1))
            .collect(),
    );
    let plan = executable_traversal(
        g().n_with_label_where("Item", predicate).values(vec!["q0"]),
        planner_ctx.clone(),
    );
    let statistics = crate::diagnostics::analyze(&plan, &planner_ctx).statistics;
    assert!(!plan.metrics().guardrail_hit);
    assert_eq!(statistics.node_accesses.label_scans, 0);
    assert_no_exec_op_family(&plan, ExecOpFamily::Filter);
}

#[test]
fn literal_lists_of_any_length_plan_one_batched_read() {
    // Ten thousand literals stay one literal-set source: set rules never
    // compare its members pairwise, so planning stays fast.
    let values = (0..10_000).collect::<Vec<i64>>();
    for predicate in [
        Predicate::is_in("p0", PropertyValue::I64Array(values.clone())),
        Predicate::or(
            values
                .iter()
                .map(|value| Predicate::eq("p0", *value))
                .collect(),
        ),
    ] {
        let started = std::time::Instant::now();
        let plan = executable_traversal(
            g().n_with_label_where("Item", predicate).values(vec!["p0"]),
            ctx(indexes()),
        );
        // Quadratic list handling took over a minute here; linear planning
        // takes well under a second, with headroom for unoptimized builds
        // sharing the machine with the rest of the suite.
        let bound = if cfg!(debug_assertions) { 4 } else { 1 };
        assert!(
            started.elapsed() < std::time::Duration::from_secs(bound),
            "planning took {:?}",
            started.elapsed()
        );
        assert_batched_node_equality_set(&plan, "Item", "p0", values.len());
        assert_no_exec_op_family(&plan, ExecOpFamily::Filter);
    }

    // Null joins the batch as the label rows outside the lane.
    let plan = executable_traversal(
        g().n_with_label_where(
            "Item",
            Predicate::is_in(
                "p0",
                PropertyValue::Array(
                    (0..100)
                        .map(PropertyValue::I64)
                        .chain(core::iter::once(PropertyValue::Null))
                        .collect(),
                ),
            ),
        )
        .values(vec!["p0"]),
        ctx(indexes()),
    );
    assert!(matches!(
        unwrapped_first_exec_access(&plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::SecondarySet {
            set: crate::exec::ExecNodeSecondarySetPlan::Union { driver, rest },
        }) if matches!(
            driver.as_ref(),
            crate::exec::ExecNodeSecondarySetPlan::Bitmap(
                crate::exec::ExecNodeBitmapExpr::BatchedUnionRead { values, .. }
            ) if values.len() == 100
        ) && matches!(
            rest.as_ref(),
            [crate::exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
                crate::exec::ExecNodeAuthoritativeScanPredicate::NullEquality { .. }
            )]
        )
    ));
}

#[test]
fn literal_lists_on_range_only_properties_read_point_ranges() {
    // Equality on a property with only a range index reads one point range
    // per value, past the union limit too, never a scan.
    let planner_ctx = ctx(IndexCatalogSnapshot::default().with_node_range(
        ScopedPropertyDirectionKey::try_new(
            "Item",
            "rank",
            helix_ast::index::RangeIndexDirection::Asc,
        )
        .unwrap(),
    ));
    for count in [3_i64, 100, 1_000] {
        let plan = executable_traversal(
            g().n_with_label_where(
                "Item",
                Predicate::is_in("rank", PropertyValue::I64Array((0..count).collect())),
            )
            .values(vec!["rank"]),
            planner_ctx.clone(),
        );
        let statistics = crate::diagnostics::analyze(&plan, &planner_ctx).statistics;
        assert_eq!(statistics.node_accesses.label_scans, 0, "{count}");
        assert_no_exec_op_family(&plan, ExecOpFamily::Filter);
    }
}

#[test]
fn counts_over_saved_streams_read_the_index() {
    // `.as()` and `.store()` save the stream the count reads; the filter
    // before them stays with the access, so the count reads a bitmap.
    for shape in [
        g().n_with_label_where("Item", Predicate::eq("p0", 0))
            .as_("x")
            .count(),
        g().n_with_label("Item")
            .where_(Predicate::eq("p0", 0))
            .store("x")
            .count(),
    ] {
        let plan = executable_traversal(shape, ctx(indexes()));
        let json = semantic(&plan);
        let mut bitmaps = 0;
        visit(&json, &mut |value| {
            if value.get("node_bitmap").is_some() || value.get("bitmap").is_some() {
                bitmaps += 1;
            }
        });
        assert!(bitmaps > 0, "{json:#}");
        assert!(scans(&json).is_empty(), "{json:#}");
        assert!(filter_strings(&json).is_empty(), "{json:#}");
        assert_no_exec_op_family(&plan, ExecOpFamily::Filter);
    }
}

#[test]
fn partly_indexed_ors_read_their_residual_free_branches_as_one_set() {
    // A thousand residual-free branches on one property and one branch with
    // a residual: the thousand are one batched read, not a thousand steps.
    let predicate = Predicate::or(
        (0..1_000)
            .map(|value| Predicate::eq("p1", value))
            .chain([Predicate::and(vec![
                Predicate::eq("p0", 0),
                Predicate::gte("rank", 3),
            ])])
            .collect(),
    );
    let plan = executable_traversal(
        g().n_with_label_where("Item", predicate).values(vec!["p0"]),
        ctx(indexes()),
    );
    let accesses = plan
        .steps()
        .iter()
        .filter(|step| matches!(step.op, ExecOp::Access { .. }))
        .count();
    assert_eq!(accesses, 2, "{:#?}", plan.steps());
    let json = semantic(&plan);
    let mut batches = Vec::new();
    visit(&json, &mut |value| {
        if let Some(values) = value
            .get("batched_union_read")
            .and_then(|read| read["values"].as_array())
        {
            batches.push(values.len());
        }
    });
    assert_eq!(batches, [1_000], "{json:#}");
    let filtered = filter_strings(&json);
    assert!(filtered.contains(&"rank".to_string()), "{json:#}");
    assert!(
        filtered
            .iter()
            .all(|text| !PROPERTIES.contains(&text.as_str())),
        "{json:#}"
    );
}

#[test]
fn counts_over_unique_literal_sets_read_one_batch() {
    // A count over a unique literal set wider than one index union is one
    // batched owner read, not one leaf per value.
    let mut planner_ctx = ctx(IndexCatalogSnapshot::default());
    planner_ctx.indexes.node_eq.insert(
        ScopedPropertyKey::try_new("User", "email").unwrap(),
        crate::catalog::NodeEqualityIndexMeta::try_new("user-email")
            .unwrap()
            .with_uniqueness(crate::catalog::IndexUniqueness::Unique),
    );
    for count in [100, 10_000] {
        let emails = (0..count).map(|n| format!("user-{n}")).collect::<Vec<_>>();
        let plan = executable_traversal(
            g().n_with_label_where(
                "User",
                Predicate::is_in("email", PropertyValue::StringArray(emails)),
            )
            .count(),
            planner_ctx.clone(),
        );
        let json = semantic(&plan);
        let (mut batches, mut singles) = (Vec::new(), 0);
        visit(&json, &mut |value| {
            if let Some(values) = value
                .get("node_unique_batch")
                .and_then(|batch| batch["values"].as_array())
            {
                batches.push(values.len());
            }
            if value.get("node_unique").is_some() {
                singles += 1;
            }
        });
        assert_eq!(batches, [count], "{json:#}");
        assert_eq!(singles, 0, "{json:#}");
    }
}

#[test]
fn unique_reads_that_may_return_many_rows_keep_their_read_limit() {
    // A unique lane holds one owner per indexed value, but a literal set
    // returns one row per member, and null (bound statically or at run time)
    // returns every label row without the property, so none of them may drop
    // the limit as if one row came back.
    let mut planner_ctx = ctx(IndexCatalogSnapshot::default());
    planner_ctx.indexes.node_eq.insert(
        ScopedPropertyKey::try_new("User", "email").unwrap(),
        crate::catalog::NodeEqualityIndexMeta::try_new("user-email")
            .unwrap()
            .with_uniqueness(crate::catalog::IndexUniqueness::Unique),
    );
    let emails = (0..70).map(|n| format!("user-{n}")).collect::<Vec<_>>();
    for predicate in [
        Predicate::is_in("email", PropertyValue::StringArray(emails)),
        Predicate::eq("email", PropertyValue::Null),
        Predicate::is_in(
            "email",
            PropertyValue::Array(vec![PropertyValue::from("a"), PropertyValue::Null]),
        ),
        Predicate::eq_param("email", "email"),
    ] {
        let plan = executable_traversal(
            g().n_with_label_where("User", predicate.clone())
                .limit(5usize)
                .values(vec!["email"]),
            planner_ctx.clone(),
        );
        assert_eq!(
            first_limited_access_limit(&plan),
            Some(5),
            "{predicate:?}: {:#?}",
            plan.steps()
        );
    }

    // One indexed literal has at most one owner, so its limit is covered.
    let plan = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("email", "a"))
            .limit(5usize)
            .values(vec!["email"]),
        planner_ctx,
    );
    assert_eq!(first_limited_access_limit(&plan), None);
}
