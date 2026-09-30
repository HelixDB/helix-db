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
