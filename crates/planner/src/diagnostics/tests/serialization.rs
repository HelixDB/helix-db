use super::support::{name, plan, plan_batch, search_context, unbounded_scans};
use crate::{catalog, context, diagnostics};
use helix_ast::batch::read_batch;
use helix_ast::expr::Predicate;
use helix_ast::query::QueryValue;
use helix_ast::traversal::g;
use serde_json::{json, Value};

fn empty_access_json() -> Value {
    json!({
        "all_scans": 0,
        "label_scans": 0,
        "point_lookups": 0,
        "equality_index_lookups": 0,
        "range_index_scans": 0,
        "reverse_range_index_scans": 0,
        "dynamic_bounded_accesses": 0,
        "vector_searches": 0,
        "text_searches": 0,
        "bounded_accesses": 0,
    })
}

fn empty_statistics_json() -> Value {
    json!({
        "memo_groups": 0,
        "memo_expressions": 0,
        "rules_fired": 0,
        "rejected_alternatives": 0,
        "alternatives_considered": 0,
        "optimization_micros": 0,
        "guardrail_hit": false,
        "total_operators": 0,
        "maximum_operator_depth": 0,
        "node_accesses": empty_access_json(),
        "edge_accesses": empty_access_json(),
        "unions": 0,
        "intersections": 0,
        "residual_filters": 0,
        "explicit_sorts": 0,
        "limits": 0,
        "skips": 0,
        "ranges": 0,
        "expansions": 0,
        "branches": 0,
        "repeats": 0,
        "for_each": 0,
    })
}

#[test]
fn empty_diagnostics_json_shape_is_an_exact_wire_contract() {
    assert_eq!(
        serde_json::to_value(diagnostics::PlannerDiagnostics::default()).unwrap(),
        json!({
            "statistics": empty_statistics_json(),
            "insights": [],
        })
    );
}

#[test]
fn every_insight_variant_has_an_exact_tagged_json_shape() {
    let cases = [
        (
            diagnostics::PlannerInsight::MissingIndex(diagnostics::MissingIndexInsight {
                element: catalog::ElementKind::Node,
                label: name("User"),
                property: name("username"),
                index_kind: diagnostics::SecondaryIndexKind::Equality,
                occurrences: 2,
            }),
            json!({
                "type": "missing_index",
                "details": {
                    "element": "node",
                    "label": "User",
                    "property": "username",
                    "index_kind": "equality",
                    "occurrences": 2,
                },
            }),
        ),
        (
            diagnostics::PlannerInsight::UnboundedScan(diagnostics::UnboundedScanInsight {
                element: catalog::ElementKind::Edge,
                label: Some(name("FOLLOWS")),
                predicate_properties: diagnostics::PredicatePropertySet::new([
                    name("weight"),
                    name("created_at"),
                    name("weight"),
                ]),
                occurrences: 3,
            }),
            json!({
                "type": "unbounded_scan",
                "details": {
                    "element": "edge",
                    "label": "FOLLOWS",
                    "predicate_properties": ["created_at", "weight"],
                    "occurrences": 3,
                },
            }),
        ),
        (
            diagnostics::PlannerInsight::DeepTraversal(diagnostics::DeepTraversalInsight {
                expansion_count: 4,
                repeat_count: 1,
                maximum_depth: 8,
            }),
            json!({
                "type": "deep_traversal",
                "details": {
                    "expansion_count": 4,
                    "repeat_count": 1,
                    "maximum_depth": 8,
                },
            }),
        ),
    ];

    for (insight, expected) in cases {
        assert_eq!(serde_json::to_value(insight).unwrap(), expected);
    }
}

#[test]
fn diagnostics_round_trip_with_all_public_insight_variants() {
    let diagnostics = diagnostics::PlannerDiagnostics {
        statistics: diagnostics::PlannerStatistics {
            memo_groups: 7,
            total_operators: 3,
            ..diagnostics::PlannerStatistics::default()
        },
        insights: vec![
            diagnostics::PlannerInsight::MissingIndex(diagnostics::MissingIndexInsight {
                element: catalog::ElementKind::Node,
                label: name("User"),
                property: name("email"),
                index_kind: diagnostics::SecondaryIndexKind::Range,
                occurrences: 1,
            }),
            diagnostics::PlannerInsight::UnboundedScan(diagnostics::UnboundedScanInsight {
                element: catalog::ElementKind::Edge,
                label: None,
                predicate_properties: diagnostics::PredicatePropertySet::default(),
                occurrences: 2,
            }),
            diagnostics::PlannerInsight::DeepTraversal(diagnostics::DeepTraversalInsight {
                expansion_count: 3,
                repeat_count: 0,
                maximum_depth: 3,
            }),
        ],
    };
    let encoded = serde_json::to_string(&diagnostics).unwrap();

    assert_eq!(
        serde_json::from_str::<diagnostics::PlannerDiagnostics>(&encoded).unwrap(),
        diagnostics
    );
}

#[test]
fn unbounded_scan_property_context_is_backward_compatible_when_absent() {
    let legacy = json!({
        "type": "unbounded_scan",
        "details": {
            "element": "node",
            "label": "User",
            "occurrences": 1,
        },
    });
    let insight = serde_json::from_value::<diagnostics::PlannerInsight>(legacy.clone()).unwrap();

    assert_eq!(
        insight,
        diagnostics::PlannerInsight::UnboundedScan(diagnostics::UnboundedScanInsight {
            element: catalog::ElementKind::Node,
            label: Some(name("User")),
            predicate_properties: diagnostics::PredicatePropertySet::default(),
            occurrences: 1,
        })
    );
    assert_eq!(serde_json::to_value(insight).unwrap(), legacy);
}

#[test]
fn human_messages_are_stable_and_pluralize_occurrences() {
    let cases = [
        (
            diagnostics::PlannerInsight::MissingIndex(diagnostics::MissingIndexInsight {
                element: catalog::ElementKind::Node,
                label: name("User"),
                property: name("username"),
                index_kind: diagnostics::SecondaryIndexKind::Equality,
                occurrences: 1,
            }),
            "missing equality index for node label `User` property `username` (1 occurrence)",
        ),
        (
            diagnostics::PlannerInsight::MissingIndex(diagnostics::MissingIndexInsight {
                element: catalog::ElementKind::Edge,
                label: name("FOLLOWS"),
                property: name("weight"),
                index_kind: diagnostics::SecondaryIndexKind::Range,
                occurrences: 2,
            }),
            "missing range index for edge label `FOLLOWS` property `weight` (2 occurrences)",
        ),
        (
            diagnostics::PlannerInsight::UnboundedScan(diagnostics::UnboundedScanInsight {
                element: catalog::ElementKind::Node,
                label: None,
                predicate_properties: diagnostics::PredicatePropertySet::default(),
                occurrences: 1,
            }),
            "unbounded node scan (1 occurrence)",
        ),
        (
            diagnostics::PlannerInsight::UnboundedScan(diagnostics::UnboundedScanInsight {
                element: catalog::ElementKind::Node,
                label: Some(name("User")),
                predicate_properties: diagnostics::PredicatePropertySet::new([name("email")]),
                occurrences: 1,
            }),
            "unbounded node label scan for `User` with residual predicate property `email` (1 occurrence)",
        ),
        (
            diagnostics::PlannerInsight::UnboundedScan(diagnostics::UnboundedScanInsight {
                element: catalog::ElementKind::Edge,
                label: Some(name("LIKES")),
                predicate_properties: diagnostics::PredicatePropertySet::new([
                    name("created_at"),
                    name("weight"),
                ]),
                occurrences: 2,
            }),
            "unbounded edge label scan for `LIKES` with residual predicate properties `created_at`, `weight` (2 occurrences)",
        ),
        (
            diagnostics::PlannerInsight::DeepTraversal(diagnostics::DeepTraversalInsight {
                expansion_count: 3,
                repeat_count: 1,
                maximum_depth: 5,
            }),
            "deep traversal with 3 expansions, 1 repeat, and maximum depth 5",
        ),
    ];

    for (insight, expected) in cases {
        assert_eq!(insight.message(), expected);
    }
}

#[test]
fn predicate_literals_and_parameter_values_never_reach_diagnostics() {
    const LITERAL_SECRET: &str = "literal-super-secret-8f7d";
    const PARAMETER_SECRET: &str = "parameter-super-secret-2ac1";
    const QUERY_SECRET: &str = "query-super-secret-e631";

    let ctx = context::PlannerContext {
        params: context::ParamBindings::default()
            .with_value(name("username_parameter"), PARAMETER_SECRET)
            .with_query_value(
                name("payload_parameter"),
                QueryValue::String(QUERY_SECRET.to_string()),
            ),
        ..context::PlannerContext::default()
    };
    let batch = read_batch()
        .var_as(
            "literal",
            g().n_with_label_where("User", Predicate::eq("username", LITERAL_SECRET)),
        )
        .var_as(
            "parameter",
            g().n_with_label_where(
                "User",
                Predicate::eq_param("username", "username_parameter"),
            ),
        );
    let output = plan_batch(&batch, &ctx);
    let encoded = serde_json::to_string(output.diagnostics()).unwrap();

    for secret in [LITERAL_SECRET, PARAMETER_SECRET, QUERY_SECRET] {
        assert!(!encoded.contains(secret));
        assert!(output
            .diagnostics()
            .insights
            .iter()
            .all(|insight| !insight.message().contains(secret)));
    }
    assert!(!encoded.contains("username_parameter"));
    assert!(!encoded.contains("payload_parameter"));
}

#[test]
fn search_payloads_index_ids_plans_traces_and_costs_never_reach_diagnostics() {
    const SEARCH_SECRET: &str = "search-super-secret-fab9";
    let ctx = search_context();
    let batch = read_batch()
        .var_as(
            "text",
            g().text_search_nodes("Doc", "body", SEARCH_SECRET, 3, None),
        )
        .var_as(
            "vector",
            g().vector_search_edges(
                "MENTIONS",
                "embedding",
                vec![91_827.125f32, 72_611.5],
                3,
                None,
            ),
        );
    let output = plan_batch(&batch, &ctx);
    let encoded = serde_json::to_string(output.diagnostics()).unwrap();

    for forbidden in [
        SEARCH_SECRET,
        "91827.125",
        "72611.5",
        "vector:",
        "text:",
        "steps",
        "trace",
        "selected_cost",
        "range_seeks",
    ] {
        assert!(
            !encoded.contains(forbidden),
            "leaked `{forbidden}`: {encoded}"
        );
    }
    assert_eq!(
        output.diagnostics().statistics.node_accesses.text_searches,
        1
    );
    assert_eq!(
        output
            .diagnostics()
            .statistics
            .edge_accesses
            .vector_searches,
        1
    );
}

#[test]
fn a_real_missing_index_result_serializes_only_its_structured_fields() {
    let output = plan(
        g().n_with_label_where(
            "User",
            Predicate::eq("secret_property_name", "secret-value-that-must-not-appear"),
        ),
        &context::PlannerContext::default(),
    );
    let value = serde_json::to_value(output.diagnostics()).unwrap();

    assert_eq!(
        unbounded_scans(output.diagnostics())[0]
            .predicate_properties
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>(),
        ["secret_property_name"]
    );
    assert!(value.to_string().contains("secret_property_name"));
    assert!(!value
        .to_string()
        .contains("secret-value-that-must-not-appear"));
}
