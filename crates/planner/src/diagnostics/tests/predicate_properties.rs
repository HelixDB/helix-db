use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use super::support::{diagnostics_for_ops, executable_plan, name, step, subplan, unbounded_scans};
use crate::{context, diagnostics, exec, ir};
use helix_ast::expr::{CompareOp, Expr, Predicate, StreamBound};
use helix_ast::traversal::Order;

fn node_scan() -> exec::ExecOp {
    exec::ExecOp::Access {
        plan: Box::new(exec::ExecAccessPlan::Node(
            exec::ExecNodeAccessPlan::LabelScan {
                label: name("User"),
            },
        )),
    }
}

fn filter(property: &str) -> exec::ExecOp {
    exec::ExecOp::Filter {
        predicate: ir::PredicatePlan::new(Predicate::eq(property, "private-value")).unwrap(),
    }
}

fn property_names(insight: &diagnostics::UnboundedScanInsight) -> Vec<&str> {
    insight
        .predicate_properties
        .iter()
        .map(AsRef::as_ref)
        .collect()
}

#[test]
fn residual_predicate_property_collection_covers_every_predicate_and_expression_shape() {
    let computed_case = Expr::case(
        vec![(
            Predicate::or(vec![
                Predicate::eq("p25_case_when", 1),
                Predicate::not(Predicate::is_null("p26_case_null")),
            ]),
            Expr::prop("p27_case_then"),
        )],
        Some(Expr::prop("p28_case_else")),
    );
    let predicate = Predicate::and(vec![
        Predicate::eq("p01_eq", "literal-secret"),
        Predicate::Neq {
            left: Expr::prop("p02_sub_left").sub_expr(Expr::prop("p03_sub_right")),
            right: Expr::val(1),
        },
        Predicate::Gt {
            left: Expr::prop("p04_mul_left").mul_expr(Expr::prop("p05_mul_right")),
            right: Expr::val(1),
        },
        Predicate::Gte {
            left: Expr::prop("p06_div_left").div_expr(Expr::prop("p07_div_right")),
            right: Expr::val(1),
        },
        Predicate::Lt {
            left: Expr::prop("p08_mod_left").modulo(Expr::prop("p09_mod_right")),
            right: Expr::val(1),
        },
        Predicate::Lte {
            left: Expr::prop("p10_negated").neg_expr(),
            right: Expr::val(1),
        },
        Predicate::Between {
            value: Expr::prop("p11_between_value"),
            min: Expr::prop("p12_between_min"),
            max: Expr::prop("p13_between_max"),
        },
        Predicate::has_key("p14_has_key"),
        Predicate::is_null("p15_is_null"),
        Predicate::is_not_null("p16_is_not_null"),
        Predicate::StartsWith {
            value: Expr::prop("p17_starts_with"),
            prefix: Expr::param("private-prefix-parameter"),
        },
        Predicate::EndsWith {
            value: Expr::prop("p18_ends_with"),
            suffix: Expr::val("private-suffix"),
        },
        Predicate::Contains {
            value: Expr::prop("p19_contains"),
            substring: Expr::val("private-substring"),
        },
        Predicate::IsIn {
            value: Expr::prop("p20_is_in"),
            values: Expr::param("private-values-parameter"),
        },
        Predicate::Compare {
            left: Expr::Id,
            op: CompareOp::Eq,
            right: Expr::prop("p21_compare"),
        },
        Predicate::Eq {
            left: Expr::prop("p22_add_left").add_expr(Expr::prop("p23_add_right")),
            right: computed_case,
        },
        Predicate::Compare {
            left: Expr::Timestamp,
            op: CompareOp::Lt,
            right: Expr::DateTimeNow,
        },
        Predicate::eq("$label", "User"),
        Predicate::not(Predicate::eq("p24_not", true)),
    ]);
    let diagnostics = diagnostics_for_ops([
        node_scan(),
        exec::ExecOp::Filter {
            predicate: ir::PredicatePlan::new(predicate).unwrap(),
        },
    ]);
    let scans = unbounded_scans(&diagnostics);

    assert_eq!(scans.len(), 1);
    let actual = property_names(scans[0])
        .into_iter()
        .collect::<BTreeSet<_>>();
    let expected = (1..=28)
        .map(|index| match index {
            1 => "p01_eq",
            2 => "p02_sub_left",
            3 => "p03_sub_right",
            4 => "p04_mul_left",
            5 => "p05_mul_right",
            6 => "p06_div_left",
            7 => "p07_div_right",
            8 => "p08_mod_left",
            9 => "p09_mod_right",
            10 => "p10_negated",
            11 => "p11_between_value",
            12 => "p12_between_min",
            13 => "p13_between_max",
            14 => "p14_has_key",
            15 => "p15_is_null",
            16 => "p16_is_not_null",
            17 => "p17_starts_with",
            18 => "p18_ends_with",
            19 => "p19_contains",
            20 => "p20_is_in",
            21 => "p21_compare",
            22 => "p22_add_left",
            23 => "p23_add_right",
            24 => "p24_not",
            25 => "p25_case_when",
            26 => "p26_case_null",
            27 => "p27_case_then",
            28 => "p28_case_else",
            _ => unreachable!("the expected property range is exhaustive"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
    assert!(!actual.contains("$label"));

    let encoded = serde_json::to_string(scans[0]).unwrap();
    for secret in [
        "literal-secret",
        "private-prefix-parameter",
        "private-suffix",
        "private-substring",
        "private-values-parameter",
    ] {
        assert!(!encoded.contains(secret), "leaked `{secret}`: {encoded}");
    }
}

#[test]
fn access_preserving_operators_propagate_properties_but_scope_changes_do_not() {
    let pass_through = diagnostics_for_ops([
        node_scan(),
        exec::ExecOp::Limit {
            count: ir::StreamBoundPlan::new(StreamBound::expr(Expr::param("limit"))).unwrap(),
        },
        exec::ExecOp::Skip {
            count: ir::StreamBoundPlan::Literal(1),
        },
        exec::ExecOp::Range {
            range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(1, 3).unwrap()),
        },
        exec::ExecOp::Distinct,
        exec::ExecOp::Order {
            plan: ir::OrderPlan::ExplicitSort(
                ir::OrderKey {
                    property: name("sort_only"),
                    order: Order::Asc,
                }
                .into(),
            ),
        },
        exec::ExecOp::Reserved {
            op: ir::ReservedOp::Fold,
        },
        exec::ExecOp::Barrier {
            name: name("barrier"),
        },
        exec::ExecOp::Noop,
        filter("preserved"),
    ]);
    assert_eq!(
        property_names(unbounded_scans(&pass_through)[0]),
        ["preserved"]
    );

    let boundaries = [
        exec::ExecOp::Expand {
            plan: ir::ExpandPlan {
                direction: ir::ExpandDirection::Out,
                output: ir::ExpandOutput::Nodes,
                label: ir::ExpandLabelPlan::Any,
            },
        },
        exec::ExecOp::Project {
            projection: ir::ProjectionPlan::Exists,
        },
        exec::ExecOp::Aggregate {
            aggregate: ir::AggregatePlan::Group(name("group")),
        },
        exec::ExecOp::Variable {
            op: exec::ExecVariableOp::Stream(ir::StreamVariableOp::Store(name("stored"))),
        },
    ];
    for boundary in boundaries {
        let diagnostics = diagnostics_for_ops([node_scan(), boundary, filter("must_not_leak")]);
        assert!(unbounded_scans(&diagnostics)[0]
            .predicate_properties
            .is_empty());
    }
}

#[test]
fn union_and_intersection_lineage_attach_filters_to_every_unbounded_source() {
    for mode in [exec::ExecMergeMode::Union, exec::ExecMergeMode::Intersect] {
        let first = exec::ExecStepId::new(1).unwrap();
        let second = exec::ExecStepId::new(2).unwrap();
        let merged = exec::ExecStepId::new(3).unwrap();
        let plan = executable_plan(
            vec![
                step(1, Vec::new(), node_scan()),
                step(2, Vec::new(), node_scan()),
                step(3, vec![first, second], exec::ExecOp::Merge { mode }),
                step(4, vec![merged], filter("shared")),
            ],
            exec::PlannerMetrics::default(),
        );
        let diagnostics = diagnostics::analyze(&plan, &context::PlannerContext::default());
        let scans = unbounded_scans(&diagnostics);

        assert_eq!(scans.len(), 1);
        assert_eq!(property_names(scans[0]), ["shared"]);
        assert_eq!(scans[0].occurrences, 2);
    }

    let first = exec::ExecStepId::new(1).unwrap();
    let second = exec::ExecStepId::new(2).unwrap();
    let merged = exec::ExecStepId::new(3).unwrap();
    let plan = executable_plan(
        vec![
            step(1, Vec::new(), node_scan()),
            step(
                2,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::LabelScan {
                            label: name("LIKES"),
                        },
                    )),
                },
            ),
            step(
                3,
                vec![first, second],
                exec::ExecOp::Merge {
                    mode: exec::ExecMergeMode::Union,
                },
            ),
            step(4, vec![merged], filter("mixed")),
        ],
        exec::PlannerMetrics::default(),
    );
    let diagnostics = diagnostics::analyze(&plan, &context::PlannerContext::default());
    let scans = unbounded_scans(&diagnostics);

    assert_eq!(scans.len(), 2);
    assert!(scans.iter().all(|scan| property_names(scan) == ["mixed"]));

    let plan = executable_plan(
        vec![
            step(1, Vec::new(), node_scan()),
            step(2, Vec::new(), exec::ExecOp::Noop),
            step(
                3,
                vec![first, second],
                exec::ExecOp::Merge {
                    mode: exec::ExecMergeMode::Union,
                },
            ),
            step(4, vec![merged], filter("known_source")),
        ],
        exec::PlannerMetrics::default(),
    );
    let diagnostics = diagnostics::analyze(&plan, &context::PlannerContext::default());

    assert_eq!(
        property_names(unbounded_scans(&diagnostics)[0]),
        ["known_source"]
    );
}

#[test]
fn nested_branch_repeat_and_foreach_subplans_collect_properties_independently() {
    let nested = |property| subplan([node_scan(), filter(property)]);
    let branch = exec::ExecBranchPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        nested("branch_a"),
        nested("branch_b"),
    ));
    let repeat = exec::ExecRepeatPlan {
        body: Box::new(nested("repeat")),
        stop: ir::RepeatStopPlan::Times {
            count: NonZeroUsize::MIN,
        },
        emit: ir::RepeatEmitPlan::None,
        max_depth: NonZeroUsize::MIN,
    };
    let diagnostics = diagnostics_for_ops([
        exec::ExecOp::Noop,
        exec::ExecOp::Branch { plan: branch },
        exec::ExecOp::Repeat { plan: repeat },
        exec::ExecOp::ForEach {
            param: name("item"),
            body: Box::new(nested("foreach")),
        },
    ]);
    let scans = unbounded_scans(&diagnostics);

    assert_eq!(scans.len(), 4);
    assert_eq!(
        scans
            .iter()
            .flat_map(|scan| property_names(scan))
            .collect::<Vec<_>>(),
        ["branch_a", "branch_b", "foreach", "repeat"]
    );
}
