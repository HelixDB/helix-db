use super::*;

#[test]
fn stream_and_order_rules_encode_properties_and_use_tunable_costs() {
    let stream = StreamImplementationRule::default();
    let order = OrderImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        sort_setup: cost::LatencyEstimate::micros(7),
        sort_per_row: cost::LatencyEstimate::micros(11),
        default_unknown_scan_rows: cost::EstimatedRows::rows(5),
        ..cost::StorageCostProfile::default()
    };
    let limit = logical::LogicalExpr::Pure(logical::PureLogicalOp::Limit {
        count: ir::StreamBoundPlan::Literal(2),
    });
    let ordered = logical::LogicalExpr::Pure(logical::PureLogicalOp::Order {
        ordering: properties::RequiredOrdering::Any,
    });

    let limit = physical_alternative(stream.apply(optimizer::RuleInput {
        expr: &limit,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let ordered = physical_alternative(order.apply(optimizer::RuleInput {
        expr: &ordered,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(limit.delivered.cardinality.upper(), Some(2));
    assert_eq!(limit.cost.latency, cost::LatencyEstimate::micros(6));
    assert_eq!(
        ordered.delivered.materialization,
        properties::Materialization::Materialized
    );
    assert_eq!(ordered.cost.latency, cost::LatencyEstimate::micros(62));
}

#[test]
fn stream_rule_covers_all_stream_contracts_and_dynamic_bounds() {
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(2),
        source_inject_overhead: cost::LatencyEstimate::micros(13),
        sort_setup: cost::LatencyEstimate::micros(5),
        sort_per_row: cost::LatencyEstimate::micros(7),
        default_unknown_scan_rows: cost::EstimatedRows::rows(4),
        ..cost::StorageCostProfile::default()
    };
    let dynamic_range = ir::StreamRangePlan::new(
        helix_ast::expr::StreamBound::expr(helix_ast::expr::Expr::param("start")),
        helix_ast::expr::StreamBound::Literal(8),
    )
    .unwrap();

    let cases = [
        (
            logical::PureLogicalOp::Skip {
                count: ir::StreamBoundPlan::Literal(1),
            },
            physical::PhysicalStreamOp::Skip,
            properties::Materialization::Streaming,
            None,
            cost::LatencyEstimate::micros(8),
        ),
        (
            logical::PureLogicalOp::Range {
                range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(1, 3).unwrap()),
            },
            physical::PhysicalStreamOp::Range,
            properties::Materialization::Streaming,
            Some(2),
            cost::LatencyEstimate::micros(4),
        ),
        (
            logical::PureLogicalOp::Range {
                range: dynamic_range,
            },
            physical::PhysicalStreamOp::Range,
            properties::Materialization::Streaming,
            None,
            cost::LatencyEstimate::micros(8),
        ),
        (
            logical::PureLogicalOp::Distinct,
            physical::PhysicalStreamOp::Distinct,
            properties::Materialization::Materialized,
            None,
            cost::LatencyEstimate::micros(33),
        ),
        (
            logical::PureLogicalOp::Expand {
                element: properties::ElementKind::Edge,
            },
            physical::PhysicalStreamOp::Expand,
            properties::Materialization::Streaming,
            None,
            cost::LatencyEstimate::micros(8),
        ),
        (
            logical::PureLogicalOp::Project,
            physical::PhysicalStreamOp::Project,
            properties::Materialization::Streaming,
            None,
            cost::LatencyEstimate::micros(8),
        ),
        (
            logical::PureLogicalOp::Aggregate,
            physical::PhysicalStreamOp::Aggregate,
            properties::Materialization::Materialized,
            None,
            cost::LatencyEstimate::micros(33),
        ),
        (
            logical::PureLogicalOp::Variable,
            physical::PhysicalStreamOp::Variable,
            properties::Materialization::Streaming,
            None,
            cost::LatencyEstimate::micros(13),
        ),
        (
            logical::PureLogicalOp::Reserved,
            physical::PhysicalStreamOp::Reserved,
            properties::Materialization::Streaming,
            None,
            cost::LatencyEstimate::micros(8),
        ),
    ];

    for (op, expected_op, materialization, upper, latency) in cases {
        let alternative = stream_alternative(op, &storage);
        assert!(matches!(
            alternative.expr,
            physical::PhysicalExpr::Stream(op) if op == expected_op
        ));
        assert_eq!(alternative.delivered.materialization, materialization);
        assert_eq!(alternative.delivered.cardinality.upper(), upper);
        assert_eq!(alternative.cost.latency, latency);
    }

    let stream_rule = StreamImplementationRule::default();
    let not_stream = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    });
    assert_eq!(
        stream_rule.apply(optimizer::RuleInput {
            expr: &not_stream,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}

#[test]
fn order_rule_preserves_requested_key_order_and_rejects_non_order_exprs() {
    let rule = OrderImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let ordered = logical::LogicalExpr::Pure(logical::PureLogicalOp::Order {
        ordering: properties::RequiredOrdering::ByKeys(order_keys()),
    });
    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &ordered,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        alternative.delivered.ordering,
        properties::DeliveredOrdering::ByKeys(_)
    ));
    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &source(properties::ElementKind::Node),
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}
