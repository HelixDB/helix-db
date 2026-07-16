use super::*;

#[test]
fn pipeline_rule_implements_serial_cost_and_delivered_properties() {
    let rule = PipelineImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        cpu_predicate_eval: cost::LatencyEstimate::micros(5),
        sort_setup: cost::LatencyEstimate::micros(7),
        sort_per_row: cost::LatencyEstimate::micros(11),
        default_unknown_scan_rows: cost::EstimatedRows::rows(4),
        ..cost::StorageCostProfile::default()
    };
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();
    let expr = pipeline_expr(vec![
        logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        },
        logical::PureLogicalOp::Filter { predicate },
        logical::PureLogicalOp::Order {
            ordering: properties::RequiredOrdering::ByKeys(order_keys()),
        },
        limit(2),
    ]);

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_pure_pipeline");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::ResidualFilter,
            physical::PhysicalPipelineOp::Sort,
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
        ]
    ));
    assert_eq!(
        alternative.delivered.element,
        Some(properties::ElementKind::Node)
    );
    assert!(matches!(
        alternative.delivered.ordering,
        properties::DeliveredOrdering::ByKeys(_)
    ));
    assert_eq!(alternative.delivered.cardinality.upper(), Some(2));
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    let rows = storage.default_unknown_scan_rows;
    let expected = storage
        .range_scan(rows)
        .serial(storage.predicate_eval(rows))
        .serial(storage.explicit_sort(rows))
        .serial(storage.stream_operator(cost::EstimatedRows::rows(2)));
    assert_eq!(alternative.cost, expected);
}

#[test]
fn pipeline_rule_clamps_cardinality_across_static_windows() {
    let rule = PipelineImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = pipeline_expr(vec![
        logical::PureLogicalOp::Source {
            element: properties::ElementKind::Edge,
        },
        limit(10),
        skip(3),
        range(2, 4),
    ]);

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Edge,
                ..
            },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Skip),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
        ]
    ));
    assert_eq!(
        alternative.delivered.element,
        Some(properties::ElementKind::Edge)
    );
    assert_eq!(alternative.delivered.cardinality.upper(), Some(2));
}

#[test]
fn pipeline_rule_declines_non_pipeline_inputs() {
    let rule = PipelineImplementationRule::default();
    let storage = cost::StorageCostProfile::default();

    for expr in [
        source(properties::ElementKind::Node),
        logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::Mutation),
    ] {
        assert_eq!(
            rule.apply(optimizer::RuleInput {
                expr: &expr,
                storage: &storage,
                indexes: empty_indexes(),
                planner_limits: default_planner_limits(),
                stats: default_stats(),
            }),
            optimizer::RuleResult::NotApplicable
        );
    }
}
