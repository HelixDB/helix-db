use super::*;

#[test]
fn stream_aggregate_implementation_rule_appends_materializing_terminal_pipeline() {
    let rule = StreamAggregateImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        sort_setup: cost::LatencyEstimate::micros(13),
        sort_per_row: cost::LatencyEstimate::micros(17),
        default_unknown_scan_rows: cost::EstimatedRows::rows(9),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
        logical::RootStream::Access(logical::AccessStream::Order(logical::AccessOrder::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            order_keys(),
        ))),
        ir::AggregatePlan::Group(name("kind")),
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_stream_aggregate");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-aggregate pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::Sort,
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Aggregate),
        ]
    ));
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .range_scan(rows)
            .serial(storage.explicit_sort(rows))
            .serial(storage.explicit_sort(rows))
    );
}

#[test]
fn stream_aggregate_implementation_rule_supports_reserved_root_streams() {
    let rule = StreamAggregateImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        sort_setup: cost::LatencyEstimate::micros(13),
        sort_per_row: cost::LatencyEstimate::micros(17),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
        logical::RootStream::Reserved(Box::new(logical::StreamReserved::new(
            logical::RootStream::Access(logical::AccessStream::Path(node_access_path(
                ir::NodeAccessPlan::AllScan,
            ))),
            ir::ReservedOp::Fold,
        ))),
        ir::AggregatePlan::Group(name("kind")),
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-aggregate pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Aggregate
        )]
    );
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    assert_eq!(
        alternative.cost,
        storage.explicit_sort(cost::EstimatedRows::rows(1))
    );
}

#[test]
fn stream_aggregate_implementation_rule_supports_control_flow_reserved_root_streams() {
    let rule = StreamAggregateImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        sort_setup: cost::LatencyEstimate::micros(13),
        sort_per_row: cost::LatencyEstimate::micros(17),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
        logical::RootStream::Reserved(Box::new(logical::StreamReserved::new(
            logical::RootStream::Branch(Box::new(optional_branch(
                node_all_expr(),
                edge_all_expr(),
            ))),
            ir::ReservedOp::Fold,
        ))),
        ir::AggregatePlan::Group(name("kind")),
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-aggregate pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Aggregate
        )]
    );
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    assert_eq!(
        alternative.cost,
        storage.explicit_sort(cost::EstimatedRows::rows(1))
    );
}

#[test]
fn stream_aggregate_implementation_rule_supports_control_flow_root_pipeline_stream() {
    let rule = StreamAggregateImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        sort_setup: cost::LatencyEstimate::micros(13),
        sort_per_row: cost::LatencyEstimate::micros(17),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expand = ir::ExpandPlan {
        direction: ir::ExpandDirection::Out,
        output: ir::ExpandOutput::Edges,
        label: ir::ExpandLabelPlan::Label(name("LIKES")),
    };
    let pipeline = logical::RootPipeline::new(
        logical::RootStream::Branch(Box::new(optional_branch(node_all_expr(), edge_all_expr()))),
        ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Expand { plan: expand }),
    )
    .unwrap();
    let expr = logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
        logical::RootStream::Pipeline(Box::new(pipeline)),
        ir::AggregatePlan::Group(name("kind")),
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-aggregate pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Aggregate
        )]
    );
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(alternative.cost, storage.explicit_sort(rows));
}
