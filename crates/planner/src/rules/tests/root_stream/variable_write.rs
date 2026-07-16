use super::*;

#[test]
fn stream_variable_write_implementation_rule_appends_state_write_terminal_pipeline() {
    let rule = StreamVariableWriteImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
        logical::RootStream::Access(logical::AccessStream::Path(node_access_path(
            ir::NodeAccessPlan::AllScan,
        ))),
        logical::StreamVariableWriteOp::Store(name("users")),
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_stream_variable_write");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-variable-write pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
        ]
    ));
    assert_eq!(
        alternative.delivered.effect,
        properties::EffectKind::Barrier
    );
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .range_scan(rows)
            .serial(storage.stream_operator(rows))
    );
}

#[test]
fn stream_variable_write_implementation_rule_supports_reserved_root_streams() {
    let rule = StreamVariableWriteImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
        logical::RootStream::Reserved(Box::new(logical::StreamReserved::new(
            logical::RootStream::Access(logical::AccessStream::Path(node_access_path(
                ir::NodeAccessPlan::AllScan,
            ))),
            ir::ReservedOp::Fold,
        ))),
        logical::StreamVariableWriteOp::Store(name("cached")),
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-variable-write pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Variable
        )]
    );
    assert_eq!(
        alternative.delivered.effect,
        properties::EffectKind::Barrier
    );
    assert_eq!(
        alternative.cost,
        storage.stream_operator(cost::EstimatedRows::rows(1))
    );
}

#[test]
fn stream_variable_write_implementation_rule_supports_control_flow_reserved_root_streams() {
    let rule = StreamVariableWriteImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
        logical::RootStream::Reserved(Box::new(logical::StreamReserved::new(
            logical::RootStream::Branch(Box::new(optional_branch(
                node_all_expr(),
                edge_all_expr(),
            ))),
            ir::ReservedOp::Fold,
        ))),
        logical::StreamVariableWriteOp::Store(name("cached")),
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-variable-write pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Variable
        )]
    );
    assert_eq!(
        alternative.delivered.effect,
        properties::EffectKind::Barrier
    );
    assert_eq!(
        alternative.cost,
        storage.stream_operator(cost::EstimatedRows::rows(1))
    );
}

#[test]
fn stream_variable_write_implementation_rule_supports_control_flow_root_pipeline_stream() {
    let rule = StreamVariableWriteImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
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
    let expr = logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
        logical::RootStream::Pipeline(Box::new(pipeline)),
        logical::StreamVariableWriteOp::Store(name("expanded")),
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-variable-write pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Variable
        )]
    );
    assert_eq!(
        alternative.delivered.effect,
        properties::EffectKind::Barrier
    );
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(alternative.cost, storage.stream_operator(rows));
}
