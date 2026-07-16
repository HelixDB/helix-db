use super::*;

#[test]
fn stream_project_implementation_rule_appends_typed_terminal_pipeline() {
    let rule = StreamProjectImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        cpu_predicate_eval: cost::LatencyEstimate::micros(5),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();
    let expr = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        logical::RootStream::Access(logical::AccessStream::Filter(logical::AccessFilter::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            predicate,
        ))),
        ir::ProjectionPlan::Count,
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_stream_project");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-project pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::ResidualFilter,
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project),
        ]
    ));
    assert_eq!(alternative.delivered.cardinality.upper(), Some(1));
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .range_scan(rows)
            .serial(storage.predicate_eval(rows))
            .serial(storage.stream_operator(rows))
    );
}

#[test]
fn stream_project_implementation_rule_supports_variable_source_root_stream() {
    let rule = StreamProjectImplementationRule::default();
    let storage = cost::StorageCostProfile {
        source_inject_overhead: cost::LatencyEstimate::micros(13),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        logical::RootStream::VariableSource(logical::VariableSource::new(name("users"))),
        ir::ProjectionPlan::Count,
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-project pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project),
        ]
    );
    assert_eq!(alternative.delivered.cardinality.upper(), Some(1));
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .source_inject()
            .serial(storage.stream_operator(rows))
    );
}

#[test]
fn stream_project_implementation_rule_supports_control_flow_root_stream() {
    let rule = StreamProjectImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        logical::RootStream::Branch(Box::new(optional_branch(node_all_expr(), edge_all_expr()))),
        ir::ProjectionPlan::Count,
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-project pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Project
        )]
    );
    assert_eq!(alternative.delivered.cardinality.upper(), Some(1));
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(alternative.cost, storage.stream_operator(rows));
}

#[test]
fn stream_project_implementation_rule_supports_control_flow_root_pipeline_stream() {
    let rule = StreamProjectImplementationRule::default();
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
        ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Expand {
            plan: expand.clone(),
        }),
    )
    .unwrap();
    let expr = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        logical::RootStream::Pipeline(Box::new(pipeline)),
        ir::ProjectionPlan::Count,
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-project pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Project
        )]
    );
    assert_eq!(alternative.delivered.cardinality.upper(), Some(1));
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(alternative.cost, storage.stream_operator(rows));
}

#[test]
fn stream_project_implementation_rule_supports_reserved_root_stream() {
    let rule = StreamProjectImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        logical::RootStream::Reserved(Box::new(logical::StreamReserved::new(
            logical::RootStream::Access(logical::AccessStream::Path(node_access_path(
                ir::NodeAccessPlan::AllScan,
            ))),
            ir::ReservedOp::Fold,
        ))),
        ir::ProjectionPlan::Count,
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-project pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Project
        )]
    );
    assert_eq!(alternative.delivered.cardinality.upper(), Some(1));
    assert_eq!(
        alternative.cost,
        storage.stream_operator(cost::EstimatedRows::rows(1))
    );
}

#[test]
fn stream_project_implementation_rule_supports_control_flow_reserved_root_stream() {
    let rule = StreamProjectImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        logical::RootStream::Reserved(Box::new(logical::StreamReserved::new(
            logical::RootStream::Branch(Box::new(optional_branch(
                node_all_expr(),
                edge_all_expr(),
            ))),
            ir::ReservedOp::Fold,
        ))),
        ir::ProjectionPlan::Count,
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-project pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Project
        )]
    );
    assert_eq!(alternative.delivered.cardinality.upper(), Some(1));
    assert_eq!(
        alternative.cost,
        storage.stream_operator(cost::EstimatedRows::rows(1))
    );
}
