use super::*;

#[test]
fn stream_reserved_implementation_rule_appends_payload_terminal_pipeline() {
    let rule = StreamReservedImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(
        logical::RootStream::Access(logical::AccessStream::Path(node_access_path(
            ir::NodeAccessPlan::AllScan,
        ))),
        ir::ReservedOp::Fold,
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_stream_reserved");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical stream-reserved pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Reserved),
        ]
    ));
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    assert_eq!(alternative.delivered.cardinality.upper(), Some(1));
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .range_scan(rows)
            .serial(storage.stream_operator(rows))
    );
}
