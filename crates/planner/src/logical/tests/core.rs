use super::*;

#[test]
fn logical_expr_separates_pure_and_barrier_effects() {
    assert_eq!(
        LogicalExpr::Pure(PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        })
        .effect(),
        properties::EffectKind::Pure
    );
    assert_eq!(
        LogicalExpr::Barrier(BarrierLogicalOp::Mutation).effect(),
        properties::EffectKind::Barrier
    );
    assert_eq!(
        LogicalExpr::PurePipeline(PurePipeline::new(ir::AtLeast::<_, 1>::from_one(
            PureLogicalOp::Reserved,
        )))
        .effect(),
        properties::EffectKind::Pure
    );
    assert_eq!(
        LogicalExpr::FilterChain(FilterChain::new(ir::AtLeast::<_, 2>::from_pair(
            predicate(),
            predicate(),
        )))
        .effect(),
        properties::EffectKind::Pure
    );
    assert_eq!(
        LogicalExpr::FilterPushdown(FilterPushdown::new(FilterPushdownOp::Distinct, predicate(),))
            .effect(),
        properties::EffectKind::Pure
    );
    assert_eq!(
        LogicalExpr::AccessPath(node_access_path(ir::NodeAccessPlan::AllScan)).effect(),
        properties::EffectKind::Pure
    );
    assert_eq!(
        LogicalExpr::AccessFilter(AccessFilter::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            predicate(),
        ))
        .effect(),
        properties::EffectKind::Pure
    );
    assert_eq!(
        LogicalExpr::AccessWindow(AccessWindow::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            AccessWindowRange::new(0, Some(1)).unwrap(),
        ))
        .effect(),
        properties::EffectKind::Pure
    );
    assert_eq!(
        LogicalExpr::AccessOrder(AccessOrder::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::OrderKeys::from(order_key()),
        ))
        .effect(),
        properties::EffectKind::Pure
    );
    assert_eq!(
        LogicalExpr::AccessDistinct(AccessDistinct::new(node_access_path(
            ir::NodeAccessPlan::AllScan,
        )))
        .effect(),
        properties::EffectKind::Pure
    );
    assert_eq!(
        LogicalExpr::StreamReserved(StreamReserved::new(
            RootStream::Access(AccessStream::Path(node_access_path(
                ir::NodeAccessPlan::AllScan,
            ))),
            ir::ReservedOp::Path,
        ))
        .effect(),
        properties::EffectKind::Pure
    );
    assert_eq!(
        LogicalExpr::StreamVariableWrite(StreamVariableWrite::new(
            RootStream::Access(AccessStream::Path(node_access_path(
                ir::NodeAccessPlan::AllScan,
            ))),
            StreamVariableWriteOp::Store(name("users")),
        ))
        .effect(),
        properties::EffectKind::Barrier
    );
    let stateful_access_pipeline = AccessPipeline::new(
        node_access_path(ir::NodeAccessPlan::AllScan),
        ir::AtLeast::<_, 1>::from_one(StreamPipelineOp::VariableWrite {
            op: StreamVariableWriteOp::Store(name("users")),
        }),
    )
    .unwrap();
    assert_eq!(
        LogicalExpr::AccessPipeline(stateful_access_pipeline).effect(),
        properties::EffectKind::Barrier
    );
    let stateful_root_pipeline = RootPipeline::new(
        RootStream::VariableSource(VariableSource::new(name("seed"))),
        ir::AtLeast::<_, 1>::from_one(StreamPipelineOp::VariableWrite {
            op: StreamVariableWriteOp::As(name("users")),
        }),
    )
    .unwrap();
    assert_eq!(
        LogicalExpr::RootPipeline(stateful_root_pipeline.clone()).effect(),
        properties::EffectKind::Barrier
    );
    assert_eq!(
        LogicalExpr::StreamProject(StreamProject::new(
            RootStream::Pipeline(Box::new(stateful_root_pipeline)),
            ir::ProjectionPlan::Count,
        ))
        .effect(),
        properties::EffectKind::Barrier
    );
}
