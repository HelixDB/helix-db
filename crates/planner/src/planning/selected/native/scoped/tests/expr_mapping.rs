use super::super::super::root_stream;
use super::support;
use crate::{error, ir, logical};
use helix_ast::expr::Predicate;
use std::num::NonZeroUsize;

#[test]
fn root_stream_from_expr_maps_supported_stream_roots() {
    let predicate = ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap();
    assert!(matches!(
        root_stream::root_stream_from_expr(
            logical::LogicalExpr::AccessPath(support::access_path())
        )
        .unwrap(),
        logical::RootStream::Access(logical::AccessStream::Path(_))
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::AccessFilter(
            logical::AccessFilter::new(support::access_path(), predicate),
        ))
        .unwrap(),
        logical::RootStream::Access(logical::AccessStream::Filter(_))
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::AccessWindow(
            logical::AccessWindow::new(
                support::access_path(),
                logical::AccessWindowRange::new(0, Some(1)).unwrap(),
            ),
        ))
        .unwrap(),
        logical::RootStream::Access(logical::AccessStream::Window(_))
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::AccessOrder(
            logical::AccessOrder::new(
                support::access_path(),
                ir::OrderKeys::from(ir::OrderKey {
                    property: support::name("age"),
                    order: helix_ast::traversal::Order::Asc,
                }),
            )
        ))
        .unwrap(),
        logical::RootStream::Access(logical::AccessStream::Order(_))
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::AccessDistinct(
            logical::AccessDistinct::new(support::access_path()),
        ))
        .unwrap(),
        logical::RootStream::Access(logical::AccessStream::Distinct(_))
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::AccessPipeline(
            logical::AccessPipeline::new(
                support::access_path(),
                ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                    count: ir::StreamBoundPlan::Literal(1),
                }),
            )
            .unwrap(),
        ))
        .unwrap(),
        logical::RootStream::Access(logical::AccessStream::Pipeline(_))
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::VariableSource(
            logical::VariableSource::new(support::name("seed")),
        ))
        .unwrap(),
        logical::RootStream::VariableSource(_)
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::RootMutation(
            logical::RootMutation::new(ir::MutationPlan::AddNode {
                input: ir::MutationInput::Source,
                label: support::name("User"),
                properties: ir::PropertyAssignments::default(),
            }),
        ))
        .unwrap(),
        logical::RootStream::Mutation(_)
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::RootBranch(
            logical::RootBranch::new(
                support::access_expr(),
                ir::BranchPlan::Optional(Box::new(support::access_expr())),
            )
        ))
        .unwrap(),
        logical::RootStream::Branch(_)
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::RootRepeat(
            logical::RootRepeat::new(
                support::access_expr(),
                ir::RepeatPlan {
                    body: Box::new(support::access_expr()),
                    stop: ir::RepeatStopPlan::MaxDepthOnly,
                    emit: ir::RepeatEmitPlan::None,
                    max_depth: NonZeroUsize::new(1).unwrap(),
                },
            )
        ))
        .unwrap(),
        logical::RootStream::Repeat(_)
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::RootPipeline(
            support::root_pipeline(),
        ))
        .unwrap(),
        logical::RootStream::Pipeline(_)
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::StreamReserved(
            logical::StreamReserved::new(support::variable_stream(), ir::ReservedOp::Fold),
        ))
        .unwrap(),
        logical::RootStream::Reserved(_)
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::StreamProject(
            logical::StreamProject::new(support::variable_stream(), ir::ProjectionPlan::Exists),
        ))
        .unwrap(),
        logical::RootStream::Project(_)
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::StreamAggregate(
            logical::StreamAggregate::new(
                support::variable_stream(),
                ir::AggregatePlan::Group(support::name("kind"))
            ),
        ))
        .unwrap(),
        logical::RootStream::Aggregate(_)
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::StreamVariableWrite(
            logical::StreamVariableWrite::new(
                support::variable_stream(),
                logical::StreamVariableWriteOp::Store(support::name("saved")),
            ),
        ))
        .unwrap(),
        logical::RootStream::VariableWrite(_)
    ));
    assert!(matches!(
        root_stream::root_stream_from_expr(logical::LogicalExpr::Pure(
            logical::PureLogicalOp::NoOp,
        )),
        Err(error::PlannerError::UnsupportedCascadesPlan { .. })
    ));
}
