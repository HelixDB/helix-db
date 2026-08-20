use super::*;
use crate::{error, ir, logical, planning};
use helix_ast::expr::{Expr, Predicate, StreamBound};
use helix_ast::graph::NodeRef;
use helix_ast::traversal::Order;

fn nodes() -> NativeAccessStream {
    NativeAccessStream::new(
        planning::selected::native::access::NativeAccessPath::nodes(&NodeRef::All).unwrap(),
    )
}

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).unwrap()
}

#[test]
fn native_access_stream_lowers_to_specific_single_op_contracts() {
    let filter = nodes()
        .filter(
            &crate::context::PlannerContext::default(),
            &Predicate::eq("age", 42),
        )
        .unwrap()
        .into_logical_expr()
        .unwrap();
    assert!(matches!(filter, logical::LogicalExpr::AccessFilter(_)));

    let distinct = nodes().distinct().into_logical_expr().unwrap();
    assert!(matches!(distinct, logical::LogicalExpr::AccessDistinct(_)));

    let order = nodes()
        .order(ir::OrderKeys::from(ir::OrderKey {
            property: name("age"),
            order: Order::Asc,
        }))
        .into_logical_expr()
        .unwrap();
    assert!(matches!(order, logical::LogicalExpr::AccessOrder(_)));
}

#[test]
fn native_access_stream_lowers_composed_ops_to_pipeline_contract() {
    let pipeline = nodes()
        .filter(
            &crate::context::PlannerContext::default(),
            &Predicate::eq("active", true),
        )
        .unwrap()
        .limit(&StreamBound::expr(Expr::param("limit")))
        .unwrap()
        .distinct()
        .into_logical_expr()
        .unwrap();
    assert!(matches!(
        pipeline,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(pipeline.ops(), [
                logical::StreamPipelineOp::Filter { .. },
                logical::StreamPipelineOp::Limit { count: ir::StreamBoundPlan::Expr(_) },
                logical::StreamPipelineOp::Distinct
            ])
    ));
}

#[test]
fn native_access_stream_lowers_single_pipeline_ops_to_typed_pipeline_contract() {
    let dynamic_limit = nodes()
        .limit(&StreamBound::expr(Expr::param("limit")))
        .unwrap()
        .into_logical_expr()
        .unwrap();
    assert!(matches!(
        dynamic_limit,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(pipeline.ops(), [
                logical::StreamPipelineOp::Limit {
                    count: ir::StreamBoundPlan::Expr(_)
                }
            ])
    ));

    let variable_write = nodes()
        .variable_write(logical::StreamVariableWriteOp::Store(name("stored")))
        .into_logical_expr()
        .unwrap();
    assert!(matches!(
        variable_write,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(pipeline.ops(), [
                logical::StreamPipelineOp::VariableWrite { .. }
            ])
    ));
}

#[test]
fn native_access_stream_composes_literal_windows() {
    let limit = nodes()
        .limit(&StreamBound::Literal(3))
        .unwrap()
        .into_logical_expr()
        .unwrap();
    assert!(matches!(
        limit,
        logical::LogicalExpr::AccessWindow(window)
            if window.window() == logical::AccessWindowRange::new(0, Some(3)).unwrap()
    ));

    let composed = nodes()
        .skip(&StreamBound::Literal(2))
        .unwrap()
        .limit(&StreamBound::Literal(3))
        .unwrap()
        .range(&StreamBound::Literal(1), &StreamBound::Literal(4))
        .unwrap()
        .into_logical_expr()
        .unwrap();
    assert!(matches!(
        composed,
        logical::LogicalExpr::AccessWindow(window)
            if window.window() == logical::AccessWindowRange::new(3, Some(5)).unwrap()
    ));
}

#[test]
fn native_access_stream_validates_stream_bounds() {
    let invalid_bound = nodes().limit(&StreamBound::expr(Expr::val(-1)));
    assert!(matches!(
        invalid_bound,
        Err(error::PlannerError::InvalidStreamBoundExpression { .. })
    ));

    let invalid_range = nodes().range(&StreamBound::Literal(8), &StreamBound::Literal(2));
    assert!(matches!(
        invalid_range,
        Err(error::PlannerError::InvalidStreamRange { start: 8, end: 2 })
    ));
}

#[test]
fn native_access_stream_filter_validates_predicates_and_defers_missing_bindings() {
    let ctx = crate::context::PlannerContext::default();
    for predicate in [Predicate::eq("", 1), Predicate::eq("$label", "")] {
        assert!(nodes().filter(&ctx, &predicate).is_err());
    }

    let predicate = Predicate::and(vec![
        Predicate::eq_param("status", "missing_status"),
        Predicate::is_in_param("group", "missing_groups"),
    ]);
    let filtered = nodes()
        .filter(&ctx, &predicate)
        .unwrap()
        .into_logical_expr()
        .unwrap();
    assert!(matches!(
        filtered,
        logical::LogicalExpr::AccessFilter(filter)
            if filter.predicate() == &ir::PredicatePlan::new(predicate).unwrap()
    ));
}
