//! Composition of recognized stream operators into logical pipeline ADTs.

use super::super::rejection::{self, NativeUnsupportedReason};
use crate::{error, ir, logical};

pub(in crate::planning::selected::native) fn pipeline_expr(
    input: logical::RootStream,
    op: logical::StreamPipelineOp,
) -> Result<logical::LogicalExpr, error::PlannerError> {
    match input {
        logical::RootStream::Access(access) => access_pipeline(access, op),
        input => root_pipeline(input, op),
    }
}

fn access_pipeline(
    input: logical::AccessStream,
    op: logical::StreamPipelineOp,
) -> Result<logical::LogicalExpr, error::PlannerError> {
    let (access, ops) = access_stream_parts(input);
    let ops = append_pipeline_op(ops, op);
    logical::AccessPipeline::new(access, ops)
        .map(logical::LogicalExpr::AccessPipeline)
        .ok_or_else(|| rejection::unsupported(NativeUnsupportedReason::AccessPipelineNonCanonical))
}

fn access_stream_parts(
    input: logical::AccessStream,
) -> (logical::AccessPath, Vec<logical::StreamPipelineOp>) {
    match input {
        logical::AccessStream::Path(access) => (access, Vec::new()),
        logical::AccessStream::Filter(filter) => (
            filter.access().clone(),
            vec![logical::StreamPipelineOp::Filter {
                predicate: filter.predicate().clone(),
            }],
        ),
        logical::AccessStream::Window(window) => (
            window.access().clone(),
            vec![logical::StreamPipelineOp::Window {
                window: window.window(),
            }],
        ),
        logical::AccessStream::Order(order) => (
            order.access().clone(),
            vec![logical::StreamPipelineOp::Order {
                ordering: order.ordering().clone(),
            }],
        ),
        logical::AccessStream::Distinct(distinct) => (
            distinct.access().clone(),
            vec![logical::StreamPipelineOp::Distinct],
        ),
        logical::AccessStream::Pipeline(pipeline) => {
            (pipeline.access().clone(), pipeline.ops().to_vec())
        }
    }
}

fn root_pipeline(
    input: logical::RootStream,
    op: logical::StreamPipelineOp,
) -> Result<logical::LogicalExpr, error::PlannerError> {
    let (input, ops) = match input {
        logical::RootStream::Pipeline(pipeline) => {
            (pipeline.input().clone(), pipeline.ops().to_vec())
        }
        input => (input, Vec::new()),
    };
    let ops = append_pipeline_op(ops, op);
    logical::RootPipeline::new(input, ops)
        .map(logical::LogicalExpr::RootPipeline)
        .ok_or_else(|| rejection::unsupported(NativeUnsupportedReason::RootPipelineNonCanonical))
}

fn append_pipeline_op(
    existing: Vec<logical::StreamPipelineOp>,
    op: logical::StreamPipelineOp,
) -> ir::AtLeast<logical::StreamPipelineOp, 1> {
    let mut existing = existing.into_iter();
    match existing.next() {
        Some(first) => {
            let mut rest = existing.collect::<Vec<_>>();
            rest.push(op);
            ir::AtLeast::from_one_and_rest(first, rest)
        }
        None => ir::AtLeast::from_one(op),
    }
}
