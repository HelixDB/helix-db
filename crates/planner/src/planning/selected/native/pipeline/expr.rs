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

/// The stream's access path and operators, moved out of it: composing an
/// N-step chain one operator at a time must not copy the steps before it.
fn access_stream_parts(
    input: logical::AccessStream,
) -> (logical::AccessPath, Vec<logical::StreamPipelineOp>) {
    match input {
        logical::AccessStream::Path(access) => (access, Vec::new()),
        logical::AccessStream::Filter(filter) => {
            let (access, predicate) = filter.into_parts();
            (
                access,
                vec![logical::StreamPipelineOp::Filter { predicate }],
            )
        }
        logical::AccessStream::Window(window) => {
            let (access, window) = window.into_parts();
            (access, vec![logical::StreamPipelineOp::Window { window }])
        }
        logical::AccessStream::Order(order) => {
            let (access, ordering) = order.into_parts();
            (access, vec![logical::StreamPipelineOp::Order { ordering }])
        }
        logical::AccessStream::Distinct(distinct) => (
            distinct.into_access(),
            vec![logical::StreamPipelineOp::Distinct],
        ),
        logical::AccessStream::Pipeline(pipeline) => {
            let (access, ops) = pipeline.into_parts();
            (access, ops.into_iter().collect())
        }
    }
}

fn root_pipeline(
    input: logical::RootStream,
    op: logical::StreamPipelineOp,
) -> Result<logical::LogicalExpr, error::PlannerError> {
    let (input, ops) = match input {
        logical::RootStream::Pipeline(pipeline) => {
            let (input, ops) = pipeline.into_parts();
            (input, ops.into_iter().collect())
        }
        input => (input, Vec::new()),
    };
    let ops = append_pipeline_op(ops, op);
    logical::RootPipeline::new(input, ops)
        .map(logical::LogicalExpr::RootPipeline)
        .ok_or_else(|| rejection::unsupported(NativeUnsupportedReason::RootPipelineNonCanonical))
}

fn append_pipeline_op(
    mut ops: Vec<logical::StreamPipelineOp>,
    op: logical::StreamPipelineOp,
) -> ir::AtLeast<logical::StreamPipelineOp, 1> {
    ops.push(op);
    ir::AtLeast::try_from_vec(ops).expect("an appended operator makes the pipeline non-empty")
}
