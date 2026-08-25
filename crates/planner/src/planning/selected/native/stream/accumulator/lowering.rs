//! Native access-stream lowering into logical access ADTs.

use super::super::super::rejection::{self, NativeUnsupportedReason};
use super::NativeAccessStream;
use crate::{error, ir, logical};

enum NativeAccessStreamOps {
    Empty,
    Single(logical::StreamPipelineOp),
    Pipeline(ir::AtLeast<logical::StreamPipelineOp, 1>),
}

impl NativeAccessStreamOps {
    fn from_vec(ops: Vec<logical::StreamPipelineOp>) -> Self {
        let mut ops = logical::canonicalize_stream_pipeline_ops(ops).into_iter();
        match ops.next() {
            None => Self::Empty,
            Some(first) => {
                let rest = ops.collect::<Vec<_>>();
                if rest.is_empty() {
                    Self::Single(first)
                } else {
                    Self::Pipeline(ir::AtLeast::from_one_and_rest(first, rest))
                }
            }
        }
    }
}

impl NativeAccessStream {
    /// Lower to the most specific logical access expression.
    pub(in crate::planning::selected::native) fn into_logical_expr(
        self,
    ) -> Result<logical::LogicalExpr, error::PlannerError> {
        self.into_access_stream().map(|stream| match stream {
            logical::AccessStream::Path(access) => logical::LogicalExpr::AccessPath(access),
            logical::AccessStream::Filter(filter) => logical::LogicalExpr::AccessFilter(filter),
            logical::AccessStream::Window(window) => logical::LogicalExpr::AccessWindow(window),
            logical::AccessStream::Order(order) => logical::LogicalExpr::AccessOrder(order),
            logical::AccessStream::Distinct(distinct) => {
                logical::LogicalExpr::AccessDistinct(distinct)
            }
            logical::AccessStream::Pipeline(pipeline) => {
                logical::LogicalExpr::AccessPipeline(pipeline)
            }
        })
    }

    /// Lower to a supported root stream for terminal consumption.
    pub(in crate::planning::selected::native) fn into_root_stream(
        self,
    ) -> Result<logical::RootStream, error::PlannerError> {
        self.into_access_stream().map(logical::RootStream::Access)
    }

    fn into_access_stream(self) -> Result<logical::AccessStream, error::PlannerError> {
        match NativeAccessStreamOps::from_vec(self.ops) {
            NativeAccessStreamOps::Empty => Ok(logical::AccessStream::Path(self.access)),
            NativeAccessStreamOps::Single(op) => Ok(match op {
                logical::StreamPipelineOp::Filter { predicate } => logical::AccessStream::Filter(
                    logical::AccessFilter::new(self.access, predicate),
                ),
                logical::StreamPipelineOp::Window { window } => {
                    logical::AccessStream::Window(logical::AccessWindow::new(self.access, window))
                }
                logical::StreamPipelineOp::Order { ordering } => {
                    logical::AccessStream::Order(logical::AccessOrder::new(self.access, ordering))
                }
                logical::StreamPipelineOp::Distinct => {
                    logical::AccessStream::Distinct(logical::AccessDistinct::new(self.access))
                }
                op => access_pipeline(self.access, ir::AtLeast::from_one(op))
                    .map(logical::AccessStream::Pipeline)?,
            }),
            NativeAccessStreamOps::Pipeline(ops) => {
                access_pipeline(self.access, ops).map(logical::AccessStream::Pipeline)
            }
        }
    }
}

fn access_pipeline(
    access: logical::AccessPath,
    ops: ir::AtLeast<logical::StreamPipelineOp, 1>,
) -> Result<logical::AccessPipeline, error::PlannerError> {
    logical::AccessPipeline::new(access, ops).ok_or_else(|| {
        rejection::unsupported(NativeUnsupportedReason::AccessStreamNonCanonicalPipeline)
    })
}
