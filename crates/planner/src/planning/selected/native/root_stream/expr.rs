//! Pure logical-expression to root-stream conversion.

use crate::planning::selected::native::rejection::{self, NativeUnsupportedReason};
use crate::{error, logical};

pub(in crate::planning::selected::native) fn root_stream_from_expr(
    expr: logical::LogicalExpr,
) -> Result<logical::RootStream, error::PlannerError> {
    match expr {
        logical::LogicalExpr::AccessPath(access) => Ok(logical::RootStream::Access(
            logical::AccessStream::Path(access),
        )),
        logical::LogicalExpr::AccessFilter(filter) => Ok(logical::RootStream::Access(
            logical::AccessStream::Filter(filter),
        )),
        logical::LogicalExpr::AccessWindow(window) => Ok(logical::RootStream::Access(
            logical::AccessStream::Window(window),
        )),
        logical::LogicalExpr::AccessOrder(order) => Ok(logical::RootStream::Access(
            logical::AccessStream::Order(order),
        )),
        logical::LogicalExpr::AccessDistinct(distinct) => Ok(logical::RootStream::Access(
            logical::AccessStream::Distinct(distinct),
        )),
        logical::LogicalExpr::AccessPipeline(pipeline) => Ok(logical::RootStream::Access(
            logical::AccessStream::Pipeline(pipeline),
        )),
        logical::LogicalExpr::VariableSource(source) => {
            Ok(logical::RootStream::VariableSource(source))
        }
        logical::LogicalExpr::RootMutation(mutation) => {
            Ok(logical::RootStream::Mutation(Box::new(mutation)))
        }
        logical::LogicalExpr::RootBranch(branch) => {
            Ok(logical::RootStream::Branch(Box::new(branch)))
        }
        logical::LogicalExpr::RootRepeat(repeat) => {
            Ok(logical::RootStream::Repeat(Box::new(repeat)))
        }
        logical::LogicalExpr::RootPipeline(pipeline) => {
            Ok(logical::RootStream::Pipeline(Box::new(pipeline)))
        }
        logical::LogicalExpr::StreamReserved(reserved) => {
            Ok(logical::RootStream::Reserved(Box::new(reserved)))
        }
        logical::LogicalExpr::StreamProject(project) => {
            Ok(logical::RootStream::Project(Box::new(project)))
        }
        logical::LogicalExpr::StreamCardinality(cardinality) => {
            Ok(logical::RootStream::Cardinality(Box::new(cardinality)))
        }
        logical::LogicalExpr::StreamAggregate(aggregate) => {
            Ok(logical::RootStream::Aggregate(Box::new(aggregate)))
        }
        logical::LogicalExpr::StreamVariableWrite(write) => {
            Ok(logical::RootStream::VariableWrite(Box::new(write)))
        }
        _ => Err(rejection::unsupported(
            NativeUnsupportedReason::RootStreamUnsupportedExpression,
        )),
    }
}
