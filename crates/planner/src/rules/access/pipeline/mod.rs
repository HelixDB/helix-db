//! Access-rooted stream-pipeline optimizer rule facade.

mod contracts;
mod filter;
mod implementation;
mod membership;
mod order;
mod simplification;
mod source_index;
mod support;

use crate::{catalog, context, ir, logical};

pub(in crate::rules) use self::membership::membership_rewrite;
pub(in crate::rules) use self::source_index::source_index_rewrite;

pub use self::{
    filter::AccessPipelineFilterRule, implementation::AccessPipelineImplementationRule,
    membership::AccessPipelineMembershipFilterRule, order::AccessPipelineOrderRule,
    simplification::AccessPipelineSimplificationRule, source_index::AccessSourceIndexFilterRule,
};

/// Whether a required stream-filter rewrite still applies to `expr`: index
/// membership for filters behind the source, or the source index for a
/// leading filter a property index serves.
///
/// The implementation rules of every kind in
/// `REQUIRED_STREAM_FILTER_KINDS` refuse `expr` exactly when this holds, so
/// an index-served filter never reaches a physical plan as a per-row filter,
/// whatever its cost, the statistics, or the exploration budget. Both
/// rewrites are idempotent and each removes the work the other leaves alone,
/// so repeated rewriting reaches an expression neither changes, which the
/// implementation rules then implement.
pub(in crate::rules) fn required_filter_rewrite_pending(
    expr: &logical::LogicalExpr,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> bool {
    membership_rewrite(expr, indexes, planner_limits).is_some()
        || source_index_rewrite(expr, indexes, planner_limits).is_some()
}

/// One required rewrite of stream filters, applied through every expression
/// kind that inlines a stream: access filters and pipelines, root pipelines,
/// and the five root-stream terminal wrappers.
///
/// Each method returns `None` when it leaves its input unchanged.
/// [`rewrite_stream_expr`] owns the walk, so every such rewrite reaches the
/// same streams, which is what lets the implementation rules of exactly
/// those kinds refuse the unrewritten forms.
trait StreamFilterRewrite {
    /// Rewrite a lone access filter.
    fn access_filter(&self, filter: &logical::AccessFilter) -> Option<logical::AccessStream>;

    /// Rewrite an access-rooted pipeline.
    fn access_pipeline(&self, pipeline: &logical::AccessPipeline) -> Option<logical::AccessStream>;

    /// Rewrite the operators of a root pipeline, which follow a complete root
    /// stream. Its input stream is rewritten separately.
    fn root_pipeline_ops(
        &self,
        pipeline: &logical::RootPipeline,
    ) -> Option<ir::AtLeast<logical::StreamPipelineOp, 1>>;
}

/// `expr` with `rewrite` applied to every stream it inlines, or `None` when
/// nothing changes.
fn rewrite_stream_expr(
    expr: &logical::LogicalExpr,
    rewrite: &impl StreamFilterRewrite,
) -> Option<logical::LogicalExpr> {
    match expr {
        logical::LogicalExpr::AccessFilter(filter) => {
            rewrite.access_filter(filter).map(access_stream_expr)
        }
        logical::LogicalExpr::AccessPipeline(pipeline) => {
            rewrite.access_pipeline(pipeline).map(access_stream_expr)
        }
        logical::LogicalExpr::RootPipeline(pipeline) => {
            rewrite_root_pipeline(pipeline, rewrite).map(logical::LogicalExpr::RootPipeline)
        }
        logical::LogicalExpr::StreamReserved(reserved) => {
            rewrite_root_stream(reserved.input(), rewrite).map(|input| {
                logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(
                    input,
                    reserved.op().clone(),
                ))
            })
        }
        logical::LogicalExpr::StreamCardinality(cardinality) => {
            rewrite_root_stream(cardinality.input(), rewrite).map(|input| {
                logical::LogicalExpr::StreamCardinality(
                    logical::StreamCardinality::new(input).with_planning_bindings(
                        cardinality.shared_params().clone(),
                        cardinality.late_bound_params().clone(),
                    ),
                )
            })
        }
        logical::LogicalExpr::StreamProject(project) => {
            rewrite_root_stream(project.input(), rewrite).map(|input| {
                logical::LogicalExpr::StreamProject(logical::StreamProject::new(
                    input,
                    project.projection().clone(),
                ))
            })
        }
        logical::LogicalExpr::StreamAggregate(aggregate) => {
            rewrite_root_stream(aggregate.input(), rewrite).map(|input| {
                logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
                    input,
                    aggregate.aggregate().clone(),
                ))
            })
        }
        logical::LogicalExpr::StreamVariableWrite(write) => {
            rewrite_root_stream(write.input(), rewrite).map(|input| {
                logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
                    input,
                    write.op().clone(),
                ))
            })
        }
        _ => None,
    }
}

fn rewrite_root_stream(
    stream: &logical::RootStream,
    rewrite: &impl StreamFilterRewrite,
) -> Option<logical::RootStream> {
    match stream {
        logical::RootStream::Access(logical::AccessStream::Filter(filter)) => rewrite
            .access_filter(filter)
            .map(logical::RootStream::Access),
        logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline)) => rewrite
            .access_pipeline(pipeline)
            .map(logical::RootStream::Access),
        logical::RootStream::Pipeline(pipeline) => rewrite_root_pipeline(pipeline, rewrite)
            .map(|pipeline| logical::RootStream::Pipeline(Box::new(pipeline))),
        _ => None,
    }
}

/// A root pipeline's operators and its input stream, rewritten together.
fn rewrite_root_pipeline(
    pipeline: &logical::RootPipeline,
    rewrite: &impl StreamFilterRewrite,
) -> Option<logical::RootPipeline> {
    match (
        rewrite.root_pipeline_ops(pipeline),
        rewrite_root_stream(pipeline.input(), rewrite),
    ) {
        (None, None) => None,
        (ops, input) => logical::RootPipeline::new(
            input.unwrap_or_else(|| pipeline.input().clone()),
            ops.unwrap_or_else(|| pipeline.ops_at_least().clone()),
        ),
    }
}

/// The logical expression of one rewritten access stream. Filter rewrites
/// only produce paths and pipelines.
fn access_stream_expr(stream: logical::AccessStream) -> logical::LogicalExpr {
    match stream {
        logical::AccessStream::Path(path) => logical::LogicalExpr::AccessPath(path),
        logical::AccessStream::Pipeline(pipeline) => logical::LogicalExpr::AccessPipeline(pipeline),
        logical::AccessStream::Filter(filter) => logical::LogicalExpr::AccessFilter(filter),
        logical::AccessStream::Order(order) => logical::LogicalExpr::AccessOrder(order),
        logical::AccessStream::Distinct(distinct) => logical::LogicalExpr::AccessDistinct(distinct),
        logical::AccessStream::Window(window) => logical::LogicalExpr::AccessWindow(window),
    }
}
