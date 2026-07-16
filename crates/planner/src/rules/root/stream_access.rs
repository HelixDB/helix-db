//! Root-stream access rewrite exploration.
//!
//! Root-stream wrappers currently inline access pipelines when implemented.
//! This rule preserves the wrapper payload while reusing access-family
//! rewrites before physical lowering chooses a concrete executable pipeline.

use super::super::access::{index_access_filter, simplify_access_filter, AccessFilterRewrite};
use crate::{catalog, context, ir, logical, optimizer, rules};

/// Push access-filter rewrites through root-stream wrappers.
pub struct RootStreamAccessRewriteRule {
    metadata: rules::RuleMetadata,
}

impl Default for RootStreamAccessRewriteRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::RootStreamAccessRewrite),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for RootStreamAccessRewriteRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        rewrite_root_expr(input.expr, input.indexes, input.planner_limits)
            .map(logical_result)
            .unwrap_or(optimizer::RuleResult::NotApplicable)
    }
}

fn rewrite_root_expr(
    expr: &logical::LogicalExpr,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Option<logical::LogicalExpr> {
    match expr {
        logical::LogicalExpr::RootPipeline(pipeline) => {
            rewrite_root_stream(pipeline.input(), indexes, planner_limits).and_then(|input| {
                logical::RootPipeline::new(input, pipeline.ops_at_least().clone())
                    .map(logical::LogicalExpr::RootPipeline)
            })
        }
        logical::LogicalExpr::StreamReserved(reserved) => {
            rewrite_root_stream(reserved.input(), indexes, planner_limits).map(|input| {
                logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(
                    input,
                    reserved.op().clone(),
                ))
            })
        }
        logical::LogicalExpr::StreamProject(project) => {
            rewrite_root_stream(project.input(), indexes, planner_limits).map(|input| {
                logical::LogicalExpr::StreamProject(logical::StreamProject::new(
                    input,
                    project.projection().clone(),
                ))
            })
        }
        logical::LogicalExpr::StreamAggregate(aggregate) => {
            rewrite_root_stream(aggregate.input(), indexes, planner_limits).map(|input| {
                logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
                    input,
                    aggregate.aggregate().clone(),
                ))
            })
        }
        logical::LogicalExpr::StreamVariableWrite(write) => {
            rewrite_root_stream(write.input(), indexes, planner_limits).map(|input| {
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
    input: &logical::RootStream,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Option<logical::RootStream> {
    let logical::RootStream::Access(access) = input else {
        return None;
    };
    rewrite_access_stream(access, indexes, planner_limits).map(logical::RootStream::Access)
}

fn rewrite_access_stream(
    input: &logical::AccessStream,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Option<logical::AccessStream> {
    match input {
        logical::AccessStream::Filter(filter) => {
            rewrite_filter(filter, indexes, planner_limits, &[])
        }
        logical::AccessStream::Pipeline(pipeline) => {
            let [logical::StreamPipelineOp::Filter { predicate }, rest @ ..] = pipeline.ops()
            else {
                return None;
            };
            let filter = logical::AccessFilter::new(pipeline.access().clone(), predicate.clone());
            rewrite_filter(&filter, indexes, planner_limits, rest)
        }
        logical::AccessStream::Path(_)
        | logical::AccessStream::Window(_)
        | logical::AccessStream::Order(_)
        | logical::AccessStream::Distinct(_) => None,
    }
}

fn rewrite_filter(
    filter: &logical::AccessFilter,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
    suffix: &[logical::StreamPipelineOp],
) -> Option<logical::AccessStream> {
    let rewrite = simplify_access_filter(filter)
        .or_else(|| index_access_filter(filter, indexes, planner_limits));
    access_stream_from_rewrite(rewrite, suffix)
}

fn access_stream_from_rewrite(
    rewrite: AccessFilterRewrite,
    suffix: &[logical::StreamPipelineOp],
) -> Option<logical::AccessStream> {
    match rewrite {
        AccessFilterRewrite::Rewritten(access) => access_stream_with_suffix(access, suffix),
        AccessFilterRewrite::RewrittenPipeline(pipeline) => {
            let mut ops = pipeline.ops().to_vec();
            ops.extend_from_slice(suffix);
            access_stream_with_ops(pipeline.access().clone(), ops)
        }
        AccessFilterRewrite::NotApplicable => None,
    }
}

fn access_stream_with_suffix(
    access: logical::AccessPath,
    suffix: &[logical::StreamPipelineOp],
) -> Option<logical::AccessStream> {
    if suffix.is_empty() {
        Some(logical::AccessStream::Path(access))
    } else {
        access_stream_with_ops(access, suffix.to_vec())
    }
}

fn access_stream_with_ops(
    access: logical::AccessPath,
    ops: Vec<logical::StreamPipelineOp>,
) -> Option<logical::AccessStream> {
    logical::AccessPipeline::new(access, ir::AtLeast::<_, 1>::try_from_vec(ops)?)
        .map(logical::AccessStream::Pipeline)
}

fn logical_result(expr: logical::LogicalExpr) -> optimizer::RuleResult {
    optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
        ir::AtLeast::<_, 1>::from_one(expr),
    ))
}
