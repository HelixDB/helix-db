//! Root-stream access rewrite exploration.
//!
//! Root-stream wrappers currently inline access pipelines when implemented.
//! This rule preserves the wrapper payload while reusing access-family
//! rewrites before physical lowering chooses a concrete executable pipeline.

use super::super::access::access_filter_alternatives;
use crate::{catalog, ir, logical, optimizer, rules};

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
        ir::AtLeast::<_, 1>::try_from_vec(rewrite_root_expr(input.expr, &input))
            .map(|exprs| optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(exprs)))
            .unwrap_or(optimizer::RuleResult::NotApplicable)
    }
}

fn rewrite_root_expr(
    expr: &logical::LogicalExpr,
    rule_input: &optimizer::RuleInput<'_>,
) -> Vec<logical::LogicalExpr> {
    match expr {
        logical::LogicalExpr::RootPipeline(pipeline) => {
            rewrite_root_stream(pipeline.input(), rule_input)
                .into_iter()
                .filter_map(|input| {
                    logical::RootPipeline::new(input, pipeline.ops_at_least().clone())
                        .map(logical::LogicalExpr::RootPipeline)
                })
                .collect()
        }
        logical::LogicalExpr::StreamReserved(reserved) => {
            rewrite_root_stream(reserved.input(), rule_input)
                .into_iter()
                .map(|input| {
                    logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(
                        input,
                        reserved.op().clone(),
                    ))
                })
                .collect()
        }
        logical::LogicalExpr::StreamCardinality(cardinality) => {
            rewrite_root_stream(cardinality.input(), rule_input)
                .into_iter()
                .map(|input| {
                    logical::LogicalExpr::StreamCardinality(
                        logical::StreamCardinality::new(input).with_planning_bindings(
                            cardinality.params().clone(),
                            cardinality.late_bound_params().clone(),
                        ),
                    )
                })
                .collect()
        }
        logical::LogicalExpr::StreamProject(project) => {
            rewrite_root_stream(project.input(), rule_input)
                .into_iter()
                .map(|input| {
                    logical::LogicalExpr::StreamProject(logical::StreamProject::new(
                        input,
                        project.projection().clone(),
                    ))
                })
                .collect()
        }
        logical::LogicalExpr::StreamAggregate(aggregate) => {
            rewrite_root_stream(aggregate.input(), rule_input)
                .into_iter()
                .map(|input| {
                    logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
                        input,
                        aggregate.aggregate().clone(),
                    ))
                })
                .collect()
        }
        logical::LogicalExpr::StreamVariableWrite(write) => {
            rewrite_root_stream(write.input(), rule_input)
                .into_iter()
                .map(|input| {
                    logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
                        input,
                        write.op().clone(),
                    ))
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

fn rewrite_root_stream(
    input: &logical::RootStream,
    rule_input: &optimizer::RuleInput<'_>,
) -> Vec<logical::RootStream> {
    let logical::RootStream::Access(access) = input else {
        return Vec::new();
    };
    rewrite_access_stream(access, rule_input)
        .into_iter()
        .map(logical::RootStream::Access)
        .collect()
}

fn rewrite_access_stream(
    input: &logical::AccessStream,
    rule_input: &optimizer::RuleInput<'_>,
) -> Vec<logical::AccessStream> {
    match input {
        logical::AccessStream::Filter(filter) => {
            access_filter_alternatives(filter, rule_input, &[])
        }
        logical::AccessStream::Pipeline(pipeline) => {
            if let [logical::StreamPipelineOp::Filter { predicate }, rest @ ..] = pipeline.ops() {
                let filter =
                    logical::AccessFilter::new(pipeline.access().clone(), predicate.clone());
                let rewritten = access_filter_alternatives(&filter, rule_input, rest);
                if !rewritten.is_empty() {
                    return rewritten;
                }
            }
            for op in pipeline.ops() {
                match op {
                    logical::StreamPipelineOp::Filter { .. } => {}
                    logical::StreamPipelineOp::Order { ordering } => {
                        return rewrite_order(pipeline.access(), ordering, rule_input.indexes)
                            .and_then(|access| {
                                access_stream_with_ops(access, pipeline.ops().to_vec())
                            })
                            .into_iter()
                            .collect();
                    }
                    _ => break,
                }
            }
            Vec::new()
        }
        logical::AccessStream::Order(order) => {
            rewrite_order(order.access(), order.ordering(), rule_input.indexes)
                .map(|access| {
                    logical::AccessStream::Order(logical::AccessOrder::new(
                        access,
                        order.ordering().clone(),
                    ))
                })
                .into_iter()
                .collect()
        }
        logical::AccessStream::Path(_)
        | logical::AccessStream::Window(_)
        | logical::AccessStream::Distinct(_) => Vec::new(),
    }
}

/// Root wrappers inline their access instead of optimizing a child memo group.
/// Apply the same ordered-driver exploration here while preserving the wrapper.
fn rewrite_order(
    access: &logical::AccessPath,
    ordering: &ir::OrderKeys,
    indexes: &catalog::IndexCatalogSnapshot,
) -> Option<logical::AccessPath> {
    let order = logical::AccessOrder::new(access.clone(), ordering.clone());
    match rules::access::rewrite_access_order_range_direction(&order, indexes) {
        rules::access::AccessOrderRangeDirectionRewrite::Rewritten(rewritten)
            if &rewritten != access =>
        {
            return Some(rewritten)
        }
        _ => {}
    }
    match rules::access::access_order_satisfaction(&order) {
        rules::access::AccessOrderSatisfaction::Satisfied(rewritten) if &rewritten != access => {
            Some(rewritten)
        }
        _ => None,
    }
}

fn access_stream_with_ops(
    access: logical::AccessPath,
    ops: Vec<logical::StreamPipelineOp>,
) -> Option<logical::AccessStream> {
    logical::AccessPipeline::new(access, ir::AtLeast::<_, 1>::try_from_vec(ops)?)
        .map(logical::AccessStream::Pipeline)
}
