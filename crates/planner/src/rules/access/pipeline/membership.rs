//! Row-preserving index membership for filters after the pipeline source.
//!
//! A leading filter is rewritten into an index access path by
//! [`super::AccessPipelineFilterRule`]. A filter behind an expansion cannot
//! change the access path, so this rule replaces it with an
//! [`logical::StreamPipelineOp::IndexMembership`] step plus a residual filter
//! for unindexed conjuncts. Root wrappers inline their streams, so the rule
//! also rewrites the pipeline inside them. Costing decides whether the
//! membership alternative beats the per-row filter.

use super::super::filter::{index_membership_filter, MembershipFilterRewrite};
use crate::{catalog, context, logical, optimizer, properties, rules};

/// Rewrite a node-stream filter behind the pipeline source into index
/// membership.
pub struct AccessPipelineMembershipFilterRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessPipelineMembershipFilterRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessPipelineMembershipFilter),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessPipelineMembershipFilterRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let rewrite = Rewrite {
            indexes: input.indexes,
            planner_limits: input.planner_limits,
        };
        rewrite
            .expr(input.expr)
            .map(rules::logical_result)
            .unwrap_or(optimizer::RuleResult::NotApplicable)
    }
}

struct Rewrite<'a> {
    indexes: &'a catalog::IndexCatalogSnapshot,
    planner_limits: &'a context::PlannerLimits,
}

impl Rewrite<'_> {
    fn expr(&self, expr: &logical::LogicalExpr) -> Option<logical::LogicalExpr> {
        match expr {
            logical::LogicalExpr::AccessPipeline(pipeline) => self
                .access_pipeline(pipeline)
                .map(logical::LogicalExpr::AccessPipeline),
            logical::LogicalExpr::RootPipeline(pipeline) => self
                .root_pipeline(pipeline)
                .map(logical::LogicalExpr::RootPipeline),
            logical::LogicalExpr::StreamReserved(reserved) => {
                self.root_stream(reserved.input()).map(|input| {
                    logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(
                        input,
                        reserved.op().clone(),
                    ))
                })
            }
            logical::LogicalExpr::StreamCardinality(cardinality) => {
                self.root_stream(cardinality.input()).map(|input| {
                    logical::LogicalExpr::StreamCardinality(
                        logical::StreamCardinality::new(input).with_planning_bindings(
                            cardinality.params().clone(),
                            cardinality.late_bound_params().clone(),
                        ),
                    )
                })
            }
            logical::LogicalExpr::StreamProject(project) => {
                self.root_stream(project.input()).map(|input| {
                    logical::LogicalExpr::StreamProject(logical::StreamProject::new(
                        input,
                        project.projection().clone(),
                    ))
                })
            }
            logical::LogicalExpr::StreamAggregate(aggregate) => {
                self.root_stream(aggregate.input()).map(|input| {
                    logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
                        input,
                        aggregate.aggregate().clone(),
                    ))
                })
            }
            logical::LogicalExpr::StreamVariableWrite(write) => {
                self.root_stream(write.input()).map(|input| {
                    logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
                        input,
                        write.op().clone(),
                    ))
                })
            }
            _ => None,
        }
    }

    fn root_stream(&self, stream: &logical::RootStream) -> Option<logical::RootStream> {
        match stream {
            logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline)) => {
                self.access_pipeline(pipeline).map(|pipeline| {
                    logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline))
                })
            }
            logical::RootStream::Pipeline(pipeline) => self
                .root_pipeline(pipeline)
                .map(|pipeline| logical::RootStream::Pipeline(Box::new(pipeline))),
            _ => None,
        }
    }

    /// The leading filter belongs to the source-index rule, so candidates
    /// start after the first operator.
    fn access_pipeline(
        &self,
        pipeline: &logical::AccessPipeline,
    ) -> Option<logical::AccessPipeline> {
        let ops = self.ops(Some(pipeline.access().element()), pipeline.ops(), 1)?;
        logical::AccessPipeline::new(pipeline.access().clone(), ops)
    }

    /// A root pipeline follows a complete root stream, so its first filter is
    /// already behind that stream's source. Its own operators are tried
    /// before the input stream.
    fn root_pipeline(&self, pipeline: &logical::RootPipeline) -> Option<logical::RootPipeline> {
        match self.ops(root_stream_element(pipeline.input()), pipeline.ops(), 0) {
            Some(ops) => logical::RootPipeline::new(pipeline.input().clone(), ops),
            None => logical::RootPipeline::new(
                self.root_stream(pipeline.input())?,
                pipeline.ops_at_least().clone(),
            ),
        }
    }

    /// Replace the first rewritable node-stream filter at or after
    /// `first_candidate`, keeping every other operator in place.
    fn ops(
        &self,
        element: Option<properties::ElementKind>,
        ops: &[logical::StreamPipelineOp],
        first_candidate: usize,
    ) -> Option<crate::ir::AtLeast<logical::StreamPipelineOp, 1>> {
        let (position, rewrite) = ops
            .iter()
            .scan(element, |element, op| {
                let input = *element;
                *element = element_after(input, op);
                Some((op, input))
            })
            .enumerate()
            .skip(first_candidate)
            .find_map(|(position, (op, input))| match (op, input) {
                (
                    logical::StreamPipelineOp::Filter { predicate },
                    Some(properties::ElementKind::Node),
                ) => index_membership_filter(predicate, self.indexes, self.planner_limits)
                    .map(|rewrite| (position, rewrite)),
                _ => None,
            })?;
        let MembershipFilterRewrite {
            membership,
            residual,
        } = rewrite;
        let rewritten = ops[..position]
            .iter()
            .cloned()
            .chain(core::iter::once(
                logical::StreamPipelineOp::IndexMembership {
                    plan: Box::new(membership),
                },
            ))
            .chain(residual.map(|predicate| logical::StreamPipelineOp::Filter { predicate }))
            .chain(ops[position + 1..].iter().cloned())
            .collect();
        crate::ir::AtLeast::try_from_vec(rewritten)
    }
}

/// Element family known to flow out of a root stream, if any.
fn root_stream_element(stream: &logical::RootStream) -> Option<properties::ElementKind> {
    match stream {
        logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline)) => pipeline
            .ops()
            .iter()
            .fold(Some(pipeline.access().element()), element_after),
        logical::RootStream::Access(access) => Some(access.access().element()),
        logical::RootStream::Pipeline(pipeline) => pipeline
            .ops()
            .iter()
            .fold(root_stream_element(pipeline.input()), element_after),
        logical::RootStream::VariableSource(_)
        | logical::RootStream::Mutation(_)
        | logical::RootStream::Branch(_)
        | logical::RootStream::Repeat(_)
        | logical::RootStream::Reserved(_)
        | logical::RootStream::Project(_)
        | logical::RootStream::Cardinality(_)
        | logical::RootStream::Aggregate(_)
        | logical::RootStream::VariableWrite(_) => None,
    }
}

/// Element family after one operator. Variable reads replace or extend the
/// stream with rows of an unknown family.
fn element_after(
    element: Option<properties::ElementKind>,
    op: &logical::StreamPipelineOp,
) -> Option<properties::ElementKind> {
    match op {
        logical::StreamPipelineOp::Expand { plan } => Some(match plan.output {
            crate::ir::ExpandOutput::Nodes => properties::ElementKind::Node,
            crate::ir::ExpandOutput::Edges => properties::ElementKind::Edge,
        }),
        logical::StreamPipelineOp::Variable {
            op: logical::PureStreamVariableOp::Select(_) | logical::PureStreamVariableOp::Inject(_),
        } => None,
        logical::StreamPipelineOp::Filter { .. }
        | logical::StreamPipelineOp::IndexMembership { .. }
        | logical::StreamPipelineOp::Window { .. }
        | logical::StreamPipelineOp::Limit { .. }
        | logical::StreamPipelineOp::Skip { .. }
        | logical::StreamPipelineOp::Range { .. }
        | logical::StreamPipelineOp::Order { .. }
        | logical::StreamPipelineOp::VectorSearch { .. }
        | logical::StreamPipelineOp::TextSearch { .. }
        | logical::StreamPipelineOp::Variable { .. }
        | logical::StreamPipelineOp::VariableWrite { .. }
        | logical::StreamPipelineOp::Distinct => element,
    }
}
