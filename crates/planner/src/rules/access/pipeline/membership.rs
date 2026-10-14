//! Row-preserving index membership for node-stream filters.
//!
//! An indexed filter must never be decided by reading records row by row. A
//! filter behind an expansion cannot change the access path, so this rule
//! replaces every such node-stream filter that [`index_membership_filter`]
//! serves with one [`logical::StreamPipelineOp::IndexMembership`] step, whose
//! fused residual keeps the conjuncts the set cannot decide. Streams of
//! unknown element kind qualify too, because the operator evaluates edge and
//! element-free rows exactly like the filter. A leading filter the
//! source-index rule declines qualifies as well: over a label-less node
//! source (point IDs, a parameter, a variable, or an all-node scan) whose
//! predicate names no label, or over a parameter or variable source, whose
//! repeated IDs an index intersection would collapse. So does the same filter
//! as a lone [`logical::AccessFilter`]. Root wrappers inline their streams, so
//! the rule also rewrites the pipelines inside them.
//!
//! The rewrite is required, not an alternative. The implementation rules for
//! every expression kind it matches refuse any expression
//! [`membership_rewrite`] would change (see
//! `RuleApplicability::StreamMembershipCandidate`), so a per-row filter over
//! an eligible predicate never reaches a physical plan, whatever its cost or
//! the exploration budget. Normalising once before exploration would not be
//! enough: exploration keeps creating pipelines (partial index rewrites,
//! root-stream merges, order rewrites), and each needs the same guarantee.
//! The rewrite replaces every eligible filter at once, so its output has none
//! left and is implemented directly, which keeps a physical alternative in
//! every memo group.

use super::super::filter::index_membership_filter;
use crate::{analysis, catalog, context, logical, optimizer, properties, rules};

/// Rewrite every eligible node-stream filter into index membership.
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
        membership_rewrite(input.expr, input.indexes, input.planner_limits)
            .map(rules::logical_result)
            .unwrap_or(optimizer::RuleResult::NotApplicable)
    }
}

/// `expr` with every eligible node-stream filter replaced by index
/// membership, or `None` when it has none.
///
/// The implementation rules of the matched expression kinds call this to
/// defer to the rewrite, so the refusal and the rewrite never disagree. The
/// rewrite is idempotent: it returns `None` for its own output.
pub(in crate::rules) fn membership_rewrite(
    expr: &logical::LogicalExpr,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Option<logical::LogicalExpr> {
    super::rewrite_stream_expr(
        expr,
        &Rewrite {
            indexes,
            planner_limits,
        },
    )
}

struct Rewrite<'a> {
    indexes: &'a catalog::IndexCatalogSnapshot,
    planner_limits: &'a context::PlannerLimits,
}

impl super::StreamFilterRewrite for Rewrite<'_> {
    /// A lone filter the source-index rule declines (over a node source that
    /// may repeat elements, or a label-less one with an unscoped predicate)
    /// becomes a membership pipeline over the same source.
    fn access_filter(&self, filter: &logical::AccessFilter) -> Option<logical::AccessStream> {
        if !declined_leading_node_filter(filter.access(), filter.predicate()) {
            return None;
        }
        let plan = index_membership_filter(filter.predicate(), self.indexes, self.planner_limits)?;
        logical::AccessPipeline::new(
            filter.access().clone(),
            crate::ir::AtLeast::from_one(logical::StreamPipelineOp::IndexMembership {
                plan: Box::new(plan),
            }),
        )
        .map(logical::AccessStream::Pipeline)
    }

    /// A leading filter belongs to the source-index rule, so candidates start
    /// after it, unless that rule declines it (see the lone filter above).
    fn access_pipeline(&self, pipeline: &logical::AccessPipeline) -> Option<logical::AccessStream> {
        let first_candidate = match pipeline.ops() {
            [logical::StreamPipelineOp::Filter { predicate }, ..]
                if declined_leading_node_filter(pipeline.access(), predicate) =>
            {
                0
            }
            _ => 1,
        };
        let ops = self.ops(
            Some(pipeline.access().element()),
            pipeline.ops(),
            first_candidate,
        )?;
        logical::AccessPipeline::new(pipeline.access().clone(), ops)
            .map(logical::AccessStream::Pipeline)
    }

    /// A root pipeline follows a complete root stream, so its first filter is
    /// already behind that stream's source.
    fn root_pipeline_ops(
        &self,
        pipeline: &logical::RootPipeline,
    ) -> Option<crate::ir::AtLeast<logical::StreamPipelineOp, 1>> {
        self.ops(root_stream_element(pipeline.input()), pipeline.ops(), 0)
    }
}

impl Rewrite<'_> {
    /// Replace every eligible filter at or after `first_candidate`, keeping
    /// every other operator in place. A filter is eligible when its rows are
    /// not known to be edges and [`index_membership_filter`] serves its
    /// predicate. `None` when no filter is eligible.
    fn ops(
        &self,
        element: Option<properties::ElementKind>,
        ops: &[logical::StreamPipelineOp],
        first_candidate: usize,
    ) -> Option<crate::ir::AtLeast<logical::StreamPipelineOp, 1>> {
        let replacements = ops
            .iter()
            .scan(element, |element, op| {
                let input = *element;
                *element = element_after(input, op);
                Some((op, input))
            })
            .enumerate()
            .map(|(position, (op, input))| match (op, input) {
                (
                    logical::StreamPipelineOp::Filter { predicate },
                    None | Some(properties::ElementKind::Node),
                ) if position >= first_candidate => {
                    index_membership_filter(predicate, self.indexes, self.planner_limits).map(
                        |plan| logical::StreamPipelineOp::IndexMembership {
                            plan: Box::new(plan),
                        },
                    )
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if replacements.iter().all(Option::is_none) {
            return None;
        }
        crate::ir::AtLeast::try_from_vec(
            replacements
                .into_iter()
                .zip(ops)
                .map(|(replacement, op)| replacement.unwrap_or_else(|| op.clone()))
                .collect(),
        )
    }
}

/// Whether a leading `predicate` filter over the non-empty node source
/// `access` is one the source-index rule declines: the source may repeat
/// elements, which an index intersection would collapse, or it has no common
/// label and `predicate` names none.
fn declined_leading_node_filter(
    access: &logical::AccessPath,
    predicate: &crate::ir::PredicatePlan,
) -> bool {
    let logical::AccessPath::Node(path) = access else {
        return false;
    };
    !access.is_direct_empty()
        && (access.may_repeat_elements()
            || (path.common_label().is_none()
                && matches!(
                    analysis::prune_borrowed(predicate.as_ref()),
                    Ok(analysis::PrunedPredicate::Feasible {
                        label: analysis::FeasibleLabelScope::Unscoped,
                        ..
                    })
                )))
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
