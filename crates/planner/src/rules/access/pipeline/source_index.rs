//! Required index access for leading stream filters.
//!
//! An indexed filter must never be decided by reading records row by row. A
//! filter directly over an access source (`N<L>.where(..)`,
//! `E<L>.where(..)`, or the first filter of an access pipeline) can change
//! the source itself: when a property index serves any of its conjuncts, the
//! source becomes the intersection of every index-served conjunct, and only
//! the conjuncts no index answers stay behind as a residual filter over that
//! narrowed source. A filter the source already answers exactly is dropped.
//! Root wrappers inline their streams, so the rule also rewrites the leading
//! filters inside them. A source that may repeat elements (parameter or
//! variable IDs) is left alone: an intersection would emit each element
//! once, so index membership decides its node filters instead.
//!
//! The rewrite is required, not an alternative. The implementation rules of
//! every kind it matches refuse any expression [`source_index_rewrite`]
//! would change (see `RuleApplicability::StreamSourceIndexCandidate`), so a
//! label scan plus a per-row filter over an index-served conjunct never
//! reaches a physical plan, whatever its cost, the statistics, or the
//! exploration budget. The rule keeps running after the budget stops
//! optional exploration, and its output holds no leading filter an index
//! serves, so that output is implemented directly and every memo group keeps
//! a physical alternative. The optional explorers produce the same full and
//! partial rewrites, which the memo deduplicates, plus the label-domain
//! alternatives, which stay optional.

use super::super::filter::required_index_access_filter;
use crate::{catalog, context, ir, logical, optimizer, rules};

/// Rewrite every leading stream filter a property index serves into index
/// access.
pub struct AccessSourceIndexFilterRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessSourceIndexFilterRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessSourceIndexFilter),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessSourceIndexFilterRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        source_index_rewrite(input.expr, input.indexes, input.planner_limits)
            .map(rules::logical_result)
            .unwrap_or(optimizer::RuleResult::NotApplicable)
    }
}

/// `expr` with every leading stream filter a property index serves replaced
/// by index access, or `None` when it has none.
///
/// The implementation rules of the matched expression kinds call this to
/// defer to the rewrite, so the refusal and the rewrite never disagree. The
/// rewrite is idempotent: it returns `None` for its own output, whose
/// leading residual, if any, holds only conjuncts no index answers.
pub(in crate::rules) fn source_index_rewrite(
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
    fn access_filter(&self, filter: &logical::AccessFilter) -> Option<logical::AccessStream> {
        required_index_access_filter(filter, self.indexes, self.planner_limits).into_stream(&[])
    }

    fn access_pipeline(&self, pipeline: &logical::AccessPipeline) -> Option<logical::AccessStream> {
        let [logical::StreamPipelineOp::Filter { predicate }, rest @ ..] = pipeline.ops() else {
            return None;
        };
        let filter = logical::AccessFilter::new(pipeline.access().clone(), predicate.clone());
        required_index_access_filter(&filter, self.indexes, self.planner_limits).into_stream(rest)
    }

    /// Root-pipeline operators follow a complete stream, so their filters
    /// are behind a source and belong to index membership.
    fn root_pipeline_ops(
        &self,
        _pipeline: &logical::RootPipeline,
    ) -> Option<ir::AtLeast<logical::StreamPipelineOp, 1>> {
        None
    }
}
