//! Leading-filter rewrites for access-rooted pipelines.

use super::super::filter::{index_access_filter, simplify_access_filter, AccessFilterRewrite};
use super::support;
use crate::{logical, optimizer, rules};

/// Rewrite a leading access-pipeline filter into a simpler or indexed access
/// path while preserving the remaining pipeline suffix.
pub struct AccessPipelineFilterRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessPipelineFilterRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessPipelineFilter),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessPipelineFilterRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let [logical::StreamPipelineOp::Filter { predicate }, rest @ ..] = pipeline.ops() else {
            return optimizer::RuleResult::NotApplicable;
        };
        let filter = logical::AccessFilter::new(pipeline.access().clone(), predicate.clone());
        match simplify_access_filter(&filter)
            .or_else(|| index_access_filter(&filter, input.indexes, input.planner_limits))
        {
            AccessFilterRewrite::Rewritten(access) => {
                support::access_pipeline_result(access, rest.to_vec())
            }
            AccessFilterRewrite::RewrittenPipeline(pipeline) => {
                let mut ops = pipeline.ops().to_vec();
                ops.extend_from_slice(rest);
                support::access_pipeline_result(pipeline.access().clone(), ops)
            }
            AccessFilterRewrite::NotApplicable => optimizer::RuleResult::NotApplicable,
        }
    }
}
