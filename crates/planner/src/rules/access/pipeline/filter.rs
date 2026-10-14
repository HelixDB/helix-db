//! Leading-filter rewrites for access-rooted pipelines.

use super::super::filter::access_filter_alternatives;
use crate::{ir, logical, optimizer, rules};

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
        let alternatives = access_filter_alternatives(&filter, &input, rest)
            .into_iter()
            .map(|stream| match stream {
                logical::AccessStream::Path(path) => logical::LogicalExpr::AccessPath(path),
                logical::AccessStream::Pipeline(pipeline) => {
                    logical::LogicalExpr::AccessPipeline(pipeline)
                }
                _ => unreachable!("filter rewrites produce access paths or pipelines"),
            })
            .collect();
        ir::AtLeast::<_, 1>::try_from_vec(alternatives)
            .map(|alternatives| {
                optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(alternatives))
            })
            .unwrap_or(optimizer::RuleResult::NotApplicable)
    }
}
