//! Local simplification rewrites for access-rooted pipelines.

use super::support;
use crate::{logical, optimizer, rules};

/// Simplify access-rooted pipelines when local invariants prove a no-op or an
/// empty stream.
pub struct AccessPipelineSimplificationRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessPipelineSimplificationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessPipelineSimplification),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessPipelineSimplificationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        support::simplify_pipeline(pipeline).into_rule_result()
    }
}
