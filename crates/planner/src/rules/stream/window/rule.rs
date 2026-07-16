//! Optimizer rule wrapper for static stream-window composition.

use super::compose;
use crate::{logical, optimizer, rules};

/// Compose adjacent static stream-window operators.
pub struct StreamCompositionRule {
    metadata: rules::RuleMetadata,
}

impl Default for StreamCompositionRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::StreamWindowComposition),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for StreamCompositionRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::PurePipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        compose::compose_static_stream_windows(pipeline.ops()).into_rule_result()
    }
}
