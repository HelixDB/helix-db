//! Access-window exploration rule.

use super::super::super::super::{KnownRuleId, RuleId, RuleKind, RuleMetadata};
use super::rewrite;
use crate::{logical, optimizer};

/// Apply static stream windows directly to residual-free access paths.
pub struct AccessWindowRule {
    metadata: RuleMetadata,
}

impl Default for AccessWindowRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::AccessWindow),
                RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessWindowRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessWindow(window) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        rewrite::rewrite_access_window(window).into_rule_result()
    }
}
