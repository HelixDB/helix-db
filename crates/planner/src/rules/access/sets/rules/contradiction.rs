//! Optimizer wrapper for static access-set contradiction detection.

use crate::{logical, optimizer, rules};

/// Collapse statically contradictory residual-free access intersections.
pub struct AccessContradictionRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessContradictionRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessContradiction),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessContradictionRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPath(access) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        super::super::simplify_access_contradiction(access).into_rule_result()
    }
}
