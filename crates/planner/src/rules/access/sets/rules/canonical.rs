//! Optimizer wrapper for canonical access-set normalization.

use crate::{logical, optimizer, rules};

/// Simplify residual-free access unions and intersections.
pub struct AccessSetSimplificationRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessSetSimplificationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessSetSimplification),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessSetSimplificationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPath(access) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        super::super::simplify_access_set(access).into_rule_result()
    }
}
