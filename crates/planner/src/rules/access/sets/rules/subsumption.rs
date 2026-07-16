//! Optimizer wrapper for access-source subsumption proofs.

use crate::{logical, optimizer, rules};

/// Remove access sources made redundant by provably wider sources.
pub struct AccessSubsumptionRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessSubsumptionRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessSubsumption),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessSubsumptionRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPath(access) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        super::super::simplify_access_subsumption(access).into_rule_result()
    }
}
