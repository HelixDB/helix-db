//! Optimizer wrapper for same-key range-intersection tightening.

use crate::{logical, optimizer, rules};

/// Merge same-key residual-free range intersections when tighter bounds are
/// proven.
pub struct AccessRangeIntersectionRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessRangeIntersectionRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessRangeIntersection),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessRangeIntersectionRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPath(access) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        super::super::simplify_access_range_intersection(access).into_rule_result()
    }
}
