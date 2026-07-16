//! Optimizer wrappers for equality/range access-set proofs.

use crate::{logical, optimizer, rules};

/// Restrict literal equality unions by same-property range constraints.
pub struct AccessEqualityRangeIntersectionRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessEqualityRangeIntersectionRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessEqualityRangeIntersection),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessEqualityRangeIntersectionRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPath(access) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        super::super::simplify_access_equality_range_intersection(access).into_rule_result()
    }
}

/// Remove literal equality union branches proven covered by range branches.
pub struct AccessEqualityRangeUnionRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessEqualityRangeUnionRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessEqualityRangeUnion),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessEqualityRangeUnionRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPath(access) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        super::super::simplify_access_equality_range_union(access).into_rule_result()
    }
}
