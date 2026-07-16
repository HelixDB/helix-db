//! Access-order range-direction exploration rule.

use super::super::super::super::{KnownRuleId, RuleId, RuleKind, RuleMetadata};
use super::super::rewrite_access_order_range_direction;
use crate::{logical, optimizer};

/// Rewrite ordered range access to an opposite-direction catalog index.
pub struct AccessOrderRangeDirectionRule {
    metadata: RuleMetadata,
}

impl Default for AccessOrderRangeDirectionRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::AccessOrderRangeDirection),
                RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessOrderRangeDirectionRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessOrder(order) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        if !order.has_range_direction_candidate() {
            return optimizer::RuleResult::NotApplicable;
        }
        rewrite_access_order_range_direction(order, input.indexes).into_rule_result()
    }
}
