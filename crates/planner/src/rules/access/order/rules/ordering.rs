//! Access-order elision and physical implementation rules.

use super::super::super::super::physical_contracts::access_order_pipeline_contract;
use super::super::super::super::{KnownRuleId, RuleId, RuleKind, RuleMetadata};
use super::super::{
    access_order_satisfaction, rewrite_access_order_range_direction,
    AccessOrderRangeDirectionRewrite, AccessOrderSatisfaction,
};
use super::shared;
use crate::{logical, optimizer};

/// Remove explicit ordering when residual-free access already delivers it.
pub struct AccessOrderRule {
    metadata: RuleMetadata,
}

impl Default for AccessOrderRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::AccessOrder),
                RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessOrderRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessOrder(order) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let candidate = match rewrite_access_order_range_direction(order, input.indexes) {
            AccessOrderRangeDirectionRewrite::Rewritten(access) => {
                AccessOrderSatisfaction::Satisfied(access)
            }
            AccessOrderRangeDirectionRewrite::NotApplicable => access_order_satisfaction(order),
        };
        let AccessOrderSatisfaction::Satisfied(access) = candidate else {
            return optimizer::RuleResult::NotApplicable;
        };
        if &access == order.access() {
            return optimizer::RuleResult::NotApplicable;
        }
        super::super::super::super::logical_result(logical::LogicalExpr::AccessOrder(
            logical::AccessOrder::new(access, order.ordering().clone()),
        ))
    }
}

/// Implement direct order requests over residual-free access paths when the
/// access does not already deliver the requested ordering.
pub struct AccessOrderImplementationRule {
    metadata: RuleMetadata,
}

impl Default for AccessOrderImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedAccessOrder),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessOrderImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessOrder(order) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        shared::access_pipeline_result(access_order_pipeline_contract(
            order,
            input.storage,
            input.stats,
        ))
    }
}
