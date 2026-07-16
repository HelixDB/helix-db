//! Access-order elision and physical implementation rules.

use super::super::super::super::physical_contracts::access_order_pipeline_contract;
use super::super::super::super::{access_path_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use super::super::{access_satisfies_order, rewrite_access_order_range_direction};
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
        if !order.has_order_elision_candidate() {
            return optimizer::RuleResult::NotApplicable;
        }
        access_satisfies_order(order)
            .then(|| order.access().clone())
            .map(access_path_result)
            .unwrap_or(optimizer::RuleResult::NotApplicable)
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
        if order.has_order_elision_candidate() && access_satisfies_order(order) {
            return optimizer::RuleResult::NotApplicable;
        }
        if order.has_range_direction_candidate()
            && rewrite_access_order_range_direction(order, input.indexes).is_rewritten()
        {
            return optimizer::RuleResult::NotApplicable;
        }
        shared::access_pipeline_result(access_order_pipeline_contract(
            order,
            input.storage,
            input.stats,
        ))
    }
}
