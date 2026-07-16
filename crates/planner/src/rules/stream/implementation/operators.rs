//! Standalone stream-operator implementation rule.

use super::super::super::physical_contracts::{stream_physical_contract, StreamPhysicalContract};
use super::super::super::{physical_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use crate::{logical, optimizer, physical};

/// Implement logical stream operators other than residual filtering and order.
pub struct StreamImplementationRule {
    metadata: RuleMetadata,
}

impl Default for StreamImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedStream),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for StreamImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::Pure(op) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let StreamPhysicalContract::Implemented(implementation) =
            stream_physical_contract(op, input.storage)
        else {
            return optimizer::RuleResult::NotApplicable;
        };
        let (op, delivered, cost) = implementation.into_parts();
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Stream(op),
            delivered,
            cost,
        ))
    }
}
