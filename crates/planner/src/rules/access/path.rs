//! Residual-free access-path implementation rule.

use super::super::physical_contracts::access_path_contract;
use super::super::{physical_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use crate::{logical, optimizer, physical};

/// Implement residual-free access-path candidates with LSM-aware costs.
pub struct AccessPathImplementationRule {
    metadata: RuleMetadata,
}

impl Default for AccessPathImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedAccessPath),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessPathImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPath(access) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let contract = access_path_contract(access, input.storage, input.stats);
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Access {
                element: access.element(),
                access: contract.access,
            },
            contract.delivered,
            contract.cost,
        ))
    }
}
