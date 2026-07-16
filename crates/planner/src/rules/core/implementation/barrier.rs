//! Physical seeding for effectful logical barriers.

use crate::{logical, optimizer, physical, rules};

/// Implement effectful logical barriers.
pub struct BarrierImplementationRule {
    metadata: rules::RuleMetadata,
}

impl Default for BarrierImplementationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::SeedBarrier),
                rules::RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for BarrierImplementationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::Barrier(op) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        rules::physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Barrier,
            rules::barrier_delivered(op),
            input.storage.barrier(),
        ))
    }
}
