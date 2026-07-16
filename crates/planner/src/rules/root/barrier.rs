//! Root barrier implementation contracts.

use super::super::physical_contracts::barrier_delivered;
use super::super::{physical_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use crate::{logical, optimizer, physical};

/// Implement root mutations while preserving the executable payload in the
/// logical source contract.
pub struct RootMutationImplementationRule {
    metadata: RuleMetadata,
}

impl Default for RootMutationImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedRootMutation),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for RootMutationImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::RootMutation(_) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Barrier,
            barrier_delivered(&logical::BarrierLogicalOp::Mutation),
            input.storage.barrier(),
        ))
    }
}

/// Implement root index DDL while preserving the executable payload in the
/// logical source contract.
pub struct RootIndexDdlImplementationRule {
    metadata: RuleMetadata,
}

impl Default for RootIndexDdlImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedRootIndexDdl),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for RootIndexDdlImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::RootIndexDdl(_) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Barrier,
            barrier_delivered(&logical::BarrierLogicalOp::IndexDdl),
            input.storage.barrier(),
        ))
    }
}

/// Implement root shortest path while preserving the executable payload in the
/// logical source contract.
pub struct RootShortestPathImplementationRule {
    metadata: RuleMetadata,
}

impl Default for RootShortestPathImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedRootShortestPath),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for RootShortestPathImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::RootShortestPath(_) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::ShortestPath,
            barrier_delivered(&logical::BarrierLogicalOp::TraversalControl),
            input.storage.barrier(),
        ))
    }
}
