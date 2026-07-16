//! Root control-flow implementation rules.

use super::super::super::{physical_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use super::contracts;
use crate::{logical, optimizer, physical};

/// Implement root branch control flow while preserving executable payloads in
/// the logical source contract.
pub struct RootBranchImplementationRule {
    metadata: RuleMetadata,
}

impl Default for RootBranchImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedRootBranch),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for RootBranchImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::RootBranch(branch) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        if contracts::empty_access_for_input(branch.input()).is_empty_access() {
            return optimizer::RuleResult::NotApplicable;
        }
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Control(physical::PhysicalControlOp::Branch),
            contracts::control_flow_delivered(),
            input.storage.barrier(),
        ))
    }
}

/// Implement root repeat control flow while preserving executable payloads in
/// the logical source contract.
pub struct RootRepeatImplementationRule {
    metadata: RuleMetadata,
}

impl Default for RootRepeatImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedRootRepeat),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for RootRepeatImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::RootRepeat(repeat) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        if contracts::empty_access_for_input(repeat.input()).is_empty_access() {
            return optimizer::RuleResult::NotApplicable;
        }
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Control(physical::PhysicalControlOp::Repeat),
            contracts::control_flow_delivered(),
            input
                .storage
                .stream_operator(input.storage.default_unknown_scan_rows),
        ))
    }
}
