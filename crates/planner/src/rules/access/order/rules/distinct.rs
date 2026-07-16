//! Access-distinct elision and physical implementation rules.

use super::super::super::super::physical_contracts::access_distinct_pipeline_contract;
use super::super::super::super::{access_path_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use super::super::access_distinct_is_noop;
use super::shared;
use crate::{logical, optimizer};

/// Remove distinct when residual-free access is already provably duplicate-free.
pub struct AccessDistinctRule {
    metadata: RuleMetadata,
}

impl Default for AccessDistinctRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::AccessDistinct),
                RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessDistinctRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessDistinct(distinct) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        if !distinct.has_noop_candidate() {
            return optimizer::RuleResult::NotApplicable;
        }
        access_distinct_is_noop(distinct)
            .then(|| distinct.access().clone())
            .map(access_path_result)
            .unwrap_or(optimizer::RuleResult::NotApplicable)
    }
}

/// Implement direct distinct over residual-free access paths when uniqueness is
/// not provable from the access contract.
pub struct AccessDistinctImplementationRule {
    metadata: RuleMetadata,
}

impl Default for AccessDistinctImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedAccessDistinct),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessDistinctImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessDistinct(distinct) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        if distinct.has_noop_candidate() && access_distinct_is_noop(distinct) {
            return optimizer::RuleResult::NotApplicable;
        }
        shared::access_pipeline_result(access_distinct_pipeline_contract(
            distinct,
            input.storage,
            input.stats,
        ))
    }
}
