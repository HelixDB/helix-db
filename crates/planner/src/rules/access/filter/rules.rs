use super::super::super::physical_contracts::access_filter_pipeline_contract;
use super::super::super::{physical_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use super::super::sources::access_path_is_direct_empty;
use super::{index_access_filter, simplify_access_filter};
use crate::{logical, optimizer, physical};

/// Simplify statically decidable residual filters over residual-free access.
pub struct AccessFilterSimplificationRule {
    metadata: RuleMetadata,
}

impl Default for AccessFilterSimplificationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::AccessFilterSimplification),
                RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessFilterSimplificationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessFilter(filter) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        simplify_access_filter(filter).into_rule_result()
    }
}

/// Explore catalog-backed indexes that fully cover access-filter predicates.
pub struct AccessFilterIndexRule {
    metadata: RuleMetadata,
}

impl Default for AccessFilterIndexRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::AccessFilterIndex),
                RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessFilterIndexRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessFilter(filter) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        index_access_filter(filter, input.indexes, input.planner_limits).into_rule_result()
    }
}

/// Implement residual access filters when no exploration rule can eliminate or
/// index the predicate.
pub struct AccessFilterImplementationRule {
    metadata: RuleMetadata,
}

impl Default for AccessFilterImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedAccessFilter),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessFilterImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessFilter(filter) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        if access_path_is_direct_empty(filter.access()) {
            return optimizer::RuleResult::NotApplicable;
        }
        let (pipeline, delivered, cost) =
            access_filter_pipeline_contract(filter, input.storage, input.stats);
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Pipeline(pipeline),
            delivered,
            cost,
        ))
    }
}
