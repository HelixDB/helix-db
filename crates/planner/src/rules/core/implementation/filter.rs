//! Physical seeding for residual filters.

use crate::{logical, optimizer, physical, rules};

/// Implement logical residual filters.
pub struct FilterImplementationRule {
    metadata: rules::RuleMetadata,
}

impl Default for FilterImplementationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::SeedFilter),
                rules::RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for FilterImplementationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter { .. }) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        rules::physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::ResidualFilter,
            rules::filtered_delivered(),
            input
                .storage
                .predicate_eval(input.storage.default_unknown_scan_rows),
        ))
    }
}
