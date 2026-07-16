//! Physical seeding for explicit sort implementation.

use crate::{logical, optimizer, physical, rules};

/// Implement logical order as an explicit sort.
pub struct OrderImplementationRule {
    metadata: rules::RuleMetadata,
}

impl Default for OrderImplementationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::SeedOrder),
                rules::RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for OrderImplementationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::Pure(logical::PureLogicalOp::Order { ordering }) = input.expr
        else {
            return optimizer::RuleResult::NotApplicable;
        };
        rules::physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Sort,
            rules::ordered_delivered(ordering.clone()),
            input
                .storage
                .explicit_sort(input.storage.default_unknown_scan_rows),
        ))
    }
}
