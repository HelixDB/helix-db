//! Physical seeding for static predicate rewrite outcomes.

use crate::{cost, logical, optimizer, physical, properties, rules};

/// Implement static predicate rewrite outcomes.
pub struct SimplifiedPredicateImplementationRule {
    metadata: rules::RuleMetadata,
}

impl Default for SimplifiedPredicateImplementationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::SeedSimplifiedPredicate),
                rules::RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for SimplifiedPredicateImplementationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::Pure(op) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        match op {
            logical::PureLogicalOp::NoOp => {
                rules::physical_result(physical::PhysicalAlternative::new(
                    physical::PhysicalExpr::NoOp,
                    properties::DeliveredProperties::default(),
                    cost::CostVector::ZERO,
                ))
            }
            logical::PureLogicalOp::Empty => {
                rules::physical_result(physical::PhysicalAlternative::new(
                    physical::PhysicalExpr::Empty,
                    rules::empty_delivered(),
                    cost::CostVector::ZERO,
                ))
            }
            logical::PureLogicalOp::Source { .. }
            | logical::PureLogicalOp::Filter { .. }
            | logical::PureLogicalOp::Limit { .. }
            | logical::PureLogicalOp::Order { .. }
            | logical::PureLogicalOp::Skip { .. }
            | logical::PureLogicalOp::Range { .. }
            | logical::PureLogicalOp::Distinct
            | logical::PureLogicalOp::Expand { .. }
            | logical::PureLogicalOp::Project
            | logical::PureLogicalOp::Aggregate
            | logical::PureLogicalOp::Variable
            | logical::PureLogicalOp::Reserved => optimizer::RuleResult::NotApplicable,
        }
    }
}
