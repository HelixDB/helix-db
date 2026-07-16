//! Static predicate logical simplification rules.

use crate::{analysis, ir, logical, optimizer, rules};

fn static_predicate_rewrite(predicate: &ir::PredicatePlan) -> Option<logical::PureLogicalOp> {
    if analysis::predicate_is_statically_tautological(predicate.as_ref()) {
        Some(logical::PureLogicalOp::NoOp)
    } else if analysis::scalar_property_conjunction_is_impossible(predicate.as_ref()) {
        Some(logical::PureLogicalOp::Empty)
    } else {
        None
    }
}

/// Simplify statically decidable residual filters.
pub struct StaticPredicateSimplificationRule {
    metadata: rules::RuleMetadata,
}

impl Default for StaticPredicateSimplificationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::StaticPredicateSimplification),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for StaticPredicateSimplificationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter { predicate }) = input.expr
        else {
            return optimizer::RuleResult::NotApplicable;
        };
        let Some(op) = static_predicate_rewrite(predicate) else {
            return optimizer::RuleResult::NotApplicable;
        };
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
            ir::AtLeast::<_, 1>::from_one(logical::LogicalExpr::Pure(op)),
        ))
    }
}
