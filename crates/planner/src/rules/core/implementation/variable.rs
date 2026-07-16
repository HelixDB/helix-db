//! Physical seeding for variable source injections.

use crate::{logical, optimizer, physical, properties, rules};

/// Implement variable source injections with their executable payload retained
/// in the logical source expression.
pub struct VariableSourceImplementationRule {
    metadata: rules::RuleMetadata,
}

impl Default for VariableSourceImplementationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::SeedVariableSource),
                rules::RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for VariableSourceImplementationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::VariableSource(_) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        rules::physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Stream(physical::PhysicalStreamOp::Variable),
            properties::DeliveredProperties::default(),
            input.storage.source_inject(),
        ))
    }
}
