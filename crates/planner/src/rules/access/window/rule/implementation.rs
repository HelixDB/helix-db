//! Access-window physical implementation rule.

use super::super::super::super::physical_contracts::access_window_pipeline_contract;
use super::super::super::super::{physical_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use super::rewrite;
use crate::{logical, optimizer, physical};

/// Implement direct static windows over residual-free access paths when the
/// window cannot be folded into the access itself.
pub struct AccessWindowImplementationRule {
    metadata: RuleMetadata,
}

impl Default for AccessWindowImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedAccessWindow),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessWindowImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessWindow(window) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        if window.has_rewrite_candidate()
            && rewrite::rewrite_access_window(window).is_folded_access()
        {
            return optimizer::RuleResult::NotApplicable;
        }
        let (pipeline, delivered, cost) =
            access_window_pipeline_contract(window, input.storage, input.stats);
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Pipeline(pipeline),
            delivered,
            cost,
        ))
    }
}
