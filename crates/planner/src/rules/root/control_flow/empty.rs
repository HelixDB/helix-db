//! Root control-flow empty-input rewrite rule.

use super::super::super::{KnownRuleId, RuleId, RuleKind, RuleMetadata};
use super::contracts;
use crate::{logical, optimizer};

/// Collapse root control-flow contracts whose input is statically empty.
pub struct RootControlFlowEmptyRule {
    metadata: RuleMetadata,
}

impl Default for RootControlFlowEmptyRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::RootControlFlowEmpty),
                RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for RootControlFlowEmptyRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let rewrite = match input.expr {
            logical::LogicalExpr::RootBranch(branch) => {
                contracts::empty_access_for_input(branch.input())
            }
            logical::LogicalExpr::RootRepeat(repeat) => {
                contracts::empty_access_for_input(repeat.input())
            }
            _ => return optimizer::RuleResult::NotApplicable,
        };
        rewrite.into_rule_result()
    }
}
