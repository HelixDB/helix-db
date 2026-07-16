//! Filter-family logical exploration rules.

use crate::{ir, logical, optimizer, rules};

/// Transpose filters below safe pure stream-preserving operators.
pub struct FilterPushdownRule {
    metadata: rules::RuleMetadata,
}

impl Default for FilterPushdownRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::FilterPushdown),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for FilterPushdownRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::FilterPushdown(candidate) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
            ir::AtLeast::<_, 1>::from_one(logical::LogicalExpr::PurePipeline(
                logical::PurePipeline::new(candidate.clone().into_pipeline_ops()),
            )),
        ))
    }
}

/// Merge adjacent residual filters into one conjunctive predicate.
pub struct FilterMergeRule {
    metadata: rules::RuleMetadata,
}

impl Default for FilterMergeRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::FilterMerge),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for FilterMergeRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::FilterChain(chain) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
            ir::AtLeast::<_, 1>::from_one(logical::LogicalExpr::Pure(
                logical::PureLogicalOp::Filter {
                    predicate: chain.merged_predicate(),
                },
            )),
        ))
    }
}
