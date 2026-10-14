//! Ordering rewrites for access-rooted pipelines.

use super::super::order::{
    access_order_satisfaction, rewrite_access_order_range_direction,
    AccessOrderRangeDirectionRewrite, AccessOrderSatisfaction,
};
use super::support;
use crate::{logical, optimizer, rules};

/// Rewrite or elide access-rooted pipelines whose next order-sensitive
/// operation is an ordering request.
pub struct AccessPipelineOrderRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessPipelineOrderRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::AccessPipelineOrder),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessPipelineOrderRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let mut order = None;
        for (index, op) in pipeline.ops().iter().enumerate() {
            match op {
                logical::StreamPipelineOp::Filter { .. } => {}
                logical::StreamPipelineOp::Order { ordering } => {
                    order = Some((index, ordering));
                    break;
                }
                _ => break,
            }
        }
        let Some((_order_index, ordering)) = order else {
            return optimizer::RuleResult::NotApplicable;
        };
        let order = logical::AccessOrder::new(pipeline.access().clone(), ordering.clone());
        match rewrite_access_order_range_direction(&order, input.indexes) {
            AccessOrderRangeDirectionRewrite::Rewritten(access) if &access != pipeline.access() => {
                return support::access_pipeline_result(access, pipeline.ops().to_vec());
            }
            _ => {}
        }
        match access_order_satisfaction(&order) {
            AccessOrderSatisfaction::Satisfied(access) if &access != pipeline.access() => {
                support::access_pipeline_result(access, pipeline.ops().to_vec())
            }
            _ => optimizer::RuleResult::NotApplicable,
        }
    }
}
