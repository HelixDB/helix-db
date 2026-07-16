//! Ordering rewrites for access-rooted pipelines.

use super::super::order::{
    access_satisfies_order, rewrite_access_order_range_direction, AccessOrderRangeDirectionRewrite,
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
        let Some((order_index, ordering)) = order else {
            return optimizer::RuleResult::NotApplicable;
        };
        let prefix = pipeline.ops()[..order_index].to_vec();
        let rest = pipeline.ops()[order_index + 1..].to_vec();
        let order = logical::AccessOrder::new(pipeline.access().clone(), ordering.clone());
        if let AccessOrderRangeDirectionRewrite::Rewritten(access) =
            rewrite_access_order_range_direction(&order, input.indexes)
        {
            let mut ops = prefix;
            ops.extend(rest);
            return support::access_pipeline_result(access, ops);
        }
        if access_satisfies_order(&order) {
            let mut ops = prefix;
            ops.extend(rest);
            return support::access_pipeline_result(pipeline.access().clone(), ops);
        }
        optimizer::RuleResult::NotApplicable
    }
}
