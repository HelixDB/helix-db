//! Root stream-pipeline implementation contracts.

use super::super::physical_contracts::root_pipeline_physical_contract;
use super::super::{physical_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use crate::{logical, optimizer, physical};

/// Implement composed stream pipelines over supported root streams.
pub struct RootPipelineImplementationRule {
    metadata: RuleMetadata,
}

impl Default for RootPipelineImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedRootPipeline),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for RootPipelineImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::RootPipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let (pipeline, delivered, cost) =
            root_pipeline_physical_contract(pipeline, input.storage, input.stats);
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Pipeline(pipeline),
            delivered,
            cost,
        ))
    }
}
