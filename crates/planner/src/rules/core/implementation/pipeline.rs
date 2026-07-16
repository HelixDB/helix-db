//! Physical seeding for side-effect-free pure pipelines.

use crate::{logical, optimizer, physical, rules};

/// Implement side-effect-free logical pipelines as one costed physical
/// pipeline alternative.
pub struct PipelineImplementationRule {
    metadata: rules::RuleMetadata,
}

impl Default for PipelineImplementationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::SeedPurePipeline),
                rules::RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for PipelineImplementationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::PurePipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let (pipeline, delivered, cost) =
            rules::physical_pipeline_contract(pipeline.ops_at_least(), input.storage);
        rules::physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Pipeline(pipeline),
            delivered,
            cost,
        ))
    }
}
