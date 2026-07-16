//! Physical implementation for access-rooted pipelines.

use super::support;
use crate::{logical, optimizer, physical, rules};

/// Implement composed stream pipelines over supported access paths.
pub struct AccessPipelineImplementationRule {
    metadata: rules::RuleMetadata,
}

impl Default for AccessPipelineImplementationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::SeedAccessPipeline),
                rules::RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessPipelineImplementationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessPipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        if pipeline.has_local_simplification_candidate()
            && support::simplify_pipeline(pipeline).is_applicable()
        {
            return optimizer::RuleResult::NotApplicable;
        }
        let (pipeline, delivered, cost) =
            rules::access_pipeline_physical_contract(pipeline, input.storage, input.stats);
        rules::physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Pipeline(pipeline),
            delivered,
            cost,
        ))
    }
}
