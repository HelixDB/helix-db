//! Stream terminal implementation rules.

use super::super::super::physical_contracts::{
    stream_aggregate_pipeline_contract, stream_project_pipeline_contract,
    stream_reserved_pipeline_contract, stream_variable_write_pipeline_contract,
};
use super::super::super::{physical_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use crate::{cost, logical, optimizer, physical, properties};

/// Implement reserved terminals over supported root stream contracts.
pub struct StreamReservedImplementationRule {
    metadata: RuleMetadata,
}

impl Default for StreamReservedImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedStreamReserved),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for StreamReservedImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::StreamReserved(reserved) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        terminal_pipeline_result(stream_reserved_pipeline_contract(
            reserved,
            input.storage,
            input.stats,
        ))
    }
}

/// Implement projection terminals over supported access stream contracts.
pub struct StreamProjectImplementationRule {
    metadata: RuleMetadata,
}

impl Default for StreamProjectImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedStreamProject),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for StreamProjectImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::StreamProject(project) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        terminal_pipeline_result(stream_project_pipeline_contract(
            project,
            input.storage,
            input.stats,
        ))
    }
}

/// Implement aggregation terminals over supported access stream contracts.
pub struct StreamAggregateImplementationRule {
    metadata: RuleMetadata,
}

impl Default for StreamAggregateImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedStreamAggregate),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for StreamAggregateImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::StreamAggregate(aggregate) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        terminal_pipeline_result(stream_aggregate_pipeline_contract(
            aggregate,
            input.storage,
            input.stats,
        ))
    }
}

/// Implement state-writing variable terminals over supported access stream
/// contracts.
pub struct StreamVariableWriteImplementationRule {
    metadata: RuleMetadata,
}

impl Default for StreamVariableWriteImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedStreamVariableWrite),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for StreamVariableWriteImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::StreamVariableWrite(write) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        terminal_pipeline_result(stream_variable_write_pipeline_contract(
            write,
            input.storage,
            input.stats,
        ))
    }
}

fn terminal_pipeline_result(
    (pipeline, delivered, cost): (
        physical::PhysicalPipeline,
        properties::DeliveredProperties,
        cost::CostVector,
    ),
) -> optimizer::RuleResult {
    physical_result(physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(pipeline),
        delivered,
        cost,
    ))
}
