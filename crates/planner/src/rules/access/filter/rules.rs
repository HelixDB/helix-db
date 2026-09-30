use super::super::super::physical_contracts::access_filter_pipeline_contract;
use super::super::super::{physical_result, KnownRuleId, RuleId, RuleKind, RuleMetadata};
use super::super::sources::access_path_is_direct_empty;
use super::{access_filter_alternatives, simplify_access_filter};
use crate::{ir, logical, optimizer, physical};

/// Simplify statically decidable residual filters over residual-free access.
pub struct AccessFilterSimplificationRule {
    metadata: RuleMetadata,
}

impl Default for AccessFilterSimplificationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::AccessFilterSimplification),
                RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessFilterSimplificationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessFilter(filter) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        simplify_access_filter(filter).into_rule_result()
    }
}

/// Explore label-domain and index alternatives for access filters.
///
/// Property-index rewrites are required via `AccessSourceIndexFilterRule`,
/// which produces the same index alternatives even after the exploration
/// budget; the memo keeps one copy. Only the label-domain alternatives are
/// optional.
pub struct AccessFilterIndexRule {
    metadata: RuleMetadata,
}

impl Default for AccessFilterIndexRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::AccessFilterIndex),
                RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessFilterIndexRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessFilter(filter) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let alternatives = access_filter_alternatives(filter, &input, &[])
            .into_iter()
            .map(|stream| match stream {
                logical::AccessStream::Path(path) => logical::LogicalExpr::AccessPath(path),
                logical::AccessStream::Pipeline(pipeline) => {
                    logical::LogicalExpr::AccessPipeline(pipeline)
                }
                _ => unreachable!("filter rewrites produce access paths or pipelines"),
            })
            .collect();
        ir::AtLeast::<_, 1>::try_from_vec(alternatives)
            .map(|alternatives| {
                optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(alternatives))
            })
            .unwrap_or(optimizer::RuleResult::NotApplicable)
    }
}

/// Implement residual access filters only when no required rewrite (index
/// membership or source index access) applies.
pub struct AccessFilterImplementationRule {
    metadata: RuleMetadata,
}

impl Default for AccessFilterImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedAccessFilter),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for AccessFilterImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::AccessFilter(filter) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        if crate::rules::required_filter_rewrite_pending(
            input.expr,
            input.indexes,
            input.planner_limits,
        ) {
            return optimizer::RuleResult::NotApplicable;
        }
        if access_path_is_direct_empty(filter.access()) {
            return optimizer::RuleResult::NotApplicable;
        }
        let (pipeline, delivered, cost) =
            access_filter_pipeline_contract(filter, input.storage, input.stats);
        physical_result(physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Pipeline(pipeline),
            delivered,
            cost,
        ))
    }
}
