//! Access-filter simplification and catalog-index derivation contracts.

mod atoms;
mod diagnostics;
mod index;
mod labels;
mod rules;
mod simplify;

use crate::{ir, logical, optimizer};

pub use rules::{
    AccessFilterImplementationRule, AccessFilterIndexRule, AccessFilterSimplificationRule,
};

pub(crate) use self::diagnostics::{missing_index_candidates, CandidateIndexKind};
pub(in crate::rules) use index::{index_access_filter, label_domain_has_candidate};
pub(in crate::rules) use simplify::simplify_access_filter;

/// Explore complete index coverage plus a linear number of equality seeds,
/// preserving every residual and any caller-owned pipeline suffix.
pub(in crate::rules) fn access_filter_alternatives(
    filter: &logical::AccessFilter,
    input: &optimizer::RuleInput<'_>,
    suffix: &[logical::StreamPipelineOp],
) -> Vec<logical::AccessStream> {
    let simplified = simplify_access_filter(filter);
    let rewrites = if simplified == AccessFilterRewrite::NotApplicable {
        // All equality seeds have the same complete predicate semantics. Keep
        // the cheapest for each delivered-property contract before inserting
        // them into the memo. This considers every usable equality without
        // spending the alternative budget on equivalent seeds or subsets.
        let mut seeds = Vec::<(crate::properties::DeliveredProperties, _, _)>::new();
        index::visit_equality_seed_rewrites(
            filter,
            input.indexes,
            input.planner_limits,
            |pipeline| {
                let (_, delivered, cost) =
                    crate::rules::physical_contracts::access_pipeline_physical_contract(
                        &pipeline,
                        input.storage,
                        input.stats,
                    );
                let access = crate::rules::physical_contracts::access_path_contract(
                    pipeline.access(),
                    input.storage,
                    input.stats,
                );
                let alternative = crate::physical::PhysicalAlternative::new(
                    crate::physical::PhysicalExpr::Access {
                        element: pipeline.access().element(),
                        access: access.access,
                    },
                    delivered.clone(),
                    cost,
                );
                let rank = (crate::optimizer::cost_key(cost), alternative.digest.get());
                match seeds
                    .iter_mut()
                    .find(|(properties, _, _)| properties == &delivered)
                {
                    Some((_, best_rank, best)) if rank < *best_rank => {
                        *best_rank = rank;
                        *best = AccessFilterRewrite::RewrittenPipeline(pipeline);
                    }
                    Some(_) => {}
                    None => seeds.push((
                        delivered,
                        rank,
                        AccessFilterRewrite::RewrittenPipeline(pipeline),
                    )),
                }
            },
        );
        std::iter::once(index_access_filter(
            filter,
            input.indexes,
            input.planner_limits,
        ))
        .chain(seeds.into_iter().map(|(_, _, rewrite)| rewrite))
        .collect()
    } else {
        vec![simplified]
    };
    rewrites
        .into_iter()
        .filter_map(|rewrite| {
            let (access, mut ops) = match rewrite {
                AccessFilterRewrite::NotApplicable => return None,
                AccessFilterRewrite::Rewritten(access) => (access, Vec::new()),
                AccessFilterRewrite::RewrittenPipeline(pipeline) => {
                    (pipeline.access().clone(), pipeline.ops().to_vec())
                }
            };
            ops.extend_from_slice(suffix);
            match ir::AtLeast::<_, 1>::try_from_vec(ops) {
                Some(ops) => {
                    logical::AccessPipeline::new(access, ops).map(logical::AccessStream::Pipeline)
                }
                None => Some(logical::AccessStream::Path(access)),
            }
        })
        .collect()
}

/// Access-filter rewrite outcome at the rule boundary.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules) enum AccessFilterRewrite {
    /// The filter could not be eliminated or replaced by an indexed access.
    NotApplicable,
    /// The filter was replaced by a validated access path.
    Rewritten(logical::AccessPath),
    /// The filter was reduced to a narrower access path plus a residual suffix.
    RewrittenPipeline(logical::AccessPipeline),
}

impl AccessFilterRewrite {
    pub(in crate::rules) fn or_else(self, rewrite: impl FnOnce() -> Self) -> Self {
        match self {
            Self::NotApplicable => rewrite(),
            Self::Rewritten(_) | Self::RewrittenPipeline(_) => self,
        }
    }

    pub(in crate::rules) fn into_rule_result(self) -> optimizer::RuleResult {
        match self {
            Self::NotApplicable => optimizer::RuleResult::NotApplicable,
            Self::Rewritten(access) => super::super::access_path_result(access),
            Self::RewrittenPipeline(pipeline) => {
                optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
                    ir::AtLeast::<_, 1>::from_one(logical::LogicalExpr::AccessPipeline(pipeline)),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir;

    fn node_access() -> logical::AccessPath {
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::AllScan),
        ))
    }

    #[test]
    fn access_filter_rewrite_or_else_uses_fallback_only_when_needed() {
        let access = node_access();

        assert_eq!(
            AccessFilterRewrite::NotApplicable
                .or_else(|| AccessFilterRewrite::Rewritten(access.clone())),
            AccessFilterRewrite::Rewritten(access.clone())
        );
        assert_eq!(
            AccessFilterRewrite::Rewritten(access.clone())
                .or_else(|| AccessFilterRewrite::NotApplicable),
            AccessFilterRewrite::Rewritten(access)
        );
    }

    #[test]
    fn access_filter_rewrite_converts_to_rule_result() {
        assert!(matches!(
            AccessFilterRewrite::Rewritten(node_access()).into_rule_result(),
            optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(exprs))
                if matches!(
                    exprs.as_ref(),
                    [logical::LogicalExpr::AccessPath(logical::AccessPath::Node(_))]
                )
        ));
        assert_eq!(
            AccessFilterRewrite::NotApplicable.into_rule_result(),
            optimizer::RuleResult::NotApplicable
        );

        let predicate =
            ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();
        let pipeline = logical::AccessPipeline::new(
            node_access(),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Filter { predicate }),
        )
        .unwrap();
        assert!(matches!(
            AccessFilterRewrite::RewrittenPipeline(pipeline).into_rule_result(),
            optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(exprs))
                if matches!(
                    exprs.as_ref(),
                    [logical::LogicalExpr::AccessPipeline(_)]
                )
        ));
    }
}
