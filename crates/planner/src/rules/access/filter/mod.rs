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
pub(in crate::rules) use index::{
    index_access_filter, index_membership_filter, label_domain_has_candidate,
};
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
        // non-dominated costs and row estimates for each delivery contract.
        // A cheaper seed with more rows may still lose after a sort/projection.
        // This considers every equality without enumerating predicate subsets.
        let mut seeds = Vec::<(crate::properties::DeliveredProperties, _, _, _)>::new();
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
                let rows = access.estimated_rows;
                if seeds.iter().any(|(properties, best_rows, best_rank, _)| {
                    properties == &delivered && *best_rows <= rows && *best_rank <= rank
                }) {
                    return;
                }
                seeds.retain(|(properties, best_rows, best_rank, _)| {
                    properties != &delivered || *best_rows < rows || *best_rank < rank
                });
                seeds.push((
                    delivered,
                    rows,
                    rank,
                    AccessFilterRewrite::RewrittenPipeline(pipeline),
                ));
            },
        );
        std::iter::once(index_access_filter(
            filter,
            input.indexes,
            input.planner_limits,
        ))
        .chain(seeds.into_iter().map(|(_, _, _, rewrite)| rewrite))
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

    /// Each equality seed carries the rest of its conjunction and is priced
    /// over it, so a wide conjunction of indexed equalities builds a bounded
    /// number of distinct seeds rather than one per conjunct, and repeated
    /// equalities share one.
    #[test]
    fn wide_indexed_conjunctions_build_a_bounded_number_of_seeds() {
        let indexes = (0..4096).fold(
            crate::catalog::IndexCatalogSnapshot::default(),
            |indexes, index| {
                indexes.with_node_eq(
                    crate::catalog::ScopedPropertyKey::try_new("User", format!("p{index}"))
                        .unwrap(),
                )
            },
        );
        let access = logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("User").unwrap(),
            }),
        ));
        for (conjuncts, seeds) in [
            (
                (0..4096)
                    .map(|index| helix_ast::expr::Predicate::eq(format!("p{index}"), index as i64))
                    .collect::<Vec<_>>(),
                32,
            ),
            (vec![helix_ast::expr::Predicate::eq("p0", 0_i64); 4096], 1),
        ] {
            let filter = logical::AccessFilter::new(
                access.clone(),
                ir::PredicatePlan::new(helix_ast::expr::Predicate::and(conjuncts)).unwrap(),
            );
            let mut built = 0;
            index::visit_equality_seed_rewrites(
                &filter,
                &indexes,
                &crate::context::PlannerLimits::default(),
                |_| built += 1,
            );
            assert_eq!(built, seeds);
        }
    }

    #[test]
    fn wide_conjunctions_keep_unique_seeds_written_after_the_cap() {
        let handle = crate::catalog::ScopedPropertyKey::try_new("User", "handle").unwrap();
        let mut indexes = (0..40).fold(
            crate::catalog::IndexCatalogSnapshot::default(),
            |indexes, index| {
                indexes.with_node_eq(
                    crate::catalog::ScopedPropertyKey::try_new("User", format!("p{index}"))
                        .unwrap(),
                )
            },
        );
        indexes.node_eq.insert(
            handle,
            crate::catalog::NodeEqualityIndexMeta::try_new("user_handle")
                .unwrap()
                .with_uniqueness(crate::catalog::IndexUniqueness::Unique),
        );
        let access = logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("User").unwrap(),
            }),
        ));
        let conjuncts = (0..40)
            .map(|index| helix_ast::expr::Predicate::eq(format!("p{index}"), index as i64))
            .chain([helix_ast::expr::Predicate::eq("handle", "ada")])
            .collect::<Vec<_>>();
        let filter = logical::AccessFilter::new(
            access,
            ir::PredicatePlan::new(helix_ast::expr::Predicate::and(conjuncts)).unwrap(),
        );
        let mut seeds = Vec::new();
        index::visit_equality_seed_rewrites(
            &filter,
            &indexes,
            &crate::context::PlannerLimits::default(),
            |pipeline| {
                let logical::AccessPath::Node(path) = pipeline.access() else {
                    panic!("node filters seed node accesses");
                };
                let ir::NodeAccessPlan::EqualityIndex { key, .. } = path.source().as_ref() else {
                    panic!("equality seeds read equality indexes");
                };
                seeds.push(key.property.to_string());
            },
        );
        // The unique seed is kept, the rest are the first non-unique seeds,
        // and all of them are emitted in written order.
        let expected = (0..31)
            .map(|index| format!("p{index}"))
            .chain(["handle".to_string()])
            .collect::<Vec<_>>();
        assert_eq!(seeds, expected);
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
