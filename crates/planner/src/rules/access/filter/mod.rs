//! Access-filter simplification and catalog-index derivation contracts.

mod atoms;
mod index;
mod labels;
mod rules;
mod simplify;

use crate::{ir, logical, optimizer};

pub use rules::{
    AccessFilterImplementationRule, AccessFilterIndexRule, AccessFilterSimplificationRule,
};

pub(in crate::rules) use index::index_access_filter;
pub(in crate::rules) use simplify::simplify_access_filter;

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
