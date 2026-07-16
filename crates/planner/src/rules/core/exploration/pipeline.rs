//! Pure-pipeline logical simplification rules.

use crate::{ir, logical, optimizer, rules};

#[derive(Debug, Clone, PartialEq)]
enum PurePipelineRewrite {
    Unchanged,
    Simplified(Box<logical::LogicalExpr>),
}

impl PurePipelineRewrite {
    fn into_rule_result(self) -> optimizer::RuleResult {
        match self {
            Self::Unchanged => optimizer::RuleResult::NotApplicable,
            Self::Simplified(expr) => optimizer::RuleResult::Applied(
                optimizer::RuleEffect::Logical(ir::AtLeast::<_, 1>::from_one(*expr)),
            ),
        }
    }
}

fn simplify_pure_pipeline(ops: &[logical::PureLogicalOp]) -> PurePipelineRewrite {
    if ops
        .iter()
        .any(|op| matches!(op, logical::PureLogicalOp::Empty))
    {
        return PurePipelineRewrite::Simplified(Box::new(logical::LogicalExpr::Pure(
            logical::PureLogicalOp::Empty,
        )));
    }

    let mut simplified = Vec::with_capacity(ops.len());
    let mut changed = false;

    for op in ops {
        match op {
            logical::PureLogicalOp::NoOp => {
                changed = true;
            }
            logical::PureLogicalOp::Skip {
                count: ir::StreamBoundPlan::Literal(0),
            } => {
                changed = true;
            }
            logical::PureLogicalOp::Distinct
                if matches!(simplified.last(), Some(logical::PureLogicalOp::Distinct)) =>
            {
                changed = true;
            }
            op => simplified.push(op.clone()),
        }
    }

    if !changed {
        return PurePipelineRewrite::Unchanged;
    }

    PurePipelineRewrite::Simplified(Box::new(
        ir::AtLeast::<_, 1>::try_from_vec(simplified)
            .map(logical::PurePipeline::new)
            .map(logical::LogicalExpr::PurePipeline)
            .unwrap_or(logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp)),
    ))
}

/// Simplify no-op and idempotent side-effect-free pipeline operators.
pub struct PurePipelineSimplificationRule {
    metadata: rules::RuleMetadata,
}

impl Default for PurePipelineSimplificationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::PurePipelineSimplification),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for PurePipelineSimplificationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::PurePipeline(pipeline) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        simplify_pure_pipeline(pipeline.ops()).into_rule_result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_op() -> logical::PureLogicalOp {
        logical::PureLogicalOp::Source {
            element: crate::properties::ElementKind::Node,
        }
    }

    #[test]
    fn pure_pipeline_rewrite_distinguishes_unchanged_from_simplified() {
        assert_eq!(
            simplify_pure_pipeline(&[source_op()]),
            PurePipelineRewrite::Unchanged
        );
        assert!(matches!(
            simplify_pure_pipeline(&[logical::PureLogicalOp::NoOp]),
            PurePipelineRewrite::Simplified(expr)
                if matches!(expr.as_ref(), logical::LogicalExpr::Pure(
                    logical::PureLogicalOp::NoOp
                ))
        ));
    }

    #[test]
    fn pure_pipeline_rewrite_converts_unchanged_to_not_applicable() {
        assert_eq!(
            PurePipelineRewrite::Unchanged.into_rule_result(),
            optimizer::RuleResult::NotApplicable
        );
    }
}
