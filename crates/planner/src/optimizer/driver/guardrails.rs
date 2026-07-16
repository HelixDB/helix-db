//! Optimizer exploration guardrail checks.

use std::time::Instant;

use crate::{memo, optimizer};

/// Check the request wall-clock budget.
pub(super) fn time_guardrail(
    started: Instant,
    config: &optimizer::OptimizerConfig,
) -> Option<optimizer::OptimizerGuardrail> {
    (started.elapsed().as_micros() as usize >= config.limits.optimization_micros.get())
        .then_some(optimizer::OptimizerGuardrail::TimeBudget)
}

/// Check memo-wide group and expression budgets.
pub(super) fn memo_size_guardrail(
    memo: &memo::Memo,
    config: &optimizer::OptimizerConfig,
) -> Option<optimizer::OptimizerGuardrail> {
    if memo.group_count() > config.limits.memo_groups.get() {
        Some(optimizer::OptimizerGuardrail::MemoGroups)
    } else if memo.expression_count() > config.limits.memo_expressions.get() {
        Some(optimizer::OptimizerGuardrail::MemoExpressions)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::{context, logical, memo, optimizer, properties};

    #[test]
    fn time_guardrail_reports_elapsed_budget() {
        let mut config =
            optimizer::OptimizerConfig::from_context(&context::PlannerContext::default());
        config.limits.optimization_micros = properties::PositiveUsize::new(1).unwrap();

        assert_eq!(
            super::time_guardrail(Instant::now() - Duration::from_micros(2), &config),
            Some(optimizer::OptimizerGuardrail::TimeBudget)
        );
    }

    #[test]
    fn memo_size_guardrail_distinguishes_group_and_expression_limits() {
        let mut memo = crate::memo::Memo::default();
        let source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        });
        memo.insert_group(
            memo::MemoExpression::new(source.clone(), memo::MemoChildGroups::empty()).unwrap(),
        )
        .expect("test memo group allocation should fit");
        memo.insert_group(
            memo::MemoExpression::new(source, memo::MemoChildGroups::empty()).unwrap(),
        )
        .expect("test memo group allocation should fit");

        let mut config =
            optimizer::OptimizerConfig::from_context(&context::PlannerContext::default());
        config.limits.memo_groups = properties::PositiveUsize::new(1).unwrap();
        assert_eq!(
            super::memo_size_guardrail(&memo, &config),
            Some(optimizer::OptimizerGuardrail::MemoGroups)
        );

        config.limits.memo_groups = properties::PositiveUsize::new(3).unwrap();
        config.limits.memo_expressions = properties::PositiveUsize::new(1).unwrap();
        assert_eq!(
            super::memo_size_guardrail(&memo, &config),
            Some(optimizer::OptimizerGuardrail::MemoExpressions)
        );
    }
}
