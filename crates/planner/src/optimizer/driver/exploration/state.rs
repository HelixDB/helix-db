//! Mutable request-scoped exploration state.
//!
//! `ExplorationRun` is the only module that owns the mutable memo, queue,
//! retained physical alternatives, and request metrics at the same time. The
//! public driver loop can then route rules while state transitions stay behind
//! one guardrail-aware contract.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::{exec, ir, logical, memo, optimizer};

use super::super::{finish, guardrails, queue};

pub(super) enum ExplorationSeed {
    Ready(ExplorationRun),
    Finished(optimizer::OptimizationResult),
}

pub(super) struct ExplorationRun {
    started: Instant,
    memo: memo::Memo,
    memoizer: optimizer::memoize::MemoExpressionMemoizer,
    queue: queue::ExplorationQueue,
    root: memo::MemoGroupId,
    roots: ir::AtLeast<memo::MemoGroupId, 1>,
    physical: BTreeMap<memo::MemoGroupId, Vec<optimizer::result::PendingPhysicalAlternative>>,
    metrics: exec::PlannerMetrics,
}

impl ExplorationRun {
    pub(super) fn seed(
        root_exprs: ir::AtLeast<logical::LogicalExpr, 1>,
        config: &optimizer::OptimizerConfig,
    ) -> Result<ExplorationSeed, memo::MemoError> {
        let started = Instant::now();
        let mut memo = memo::Memo::default();
        let mut memoizer = optimizer::memoize::MemoExpressionMemoizer::default();
        let (first_expr, root_exprs) = root_exprs.into_first_and_rest();
        let first = memoizer.insert_root(&mut memo, first_expr)?;
        let root = first.group;
        let mut root_rest = Vec::new();
        let mut queue = queue::ExplorationQueue::default();
        queue::push_memoized(&mut queue, memoizer.drain_queued());

        if let Some(guardrail) = guardrails::memo_size_guardrail(&memo, config) {
            let roots = ir::AtLeast::<_, 1>::from_one(root);
            return Ok(ExplorationSeed::Finished(finish::finish(
                memo,
                root,
                roots,
                BTreeMap::new(),
                exec::PlannerMetrics::default(),
                Some(guardrail),
                started,
            )));
        }

        for expr in root_exprs {
            if memo.group_count() >= config.limits.memo_groups.get() {
                let roots = ir::AtLeast::<_, 1>::from_one_and_rest(root, root_rest);
                return Ok(ExplorationSeed::Finished(finish::finish(
                    memo,
                    root,
                    roots,
                    BTreeMap::new(),
                    exec::PlannerMetrics::default(),
                    Some(optimizer::OptimizerGuardrail::MemoGroups),
                    started,
                )));
            }
            let inserted = memoizer.insert_root(&mut memo, expr)?;
            root_rest.push(inserted.group);
            queue::push_memoized(&mut queue, memoizer.drain_queued());
            if let Some(guardrail) = guardrails::memo_size_guardrail(&memo, config) {
                let roots = ir::AtLeast::<_, 1>::from_one_and_rest(root, root_rest);
                return Ok(ExplorationSeed::Finished(finish::finish(
                    memo,
                    root,
                    roots,
                    BTreeMap::new(),
                    exec::PlannerMetrics::default(),
                    Some(guardrail),
                    started,
                )));
            }
        }

        let roots = ir::AtLeast::<_, 1>::from_one_and_rest(root, root_rest);
        let metrics = exec::PlannerMetrics {
            memo_groups: memo.group_count(),
            memo_exprs: memo.expression_count(),
            ..exec::PlannerMetrics::default()
        };

        Ok(ExplorationSeed::Ready(Self {
            started,
            memo,
            memoizer,
            queue,
            root,
            roots,
            physical: BTreeMap::new(),
            metrics,
        }))
    }

    pub(super) fn pop_task(&mut self) -> Option<queue::ExplorationTask> {
        self.queue.pop_front()
    }

    pub(super) fn time_guardrail(
        &self,
        config: &optimizer::OptimizerConfig,
    ) -> Option<optimizer::OptimizerGuardrail> {
        guardrails::time_guardrail(self.started, config)
    }

    pub(super) fn rule_budget_guardrail(
        &self,
        config: &optimizer::OptimizerConfig,
    ) -> Option<optimizer::OptimizerGuardrail> {
        (self.metrics.rule_fires >= config.limits.rule_fires.get())
            .then_some(optimizer::OptimizerGuardrail::RuleFires)
    }

    pub(super) fn record_rule_fire(&mut self) {
        self.metrics.rule_fires += 1;
    }

    pub(super) fn record_rejection(&mut self) {
        self.metrics.rejected_alternatives = self.metrics.rejected_alternatives.saturating_add(1);
    }

    pub(super) fn apply_logical_effect(
        &mut self,
        group: memo::MemoGroupId,
        expressions: ir::AtLeast<logical::LogicalExpr, 1>,
        config: &optimizer::OptimizerConfig,
    ) -> Result<Option<optimizer::OptimizerGuardrail>, memo::MemoError> {
        for expression in expressions {
            if self.memo.expression_count() >= config.limits.memo_expressions.get() {
                return Ok(Some(optimizer::OptimizerGuardrail::MemoExpressions));
            }
            let memo_expression = self
                .memoizer
                .memo_expression_for_expr(&mut self.memo, expression.clone())?;
            queue::push_memoized(&mut self.queue, self.memoizer.drain_queued());
            if let Some(guardrail) = guardrails::memo_size_guardrail(&self.memo, config) {
                return Ok(Some(guardrail));
            }
            match self.memo.contains_expr(group, &memo_expression) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(_) => return Ok(Some(optimizer::OptimizerGuardrail::MemoIntegrity)),
            }
            let inserted = match self.memo.insert_expr(group, memo_expression) {
                Ok(inserted) => inserted,
                Err(_) => return Ok(Some(optimizer::OptimizerGuardrail::MemoIntegrity)),
            };
            self.queue.push_back(queue::ExplorationTask {
                group: inserted.group,
                source_expr: inserted.expr,
                expr: expression,
            });
            self.metrics.memo_groups = self.memo.group_count();
            self.metrics.memo_exprs = self.memo.expression_count();
        }
        Ok(None)
    }

    pub(super) fn apply_physical_effect(
        &mut self,
        group: memo::MemoGroupId,
        source_expr: memo::MemoExprId,
        provenance: optimizer::RuleProvenance,
        alternatives: ir::AtLeast<crate::physical::PhysicalAlternative, 1>,
        config: &optimizer::OptimizerConfig,
    ) -> Option<optimizer::OptimizerGuardrail> {
        let retained = self.physical.entry(group).or_default();
        for alternative in alternatives {
            if retained.len() >= config.limits.alternatives_per_group.get() {
                return Some(optimizer::OptimizerGuardrail::AlternativesPerGroup);
            }
            retained.push(optimizer::result::PendingPhysicalAlternative {
                source_expr,
                provenance: provenance.clone(),
                alternative,
            });
        }
        None
    }

    pub(super) fn finish(
        self,
        guardrail: Option<optimizer::OptimizerGuardrail>,
    ) -> optimizer::OptimizationResult {
        finish::finish(
            self.memo,
            self.root,
            self.roots,
            self.physical,
            self.metrics,
            guardrail,
            self.started,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{context, cost, physical, properties, rules};

    use super::*;

    fn source() -> logical::LogicalExpr {
        logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        })
    }

    fn edge_source() -> logical::LogicalExpr {
        logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
            element: properties::ElementKind::Edge,
        })
    }

    fn limit() -> logical::LogicalExpr {
        logical::LogicalExpr::Pure(logical::PureLogicalOp::Limit {
            count: ir::StreamBoundPlan::Literal(1),
        })
    }

    fn variable_root_pipeline(count: usize) -> logical::LogicalExpr {
        logical::LogicalExpr::RootPipeline(
            logical::RootPipeline::new(
                logical::RootStream::VariableSource(logical::VariableSource::new(
                    ir::NonEmptyString::new("seed").unwrap(),
                )),
                ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                    count: ir::StreamBoundPlan::Literal(count),
                }),
            )
            .unwrap(),
        )
    }

    fn nested_variable_root_pipeline() -> logical::LogicalExpr {
        let inner = match variable_root_pipeline(1) {
            logical::LogicalExpr::RootPipeline(pipeline) => pipeline,
            _ => unreachable!("helper always builds a root pipeline"),
        };
        logical::LogicalExpr::RootPipeline(
            logical::RootPipeline::new(
                logical::RootStream::Pipeline(Box::new(inner)),
                ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                    count: ir::StreamBoundPlan::Literal(2),
                }),
            )
            .unwrap(),
        )
    }

    fn config() -> optimizer::OptimizerConfig {
        optimizer::OptimizerConfig::from_context(&context::PlannerContext::default())
    }

    fn alternative(latency: u64) -> physical::PhysicalAlternative {
        physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Sort,
            properties::DeliveredProperties::default(),
            cost::CostVector {
                latency: cost::LatencyEstimate::micros(latency),
                ..cost::CostVector::ZERO
            },
        )
    }

    fn provenance() -> optimizer::RuleProvenance {
        let metadata = rules::RuleMetadata::new(
            rules::RuleId::new("state_test_rule").unwrap(),
            rules::RuleKind::Implementation,
        );
        optimizer::RuleProvenance::from_metadata(&metadata)
    }

    fn ready_run(
        roots: ir::AtLeast<logical::LogicalExpr, 1>,
        config: &optimizer::OptimizerConfig,
    ) -> ExplorationRun {
        match ExplorationRun::seed(roots, config).unwrap() {
            ExplorationSeed::Ready(run) => run,
            ExplorationSeed::Finished(_) => panic!("test seed should stay under guardrails"),
        }
    }

    #[test]
    fn seed_initializes_roots_queue_and_metrics() {
        let config = config();
        let mut run = ready_run(
            ir::AtLeast::<_, 1>::from_one_and_rest(source(), vec![edge_source()]),
            &config,
        );

        assert_eq!(run.root.get(), 1);
        assert_eq!(
            run.roots.iter().map(|root| root.get()).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(run.metrics.memo_groups, 2);
        assert_eq!(run.metrics.memo_exprs, 2);
        assert_eq!(run.pop_task().unwrap().group.get(), 1);
        assert_eq!(run.pop_task().unwrap().group.get(), 2);
        assert!(run.pop_task().is_none());
    }

    #[test]
    fn seed_returns_finished_result_when_root_budget_is_exhausted() {
        let mut config = config();
        config.limits.memo_groups = properties::PositiveUsize::new(1).unwrap();

        let result = ExplorationRun::seed(
            ir::AtLeast::<_, 1>::from_one_and_rest(source(), vec![edge_source()]),
            &config,
        )
        .unwrap();

        let ExplorationSeed::Finished(result) = result else {
            panic!("second root should hit the memo-group guardrail");
        };
        assert_eq!(
            result.guardrail(),
            Some(optimizer::OptimizerGuardrail::MemoGroups)
        );
        assert_eq!(result.roots().as_ref().len(), 1);
    }

    #[test]
    fn seed_reports_guardrail_when_first_root_children_exceed_budget() {
        let mut config = config();
        config.limits.memo_groups = properties::PositiveUsize::new(1).unwrap();

        let result = ExplorationRun::seed(
            ir::AtLeast::<_, 1>::from_one(nested_variable_root_pipeline()),
            &config,
        )
        .unwrap();

        let ExplorationSeed::Finished(result) = result else {
            panic!("child-bearing first root should hit the memo-group guardrail");
        };
        assert_eq!(
            result.guardrail(),
            Some(optimizer::OptimizerGuardrail::MemoGroups)
        );
        assert_eq!(result.roots().as_ref().len(), 1);
    }

    #[test]
    fn seed_reports_guardrail_when_later_root_children_exceed_expression_budget() {
        let mut config = config();
        config.limits.memo_expressions = properties::PositiveUsize::new(2).unwrap();

        let result = ExplorationRun::seed(
            ir::AtLeast::<_, 1>::from_one_and_rest(source(), vec![nested_variable_root_pipeline()]),
            &config,
        )
        .unwrap();

        let ExplorationSeed::Finished(result) = result else {
            panic!("child-bearing later root should hit the memo-expression guardrail");
        };
        assert_eq!(
            result.guardrail(),
            Some(optimizer::OptimizerGuardrail::MemoExpressions)
        );
        assert_eq!(result.roots().as_ref().len(), 2);
    }

    #[test]
    fn logical_effect_inserts_new_expression_and_skips_duplicates() {
        let config = config();
        let mut run = ready_run(ir::AtLeast::<_, 1>::from_one(source()), &config);
        let task = run.pop_task().unwrap();

        assert_eq!(
            run.apply_logical_effect(task.group, ir::AtLeast::<_, 1>::from_one(limit()), &config)
                .unwrap(),
            None
        );
        assert_eq!(run.metrics.memo_exprs, 2);
        assert_eq!(run.pop_task().unwrap().group, task.group);

        assert_eq!(
            run.apply_logical_effect(task.group, ir::AtLeast::<_, 1>::from_one(limit()), &config)
                .unwrap(),
            None
        );
        assert_eq!(run.metrics.memo_exprs, 2);
    }

    #[test]
    fn logical_effect_reports_expression_budget_before_insert() {
        let mut config = config();
        config.limits.memo_expressions = properties::PositiveUsize::new(1).unwrap();
        let mut run = ready_run(ir::AtLeast::<_, 1>::from_one(source()), &config);
        let task = run.pop_task().unwrap();

        assert_eq!(
            run.apply_logical_effect(task.group, ir::AtLeast::<_, 1>::from_one(limit()), &config)
                .unwrap(),
            Some(optimizer::OptimizerGuardrail::MemoExpressions)
        );
    }

    #[test]
    fn logical_effect_reports_guardrail_when_derived_children_exceed_budget() {
        let mut config = config();
        config.limits.memo_groups = properties::PositiveUsize::new(1).unwrap();
        let mut run = ready_run(ir::AtLeast::<_, 1>::from_one(source()), &config);
        let task = run.pop_task().unwrap();

        assert_eq!(
            run.apply_logical_effect(
                task.group,
                ir::AtLeast::<_, 1>::from_one(nested_variable_root_pipeline()),
                &config,
            )
            .unwrap(),
            Some(optimizer::OptimizerGuardrail::MemoGroups)
        );
    }

    #[test]
    fn logical_effect_reports_memo_integrity_for_missing_target_group() {
        let config = config();
        let mut run = ready_run(ir::AtLeast::<_, 1>::from_one(source()), &config);
        let missing_group = memo::MemoGroupId::new(999).unwrap();

        assert_eq!(
            run.apply_logical_effect(
                missing_group,
                ir::AtLeast::<_, 1>::from_one(limit()),
                &config,
            )
            .unwrap(),
            Some(optimizer::OptimizerGuardrail::MemoIntegrity)
        );
    }

    #[test]
    fn physical_effect_retains_alternatives_until_group_budget() {
        let mut config = config();
        config.limits.alternatives_per_group = properties::PositiveUsize::new(1).unwrap();
        let mut run = ready_run(ir::AtLeast::<_, 1>::from_one(source()), &config);
        let task = run.pop_task().unwrap();

        assert_eq!(
            run.apply_physical_effect(
                task.group,
                task.source_expr,
                provenance(),
                ir::AtLeast::<_, 1>::from_one(alternative(7)),
                &config,
            ),
            None
        );
        assert_eq!(
            run.apply_physical_effect(
                task.group,
                task.source_expr,
                provenance(),
                ir::AtLeast::<_, 1>::from_one(alternative(9)),
                &config,
            ),
            Some(optimizer::OptimizerGuardrail::AlternativesPerGroup)
        );
    }

    #[test]
    fn counters_and_finish_preserve_request_metrics() {
        let mut config = config();
        config.limits.rule_fires = properties::PositiveUsize::new(1).unwrap();
        let mut run = ready_run(ir::AtLeast::<_, 1>::from_one(source()), &config);

        assert_eq!(run.rule_budget_guardrail(&config), None);
        run.record_rule_fire();
        run.record_rejection();
        assert_eq!(
            run.rule_budget_guardrail(&config),
            Some(optimizer::OptimizerGuardrail::RuleFires)
        );

        let result = run.finish(Some(optimizer::OptimizerGuardrail::RuleFires));
        assert_eq!(result.metrics().rule_fires, 1);
        assert_eq!(result.metrics().rejected_alternatives, 1);
        assert_eq!(
            result.guardrail(),
            Some(optimizer::OptimizerGuardrail::RuleFires)
        );
    }
}
