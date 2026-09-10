//! Read-only diagnostics for the selected program. Result rows and planner
//! details have separate response types; unused access candidates are omitted.
use super::*;
use crate::exec;

#[derive(Debug, serde::Serialize)]
pub struct Explanation<'a> {
    pub effect: Effect,
    pub bindings: &'a [Binding],
    pub returns: &'a [(String, Slot)],
    pub operators: Vec<ExplainedOperator<'a>>,
    pub planner: &'a exec::PlannerMetrics,
    pub notices: Vec<PlanNotice>,
}

#[derive(Debug, serde::Serialize)]
pub struct ExplainedOperator<'a> {
    pub position: usize,
    pub logical: &'a Operator,
    pub contract: &'a OperatorContract,
    pub graph: Option<ExplainedMatch<'a>>,
    pub batch_consumer: Option<BatchConsumer>,
    pub blocking: Vec<BlockingWork>,
}

#[derive(Debug, serde::Serialize)]
pub struct ExplainedMatch<'a> {
    pub steps: &'a [MatchStep],
    /// Only scans and hash-build sources that appear in the selected schedule.
    /// Correlated access metadata lives in its IndexLookup step.
    pub sources: Vec<&'a PlannedNode>,
    pub estimated_rows_per_input: u64,
    pub cartesian_products: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockingWork {
    Aggregation,
    Distinct,
    Ordering,
    HashBuild,
    MaterializedRelation,
    InputBeforeMutation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlanNotice {
    OptimizerBudgetExhausted,
    CartesianProduct { operator: usize, count: usize },
    CursorDependentBatching { operator: usize },
    BufferedResponse,
}

impl RowPlan {
    /// Explain the selected logical/physical contracts without executing them.
    /// Cardinalities are estimates, and do not impose a result-row limit.
    /// Cursor-dependent batching is reported explicitly because runtime access
    /// availability can require the validated materialized fallback.
    pub fn explain(&self) -> Explanation<'_> {
        let mut notices = vec![PlanNotice::BufferedResponse];
        if self.metrics.guardrail_hit {
            notices.push(PlanNotice::OptimizerBudgetExhausted);
        }
        let mut batched_until: Option<usize> = None;
        let operators = self
            .query()
            .operators()
            .iter()
            .enumerate()
            .map(|(position, logical)| {
                let consumer = self.batch_consumer(position);
                let batched = batched_until.is_some_and(|end| position <= end);
                if let Some(consumer) = consumer {
                    let end = match consumer {
                        BatchConsumer::Pipeline { end } => end,
                        _ => position + 1,
                    };
                    batched_until = Some(batched_until.map_or(end, |previous| previous.max(end)));
                }
                let mut blocking = Vec::new();
                let graph = self.matches().get(&position).map(|plan| {
                    if plan.cartesian_products > 0 {
                        notices.push(PlanNotice::CartesianProduct {
                            operator: position,
                            count: plan.cartesian_products,
                        });
                    }
                    if plan
                        .steps
                        .iter()
                        .any(|step| matches!(step, MatchStep::HashJoin { .. }))
                    {
                        blocking.push(BlockingWork::HashBuild);
                    }
                    if consumer.is_some() {
                        notices.push(PlanNotice::CursorDependentBatching { operator: position });
                    } else {
                        blocking.push(BlockingWork::MaterializedRelation);
                    }
                    let sources = plan
                        .steps
                        .iter()
                        .filter_map(|step| {
                            let (MatchStep::Scan(slot) | MatchStep::HashJoin { slot, .. }) = step
                            else {
                                return None;
                            };
                            Some(
                                plan.sources
                                    .iter()
                                    .find(|source| source.slot == *slot)
                                    .expect("validated access source"),
                            )
                        })
                        .collect();
                    ExplainedMatch {
                        steps: &plan.steps,
                        sources,
                        estimated_rows_per_input: plan.estimated_rows,
                        cartesian_products: plan.cartesian_products,
                    }
                });
                match logical {
                    Operator::Project {
                        items,
                        distinct,
                        ordering,
                        ..
                    } => {
                        if items.iter().any(|item| item.expression.has_aggregate()) {
                            blocking.push(BlockingWork::Aggregation);
                        }
                        if *distinct {
                            blocking.push(BlockingWork::Distinct);
                        }
                        if !ordering.is_empty() {
                            blocking.push(BlockingWork::Ordering);
                        }
                        if !batched {
                            blocking.push(BlockingWork::MaterializedRelation);
                        }
                    }
                    Operator::Create(_) | Operator::Update(_) | Operator::Delete { .. } => {
                        blocking.push(BlockingWork::InputBeforeMutation);
                    }
                    Operator::Filter(_) | Operator::Unwind { .. }
                        if !batched && consumer.is_none() =>
                    {
                        blocking.push(BlockingWork::MaterializedRelation);
                    }
                    Operator::Match { .. } | Operator::Filter(_) | Operator::Unwind { .. } => {}
                }
                ExplainedOperator {
                    position,
                    logical,
                    contract: &self.query().contracts()[position],
                    graph,
                    batch_consumer: consumer,
                    blocking,
                }
            })
            .collect();
        Explanation {
            effect: self.query().effect(),
            bindings: self.query().bindings(),
            returns: self.query().returns(),
            operators,
            planner: &self.metrics,
            notices,
        }
    }
}
