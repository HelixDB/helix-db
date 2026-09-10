//! Common row pipelines are physical alternatives in the production memo.
//! Every alternative owns the same validated operators and effect boundaries;
//! execution strategy changes cannot reorder mutations or optional matches.
use super::*;
use crate::{cost, logical, optimizer, physical, properties, rules};
use std::{collections::BTreeMap, sync::Arc};

/// The fully materialized strategy is an independent execution reference and a
/// deterministic fallback. Batching is admitted only by validated local proofs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RowExecution {
    Materialized,
    Batched,
}

/// Selected pipeline. Serialized derived choices cannot bypass semantic proofs.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "PipelineInput")]
pub struct RowPipeline {
    query: Arc<Query>,
    execution: RowExecution,
    input_windows: BTreeMap<usize, InputWindow>,
    batch_consumers: BTreeMap<usize, BatchConsumer>,
}
#[derive(serde::Deserialize)]
struct PipelineInput {
    query: Arc<Query>,
    execution: RowExecution,
}
impl TryFrom<PipelineInput> for RowPipeline {
    type Error = QueryError;
    fn try_from(input: PipelineInput) -> Result<Self> {
        Ok(Self::new(input.query, input.execution))
    }
}

/// A downstream consumer that can finish without retaining its input relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum BatchConsumer {
    Aggregate,
    TopK,
    Project {
        termination: Termination,
    },
    /// Nonblocking projections, filters and UNWIND stages, ending at an
    /// inclusive operator index. The terminal projection may also be a direct
    /// aggregation or bounded top-k. Mutations and correlated matches are barriers.
    Pipeline {
        end: usize,
    },
}

/// A window's proof for stopping source evaluation. An initial UNWIND must
/// evaluate its one input expression first. Correlated inputs may contain later
/// errors and must instead be drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Termination {
    Drain,
    BeforeInput,
    AfterFirstBatch,
}
impl Termination {
    pub fn may_stop(self, input_started: bool) -> bool {
        match self {
            Self::Drain => false,
            Self::BeforeInput => true,
            Self::AfterFirstBatch => input_started,
        }
    }
}

impl BatchConsumer {
    fn projection(operator: &Operator) -> Option<Self> {
        let Operator::Project {
            items,
            distinct,
            ordering,
            limit,
            ..
        } = operator
        else {
            return None;
        };
        let aggregate = items.iter().any(|item| item.expression.has_aggregate());
        if aggregate
            && items.iter().all(|item| {
                !item.expression.has_aggregate()
                    || matches!(item.expression, Expression::Aggregate { .. })
            })
        {
            Some(Self::Aggregate)
        } else if !aggregate && !distinct && !ordering.is_empty() && limit.is_some() {
            Some(Self::TopK)
        } else if !aggregate && !distinct && ordering.is_empty() {
            Some(Self::Project {
                termination: Termination::Drain,
            })
        } else {
            None
        }
    }
}

impl RowPipeline {
    pub fn new(query: Arc<Query>, execution: RowExecution) -> Self {
        if execution == RowExecution::Materialized {
            return Self {
                query,
                execution,
                input_windows: BTreeMap::new(),
                batch_consumers: BTreeMap::new(),
            };
        }
        let input_windows: BTreeMap<_, _> = InputWindow::for_initial_source(&query)
            .map(|window| (0, window))
            .into_iter()
            .collect();
        // One reverse suffix pass bounds planning work even with thousands of
        // alternating UNWIND/WITH stages. An unsupported or blocking operator
        // ends the suffix; pure filters and expansion can feed a later consumer.
        let mut batch_consumers = BTreeMap::new();
        let mut suffix: Option<(usize, BatchConsumer)> = None;
        for (index, operator) in query.operators().iter().enumerate().rev() {
            let source = matches!(operator, Operator::Unwind { .. })
                || matches!(operator, Operator::Match { pattern, .. } if index == 0 && !pattern.nodes.is_empty());
            if source && let Some((end, consumer)) = suffix {
                let consumer = if end > index + 1 {
                    BatchConsumer::Pipeline { end }
                } else {
                    match consumer {
                        BatchConsumer::Project { .. } => BatchConsumer::Project {
                            termination: input_windows
                                .get(&index)
                                .map_or(Termination::Drain, InputWindow::termination),
                        },
                        unchanged => unchanged,
                    }
                };
                batch_consumers.insert(index, consumer);
            }
            suffix = match (operator, BatchConsumer::projection(operator)) {
                (_, Some(consumer @ BatchConsumer::Project { .. })) => {
                    suffix.or(Some((index, consumer)))
                }
                (_, Some(consumer)) => Some((index, consumer)),
                (Operator::Unwind { .. } | Operator::Filter(_), None) => suffix,
                _ => None,
            };
        }
        Self {
            query,
            execution,
            input_windows,
            batch_consumers,
        }
    }
    pub fn query(&self) -> &Query {
        &self.query
    }
    pub fn execution(&self) -> RowExecution {
        self.execution
    }
    pub fn batch_consumer(&self, source: usize) -> Option<BatchConsumer> {
        self.batch_consumers.get(&source).copied()
    }
    pub fn input_window(&self, source: usize) -> Option<&InputWindow> {
        self.input_windows.get(&source)
    }
    /// Row work only: access and pattern roots contribute their own storage costs
    /// in the same memo. Unknown cardinalities are estimates, never limits.
    pub fn cost(&self, storage: &cost::StorageCostProfile) -> cost::CostVector {
        let mut rows = 1_u64;
        let mut total = cost::CostVector::ZERO;
        let row_bytes = (self.query.bindings().len() as u64)
            .saturating_mul(size_of::<Value>() as u64)
            .saturating_add(size_of::<Row>() as u64);
        let mut batched_until = None;
        for (index, operator) in self.query.operators().iter().enumerate() {
            batched_until = self
                .batch_consumers
                .get(&index)
                .map(|consumer| match consumer {
                    BatchConsumer::Pipeline { end, .. } => *end,
                    _ => index + 1,
                })
                .or(batched_until);
            match operator {
                Operator::Match { .. } | Operator::Unwind { .. } => {
                    rows = storage.default_unknown_scan_rows.as_rows();
                }
                _ => {}
            }
            rows = self
                .input_windows
                .get(&index)
                .and_then(InputWindow::literal_demand)
                .map_or(rows, |demand| rows.min(demand));
            let mut work = storage.stream_operator(cost::EstimatedRows::rows(rows));
            // The batch width is the default execution width; runtime limits can
            // choose a smaller width without invalidating the selected plan.
            let retained = if batched_until.is_some_and(|end| index <= end) {
                rows.min(512)
            } else {
                rows
            };
            work.peak_memory = cost::ByteEstimate::bytes(retained.saturating_mul(row_bytes));
            total = total.serial(work);
            if let Operator::Project { items, limit, .. } = operator {
                if items.iter().any(|item| item.expression.has_aggregate())
                    && items.iter().all(|item| item.expression.has_aggregate())
                {
                    rows = 1;
                }
                if let Some(Expression::Literal(Value::Integer(limit))) = limit
                    && *limit >= 0
                {
                    rows = rows.min(*limit as u64);
                }
            }
        }
        total
    }
}

pub struct RowPipelineImplementationRule {
    metadata: rules::RuleMetadata,
}
impl Default for RowPipelineImplementationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::SeedRows),
                rules::RuleKind::Implementation,
            ),
        }
    }
}
impl optimizer::OptimizerRule for RowPipelineImplementationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }
    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::Rows(query) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let alternatives = [RowExecution::Materialized, RowExecution::Batched]
            .into_iter()
            .map(|execution| {
                let pipeline = RowPipeline::new(Arc::clone(query), execution);
                let cost = pipeline.cost(input.storage);
                physical::PhysicalAlternative::new(
                    physical::PhysicalExpr::Rows(pipeline),
                    properties::DeliveredProperties::unknown(),
                    cost,
                )
            })
            .collect();
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            crate::ir::AtLeast::try_from_vec(alternatives)
                .expect("two row implementation strategies"),
        ))
    }
}
