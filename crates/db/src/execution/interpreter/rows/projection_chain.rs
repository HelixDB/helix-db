//! Nonblocking row stages with an explicit expansion stack. Every projection
//! owns global window counters; mutations run only after the consumer finishes.
use super::{
    bound_match::{BoundCursor, BoundMatch},
    correlated::{MatchCursor, NodeMatch},
    matches,
    memory::Rows,
    projection::Projection,
    push_row,
    streaming::UnwindCursor,
    ConsumedProjection, ExecutionContext, GraphBatch, Limits, Result, RowBuffer,
};
use futures::StreamExt;
use helix_planner::relational as r;
use std::{collections::BTreeMap, ops};

enum Stage<'a> {
    Project {
        items: &'a r::ProjectionProgram,
        predicate: Option<&'a r::SelectionProgram>,
        skip: usize,
        remaining: usize,
    },
    Filter(&'a r::SelectionProgram),
    Match(NodeMatch<'a>),
    BoundMatch(BoundMatch<'a>),
    Unwind {
        expression: &'a r::Expression,
        slot: r::Slot,
    },
}
struct Expansion<'a> {
    stage: usize,
    cursor: PipelineCursor<'a>,
}

enum PipelineCursor<'a> {
    Unwind(UnwindCursor),
    Match(MatchCursor),
    BoundMatch(BoundCursor<'a>),
}

impl ExecutionContext<'_> {
    /// Shared consumer dispatch for graph and UNWIND producers. Every future
    /// with storage work is boxed to keep debug/coverage stacks bounded.
    pub(super) async fn consume_batches<'a, S>(
        &'a self,
        batches: S,
        plan: &'a r::RowPlan,
        source: usize,
        consumer: r::BatchConsumer,
        parameters: &'a BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<(Rows, ConsumedProjection)>
    where
        S: futures::Stream<Item = Result<Rows>> + Send + 'a,
    {
        let end = match consumer {
            r::BatchConsumer::Pipeline { end } => {
                return self
                    .row_budget()
                    .admitted_future(self.projection_chain(
                        batches,
                        plan,
                        source + 1..=end,
                        plan.input_window(source),
                        parameters,
                        limits,
                    ))?
                    .await;
            }
            r::BatchConsumer::Aggregate
            | r::BatchConsumer::TopK
            | r::BatchConsumer::Project { .. } => source + 1,
        };
        let r::Operator::Project {
            items,
            distinct,
            ordering,
            predicate,
            skip,
            limit,
        } = &plan.query().operators()[end]
        else {
            unreachable!("validated direct projection consumer");
        };
        let projection = Projection {
            items,
            distinct: *distinct,
            ordering,
            predicate: predicate.as_ref(),
            skip: skip.as_ref(),
            limit: limit.as_ref(),
        };
        let width = plan.query().bindings().len();
        match consumer {
            r::BatchConsumer::Aggregate => Ok((
                self.row_budget()
                    .admitted_future(
                        self.aggregate_batches(batches, width, items, parameters, limits),
                    )?
                    .await?
                    .finish(),
                ConsumedProjection::Aggregate(end),
            )),
            r::BatchConsumer::Project { termination } => Ok((
                self.row_budget()
                    .admitted_future(self.project_batches(
                        batches,
                        width,
                        projection,
                        parameters,
                        limits,
                        termination,
                    ))?
                    .await?,
                ConsumedProjection::Complete(end),
            )),
            r::BatchConsumer::TopK => Ok((
                self.row_budget()
                    .admitted_future(self.top_k_batches(batches, projection, parameters, limits))?
                    .await?,
                ConsumedProjection::Complete(end),
            )),
            r::BatchConsumer::Pipeline { .. } => unreachable!("pipeline was handled above"),
        }
    }

    pub(super) async fn filter_relation(
        &self,
        rows: Rows,
        predicate: &r::SelectionProgram,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows> {
        let mut output = RowBuffer::new(self.row_budget())?;
        let mut rows = rows.into_iter();
        while !rows.as_slice().is_empty() {
            self.check_execution_deadline()?;
            let count = rows.as_slice().len().min(limits.batch_rows);
            let graph = self
                .expression_graph_batch(&rows.as_slice()[..count], [predicate.expression()])
                .await?;
            let mut evaluator = RowSelection {
                context: self,
                graph: &graph,
                parameters,
                limits,
                output: &mut output,
            };
            self.row_budget()
                .admitted_future(predicate.select(rows.by_ref().take(count), &mut evaluator))?
                .await?;
        }
        Ok(output.finish())
    }

    pub(super) async fn projection_chain<'a, S>(
        &'a self,
        batches: S,
        plan: &'a r::RowPlan,
        operators: ops::RangeInclusive<usize>,
        window: Option<&'a r::InputWindow>,
        parameters: &'a BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<(Rows, ConsumedProjection)>
    where
        S: futures::Stream<Item = Result<Rows>> + Send + 'a,
    {
        let query = plan.query();
        let width = query.bindings().len();
        let end = *operators.end();
        let r::Operator::Project {
            items,
            distinct,
            ordering,
            predicate,
            skip,
            limit,
        } = &query.operators()[end]
        else {
            unreachable!("validated pipeline ends in a projection");
        };
        let aggregate = items.iter().any(|item| item.expression.has_aggregate());
        let projected_terminal = !aggregate && !distinct && ordering.is_empty();
        // Include a plain terminal projection so LIMIT 0 observes initial source
        // evaluation even when earlier SKIP stages never emit a row.
        let start = *operators.start();
        let operators = &query.operators()[start..end + usize::from(projected_terminal)];
        assert!(!operators.is_empty(), "a pipeline has intermediate stages");
        let stop = window.map(|window| {
            assert!(window.projection() >= start && window.projection() < start + operators.len());
            (window.projection() - start, window.termination())
        });
        let expansions = operators
            .iter()
            .filter(|op| matches!(op, r::Operator::Unwind { .. } | r::Operator::Match { .. }))
            .count();
        let memory = self.row_budget().reserve(
            size_of::<Vec<Stage<'_>>>()
                .saturating_add(operators.len().saturating_mul(size_of::<Stage<'_>>()))
                .saturating_add(size_of::<Vec<Expansion<'_>>>())
                .saturating_add(expansions.saturating_mul(size_of::<Expansion<'_>>())),
        )?;
        let empty = GraphBatch::default();
        let evaluation = self.evaluate(&[], parameters, &empty, limits);
        let mut stages = Vec::with_capacity(operators.len());
        for (offset, operator) in operators.iter().enumerate() {
            stages.push(match operator {
                r::Operator::Project {
                    items,
                    distinct,
                    ordering,
                    predicate,
                    skip,
                    limit,
                } => {
                    assert!(
                        !distinct
                            && ordering.is_empty()
                            && items.iter().all(|item| !item.expression.has_aggregate())
                    );
                    Stage::Project {
                        items,
                        predicate: predicate.as_ref(),
                        skip: skip
                            .as_ref()
                            .map(|e| evaluation.eval(e).and_then(|v| r::nonnegative(&v)))
                            .transpose()?
                            .unwrap_or(0),
                        remaining: limit
                            .as_ref()
                            .map(|e| evaluation.eval(e).and_then(|v| r::nonnegative(&v)))
                            .transpose()?
                            .unwrap_or(usize::MAX),
                    }
                }
                r::Operator::Filter(predicate) => Stage::Filter(predicate),
                r::Operator::Unwind { expression, slot } => Stage::Unwind {
                    expression,
                    slot: *slot,
                },
                r::Operator::Match {
                    pattern,
                    optional,
                    predicate,
                } => {
                    let operation = matches::Match {
                        pattern,
                        optional: *optional,
                        predicate: predicate.as_deref(),
                        demand: usize::MAX,
                    };
                    let physical = &plan.matches()[&(start + offset)];
                    if matches!(physical.steps.as_slice(), [r::MatchStep::IndexLookup(_)]) {
                        Stage::Match(NodeMatch::new(operation, physical))
                    } else {
                        Stage::BoundMatch(BoundMatch::new(operation, physical))
                    }
                }
                r::Operator::Create(_) | r::Operator::Update(_) | r::Operator::Delete { .. } => {
                    unreachable!("validated nonblocking pipeline stage")
                }
            });
        }
        let stack = Vec::<Expansion<'_>>::with_capacity(expansions);
        // Expand using continuations, not one recursively polled stream per
        // clause. A source batch and each suspended expansion own admitted input.
        let batches = self
            .row_budget()
            .admitted_stream(futures::stream::try_unfold(
                (
                    self.row_budget().admitted_stream(batches)?,
                    stages,
                    stack,
                    memory,
                    false,
                ),
                move |(mut batches, mut stages, mut stack, memory, mut input_started)| async move {
                    'input: loop {
                        self.check_execution_deadline()?;
                        let (mut rows, first) = if let Some(expansion) = stack.last_mut() {
                            let next = match (&mut stages[expansion.stage], &mut expansion.cursor) {
                                (
                                    Stage::Unwind { expression, slot },
                                    PipelineCursor::Unwind(cursor),
                                ) => {
                                    self.row_budget()
                                        .admitted_future(cursor.next_batch(
                                            self, expression, *slot, parameters, limits,
                                        ))?
                                        .await?
                                }
                                (Stage::Match(stage), PipelineCursor::Match(cursor)) => {
                                    self.row_budget()
                                        .admitted_future(
                                            stage.next_batch(cursor, self, parameters, limits),
                                        )?
                                        .await?
                                }
                                (Stage::BoundMatch(stage), PipelineCursor::BoundMatch(cursor)) => {
                                    self.row_budget()
                                        .admitted_future(
                                            stage.next_batch(cursor, self, parameters, limits),
                                        )?
                                        .await?
                                }
                                _ => unreachable!("continuation matches its stage"),
                            };
                            let Some(rows) = next else {
                                stack.pop();
                                continue;
                            };
                            (rows, expansion.stage + 1)
                        } else {
                            // Drain downstream continuations before stopping the
                            // upstream source; a limited row may still expand.
                            if stop.is_some_and(|(stage, termination)| {
                                matches!(stages[stage], Stage::Project { remaining: 0, .. })
                                    && termination.may_stop(input_started)
                            }) {
                                return Ok(None);
                            }
                            let Some(batch) = batches.next().await else {
                                return Ok(None);
                            };
                            input_started = true;
                            (batch?, 0)
                        };
                        for (position, stage) in stages.iter_mut().enumerate().skip(first) {
                            self.check_execution_deadline()?;
                            rows = match stage {
                                Stage::Project {
                                    items,
                                    predicate,
                                    skip,
                                    remaining,
                                } => {
                                    let projected = self
                                        .row_budget()
                                        .admitted_future(self.project_rows(
                                            rows,
                                            width,
                                            Projection {
                                                items,
                                                distinct: false,
                                                ordering: &[],
                                                predicate: *predicate,
                                                skip: None,
                                                limit: None,
                                            },
                                            parameters,
                                            limits,
                                        ))?
                                        .await?;
                                    let mut output = RowBuffer::new(self.row_budget())?;
                                    for row in projected {
                                        if *skip > 0 {
                                            *skip -= 1;
                                        } else if *remaining > 0 {
                                            push_row(&mut output, row, limits)?;
                                            *remaining -= 1;
                                        }
                                    }
                                    output.finish()
                                }
                                Stage::Filter(predicate) => {
                                    self.row_budget()
                                        .admitted_future(
                                            self.filter_relation(
                                                rows, predicate, parameters, limits,
                                            ),
                                        )?
                                        .await?
                                }
                                Stage::Unwind { .. } | Stage::Match(_) | Stage::BoundMatch(_) => {
                                    assert!(
                                        stack.len() < stack.capacity(),
                                        "one continuation per expansion stage"
                                    );
                                    stack.push(Expansion {
                                        stage: position,
                                        cursor: match stage {
                                            Stage::Unwind { .. } => {
                                                PipelineCursor::Unwind(UnwindCursor::new(rows))
                                            }
                                            Stage::Match(_) => PipelineCursor::Match(
                                                MatchCursor::new(rows, self.row_budget())?,
                                            ),
                                            Stage::BoundMatch(_) => PipelineCursor::BoundMatch(
                                                BoundCursor::new(rows, self.row_budget())?,
                                            ),
                                            Stage::Project { .. } | Stage::Filter(_) => {
                                                unreachable!("expanding stage")
                                            }
                                        },
                                    });
                                    continue 'input;
                                }
                            };
                        }
                        if !rows.is_empty() {
                            return Ok(Some((
                                rows,
                                (batches, stages, stack, memory, input_started),
                            )));
                        }
                    }
                },
            ))?;
        if projected_terminal {
            let mut batches = batches;
            let mut output = RowBuffer::new(self.row_budget())?;
            while let Some(batch) = batches.next().await {
                for row in batch? {
                    push_row(&mut output, row, limits)?;
                }
            }
            return Ok((output.finish(), ConsumedProjection::Complete(end)));
        }
        let projection = Projection {
            items,
            distinct: *distinct,
            ordering,
            predicate: predicate.as_ref(),
            skip: skip.as_ref(),
            limit: limit.as_ref(),
        };
        if aggregate {
            Ok((
                self.row_budget()
                    .admitted_future(
                        self.aggregate_batches(batches, width, items, parameters, limits),
                    )?
                    .await?
                    .finish(),
                ConsumedProjection::Aggregate(end),
            ))
        } else {
            assert!(
                !ordering.is_empty(),
                "validated terminal consumer is aggregation, projection, or top-k"
            );
            Ok((
                self.row_budget()
                    .admitted_future(self.top_k_batches(batches, projection, parameters, limits))?
                    .await?,
                ConsumedProjection::Complete(end),
            ))
        }
    }
}

struct RowSelection<'a, 'db> {
    context: &'a ExecutionContext<'db>,
    graph: &'a GraphBatch,
    parameters: &'a BTreeMap<String, r::Value>,
    limits: Limits,
    output: &'a mut RowBuffer,
}
impl r::SelectionEvaluator<r::Expression> for RowSelection<'_, '_> {
    type Row = r::Row;
    type Error = crate::cypher::Error;
    async fn evaluate(&mut self, row: &r::Row, expression: &r::Expression) -> Result<r::Selection> {
        self.context.check_execution_deadline()?;
        Ok(self
            .context
            .evaluate(row, self.parameters, self.graph, self.limits)
            .eval(expression)?
            .truth()?
            .into())
    }
    fn retain(&mut self, row: r::Row) -> Result<()> {
        push_row(self.output, row, self.limits)
    }
}
