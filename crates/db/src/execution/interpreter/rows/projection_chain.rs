//! Nonblocking row stages with an explicit expansion stack. Every projection
//! owns global window counters; mutations run only after the consumer finishes.
use super::{
    memory::Rows, projection::Projection, push_row, row_bytes, streaming::UnwindCursor,
    ConsumedProjection, ExecutionContext, GraphBatch, Limits, Result, RowBuffer,
};
use futures::StreamExt;
use helix_planner::relational as r;
use std::{collections::BTreeMap, ops};

enum Stage<'a> {
    Project {
        items: &'a r::ProjectionProgram,
        predicate: Option<&'a r::Expression>,
        skip: usize,
        remaining: usize,
    },
    Filter(&'a r::Expression),
    Unwind {
        expression: &'a r::Expression,
        slot: r::Slot,
    },
}
struct Expansion {
    stage: usize,
    cursor: UnwindCursor,
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
                return Box::pin(self.projection_chain(
                    batches,
                    plan.query(),
                    source + 1..=end,
                    plan.input_window(source),
                    parameters,
                    limits,
                ))
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
                Box::pin(self.aggregate_batches(batches, width, items, parameters, limits))
                    .await?
                    .finish(),
                ConsumedProjection::Aggregate(end),
            )),
            r::BatchConsumer::Project { termination } => Ok((
                Box::pin(self.project_batches(
                    batches,
                    width,
                    projection,
                    parameters,
                    limits,
                    termination,
                ))
                .await?,
                ConsumedProjection::Complete(end),
            )),
            r::BatchConsumer::TopK => Ok((
                Box::pin(self.top_k_batches(batches, projection, parameters, limits)).await?,
                ConsumedProjection::Complete(end),
            )),
            r::BatchConsumer::Pipeline { .. } => unreachable!("pipeline was handled above"),
        }
    }

    pub(super) async fn filter_relation(
        &self,
        rows: Rows,
        predicate: &r::Expression,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows> {
        let mut output = RowBuffer::new(self.row_budget())?;
        for batch in rows.chunks(limits.batch_rows) {
            self.check_execution_deadline()?;
            let graph = self.expression_graph_batch(batch, [predicate]).await?;
            for row in batch {
                if self
                    .evaluate(row, parameters, &graph, limits)
                    .eval(predicate)?
                    .truth()?
                    == Some(true)
                {
                    output.push_with(row_bytes(row), || row.clone())?;
                }
            }
        }
        Ok(output.finish())
    }

    pub(super) async fn projection_chain<'a, S>(
        &'a self,
        batches: S,
        query: &'a r::Query,
        operators: ops::RangeInclusive<usize>,
        window: Option<&'a r::InputWindow>,
        parameters: &'a BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<(Rows, ConsumedProjection)>
    where
        S: futures::Stream<Item = Result<Rows>> + Send + 'a,
    {
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
            .filter(|op| matches!(op, r::Operator::Unwind { .. }))
            .count();
        let memory = self.row_budget().reserve(
            size_of::<Vec<Stage<'_>>>()
                .saturating_add(operators.len().saturating_mul(size_of::<Stage<'_>>()))
                .saturating_add(size_of::<Vec<Expansion>>())
                .saturating_add(expansions.saturating_mul(size_of::<Expansion>()))
                .saturating_add(size_of::<S>()),
        )?;
        let empty = GraphBatch::default();
        let evaluation = self.evaluate(&[], parameters, &empty, limits);
        let mut stages = Vec::with_capacity(operators.len());
        for operator in operators {
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
                r::Operator::Match { .. }
                | r::Operator::Create(_)
                | r::Operator::Update(_)
                | r::Operator::Delete { .. } => {
                    unreachable!("validated nonblocking pipeline stage")
                }
            });
        }
        let stack = Vec::<Expansion>::with_capacity(expansions);
        // Expand using continuations, not one recursively polled stream per
        // clause. A source batch and each suspended UNWIND own admitted input.
        let batches = Box::pin(futures::stream::try_unfold(
            (Box::pin(batches), stages, stack, memory, false),
            move |(mut batches, mut stages, mut stack, memory, mut input_started)| async move {
                'input: loop {
                    self.check_execution_deadline()?;
                    let (mut rows, first) = if let Some(expansion) = stack.last_mut() {
                        let Stage::Unwind { expression, slot } = &stages[expansion.stage] else {
                            unreachable!("expansion resumes an UNWIND stage");
                        };
                        let Some(rows) = Box::pin(
                            expansion
                                .cursor
                                .next_batch(self, expression, *slot, parameters, limits),
                        )
                        .await?
                        else {
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
                                let projected = Box::pin(self.project_rows(
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
                                ))
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
                                Box::pin(self.filter_relation(rows, predicate, parameters, limits))
                                    .await?
                            }
                            Stage::Unwind { .. } => {
                                assert!(
                                    stack.len() < stack.capacity(),
                                    "one continuation per UNWIND stage"
                                );
                                stack.push(Expansion {
                                    stage: position,
                                    cursor: UnwindCursor::new(rows),
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
        ));
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
                Box::pin(self.aggregate_batches(batches, width, items, parameters, limits))
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
                Box::pin(self.top_k_batches(batches, projection, parameters, limits)).await?,
                ConsumedProjection::Complete(end),
            ))
        }
    }
}
