//! Correlated fixed patterns with bound expansions and cursor-backed sources.
//! Optional fallback belongs to the outer row, after the whole pattern fails.
use super::{
    correlated_batch,
    expansion_stack::{ExpansionStack, SourceCache},
    matches, memory, ExecutionContext, Limits, Result, RowBuffer,
};
use helix_planner::relational as r;
use std::collections::BTreeMap;

pub(super) struct BoundMatch<'a> {
    operation: matches::Match<'a>,
    plan: &'a r::MatchPlan,
    sources: Option<SourceCache<'a>>,
}

pub(super) struct BoundCursor<'a> {
    input: correlated_batch::Parents,
    parent: usize,
    active: Option<ActivePattern<'a>>,
    pending_error: Option<crate::cypher::Error>,
}
struct ActivePattern<'a> {
    stack: ExpansionStack<'a>,
    pending: Option<memory::IntoRows>,
}
impl<'a> BoundCursor<'a> {
    pub(super) fn new(input: memory::Rows, budget: &memory::Budget) -> Result<Self> {
        Ok(Self {
            input: correlated_batch::Parents::new(input, budget)?,
            parent: 0,
            active: None,
            pending_error: None,
        })
    }
}

impl<'a> BoundMatch<'a> {
    pub(super) fn new(operation: matches::Match<'a>, plan: &'a r::MatchPlan) -> Self {
        Self {
            operation,
            plan,
            sources: None,
        }
    }

    /// Produce from the actual relation retained by a preceding row or write
    /// barrier. Source caches and outer-row completion belong to this stream.
    pub(super) fn batches(
        self,
        input: memory::Rows,
        context: &'a ExecutionContext<'_>,
        parameters: &'a BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> impl futures::Stream<Item = Result<memory::Rows>> + 'a {
        let cursor = BoundCursor::new(input, context.row_budget());
        futures::stream::try_unfold((self, cursor), move |(mut stage, cursor)| async move {
            let mut cursor = cursor?;
            let output = context
                .row_budget()
                .admitted_future(stage.next_batch(&mut cursor, context, parameters, limits))?
                .await?;
            Ok(output.map(|rows| (rows, (stage, Ok(cursor)))))
        })
    }

    pub(super) async fn next_batch(
        &mut self,
        cursor: &mut BoundCursor<'a>,
        context: &'a ExecutionContext<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Option<memory::Rows>> {
        // Bound the candidate width by the pattern's graph bindings. A wide
        // path must not multiply one hydration batch by every constituent slot.
        // The resource budget still admits actual values and expression demand.
        const MAX_PATTERN_BINDINGS_PER_BATCH: usize = 512;
        let batch_rows = limits
            .batch_rows
            .min(
                MAX_PATTERN_BINDINGS_PER_BATCH
                    / self
                        .operation
                        .pattern
                        .nodes
                        .len()
                        .saturating_add(self.operation.pattern.relationships.len())
                        .max(1),
            )
            .max(1);
        let graph_limits = Limits {
            batch_rows,
            ..limits
        };
        loop {
            context.check_execution_deadline()?;
            cursor.pending_error.take().map_or(Ok(()), Err)?;
            if cursor.parent == cursor.input.len() {
                return Ok(None);
            }
            let mut batch = correlated_batch::Batch::new(batch_rows, context.row_budget())?;
            let gathered: Result<()> = async {
                while batch.remaining() > 0 {
                    context.check_execution_deadline()?;
                    let Some(outer) = cursor.input.get(cursor.parent) else {
                        break;
                    };
                    if cursor.active.is_none() {
                        if !self.operation.validate_incoming(outer, self.plan)? {
                            batch.complete(cursor.parent);
                            cursor.parent += 1;
                            continue;
                        }
                        if self.sources.is_none() {
                            self.sources = Some(SourceCache::new(
                                self.plan,
                                0,
                                graph_limits.batch_rows,
                                context.row_budget(),
                            )?);
                        }
                        let mut seed = RowBuffer::new(context.row_budget())?;
                        seed.push_with(super::row_bytes(outer), || outer.clone())?;
                        let seed = seed.finish();
                        cursor.active = Some(ActivePattern {
                            stack: ExpansionStack::new(
                                self.operation.pattern,
                                self.plan,
                                0,
                                futures::stream::once(async move { Ok(seed) }),
                                context.row_budget(),
                            )?,
                            pending: None,
                        });
                    }
                    let active = cursor.active.as_mut().expect("initialized pattern");
                    match active.pending.take() {
                        Some(mut rows) => {
                            while batch.remaining() > 0 {
                                let Some(row) = rows.next() else {
                                    break;
                                };
                                batch.candidate_with(
                                    cursor.parent,
                                    super::row_bytes(&row),
                                    || row,
                                )?;
                            }
                            if !rows.as_slice().is_empty() {
                                active.pending = Some(rows);
                            }
                        }
                        None => {
                            let Some(rows) = context
                                .row_budget()
                                .admitted_future(
                                    active.stack.next_batch(
                                        context,
                                        graph_limits,
                                        self.sources
                                            .as_mut()
                                            .expect("active pattern has a source cache"),
                                    ),
                                )?
                                .await?
                            else {
                                cursor.active = None;
                                batch.complete(cursor.parent);
                                cursor.parent += 1;
                                continue;
                            };
                            active.pending = Some(rows.into_iter());
                        }
                    }
                }
                Ok(())
            }
            .await;
            match gathered {
                Ok(()) => {}
                Err(error) => {
                    cursor.pending_error = Some(error);
                    cursor.active = None;
                }
            }
            let output = context
                .row_budget()
                .admitted_future(batch.finish(
                    &mut cursor.input,
                    self.operation,
                    context,
                    parameters,
                    limits,
                ))?
                .await?;
            if !output.is_empty() {
                return Ok(Some(output));
            }
        }
    }
}
