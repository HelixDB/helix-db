//! Resumable indexed single-node MATCH. Plain scans and bound-node validation
//! use the shared graph stack. This adapter owns one active index lookup and an
//! optional fallback source for values without an exact index representation.
use super::{
    correlated_batch,
    cross_product::{ScanCache, ScanCursor},
    lookup, matches,
    memory::Rows,
    ExecutionContext, Limits, Result, RowBuffer,
};
use helix_planner::{exec, relational as r};
use std::collections::BTreeMap;

pub(super) struct NodeMatch<'a> {
    operation: matches::Match<'a>,
    lookup: &'a r::PatternLookup,
    fallback: &'a exec::ExecOp,
    cache: Option<Box<ScanCache>>,
}

pub(super) struct MatchCursor {
    input: correlated_batch::Parents,
    parent: usize,
    active: Option<Candidates>,
    pending_error: Option<crate::cypher::Error>,
}
enum Candidates {
    Nodes(Box<lookup::IndexedCursor>),
    Scan(ScanCursor),
    Exhausted,
}

impl MatchCursor {
    pub(super) fn new(input: Rows, budget: &crate::query_resources::Budget) -> Result<Self> {
        Ok(Self {
            input: correlated_batch::Parents::new(input, budget)?,
            parent: 0,
            active: None,
            pending_error: None,
        })
    }
}

impl<'a> NodeMatch<'a> {
    pub(super) fn new(operation: matches::Match<'a>, plan: &'a r::MatchPlan) -> Self {
        let [r::MatchStep::IndexLookup(lookup)] = plan.steps.as_slice() else {
            unreachable!("indexed node adapter requires one lookup");
        };
        assert_eq!(operation.pattern.single_node(), Some(lookup.slot));
        assert!(plan.incoming.is_empty(), "lookup binds a new graph pattern");
        let source = plan
            .sources
            .iter()
            .find(|source| source.slot == lookup.slot)
            .expect("validated lookup has a fallback source");
        let [step] = source.access.steps() else {
            unreachable!("validated lookup fallback has one cursor primitive");
        };
        assert!(step.op.node_cursor_access().is_some());
        Self {
            operation,
            lookup,
            fallback: &step.op,
            cache: None,
        }
    }

    async fn candidates(
        &mut self,
        outer: &r::Row,
        context: &ExecutionContext<'_>,
    ) -> Result<Candidates> {
        match context
            .row_budget()
            .admitted_future(lookup::Probe::new(
                context,
                self.lookup,
                &outer[self.lookup.probe.0 as usize],
            ))?
            .await?
        {
            lookup::Probe::Empty => Ok(Candidates::Exhausted),
            lookup::Probe::Index(cursor) => Ok(Candidates::Nodes(cursor)),
            lookup::Probe::Scan => {
                if self.cache.is_none() {
                    let cursor = context
                        .row_budget()
                        .admitted_future(context.node_cursor(self.fallback))?
                        .await?
                        .expect("validated fallback cursor");
                    self.cache = Some(ScanCache::new(cursor, context.row_budget())?);
                }
                let mut seed = RowBuffer::new(context.row_budget())?;
                seed.push_with(super::row_bytes(outer), || outer.clone())?;
                Ok(Candidates::Scan(ScanCursor::new(
                    seed.finish(),
                    self.lookup.slot,
                )))
            }
        }
    }

    pub(super) async fn next_batch(
        &mut self,
        cursor: &mut MatchCursor,
        context: &ExecutionContext<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Option<Rows>> {
        loop {
            context.check_execution_deadline()?;
            cursor.pending_error.take().map_or(Ok(()), Err)?;
            if cursor.parent == cursor.input.len() {
                return Ok(None);
            }
            let mut batch = correlated_batch::Batch::new(limits.batch_rows, context.row_budget())?;
            // A later source error is reported after preceding candidates have
            // been validated. This retains error order while batching hydration.
            let gathered: Result<()> = async {
                while batch.remaining() > 0 {
                    context.check_execution_deadline()?;
                    let Some(outer) = cursor.input.get(cursor.parent) else {
                        break;
                    };
                    if cursor.active.is_none() {
                        cursor.active = Some(
                            context
                                .row_budget()
                                .admitted_future(self.candidates(outer, context))?
                                .await?,
                        );
                    }
                    match cursor.active.take().expect("initialized parent cursor") {
                        Candidates::Nodes(nodes) => {
                            let count = batch.remaining();
                            let Some((ids, next)) = context
                                .row_budget()
                                .admitted_future(nodes.cursor.next_batch(
                                    context,
                                    1,
                                    r::Slot(0),
                                    Limits {
                                        batch_rows: count,
                                        ..limits
                                    },
                                ))?
                                .await?
                            else {
                                cursor.active = Some(Candidates::Exhausted);
                                continue;
                            };
                            for found in ids {
                                batch.candidate_with(
                                    cursor.parent,
                                    super::row_bytes(outer),
                                    || {
                                        let mut row = outer.clone();
                                        row[self.lookup.slot.0 as usize] = found[0].clone();
                                        row
                                    },
                                )?;
                            }
                            cursor.active =
                                Some(Candidates::Nodes(Box::new(lookup::IndexedCursor {
                                    cursor: next,
                                    memory: nodes.memory,
                                })));
                        }
                        Candidates::Scan(mut scan) => {
                            let Some(rows) = context
                                .row_budget()
                                .admitted_future(
                                    scan.next_batch(
                                        context,
                                        self.cache
                                            .as_mut()
                                            .expect("fallback cursor owns a source prefix"),
                                        Limits {
                                            batch_rows: batch.remaining(),
                                            ..limits
                                        },
                                    ),
                                )?
                                .await?
                            else {
                                cursor.active = Some(Candidates::Exhausted);
                                continue;
                            };
                            for row in rows {
                                batch.candidate_with(
                                    cursor.parent,
                                    super::row_bytes(&row),
                                    || row,
                                )?;
                            }
                            cursor.active = Some(Candidates::Scan(scan));
                        }
                        Candidates::Exhausted => {
                            batch.complete(cursor.parent);
                            cursor.parent += 1;
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
