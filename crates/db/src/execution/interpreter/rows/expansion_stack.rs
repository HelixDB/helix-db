//! A depth-first continuation stack for fixed graph patterns. Each level owns
//! one admitted batch; frames and boxed stream state are admitted before growth.
use super::{
    cross_product::{ScanCache, ScanCursor},
    joins::HashJoinTable,
    lookup_cursor::LookupCursor,
    memory, ExecutionContext, Limits, Result,
};
use futures::{Stream, StreamExt};
use helix_planner::relational as r;
use std::{collections::BTreeMap, pin::Pin, sync::Arc};

struct Frame<'a> {
    depth: usize,
    cursor: FrameCursor<'a>,
    _memory: memory::Reservation,
}
enum FrameCursor<'a> {
    Stream(Pin<Box<dyn Stream<Item = Result<memory::Rows>> + Send + 'a>>),
    Scan {
        slot: r::Slot,
        cursor: ScanCursor,
    },
    Lookup {
        lookup: &'a r::PatternLookup,
        cursor: Box<LookupCursor>,
    },
}
impl<'a> Frame<'a> {
    fn new<S>(depth: usize, batches: S, budget: &memory::Budget) -> Result<Self>
    where
        S: Stream<Item = Result<memory::Rows>> + Send + 'a,
    {
        let memory = budget.reserve(size_of::<S>())?;
        Ok(Self {
            depth,
            cursor: FrameCursor::Stream(Box::pin(batches)),
            _memory: memory,
        })
    }
}

enum CachedSource {
    Scan(Box<ScanCache>),
    Hash(Arc<HashJoinTable>),
}

/// Shared across every continuation of one MATCH stage. Scan frames borrow the
/// mutable source prefix while polling; hash frames retain immutable table
/// handles. No database handle or cross-frame cache borrow is retained.
pub(super) struct SourceCache<'a> {
    plan: &'a r::MatchPlan,
    sources: BTreeMap<r::Slot, CachedSource>,
    build_batch_rows: usize,
    _memory: memory::Reservation,
}
impl<'a> SourceCache<'a> {
    pub(super) fn new(
        plan: &'a r::MatchPlan,
        completed: usize,
        build_batch_rows: usize,
        budget: &memory::Budget,
    ) -> Result<Self> {
        assert!(completed <= plan.steps.len());
        assert!(build_batch_rows > 0);
        let count = plan.steps[completed..]
            .iter()
            .filter(|step| {
                matches!(
                    step,
                    r::MatchStep::Scan(_)
                        | r::MatchStep::HashJoin { .. }
                        | r::MatchStep::IndexLookup(_)
                )
            })
            .count();
        let bytes = if count == 0 {
            0
        } else {
            r::allocation::btree_bytes::<r::Slot, CachedSource>(count)
        };
        Ok(Self {
            plan,
            sources: BTreeMap::new(),
            build_batch_rows,
            _memory: budget.reserve(bytes)?,
        })
    }

    /// Open each original scan source at most once, and only on demand. Indexed
    /// probes borrow this same prefix when native equality cannot represent them.
    pub(super) async fn scan(
        &mut self,
        slot: r::Slot,
        context: &ExecutionContext<'_>,
    ) -> Result<&mut ScanCache> {
        match self.sources.get(&slot) {
            Some(CachedSource::Scan(_)) => {}
            Some(CachedSource::Hash(_)) => unreachable!("one physical source step per slot"),
            None => {
                let source = self
                    .plan
                    .sources
                    .iter()
                    .find(|source| source.slot == slot)
                    .expect("validated scan has an original source");
                let [step] = source.access.steps() else {
                    unreachable!("validated source has one cursor primitive");
                };
                let cursor = context
                    .row_budget()
                    .admitted_future(context.node_cursor(&step.op))?
                    .await?
                    .expect("validated source supports a cursor");
                let source = ScanCache::new(cursor, context.row_budget())?;
                self.sources.insert(slot, CachedSource::Scan(source));
            }
        }
        let Some(CachedSource::Scan(source)) = self.sources.get_mut(&slot) else {
            unreachable!("initialized scan source");
        };
        Ok(source)
    }
}

/// The source has already applied `completed` physical steps. All remaining
/// steps must have cursor-backed inputs, including indexed-probe fallbacks.
/// Input bindings and selected access capabilities are validated by the planner.
/// A failed poll consumes no recoverable state: callers drop this continuation.
pub(super) struct ExpansionStack<'a> {
    pattern: &'a r::Pattern,
    plan: &'a r::MatchPlan,
    frames: Vec<Frame<'a>>,
    _memory: memory::Reservation,
}
impl<'a> ExpansionStack<'a> {
    pub(super) fn new<S>(
        pattern: &'a r::Pattern,
        plan: &'a r::MatchPlan,
        completed: usize,
        source: S,
        budget: &memory::Budget,
    ) -> Result<Self>
    where
        S: Stream<Item = Result<memory::Rows>> + Send + 'a,
    {
        let steps = &plan.steps;
        assert!(completed <= steps.len());
        assert!(steps[completed..].iter().all(|step| matches!(
            step,
            r::MatchStep::Scan(_)
                | r::MatchStep::Expand { .. }
                | r::MatchStep::HashJoin { .. }
                | r::MatchStep::IndexLookup(_)
        )));
        let capacity = steps.len() - completed + 1;
        let memory = budget.reserve(
            size_of::<Self>().saturating_add(capacity.saturating_mul(size_of::<Frame<'_>>())),
        )?;
        let mut frames = Vec::with_capacity(capacity);
        frames.push(Frame::new(completed, source, budget)?);
        Ok(Self {
            pattern,
            plan,
            frames,
            _memory: memory,
        })
    }

    pub(super) async fn next_batch(
        &mut self,
        context: &'a ExecutionContext<'_>,
        limits: Limits,
        cache: &mut SourceCache<'a>,
    ) -> Result<Option<memory::Rows>> {
        assert!(
            std::ptr::eq(self.plan, cache.plan),
            "source cache belongs to this physical pattern"
        );
        // Hash build inputs are consumed completely; keep their original width.
        // Scan caches instead extend only when a candidate frame needs more IDs.
        let build_limits = Limits {
            batch_rows: cache.build_batch_rows,
            ..limits
        };
        loop {
            context.check_execution_deadline()?;
            let Some(frame) = self.frames.last_mut() else {
                return Ok(None);
            };
            let batch = match &mut frame.cursor {
                FrameCursor::Stream(batches) => batches.next().await.transpose()?,
                FrameCursor::Scan { slot, cursor } => {
                    let source = context
                        .row_budget()
                        .admitted_future(cache.scan(*slot, context))?
                        .await?;
                    context
                        .row_budget()
                        .admitted_future(cursor.next_batch(context, source, limits))?
                        .await?
                }
                FrameCursor::Lookup { lookup, cursor } => {
                    context
                        .row_budget()
                        .admitted_future(cursor.next_batch(context, lookup, cache, limits))?
                        .await?
                }
            };
            let Some(batch) = batch else {
                self.frames.pop();
                continue;
            };
            let depth = frame.depth;
            if depth == self.plan.steps.len() {
                return Ok(Some(batch));
            }
            assert!(self.frames.len() < self.frames.capacity());
            let frame = match &self.plan.steps[depth] {
                step @ r::MatchStep::Expand { .. } => Frame::new(
                    depth + 1,
                    context.expansion_batches(batch, self.pattern, step, limits),
                    context.row_budget(),
                )?,
                r::MatchStep::HashJoin {
                    slot,
                    property,
                    probe,
                    probe_property,
                } => {
                    let table = match cache.sources.get(slot) {
                        Some(CachedSource::Hash(table)) => Arc::clone(table),
                        Some(CachedSource::Scan(_)) => {
                            unreachable!("one physical source step per slot")
                        }
                        None => {
                            let source = self
                                .plan
                                .sources
                                .iter()
                                .find(|source| source.slot == *slot)
                                .expect("validated hash join has a build source");
                            let [step] = source.access.steps() else {
                                unreachable!("validated hash source has one cursor primitive");
                            };
                            let cursor = context
                                .row_budget()
                                .admitted_future(context.node_cursor(&step.op))?
                                .await?
                                .expect("validated hash source supports a cursor");
                            let width = batch
                                .first()
                                .expect("source cursors emit nonempty batches")
                                .len();
                            let batches =
                                context.node_id_batches(cursor, width, *slot, build_limits);
                            let table = context
                                .row_budget()
                                .admitted_future(HashJoinTable::build_batches(
                                    context, batches, *slot, property,
                                ))?
                                .await?;
                            // The table reservation includes its Arc allocation;
                            // cache nodes were admitted before any insertion.
                            let table = Arc::new(table);
                            cache
                                .sources
                                .insert(*slot, CachedSource::Hash(Arc::clone(&table)));
                            table
                        }
                    };
                    Frame::new(
                        depth + 1,
                        table.probe_batches(context, batch, *slot, *probe, probe_property, limits),
                        context.row_budget(),
                    )?
                }
                r::MatchStep::Scan(slot) => {
                    Frame {
                        depth: depth + 1,
                        cursor: FrameCursor::Scan {
                            slot: *slot,
                            cursor: ScanCursor::new(batch, *slot),
                        },
                        // Inline scan state was admitted with the frame vector.
                        _memory: context.row_budget().reserve(0)?,
                    }
                }
                r::MatchStep::IndexLookup(lookup) => Frame {
                    depth: depth + 1,
                    cursor: FrameCursor::Lookup {
                        lookup,
                        cursor: LookupCursor::new(batch, context.row_budget())?,
                    },
                    _memory: context.row_budget().reserve(0)?,
                },
            };
            self.frames.push(frame);
        }
    }
}
