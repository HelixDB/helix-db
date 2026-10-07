//! Cartesian continuations replay an admitted source prefix and extend it only
//! on demand. One stage owns each source; frames borrow it only while polling.
use super::{memory, scan::NodeCursor, ExecutionContext, Limits, Result, RowBuffer};
use crate::query_resources::bitmap;
use helix_planner::relational as r;

/// Arrival-ordered IDs a range source keeps before reading the rest of its
/// range at once: a short or limited read stays lazy, while a long one
/// replays the remainder from a compressed bitmap instead of eight bytes per
/// ID. Unit tests use a small prefix so both parts are exercised.
const ARRIVAL_PREFIX_IDS: usize = if cfg!(test) { 4 } else { 64 * 1024 };

/// IDs a source has delivered, replayed to each parent by position.
enum Delivered {
    /// A source yielding increasing IDs, kept compressed. Position `n` is the
    /// ID of rank `n`, which later IDs never move.
    Increasing {
        ids: bitmap::Builder,
        last: Option<u64>,
    },
    /// A range source yields its owners in index order. Up to
    /// [`ARRIVAL_PREFIX_IDS`] keep that order; once a parent needs more, the
    /// rest of the range is read at once and follows the prefix in rank order.
    Arrival {
        prefix: Vec<u64>,
        memory: memory::Reservation,
        rest: Option<bitmap::Bitmap>,
    },
}

impl Delivered {
    /// Visit up to `limit` IDs from `position` on and return how many.
    fn replay(
        &self,
        position: usize,
        limit: usize,
        mut visit: impl FnMut(u64) -> Result<()>,
    ) -> Result<usize> {
        let mut replayed = 0;
        match self {
            Self::Increasing { ids, .. } => {
                replayed += replay_ranked(ids.treemap(), position, limit, &mut visit)?;
            }
            Self::Arrival { prefix, rest, .. } => {
                for &id in prefix.iter().skip(position).take(limit) {
                    visit(id)?;
                    replayed += 1;
                }
                let Some(rest) = rest.as_ref().filter(|_| replayed < limit) else {
                    return Ok(replayed);
                };
                replayed += replay_ranked(
                    rest,
                    position.saturating_sub(prefix.len()),
                    limit - replayed,
                    &mut visit,
                )?;
            }
        }
        Ok(replayed)
    }

    fn push(&mut self, id: u64) -> Result<()> {
        match self {
            Self::Increasing { ids, last } => {
                assert!(
                    last.is_none_or(|previous| previous < id),
                    "node source IDs increase"
                );
                ids.insert(id)?;
                *last = Some(id);
            }
            Self::Arrival { prefix, memory, .. } => {
                assert!(
                    prefix.len() < ARRIVAL_PREFIX_IDS,
                    "a range source reads no further than its arrival prefix"
                );
                if prefix.len() == prefix.capacity() {
                    // Admit the old and new buffers together before growing.
                    let capacity = prefix
                        .capacity()
                        .saturating_mul(2)
                        .clamp(ARRIVAL_PREFIX_IDS.min(64), ARRIVAL_PREFIX_IDS);
                    memory.resize((prefix.capacity() + capacity) * size_of::<u64>())?;
                    prefix.reserve_exact(capacity - prefix.len());
                    memory.resize(prefix.capacity() * size_of::<u64>())?;
                }
                prefix.push(id);
            }
        }
        Ok(())
    }
}

/// Visit up to `limit` members of `ids` from rank `start` on, in rank order.
fn replay_ranked(
    ids: &roaring::RoaringTreemap,
    start: usize,
    limit: usize,
    visit: &mut impl FnMut(u64) -> Result<()>,
) -> Result<usize> {
    let Some(first) = ids.select(start as u64) else {
        return Ok(0);
    };
    let mut ids = ids.iter();
    ids.advance_to(first);
    ids.take(limit).try_fold(0, |replayed, id| {
        visit(id)?;
        Ok(replayed + 1)
    })
}

enum Source {
    Open {
        delivered: Delivered,
        cursor: Box<NodeCursor>,
    },
    Complete(Delivered),
    // Taking the continuation across an await closes it until success restores
    // ownership. A cancelled or failed poll cannot be resumed or reopened.
    Closed,
}

pub(super) struct ScanCache {
    source: Source,
    _memory: memory::Reservation,
}
impl ScanCache {
    pub(super) fn new(cursor: NodeCursor, budget: &memory::Budget) -> Result<Box<Self>> {
        // Keep the enum compact. Cover the cache box and cursor boxes during
        // continuation transfer; ID payloads own separate reservations.
        let memory = budget.reserve(size_of::<Self>() + 2 * size_of::<NodeCursor>())?;
        let delivered = match cursor {
            NodeCursor::Range(_) => Delivered::Arrival {
                prefix: Vec::new(),
                memory: budget.reserve(0)?,
                rest: None,
            },
            NodeCursor::Scan(_) | NodeCursor::Indexed { .. } => Delivered::Increasing {
                ids: bitmap::Builder::new(Some(budget))?,
                last: None,
            },
        };
        Ok(Box::new(Self {
            source: Source::Open {
                delivered,
                cursor: Box::new(cursor),
            },
            _memory: memory,
        }))
    }

    fn replay(
        &self,
        position: usize,
        limit: usize,
        visit: impl FnMut(u64) -> Result<()>,
    ) -> Result<usize> {
        match &self.source {
            Source::Open { delivered, .. } | Source::Complete(delivered) => {
                delivered.replay(position, limit, visit)
            }
            Source::Closed => Err(crate::HelixDbError::InvariantViolation(
                "a failed or cancelled source continuation cannot resume".into(),
            )
            .into()),
        }
    }

    async fn extend(&mut self, context: &ExecutionContext<'_>, limits: Limits) -> Result<bool> {
        let source = std::mem::replace(&mut self.source, Source::Closed);
        let (mut delivered, cursor) = match source {
            Source::Open { delivered, cursor } => (delivered, cursor),
            Source::Complete(delivered) => {
                self.source = Source::Complete(delivered);
                return Ok(false);
            }
            Source::Closed => {
                return Err(crate::HelixDbError::InvariantViolation(
                    "a failed or cancelled source continuation cannot resume".into(),
                )
                .into())
            }
        };
        let batch_rows = match &delivered {
            Delivered::Increasing { .. } => limits.batch_rows,
            Delivered::Arrival { prefix, .. } => ARRIVAL_PREFIX_IDS - prefix.len(),
        };
        if batch_rows == 0 {
            // Past its prefix, a range source is read to its end at once into
            // a compressed bitmap, so every later position has a fixed rank.
            let mut cursor = *cursor;
            let mut rest = bitmap::Builder::new(Some(context.row_budget()))?;
            let drain = Limits {
                batch_rows: 512,
                ..limits
            };
            while let Some((batch, next)) = context
                .row_budget()
                .admitted_future(cursor.next_batch(context, 1, r::Slot(0), drain))?
                .await?
            {
                for row in batch {
                    let r::Value::Entity(r::Entity::Node(id)) = row[0] else {
                        unreachable!("a node source yields node references");
                    };
                    rest.insert(id)?;
                }
                cursor = next;
            }
            let rest = rest.finish();
            let extended = !rest.is_empty();
            let Delivered::Arrival { prefix, memory, .. } = delivered else {
                unreachable!("only a range source stops at a prefix");
            };
            self.source = Source::Complete(Delivered::Arrival {
                prefix,
                memory,
                rest: Some(rest),
            });
            return Ok(extended);
        }
        let limits = Limits {
            batch_rows: limits.batch_rows.min(batch_rows),
            ..limits
        };
        let Some((batch, cursor)) = context
            .row_budget()
            .admitted_future((*cursor).next_batch(context, 1, r::Slot(0), limits))?
            .await?
        else {
            self.source = Source::Complete(delivered);
            return Ok(false);
        };
        for row in batch {
            let r::Value::Entity(r::Entity::Node(id)) = row[0] else {
                unreachable!("a node source yields node references");
            };
            delivered.push(id)?;
        }
        self.source = Source::Open {
            delivered,
            cursor: Box::new(cursor),
        };
        Ok(true)
    }
}

pub(super) struct ScanCursor {
    input: memory::Rows,
    slot: r::Slot,
    parent: usize,
    /// Delivered IDs already paired with the current parent.
    position: usize,
}
impl ScanCursor {
    pub(super) fn new(input: memory::Rows, slot: r::Slot) -> Self {
        Self {
            input,
            slot,
            parent: 0,
            position: 0,
        }
    }

    pub(super) async fn next_batch(
        &mut self,
        context: &ExecutionContext<'_>,
        source: &mut ScanCache,
        limits: Limits,
    ) -> Result<Option<memory::Rows>> {
        let mut output = RowBuffer::new(context.row_budget())?;
        while output.len() < limits.batch_rows {
            context.check_execution_deadline()?;
            let Some(row) = self.input.get(self.parent) else {
                break;
            };
            self.position +=
                source.replay(self.position, limits.batch_rows - output.len(), |id| {
                    output.push_replacing(row, self.slot, r::Value::Entity(r::Entity::Node(id)))
                })?;
            if output.len() == limits.batch_rows {
                break;
            }
            if context
                .row_budget()
                .admitted_future(source.extend(
                    context,
                    Limits {
                        batch_rows: limits.batch_rows - output.len(),
                        ..limits
                    },
                ))?
                .await?
            {
                continue;
            }
            self.input.release_rows(self.parent..self.parent + 1);
            self.parent += 1;
            self.position = 0;
        }
        Ok((output.len() > 0).then(|| output.finish()))
    }
}

#[cfg(test)]
#[path = "tests/cross_product.rs"]
mod tests;
