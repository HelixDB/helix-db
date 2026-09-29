//! Cartesian continuations replay an admitted source prefix and extend it only
//! on demand. One stage owns each source; frames borrow it only while polling.
use super::{memory, scan::NodeCursor, ExecutionContext, Limits, Result, RowBuffer};
use crate::query_resources::bitmap;
use helix_planner::relational as r;

/// IDs a source has delivered, replayed to each parent by position.
enum Delivered {
    /// A source yielding increasing IDs, kept compressed. Position `n` is the
    /// ID of rank `n`, which later IDs never move.
    Increasing {
        ids: bitmap::Builder,
        last: Option<u64>,
    },
    /// A range source yields its owners in index order, so they stay in the
    /// order they arrived.
    Arrival {
        ids: Vec<u64>,
        memory: memory::Reservation,
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
                let Some(first) = ids.select(position as u64) else {
                    return Ok(0);
                };
                let mut ids = ids.iter();
                ids.advance_to(first);
                for id in ids.take(limit) {
                    visit(id)?;
                    replayed += 1;
                }
            }
            Self::Arrival { ids, .. } => {
                for &id in ids.iter().skip(position).take(limit) {
                    visit(id)?;
                    replayed += 1;
                }
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
            Self::Arrival { ids, memory } => {
                if ids.len() == ids.capacity() {
                    // Admit the old and new buffers together before growing.
                    let capacity = ids.capacity().saturating_mul(2).max(64);
                    memory.resize((ids.capacity() + capacity) * size_of::<u64>())?;
                    ids.reserve_exact(capacity - ids.len());
                    memory.resize(ids.capacity() * size_of::<u64>())?;
                }
                ids.push(id);
            }
        }
        Ok(())
    }
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
                ids: Vec::new(),
                memory: budget.reserve(0)?,
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
