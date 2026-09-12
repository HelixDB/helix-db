//! Cartesian continuations replay an admitted source prefix and extend it only
//! on demand. One stage owns each source; frames borrow it only while polling.
use super::{memory, scan::NodeCursor, ExecutionContext, Limits, Result, RowBuffer};
use crate::query_resources::bitmap;
use helix_planner::relational as r;

enum Source {
    Open {
        ids: bitmap::Builder,
        cursor: Box<NodeCursor>,
        last: Option<u64>,
    },
    Complete(bitmap::Bitmap),
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
        // continuation transfer; bitmap payloads own separate reservations.
        let memory = budget.reserve(size_of::<Self>() + 2 * size_of::<NodeCursor>())?;
        let ids = bitmap::Builder::new(Some(budget))?;
        Ok(Box::new(Self {
            source: Source::Open {
                ids,
                cursor: Box::new(cursor),
                last: None,
            },
            _memory: memory,
        }))
    }

    fn iter(&self) -> Result<roaring::treemap::Iter<'_>> {
        match &self.source {
            Source::Open { ids, .. } => Ok(ids.iter()),
            Source::Complete(ids) => Ok(ids.iter()),
            Source::Closed => Err(crate::HelixDbError::InvariantViolation(
                "a failed or cancelled source continuation cannot resume".into(),
            )
            .into()),
        }
    }

    async fn extend(&mut self, context: &ExecutionContext<'_>, limits: Limits) -> Result<bool> {
        let source = std::mem::replace(&mut self.source, Source::Closed);
        let (mut ids, cursor, mut last) = match source {
            Source::Open { ids, cursor, last } => (ids, cursor, last),
            Source::Complete(ids) => {
                self.source = Source::Complete(ids);
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
            self.source = Source::Complete(ids.finish());
            return Ok(false);
        };
        for row in batch {
            let r::Value::Entity(r::Entity::Node(id)) = row[0] else {
                unreachable!("a node source yields node references");
            };
            assert!(
                last.is_none_or(|previous| previous < id),
                "node source IDs increase"
            );
            ids.insert(id)?;
            last = Some(id);
        }
        self.source = Source::Open {
            ids,
            cursor: Box::new(cursor),
            last,
        };
        Ok(true)
    }
}

pub(super) struct ScanCursor {
    input: memory::Rows,
    slot: r::Slot,
    parent: usize,
    last: Option<u64>,
}
impl ScanCursor {
    pub(super) fn new(input: memory::Rows, slot: r::Slot) -> Self {
        Self {
            input,
            slot,
            parent: 0,
            last: None,
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
            {
                let mut ids = source.iter()?;
                if let Some(previous) = self.last {
                    // Seek to an existing ID, then exclude it. Seeking last+1
                    // overflows at u64::MAX and can skip sparse tree partitions.
                    ids.advance_to(previous);
                    assert_eq!(
                        ids.next(),
                        Some(previous),
                        "cursor belongs to this source prefix"
                    );
                }
                while output.len() < limits.batch_rows {
                    let Some(id) = ids.next() else {
                        break;
                    };
                    output.push_replacing(row, self.slot, r::Value::Entity(r::Entity::Node(id)))?;
                    self.last = Some(id);
                }
            }
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
            self.last = None;
        }
        Ok((output.len() > 0).then(|| output.finish()))
    }
}

#[cfg(test)]
#[path = "tests/cross_product.rs"]
mod tests;
