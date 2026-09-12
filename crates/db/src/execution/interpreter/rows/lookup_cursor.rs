//! A physical index-probe continuation inside the shared graph stack. Fallback
//! scans borrow their stage-owned prefix only while polling. Pattern validation
//! and optional null extension belong to the enclosing MATCH operator.
use super::{
    cross_product::ScanCursor, expansion_stack::SourceCache, lookup, memory, ExecutionContext,
    Limits, Result, RowBuffer,
};
use helix_planner::relational as r;

enum Active {
    Ready,
    Index(Box<lookup::IndexedCursor>),
    Scan(ScanCursor),
    Complete,
    Closed,
}

pub(super) struct LookupCursor {
    input: memory::Rows,
    parent: usize,
    active: Active,
    pending_error: Option<crate::cypher::Error>,
    _memory: memory::Reservation,
}
impl LookupCursor {
    pub(super) fn new(input: memory::Rows, budget: &memory::Budget) -> Result<Box<Self>> {
        let memory = budget.reserve(size_of::<Self>())?;
        Ok(Box::new(Self {
            input,
            parent: 0,
            active: Active::Ready,
            pending_error: None,
            _memory: memory,
        }))
    }

    /// Each active source transfers into its awaited poll. A failed or cancelled
    /// transfer cannot resume or reopen it. Earlier rows precede a later source
    /// error, so their validation keeps its order.
    pub(super) async fn next_batch(
        &mut self,
        context: &ExecutionContext<'_>,
        lookup: &r::PatternLookup,
        cache: &mut SourceCache<'_>,
        limits: Limits,
    ) -> Result<Option<memory::Rows>> {
        self.pending_error.take().map_or(Ok(()), Err)?;
        let mut output = RowBuffer::new(context.row_budget())?;
        let gathered: Result<()> = async {
            while output.len() < limits.batch_rows {
                context.check_execution_deadline()?;
                let Some(row) = self.input.get(self.parent) else {
                    break;
                };
                let active = std::mem::replace(&mut self.active, Active::Closed);
                let remaining = Limits {
                    batch_rows: limits.batch_rows - output.len(),
                    ..limits
                };
                self.active = match active {
                    Active::Ready => match context
                        .row_budget()
                        .admitted_future(lookup::Probe::new(
                            context,
                            lookup,
                            &row[lookup.probe.0 as usize],
                        ))?
                        .await?
                    {
                        lookup::Probe::Empty => Active::Complete,
                        lookup::Probe::Index(cursor) => Active::Index(cursor),
                        lookup::Probe::Scan => {
                            let mut seed = RowBuffer::new(context.row_budget())?;
                            seed.push_with(super::row_bytes(row), || row.clone())?;
                            Active::Scan(ScanCursor::new(seed.finish(), lookup.slot))
                        }
                    },
                    Active::Index(cursor) => {
                        let lookup::IndexedCursor { cursor, memory } = *cursor;
                        match context
                            .row_budget()
                            .admitted_future(cursor.next_batch(context, 1, r::Slot(0), remaining))?
                            .await?
                        {
                            Some((ids, next)) => {
                                for found in ids {
                                    output.push_replacing(row, lookup.slot, found[0].clone())?;
                                }
                                Active::Index(Box::new(lookup::IndexedCursor {
                                    cursor: next,
                                    memory,
                                }))
                            }
                            None => Active::Complete,
                        }
                    }
                    Active::Scan(mut cursor) => {
                        let source = context
                            .row_budget()
                            .admitted_future(cache.scan(lookup.slot, context))?
                            .await?;
                        let next = context
                            .row_budget()
                            .admitted_future(cursor.next_batch(context, source, remaining))?
                            .await?;
                        match next {
                            Some(rows) => {
                                for row in rows {
                                    output.push_with(super::row_bytes(&row), || row)?;
                                }
                                Active::Scan(cursor)
                            }
                            None => Active::Complete,
                        }
                    }
                    Active::Complete => {
                        self.input.release_rows(self.parent..self.parent + 1);
                        self.parent += 1;
                        Active::Ready
                    }
                    Active::Closed => {
                        return Err(crate::HelixDbError::InvariantViolation(
                            "a failed or cancelled lookup continuation cannot resume".into(),
                        )
                        .into());
                    }
                };
            }
            Ok(())
        }
        .await;
        match gathered {
            Ok(()) => {}
            Err(error) if output.len() == 0 => return Err(error),
            Err(error) => self.pending_error = Some(error),
        }
        Ok((output.len() > 0).then(|| output.finish()))
    }
}
