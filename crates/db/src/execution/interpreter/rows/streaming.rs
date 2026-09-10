//! Bounded generators feeding shared row consumers. Input properties are fetched
//! in batches; expansion state owns its rows and never retains a database handle.
use super::{memory, ExecutionContext, GraphBatch, Limits, Result, RowBuffer};
use helix_planner::relational as r;
use std::collections::BTreeMap;

struct ExpandedRow {
    row: r::Row,
    values: r::UnwindValues,
    _memory: memory::Reservation,
}

/// Resumable expansion, also usable between two nonblocking row operators.
/// The input reservation remains alive while a moved row is being expanded.
/// Graph hydration covers exactly the next `hydrated` input rows.
pub(super) struct UnwindCursor {
    rows: memory::IntoRows,
    graph: GraphBatch,
    hydrated: usize,
    current: Option<ExpandedRow>,
}
impl UnwindCursor {
    pub(super) fn new(rows: memory::Rows) -> Self {
        Self {
            rows: rows.into_iter(),
            graph: GraphBatch::default(),
            hydrated: 0,
            current: None,
        }
    }

    pub(super) async fn next_batch(
        &mut self,
        context: &ExecutionContext<'_>,
        expression: &r::Expression,
        slot: r::Slot,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Option<memory::Rows>> {
        assert!(limits.batch_rows > 0, "validated batch width");
        let mut output = RowBuffer::new(context.row_budget())?;
        while output.len() < limits.batch_rows {
            context.check_execution_deadline()?;
            if let Some(cursor) = &mut self.current
                && let Some(value) = cursor.values.next()
            {
                output.push_replacing(&cursor.row, slot, value)?;
                continue;
            }
            self.current = None;
            if self.hydrated == 0 {
                self.graph = GraphBatch::default();
                let count = self.rows.as_slice().len().min(limits.batch_rows);
                if count == 0 {
                    break;
                }
                self.graph = context
                    .expression_graph_batch(&self.rows.as_slice()[..count], [expression])
                    .await?;
                self.hydrated = count;
            }
            let row = self.rows.next().expect("hydration covers pending input");
            self.hydrated -= 1;
            // The input iterator still owns the row reservation. Admit the
            // cursor header before evaluation, then transfer the evaluator's
            // allocation bound to a reservation that follows the yielded list.
            let mut memory = context.row_budget().reserve(size_of::<ExpandedRow>())?;
            let values = context
                .evaluate(&row, parameters, &self.graph, limits)
                .unwind(expression)?;
            memory.resize(
                size_of::<ExpandedRow>().saturating_add(
                    values
                        .allocated_bytes()
                        .saturating_sub(size_of::<r::UnwindValues>()),
                ),
            )?;
            if self.hydrated == 0 {
                self.graph = GraphBatch::default();
            }
            self.current = Some(ExpandedRow {
                row,
                values,
                _memory: memory,
            });
        }
        Ok((output.len() > 0).then(|| output.finish()))
    }
}

impl ExecutionContext<'_> {
    pub(super) fn unwind_batches<'a>(
        &'a self,
        rows: memory::Rows,
        expression: &'a r::Expression,
        slot: r::Slot,
        parameters: &'a BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> impl futures::Stream<Item = Result<memory::Rows>> + 'a {
        futures::stream::try_unfold(UnwindCursor::new(rows), move |mut cursor| async move {
            Ok(cursor
                .next_batch(self, expression, slot, parameters, limits)
                .await?
                .map(|batch| (batch, cursor)))
        })
    }
}
