//! A bounded top-k consumer shared by materialized and streaming row sources.
use super::projection::Projection;
use super::{
    memory::{Batch, Budget, Reservation, Rows},
    row_bytes, ExecutionContext, GraphBatch, Limits, Result, RowBuffer,
};
use helix_planner::relational as r;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
};

impl ExecutionContext<'_> {
    pub(super) async fn top_k_batches<'rows, S>(
        &self,
        batches: S,
        width: usize,
        projection: Projection<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows>
    where
        S: futures::Stream<Item = Result<Batch<'rows>>>,
    {
        use futures::StreamExt;
        futures::pin_mut!(batches);
        let Projection {
            items,
            ordering,
            predicate,
            skip,
            limit,
            ..
        } = projection;
        let empty = GraphBatch::default();
        let evaluation = self.evaluate(&[], parameters, &empty, limits);
        let window = r::Window::evaluate(skip, limit, |expression| evaluation.eval(expression))?;
        let inputs = projection.input_slots(self, width)?;
        let mut top_k = TopK::new(self.row_budget(), window.retained_rows())?;
        let mut ordinal = 0_usize;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            let batch = batch.as_ref();
            self.check_execution_deadline()?;
            let graph = self
                .expression_graph_batch(batch, items.iter().map(|i| &i.expression))
                .await?;
            let mut projected = RowBuffer::new(self.row_budget())?;
            for row in batch.iter() {
                self.check_execution_deadline()?;
                let mut evaluation = self.evaluate(row, parameters, &graph, limits);
                let values = items.evaluate(&mut evaluation).await?;
                projected.push_projection(row, items, values, &inputs, self.row_budget())?;
            }
            drop(graph);
            let projected = projected.finish();
            let graph = self
                .expression_graph_batch(
                    &projected,
                    ordering
                        .iter()
                        .map(|o| &o.expression)
                        .chain(predicate.map(r::SelectionProgram::expression)),
                )
                .await?;
            for mut row in projected {
                let evaluation = self.evaluate(&row, parameters, &graph, limits);
                if let Some(predicate) = predicate
                    && evaluation.eval(predicate)?.truth()? != Some(true)
                {
                    continue;
                }
                let keys = evaluation.eval_sequence(ordering.iter().map(|key| &key.expression))?;
                for (index, value) in row.iter_mut().enumerate() {
                    if !items.outputs().contains(&r::Slot(index as u32)) {
                        *value = r::Value::Null;
                    }
                }
                let candidate = RankedRow {
                    keys,
                    row,
                    ordering,
                    ordinal,
                };
                ordinal = ordinal.saturating_add(1);
                top_k.push(candidate)?;
            }
        }
        Ok(top_k.into_rows(window.skip()))
    }
}

/// The heap drops before its reservation on errors and cancelled futures.
struct TopK<'a> {
    heap: BinaryHeap<RankedRow<'a>>,
    memory: Reservation,
    retained_bytes: usize,
    keep: usize,
}
impl<'a> TopK<'a> {
    fn new(budget: &Budget, keep: usize) -> Result<Self> {
        Ok(Self {
            heap: BinaryHeap::new(),
            memory: budget.reserve(0)?,
            retained_bytes: 0,
            keep,
        })
    }
    fn push(&mut self, candidate: RankedRow<'a>) -> Result<()> {
        if self.keep == 0
            || self
                .heap
                .peek()
                .is_some_and(|worst| self.heap.len() >= self.keep && &candidate >= worst)
        {
            return Ok(());
        }
        if self.heap.len() == self.keep {
            let removed = self.heap.pop().expect("nonzero full heap");
            self.retained_bytes = self
                .retained_bytes
                .saturating_sub(removed.allocated_bytes());
        }
        let retained_bytes = self
            .retained_bytes
            .saturating_add(candidate.allocated_bytes());
        let capacity = if self.heap.len() == self.heap.capacity() {
            self.heap.capacity().saturating_mul(2).max(4).min(self.keep)
        } else {
            self.heap.capacity()
        };
        self.memory.resize(
            retained_bytes.saturating_add(capacity.saturating_mul(size_of::<RankedRow<'_>>())),
        )?;
        if capacity > self.heap.capacity() {
            self.heap
                .try_reserve_exact(capacity - self.heap.len())
                .map_err(|_| super::resource("MemoryLimit", "top-k allocation failed"))?;
        }
        self.heap.push(candidate);
        self.retained_bytes = retained_bytes;
        Ok(())
    }
    fn into_rows(self, skip: usize) -> Rows {
        // Retained candidates pre-admit final row slots. Sorting reuses heap
        // storage; moving payload into the exact output needs no second charge.
        let count = self.heap.len().saturating_sub(skip);
        let mut rows = Vec::with_capacity(count);
        for entry in self.heap.into_sorted_vec().into_iter().skip(skip) {
            assert!(rows.len() < count, "fixed top-k output capacity");
            rows.push(entry.row);
        }
        Rows::from_admitted(rows, self.memory)
    }
}

/// The heap root is the worst retained row. A stable ordinal breaks ties
/// without changing equality/grouping semantics or depending on storage order.
struct RankedRow<'a> {
    keys: Vec<r::Value>,
    row: r::Row,
    ordering: &'a [r::Ordering],
    ordinal: usize,
}
impl RankedRow<'_> {
    fn allocated_bytes(&self) -> usize {
        // Heap storage is counted separately. Reserve payload plus the final
        // row slot that coexists with that storage during ownership transfer.
        row_bytes(&self.row)
            .saturating_add(row_bytes(&self.keys).saturating_sub(size_of::<Vec<r::Value>>()))
    }
}
impl PartialEq for RankedRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for RankedRow<'_> {}
impl PartialOrd for RankedRow<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RankedRow<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ordering
            .iter()
            .zip(self.keys.iter().zip(&other.keys))
            .map(|(order, (a, b))| {
                let cmp = a.total_cmp(b);
                if order.descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            })
            .find(|cmp| !cmp.is_eq())
            .unwrap_or_else(|| self.ordinal.cmp(&other.ordinal))
    }
}
