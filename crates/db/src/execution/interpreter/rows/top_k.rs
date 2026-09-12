//! A bounded top-k consumer shared by materialized and streaming row sources.
use super::projection::Projection;
use super::{
    memory::Rows, push_row, row_bytes, ExecutionContext, GraphBatch, Limits, Result, RowBuffer,
};
use helix_planner::relational as r;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
};

impl ExecutionContext<'_> {
    pub(super) async fn top_k_batches<S: futures::Stream<Item = Result<Rows>>>(
        &self,
        batches: S,
        projection: Projection<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows> {
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
        let skip = skip
            .map(|e| evaluation.eval(e).and_then(|v| r::nonnegative(&v)))
            .transpose()?
            .unwrap_or(0);
        let limit = limit
            .map(|e| evaluation.eval(e).and_then(|v| r::nonnegative(&v)))
            .transpose()?
            .unwrap_or(usize::MAX);
        let mut heap = BinaryHeap::<RankedRow<'_>>::new();
        let keep = skip.saturating_add(limit);
        let mut heap_memory = self.row_budget().reserve(0)?;
        let mut retained_bytes = 0_usize;
        let mut ordinal = 0_usize;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            self.check_execution_deadline()?;
            let graph = self
                .expression_graph_batch(&batch, items.iter().map(|i| &i.expression))
                .await?;
            let mut projected = RowBuffer::new(self.row_budget())?;
            for row in batch.iter() {
                self.check_execution_deadline()?;
                let mut evaluation = self.evaluate(row, parameters, &graph, limits);
                let values = items.evaluate(&mut evaluation).await?;
                let mut row = row.clone();
                for (item, value) in items.iter().zip(values) {
                    row[item.slot.0 as usize] = value;
                }
                push_row(&mut projected, row, limits)?;
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
                let keys = ordering
                    .iter()
                    .map(|key| evaluation.eval(&key.expression))
                    .collect::<r::Result<Vec<_>>>()?;
                for (index, value) in row.iter_mut().enumerate() {
                    if !items.iter().any(|item| item.slot.0 as usize == index) {
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
                if keep == 0
                    || heap
                        .peek()
                        .is_some_and(|worst| heap.len() >= keep && &candidate >= worst)
                {
                    continue;
                }
                if heap.len() == keep {
                    let removed = heap.pop().expect("nonzero full heap");
                    retained_bytes = retained_bytes.saturating_sub(removed.allocated_bytes());
                }
                retained_bytes = retained_bytes.saturating_add(candidate.allocated_bytes());
                let capacity = if heap.len() == heap.capacity() {
                    heap.capacity().saturating_mul(2).max(4).min(keep)
                } else {
                    heap.capacity()
                };
                heap_memory.resize(
                    retained_bytes
                        .saturating_add(capacity.saturating_mul(size_of::<RankedRow<'_>>())),
                )?;
                heap.reserve_exact(capacity.saturating_sub(heap.len()));
                heap.push(candidate);
            }
        }
        Rows::new(
            heap.into_sorted_vec()
                .into_iter()
                .skip(skip)
                .map(|entry| entry.row)
                .collect(),
            self.row_budget(),
        )
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
        row_bytes(&self.row)
            .saturating_add(row_bytes(&self.keys))
            .saturating_add(size_of::<Self>())
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
