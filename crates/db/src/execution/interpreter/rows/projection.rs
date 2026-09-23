use super::memory::{Budget, Reservation, Rows};
use super::{
    check_memory, push_row, row_bytes, ExecutionContext, GraphBatch, Limits, Result, RowBuffer,
};
use helix_planner::relational as r;
use std::{cmp::Ordering, collections::BTreeMap};

pub(super) struct Projection<'a> {
    pub items: &'a r::ProjectionProgram,
    pub distinct: bool,
    pub ordering: &'a [r::Ordering],
    pub predicate: Option<&'a r::SelectionProgram>,
    pub skip: Option<&'a r::Expression>,
    pub limit: Option<&'a r::Expression>,
}

/// Incoming values needed after simultaneous projection, including ORDER BY
/// references that are not output columns. The mask stays admitted with its owner.
pub(super) enum ProjectionInputs {
    Discard,
    Keep {
        slots: Vec<bool>,
        _memory: super::memory::Reservation,
    },
}
impl ProjectionInputs {
    /// Add grouping dependencies to an admitted incoming-value mask. Allocate
    /// only when an expression actually references an input binding.
    pub(super) fn include_references<'e>(
        &mut self,
        ctx: &ExecutionContext<'_>,
        width: usize,
        expressions: impl Iterator<Item = &'e r::Expression>,
    ) -> Result<()> {
        for expression in expressions {
            ctx.check_execution_deadline()?;
            expression.try_visit(&mut |expression| {
                let (r::Expression::Slot(slot) | r::Expression::HasLabel(slot, _)) = expression
                else {
                    return Ok::<_, super::Error>(());
                };
                if matches!(self, Self::Discard) {
                    let memory = ctx
                        .row_budget()
                        .reserve(width.saturating_mul(size_of::<bool>()))?;
                    *self = Self::Keep {
                        slots: vec![false; width],
                        _memory: memory,
                    };
                }
                let Self::Keep { slots, .. } = self else {
                    unreachable!("referenced input has an admitted mask");
                };
                slots[slot.0 as usize] = true;
                Ok(())
            })?;
        }
        ctx.check_execution_deadline()?;
        Ok(())
    }
    pub(super) fn keeps(&self, slot: usize) -> bool {
        match self {
            Self::Discard => false,
            Self::Keep { slots, .. } => slots[slot],
        }
    }
}

impl Projection<'_> {
    pub(super) fn input_slots(
        &self,
        ctx: &ExecutionContext<'_>,
        width: usize,
    ) -> Result<ProjectionInputs> {
        ctx.check_execution_deadline()?;
        if self.ordering.is_empty() && self.predicate.is_none() {
            return Ok(ProjectionInputs::Discard);
        }
        let memory = ctx
            .row_budget()
            .reserve(width.saturating_mul(size_of::<bool>()))?;
        let mut slots = vec![false; width];
        for expression in self
            .ordering
            .iter()
            .map(|order| &order.expression)
            .chain(self.predicate.map(r::SelectionProgram::expression))
        {
            ctx.check_execution_deadline()?;
            expression.visit(&mut |expression| {
                let (r::Expression::Slot(slot) | r::Expression::HasLabel(slot, _)) = expression
                else {
                    return;
                };
                // Query validation proves every reference fits the row schema.
                slots[slot.0 as usize] = true;
            });
        }
        ctx.check_execution_deadline()?;
        Ok(ProjectionInputs::Keep {
            slots,
            _memory: memory,
        })
    }
}

/// Keys, copied rows, vector growth and stable-sort scratch share one bound.
/// Once sorting finishes, the scratch allowance covers the row headers used by
/// grouping or final output while the keyed allocation still exists.
struct KeyedRows {
    entries: Vec<KeyedRow>,
    memory: Reservation,
    bytes: usize,
}
struct KeyedRow {
    keys: Vec<r::Value>,
    row: r::Row,
}
impl KeyedRows {
    fn new(budget: &Budget) -> Result<Self> {
        Ok(Self {
            entries: Vec::new(),
            memory: budget.reserve(0)?,
            bytes: 0,
        })
    }
    fn push(&mut self, keys: Vec<r::Value>, row: &r::Row) -> Result<()> {
        let bytes = self
            .bytes
            .saturating_add(row_bytes(&keys).saturating_sub(size_of::<Vec<r::Value>>()))
            .saturating_add(row_bytes(row).saturating_sub(size_of::<r::Row>()))
            .saturating_add(size_of::<KeyedRow>().max(size_of::<r::Row>()));
        let capacity = if self.entries.len() == self.entries.capacity() {
            self.entries.capacity().saturating_mul(2).max(4)
        } else {
            self.entries.capacity()
        };
        self.memory
            .resize(bytes.saturating_add(capacity.saturating_mul(size_of::<KeyedRow>())))?;
        if capacity > self.entries.capacity() {
            self.entries
                .try_reserve_exact(capacity - self.entries.len())
                .map_err(|_| super::resource("MemoryLimit", "sort buffer allocation failed"))?;
        }
        self.entries.push(KeyedRow {
            keys,
            row: row.clone(),
        });
        self.bytes = bytes;
        Ok(())
    }
    fn into_rows(self) -> Rows {
        let count = self.entries.len();
        let mut rows = Vec::with_capacity(count);
        for entry in self.entries {
            assert!(rows.len() < count, "fixed sorted output capacity");
            rows.push(entry.row);
        }
        Rows::from_admitted(rows, self.memory)
    }
}

impl ExecutionContext<'_> {
    /// Project each batch before retaining output. Window counters are global;
    /// an unsafe upstream stop still drains and evaluates every remaining batch
    /// so a later expression error cannot disappear behind LIMIT.
    pub(super) async fn project_batches<S: futures::Stream<Item = Result<Rows>>>(
        &self,
        batches: S,
        width: usize,
        projection: Projection<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
        termination: r::Termination,
    ) -> Result<Rows> {
        use futures::StreamExt;
        futures::pin_mut!(batches);
        assert!(!projection.distinct && projection.ordering.is_empty());
        assert!(projection
            .items
            .iter()
            .all(|item| !item.expression.has_aggregate()));
        let empty = GraphBatch::default();
        let evaluation = self.evaluate(&[], parameters, &empty, limits);
        let window = r::Window::evaluate(projection.skip, projection.limit, |expression| {
            evaluation.eval(expression)
        })?;
        let mut skip = window.skip();
        let limit = window.limit();
        let mut output = RowBuffer::new(self.row_budget())?;
        let mut input_started = false;
        loop {
            if termination.may_stop(input_started) && output.len() >= limit {
                break;
            }
            let Some(batch) = batches.next().await else {
                break;
            };
            input_started = true;
            let projected = self
                .project_rows(
                    batch?,
                    width,
                    Projection {
                        items: projection.items,
                        predicate: projection.predicate,
                        distinct: false,
                        ordering: &[],
                        skip: None,
                        limit: None,
                    },
                    parameters,
                    limits,
                )
                .await?;
            for row in projected {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                if output.len() < limit {
                    push_row(&mut output, row, limits)?;
                }
            }
        }
        Ok(output.finish())
    }

    pub(super) async fn project_rows(
        &self,
        rows: Rows,
        width: usize,
        projection: Projection<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows> {
        let Projection {
            items,
            distinct,
            ordering,
            predicate,
            skip,
            limit,
        } = projection;
        let empty = GraphBatch::default();
        let evaluation = self.evaluate(&[], parameters, &empty, limits);
        let window = r::Window::evaluate(skip, limit, |expression| evaluation.eval(expression))?;
        let skip = window.skip();
        let limit = window.limit();
        let aggregated = items.iter().any(|item| item.expression.has_aggregate());
        if !aggregated && !distinct && !ordering.is_empty() && limit != usize::MAX {
            // The parent Rows owner retains admission while these slices are
            // consumed. Switching to a batch interface needs no payload copy.
            let batches = futures::stream::iter(rows.batches(limits.batch_rows).map(Ok));
            return self
                .top_k_batches(batches, width, projection, parameters, limits)
                .await;
        }

        let mut projected = RowBuffer::new(self.row_budget())?;
        if aggregated
            && items.iter().all(|item| {
                !item.expression.has_aggregate()
                    || matches!(item.expression, r::Expression::Aggregate { .. })
            })
        {
            projected = self
                .aggregate_rows(&rows, width, projection, parameters, limits)
                .await?;
            drop(rows);
        } else if aggregated {
            let inputs = projection.input_slots(self, width)?;
            let key_count = items
                .iter()
                .filter(|item| !item.expression.has_aggregate())
                .count();
            let _key_slots = self
                .row_budget()
                .reserve(key_count.saturating_mul(size_of::<&r::Expression>()))?;
            let mut keys = Vec::with_capacity(key_count);
            keys.extend(
                items
                    .iter()
                    .filter(|item| !item.expression.has_aggregate())
                    .map(|item| &item.expression),
            );
            let mut keyed = KeyedRows::new(self.row_budget())?;
            for batch in rows.chunks(limits.batch_rows) {
                let graph = self
                    .expression_graph_batch(batch, keys.iter().copied())
                    .await?;
                for row in batch {
                    let key = self
                        .evaluate(row, parameters, &graph, limits)
                        .eval_sequence(keys.iter().copied())?;
                    keyed.push(key, row)?;
                }
            }
            drop(rows);
            keyed.entries.sort_by(|a, b| compare(&a.keys, &b.keys));
            let empty_global = keyed.entries.is_empty() && keys.is_empty();
            let mut empty: [KeyedRow; 0] = [];
            for entries in keyed
                .entries
                .chunk_by_mut(|a, b| compare(&a.keys, &b.keys).is_eq())
                .chain(empty_global.then_some(empty.as_mut_slice()))
            {
                // KeyedRows' post-sort workspace covers these exact headers.
                // Its reservation continues owning payload moved into this group.
                let mut group = Vec::with_capacity(entries.len());
                for entry in entries {
                    entry.keys = Vec::new();
                    group.push(std::mem::take(&mut entry.row));
                }
                let graph = self
                    .expression_graph_batch(&group, items.iter().map(|i| &i.expression))
                    .await?;
                let _base_memory = group
                    .is_empty()
                    .then(|| {
                        self.row_budget().reserve(
                            size_of::<r::Row>()
                                .saturating_add(width.saturating_mul(size_of::<r::Value>())),
                        )
                    })
                    .transpose()?;
                let empty_base = group.is_empty().then(|| vec![r::Value::Null; width]);
                let base = group
                    .first()
                    .or(empty_base.as_ref())
                    .expect("group representative or admitted null row");
                let mut evaluation = r::Evaluation {
                    group: Some(&group),
                    ..self.evaluate(base, parameters, &graph, limits)
                };
                let values = items.evaluate(&mut evaluation).await?;
                projected.push_projection(base, items, values, &inputs, self.row_budget())?;
            }
        } else {
            let inputs = projection.input_slots(self, width)?;
            for batch in rows.chunks(limits.batch_rows) {
                let graph = self
                    .expression_graph_batch(batch, items.iter().map(|i| &i.expression))
                    .await?;
                for row in batch {
                    self.check_execution_deadline()?;
                    let mut evaluation = self.evaluate(row, parameters, &graph, limits);
                    let values = items.evaluate(&mut evaluation).await?;
                    projected.push_projection(row, items, values, &inputs, self.row_budget())?;
                }
            }
            drop(rows);
        }
        let mut projected = projected.finish();
        if let Some(predicate) = predicate {
            projected = self
                .row_budget()
                .admitted_future(self.filter_relation(projected, predicate, parameters, limits))?
                .await?;
        }
        if distinct {
            let scratch = self
                .row_budget()
                .reserve(projected.len().saturating_mul(size_of::<r::Row>()))?;
            projected.sort_by(|a, b| {
                items
                    .iter()
                    .map(|item| a[item.slot.0 as usize].total_cmp(&b[item.slot.0 as usize]))
                    .find(|o| !o.is_eq())
                    .unwrap_or(Ordering::Equal)
            });
            drop(scratch);
            projected.dedup_by(|a, b| {
                items.iter().all(|item| {
                    a[item.slot.0 as usize]
                        .total_cmp(&b[item.slot.0 as usize])
                        .is_eq()
                })
            });
            projected.refresh()?;
        }
        if !ordering.is_empty() {
            let mut keyed = KeyedRows::new(self.row_budget())?;
            for batch in projected.chunks(limits.batch_rows) {
                let graph = self
                    .expression_graph_batch(batch, ordering.iter().map(|o| &o.expression))
                    .await?;
                for row in batch {
                    let keys = self
                        .evaluate(row, parameters, &graph, limits)
                        .eval_sequence(ordering.iter().map(|key| &key.expression))?;
                    keyed.push(keys, row)?;
                }
            }
            drop(projected);
            let order = |a: &KeyedRow, b: &KeyedRow| {
                let (a, b) = (&a.keys, &b.keys);
                ordering
                    .iter()
                    .zip(a.iter().zip(b))
                    .map(|(order, (a, b))| {
                        let cmp = a.total_cmp(b);
                        if order.descending {
                            cmp.reverse()
                        } else {
                            cmp
                        }
                    })
                    .find(|o| !o.is_eq())
                    .unwrap_or(Ordering::Equal)
            };
            let keep = skip.saturating_add(limit);
            if keep < keyed.entries.len() {
                keyed.entries.select_nth_unstable_by(keep, order);
                keyed.entries.truncate(keep);
            }
            keyed.entries.sort_by(order);
            projected = keyed.into_rows();
        }
        check_memory(&projected, limits)?;
        // Window the admitted relation in place, including the empty window.
        // Its vector capacity remains accounted without another header buffer.
        projected.truncate(skip.saturating_add(limit));
        let discarded = skip.min(projected.len());
        drop(projected.drain(..discarded));
        // Projection is the scope boundary: release unreachable values/paths.
        for row in &mut projected {
            for (index, value) in row.iter_mut().enumerate() {
                if !items.outputs().contains(&r::Slot(index as u32)) {
                    *value = r::Value::Null;
                }
            }
        }
        projected.refresh()?;
        Ok(projected)
    }
}

fn compare(a: &[r::Value], b: &[r::Value]) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(a, b)| a.total_cmp(b))
        .find(|o| !o.is_eq())
        .unwrap_or_else(|| a.len().cmp(&b.len()))
}
