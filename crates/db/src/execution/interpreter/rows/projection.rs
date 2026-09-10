use super::memory::Rows;
use super::{
    check_memory, push_row, row_bytes, ExecutionContext, GraphBatch, Limits, Result, RowBuffer,
};
use helix_planner::relational as r;
use std::{cmp::Ordering, collections::BTreeMap};

pub(super) struct Projection<'a> {
    pub items: &'a r::ProjectionProgram,
    pub distinct: bool,
    pub ordering: &'a [r::Ordering],
    pub predicate: Option<&'a r::Expression>,
    pub skip: Option<&'a r::Expression>,
    pub limit: Option<&'a r::Expression>,
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
        let mut skip = projection
            .skip
            .map(|expression| {
                evaluation
                    .eval(expression)
                    .and_then(|value| r::nonnegative(&value))
            })
            .transpose()?
            .unwrap_or(0);
        let limit = projection
            .limit
            .map(|expression| {
                evaluation
                    .eval(expression)
                    .and_then(|value| r::nonnegative(&value))
            })
            .transpose()?
            .unwrap_or(usize::MAX);
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
        let skip = skip
            .map(|e| evaluation.eval(e).and_then(|v| r::nonnegative(&v)))
            .transpose()?
            .unwrap_or(0);
        let limit = limit
            .map(|e| evaluation.eval(e).and_then(|v| r::nonnegative(&v)))
            .transpose()?
            .unwrap_or(usize::MAX);
        let aggregated = items.iter().any(|item| item.expression.has_aggregate());
        if !aggregated && !distinct && !ordering.is_empty() && limit != usize::MAX {
            let batches = futures::stream::iter(
                rows.chunks(limits.batch_rows)
                    .map(|batch| Rows::new(batch.to_vec(), self.row_budget())),
            );
            return self
                .top_k_batches(batches, projection, parameters, limits)
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
                .aggregate_rows(&rows, width, items, parameters, limits)
                .await?;
        } else if aggregated {
            let keys = items
                .iter()
                .filter(|item| !item.expression.has_aggregate())
                .map(|item| &item.expression)
                .collect::<Vec<_>>();
            let mut keyed = Vec::new();
            let mut keyed_bytes = 0_usize;
            let mut keyed_memory = self.row_budget().reserve(0)?;
            for batch in rows.chunks(limits.batch_rows) {
                let graph = self
                    .expression_graph_batch(batch, keys.iter().copied())
                    .await?;
                for row in batch {
                    let key = keys
                        .iter()
                        .map(|e| self.evaluate(row, parameters, &graph, limits).eval(e))
                        .collect::<r::Result<Vec<_>>>()?;
                    keyed_bytes = keyed_bytes
                        .saturating_add(row_bytes(&key))
                        .saturating_add(row_bytes(row))
                        .saturating_add(2 * size_of::<(Vec<r::Value>, r::Row)>());
                    keyed_memory.resize(keyed_bytes)?;
                    keyed.push((key, row.clone()));
                }
            }
            keyed.sort_by(|(a, _), (b, _)| compare(a, b));
            let mut groups: Vec<Vec<r::Row>> = Vec::new();
            let mut previous: Option<Vec<r::Value>> = None;
            for (key, row) in keyed {
                if previous
                    .as_ref()
                    .is_none_or(|old| compare(old, &key) != Ordering::Equal)
                {
                    groups.push(Vec::new());
                    previous = Some(key);
                }
                groups.last_mut().expect("group was added").push(row);
            }
            if groups.is_empty() && keys.is_empty() {
                groups.push(Vec::new());
            }
            for group in groups {
                let graph = self
                    .expression_graph_batch(&group, items.iter().map(|i| &i.expression))
                    .await?;
                let base = group
                    .first()
                    .cloned()
                    .unwrap_or_else(|| vec![r::Value::Null; width]);
                let mut evaluation = r::Evaluation {
                    group: Some(&group),
                    ..self.evaluate(&base, parameters, &graph, limits)
                };
                let values = items.evaluate(&mut evaluation).await?;
                let mut row = base;
                for (item, value) in items.iter().zip(values) {
                    row[item.slot.0 as usize] = value;
                }
                push_row(&mut projected, row, limits)?;
            }
        } else {
            for batch in rows.chunks(limits.batch_rows) {
                let graph = self
                    .expression_graph_batch(batch, items.iter().map(|i| &i.expression))
                    .await?;
                for row in batch {
                    self.check_execution_deadline()?;
                    let mut evaluation = self.evaluate(row, parameters, &graph, limits);
                    let values = items.evaluate(&mut evaluation).await?;
                    let mut row = row.clone();
                    for (item, value) in items.iter().zip(values) {
                        row[item.slot.0 as usize] = value;
                    }
                    push_row(&mut projected, row, limits)?;
                }
            }
        }
        let mut projected = projected.finish();
        if let Some(predicate) = predicate {
            let mut filtered = RowBuffer::new(self.row_budget())?;
            for batch in projected.chunks(limits.batch_rows) {
                let graph = self.expression_graph_batch(batch, [predicate]).await?;
                for row in batch {
                    if self
                        .evaluate(row, parameters, &graph, limits)
                        .eval(predicate)?
                        .truth()?
                        == Some(true)
                    {
                        push_row(&mut filtered, row.clone(), limits)?;
                    }
                }
            }
            projected = filtered.finish();
        }
        if distinct {
            projected.sort_by(|a, b| {
                items
                    .iter()
                    .map(|item| a[item.slot.0 as usize].total_cmp(&b[item.slot.0 as usize]))
                    .find(|o| !o.is_eq())
                    .unwrap_or(Ordering::Equal)
            });
            projected.dedup_by(|a, b| {
                items.iter().all(|item| {
                    a[item.slot.0 as usize]
                        .total_cmp(&b[item.slot.0 as usize])
                        .is_eq()
                })
            });
        }
        if !ordering.is_empty() {
            let mut keyed = Vec::new();
            let mut keyed_bytes = 0_usize;
            let mut keyed_memory = self.row_budget().reserve(0)?;
            for batch in projected.chunks(limits.batch_rows) {
                let graph = self
                    .expression_graph_batch(batch, ordering.iter().map(|o| &o.expression))
                    .await?;
                for row in batch {
                    let keys = ordering
                        .iter()
                        .map(|key| {
                            self.evaluate(row, parameters, &graph, limits)
                                .eval(&key.expression)
                        })
                        .collect::<r::Result<Vec<_>>>()?;
                    keyed_bytes = keyed_bytes
                        .saturating_add(row_bytes(&keys))
                        .saturating_add(row_bytes(row))
                        .saturating_add(2 * size_of::<(Vec<r::Value>, r::Row)>());
                    keyed_memory.resize(keyed_bytes)?;
                    keyed.push((keys, row.clone()));
                }
            }
            let order = |(a, _): &(Vec<r::Value>, r::Row), (b, _): &(Vec<r::Value>, r::Row)| {
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
            if keep < keyed.len() {
                keyed.select_nth_unstable_by(keep, order);
                keyed.truncate(keep);
            }
            keyed.sort_by(order);
            projected = Rows::new(
                keyed.into_iter().map(|(_, row)| row).collect(),
                self.row_budget(),
            )?;
        }
        check_memory(&projected, limits)?;
        let mut output = projected
            .into_iter()
            .skip(skip)
            .take(limit)
            .collect::<Vec<_>>();
        // Projection is the scope boundary: release unreachable values/paths.
        for row in &mut output {
            for (index, value) in row.iter_mut().enumerate() {
                if !items.iter().any(|item| item.slot.0 as usize == index) {
                    *value = r::Value::Null;
                }
            }
        }
        Rows::new(output, self.row_budget())
    }
}

fn compare(a: &[r::Value], b: &[r::Value]) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(a, b)| a.total_cmp(b))
        .find(|o| !o.is_eq())
        .unwrap_or_else(|| a.len().cmp(&b.len()))
}
