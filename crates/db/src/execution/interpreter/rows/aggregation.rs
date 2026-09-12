//! Incremental direct aggregates retain one base row and accumulator set per
//! group. Input rows are never copied into per-group relations.
use super::{memory, push_row, row_bytes, ExecutionContext, Limits, Result, RowBuffer};
use helix_planner::relational as r;
use std::collections::{BTreeMap, HashMap};

struct Group {
    base: r::Row,
    accumulators: Vec<r::Accumulator>,
}

impl ExecutionContext<'_> {
    pub(super) async fn aggregate_rows(
        &self,
        rows: &memory::Rows,
        width: usize,
        items: &[r::Projection],
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<RowBuffer> {
        let batches = futures::stream::iter(
            rows.chunks(limits.batch_rows)
                .map(|batch| memory::Rows::new(batch.to_vec(), self.row_budget())),
        );
        self.aggregate_batches(batches, width, items, parameters, limits)
            .await
    }

    pub(super) async fn aggregate_batches<S: futures::Stream<Item = Result<memory::Rows>>>(
        &self,
        batches: S,
        width: usize,
        items: &[r::Projection],
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<RowBuffer> {
        use futures::StreamExt;
        futures::pin_mut!(batches);
        let keys = items
            .iter()
            .filter(|item| !item.expression.has_aggregate())
            .map(|item| &item.expression)
            .collect::<Vec<_>>();
        let specifications = items
            .iter()
            .filter_map(|item| {
                let r::Expression::Aggregate {
                    function,
                    argument,
                    distinct,
                } = &item.expression
                else {
                    return None;
                };
                Some((*function, argument.as_deref(), *distinct))
            })
            .collect::<Vec<_>>();
        let mut by_key = HashMap::new();
        let mut groups = Vec::<Group>::new();
        let mut bytes = 0_usize;
        let mut memory = self.row_budget().reserve(0)?;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            self.check_execution_deadline()?;
            let graph = self
                .row_budget()
                .admitted_future(
                    self.expression_graph_batch(
                        &batch,
                        keys.iter().copied().chain(
                            specifications
                                .iter()
                                .filter_map(|(_, argument, _)| *argument),
                        ),
                    ),
                )?
                .await?;
            for row in batch.iter() {
                let evaluation = self.evaluate(row, parameters, &graph, limits);
                let key = r::GroupingKey::new(r::Value::List(
                    keys.iter()
                        .map(|key| evaluation.eval(key))
                        .collect::<r::Result<_>>()?,
                ))?;
                let index = match by_key.get(&key).copied() {
                    Some(index) => index,
                    None => {
                        let accumulators = specifications
                            .iter()
                            .map(|(function, _, distinct)| {
                                r::Accumulator::new(*function, *distinct)
                            })
                            .collect::<Vec<_>>();
                        bytes = bytes
                            .saturating_add(row_bytes(row))
                            .saturating_add(key.value().allocated_bytes())
                            .saturating_add(256)
                            .saturating_add(
                                accumulators
                                    .iter()
                                    .map(r::Accumulator::allocated_bytes)
                                    .fold(0_usize, usize::saturating_add),
                            );
                        memory.resize(bytes)?;
                        let index = groups.len();
                        by_key.insert(key, index);
                        groups.push(Group {
                            base: row.clone(),
                            accumulators,
                        });
                        index
                    }
                };
                for (accumulator, (_, argument, _)) in
                    groups[index].accumulators.iter_mut().zip(&specifications)
                {
                    let value = match argument {
                        Some(argument) => self
                            .evaluate(row, parameters, &graph, limits)
                            .eval(argument)?,
                        None => r::Value::Integer(1),
                    };
                    let before = accumulator.allocated_bytes();
                    accumulator.push(
                        value,
                        limits.collection_items,
                        self.row_budget().available().saturating_add(before),
                    )?;
                    bytes = bytes
                        .saturating_sub(before)
                        .saturating_add(accumulator.allocated_bytes());
                    memory.resize(bytes)?;
                }
            }
        }
        if groups.is_empty() && keys.is_empty() {
            let accumulators = specifications
                .iter()
                .map(|(function, _, distinct)| r::Accumulator::new(*function, *distinct))
                .collect::<Vec<_>>();
            let base = vec![r::Value::Null; width];
            bytes = bytes.saturating_add(row_bytes(&base)).saturating_add(
                accumulators
                    .iter()
                    .map(r::Accumulator::allocated_bytes)
                    .fold(0_usize, usize::saturating_add),
            );
            memory.resize(bytes)?;
            groups.push(Group { base, accumulators });
        }
        let mut output = RowBuffer::new(self.row_budget())?;
        for Group {
            mut base,
            accumulators,
        } in groups
        {
            let graph = self
                .row_budget()
                .admitted_future(
                    self.expression_graph_batch(std::slice::from_ref(&base), keys.iter().copied()),
                )?
                .await?;
            let evaluation = self.evaluate(&base, parameters, &graph, limits);
            let mut results = accumulators.into_iter();
            let values = items
                .iter()
                .map(|item| {
                    let r::Expression::Aggregate { .. } = &item.expression else {
                        return evaluation.eval(&item.expression);
                    };
                    results
                        .next()
                        .expect("one state per direct aggregate")
                        .finish()
                })
                .collect::<r::Result<Vec<_>>>()?;
            for (item, value) in items.iter().zip(values) {
                base[item.slot.0 as usize] = value;
            }
            push_row(&mut output, base, limits)?;
        }
        Ok(output)
    }
}
