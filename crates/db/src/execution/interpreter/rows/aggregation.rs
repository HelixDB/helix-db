//! Direct aggregation retains admitted representatives and accumulator state.
//! Source batches are borrowed or moved with their original reservation.
use super::{memory, projection, row_bytes, ExecutionContext, Limits, Result, RowBuffer};
use helix_planner::relational as r;
use std::collections::BTreeMap;

mod groups;
type Specification<'a> = (r::Aggregate, Option<&'a r::Expression>, bool);
struct Group {
    base: r::Row,
    accumulators: Vec<r::Accumulator>,
}

impl ExecutionContext<'_> {
    pub(super) async fn aggregate_rows(
        &self,
        rows: &memory::Rows,
        width: usize,
        projection: projection::Projection<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<RowBuffer> {
        let batches = futures::stream::iter(rows.batches(limits.batch_rows).map(Ok));
        self.aggregate_batches(batches, width, projection, parameters, limits)
            .await
    }

    pub(super) async fn aggregate_batches<'rows, S>(
        &self,
        batches: S,
        width: usize,
        projection: projection::Projection<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<RowBuffer>
    where
        S: futures::Stream<Item = Result<memory::Batch<'rows>>>,
    {
        use futures::StreamExt;
        self.check_execution_deadline()?;
        futures::pin_mut!(batches);
        let items = projection.items;
        assert!(
            items.iter().all(|item| !item.expression.has_aggregate()
                || matches!(item.expression, r::Expression::Aggregate { .. })),
            "direct aggregate expressions"
        );
        let key_count = items
            .iter()
            .filter(|item| !item.expression.has_aggregate())
            .count();
        let state_count = items.len() - key_count;
        assert!(state_count > 0, "direct projection contains an aggregate");
        let _metadata = self.row_budget().reserve(
            key_count
                .saturating_mul(size_of::<&r::Expression>())
                .saturating_add(state_count.saturating_mul(size_of::<Specification<'_>>())),
        )?;
        let mut keys = Vec::with_capacity(key_count);
        keys.extend(
            items
                .iter()
                .filter(|item| !item.expression.has_aggregate())
                .map(|item| &item.expression),
        );
        let mut specifications = Vec::with_capacity(state_count);
        specifications.extend(items.iter().filter_map(|item| {
            let r::Expression::Aggregate {
                function,
                argument,
                distinct,
            } = &item.expression
            else {
                return None;
            };
            Some((*function, argument.as_deref(), *distinct))
        }));
        let mut inputs = projection.input_slots(self, width)?;
        // Later aliases read the completed output; their incoming values need
        // retention only when a grouping expression independently reads them.
        match &mut inputs {
            projection::ProjectionInputs::Discard => {}
            projection::ProjectionInputs::Keep { slots, .. } => {
                for slot in items.outputs() {
                    slots[slot.0 as usize] = false;
                }
            }
        }
        inputs.include_references(self, width, keys.iter().copied())?;
        let mut groups = groups::GroupBuffer::new(self.row_budget())?;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            let batch = batch.as_ref();
            self.check_execution_deadline()?;
            let graph = self
                .row_budget()
                .admitted_future(
                    self.expression_graph_batch(
                        batch,
                        keys.iter().copied().chain(
                            specifications
                                .iter()
                                .filter_map(|(_, argument, _)| *argument),
                        ),
                    ),
                )?
                .await?;
            for row in batch {
                self.check_execution_deadline()?;
                let key = r::GroupingKey::row(
                    self.evaluate(row, parameters, &graph, limits)
                        .eval_sequence(keys.iter().copied())?,
                )?;
                let key_memory = self.row_budget().reserve(key.value().allocated_bytes())?;
                let index = match groups.index(&key) {
                    Some(index) => {
                        drop(key);
                        drop(key_memory);
                        index
                    }
                    None => groups.insert(
                        key,
                        key_memory,
                        Some(row),
                        width,
                        &inputs,
                        &specifications,
                    )?,
                };
                for (aggregate, (_, argument, _)) in specifications.iter().enumerate() {
                    let value = match argument {
                        Some(argument) => self
                            .evaluate(row, parameters, &graph, limits)
                            .eval(argument)?,
                        None => r::Value::Integer(1),
                    };
                    groups.push(index, aggregate, value, self.row_budget(), limits)?;
                }
            }
        }
        if groups.is_empty() && keys.is_empty() {
            let key = r::GroupingKey::row(Vec::new())?;
            let memory = self.row_budget().reserve(key.value().allocated_bytes())?;
            groups.insert(key, memory, None, width, &inputs, &specifications)?;
        }
        // The lookup table is no longer needed. Its keys and table allocation
        // are released before materializing any result values.
        let mut groups = groups.into_drain();
        let mut output = RowBuffer::new(self.row_budget())?;
        for Group {
            mut base,
            accumulators,
        } in groups.groups.by_ref()
        {
            self.check_execution_deadline()?;
            let group_bytes = accumulators.iter().fold(row_bytes(&base), |bytes, state| {
                bytes.saturating_add(state.allocated_bytes())
            });
            let graph = self
                .row_budget()
                .admitted_future(
                    self.expression_graph_batch(std::slice::from_ref(&base), keys.iter().copied()),
                )?
                .await?;
            let values_memory = self
                .row_budget()
                .reserve(items.len().saturating_mul(size_of::<r::Value>()))?;
            let mut values = Vec::with_capacity(items.len());
            let mut results = accumulators.into_iter();
            let mut additional = 0_usize;
            for item in items {
                let value = if matches!(item.expression, r::Expression::Aggregate { .. }) {
                    results
                        .next()
                        .expect("one state per direct aggregate")
                        .finish()?
                } else {
                    let value = self
                        .evaluate(&base, parameters, &graph, limits)
                        .eval(&item.expression)?;
                    let bytes = value.allocated_bytes();
                    groups.memory.absorb(self.row_budget().reserve(bytes)?);
                    additional = additional.saturating_add(bytes);
                    value
                };
                assert!(
                    values.len() < items.len(),
                    "fixed aggregate output capacity"
                );
                values.push(value);
            }
            assert_eq!(results.len(), 0, "all accumulator states were finalized");
            drop(results);
            for (item, value) in items.iter().zip(values) {
                base[item.slot.0 as usize] = value;
            }
            drop(graph);
            drop(values_memory);
            let retained = row_bytes(&base);
            let surplus = group_bytes
                .saturating_add(additional)
                .checked_sub(retained)
                .expect("aggregate result fits its admitted state and values");
            groups.memory.release(surplus);
            output.push_admitted(base, groups.memory.split(retained))?;
        }
        Ok(output)
    }
}
