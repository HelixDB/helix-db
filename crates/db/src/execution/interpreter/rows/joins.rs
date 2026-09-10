//! Equality join state is admitted once and reused across correlated outer rows.
//! Keys use total hashing; null/NaN keys cannot satisfy predicate equality.
use super::{memory, push_row, ExecutionContext, Limits, Result, RowBuffer};
use helix_planner::relational as r;
use r::GraphValues;
use std::collections::{BTreeMap, HashMap};

pub(super) struct HashJoinTable {
    buckets: HashMap<r::GroupingKey, Vec<u64>>,
    _memory: memory::Reservation,
}
impl HashJoinTable {
    pub async fn build(
        context: &ExecutionContext<'_>,
        ids: &[u64],
        width: usize,
        slot: r::Slot,
        property: &str,
        limits: Limits,
    ) -> Result<Self> {
        let mut buckets: HashMap<r::GroupingKey, Vec<u64>> = HashMap::new();
        let mut memory = context.row_budget().reserve(0)?;
        let mut bytes = 0_usize;
        let demand = BTreeMap::from([(
            slot,
            r::PropertyDemand::Keys([property.to_owned()].into_iter().collect()),
        )]);
        for batch in ids.chunks(limits.batch_rows) {
            context.check_execution_deadline()?;
            let mut rows = RowBuffer::new(context.row_budget())?;
            for id in batch {
                let mut row = vec![r::Value::Null; width];
                row[slot.0 as usize] = r::Value::Entity(r::Entity::Node(*id));
                push_row(&mut rows, row, limits)?;
            }
            let rows = rows.finish();
            let graph = context.graph_batch_required(&rows, &demand).await?;
            for id in batch {
                let value = graph.property(r::Entity::Node(*id), property)?;
                if value.equals(value) != Some(true) {
                    continue;
                }
                // Per-entry admission includes hash control bytes, spare bucket
                // capacity and duplicate-ID vector growth, before allocation.
                bytes = bytes
                    .saturating_add(value.allocated_bytes())
                    .saturating_add(160);
                memory.resize(bytes)?;
                buckets
                    .try_reserve(1)
                    .map_err(|_| super::resource("MemoryLimit", "join allocation failed"))?;
                buckets
                    .entry(r::GroupingKey::new(value.clone())?)
                    .or_default()
                    .push(*id);
            }
        }
        Ok(Self {
            buckets,
            _memory: memory,
        })
    }

    pub async fn probe(
        &self,
        context: &ExecutionContext<'_>,
        rows: memory::Rows,
        slot: r::Slot,
        probe: r::Slot,
        property: &str,
        limits: Limits,
    ) -> Result<memory::Rows> {
        let demand = BTreeMap::from([(
            probe,
            r::PropertyDemand::Keys([property.to_owned()].into_iter().collect()),
        )]);
        let mut output = RowBuffer::new(context.row_budget())?;
        for batch in rows.chunks(limits.batch_rows) {
            context.check_execution_deadline()?;
            let graph = context.graph_batch_required(batch, &demand).await?;
            for row in batch {
                let r::Value::Entity(entity) = row[probe.0 as usize] else {
                    continue;
                };
                let value = graph.property(entity, property)?;
                if value.equals(value) != Some(true) {
                    continue;
                }
                let _probe_memory = context.row_budget().reserve(value.allocated_bytes())?;
                let Some(ids) = self.buckets.get(&r::GroupingKey::new(value.clone())?) else {
                    continue;
                };
                for id in ids {
                    if output.len().is_multiple_of(limits.batch_rows) {
                        context.check_execution_deadline()?;
                    }
                    let mut row = row.clone();
                    row[slot.0 as usize] = r::Value::Entity(r::Entity::Node(*id));
                    push_row(&mut output, row, limits)?;
                }
            }
        }
        Ok(output.finish())
    }
}
