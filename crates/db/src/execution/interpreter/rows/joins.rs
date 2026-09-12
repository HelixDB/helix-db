//! Equality join state is admitted once and reused across correlated outer rows.
//! Keys use total hashing; null/NaN keys cannot satisfy predicate equality.
use super::{memory, ExecutionContext, Limits, Result, RowBuffer};
use futures::{Stream, StreamExt};
use helix_planner::relational as r;
use r::GraphValues;
use std::collections::{BTreeMap, HashMap};

pub(super) struct HashJoinTable {
    buckets: HashMap<r::GroupingKey, Vec<u64>>,
    _memory: memory::Reservation,
}
impl HashJoinTable {
    pub(super) fn bucket(&self, key: &r::GroupingKey) -> Option<&[u64]> {
        self.buckets.get(key).map(Vec::as_slice)
    }

    pub async fn build(
        context: &ExecutionContext<'_>,
        ids: &crate::query_resources::bitmap::Bitmap,
        width: usize,
        slot: r::Slot,
        property: &str,
        limits: Limits,
    ) -> Result<Self> {
        let batches = futures::stream::try_unfold(ids.iter(), move |mut ids| async move {
            context.check_execution_deadline()?;
            let mut rows = RowBuffer::new(context.row_budget())?;
            for id in ids.by_ref().take(limits.batch_rows) {
                rows.push_with(
                    size_of::<r::Row>().saturating_add(width.saturating_mul(size_of::<r::Value>())),
                    || {
                        let mut row = vec![r::Value::Null; width];
                        row[slot.0 as usize] = r::Value::Entity(r::Entity::Node(id));
                        row
                    },
                )?;
            }
            Ok((rows.len() > 0).then(|| (rows.finish(), ids)))
        });
        Self::build_batches(context, batches, slot, property).await
    }

    /// The source emits bounded rows through the selected access primitive.
    /// Retained state is proportional to build IDs and distinct key payloads,
    /// independently of the join's result multiplicity.
    pub(super) async fn build_batches<S>(
        context: &ExecutionContext<'_>,
        batches: S,
        slot: r::Slot,
        property: &str,
    ) -> Result<Self>
    where
        S: Stream<Item = Result<memory::Rows>>,
    {
        let mut buckets: HashMap<r::GroupingKey, Vec<u64>> = HashMap::new();
        let fixed = size_of::<Self>().saturating_add(2 * size_of::<usize>());
        let mut memory = context.row_budget().reserve(fixed)?;
        let _source_memory = context.row_budget().reserve(size_of::<S>())?;
        let _demand_memory = context.row_budget().reserve(
            r::allocation::btree_bytes::<r::Slot, r::PropertyDemand>(1)
                .saturating_add(r::allocation::btree_bytes::<String, ()>(1))
                .saturating_add(property.len()),
        )?;
        let demand = BTreeMap::from([(
            slot,
            r::PropertyDemand::Keys([property.to_owned()].into_iter().collect()),
        )]);
        let mut payload = 0_usize;
        let mut ids = 0_usize;
        futures::pin_mut!(batches);
        while let Some(rows) = batches.next().await {
            context.check_execution_deadline()?;
            let rows = rows?;
            let graph = context.graph_batch_required(&rows, &demand).await?;
            for row in &rows {
                let r::Value::Entity(r::Entity::Node(id)) = row[slot.0 as usize] else {
                    unreachable!("join hydration rows contain source node IDs");
                };
                let value = graph.property(r::Entity::Node(id), property)?;
                if value.equals(value) != Some(true) {
                    continue;
                }
                let _key_memory = context.row_budget().reserve(value.allocated_bytes())?;
                let key = r::GroupingKey::new(value.clone())?;
                let new_key = !buckets.contains_key(&key);
                payload = payload.saturating_add(if new_key { value.allocated_bytes() } else { 0 });
                ids = ids.saturating_add(1);
                // The old table exists alongside the destination only during
                // rehashing. Keep its growth allowance out of subsequent graph
                // hydration and probe batches. Duplicate keys never grow it.
                let old_table = if new_key && buckets.len() == buckets.capacity() {
                    r::allocation::hash_table_retained_bytes::<r::GroupingKey, Vec<u64>>(
                        buckets.len(),
                    )
                } else {
                    0
                };
                let retained = fixed
                    .saturating_add(payload)
                    .saturating_add(r::allocation::hash_table_retained_bytes::<
                        r::GroupingKey,
                        Vec<u64>,
                    >(
                        buckets.len().saturating_add(usize::from(new_key))
                    ))
                    // Cover each duplicate vector's minimum capacity and
                    // overlapping old/new buffers during its own growth.
                    .saturating_add(ids.saturating_mul(4 * size_of::<u64>()));
                memory.resize(retained.saturating_add(old_table))?;
                buckets
                    .try_reserve(usize::from(new_key))
                    .map_err(|_| super::resource("MemoryLimit", "join allocation failed"))?;
                let bucket = buckets.entry(key).or_default();
                bucket
                    .try_reserve(1)
                    .map_err(|_| super::resource("MemoryLimit", "join bucket allocation failed"))?;
                bucket.push(id);
                memory.resize(retained)?;
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
        let mut cursor = super::hash_probe::ProbeCursor::new(rows);
        let mut output = RowBuffer::new(context.row_budget())?;
        while let Some(rows) = context
            .row_budget()
            .admitted_future(cursor.next_batch(self, context, slot, probe, property, limits))?
            .await?
        {
            for row in rows {
                output.push_with(super::row_bytes(&row), || row)?;
            }
        }
        Ok(output.finish())
    }
}
