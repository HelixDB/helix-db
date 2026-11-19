//! Equality join state is admitted once and reused across correlated outer rows.
//! Keys use total hashing; null/NaN keys cannot satisfy predicate equality.
use super::{memory, ExecutionContext, Limits, Result, RowBuffer};
use futures::{Stream, StreamExt};
use helix_planner::relational as r;
use helix_planner::relational::detail;
use r::GraphValues;
use std::collections::{BTreeMap, HashMap};

pub(super) struct HashJoinTable {
    buckets: HashMap<r::GroupingKey, Vec<u64>>,
    /// Whether any key is a datetime, and whether any is text: a probe of the
    /// other kind fails as the nested-loop comparison of the pair would,
    /// rather than silently finding no bucket.
    datetime_keys: bool,
    text_keys: bool,
    /// The same for list and map keys, at any depth. A list or map probe
    /// holding the other kind is compared with each list and map key, so it
    /// fails exactly where the nested loop's comparison of some pair would.
    nested_datetimes: bool,
    nested_text: bool,
    /// List and map keys holding null, which no probe equals, so they have no
    /// bucket, but which hold a datetime or text a probe may still compare
    /// with before the null.
    unmatched: Vec<r::Value>,
    _memory: memory::Reservation,
}

/// Whether `value` holds a datetime, and whether it holds text, at any depth.
fn kinds(value: &r::Value) -> (bool, bool) {
    let any = |values: &mut dyn Iterator<Item = &r::Value>| {
        values
            .map(kinds)
            .fold((false, false), |(datetime, text), (d, t)| {
                (datetime || d, text || t)
            })
    };
    match value {
        r::Value::DateTime(_) => (true, false),
        r::Value::String(_) => (false, true),
        r::Value::List(items) => any(&mut items.iter()),
        r::Value::Map(entries) => any(&mut entries.values()),
        r::Value::Null
        | r::Value::Boolean(_)
        | r::Value::Integer(_)
        | r::Value::Float(_)
        | r::Value::Entity(_)
        | r::Value::Path(_) => (false, false),
    }
}
impl HashJoinTable {
    pub(super) fn bucket(&self, key: &r::GroupingKey) -> Option<&[u64]> {
        self.buckets.get(key).map(Vec::as_slice)
    }

    /// Fails when comparing `probe` with some key fails because the pair, or
    /// a pair of their members, is a datetime and text. A list or map probe
    /// that holds one kind is compared with every list and map key only when
    /// some such key holds the other, so this costs nothing unless both sides
    /// hold datetimes and text inside lists or maps.
    pub(super) fn check_probe(&self, probe: &r::Value) -> Result<()> {
        let mismatched = match probe {
            r::Value::DateTime(_) => self.text_keys,
            r::Value::String(_) => self.datetime_keys,
            r::Value::List(_) | r::Value::Map(_) => {
                let (datetime, text) = kinds(probe);
                if (datetime && self.nested_text) || (text && self.nested_datetimes) {
                    self.buckets
                        .keys()
                        .map(r::GroupingKey::value)
                        .filter(|key| matches!(key, r::Value::List(_) | r::Value::Map(_)))
                        .chain(&self.unmatched)
                        .try_for_each(|key| probe.equals(key).map(drop))?;
                }
                false
            }
            r::Value::Null
            | r::Value::Boolean(_)
            | r::Value::Integer(_)
            | r::Value::Float(_)
            | r::Value::Entity(_)
            | r::Value::Path(_) => false,
        };
        if mismatched {
            return Err(r::datetime_text_comparison().into());
        }
        Ok(())
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
        let (mut datetime_keys, mut text_keys) = (false, false);
        let (mut nested_datetimes, mut nested_text) = (false, false);
        let mut unmatched = Vec::new();
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
                let (datetime, text) = kinds(value);
                let nested = matches!(value, r::Value::List(_) | r::Value::Map(_));
                if nested {
                    nested_datetimes |= datetime;
                    nested_text |= text;
                } else {
                    datetime_keys |= datetime;
                    text_keys |= text;
                }
                if value.equals(value)? != Some(true) {
                    if nested && (datetime || text) {
                        let bytes = value
                            .allocated_bytes()
                            .saturating_add(size_of::<r::Value>());
                        payload = payload.saturating_add(bytes);
                        memory.resize(memory.bytes().saturating_add(bytes))?;
                        unmatched.try_reserve(1).map_err(|_| {
                            super::resource(detail::MEMORY_LIMIT, "join allocation failed")
                        })?;
                        unmatched.push(value.clone());
                    }
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
                    .map_err(|_| super::resource(detail::MEMORY_LIMIT, "join allocation failed"))?;
                let bucket = buckets.entry(key).or_default();
                bucket.try_reserve(1).map_err(|_| {
                    super::resource(detail::MEMORY_LIMIT, "join bucket allocation failed")
                })?;
                bucket.push(id);
                memory.resize(retained)?;
            }
        }
        Ok(Self {
            buckets,
            datetime_keys,
            text_keys,
            nested_datetimes,
            nested_text,
            unmatched,
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
