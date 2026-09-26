//! Group state, lookup capacity and ownership transfer for direct aggregation.
use super::super::{
    memory, projection::ProjectionInputs, resource, row_bytes, Error, Limits, Result,
};
use super::{Group, Specification};
use helix_planner::relational as r;
use std::collections::HashMap;

pub(super) struct GroupBuffer {
    by_key: HashMap<r::GroupingKey, usize>,
    groups: Vec<Group>,
    memory: memory::Reservation,
    payload: usize,
}

pub(super) struct GroupDrain {
    pub(super) groups: std::vec::IntoIter<Group>,
    pub(super) memory: memory::Reservation,
}

impl GroupBuffer {
    pub(super) fn new(budget: &memory::Budget) -> Result<Self> {
        Ok(Self {
            by_key: HashMap::new(),
            groups: Vec::new(),
            memory: budget.reserve(0)?,
            payload: 0,
        })
    }
    pub(super) fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
    pub(super) fn index(&self, key: &r::GroupingKey) -> Option<usize> {
        self.by_key.get(key).copied()
    }
    fn retained(&self, payload: usize) -> usize {
        payload
            .saturating_add(self.groups.capacity().saturating_mul(size_of::<Group>()))
            .saturating_add(r::allocation::hash_table_retained_bytes::<
                r::GroupingKey,
                usize,
            >(self.by_key.len()))
    }

    /// Any allocation failure aborts this operator. Admission precedes vector
    /// and table growth, representative copies and accumulator construction.
    pub(super) fn insert(
        &mut self,
        key: r::GroupingKey,
        mut key_memory: memory::Reservation,
        row: Option<&r::Row>,
        width: usize,
        inputs: &ProjectionInputs,
        specifications: &[Specification<'_>],
    ) -> Result<usize> {
        assert!(
            row.is_none_or(|row| row.len() == width),
            "validated group row width"
        );
        let base_bytes = row
            .map_or(0, |row| {
                row.iter()
                    .enumerate()
                    .filter(|(slot, _)| inputs.keeps(*slot))
                    .fold(0_usize, |bytes, (_, value)| {
                        bytes.saturating_add(
                            value
                                .allocated_bytes()
                                .saturating_sub(size_of::<r::Value>()),
                        )
                    })
            })
            .saturating_add(width.saturating_mul(size_of::<r::Value>()))
            // This extra slot follows the row into the final output; the
            // Group Vec's own base descriptor is accounted by capacity below.
            .saturating_add(size_of::<r::Row>());
        let accumulator_bytes =
            specifications
                .iter()
                .fold(0_usize, |bytes, (function, _, distinct)| {
                    bytes
                        .saturating_add(r::Accumulator::new(*function, *distinct).allocated_bytes())
                });
        let key_bytes = key.value().allocated_bytes();
        key_memory.shrink_to(key_bytes);
        self.memory.absorb(key_memory);
        let payload = self
            .payload
            .saturating_add(key_bytes)
            .saturating_add(base_bytes)
            .saturating_add(accumulator_bytes);
        let capacity = if self.groups.len() == self.groups.capacity() {
            self.groups.capacity().saturating_mul(2).max(4)
        } else {
            self.groups.capacity()
        };
        let table = r::allocation::hash_table_retained_bytes::<r::GroupingKey, usize>(
            self.by_key.len().saturating_add(1),
        );
        let old_table = if self.by_key.len() == self.by_key.capacity() {
            r::allocation::hash_table_retained_bytes::<r::GroupingKey, usize>(self.by_key.len())
        } else {
            0
        };
        let retained = payload
            .saturating_add(capacity.saturating_mul(size_of::<Group>()))
            .saturating_add(table);
        self.memory.resize(retained.saturating_add(old_table))?;
        if capacity > self.groups.capacity() {
            self.groups
                .try_reserve_exact(capacity - self.groups.len())
                .map_err(|_| resource("MemoryLimit", "aggregate group allocation failed"))?;
        }
        self.by_key
            .try_reserve(1)
            .map_err(|_| resource("MemoryLimit", "aggregate key table allocation failed"))?;
        let mut base = vec![r::Value::Null; width];
        for (slot, value) in row
            .into_iter()
            .flat_map(|row| row.iter().enumerate())
            .filter(|(slot, _)| inputs.keeps(*slot))
        {
            base[slot] = value.clone();
        }
        assert!(
            row_bytes(&base) <= base_bytes,
            "representative fits its admitted bound"
        );
        let mut accumulators = Vec::with_capacity(specifications.len());
        accumulators.extend(
            specifications
                .iter()
                .map(|(function, _, distinct)| r::Accumulator::new(*function, *distinct)),
        );
        let index = self.groups.len();
        self.by_key.insert(key, index);
        self.groups.push(Group { base, accumulators });
        self.payload = payload;
        self.memory.shrink_to(retained);
        Ok(index)
    }

    pub(super) fn push(
        &mut self,
        group: usize,
        aggregate: usize,
        value: r::Value,
        budget: &memory::Budget,
        limits: Limits,
    ) -> Result<()> {
        let input = budget.reserve(value.allocated_bytes())?;
        let before = self.groups[group].accumulators[aggregate].allocated_bytes();
        let fixed = self.retained(self.payload.saturating_sub(before));
        let accumulator = &mut self.groups[group].accumulators[aggregate];
        let memory = &mut self.memory;
        accumulator.push_with_admission::<Error>(
            value,
            limits.collection_items,
            budget.available().saturating_add(before),
            |bound| {
                memory
                    .resize(fixed.saturating_add(bound))
                    .map_err(Into::into)
            },
        )?;
        self.payload = self
            .payload
            .saturating_sub(before)
            .saturating_add(accumulator.allocated_bytes());
        self.memory.absorb(input);
        let retained = self.retained(self.payload);
        self.memory.shrink_to(retained);
        Ok(())
    }

    /// Move admitted key payloads into their unique output slots. The group
    /// vector preserves input order even though hash-table iteration is unordered.
    /// Cancellation drops every remaining key, representative and reservation.
    pub(super) fn into_drain(
        mut self,
        items: &r::ProjectionProgram,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<GroupDrain> {
        let mut retained = self.groups.capacity().saturating_mul(size_of::<Group>());
        for (key, index) in self.by_key {
            checkpoint()?;
            let r::Value::List(values) = key.into_value() else {
                unreachable!("direct aggregation uses framed row keys");
            };
            let destinations = items.iter().filter(|item| !item.expression.has_aggregate());
            assert_eq!(
                values.len(),
                destinations.clone().count(),
                "one retained value per grouping expression"
            );
            let group = &mut self.groups[index];
            for (item, value) in destinations.zip(values) {
                group.base[item.slot.0 as usize] = value;
            }
            retained = group.accumulators.iter().fold(
                retained.saturating_add(row_bytes(&group.base)),
                |bytes, state| bytes.saturating_add(state.allocated_bytes()),
            );
        }
        self.memory.shrink_to(retained);
        Ok(GroupDrain {
            groups: self.groups.into_iter(),
            memory: self.memory,
        })
    }
}

#[cfg(test)]
#[path = "../tests/direct_group_keys.rs"]
mod tests;
