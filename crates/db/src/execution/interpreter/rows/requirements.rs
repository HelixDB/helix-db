//! Admit owned property demand before allocating its slot map and key sets.
use super::{memory, Result};
use helix_planner::relational as r;
use std::collections::BTreeMap;

pub(super) struct Requirements {
    values: BTreeMap<r::Slot, r::PropertyDemand>,
    memory: memory::Reservation,
    bytes: usize,
}

impl Requirements {
    pub(super) fn new(budget: &memory::Budget) -> Result<Self> {
        Ok(Self {
            values: BTreeMap::new(),
            memory: budget.reserve(0)?,
            bytes: 0,
        })
    }

    pub(super) fn values(&self) -> &BTreeMap<r::Slot, r::PropertyDemand> {
        &self.values
    }

    /// Repeated borrowed probes do not copy keys or increase retained admission.
    /// All-property demand subsumes and releases any prior selected-key state.
    pub(super) fn insert(
        &mut self,
        slot: r::Slot,
        requirement: r::PropertyRequirement<'_>,
    ) -> Result<()> {
        let previous = self.values.get(&slot);
        let (added, removed) = match (previous, requirement) {
            (Some(r::PropertyDemand::All), _) => return Ok(()),
            (Some(r::PropertyDemand::Keys(keys)), r::PropertyRequirement::Key(key))
                if keys.contains(key) =>
            {
                return Ok(())
            }
            (Some(_), r::PropertyRequirement::Metadata) => return Ok(()),
            (previous, r::PropertyRequirement::Key(key)) => {
                let count = match previous {
                    Some(r::PropertyDemand::Keys(keys)) => keys.len(),
                    Some(r::PropertyDemand::All) => unreachable!("all demand returned above"),
                    None => 0,
                };
                let old_tree = if count == 0 {
                    0
                } else {
                    r::allocation::btree_bytes::<String, ()>(count)
                };
                let new_tree = r::allocation::btree_bytes::<String, ()>(count.saturating_add(1));
                (
                    new_tree.saturating_sub(old_tree).saturating_add(key.len()),
                    0,
                )
            }
            (Some(r::PropertyDemand::Keys(keys)), r::PropertyRequirement::All) => {
                let old_tree = if keys.is_empty() {
                    0
                } else {
                    r::allocation::btree_bytes::<String, ()>(keys.len())
                };
                (
                    0,
                    keys.iter()
                        .fold(old_tree, |bytes, key| bytes.saturating_add(key.len())),
                )
            }
            (None, r::PropertyRequirement::All | r::PropertyRequirement::Metadata) => (0, 0),
        };
        let slot_growth = if previous.is_none() {
            let old_tree = if self.values.is_empty() {
                0
            } else {
                r::allocation::btree_bytes::<r::Slot, r::PropertyDemand>(self.values.len())
            };
            r::allocation::btree_bytes::<r::Slot, r::PropertyDemand>(
                self.values.len().saturating_add(1),
            )
            .saturating_sub(old_tree)
        } else {
            0
        };
        let next = self
            .bytes
            .checked_sub(removed)
            .expect("obsolete key demand is admitted")
            .saturating_add(added)
            .saturating_add(slot_growth);
        self.memory.resize(self.bytes.max(next))?;
        let demand = self.values.entry(slot).or_default();
        match requirement {
            r::PropertyRequirement::All => *demand = r::PropertyDemand::All,
            r::PropertyRequirement::Metadata => {}
            r::PropertyRequirement::Key(key) => {
                let r::PropertyDemand::Keys(keys) = demand else {
                    unreachable!("all demand returned before insertion")
                };
                keys.insert(key.to_owned());
            }
        }
        self.memory.resize(next)?;
        self.bytes = next;
        Ok(())
    }
}
