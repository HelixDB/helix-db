//! One evaluated property map becomes one canonical graph transition.
//! Keys are validated on insertion; native null storage and removal remain
//! distinct operations. Replacement preserves the storage metadata namespace.
use super::*;
use crate::encoding::v2::values::property::property_value::PropertyValue;
use std::collections::BTreeMap;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Extend,
    ReplaceUserProperties,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum NameError {
    #[error("storage requires nonempty property names")]
    Empty,
    #[error("internal metadata cannot be assigned")]
    Reserved,
}

/// The producer admits map construction and keeps that admission until apply
/// returns. Private entries prevent bypassing name validation or mutating a
/// prepared map from another consumer. None removes; Some(Null) stores null.
pub(crate) struct Edit {
    mode: Mode,
    entries: BTreeMap<String, Entry>,
}
struct Entry {
    value: Option<PropertyValue>,
    matched: bool,
}
impl Edit {
    pub(crate) fn new(mode: Mode) -> Self {
        Self {
            mode,
            entries: BTreeMap::new(),
        }
    }

    pub(crate) fn insert(
        &mut self,
        name: String,
        value: Option<PropertyValue>,
    ) -> std::result::Result<(), NameError> {
        if name.is_empty() {
            return Err(NameError::Empty);
        }
        if name.starts_with('$') {
            return Err(NameError::Reserved);
        }
        self.entries.insert(
            name,
            Entry {
                value,
                matched: false,
            },
        );
        Ok(())
    }

    pub(crate) fn apply(
        mut self,
        scope: DataScope,
        entity: GraphEntity,
        before: CanonicalPropertyRow,
        budget: Option<&query_resources::Budget>,
    ) -> Result<PropertyEditOutcome> {
        if self.entries.is_empty() && self.mode == Mode::Extend {
            return Ok(PropertyEditOutcome::Unchanged(before));
        }
        if budget.is_some() {
            property::write::Prepared::new(before.properties())?;
            for entry in self.entries.values() {
                let Some(value) = &entry.value else {
                    continue;
                };
                property::write::validate_value(value)?;
            }
        }
        // Mark matches without allocating a lookup table for the whole stored
        // row. Work is O(existing properties * log(update keys) + update keys).
        for property in before.properties() {
            let Some(entry) = self.entries.get_mut(&property.name) else {
                continue;
            };
            entry.matched = true;
        }
        let old_changes = before
            .properties()
            .iter()
            .filter(|property| match self.entries.get(&property.name) {
                Some(entry) => entry
                    .value
                    .as_ref()
                    .is_none_or(|value| !property.value.same_v1_representation(value)),
                None => self.mode == Mode::ReplaceUserProperties && !property.name.starts_with('$'),
            })
            .map(|property| property.name.as_str());
        let additions = self
            .entries
            .iter()
            .filter(|(_, entry)| !entry.matched && entry.value.is_some())
            .map(|(name, _)| name.as_str());
        let Some(changed) = ChangedProperties::from_names(old_changes.chain(additions), budget)?
        else {
            return Ok(PropertyEditOutcome::Unchanged(before));
        };
        let capacity = before
            .properties()
            .len()
            .checked_add(self.entries.len())
            .ok_or(crate::HelixDbError::QueryMemoryLimitExceeded)?;
        let mut output = properties::Builder::new(capacity, budget)?;
        for property in before.properties() {
            match self.entries.remove_entry(&property.name) {
                Some((name, entry)) => {
                    let Some(value) = entry.value else {
                        continue;
                    };
                    output.push_owned(Property::new(name, value))?;
                }
                None if self.mode == Mode::Extend || property.name.starts_with('$') => {
                    output.push_cloned(property)?
                }
                None => {}
            }
        }
        for (name, entry) in self.entries {
            let Some(value) = entry.value else {
                continue;
            };
            output.push_owned(Property::new(name, value))?;
        }
        let after = CanonicalPropertyRow::from_decoded(output.finish())?;
        Ok(PropertyEditOutcome::Changed(
            GraphMutationTransition::Replace {
                scope,
                entity,
                before,
                after,
                changed,
            },
        ))
    }
}
