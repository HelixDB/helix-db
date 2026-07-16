//! Indexed multi-get key proofs.

use std::collections::BTreeSet;

use super::positions;
use crate::exec::kv::key;
use crate::exec::ExecPlanError;
use crate::ir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IndexedKvKey {
    pub(super) original_position: usize,
    pub(super) key: key::KvKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IndexedKvKeys {
    items: ir::AtLeast<IndexedKvKey, 1>,
}

impl IndexedKvKeys {
    pub(super) fn from_keys(keys: Vec<key::KvKey>) -> Result<Self, ExecPlanError> {
        Self::try_from_indexed(keys.into_iter().enumerate())
    }

    pub(super) fn from_unique_enumerated(items: ir::AtLeast<IndexedKvKey, 1>) -> Self {
        Self { items }
    }

    pub(super) fn try_from_indexed<I>(keys: I) -> Result<Self, ExecPlanError>
    where
        I: IntoIterator<Item = (usize, key::KvKey)>,
    {
        let items = keys
            .into_iter()
            .map(|(original_position, key)| IndexedKvKey {
                original_position,
                key,
            })
            .collect::<Vec<_>>();
        let items = ir::AtLeast::<_, 1>::try_from_vec(items).ok_or(ExecPlanError::EmptyMultiGet)?;
        let indexed = Self { items };
        indexed.ensure_unique_original_positions()?;
        Ok(indexed)
    }

    fn ensure_unique_original_positions(&self) -> Result<(), ExecPlanError> {
        let mut seen = BTreeSet::new();
        self.items
            .iter()
            .map(|indexed| indexed.original_position)
            .find(|position| !seen.insert(*position))
            .map_or(Ok(()), |position| {
                Err(ExecPlanError::DuplicateMultiGetOriginalPosition { position })
            })
    }

    pub(super) fn keyspace(&self) -> key::ElementKeyspace {
        self.items[0].key.keyspace()
    }

    pub(super) fn len(&self) -> usize {
        self.items.len()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &IndexedKvKey> {
        self.items.iter()
    }

    pub(super) fn sort_by_encoded_key(&mut self) {
        self.items.sort_by_key(|indexed| *indexed.key.bytes());
    }

    pub(super) fn original_positions(&self) -> positions::OriginalPositions {
        positions::OriginalPositions::from_unique_unchecked(
            self.items.map_ref(|indexed| indexed.original_position),
        )
    }

    pub(super) fn into_keys(self) -> ir::AtLeast<key::KvKey, 1> {
        self.items.map(|indexed| indexed.key)
    }
}
