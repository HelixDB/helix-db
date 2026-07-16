//! Prevalidated same-keyspace multi-get key sets.

use super::indexed;
use crate::exec::kv::key;
use crate::{ir, properties};

/// Non-empty same-keyspace multi-get keys whose size is within the selected
/// batch limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvMultiGetKeys {
    keys: indexed::IndexedKvKeys,
    max_batch_size: properties::PositiveUsize,
}

impl KvMultiGetKeys {
    /// Build same-keyspace point keys from concrete element IDs.
    pub fn from_element_ids(
        keyspace: key::ElementKeyspace,
        ids: &ir::ElementIds,
        max_batch_size: properties::PositiveUsize,
    ) -> Option<Self> {
        (ids.as_ref().len() <= max_batch_size.get()).then(|| {
            let keys = ids
                .as_at_least()
                .enumerate_map_ref(|original_position, id| indexed::IndexedKvKey {
                    original_position,
                    key: keyspace.point_key(*id),
                });
            Self {
                keys: indexed::IndexedKvKeys::from_unique_enumerated(keys),
                max_batch_size,
            }
        })
    }

    /// Shared keyspace for every prepared key.
    pub fn keyspace(&self) -> key::ElementKeyspace {
        self.keys.keyspace()
    }

    /// Number of prepared keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether this prepared batch is empty.
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// Maximum batch size used to validate this key set.
    pub const fn max_batch_size(&self) -> properties::PositiveUsize {
        self.max_batch_size
    }

    pub(super) fn into_parts(self) -> (indexed::IndexedKvKeys, properties::PositiveUsize) {
        (self.keys, self.max_batch_size)
    }
}
