//! Executable multi-get plan contract and serde validation.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use super::{indexed, positions, prepared};
use crate::exec::kv::key;
use crate::exec::ExecPlanError;
use crate::{ir, properties};

/// Validated multi-get batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KvMultiGetPlan {
    /// Shared keyspace.
    keyspace: key::ElementKeyspace,
    /// Sorted physical keys.
    keys: ir::AtLeast<key::KvKey, 1>,
    /// Mapping from sorted keys back to original input positions.
    original_positions: positions::OriginalPositions,
    /// Planner locality classification.
    locality: properties::KeyLocality,
    /// Maximum batch size used when this plan was formed.
    max_batch_size: properties::PositiveUsize,
}

impl KvMultiGetPlan {
    /// Build a sorted multi-get batch from prevalidated keys.
    pub fn from_keys(keys: prepared::KvMultiGetKeys, locality: properties::KeyLocality) -> Self {
        let (mut keys, max_batch_size) = keys.into_parts();
        let keyspace = keys.keyspace();
        keys.sort_by_encoded_key();
        let original_positions = keys.original_positions();
        let keys = keys.into_keys();

        Self {
            keyspace,
            keys,
            original_positions,
            locality,
            max_batch_size,
        }
    }

    /// Build a sorted multi-get batch from input keys.
    pub fn new(
        keys: Vec<key::KvKey>,
        locality: properties::KeyLocality,
        max_batch_size: properties::PositiveUsize,
    ) -> Result<Self, ExecPlanError> {
        Self::from_indexed_keys(
            indexed::IndexedKvKeys::from_keys(keys)?,
            locality,
            max_batch_size,
        )
    }

    pub(super) fn from_indexed_keys(
        mut keys: indexed::IndexedKvKeys,
        locality: properties::KeyLocality,
        max_batch_size: properties::PositiveUsize,
    ) -> Result<Self, ExecPlanError> {
        let keyspace = keys.keyspace();
        if let Some(mixed) = keys
            .iter()
            .find(|indexed| indexed.key.keyspace() != keyspace)
        {
            return Err(ExecPlanError::MixedMultiGetKeyspace {
                expected: keyspace,
                actual: mixed.key.keyspace(),
            });
        }
        if keys.len() > max_batch_size.get() {
            return Err(ExecPlanError::MultiGetBatchTooLarge {
                max: max_batch_size,
                actual: keys.len(),
            });
        }

        keys.sort_by_encoded_key();
        let original_positions = keys.original_positions();
        let keys = keys.into_keys();

        Ok(Self {
            keyspace,
            keys,
            original_positions,
            locality,
            max_batch_size,
        })
    }

    /// Shared keyspace for all keys in this batch.
    pub const fn keyspace(&self) -> key::ElementKeyspace {
        self.keyspace
    }

    /// Sorted physical keys.
    pub fn keys(&self) -> &[key::KvKey] {
        self.keys.as_ref()
    }

    /// Sorted-key position back to the original logical input position.
    pub fn original_positions(&self) -> &[usize] {
        self.original_positions.as_ref()
    }

    /// Number of keys in this batch.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether this batch has no keys.
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// Planner locality classification.
    pub const fn locality(&self) -> properties::KeyLocality {
        self.locality
    }

    /// Maximum batch size used when this plan was formed.
    pub const fn max_batch_size(&self) -> properties::PositiveUsize {
        self.max_batch_size
    }

    /// Iterate sorted keys with their original logical positions.
    pub fn keyed_original_positions(&self) -> impl Iterator<Item = (&key::KvKey, usize)> + '_ {
        self.keys
            .iter()
            .zip(self.original_positions.iter().copied())
    }

    /// Return the logical prefix of this multi-get batch by original input
    /// position, then rebuild the sorted physical-key representation.
    ///
    /// ```
    /// use helix_planner::exec::{ElementKeyspace, KvKey, KvMultiGetPlan};
    /// use helix_planner::properties::{KeyLocality, PositiveUsize};
    ///
    /// let keyspace = ElementKeyspace::NodeProperty;
    /// let plan = KvMultiGetPlan::new(
    ///     vec![
    ///         keyspace.point_key(30),
    ///         keyspace.point_key(10),
    ///         keyspace.point_key(20),
    ///     ],
    ///     KeyLocality::Close,
    ///     PositiveUsize::new(4).unwrap(),
    /// )
    /// .unwrap();
    ///
    /// let prefix = plan
    ///     .prefix_by_original_position(PositiveUsize::new(2).unwrap())
    ///     .unwrap();
    ///
    /// assert_eq!(
    ///     prefix.keys().iter().map(KvKey::id).collect::<Vec<_>>(),
    ///     vec![10, 30]
    /// );
    /// assert_eq!(prefix.original_positions(), &[1, 0]);
    /// ```
    pub fn prefix_by_original_position(
        &self,
        limit: properties::PositiveUsize,
    ) -> Result<Self, ExecPlanError> {
        if limit.get() >= self.len() {
            return Ok(self.clone());
        }
        let items = self
            .keyed_original_positions()
            .filter(|(_, original_position)| *original_position < limit.get())
            .map(|(key, original_position)| indexed::IndexedKvKey {
                original_position,
                key: key.clone(),
            })
            .collect::<Vec<_>>();
        let items = ir::AtLeast::<_, 1>::try_from_vec(items).ok_or(ExecPlanError::EmptyMultiGet)?;
        Self::from_indexed_keys(
            indexed::IndexedKvKeys::from_unique_enumerated(items),
            self.locality,
            self.max_batch_size,
        )
    }
}

#[derive(Deserialize)]
struct KvMultiGetPlanWire {
    keyspace: key::ElementKeyspace,
    keys: ir::AtLeast<key::KvKey, 1>,
    original_positions: positions::OriginalPositions,
    locality: properties::KeyLocality,
    max_batch_size: properties::PositiveUsize,
}

impl TryFrom<KvMultiGetPlanWire> for KvMultiGetPlan {
    type Error = String;

    fn try_from(wire: KvMultiGetPlanWire) -> Result<Self, Self::Error> {
        if wire.keys.len() != wire.original_positions.len() {
            return Err(format!(
                "multi_get keys/original_positions length mismatch: {} != {}",
                wire.keys.len(),
                wire.original_positions.len()
            ));
        }
        if wire.keys.len() > wire.max_batch_size.get() {
            return Err(format!(
                "multi_get has {} keys but max_batch_size is {}",
                wire.keys.len(),
                wire.max_batch_size.get()
            ));
        }
        if let Some(mixed) = wire.keys.iter().find(|key| key.keyspace() != wire.keyspace) {
            return Err(format!(
                "multi_get keyspace mismatch: expected {}, got {}",
                wire.keyspace,
                mixed.keyspace()
            ));
        }
        if wire
            .keys
            .windows(2)
            .any(|pair| pair[0].bytes() > pair[1].bytes())
        {
            return Err("multi_get keys must be sorted by encoded bytes".to_string());
        }
        Ok(Self {
            keyspace: wire.keyspace,
            keys: wire.keys,
            original_positions: wire.original_positions,
            locality: wire.locality,
            max_batch_size: wire.max_batch_size,
        })
    }
}

impl<'de> Deserialize<'de> for KvMultiGetPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        KvMultiGetPlanWire::deserialize(deserializer)?
            .try_into()
            .map_err(D::Error::custom)
    }
}
