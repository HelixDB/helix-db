//! LSM-locality multi-get coalescing.

use std::collections::BTreeMap;

use super::{indexed, plan};
use crate::exec::kv::key;
use crate::exec::ExecPlanError;
use crate::{cost, ir, properties};

/// Coalesce non-empty point keys into a non-empty batch list.
pub fn coalesce_non_empty_multi_get_batches(
    keys: ir::AtLeast<key::KvKey, 1>,
    locality: properties::KeyLocality,
    profile: &cost::StorageCostProfile,
) -> Result<ir::AtLeast<plan::KvMultiGetPlan, 1>, ExecPlanError> {
    let batches = coalesce_multi_get_batches(keys.into_iter().collect(), locality, profile)?;
    ir::AtLeast::<_, 1>::try_from_vec(batches).ok_or(ExecPlanError::EmptyMultiGet)
}

/// Coalesce point keys into sorted executable `multi_get` batches.
pub fn coalesce_multi_get_batches(
    keys: Vec<key::KvKey>,
    locality: properties::KeyLocality,
    profile: &cost::StorageCostProfile,
) -> Result<Vec<plan::KvMultiGetPlan>, ExecPlanError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let max_batch_size = profile.multi_get_batch_size(locality);
    let mut by_keyspace = BTreeMap::<key::ElementKeyspace, Vec<(usize, key::KvKey)>>::new();
    for (index, key) in keys.into_iter().enumerate() {
        by_keyspace
            .entry(key.keyspace())
            .or_default()
            .push((index, key));
    }

    by_keyspace
        .into_values()
        .flat_map(|indexed| {
            indexed
                .chunks(max_batch_size.get())
                .map(|chunk| {
                    let indexed_keys =
                        indexed::IndexedKvKeys::try_from_indexed(chunk.iter().cloned())?;
                    plan::KvMultiGetPlan::from_indexed_keys(indexed_keys, locality, max_batch_size)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}
