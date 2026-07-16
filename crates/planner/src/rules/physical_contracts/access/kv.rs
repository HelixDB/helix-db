use crate::{cost, exec, ir, physical, properties};

pub(super) fn point_ids_access_contract(
    keyspace: exec::ElementKeyspace,
    ids: &ir::ElementIds,
    storage: &cost::StorageCostProfile,
) -> (physical::PhysicalAccess, cost::CostVector) {
    let id_values = ids.as_at_least();
    let max_batch_size = storage.multi_get_batch_size(properties::KeyLocality::Unknown);
    let access = if let [id] = id_values.as_ref() {
        physical::PhysicalAccess::Kv(exec::KvReadPlan::Get {
            key: keyspace.point_key(*id),
        })
    } else if let Some(keys) = exec::KvMultiGetKeys::from_element_ids(keyspace, ids, max_batch_size)
    {
        physical::PhysicalAccess::Kv(exec::KvReadPlan::MultiGet(exec::KvMultiGetPlan::from_keys(
            keys,
            properties::KeyLocality::Unknown,
        )))
    } else {
        physical::PhysicalAccess::PointReads {
            locality: properties::KeyLocality::Unknown,
        }
    };
    (access, point_ids_cost(id_values, storage))
}

pub(super) fn unbounded_range_access(keyspace: exec::ElementKeyspace) -> physical::PhysicalAccess {
    physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan {
        keyspace,
        start: exec::KvKeyBound::Unbounded,
        end: exec::KvKeyBound::Unbounded,
        limit: None,
    })
}

fn point_ids_cost(
    ids: &ir::AtLeast<u64, 1>,
    storage: &cost::StorageCostProfile,
) -> cost::CostVector {
    if ids.len() == 1 {
        return storage.point_gets(properties::PositiveUsize::at_least_one(1));
    }
    let batch_size = storage
        .multi_get_batch_size(properties::KeyLocality::Unknown)
        .get();
    let batches = ids
        .chunks(batch_size)
        .map(|chunk| {
            storage.multi_get(
                properties::PositiveUsize::at_least_one(chunk.len()),
                properties::KeyLocality::Unknown,
            )
        })
        .collect::<Vec<_>>();
    if let [batch] = batches.as_slice() {
        *batch
    } else {
        storage.parallel(&batches, storage.max_parallel_kv_reads)
    }
}
