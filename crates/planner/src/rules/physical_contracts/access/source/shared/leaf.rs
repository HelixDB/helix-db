//! Leaf access-source physical contracts.

use super::super::super::contract::AccessPhysicalContract;
use super::super::super::{
    delivered::{
        access_delivered_close, access_delivered_with, range_delivered_ordering, with_key_locality,
        with_ordering,
    },
    estimates::{
        equality_index_rows, search_cardinality, search_estimated_rows, stats_rows,
        unique_equality_rows,
    },
    kv::{point_ids_access_contract, unbounded_range_access},
};
use super::family::{AccessSourceFamily, EqualityIndexKind};
use crate::{catalog, cost, ir, physical, properties};

pub(super) fn empty_access_contract(element: properties::ElementKind) -> AccessPhysicalContract {
    AccessPhysicalContract::new(
        physical::PhysicalAccess::Empty,
        access_delivered_with(element, properties::CardinalityBounds::exact(0)),
        cost::CostVector::ZERO,
        cost::EstimatedRows::ZERO,
    )
}

pub(super) fn point_ids_contract<F>(
    ids: &ir::ElementIds,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract
where
    F: AccessSourceFamily,
{
    let (access, cost) = point_ids_access_contract(F::point_keyspace(), ids, storage);
    let rows = cost::EstimatedRows::rows(ids.as_ref().len() as u64);
    AccessPhysicalContract::new(
        access,
        access_delivered_with(
            F::element(),
            properties::CardinalityBounds::exact(ids.as_ref().len()),
        ),
        cost,
        rows,
    )
}

pub(super) fn runtime_input_contract(
    element: properties::ElementKind,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    AccessPhysicalContract::new(
        physical::PhysicalAccess::RuntimeInput,
        super::super::super::super::support::access_delivered(element),
        storage.source_inject(),
        storage.default_unknown_scan_rows,
    )
}

pub(super) fn all_scan_contract<F>(
    element: properties::ElementKind,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract
where
    F: AccessSourceFamily,
{
    AccessPhysicalContract::new(
        unbounded_range_access(F::all_scan_keyspace()),
        super::super::super::super::support::access_delivered(element),
        storage.range_scan(storage.default_unknown_scan_rows),
        storage.default_unknown_scan_rows,
    )
}

pub(super) fn label_scan_contract(
    element: properties::ElementKind,
    cardinality: Option<u64>,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    let rows = stats_rows(cardinality, storage);
    AccessPhysicalContract::new(
        physical::PhysicalAccess::LabelScan,
        access_delivered_close(element),
        storage.range_scan(rows),
        rows,
    )
}

pub(super) fn equality_index_contract(
    element: properties::ElementKind,
    cardinality: Option<u64>,
    kind: EqualityIndexKind,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    let rows = equality_rows(cardinality, kind, storage);
    AccessPhysicalContract::new(
        physical::PhysicalAccess::EqualityIndex,
        with_key_locality(
            access_delivered_with(element, equality_cardinality(kind)),
            properties::KeyLocality::Close,
        ),
        storage.equality_index_lookup(rows),
        rows,
    )
}

fn equality_rows(
    cardinality: Option<u64>,
    kind: EqualityIndexKind,
    storage: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    match kind {
        EqualityIndexKind::Unique => unique_equality_rows(cardinality, storage),
        EqualityIndexKind::NonUnique => equality_index_rows(cardinality, storage),
    }
}

const fn equality_cardinality(kind: EqualityIndexKind) -> properties::CardinalityBounds {
    match kind {
        EqualityIndexKind::Unique => properties::CardinalityBounds::zero_to(Some(1)),
        EqualityIndexKind::NonUnique => properties::CardinalityBounds::unknown(),
    }
}

pub(super) fn range_index_contract(
    element: properties::ElementKind,
    key: &catalog::ScopedPropertyDirectionKey,
    cardinality: Option<u64>,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    let rows = stats_rows(cardinality, storage);
    AccessPhysicalContract::new(
        physical::PhysicalAccess::RangeIndex,
        with_ordering(
            access_delivered_close(element),
            range_delivered_ordering(key),
        ),
        storage.range_scan(rows),
        rows,
    )
}

pub(super) fn search_contract(
    element: properties::ElementKind,
    access: physical::PhysicalAccess,
    k: &ir::SearchLimitPlan,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    let rows = search_estimated_rows(k, storage);
    AccessPhysicalContract::new(
        access,
        access_delivered_with(element, search_cardinality(k)),
        storage.range_scan(rows),
        rows,
    )
}
