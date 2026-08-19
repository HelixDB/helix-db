//! Source-family physical contract dispatch.

use super::super::super::{contract::AccessPhysicalContract, sets};
use super::{family, filter, leaf, set};
use crate::{context, cost, physical};

pub(in crate::rules) fn access_contract<F>(
    plan: &F::Plan,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> AccessPhysicalContract
where
    F: family::AccessSourceFamily,
{
    let element = F::element();
    match F::source_parts(plan) {
        family::AccessSourceParts::Empty => leaf::empty_access_contract(element),
        family::AccessSourceParts::PointIds(ids) => leaf::point_ids_contract::<F>(ids, storage),
        family::AccessSourceParts::RuntimeInput => leaf::runtime_input_contract(element, storage),
        family::AccessSourceParts::AllScan => leaf::all_scan_contract::<F>(element, storage),
        family::AccessSourceParts::LabelScan { label } => {
            leaf::label_scan_contract(element, F::label_cardinality(stats, label), storage)
        }
        family::AccessSourceParts::EqualityIndex {
            access,
            index_id,
            key,
            kind,
            semantics,
        } => leaf::equality_index_contract(
            leaf::EqualityIndexContractInput {
                access,
                element,
                index_id,
                key,
                cardinality: F::equality_cardinality(stats, key),
                label_cardinality: F::label_cardinality(stats, &key.label),
                kind,
                semantics,
            },
            storage,
        ),
        family::AccessSourceParts::RangeIndex { key } => {
            leaf::range_index_contract(element, key, F::range_cardinality(stats, key), storage)
        }
        family::AccessSourceParts::VectorSearch { k } => {
            leaf::search_contract(element, physical::PhysicalAccess::VectorSearch, k, storage)
        }
        family::AccessSourceParts::TextSearch { k } => {
            leaf::search_contract(element, physical::PhysicalAccess::TextSearch, k, storage)
        }
        family::AccessSourceParts::Intersect(plans) => set_contract::<F>(
            element,
            physical::PhysicalAccess::SetIntersection,
            plans,
            sets::set_intersection_cardinality,
            sets::set_intersection_estimated_rows,
            storage,
            stats,
        ),
        family::AccessSourceParts::Union(plans) => set_contract::<F>(
            element,
            physical::PhysicalAccess::SetUnion,
            plans,
            sets::set_union_cardinality,
            sets::set_union_estimated_rows,
            storage,
            stats,
        ),
        family::AccessSourceParts::ScanThenFilter { source } => {
            let child = access_contract::<F>(source, storage, stats);
            filter::scan_then_filter_contract(child, storage)
        }
    }
}

fn set_contract<F>(
    element: crate::properties::ElementKind,
    access: physical::PhysicalAccess,
    plans: Vec<&F::Plan>,
    cardinality: fn(
        &[crate::properties::DeliveredProperties],
    ) -> crate::properties::CardinalityBounds,
    estimated_rows: fn(&[cost::EstimatedRows]) -> cost::EstimatedRows,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> AccessPhysicalContract
where
    F: family::AccessSourceFamily,
{
    set::set_contract(
        element,
        access,
        plans
            .into_iter()
            .map(|plan| access_contract::<F>(plan, storage, stats))
            .collect(),
        cardinality,
        estimated_rows,
        storage,
    )
}
