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
    AccessPhysicalContract::new_secondary(
        physical::PhysicalAccess::Empty,
        access_delivered_with(element, properties::CardinalityBounds::exact(0)),
        cost::CostVector::ZERO,
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
        storage.element_scan(storage.default_unknown_scan_rows),
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
        storage.label_scan(rows),
        rows,
    )
}

pub(super) struct EqualityIndexContractInput<'a> {
    pub(super) access: physical::PhysicalAccess,
    pub(super) element: properties::ElementKind,
    pub(super) index_id: &'a ir::NonEmptyString,
    pub(super) key: &'a catalog::ScopedPropertyKey,
    pub(super) cardinality: Option<u64>,
    pub(super) label_cardinality: Option<u64>,
    pub(super) kind: EqualityIndexKind,
    pub(super) semantics: ir::EqualityIndexValueSemantics,
    pub(super) indexed_values: usize,
}

/// Contract of one equality lookup, or of a literal set read as one batch.
///
/// A literal set reads its indexed values in one batched lookup and, when it
/// holds null, the label rows outside the lane concurrently; each value is
/// estimated as one equality, bounded by the label.
pub(super) fn equality_index_contract(
    input: EqualityIndexContractInput<'_>,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    let per_value = equality_rows(input.cardinality, input.kind, storage);
    let label_rows = input
        .label_cardinality
        .map_or(storage.default_unknown_scan_rows, cost::EstimatedRows::rows);
    // Several values together never exceed their label.
    let times = |rows: cost::EstimatedRows, values: usize| match values {
        0 | 1 => rows,
        values => {
            let rows = rows.as_rows().saturating_mul(values as u64);
            cost::EstimatedRows::rows(
                input
                    .label_cardinality
                    .map_or(rows, |label_rows| rows.min(label_rows)),
            )
        }
    };
    let null_branch =
        usize::from(input.semantics == ir::EqualityIndexValueSemantics::AuthoritativeNull);
    let rows = match input.semantics {
        ir::EqualityIndexValueSemantics::NonReflexive => cost::EstimatedRows::ZERO,
        ir::EqualityIndexValueSemantics::Indexed
        | ir::EqualityIndexValueSemantics::AuthoritativeNull
        | ir::EqualityIndexValueSemantics::RuntimeDependent => {
            times(per_value, input.indexed_values + null_branch)
        }
    };
    let indexed_cost = match (input.indexed_values, input.kind) {
        (0, _) => cost::CostVector::ZERO,
        (1, EqualityIndexKind::Unique) => storage.unique_equality_lookup(per_value),
        (1, EqualityIndexKind::NonUnique) => storage.bitmap_equality_lookup(per_value),
        (values, EqualityIndexKind::Unique) => storage.unique_equality_batch(
            crate::properties::PositiveUsize::at_least_one(values),
            times(per_value, values),
        ),
        (values, EqualityIndexKind::NonUnique) => storage.bitmap_equality_batch(
            crate::properties::PositiveUsize::at_least_one(values),
            times(per_value, values),
        ),
    };
    let id_cost = match input.semantics {
        ir::EqualityIndexValueSemantics::NonReflexive => cost::CostVector::ZERO,
        ir::EqualityIndexValueSemantics::AuthoritativeNull => {
            storage.parallel_reads(&[storage.null_equality_scan(label_rows), indexed_cost])
        }
        ir::EqualityIndexValueSemantics::Indexed
        | ir::EqualityIndexValueSemantics::RuntimeDependent => indexed_cost,
    };
    let cardinality = match (input.semantics, input.kind) {
        (ir::EqualityIndexValueSemantics::NonReflexive, _) => {
            properties::CardinalityBounds::exact(0)
        }
        (ir::EqualityIndexValueSemantics::Indexed, EqualityIndexKind::Unique) => {
            properties::CardinalityBounds::zero_to(Some(input.indexed_values))
        }
        (ir::EqualityIndexValueSemantics::Indexed, EqualityIndexKind::NonUnique)
        | (
            ir::EqualityIndexValueSemantics::AuthoritativeNull
            | ir::EqualityIndexValueSemantics::RuntimeDependent,
            _,
        ) => properties::CardinalityBounds::unknown(),
    };
    let delivered = with_key_locality(
        access_delivered_with(input.element, cardinality),
        properties::KeyLocality::Close,
    );
    let contract = AccessPhysicalContract::new_secondary(
        input.access,
        delivered,
        id_cost,
        storage.secondary_row_materialization(rows),
        rows,
    );
    // Only a single indexed value joins a same-key union's batch; a literal
    // set is already one.
    if input.semantics == ir::EqualityIndexValueSemantics::Indexed && input.indexed_values == 1 {
        contract.with_batchable_equality(
            input.index_id.clone(),
            input.key.clone(),
            match input.kind {
                EqualityIndexKind::Unique => catalog::IndexUniqueness::Unique,
                EqualityIndexKind::NonUnique => catalog::IndexUniqueness::NonUnique,
            },
        )
    } else {
        contract
    }
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

pub(super) fn range_index_contract(
    element: properties::ElementKind,
    key: &catalog::ScopedPropertyDirectionKey,
    iteration: crate::ir::RangeScanIteration,
    cardinality: Option<u64>,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    let rows = cardinality.map_or(storage.default_range_index_rows, cost::EstimatedRows::rows);
    AccessPhysicalContract::new_secondary(
        physical::PhysicalAccess::RangeIndex,
        with_ordering(
            access_delivered_close(element),
            range_delivered_ordering(key),
        ),
        storage
            .ordered_range_scan(rows, iteration)
            .serial(storage.authoritative_verification(rows)),
        storage.secondary_row_materialization(rows),
        rows,
    )
    .with_range_iteration(iteration)
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
