//! Access-set source physical contracts.

use super::super::super::{contract::AccessPhysicalContract, sets};
use crate::{cost, physical, properties};

pub(super) fn set_contract(
    element: properties::ElementKind,
    access: physical::PhysicalAccess,
    child_contracts: Vec<AccessPhysicalContract>,
    cardinality: fn(&[properties::DeliveredProperties]) -> properties::CardinalityBounds,
    estimated_rows: fn(&[cost::EstimatedRows]) -> cost::EstimatedRows,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    sets::access_set_contract(
        element,
        access,
        child_contracts,
        cardinality,
        estimated_rows,
        storage,
    )
}
