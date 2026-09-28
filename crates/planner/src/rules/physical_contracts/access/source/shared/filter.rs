//! Residual-filter source physical contracts.

use super::super::super::contract::AccessPhysicalContract;
use crate::cost;

pub(super) fn scan_then_filter_contract(
    contract: AccessPhysicalContract,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    AccessPhysicalContract::new(
        contract.access,
        contract.delivered,
        contract
            .cost
            .serial(storage.stored_predicate_filter(contract.estimated_rows)),
        contract.estimated_rows,
    )
}
