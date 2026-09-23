//! Residual-filter source physical contracts.

use super::super::super::contract::AccessPhysicalContract;
use crate::cost;

pub(super) fn scan_then_filter_contract(
    contract: AccessPhysicalContract,
    residual: &crate::ir::PredicatePlan,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    AccessPhysicalContract::new(
        contract.access,
        contract.delivered,
        contract
            .cost
            .serial(storage.residual_filter(residual.as_ref(), contract.estimated_rows)),
        contract.estimated_rows,
    )
}
