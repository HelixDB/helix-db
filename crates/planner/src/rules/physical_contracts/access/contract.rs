use crate::{cost, physical, properties};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::rules) struct AccessPhysicalContract {
    pub(in crate::rules) access: physical::PhysicalAccess,
    pub(in crate::rules) delivered: properties::DeliveredProperties,
    pub(in crate::rules) cost: cost::CostVector,
    pub(in crate::rules) estimated_rows: cost::EstimatedRows,
}

impl AccessPhysicalContract {
    pub(in crate::rules) fn new(
        access: physical::PhysicalAccess,
        delivered: properties::DeliveredProperties,
        cost: cost::CostVector,
        estimated_rows: cost::EstimatedRows,
    ) -> Self {
        Self {
            access,
            delivered,
            cost,
            estimated_rows,
        }
    }
}
