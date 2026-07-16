//! Standalone stream physical-contract outcomes.

use crate::{cost, logical, physical, properties};

#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules) enum StreamPhysicalContract {
    Implemented(StreamPhysicalImplementation),
    Unsupported(StreamPhysicalContractRejection),
}

#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules) struct StreamPhysicalImplementation {
    op: physical::PhysicalStreamOp,
    delivered: properties::DeliveredProperties,
    cost: cost::CostVector,
}

impl StreamPhysicalImplementation {
    pub(in crate::rules) const fn new(
        op: physical::PhysicalStreamOp,
        delivered: properties::DeliveredProperties,
        cost: cost::CostVector,
    ) -> Self {
        Self {
            op,
            delivered,
            cost,
        }
    }

    pub(in crate::rules) fn into_parts(
        self,
    ) -> (
        physical::PhysicalStreamOp,
        properties::DeliveredProperties,
        cost::CostVector,
    ) {
        (self.op, self.delivered, self.cost)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::rules) enum StreamPhysicalContractRejection {
    UnsupportedPureOp(logical::PureLogicalOpKind),
}
