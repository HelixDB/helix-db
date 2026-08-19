use crate::{catalog, cost, ir, physical, properties};

#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules) struct AccessPhysicalContract {
    pub(in crate::rules) access: physical::PhysicalAccess,
    pub(in crate::rules) delivered: properties::DeliveredProperties,
    pub(in crate::rules) cost: cost::CostVector,
    pub(in crate::rules) estimated_rows: cost::EstimatedRows,
    execution: AccessExecutionCost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AccessExecutionCost {
    MaterializedRows,
    SecondaryIds {
        cost: cost::CostVector,
        source: SecondaryIdSource,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SecondaryIdSource {
    Other,
    BatchableEquality {
        index_id: ir::NonEmptyString,
        key: catalog::ScopedPropertyKey,
    },
}

impl AccessPhysicalContract {
    pub(in crate::rules) fn with_access(mut self, access: physical::PhysicalAccess) -> Self {
        self.access = access;
        self
    }

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
            execution: AccessExecutionCost::MaterializedRows,
        }
    }

    pub(in crate::rules) fn new_secondary(
        access: physical::PhysicalAccess,
        delivered: properties::DeliveredProperties,
        id_cost: cost::CostVector,
        materialization_cost: cost::CostVector,
        estimated_rows: cost::EstimatedRows,
    ) -> Self {
        Self {
            access,
            delivered,
            cost: id_cost.serial(materialization_cost),
            estimated_rows,
            execution: AccessExecutionCost::SecondaryIds {
                cost: id_cost,
                source: SecondaryIdSource::Other,
            },
        }
    }

    pub(in crate::rules) fn new_batchable_equality(
        access: physical::PhysicalAccess,
        delivered: properties::DeliveredProperties,
        id_cost: cost::CostVector,
        materialization_cost: cost::CostVector,
        estimated_rows: cost::EstimatedRows,
        index_id: ir::NonEmptyString,
        key: catalog::ScopedPropertyKey,
    ) -> Self {
        Self {
            access,
            delivered,
            cost: id_cost.serial(materialization_cost),
            estimated_rows,
            execution: AccessExecutionCost::SecondaryIds {
                cost: id_cost,
                source: SecondaryIdSource::BatchableEquality { index_id, key },
            },
        }
    }

    pub(in crate::rules) fn secondary_id_cost(&self) -> Option<cost::CostVector> {
        match &self.execution {
            AccessExecutionCost::MaterializedRows => None,
            AccessExecutionCost::SecondaryIds { cost, .. } => Some(*cost),
        }
    }

    pub(in crate::rules) fn batchable_equality_identity(
        &self,
    ) -> Option<(&ir::NonEmptyString, &catalog::ScopedPropertyKey)> {
        match &self.execution {
            AccessExecutionCost::SecondaryIds {
                source: SecondaryIdSource::BatchableEquality { index_id, key },
                ..
            } => Some((index_id, key)),
            AccessExecutionCost::MaterializedRows
            | AccessExecutionCost::SecondaryIds {
                source: SecondaryIdSource::Other,
                ..
            } => None,
        }
    }
}
