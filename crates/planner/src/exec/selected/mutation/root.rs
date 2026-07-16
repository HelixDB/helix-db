//! Selected root mutation wrapper.

use super::plan::SelectedMutationPlan;
use crate::exec::selected::physical::SelectedPhysicalPlan;
use crate::exec::selected::provenance::SelectedRootProvenance;
use crate::exec::selected::SelectedRootConstructionError;
use crate::physical;

/// Selected root mutation.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedRootMutation {
    /// Selected root mutation implementation.
    alternative: SelectedPhysicalPlan,
    /// How this selected root was produced.
    provenance: SelectedRootProvenance,
    /// Mutation payload with selected child inputs where required.
    plan: SelectedMutationPlan,
}

impl SelectedRootMutation {
    /// Build a selected root mutation.
    pub fn new(
        alternative: SelectedPhysicalPlan,
        provenance: SelectedRootProvenance,
        plan: SelectedMutationPlan,
    ) -> Result<Self, SelectedRootConstructionError> {
        if !matches!(alternative.expr(), physical::PhysicalExpr::Barrier) {
            return Err(SelectedRootConstructionError::IncompatiblePhysicalShape);
        }
        Ok(Self {
            alternative,
            provenance,
            plan,
        })
    }

    /// Selected root mutation implementation.
    pub const fn alternative(&self) -> &SelectedPhysicalPlan {
        &self.alternative
    }

    /// How this selected root was produced.
    pub const fn provenance(&self) -> &SelectedRootProvenance {
        &self.provenance
    }

    /// Mutation payload with selected child inputs where required.
    pub const fn plan(&self) -> &SelectedMutationPlan {
        &self.plan
    }

    pub(in crate::exec::selected) fn into_parts(
        self,
    ) -> (
        SelectedPhysicalPlan,
        SelectedRootProvenance,
        SelectedMutationPlan,
    ) {
        (self.alternative, self.provenance, self.plan)
    }
}
