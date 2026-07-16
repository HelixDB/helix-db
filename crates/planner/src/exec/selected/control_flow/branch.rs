//! Selected branch control-flow contract.

use crate::exec::selected::physical::SelectedPhysicalPlan;
use crate::exec::selected::provenance::SelectedRootProvenance;
use crate::exec::selected::run::SelectedExecutableRunRoot;
use crate::exec::selected::SelectedRootConstructionError;
use crate::{ir, physical};

/// Selected branch control-flow payload.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectedBranchPlan {
    /// Run all branch plans and union their outputs.
    Union(ir::AtLeast<SelectedExecutableRunRoot, 2>),
    /// Run the branch only for rows satisfying the condition.
    Choose {
        /// Branch condition.
        condition: ir::PredicatePlan,
        /// Then plan.
        then_plan: Box<SelectedExecutableRunRoot>,
    },
    /// Run one of two branch plans by condition.
    ChooseElse {
        /// Branch condition.
        condition: ir::PredicatePlan,
        /// Then plan.
        then_plan: Box<SelectedExecutableRunRoot>,
        /// Else plan.
        else_plan: Box<SelectedExecutableRunRoot>,
    },
    /// Run branch plans until one produces output.
    Coalesce(ir::AtLeast<SelectedExecutableRunRoot, 1>),
    /// Run an optional branch plan.
    Optional(Box<SelectedExecutableRunRoot>),
}

/// Selected root branch.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedRootBranch {
    /// Selected root branch implementation.
    alternative: SelectedPhysicalPlan,
    /// How this selected root was produced.
    provenance: SelectedRootProvenance,
    /// Selected branch input.
    input: Box<SelectedExecutableRunRoot>,
    /// Selected branch payload.
    plan: SelectedBranchPlan,
}

impl SelectedRootBranch {
    /// Build a selected root branch.
    pub fn new(
        alternative: SelectedPhysicalPlan,
        provenance: SelectedRootProvenance,
        input: Box<SelectedExecutableRunRoot>,
        plan: SelectedBranchPlan,
    ) -> Result<Self, SelectedRootConstructionError> {
        if !matches!(
            alternative.expr(),
            physical::PhysicalExpr::Control(physical::PhysicalControlOp::Branch)
        ) {
            return Err(SelectedRootConstructionError::IncompatiblePhysicalShape);
        }
        Ok(Self {
            alternative,
            provenance,
            input,
            plan,
        })
    }

    /// Selected root branch implementation.
    pub const fn alternative(&self) -> &SelectedPhysicalPlan {
        &self.alternative
    }

    /// How this selected root was produced.
    pub const fn provenance(&self) -> &SelectedRootProvenance {
        &self.provenance
    }

    /// Selected branch input.
    pub const fn input(&self) -> &SelectedExecutableRunRoot {
        &self.input
    }

    /// Selected branch payload.
    pub const fn plan(&self) -> &SelectedBranchPlan {
        &self.plan
    }

    pub(in crate::exec::selected) fn into_parts(
        self,
    ) -> (
        SelectedPhysicalPlan,
        SelectedRootProvenance,
        Box<SelectedExecutableRunRoot>,
        SelectedBranchPlan,
    ) {
        (self.alternative, self.provenance, self.input, self.plan)
    }
}
