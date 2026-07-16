//! Selected repeat control-flow contract.

use std::num::NonZeroUsize;

use crate::exec::selected::physical::SelectedPhysicalPlan;
use crate::exec::selected::provenance::SelectedRootProvenance;
use crate::exec::selected::run::SelectedExecutableRunRoot;
use crate::exec::selected::SelectedRootConstructionError;
use crate::{ir, physical};

/// Selected repeat control-flow payload.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedRepeatPlan {
    /// Body executed per iteration.
    pub body: Box<SelectedExecutableRunRoot>,
    /// Early stop condition.
    pub stop: ir::RepeatStopPlan,
    /// Emit behavior.
    pub emit: ir::RepeatEmitPlan,
    /// Positive maximum depth.
    pub max_depth: NonZeroUsize,
}

/// Selected root repeat.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedRootRepeat {
    /// Selected root repeat implementation.
    alternative: SelectedPhysicalPlan,
    /// How this selected root was produced.
    provenance: SelectedRootProvenance,
    /// Selected repeat input.
    input: Box<SelectedExecutableRunRoot>,
    /// Selected repeat payload.
    plan: SelectedRepeatPlan,
}

impl SelectedRootRepeat {
    /// Build a selected root repeat.
    pub fn new(
        alternative: SelectedPhysicalPlan,
        provenance: SelectedRootProvenance,
        input: Box<SelectedExecutableRunRoot>,
        plan: SelectedRepeatPlan,
    ) -> Result<Self, SelectedRootConstructionError> {
        if !matches!(
            alternative.expr(),
            physical::PhysicalExpr::Control(physical::PhysicalControlOp::Repeat)
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

    /// Selected root repeat implementation.
    pub const fn alternative(&self) -> &SelectedPhysicalPlan {
        &self.alternative
    }

    /// How this selected root was produced.
    pub const fn provenance(&self) -> &SelectedRootProvenance {
        &self.provenance
    }

    /// Selected repeat input.
    pub const fn input(&self) -> &SelectedExecutableRunRoot {
        &self.input
    }

    /// Selected repeat payload.
    pub const fn plan(&self) -> &SelectedRepeatPlan {
        &self.plan
    }

    pub(in crate::exec::selected) fn into_parts(
        self,
    ) -> (
        SelectedPhysicalPlan,
        SelectedRootProvenance,
        Box<SelectedExecutableRunRoot>,
        SelectedRepeatPlan,
    ) {
        (self.alternative, self.provenance, self.input, self.plan)
    }
}
