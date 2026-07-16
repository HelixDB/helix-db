//! Selected root-stream terminal contract.

use super::input::SelectedRootStreamInput;
use super::prefix::SelectedRootStreamInputPrefix;
use crate::exec::selected::physical::SelectedPhysicalPlan;
use crate::exec::selected::provenance::SelectedRootProvenance;
use crate::exec::selected::{matching, SelectedRootConstructionError};
use crate::{ir, logical, physical};

/// Terminal stream operation over a selected control-flow root stream.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectedRootTerminal {
    /// Projection terminal.
    Project {
        /// Selected terminal input.
        input: SelectedRootStreamInput,
        /// Projection payload.
        projection: ir::ProjectionPlan,
    },
    /// Aggregation terminal.
    Aggregate {
        /// Selected terminal input.
        input: SelectedRootStreamInput,
        /// Aggregate payload.
        aggregate: ir::AggregatePlan,
    },
    /// Reserved traversal terminal.
    Reserved {
        /// Selected terminal input.
        input: SelectedRootStreamInput,
        /// Reserved payload.
        op: ir::ReservedOp,
    },
    /// State-writing variable terminal.
    VariableWrite {
        /// Selected terminal input.
        input: SelectedRootStreamInput,
        /// Variable write payload.
        op: logical::StreamVariableWriteOp,
    },
}

impl SelectedRootTerminal {
    const fn input(&self) -> &SelectedRootStreamInput {
        match self {
            Self::Project { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Reserved { input, .. }
            | Self::VariableWrite { input, .. } => input,
        }
    }
}

/// Selected terminal pipeline whose input is itself a selected run root.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedRootTerminalPlan {
    /// Selected terminal implementation.
    alternative: SelectedPhysicalPlan,
    /// How this selected root was produced.
    provenance: SelectedRootProvenance,
    /// Physical operators that belong to the selected stream input.
    input_prefix: SelectedRootStreamInputPrefix,
    /// Selected terminal payload.
    plan: SelectedRootTerminal,
}

impl SelectedRootTerminalPlan {
    /// Build a selected root-stream terminal.
    pub fn new(
        alternative: SelectedPhysicalPlan,
        provenance: SelectedRootProvenance,
        plan: SelectedRootTerminal,
    ) -> Result<Self, SelectedRootConstructionError> {
        let physical::PhysicalExpr::Pipeline(physical_pipeline) = alternative.expr() else {
            return Err(SelectedRootConstructionError::IncompatiblePhysicalShape);
        };
        let physical_split = physical_pipeline.terminal_split();
        if !matching::selected_root_terminal_op_matches(&plan, physical_split.terminal()) {
            return Err(SelectedRootConstructionError::RootTerminalPhysicalSuffixMismatch);
        }
        let input_prefix =
            SelectedRootStreamInputPrefix::new(plan.input(), physical_split.prefix())?;

        Ok(Self {
            alternative,
            provenance,
            input_prefix,
            plan,
        })
    }

    /// Selected terminal implementation.
    pub const fn alternative(&self) -> &SelectedPhysicalPlan {
        &self.alternative
    }

    /// How this selected root was produced.
    pub const fn provenance(&self) -> &SelectedRootProvenance {
        &self.provenance
    }

    /// Selected terminal payload.
    pub const fn plan(&self) -> &SelectedRootTerminal {
        &self.plan
    }

    /// Physical operators localized to the selected terminal input.
    #[cfg(test)]
    pub(in crate::exec::selected) const fn input_prefix(&self) -> &SelectedRootStreamInputPrefix {
        &self.input_prefix
    }

    pub(in crate::exec::selected) fn into_parts(
        self,
    ) -> (
        SelectedPhysicalPlan,
        SelectedRootProvenance,
        SelectedRootStreamInputPrefix,
        SelectedRootTerminal,
    ) {
        (
            self.alternative,
            self.provenance,
            self.input_prefix,
            self.plan,
        )
    }
}
