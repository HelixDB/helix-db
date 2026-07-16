//! Selected root-stream pipeline contract.

use super::input::SelectedRootStreamInput;
use super::prefix::SelectedRootStreamInputPrefix;
use crate::exec::selected::physical::SelectedPhysicalPlan;
use crate::exec::selected::provenance::SelectedRootProvenance;
use crate::exec::selected::{matching, SelectedRootConstructionError};
use crate::{ir, logical, physical};

/// Selected root-stream pipeline whose input is itself a selected run root.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedRootPipeline {
    /// Selected root-pipeline implementation.
    alternative: SelectedPhysicalPlan,
    /// How this selected root was produced.
    provenance: SelectedRootProvenance,
    /// Selected stream input.
    input: SelectedRootStreamInput,
    /// Physical operators that belong to the selected stream input.
    input_prefix: SelectedRootStreamInputPrefix,
    /// Stream operators above the selected input.
    ops: ir::AtLeast<logical::StreamPipelineOp, 1>,
}

impl SelectedRootPipeline {
    /// Build a selected root-stream pipeline.
    pub fn new(
        alternative: SelectedPhysicalPlan,
        provenance: SelectedRootProvenance,
        input: SelectedRootStreamInput,
        ops: ir::AtLeast<logical::StreamPipelineOp, 1>,
    ) -> Result<Self, SelectedRootConstructionError> {
        let physical::PhysicalExpr::Pipeline(physical_pipeline) = alternative.expr() else {
            return Err(SelectedRootConstructionError::IncompatiblePhysicalShape);
        };
        let Some(prefix_len) = physical_pipeline.ops().len().checked_sub(ops.len()) else {
            return Err(SelectedRootConstructionError::RootPipelineLogicalSuffixTooLong);
        };
        let (prefix, suffix) = physical_pipeline.ops().split_at(prefix_len);
        if !matching::selected_stream_pipeline_ops_match(ops.as_ref(), suffix) {
            return Err(SelectedRootConstructionError::RootPipelinePhysicalSuffixMismatch);
        }
        let input_prefix = SelectedRootStreamInputPrefix::new(&input, prefix)?;

        Ok(Self {
            alternative,
            provenance,
            input,
            input_prefix,
            ops,
        })
    }

    /// Selected root-pipeline implementation.
    pub const fn alternative(&self) -> &SelectedPhysicalPlan {
        &self.alternative
    }

    /// How this selected root was produced.
    pub const fn provenance(&self) -> &SelectedRootProvenance {
        &self.provenance
    }

    /// Selected stream input.
    pub const fn input(&self) -> &SelectedRootStreamInput {
        &self.input
    }

    /// Physical operators localized to the selected stream input.
    #[cfg(test)]
    pub(in crate::exec::selected) const fn input_prefix(&self) -> &SelectedRootStreamInputPrefix {
        &self.input_prefix
    }

    /// Stream operators above the selected input.
    pub const fn ops(&self) -> &ir::AtLeast<logical::StreamPipelineOp, 1> {
        &self.ops
    }

    pub(in crate::exec::selected) fn into_parts(
        self,
    ) -> (
        SelectedPhysicalPlan,
        SelectedRootProvenance,
        SelectedRootStreamInput,
        SelectedRootStreamInputPrefix,
        ir::AtLeast<logical::StreamPipelineOp, 1>,
    ) {
        (
            self.alternative,
            self.provenance,
            self.input,
            self.input_prefix,
            self.ops,
        )
    }
}
