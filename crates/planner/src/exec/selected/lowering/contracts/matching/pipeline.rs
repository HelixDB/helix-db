//! Stream-pipeline logical-to-physical matching contracts.

use super::super::*;

pub(in crate::exec::selected::lowering) use crate::exec::selected::matching::selected_stream_pipeline_ops_match;

pub(in crate::exec::selected::lowering) fn selected_pipeline_from_ops(
    ops: &[physical::PhysicalPipelineOp],
) -> Result<physical::PhysicalPipeline, ExecPlanError> {
    let ops = ir::AtLeast::<_, 1>::try_from_vec(ops.to_vec()).ok_or_else(|| {
        unsupported_selected_alternative(rejection::Reason::AccessStreamPipelinePrefixEmpty)
    })?;
    Ok(physical::PhysicalPipeline::new(ops))
}
