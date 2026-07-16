//! Selected access-window executable lowering.

use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_access_window(
        &mut self,
        window: &logical::AccessWindow,
        pipeline: &physical::PhysicalPipeline,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let parts = match selected_access_pipeline_parts(window.access(), pipeline) {
            SelectedAccessPipelineMatch::Matched(parts) => parts,
            SelectedAccessPipelineMatch::NotMatched(_) => {
                return Err(unsupported_selected_alternative(
                    rejection::Reason::AccessWindowSourceMismatch,
                ));
            }
        };
        let (access, ops) = parts.into_parts();
        if !selected_access_window_pipeline_matches(window.window(), ops) {
            return Err(unsupported_selected_alternative(
                rejection::Reason::AccessWindowPhysicalSuffixMismatch,
            ));
        }
        if ops.is_empty() {
            return self.push_selected_access_path(
                window.access(),
                access,
                dependencies,
                output,
                condition,
            );
        }

        let read_plan = WindowAccessReadPlan::for_window(access, window.window());
        let bounded_range = window.window().bounded_stream_range();
        if read_plan.suffix() == WindowSuffix::ElidedByReadLimit
            && matches!(
                ops,
                [physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Range
                )]
            )
        {
            return self.push_selected_access_path_with_read_limit(
                window.access(),
                read_plan.access(),
                read_plan.read_limit(),
                dependencies,
                output,
                condition,
            );
        }
        let input_id = self.push_selected_access_path_with_read_limit(
            window.access(),
            read_plan.access(),
            read_plan.read_limit(),
            dependencies,
            ir::BatchOutputPlan::Discard,
            condition.clone(),
        )?;
        match ops {
            [physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range)] => {
                let Some(range) = bounded_range else {
                    return Err(unsupported_selected_alternative(
                        rejection::Reason::AccessWindowRangeSuffixMissingBoundedWindow,
                    ));
                };
                self.push_step(StepDraft {
                    dependencies: vec![input_id],
                    output,
                    condition,
                    op: ExecOp::Range {
                        range: ir::StreamRangePlan::Literal(range),
                    },
                    schedule: ExecSchedule::Pipeline,
                    delivered: range_delivered_properties(
                        selected_access_path_delivered_properties(window.access()),
                        Some((range.start(), range.end())),
                    ),
                    cost: self
                        .profile
                        .stream_operator(selected_access_path_estimated_rows(
                            window.access(),
                            self.profile,
                        )),
                })
            }
            [physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Skip)] => self
                .push_step(StepDraft {
                    dependencies: vec![input_id],
                    output,
                    condition,
                    op: ExecOp::Skip {
                        count: ir::StreamBoundPlan::Literal(window.window().start()),
                    },
                    schedule: ExecSchedule::Pipeline,
                    delivered: skip_delivered_properties(
                        selected_access_path_delivered_properties(window.access()),
                        Some(window.window().start()),
                    ),
                    cost: self
                        .profile
                        .stream_operator(selected_access_path_estimated_rows(
                            window.access(),
                            self.profile,
                        )),
                }),
            _ => Err(unsupported_selected_alternative(
                rejection::Reason::AccessWindowPhysicalSuffixMismatch,
            )),
        }
    }
}
