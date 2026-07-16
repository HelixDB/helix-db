//! Logical-to-physical matching contracts shared by selected IR and lowering.
//!
//! These helpers are pure shape checks. They do not allocate executable steps,
//! derive costs, or inspect runtime state.

use super::root_stream::SelectedRootTerminal;
use crate::{logical, physical};

pub(in crate::exec::selected) fn selected_stream_pipeline_ops_match(
    logical_ops: &[logical::StreamPipelineOp],
    physical_ops: &[physical::PhysicalPipelineOp],
) -> bool {
    logical_ops.len() == physical_ops.len()
        && logical_ops
            .iter()
            .zip(physical_ops)
            .all(|(logical, physical)| selected_stream_pipeline_op_matches(logical, physical))
}

fn selected_stream_pipeline_op_matches(
    logical: &logical::StreamPipelineOp,
    physical: &physical::PhysicalPipelineOp,
) -> bool {
    match (logical, physical) {
        (
            logical::StreamPipelineOp::Filter { .. },
            physical::PhysicalPipelineOp::ResidualFilter,
        )
        | (
            logical::StreamPipelineOp::Limit { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
        )
        | (
            logical::StreamPipelineOp::Skip { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Skip),
        )
        | (
            logical::StreamPipelineOp::Range { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
        )
        | (logical::StreamPipelineOp::Order { .. }, physical::PhysicalPipelineOp::Sort)
        | (
            logical::StreamPipelineOp::Distinct,
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
        )
        | (
            logical::StreamPipelineOp::Expand { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Expand),
        )
        | (
            logical::StreamPipelineOp::Variable { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
        )
        | (
            logical::StreamPipelineOp::VariableWrite { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
        ) => true,
        (
            logical::StreamPipelineOp::Window { window },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
        ) => window.end().is_some(),
        (
            logical::StreamPipelineOp::Window { window },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Skip),
        ) => window.end().is_none() && window.start() > 0,
        _ => false,
    }
}

pub(in crate::exec::selected) fn selected_root_terminal_op_matches(
    logical: &SelectedRootTerminal,
    physical: &physical::PhysicalPipelineOp,
) -> bool {
    matches!(
        (logical, physical),
        (
            SelectedRootTerminal::Project { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project),
        ) | (
            SelectedRootTerminal::Aggregate { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Aggregate),
        ) | (
            SelectedRootTerminal::Reserved { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Reserved),
        ) | (
            SelectedRootTerminal::VariableWrite { .. },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_pipeline_matching_requires_one_physical_op_per_logical_op() {
        let logical = [logical::StreamPipelineOp::Window {
            window: logical::AccessWindowRange::new(1, Some(3)).expect("window is valid"),
        }];

        assert!(selected_stream_pipeline_ops_match(
            &logical,
            &[physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Range,
            )],
        ));
        assert!(!selected_stream_pipeline_ops_match(
            &logical,
            &[
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
            ],
        ));
    }

    #[test]
    fn root_terminal_matching_requires_terminal_family() {
        let terminal = SelectedRootTerminal::Project {
            input: crate::exec::SelectedRootStreamInput::VariableSource(
                logical::VariableSource::new(crate::ir::NonEmptyString::from_static("seed")),
            ),
            projection: crate::ir::ProjectionPlan::Count,
        };

        assert!(selected_root_terminal_op_matches(
            &terminal,
            &physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project)
        ));
        assert!(!selected_root_terminal_op_matches(
            &terminal,
            &physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Aggregate)
        ));
    }
}
