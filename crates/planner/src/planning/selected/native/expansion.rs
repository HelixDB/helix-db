//! Native graph-expansion payload validation.

use super::names;
use crate::{error, ir};

pub(super) fn plan(
    direction: ir::ExpandDirection,
    output: ir::ExpandOutput,
    label: Option<&str>,
) -> Result<ir::ExpandPlan, error::PlannerError> {
    Ok(ir::ExpandPlan {
        direction,
        output,
        label: label_plan(label)?,
    })
}

fn label_plan(label: Option<&str>) -> Result<ir::ExpandLabelPlan, error::PlannerError> {
    match label {
        Some(label) => {
            names::non_empty(label, ir::NameField::Label).map(ir::ExpandLabelPlan::Label)
        }
        None => Ok(ir::ExpandLabelPlan::Any),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expansion_payloads_preserve_direction_output_and_label() {
        let out_edges = plan(
            ir::ExpandDirection::Out,
            ir::ExpandOutput::Edges,
            Some("LIKES"),
        )
        .unwrap();
        assert!(matches!(out_edges.direction, ir::ExpandDirection::Out));
        assert!(matches!(out_edges.output, ir::ExpandOutput::Edges));
        assert!(matches!(
            out_edges.label,
            ir::ExpandLabelPlan::Label(label) if label.as_ref() == "LIKES"
        ));

        let unlabeled = plan(ir::ExpandDirection::Both, ir::ExpandOutput::Nodes, None).unwrap();
        assert!(matches!(unlabeled.label, ir::ExpandLabelPlan::Any));
    }

    #[test]
    fn expansion_payloads_validate_labels() {
        assert!(matches!(
            plan(ir::ExpandDirection::Out, ir::ExpandOutput::Nodes, Some("")),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Label
            })
        ));
    }
}
