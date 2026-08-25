//! Scalar terminal projection contracts.

use super::super::values::scalar_items;
use super::*;

pub(in crate::execution::interpreter::stream::projection) fn project_scalar_items(
    value: ExecutionValue,
    projection: &ir::ProjectionPlan,
) -> Result<ExecutionValue> {
    let values = scalar_items(value);
    match projection {
        ir::ProjectionPlan::Exists => Ok(ExecutionValue::Bool(!values.is_empty())),
        ir::ProjectionPlan::Id => Ok(ExecutionValue::Scalars(
            values
                .into_iter()
                .filter(|value| {
                    matches!(
                        value,
                        ExecutionScalar::NodeId(_) | ExecutionScalar::EdgeId(_)
                    )
                })
                .collect(),
        )),
        ir::ProjectionPlan::Values(_)
        | ir::ProjectionPlan::ValueMap(_)
        | ir::ProjectionPlan::Project(_)
        | ir::ProjectionPlan::ProjectBindings { .. }
        | ir::ProjectionPlan::Label
        | ir::ProjectionPlan::EdgeProperties => Err(HelixDbError::Query(format!(
            "project {projection:?} expected element stream input, got scalar terminal input"
        ))),
    }
}
