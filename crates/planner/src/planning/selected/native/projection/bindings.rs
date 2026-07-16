//! Binding projection payloads.

use helix_ast::projection::{BindingProjection, BindingTarget, BindingValueRef};

use super::super::names;
use crate::{error, ir};

/// Lower `project_bindings(...)` items into a non-empty duplicate-free list.
pub(in crate::planning::selected::native) fn binding_projection_items(
    projections: &[BindingProjection],
) -> Result<ir::BindingProjectionItems, error::PlannerError> {
    let projections = projections
        .iter()
        .map(binding_projection_item)
        .collect::<Result<Vec<_>, _>>()?;
    let projections = ir::AtLeast::<_, 1>::try_from_vec(projections).ok_or(
        error::PlannerError::InvalidProjectionArity {
            op: error::ProjectionOp::ProjectBindings,
            min: 1,
            actual: 0,
        },
    )?;
    ir::BindingProjectionItems::new(projections).map_err(|err| match err {
        ir::ProjectionItemsError::DuplicateAlias { alias } => {
            error::PlannerError::DuplicateProjectionAlias { alias }
        }
    })
}

fn binding_projection_item(
    projection: &BindingProjection,
) -> Result<ir::BindingProjectionPlan, error::PlannerError> {
    match projection {
        BindingProjection::Property {
            target,
            source,
            alias,
        } => Ok(ir::BindingProjectionPlan::Property {
            target: binding_target(target)?,
            source: names::non_empty(source.as_str(), ir::NameField::Property)?,
            alias: names::non_empty(alias.as_str(), ir::NameField::Alias)?,
        }),
        BindingProjection::Coalesce { refs, alias } => Ok(ir::BindingProjectionPlan::Coalesce {
            refs: binding_value_refs(refs)?,
            alias: names::non_empty(alias.as_str(), ir::NameField::Alias)?,
        }),
    }
}

fn binding_target(target: &BindingTarget) -> Result<ir::BindingTargetPlan, error::PlannerError> {
    match target {
        BindingTarget::Current => Ok(ir::BindingTargetPlan::Current),
        BindingTarget::Binding(name) => names::non_empty(name.as_str(), ir::NameField::Binding)
            .map(ir::BindingTargetPlan::Binding),
    }
}

fn binding_value_refs(
    refs: &[BindingValueRef],
) -> Result<ir::AtLeast<ir::BindingValueRefPlan, 1>, error::PlannerError> {
    let refs = refs
        .iter()
        .map(|value_ref| {
            Ok(ir::BindingValueRefPlan {
                target: binding_target(&value_ref.target)?,
                source: names::non_empty(value_ref.source.as_str(), ir::NameField::Property)?,
            })
        })
        .collect::<Result<Vec<_>, error::PlannerError>>()?;
    ir::AtLeast::<_, 1>::try_from_vec(refs).ok_or(error::PlannerError::InvalidProjectionArity {
        op: error::ProjectionOp::Coalesce,
        min: 1,
        actual: 0,
    })
}
