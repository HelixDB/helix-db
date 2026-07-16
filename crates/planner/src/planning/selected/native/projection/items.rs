//! Row projection payloads.

use helix_ast::projection::Projection;

use super::super::names;
use crate::{error, ir};

/// Lower `project(...)` items into a non-empty duplicate-free projection list.
pub(in crate::planning::selected::native) fn projection_items(
    projections: &[Projection],
) -> Result<ir::ProjectionItems, error::PlannerError> {
    let projections = projections
        .iter()
        .map(projection_item)
        .collect::<Result<Vec<_>, _>>()?;
    let projections = ir::AtLeast::<_, 1>::try_from_vec(projections).ok_or(
        error::PlannerError::InvalidProjectionArity {
            op: error::ProjectionOp::Project,
            min: 1,
            actual: 0,
        },
    )?;
    ir::ProjectionItems::new(projections).map_err(|err| match err {
        ir::ProjectionItemsError::DuplicateAlias { alias } => {
            error::PlannerError::DuplicateProjectionAlias { alias }
        }
    })
}

fn projection_item(projection: &Projection) -> Result<ir::ProjectionItem, error::PlannerError> {
    match projection {
        Projection::Property(projection) => Ok(ir::ProjectionItem::Property {
            source: names::non_empty(projection.source.as_str(), ir::NameField::Property)?,
            alias: names::non_empty(projection.alias.as_str(), ir::NameField::Alias)?,
        }),
        Projection::Expr(projection) => Ok(ir::ProjectionItem::Expr {
            alias: names::non_empty(projection.alias.as_str(), ir::NameField::Alias)?,
            expr: ir::ExprPlan::new(projection.expr.clone())?,
        }),
    }
}
