//! Native edge access source lowering.

use helix_ast::graph::EdgeRef;

use super::super::names;
use super::ids::{element_ids, NativeElementIds};
use crate::{catalog, error, ir};

pub(super) fn edge_access_plan(
    reference: &EdgeRef,
) -> Result<ir::EdgeAccessPlan, error::PlannerError> {
    Ok(match reference {
        EdgeRef::All => ir::EdgeAccessPlan::AllScan,
        EdgeRef::Ids(ids) => match element_ids(ids, catalog::ElementKind::Edge)? {
            NativeElementIds::NonEmpty(ids) => ir::EdgeAccessPlan::PointIds { ids },
            NativeElementIds::EmptyReference => ir::EdgeAccessPlan::Empty,
        },
        EdgeRef::Var(variable) => ir::EdgeAccessPlan::FromVar {
            variable: names::non_empty(variable.as_str(), ir::NameField::Variable)?,
        },
        EdgeRef::Param(param) => ir::EdgeAccessPlan::FromParam {
            param: names::non_empty(param.as_str(), ir::NameField::Param)?,
        },
    })
}
