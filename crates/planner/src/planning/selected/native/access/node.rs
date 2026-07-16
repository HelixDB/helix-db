//! Native node access source lowering.

use helix_ast::graph::NodeRef;

use super::super::names;
use super::ids::{element_ids, NativeElementIds};
use crate::{catalog, error, ir};

pub(super) fn node_access_plan(
    reference: &NodeRef,
) -> Result<ir::NodeAccessPlan, error::PlannerError> {
    Ok(match reference {
        NodeRef::All => ir::NodeAccessPlan::AllScan,
        NodeRef::Ids(ids) => match element_ids(ids, catalog::ElementKind::Node)? {
            NativeElementIds::NonEmpty(ids) => ir::NodeAccessPlan::PointIds { ids },
            NativeElementIds::EmptyReference => ir::NodeAccessPlan::Empty,
        },
        NodeRef::Var(variable) => ir::NodeAccessPlan::FromVar {
            variable: names::non_empty(variable.as_str(), ir::NameField::Variable)?,
        },
        NodeRef::Param(param) => ir::NodeAccessPlan::FromParam {
            param: names::non_empty(param.as_str(), ir::NameField::Param)?,
        },
    })
}
