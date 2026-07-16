use helix_ast::graph::{EdgeRef, NodeRef};

use crate::{catalog, error, ir};

use super::shared;

/// Convert an AST node target into the mutation IR contract.
///
/// Empty concrete ID lists become [`ir::NodeTargetPlan::Empty`]; non-empty ID
/// lists reject duplicates; variable and parameter targets reject empty names.
///
/// ```
/// use helix_ast::graph::NodeRef;
/// use helix_planner::{ir, planning::mutation};
///
/// assert!(matches!(
///     mutation::node_target(&NodeRef::Ids(vec![7])).unwrap(),
///     ir::NodeTargetPlan::PointIds { .. }
/// ));
/// assert_eq!(mutation::node_target(&NodeRef::Ids(Vec::new())).unwrap(), ir::NodeTargetPlan::Empty);
/// ```
pub fn node_target(reference: &NodeRef) -> Result<ir::NodeTargetPlan, error::PlannerError> {
    match reference {
        NodeRef::All => Ok(ir::NodeTargetPlan::All),
        NodeRef::Ids(ids) => match shared::element_ids(ids, catalog::ElementKind::Node)? {
            shared::MutationElementIds::NonEmpty(ids) => Ok(ir::NodeTargetPlan::PointIds { ids }),
            shared::MutationElementIds::EmptyReference => Ok(ir::NodeTargetPlan::Empty),
        },
        NodeRef::Var(variable) => Ok(ir::NodeTargetPlan::FromVar {
            variable: shared::variable_name(variable)?,
        }),
        NodeRef::Param(param) => Ok(ir::NodeTargetPlan::FromParam {
            param: shared::param_name(param)?,
        }),
    }
}

/// Convert an AST edge target into the mutation IR contract.
///
/// Unlike node targets, `EdgeRef::All` is not a valid mutation target and is
/// rejected instead of becoming an executable all-edges delete.
///
/// ```
/// use helix_ast::graph::EdgeRef;
/// use helix_planner::{ir, planning::mutation};
///
/// assert!(matches!(
///     mutation::edge_target(&EdgeRef::Ids(vec![7])).unwrap(),
///     ir::EdgeTargetPlan::PointIds { .. }
/// ));
/// assert_eq!(mutation::edge_target(&EdgeRef::Ids(Vec::new())).unwrap(), ir::EdgeTargetPlan::Empty);
/// ```
pub fn edge_target(reference: &EdgeRef) -> Result<ir::EdgeTargetPlan, error::PlannerError> {
    match reference {
        EdgeRef::All => Err(error::PlannerError::UnsupportedEdgeAllTarget),
        EdgeRef::Ids(ids) => match shared::element_ids(ids, catalog::ElementKind::Edge)? {
            shared::MutationElementIds::NonEmpty(ids) => Ok(ir::EdgeTargetPlan::PointIds { ids }),
            shared::MutationElementIds::EmptyReference => Ok(ir::EdgeTargetPlan::Empty),
        },
        EdgeRef::Var(variable) => Ok(ir::EdgeTargetPlan::FromVar {
            variable: shared::variable_name(variable)?,
        }),
        EdgeRef::Param(param) => Ok(ir::EdgeTargetPlan::FromParam {
            param: shared::param_name(param)?,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_target_validates_all_empty_ids_variables_and_params() {
        assert_eq!(node_target(&NodeRef::All).unwrap(), ir::NodeTargetPlan::All);
        assert_eq!(
            node_target(&NodeRef::Ids(Vec::new())).unwrap(),
            ir::NodeTargetPlan::Empty
        );
        assert!(matches!(
            node_target(&NodeRef::Ids(vec![7, 9])).unwrap(),
            ir::NodeTargetPlan::PointIds { .. }
        ));
        assert!(matches!(
            node_target(&NodeRef::Var("nodes".to_owned())).unwrap(),
            ir::NodeTargetPlan::FromVar { variable } if variable.as_ref() == "nodes"
        ));
        assert!(matches!(
            node_target(&NodeRef::Param("nodes".to_owned())).unwrap(),
            ir::NodeTargetPlan::FromParam { param } if param.as_ref() == "nodes"
        ));
    }

    #[test]
    fn node_target_rejects_duplicate_ids_and_empty_runtime_names() {
        let duplicate = node_target(&NodeRef::Ids(vec![7, 7])).unwrap_err();
        assert!(matches!(
            duplicate,
            error::PlannerError::DuplicateElementId {
                element: catalog::ElementKind::Node,
                id: 7,
            }
        ));

        let empty_variable = node_target(&NodeRef::Var(String::new())).unwrap_err();
        assert!(matches!(
            empty_variable,
            error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Variable
            }
        ));
    }

    #[test]
    fn edge_target_rejects_all_and_validates_empty_ids_variables_and_params() {
        assert!(matches!(
            edge_target(&EdgeRef::All),
            Err(error::PlannerError::UnsupportedEdgeAllTarget)
        ));
        assert_eq!(
            edge_target(&EdgeRef::Ids(Vec::new())).unwrap(),
            ir::EdgeTargetPlan::Empty
        );
        assert!(matches!(
            edge_target(&EdgeRef::Ids(vec![7, 9])).unwrap(),
            ir::EdgeTargetPlan::PointIds { .. }
        ));
        assert!(matches!(
            edge_target(&EdgeRef::Var("edges".to_owned())).unwrap(),
            ir::EdgeTargetPlan::FromVar { variable } if variable.as_ref() == "edges"
        ));
        assert!(matches!(
            edge_target(&EdgeRef::Param("edges".to_owned())).unwrap(),
            ir::EdgeTargetPlan::FromParam { param } if param.as_ref() == "edges"
        ));
    }

    #[test]
    fn edge_target_rejects_duplicate_ids_and_empty_runtime_names() {
        let duplicate = edge_target(&EdgeRef::Ids(vec![7, 7])).unwrap_err();
        assert!(matches!(
            duplicate,
            error::PlannerError::DuplicateElementId {
                element: catalog::ElementKind::Edge,
                id: 7,
            }
        ));

        let empty_param = edge_target(&EdgeRef::Param(String::new())).unwrap_err();
        assert!(matches!(
            empty_param,
            error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Param
            }
        ));
    }
}
