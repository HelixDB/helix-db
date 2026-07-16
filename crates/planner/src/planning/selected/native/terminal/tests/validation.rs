use super::support;
use crate::{error, ir};
use helix_ast::traversal::AstNode;

#[test]
fn native_terminals_validate_payloads() {
    let empty_values = support::lower(&AstNode::Values {
        input: support::node_source(),
        properties: Vec::new(),
    });
    assert!(matches!(
        empty_values,
        Err(error::PlannerError::InvalidProjectionArity {
            op: error::ProjectionOp::Values,
            min: 1,
            actual: 0
        })
    ));

    let empty_group = support::lower(&AstNode::Group {
        input: support::node_source(),
        property: String::new(),
    });
    assert!(matches!(
        empty_group,
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Property
        })
    ));

    let empty_sack = support::lower(&AstNode::SackSet {
        input: support::node_source(),
        property: String::new(),
    });
    assert!(matches!(
        empty_sack,
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Property
        })
    ));
}
