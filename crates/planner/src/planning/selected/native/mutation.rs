//! Native source mutation root lowering.
//!
//! This module only lowers mutation roots whose AST shape is source-only. Input
//! consuming mutations are scope-dependent because their child traversal must be
//! recursively lowered, so `native::scoped::input_mutation` owns that contract.

use helix_ast::traversal::AstNode;

use super::names;
use crate::planning::mutation as mutation_contracts;
use crate::{error, ir, logical};

/// Native source-mutation recognition result.
pub(super) enum NativeMutationRoot {
    /// The AST root is a validated source-only mutation.
    Source(logical::RootMutation),
    /// The AST root is a mutation family that requires a selected input child.
    InputConsuming,
    /// The AST root is not a mutation.
    NotMutation,
}

pub(super) fn native_mutation_from_ast(
    root: &AstNode,
) -> Result<NativeMutationRoot, error::PlannerError> {
    match root {
        AstNode::AddN {
            input: None,
            label,
            properties,
        } => Ok(NativeMutationRoot::Source(logical::RootMutation::new(
            ir::MutationPlan::AddNode {
                input: ir::MutationInput::Source,
                label: names::non_empty(label, ir::NameField::Label)?,
                properties: mutation_contracts::property_assignments(properties)?,
            },
        ))),
        AstNode::DropEdgeById { input: None, edges } => Ok(NativeMutationRoot::Source(
            logical::RootMutation::new(ir::MutationPlan::DropEdgeById {
                input: ir::MutationInput::Source,
                edges: mutation_contracts::edge_target(edges)?,
            }),
        )),
        AstNode::AddN { input: Some(_), .. }
        | AstNode::AddE { .. }
        | AstNode::SetProperty { .. }
        | AstNode::RemoveProperty { .. }
        | AstNode::Drop { .. }
        | AstNode::DropEdge { .. }
        | AstNode::DropEdgeLabeled { .. }
        | AstNode::DropEdgeById { input: Some(_), .. } => Ok(NativeMutationRoot::InputConsuming),
        _ => Ok(NativeMutationRoot::NotMutation),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::graph::{EdgeRef, NodeRef};
    use helix_ast::value::PropertyInput;

    #[test]
    fn source_mutations_lower_add_node_and_drop_edge_by_id() {
        let add = native_mutation_from_ast(&AstNode::AddN {
            input: None,
            label: "Person".to_owned(),
            properties: vec![("name".to_owned(), PropertyInput::from("alice"))],
        })
        .unwrap();
        let NativeMutationRoot::Source(add) = add else {
            panic!("source add node is native");
        };
        assert!(matches!(
            add.plan(),
            ir::MutationPlan::AddNode {
                input: ir::MutationInput::Source,
                label,
                properties,
            } if label.as_ref() == "Person" && properties.as_ref().len() == 1
        ));

        let drop = native_mutation_from_ast(&AstNode::DropEdgeById {
            input: None,
            edges: EdgeRef::Ids(vec![7]),
        })
        .unwrap();
        let NativeMutationRoot::Source(drop) = drop else {
            panic!("source drop edge by id is native");
        };
        assert!(matches!(
            drop.plan(),
            ir::MutationPlan::DropEdgeById {
                input: ir::MutationInput::Source,
                edges: ir::EdgeTargetPlan::PointIds { .. },
            }
        ));
    }

    #[test]
    fn source_mutations_validate_payloads_and_reject_input_consuming_shapes() {
        let invalid_label = native_mutation_from_ast(&AstNode::AddN {
            input: None,
            label: String::new(),
            properties: Vec::new(),
        });
        assert!(matches!(
            invalid_label,
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Label
            })
        ));

        let input_add = native_mutation_from_ast(&AstNode::AddN {
            input: Some(Box::new(AstNode::Nodes {
                reference: NodeRef::All,
            })),
            label: "Person".to_owned(),
            properties: Vec::new(),
        })
        .unwrap();
        assert!(matches!(input_add, NativeMutationRoot::InputConsuming));

        let input_drop = native_mutation_from_ast(&AstNode::DropEdgeById {
            input: Some(Box::new(AstNode::Nodes {
                reference: NodeRef::All,
            })),
            edges: EdgeRef::Ids(vec![7]),
        })
        .unwrap();
        assert!(matches!(input_drop, NativeMutationRoot::InputConsuming));

        assert!(matches!(
            native_mutation_from_ast(&AstNode::Context).unwrap(),
            NativeMutationRoot::NotMutation
        ));
    }
}
