//! Native source-injected variable root lowering.
//!
//! `Inject { input: None, .. }` is a root stream source, not an access source
//! and not a stream variable wrapper. This module keeps that contract explicit
//! so terminal and pipeline lowering can consume it as `RootStream::VariableSource`.

use helix_ast::traversal::AstNode;

use super::names;
use crate::{error, ir, logical};

/// Native variable-source recognition result.
pub(super) enum NativeVariableSourceRoot {
    /// The AST root is a source-injected variable.
    Source(logical::VariableSource),
    /// The AST root is an input-consuming variable pipeline wrapper.
    InputConsuming,
    /// The AST root is not a variable-source family root.
    NotVariableSource,
}

pub(super) fn native_variable_source_from_ast(
    root: &AstNode,
) -> Result<NativeVariableSourceRoot, error::PlannerError> {
    match root {
        AstNode::Inject {
            input: None,
            variable,
        } => Ok(NativeVariableSourceRoot::Source(
            logical::VariableSource::new(names::non_empty(variable, ir::NameField::Variable)?),
        )),
        AstNode::Inject { input: Some(_), .. } => Ok(NativeVariableSourceRoot::InputConsuming),
        _ => Ok(NativeVariableSourceRoot::NotVariableSource),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::graph::NodeRef;

    #[test]
    fn variable_source_payloads_validate_names() {
        let source = native_variable_source_from_ast(&AstNode::Inject {
            input: None,
            variable: "seed".to_owned(),
        })
        .unwrap();
        let NativeVariableSourceRoot::Source(source) = source else {
            panic!("source inject is native");
        };
        assert_eq!(source.variable().as_ref(), "seed");

        let empty = native_variable_source_from_ast(&AstNode::Inject {
            input: None,
            variable: String::new(),
        });
        assert!(matches!(
            empty,
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Variable
            })
        ));
    }

    #[test]
    fn variable_source_rejects_input_rooted_inject() {
        let source = native_variable_source_from_ast(&AstNode::Inject {
            input: Some(Box::new(AstNode::Nodes {
                reference: NodeRef::All,
            })),
            variable: "seed".to_owned(),
        })
        .unwrap();
        assert!(matches!(source, NativeVariableSourceRoot::InputConsuming));
        assert!(matches!(
            native_variable_source_from_ast(&AstNode::Context).unwrap(),
            NativeVariableSourceRoot::NotVariableSource
        ));
    }
}
