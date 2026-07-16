//! Scoped input-consuming mutation AST contracts.
//!
//! Source-only mutations are handled by `native::mutation`. This module owns
//! mutations that need a recursively planned logical input, keeping mutation
//! children inside memo groups instead of executable compatibility payloads.

mod edge;
mod element;
mod node;
mod property;
mod source;
#[cfg(test)]
mod tests;

use helix_ast::traversal::AstNode;

use super::super::scope::NativeAstScope;
use crate::{context, error, ir, logical};

/// Scoped input-mutation recognition result.
pub(super) enum InputMutationRoot {
    /// The AST root is an input-consuming mutation with recursively lowered input.
    Mutation(logical::RootMutation),
    /// The AST root is a source-only mutation handled by unscoped root lowering.
    SourceOnly,
    /// The AST root is not a mutation family root.
    NotMutation,
}

pub(super) enum InputMutationFamilyRoot {
    Mutation(Box<ir::MutationPlan<logical::LogicalExpr>>),
    NotThisFamily,
}

pub(super) fn input_mutation_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<InputMutationRoot, error::PlannerError> {
    if source::is_source_only_mutation(root) {
        return Ok(InputMutationRoot::SourceOnly);
    }

    let family = match root {
        AstNode::AddN { input: Some(_), .. } => node::node_mutation_from_ast(ctx, root, scope)?,
        AstNode::AddE { .. }
        | AstNode::DropEdge { .. }
        | AstNode::DropEdgeLabeled { .. }
        | AstNode::DropEdgeById { input: Some(_), .. } => {
            edge::edge_mutation_from_ast(ctx, root, scope)?
        }
        AstNode::SetProperty { .. } | AstNode::RemoveProperty { .. } => {
            property::property_mutation_from_ast(ctx, root, scope)?
        }
        AstNode::Drop { .. } => element::element_mutation_from_ast(ctx, root, scope)?,
        _ => InputMutationFamilyRoot::NotThisFamily,
    };

    Ok(match family {
        InputMutationFamilyRoot::Mutation(plan) => {
            InputMutationRoot::Mutation(logical::RootMutation::new(*plan))
        }
        InputMutationFamilyRoot::NotThisFamily => InputMutationRoot::NotMutation,
    })
}
