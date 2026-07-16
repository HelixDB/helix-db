//! Input-consuming edge mutation lowering.

use helix_ast::traversal::AstNode;

use super::super::super::{names, scope::NativeAstScope};
use super::super::scoped_required_expr_from_ast;
use super::InputMutationFamilyRoot;
use crate::planning::mutation as mutation_contracts;
use crate::{context, error, ir};

pub(super) fn edge_mutation_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<InputMutationFamilyRoot, error::PlannerError> {
    Ok(match root {
        AstNode::AddE {
            input,
            label,
            to,
            properties,
        } => InputMutationFamilyRoot::Mutation(Box::new(ir::MutationPlan::AddEdge {
            input: Box::new(scoped_required_expr_from_ast(ctx, input, scope)?),
            label: names::non_empty(label, ir::NameField::Label)?,
            to: mutation_contracts::node_target(to)?,
            properties: mutation_contracts::property_assignments(properties)?,
        })),
        AstNode::DropEdge { input, to } => {
            InputMutationFamilyRoot::Mutation(Box::new(ir::MutationPlan::DropEdge {
                input: Box::new(scoped_required_expr_from_ast(ctx, input, scope)?),
                to: mutation_contracts::node_target(to)?,
            }))
        }
        AstNode::DropEdgeLabeled { input, to, label } => {
            InputMutationFamilyRoot::Mutation(Box::new(ir::MutationPlan::DropEdgeLabeled {
                input: Box::new(scoped_required_expr_from_ast(ctx, input, scope)?),
                to: mutation_contracts::node_target(to)?,
                label: names::non_empty(label, ir::NameField::Label)?,
            }))
        }
        AstNode::DropEdgeById {
            input: Some(input),
            edges,
        } => InputMutationFamilyRoot::Mutation(Box::new(ir::MutationPlan::DropEdgeById {
            input: ir::MutationInput::FromInput {
                input: Box::new(scoped_required_expr_from_ast(ctx, input, scope)?),
            },
            edges: mutation_contracts::edge_target(edges)?,
        })),
        _ => InputMutationFamilyRoot::NotThisFamily,
    })
}
