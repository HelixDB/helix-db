//! Input-consuming node mutation lowering.

use helix_ast::traversal::AstNode;

use super::super::super::{names, scope::NativeAstScope};
use super::super::scoped_required_expr_from_ast;
use super::InputMutationFamilyRoot;
use crate::planning::mutation as mutation_contracts;
use crate::{context, error, ir};

pub(super) fn node_mutation_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<InputMutationFamilyRoot, error::PlannerError> {
    Ok(match root {
        AstNode::AddN {
            input: Some(input),
            label,
            properties,
        } => InputMutationFamilyRoot::Mutation(Box::new(ir::MutationPlan::AddNode {
            input: ir::MutationInput::FromInput {
                input: Box::new(scoped_required_expr_from_ast(ctx, input, scope)?),
            },
            label: names::non_empty(label, ir::NameField::Label)?,
            properties: mutation_contracts::property_assignments(properties)?,
        })),
        _ => InputMutationFamilyRoot::NotThisFamily,
    })
}
