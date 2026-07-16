//! Input-consuming property mutation lowering.

use helix_ast::traversal::AstNode;

use super::super::super::{names, scope::NativeAstScope};
use super::super::scoped_required_expr_from_ast;
use super::InputMutationFamilyRoot;
use crate::{context, error, ir};

pub(super) fn property_mutation_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<InputMutationFamilyRoot, error::PlannerError> {
    Ok(match root {
        AstNode::SetProperty { input, name, value } => {
            InputMutationFamilyRoot::Mutation(Box::new(ir::MutationPlan::SetProperty {
                input: Box::new(scoped_required_expr_from_ast(ctx, input, scope)?),
                name: names::non_empty(name, ir::NameField::Property)?,
                value: ir::PropertyInputPlan::new(value.clone())?,
            }))
        }
        AstNode::RemoveProperty { input, name } => {
            InputMutationFamilyRoot::Mutation(Box::new(ir::MutationPlan::RemoveProperty {
                input: Box::new(scoped_required_expr_from_ast(ctx, input, scope)?),
                name: names::non_empty(name, ir::NameField::Property)?,
            }))
        }
        _ => InputMutationFamilyRoot::NotThisFamily,
    })
}
