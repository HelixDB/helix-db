//! Input-consuming element mutation lowering.

use helix_ast::traversal::AstNode;

use super::super::super::scope::NativeAstScope;
use super::super::scoped_required_expr_from_ast;
use super::InputMutationFamilyRoot;
use crate::{context, error, ir};

pub(super) fn element_mutation_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<InputMutationFamilyRoot, error::PlannerError> {
    Ok(match root {
        AstNode::Drop { input } => {
            InputMutationFamilyRoot::Mutation(Box::new(ir::MutationPlan::Drop {
                input: Box::new(scoped_required_expr_from_ast(ctx, input, scope)?),
            }))
        }
        _ => InputMutationFamilyRoot::NotThisFamily,
    })
}
