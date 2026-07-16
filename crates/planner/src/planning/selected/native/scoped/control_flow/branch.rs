//! Scoped branch AST lowering.

use helix_ast::traversal::AstNode;

use super::super::super::scope::NativeAstScope;
use super::super::scoped_required_expr_from_ast;
use crate::{context, error, ir, logical};

pub(super) enum ScopedBranchRoot {
    Branch(Box<logical::RootBranch>),
    NotBranch,
}

pub(super) fn branch_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedBranchRoot, error::PlannerError> {
    Ok(match root {
        AstNode::Union { input, traversals } => {
            let input = scoped_required_expr_from_ast(ctx, input, scope)?;
            let actual = traversals.len();
            let arms = traversals
                .iter()
                .map(|traversal| {
                    scoped_required_expr_from_ast(
                        ctx,
                        &traversal.root,
                        NativeAstScope::SubTraversal,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let arms = ir::AtLeast::<_, 2>::try_from_vec(arms).ok_or(
                error::PlannerError::InvalidBranchArity {
                    op: error::BranchOp::Union,
                    min: 2,
                    actual,
                },
            )?;
            ScopedBranchRoot::Branch(Box::new(logical::RootBranch::new(
                input,
                ir::BranchPlan::Union(arms),
            )))
        }
        AstNode::Choose {
            input,
            condition,
            then_traversal,
            else_traversal,
        } => {
            let input = scoped_required_expr_from_ast(ctx, input, scope)?;
            let then_plan = scoped_required_expr_from_ast(
                ctx,
                &then_traversal.root,
                NativeAstScope::SubTraversal,
            )?;
            let condition = ir::PredicatePlan::new(condition.clone())?;
            let plan = match else_traversal {
                Some(else_traversal) => ir::BranchPlan::ChooseElse {
                    condition,
                    then_plan: Box::new(then_plan),
                    else_plan: Box::new(scoped_required_expr_from_ast(
                        ctx,
                        &else_traversal.root,
                        NativeAstScope::SubTraversal,
                    )?),
                },
                None => ir::BranchPlan::Choose {
                    condition,
                    then_plan: Box::new(then_plan),
                },
            };
            ScopedBranchRoot::Branch(Box::new(logical::RootBranch::new(input, plan)))
        }
        AstNode::Coalesce { input, traversals } => {
            let input = scoped_required_expr_from_ast(ctx, input, scope)?;
            let actual = traversals.len();
            let arms = traversals
                .iter()
                .map(|traversal| {
                    scoped_required_expr_from_ast(
                        ctx,
                        &traversal.root,
                        NativeAstScope::SubTraversal,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let arms = ir::AtLeast::<_, 1>::try_from_vec(arms).ok_or(
                error::PlannerError::InvalidBranchArity {
                    op: error::BranchOp::Coalesce,
                    min: 1,
                    actual,
                },
            )?;
            ScopedBranchRoot::Branch(Box::new(logical::RootBranch::new(
                input,
                ir::BranchPlan::Coalesce(arms),
            )))
        }
        AstNode::Optional { input, traversal } => {
            let input = scoped_required_expr_from_ast(ctx, input, scope)?;
            let optional =
                scoped_required_expr_from_ast(ctx, &traversal.root, NativeAstScope::SubTraversal)?;
            ScopedBranchRoot::Branch(Box::new(logical::RootBranch::new(
                input,
                ir::BranchPlan::Optional(Box::new(optional)),
            )))
        }
        _ => ScopedBranchRoot::NotBranch,
    })
}
