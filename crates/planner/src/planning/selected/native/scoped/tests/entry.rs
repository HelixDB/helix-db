use super::super::super::scope;
use super::super::entry;
use crate::{context, ir, logical};
use helix_ast::graph::NodeRef;
use helix_ast::traversal::{self, AstNode};
use helix_ast::value::PropertyInput;

#[test]
fn scoped_roots_bind_context_only_inside_sub_traversals() {
    assert!(entry::scoped_selectable_root_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Context,
        scope::NativeAstScope::QueryRoot,
    )
    .is_ok_and(|root| matches!(root, entry::ScopedSelectableRoot::NotSelectable)));

    let root = entry::scoped_selectable_root_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Context,
        scope::NativeAstScope::SubTraversal,
    )
    .unwrap()
    .expect_selectable("context binds in sub-traversals");

    assert!(matches!(
        root.expr(),
        logical::LogicalExpr::VariableSource(source) if source.variable().as_ref() == "$context"
    ));
}

#[test]
fn scoped_roots_keep_context_pipelines_inside_cascades() {
    let ast = *traversal::sub().out(Some("FOLLOWS")).root;
    let root = entry::scoped_selectable_root_from_ast(
        &context::PlannerContext::default(),
        &ast,
        scope::NativeAstScope::SubTraversal,
    )
    .unwrap()
    .expect_selectable("context pipeline is selectable");

    assert!(matches!(
        root.expr(),
        logical::LogicalExpr::RootPipeline(pipeline)
            if matches!(pipeline.input(), logical::RootStream::VariableSource(source)
                if source.variable().as_ref() == "$context")
            && pipeline.ops().len() == 1
    ));
}

#[test]
fn scoped_roots_dispatch_context_sensitive_families_directly() {
    let terminal = entry::scoped_selectable_root_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Count {
            input: Box::new(AstNode::Context),
        },
        scope::NativeAstScope::SubTraversal,
    )
    .unwrap()
    .expect_selectable("scoped context terminal is selectable");
    assert!(matches!(
        terminal.expr(),
        logical::LogicalExpr::StreamProject(project)
            if matches!(
                project.input(),
                logical::RootStream::VariableSource(source)
                    if source.variable().as_ref() == "$context"
            )
    ));

    let mutation = entry::scoped_selectable_root_from_ast(
        &context::PlannerContext::default(),
        &AstNode::SetProperty {
            input: Box::new(AstNode::Context),
            name: "active".to_owned(),
            value: PropertyInput::from(true),
        },
        scope::NativeAstScope::SubTraversal,
    )
    .unwrap()
    .expect_selectable("scoped context mutation is selectable");
    assert!(matches!(
        mutation.expr(),
        logical::LogicalExpr::RootMutation(mutation)
            if matches!(
                mutation.plan(),
                ir::MutationPlan::SetProperty { input, name, .. }
                    if name.as_ref() == "active"
                        && matches!(
                            input.as_ref(),
                            logical::LogicalExpr::VariableSource(source)
                                if source.variable().as_ref() == "$context"
                        )
            )
    ));

    let branch = entry::scoped_selectable_root_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Optional {
            input: Box::new(AstNode::Nodes {
                reference: NodeRef::All,
            }),
            traversal: traversal::sub().out(Some("FOLLOWS")),
        },
        scope::NativeAstScope::QueryRoot,
    )
    .unwrap()
    .expect_selectable("branch is selectable");
    assert!(matches!(
        branch.expr(),
        logical::LogicalExpr::RootBranch(branch)
            if matches!(branch.input(), logical::LogicalExpr::AccessPath(_))
    ));
}
