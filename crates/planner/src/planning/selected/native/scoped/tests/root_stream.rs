use super::super::super::rejection::{self, NativeUnsupportedReason};
use super::super::super::scope;
use super::super::root_stream;
use super::support;
use crate::{context, logical};
use helix_ast::traversal::{self, AstNode};

#[test]
fn scoped_root_stream_normalizes_direct_stream_families() {
    let ctx = context::PlannerContext::default();
    let scope = scope::NativeAstScope::QueryRoot;

    let access = root_stream::root_stream_from_ast(&ctx, &support::node_ast(), scope)
        .unwrap()
        .expect_stream("node source is a root stream");
    assert!(matches!(
        access,
        logical::RootStream::Access(logical::AccessStream::Path(_))
    ));

    let variable = root_stream::root_stream_from_ast(
        &ctx,
        &AstNode::Inject {
            input: None,
            variable: "seed".to_owned(),
        },
        scope,
    )
    .unwrap()
    .expect_stream("variable injection is a root stream");
    assert!(matches!(
        variable,
        logical::RootStream::VariableSource(source) if source.variable().as_ref() == "seed"
    ));

    let source_mutation = root_stream::root_stream_from_ast(
        &ctx,
        &AstNode::AddN {
            input: None,
            label: "User".to_owned(),
            properties: Vec::new(),
        },
        scope,
    )
    .unwrap()
    .expect_stream("source mutation is a root stream");
    assert!(matches!(source_mutation, logical::RootStream::Mutation(_)));

    let input_mutation = root_stream::root_stream_from_ast(
        &ctx,
        &AstNode::SetProperty {
            input: support::node_box(),
            name: "active".to_owned(),
            value: helix_ast::value::PropertyInput::from(true),
        },
        scope,
    )
    .unwrap()
    .expect_stream("input mutation is a root stream");
    assert!(matches!(input_mutation, logical::RootStream::Mutation(_)));
}

#[test]
fn scoped_root_stream_normalizes_recursive_wrappers() {
    let ctx = context::PlannerContext::default();
    let scope = scope::NativeAstScope::QueryRoot;

    let branch = root_stream::root_stream_from_ast(
        &ctx,
        &AstNode::Optional {
            input: support::node_box(),
            traversal: traversal::sub().out(Some("FOLLOWS")),
        },
        scope,
    )
    .unwrap()
    .expect_stream("optional is a root stream");
    assert!(matches!(branch, logical::RootStream::Branch(_)));

    let terminal = root_stream::root_stream_from_ast(
        &ctx,
        &AstNode::Count {
            input: support::node_box(),
        },
        scope,
    )
    .unwrap()
    .expect_stream("terminal is a root stream");
    assert!(matches!(terminal, logical::RootStream::Project(_)));

    let pipeline = root_stream::root_stream_from_ast(
        &ctx,
        &AstNode::Limit {
            input: Box::new(AstNode::Count {
                input: support::node_box(),
            }),
            count: helix_ast::expr::StreamBound::Literal(1),
        },
        scope,
    )
    .unwrap()
    .expect_stream("terminal-rooted pipeline is a root stream");
    assert!(matches!(pipeline, logical::RootStream::Pipeline(_)));
}

#[test]
fn scoped_root_stream_honors_context_scope() {
    let ctx = context::PlannerContext::default();

    assert!(root_stream::root_stream_from_ast(
        &ctx,
        &AstNode::Context,
        scope::NativeAstScope::QueryRoot,
    )
    .is_ok_and(|stream| matches!(stream, root_stream::ScopedRootStream::NotRootStream)));

    let bound_context = root_stream::root_stream_from_ast(
        &ctx,
        &AstNode::Context,
        scope::NativeAstScope::SubTraversal,
    )
    .unwrap()
    .expect_stream("scoped context is a root stream");
    assert!(matches!(
        bound_context,
        logical::RootStream::VariableSource(source) if source.variable().as_ref() == "$context"
    ));

    let unsupported_query_context_terminal = root_stream::root_stream_from_ast(
        &ctx,
        &AstNode::Count {
            input: Box::new(AstNode::Context),
        },
        scope::NativeAstScope::QueryRoot,
    )
    .unwrap_err();
    assert_eq!(
        unsupported_query_context_terminal,
        rejection::unsupported(NativeUnsupportedReason::RootStreamInputUnsupported)
    );

    let bound_context_terminal = root_stream::root_stream_from_ast(
        &ctx,
        &AstNode::Count {
            input: Box::new(AstNode::Context),
        },
        scope::NativeAstScope::SubTraversal,
    )
    .unwrap()
    .expect_stream("scoped context terminal is a root stream");
    assert!(matches!(
        bound_context_terminal,
        logical::RootStream::Project(project)
            if matches!(
                project.input(),
                logical::RootStream::VariableSource(source)
                    if source.variable().as_ref() == "$context"
            )
    ));
}
