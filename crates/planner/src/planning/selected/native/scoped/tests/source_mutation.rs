use super::super::super::scope;
use super::super::entry;
use crate::{context, logical};
use helix_ast::traversal::AstNode;

#[test]
fn scoped_roots_keep_source_mutation_stream_consumers_inside_cascades() {
    let source_mutation = || {
        Box::new(AstNode::AddN {
            input: None,
            label: "User".to_owned(),
            properties: Vec::new(),
        })
    };

    let terminal = entry::scoped_selectable_root_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Count {
            input: source_mutation(),
        },
        scope::NativeAstScope::QueryRoot,
    )
    .unwrap()
    .expect_selectable("source mutation terminal is selectable");
    assert!(matches!(
        terminal.expr(),
        logical::LogicalExpr::StreamCardinality(project)
            if matches!(project.input(), logical::RootStream::Mutation(_))
    ));

    let pipeline = entry::scoped_selectable_root_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Limit {
            input: source_mutation(),
            count: helix_ast::expr::StreamBound::Literal(1),
        },
        scope::NativeAstScope::QueryRoot,
    )
    .unwrap()
    .expect_selectable("source mutation pipeline is selectable");
    assert!(matches!(
        pipeline.expr(),
        logical::LogicalExpr::RootPipeline(pipeline)
            if matches!(pipeline.input(), logical::RootStream::Mutation(_))
    ));
}
