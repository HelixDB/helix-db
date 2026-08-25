use super::super::super::rejection::{self, NativeUnsupportedReason};
use super::support;
use crate::{ir, logical};
use helix_ast::expr::Predicate;
use helix_ast::traversal::{self, AstNode};

#[test]
fn native_terminals_accept_source_stream_wrappers() {
    let expr = support::lower(&AstNode::Count {
        input: Box::new(AstNode::Where {
            input: support::node_source(),
            predicate: Predicate::eq("active", true),
        }),
    })
    .unwrap()
    .expect_native("filtered count is native");
    assert!(matches!(
        expr,
        logical::LogicalExpr::StreamCardinality(project)
            if matches!(
                project.input(),
                logical::RootStream::Access(logical::AccessStream::Filter(_))
            )
    ));
}

#[test]
fn native_terminals_accept_terminal_chains() {
    let reserved_project = support::lower(&AstNode::Count {
        input: Box::new(AstNode::Path {
            input: support::node_source(),
        }),
    })
    .unwrap()
    .expect_native("reserved-to-project chain is native");
    assert!(matches!(
        reserved_project,
        logical::LogicalExpr::StreamCardinality(project)
            if matches!(
                project.input(),
                logical::RootStream::Reserved(reserved)
                    if matches!(reserved.op(), ir::ReservedOp::Path)
            )
    ));

    let project_project = support::lower(&AstNode::Count {
        input: Box::new(AstNode::Count {
            input: support::node_source(),
        }),
    })
    .unwrap()
    .expect_native("project-to-project chain is native");
    assert!(matches!(
        project_project,
        logical::LogicalExpr::StreamCardinality(project)
            if matches!(project.input(), logical::RootStream::Cardinality(_))
    ));

    let aggregate_project = support::lower(&AstNode::Count {
        input: Box::new(AstNode::Group {
            input: support::node_source(),
            property: "kind".to_owned(),
        }),
    })
    .unwrap()
    .expect_native("aggregate-to-project chain is native");
    assert!(matches!(
        aggregate_project,
        logical::LogicalExpr::StreamCardinality(project)
            if matches!(project.input(), logical::RootStream::Aggregate(_))
    ));
}

#[test]
fn native_terminals_accept_control_flow_inputs() {
    let optional_count = support::lower(&AstNode::Count {
        input: Box::new(AstNode::Optional {
            input: support::node_source(),
            traversal: traversal::sub().out(Some("FOLLOWS")),
        }),
    })
    .unwrap()
    .expect_native("optional-to-project chain is native");
    assert!(matches!(
        optional_count,
        logical::LogicalExpr::StreamCardinality(project)
            if matches!(project.input(), logical::RootStream::Branch(_))
    ));

    let repeat_count = support::lower(&AstNode::Count {
        input: Box::new(AstNode::Repeat {
            input: support::node_source(),
            config: traversal::RepeatConfig::new(traversal::sub().out(Some("FOLLOWS"))).times(2),
        }),
    })
    .unwrap()
    .expect_native("repeat-to-project chain is native");
    assert!(matches!(
        repeat_count,
        logical::LogicalExpr::StreamCardinality(project)
            if matches!(project.input(), logical::RootStream::Repeat(_))
    ));
}

#[test]
fn native_terminals_accept_variable_source_inputs() {
    let expr = support::lower(&AstNode::Count {
        input: Box::new(AstNode::Inject {
            input: None,
            variable: "seed".to_owned(),
        }),
    })
    .unwrap()
    .expect_native("variable-source count is native");
    assert!(matches!(
        expr,
        logical::LogicalExpr::StreamCardinality(project)
            if matches!(
                project.input(),
                logical::RootStream::VariableSource(source)
                    if source.variable().as_ref() == "seed"
            )
    ));
}

#[test]
fn native_terminals_reject_unsupported_inputs_without_partial_lowering() {
    let unsupported = support::lower(&AstNode::Count {
        input: Box::new(AstNode::Context),
    })
    .unwrap_err();
    assert_eq!(
        unsupported,
        rejection::unsupported(NativeUnsupportedReason::RootStreamInputUnsupported)
    );
}
