use super::super::source;
use super::*;
use crate::{context, error, ir};
use helix_ast::expr::StreamBound;
use helix_ast::graph::NodeRef;
use helix_ast::traversal::AstNode;

fn nodes() -> Box<AstNode> {
    Box::new(AstNode::Nodes {
        reference: NodeRef::All,
    })
}

#[test]
fn access_stream_shape_classifies_source_wrapper_and_non_access_roots() {
    assert!(matches!(
        family::access_stream_shape_from_ast(&AstNode::Nodes {
            reference: NodeRef::All,
        }),
        family::NativeAccessStreamShape::Source(source::NativeSourceAst::Nodes(_))
    ));

    let limit = AstNode::Limit {
        input: nodes(),
        count: StreamBound::Literal(2),
    };
    let family::NativeAccessStreamShape::Wrapper(wrapper) =
        family::access_stream_shape_from_ast(&limit)
    else {
        panic!("limit is an access-stream wrapper");
    };
    assert!(matches!(wrapper.input(), AstNode::Nodes { .. }));

    assert!(matches!(
        family::access_stream_shape_from_ast(&AstNode::Context),
        family::NativeAccessStreamShape::NotAccessStream
    ));
}

#[test]
fn access_stream_shape_carries_typed_source_payload() {
    let root = AstNode::Nodes {
        reference: NodeRef::Var("users".to_owned()),
    };

    assert!(matches!(
        family::access_stream_shape_from_ast(&root),
        family::NativeAccessStreamShape::Source(source::NativeSourceAst::Nodes(
            NodeRef::Var(name),
        )) if name == "users"
    ));
}

#[test]
fn access_stream_reports_source_wrappers_and_non_access_roots() {
    assert!(matches!(
        native_access_stream_from_ast(
            &context::PlannerContext::default(),
            &AstNode::Nodes {
                reference: NodeRef::All,
            },
        )
        .unwrap(),
        NativeAccessStreamRoot::Stream(_)
    ));
    assert!(matches!(
        native_access_stream_from_ast(
            &context::PlannerContext::default(),
            &AstNode::Limit {
                input: nodes(),
                count: StreamBound::Literal(2),
            },
        )
        .unwrap(),
        NativeAccessStreamRoot::Stream(_)
    ));
    assert!(matches!(
        native_access_stream_from_ast(&context::PlannerContext::default(), &AstNode::Context)
            .unwrap(),
        NativeAccessStreamRoot::NotAccessStream
    ));
    assert!(matches!(
        native_access_stream_from_ast(
            &context::PlannerContext::default(),
            &AstNode::Limit {
                input: Box::new(AstNode::Context),
                count: StreamBound::Literal(2),
            },
        )
        .unwrap(),
        NativeAccessStreamRoot::NotAccessStream
    ));
}

#[test]
fn access_stream_propagates_wrapper_validation_errors() {
    assert!(matches!(
        native_access_stream_from_ast(
            &context::PlannerContext::default(),
            &AstNode::Select {
                input: nodes(),
                name: String::new(),
            },
        ),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Name
        })
    ));
}

#[test]
fn access_stream_defers_wrapper_validation_until_input_is_access_stream() {
    assert!(matches!(
        native_access_stream_from_ast(
            &context::PlannerContext::default(),
            &AstNode::Select {
                input: Box::new(AstNode::Context),
                name: String::new(),
            },
        )
        .unwrap(),
        NativeAccessStreamRoot::NotAccessStream
    ));
}
