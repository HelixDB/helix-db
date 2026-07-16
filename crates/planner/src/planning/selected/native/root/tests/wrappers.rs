use helix_ast::expr::{Predicate, StreamBound};
use helix_ast::graph::NodeRef;
use helix_ast::traversal::{AstNode, Order};

use super::support;
use crate::logical;

#[test]
fn native_root_lowers_source_wrappers_to_access_contracts() {
    let dedup = support::lower(AstNode::Dedup {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::All,
        }),
    })
    .unwrap()
    .expect_native("source distinct is native");
    assert!(matches!(dedup, logical::LogicalExpr::AccessDistinct(_)));

    let filtered = support::lower(AstNode::Has {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::All,
        }),
        property: "age".to_owned(),
        value: 42.into(),
    })
    .unwrap()
    .expect_native("source has filter is native");
    assert!(matches!(filtered, logical::LogicalExpr::AccessFilter(_)));

    let pipeline = support::lower(AstNode::Store {
        input: Box::new(AstNode::OrderBy {
            input: Box::new(AstNode::Range {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::All,
                }),
                start: StreamBound::Literal(2),
                end: StreamBound::Literal(8),
            }),
            property: "age".to_owned(),
            order: Order::Asc,
        }),
        name: "cached".to_owned(),
    })
    .unwrap()
    .expect_native("source-rooted pipeline is native");
    assert!(matches!(
        pipeline,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(pipeline.ops(), [
                logical::StreamPipelineOp::Window { .. },
                logical::StreamPipelineOp::Order { .. },
                logical::StreamPipelineOp::VariableWrite { .. }
            ])
    ));
}

#[test]
fn native_root_accepts_supported_source_stream_wrappers() {
    [
        AstNode::HasLabel {
            input: support::node_source(),
            label: "User".to_owned(),
        },
        AstNode::HasKey {
            input: support::node_source(),
            property: "email".to_owned(),
        },
        AstNode::Where {
            input: support::node_source(),
            predicate: Predicate::eq("active", true),
        },
        AstNode::EdgeHas {
            input: support::edge_source(),
            property: "active".to_owned(),
            value: true.into(),
        },
        AstNode::EdgeHasLabel {
            input: support::edge_source(),
            label: "FOLLOWS".to_owned(),
        },
        AstNode::Limit {
            input: support::node_source(),
            count: StreamBound::Literal(10),
        },
        AstNode::Skip {
            input: support::node_source(),
            count: StreamBound::Literal(2),
        },
        AstNode::OrderByMultiple {
            input: support::node_source(),
            orderings: vec![
                ("age".to_owned(), Order::Asc),
                ("name".to_owned(), Order::Desc),
            ],
        },
        AstNode::Within {
            input: support::node_source(),
            variable: "allowed".to_owned(),
        },
        AstNode::Without {
            input: support::node_source(),
            variable: "blocked".to_owned(),
        },
        AstNode::Select {
            input: support::node_source(),
            name: "cached".to_owned(),
        },
        AstNode::Bind {
            input: support::node_source(),
            name: "row".to_owned(),
        },
        AstNode::Inject {
            input: Some(support::node_source()),
            variable: "seed".to_owned(),
        },
        AstNode::As {
            input: support::node_source(),
            name: "aliased".to_owned(),
        },
    ]
    .into_iter()
    .for_each(support::assert_native);
}
