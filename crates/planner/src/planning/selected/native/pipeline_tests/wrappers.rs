use helix_ast::expr::{Predicate, StreamBound};
use helix_ast::traversal::{AstNode, Order};
use helix_ast::value::PropertyInput;

use super::support;
use crate::logical;

#[derive(Clone, Copy)]
enum ExpectedOp {
    Filter,
    Distinct,
    Limit,
    Skip,
    Range,
    Order,
    Variable,
    VariableWrite,
}

impl ExpectedOp {
    fn matches(self, op: &logical::StreamPipelineOp) -> bool {
        matches!(
            (self, op),
            (Self::Filter, logical::StreamPipelineOp::Filter { .. })
                | (Self::Distinct, logical::StreamPipelineOp::Distinct)
                | (Self::Limit, logical::StreamPipelineOp::Limit { .. })
                | (Self::Skip, logical::StreamPipelineOp::Skip { .. })
                | (Self::Range, logical::StreamPipelineOp::Range { .. })
                | (Self::Order, logical::StreamPipelineOp::Order { .. })
                | (Self::Variable, logical::StreamPipelineOp::Variable { .. })
                | (
                    Self::VariableWrite,
                    logical::StreamPipelineOp::VariableWrite { .. }
                )
        )
    }
}

#[test]
fn native_pipeline_lowers_stream_wrappers_above_terminals() {
    [
        (
            AstNode::Has {
                input: support::count_source(),
                property: "active".to_owned(),
                value: true.into(),
            },
            ExpectedOp::Filter,
        ),
        (
            AstNode::EdgeHas {
                input: support::count_source(),
                property: "active".to_owned(),
                value: PropertyInput::from(true),
            },
            ExpectedOp::Filter,
        ),
        (
            AstNode::HasLabel {
                input: support::count_source(),
                label: "User".to_owned(),
            },
            ExpectedOp::Filter,
        ),
        (
            AstNode::EdgeHasLabel {
                input: support::count_source(),
                label: "LIKES".to_owned(),
            },
            ExpectedOp::Filter,
        ),
        (
            AstNode::HasKey {
                input: support::count_source(),
                property: "email".to_owned(),
            },
            ExpectedOp::Filter,
        ),
        (
            AstNode::Where {
                input: support::count_source(),
                predicate: Predicate::eq("active", true),
            },
            ExpectedOp::Filter,
        ),
        (
            AstNode::Dedup {
                input: support::count_source(),
            },
            ExpectedOp::Distinct,
        ),
        (
            AstNode::Limit {
                input: support::count_source(),
                count: StreamBound::Literal(10),
            },
            ExpectedOp::Limit,
        ),
        (
            AstNode::Skip {
                input: support::count_source(),
                count: StreamBound::Literal(2),
            },
            ExpectedOp::Skip,
        ),
        (
            AstNode::Range {
                input: support::count_source(),
                start: StreamBound::Literal(2),
                end: StreamBound::Literal(10),
            },
            ExpectedOp::Range,
        ),
        (
            AstNode::OrderBy {
                input: support::count_source(),
                property: "age".to_owned(),
                order: Order::Asc,
            },
            ExpectedOp::Order,
        ),
        (
            AstNode::OrderByMultiple {
                input: support::count_source(),
                orderings: vec![
                    ("age".to_owned(), Order::Asc),
                    ("name".to_owned(), Order::Desc),
                ],
            },
            ExpectedOp::Order,
        ),
        (
            AstNode::Within {
                input: support::count_source(),
                variable: "allowed".to_owned(),
            },
            ExpectedOp::Variable,
        ),
        (
            AstNode::Without {
                input: support::count_source(),
                variable: "blocked".to_owned(),
            },
            ExpectedOp::Variable,
        ),
        (
            AstNode::Select {
                input: support::count_source(),
                name: "cached".to_owned(),
            },
            ExpectedOp::Variable,
        ),
        (
            AstNode::Bind {
                input: support::count_source(),
                name: "row".to_owned(),
            },
            ExpectedOp::Variable,
        ),
        (
            AstNode::Inject {
                input: Some(support::count_source()),
                variable: "seed".to_owned(),
            },
            ExpectedOp::Variable,
        ),
        (
            AstNode::As {
                input: support::count_source(),
                name: "aliased".to_owned(),
            },
            ExpectedOp::VariableWrite,
        ),
        (
            AstNode::Store {
                input: support::count_source(),
                name: "stored".to_owned(),
            },
            ExpectedOp::VariableWrite,
        ),
    ]
    .into_iter()
    .for_each(|(root, expected)| {
        let expr = support::lower(root)
            .unwrap()
            .expect_native("terminal-rooted pipeline is native");
        assert!(matches!(
            expr,
            logical::LogicalExpr::RootPipeline(pipeline)
                if matches!(pipeline.input(), logical::RootStream::Cardinality(_))
                    && matches!(pipeline.ops(), [op] if expected.matches(op))
        ));
    });

    let ordered = support::lower(AstNode::OrderBy {
        input: Box::new(AstNode::Where {
            input: support::count_source(),
            predicate: Predicate::eq("active", true),
        }),
        property: "age".to_owned(),
        order: Order::Desc,
    })
    .unwrap()
    .expect_native("terminal-rooted pipeline chain is native");
    assert!(matches!(
        ordered,
        logical::LogicalExpr::RootPipeline(pipeline)
            if matches!(pipeline.input(), logical::RootStream::Cardinality(_))
                && matches!(
                    pipeline.ops(),
                    [
                        logical::StreamPipelineOp::Filter { .. },
                        logical::StreamPipelineOp::Order { .. }
                    ]
                )
    ));
}
