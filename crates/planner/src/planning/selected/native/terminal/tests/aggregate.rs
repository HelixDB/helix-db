use super::support;
use crate::logical;
use helix_ast::traversal::{AggregateFunction, AstNode};

#[test]
fn native_terminals_lower_aggregate_contracts() {
    [
        AstNode::Group {
            input: support::node_source(),
            property: "kind".to_owned(),
        },
        AstNode::GroupCount {
            input: support::node_source(),
            property: "kind".to_owned(),
        },
        AstNode::AggregateBy {
            input: support::node_source(),
            function: AggregateFunction::Count,
            property: "kind".to_owned(),
        },
    ]
    .into_iter()
    .for_each(|root| {
        let expr = support::lower(&root)
            .unwrap()
            .expect_native("aggregate should lower");
        assert!(matches!(expr, logical::LogicalExpr::StreamAggregate(_)));
    });
}
