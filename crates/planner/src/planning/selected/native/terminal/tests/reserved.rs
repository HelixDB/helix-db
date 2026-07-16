use super::support;
use crate::{ir, logical};
use helix_ast::traversal::AstNode;
use helix_ast::value::PropertyValue;

#[test]
fn native_terminals_lower_reserved_contracts() {
    [
        (
            AstNode::Fold {
                input: support::node_source(),
            },
            ir::ReservedOp::Fold,
        ),
        (
            AstNode::Unfold {
                input: support::node_source(),
            },
            ir::ReservedOp::Unfold,
        ),
        (
            AstNode::Path {
                input: support::node_source(),
            },
            ir::ReservedOp::Path,
        ),
        (
            AstNode::SimplePath {
                input: support::node_source(),
            },
            ir::ReservedOp::SimplePath,
        ),
        (
            AstNode::WithSack {
                input: support::node_source(),
                initial: PropertyValue::from(1),
            },
            ir::ReservedOp::WithSack(PropertyValue::from(1)),
        ),
        (
            AstNode::SackSet {
                input: support::node_source(),
                property: "score".to_owned(),
            },
            ir::ReservedOp::SackSet(ir::NonEmptyString::new("score").unwrap()),
        ),
        (
            AstNode::SackAdd {
                input: support::node_source(),
                property: "score".to_owned(),
            },
            ir::ReservedOp::SackAdd(ir::NonEmptyString::new("score").unwrap()),
        ),
        (
            AstNode::SackGet {
                input: support::node_source(),
            },
            ir::ReservedOp::SackGet,
        ),
    ]
    .into_iter()
    .for_each(|(root, expected)| {
        let expr = support::lower(&root)
            .unwrap()
            .expect_native("reserved terminal should lower");
        assert!(matches!(
            expr,
            logical::LogicalExpr::StreamReserved(reserved)
                if reserved.op() == &expected
        ));
    });
}
