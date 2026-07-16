use helix_ast::expr::{Expr, StreamBound};
use helix_ast::traversal::AstNode;

use super::super::rejection::{self, NativeUnsupportedReason};
use super::support;
use crate::{error, ir};

#[test]
fn native_pipeline_validates_payloads() {
    let invalid_label = support::lower(AstNode::Out {
        input: support::node_source(),
        label: Some(String::new()),
    });
    assert!(matches!(
        invalid_label,
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Label
        })
    ));

    let invalid_limit = support::lower(AstNode::Limit {
        input: support::count_source(),
        count: StreamBound::expr(Expr::val(-1)),
    });
    assert!(matches!(
        invalid_limit,
        Err(error::PlannerError::InvalidStreamBoundExpression { .. })
    ));

    let unsupported_input = support::lower(AstNode::Limit {
        input: Box::new(AstNode::Context),
        count: StreamBound::Literal(1),
    });
    assert_eq!(
        unsupported_input.unwrap_err(),
        rejection::unsupported(NativeUnsupportedReason::RootStreamInputUnsupported)
    );
}
