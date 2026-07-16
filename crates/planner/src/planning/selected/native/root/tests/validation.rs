use helix_ast::expr::Predicate;
use helix_ast::traversal::{AstNode, Order};

use super::support;
use crate::{error, ir, logical};

#[test]
fn native_root_lowers_source_predicates_with_validation() {
    let root = support::lower(AstNode::NodesWhere {
        predicate: Predicate::eq("age", 42),
    })
    .unwrap()
    .expect_native("source predicate is native");
    assert!(matches!(
        root,
        logical::LogicalExpr::AccessFilter(filter)
            if matches!(filter.access(), logical::AccessPath::Node(_))
    ));

    let invalid = support::lower(AstNode::NodesWhere {
        predicate: Predicate::eq("$label", ""),
    });
    assert!(matches!(
        invalid,
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Label
        })
    ));
}

#[test]
fn native_root_validates_supported_source_stream_wrappers() {
    let duplicate_order = support::lower(AstNode::OrderByMultiple {
        input: support::node_source(),
        orderings: vec![
            ("age".to_owned(), Order::Asc),
            ("age".to_owned(), Order::Desc),
        ],
    });
    assert!(matches!(
        duplicate_order,
        Err(error::PlannerError::DuplicateOrderKey { property }) if property.as_ref() == "age"
    ));

    let empty_select = support::lower(AstNode::Select {
        input: support::node_source(),
        name: String::new(),
    });
    assert!(matches!(
        empty_select,
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Name
        })
    ));

    let empty_within = support::lower(AstNode::Within {
        input: support::node_source(),
        variable: String::new(),
    });
    assert!(matches!(
        empty_within,
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Variable
        })
    ));
}
