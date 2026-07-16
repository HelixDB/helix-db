use super::support;
use crate::{context, error, ir};
use helix_ast::{batch as ast_batch, traversal as ast_traversal};

#[test]
fn native_batch_boundary_rejects_unsupported_batches() {
    let unsupported = ast_batch::BatchQuery::Write(ast_batch::WriteBatch {
        entries: vec![support::query(ast_traversal::AstNode::Context)],
        returns: Vec::new(),
    });
    assert_eq!(
        support::lower_batch(&unsupported, &context::PlannerContext::default()).unwrap_err(),
        error::PlannerError::UnboundContext
    );

    let unsupported_body = ast_batch::BatchQuery::Write(ast_batch::WriteBatch {
        entries: vec![ast_batch::BatchEntry::ForEach {
            param: "id".to_owned(),
            body: vec![support::query(ast_traversal::AstNode::Context)],
        }],
        returns: Vec::new(),
    });
    assert_eq!(
        support::lower_batch(&unsupported_body, &context::PlannerContext::default()).unwrap_err(),
        error::PlannerError::UnboundContext
    );

    let unsupported_wrapped_context = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![support::query(ast_traversal::AstNode::Count {
            input: Box::new(ast_traversal::AstNode::Context),
        })],
        returns: Vec::new(),
    });
    assert_eq!(
        support::lower_batch(
            &unsupported_wrapped_context,
            &context::PlannerContext::default()
        )
        .unwrap_err(),
        error::PlannerError::UnboundContext
    );

    let unsupported_wrapped_body = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![ast_batch::BatchEntry::ForEach {
            param: "id".to_owned(),
            body: vec![support::query(ast_traversal::AstNode::Count {
                input: Box::new(ast_traversal::AstNode::Context),
            })],
        }],
        returns: Vec::new(),
    });
    assert_eq!(
        support::lower_batch(
            &unsupported_wrapped_body,
            &context::PlannerContext::default()
        )
        .unwrap_err(),
        error::PlannerError::UnboundContext
    );
}

#[test]
fn native_batch_boundary_validates_empty_and_initial_conditions() {
    let empty = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: Vec::new(),
        returns: Vec::new(),
    });
    assert!(matches!(
        support::lower_batch(&empty, &context::PlannerContext::default()),
        Err(error::PlannerError::InvalidBatchArity {
            op: error::BatchOp::Batch,
            min: 1,
            actual: 0
        })
    ));

    let invalid_initial_condition = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![support::conditional_query(
            support::node_source(),
            ast_batch::BatchCondition::PrevNotEmpty,
        )],
        returns: Vec::new(),
    });
    assert!(matches!(
        support::lower_batch(
            &invalid_initial_condition,
            &context::PlannerContext::default()
        ),
        Err(error::PlannerError::InvalidInitialBatchCondition {
            condition: error::InitialBatchCondition::PrevNotEmpty
        })
    ));
}

#[test]
fn native_batch_boundary_validates_foreach_shape() {
    let empty_param = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![ast_batch::BatchEntry::ForEach {
            param: String::new(),
            body: vec![support::query(support::node_source())],
        }],
        returns: Vec::new(),
    });
    assert!(matches!(
        support::lower_batch(&empty_param, &context::PlannerContext::default()),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Param
        })
    ));

    let empty_body = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![ast_batch::BatchEntry::ForEach {
            param: "id".to_owned(),
            body: Vec::new(),
        }],
        returns: Vec::new(),
    });
    assert!(matches!(
        support::lower_batch(&empty_body, &context::PlannerContext::default()),
        Err(error::PlannerError::InvalidBatchArity {
            op: error::BatchOp::ForEach,
            min: 1,
            actual: 0
        })
    ));
}
