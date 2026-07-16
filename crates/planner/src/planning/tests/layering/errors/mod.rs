//! Planner error-boundary contract tests.
//!
//! These tests exercise production planning entrypoints and assert that
//! invalid AST, batch, search, mutation, and DDL shapes fail at explicit
//! contract boundaries instead of leaking partial executable plans.

mod batch_validation;
mod control_validation;
mod index_candidates;
mod index_ddl;
mod propagation;
mod property_projection_mutation;
mod residual_validation;
mod search_indexes;
mod search_inputs;
mod variables_and_ids;

use crate::planning::tests::support::*;

type AstWrapper = fn(Box<AstNode>) -> AstNode;

fn plan_read_checked(
    batch: &ReadBatch,
    ctx: &PlannerContext,
) -> Result<ExecutablePlan, PlannerError> {
    crate::planning::plan_read_batch(batch, ctx)
}

fn plan_write_checked(
    batch: &helix_ast::batch::WriteBatch,
    ctx: &PlannerContext,
) -> Result<ExecutablePlan, PlannerError> {
    crate::planning::plan_write_batch(batch, ctx)
}

fn plan_order_by_multiple_without_keys() -> Traversal<helix_ast::traversal::OnNodes, ReadOnly> {
    g().n(NodeRef::all())
        .order_by_multiple(Vec::<(&str, Order)>::new())
}

fn raw_read(root: AstNode) -> ReadBatch {
    ReadBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some("invalid".to_string()),
            root,
            condition: None,
        }))],
        returns: Vec::new(),
    }
}

fn boxed_nodes_root() -> Box<AstNode> {
    Box::new(AstNode::Nodes {
        reference: NodeRef::all(),
    })
}

fn invalid_param_node_source() -> Box<AstNode> {
    Box::new(AstNode::Nodes {
        reference: NodeRef::param(String::new()),
    })
}

fn invalid_sub_traversal() -> helix_ast::traversal::SubTraversal {
    helix_ast::traversal::SubTraversal {
        root: Box::new(AstNode::Limit {
            input: Box::new(AstNode::Context),
            count: StreamBound::expr(Expr::param(String::new())),
        }),
    }
}
