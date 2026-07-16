//! Executable planner surface contract tests.
//!
//! The child modules split production-entrypoint coverage by planner boundary
//! so native AST lowering, Cascades selection, executable lowering, and typed
//! payload contracts can evolve independently.

mod access_streams;
mod batch_entries;
mod control_payloads;
mod control_roots;
mod ddl_mutation_payloads;
mod entrypoints;
mod misc_payloads;
mod nested_terminals;
mod projection_payloads;
mod reserved_streams;
mod shape_inventory;
mod variable_streams;
mod wrappers;

use crate::planning::tests::support::*;

fn nodes_root() -> AstNode {
    AstNode::Nodes {
        reference: NodeRef::all(),
    }
}

fn edges_root() -> AstNode {
    AstNode::Edges {
        reference: EdgeRef::ids([11u64, 13]),
    }
}

fn boxed(node: AstNode) -> Box<AstNode> {
    Box::new(node)
}

fn projection_of(root: AstNode) -> ProjectionPlan {
    let plan = executable_ast(root, PlannerContext::default());
    let ExecOp::Project { projection } =
        first_exec_op(&plan, |op| matches!(op, ExecOp::Project { .. }))
    else {
        panic!("expected projection");
    };
    projection.clone()
}

fn aggregate_of(root: AstNode) -> AggregatePlan {
    let plan = executable_ast(root, PlannerContext::default());
    let ExecOp::Aggregate { aggregate } =
        first_exec_op(&plan, |op| matches!(op, ExecOp::Aggregate { .. }))
    else {
        panic!("expected aggregate");
    };
    aggregate.clone()
}

fn mutation_of(root: AstNode) -> ExecMutationPlan {
    let plan = executable_ast(root, PlannerContext::default());
    let ExecOp::Mutation { plan: mutation } =
        first_exec_op(&plan, |op| matches!(op, ExecOp::Mutation { .. }))
    else {
        panic!("expected mutation");
    };
    mutation.clone()
}

fn ddl_of(root: AstNode) -> IndexDdlPlan {
    let plan = executable_ast(root, PlannerContext::default());
    let ExecOp::IndexDdl { plan: ddl } =
        first_exec_op(&plan, |op| matches!(op, ExecOp::IndexDdl { .. }))
    else {
        panic!("expected index DDL");
    };
    ddl.clone()
}
