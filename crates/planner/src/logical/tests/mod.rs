use super::*;
use std::num::NonZeroUsize;

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).unwrap()
}

fn predicate() -> ir::PredicatePlan {
    ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap()
}

fn order_key() -> ir::OrderKey {
    ir::OrderKey {
        property: name("age"),
        order: helix_ast::traversal::Order::Asc,
    }
}

fn node_access_path(plan: ir::NodeAccessPlan) -> AccessPath {
    AccessPath::Node(NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(plan).unwrap(),
    ))
}

fn edge_access_path(plan: ir::EdgeAccessPlan) -> AccessPath {
    AccessPath::Edge(EdgeAccessPath::new(
        ir::EdgeAccessSourcePlan::new(plan).unwrap(),
    ))
}

fn node_root() -> LogicalExpr {
    LogicalExpr::AccessPath(node_access_path(ir::NodeAccessPlan::AllScan))
}

fn edge_root() -> LogicalExpr {
    LogicalExpr::AccessPath(edge_access_path(ir::EdgeAccessPlan::AllScan))
}

mod access;
mod core;
mod memo;
