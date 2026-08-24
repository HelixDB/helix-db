use super::*;
use crate::{ir, logical};
use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::graph::NodeRef;

#[test]
fn source_stream_reports_source_and_non_source_roots() {
    let source = source_stream_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Nodes {
            reference: NodeRef::All,
        },
    )
    .unwrap();
    let NativeSourceStreamRoot::Source(stream) = source else {
        panic!("nodes root is a source stream");
    };
    assert!(matches!(
        stream.into_logical_expr().unwrap(),
        logical::LogicalExpr::AccessPath(_)
    ));

    assert!(matches!(
        source_stream_from_ast(&context::PlannerContext::default(), &AstNode::Context).unwrap(),
        NativeSourceStreamRoot::NotSource
    ));
}

#[test]
fn source_stream_extracts_label_scopes_and_real_residuals() {
    let node = source_stream_from_ast(
        &context::PlannerContext::default(),
        &AstNode::NodesWhere {
            predicate: Predicate::and(vec![
                Predicate::eq("$label", "User"),
                Predicate::compare(Expr::val(1), CompareOp::Eq, Expr::val(1)),
            ]),
        },
    )
    .unwrap();
    let NativeSourceStreamRoot::Source(node) = node else {
        panic!("nodes_where is a source stream");
    };
    assert!(matches!(
        node.into_logical_expr().unwrap(),
        logical::LogicalExpr::AccessPath(logical::AccessPath::Node(path))
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::LabelScan { label } if label.as_ref() == "User"
            )
    ));

    let edge = source_stream_from_ast(
        &context::PlannerContext::default(),
        &AstNode::EdgesWhere {
            predicate: Predicate::and(vec![
                Predicate::eq("$label", "FOLLOWS"),
                Predicate::eq("active", true),
            ]),
        },
    )
    .unwrap();
    let NativeSourceStreamRoot::Source(edge) = edge else {
        panic!("edges_where is a source stream");
    };
    assert!(matches!(
        edge.into_logical_expr().unwrap(),
        logical::LogicalExpr::AccessFilter(filter)
            if matches!(
                filter.access(),
                logical::AccessPath::Edge(path)
                    if matches!(
                        path.source().as_ref(),
                        ir::EdgeAccessPlan::LabelScan { label } if label.as_ref() == "FOLLOWS"
                    )
            )
            && filter.predicate()
                == &ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap()
    ));
}

#[test]
fn source_stream_collapses_static_predicates_before_filtering() {
    let tautology = source_stream_from_ast(
        &context::PlannerContext::default(),
        &AstNode::NodesWhere {
            predicate: Predicate::compare(Expr::val(1), CompareOp::Eq, Expr::val(1)),
        },
    )
    .unwrap();
    let NativeSourceStreamRoot::Source(tautology) = tautology else {
        panic!("nodes_where is a source stream");
    };
    assert!(matches!(
        tautology.into_logical_expr().unwrap(),
        logical::LogicalExpr::AccessPath(logical::AccessPath::Node(path))
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::AllScan)
    ));

    let impossible = source_stream_from_ast(
        &context::PlannerContext::default(),
        &AstNode::EdgesWhere {
            predicate: Predicate::compare(Expr::val(1), CompareOp::Eq, Expr::val(2)),
        },
    )
    .unwrap();
    let NativeSourceStreamRoot::Source(impossible) = impossible else {
        panic!("edges_where is a source stream");
    };
    assert!(matches!(
        impossible.into_logical_expr().unwrap(),
        logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(path))
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
}

#[test]
fn source_stream_preserves_missing_predicate_bindings_for_runtime_evaluation() {
    for predicate in [
        Predicate::eq_param("status", "missing_status"),
        Predicate::is_in_param("group", "missing_groups"),
    ] {
        let node = source_stream_from_ast(
            &context::PlannerContext::default(),
            &AstNode::NodesWhere {
                predicate: predicate.clone(),
            },
        )
        .unwrap();
        let NativeSourceStreamRoot::Source(node) = node else {
            panic!("nodes_where is a source stream");
        };
        assert!(matches!(
            node.into_logical_expr().unwrap(),
            logical::LogicalExpr::AccessFilter(filter)
                if matches!(
                    filter.access(),
                    logical::AccessPath::Node(path)
                        if matches!(path.source().as_ref(), ir::NodeAccessPlan::AllScan)
                )
                && filter.predicate() == &ir::PredicatePlan::new(predicate.clone()).unwrap()
        ));

        let edge = source_stream_from_ast(
            &context::PlannerContext::default(),
            &AstNode::EdgesWhere {
                predicate: predicate.clone(),
            },
        )
        .unwrap();
        let NativeSourceStreamRoot::Source(edge) = edge else {
            panic!("edges_where is a source stream");
        };
        assert!(matches!(
            edge.into_logical_expr().unwrap(),
            logical::LogicalExpr::AccessFilter(filter)
                if matches!(
                    filter.access(),
                    logical::AccessPath::Edge(path)
                        if matches!(path.source().as_ref(), ir::EdgeAccessPlan::AllScan)
                )
                && filter.predicate() == &ir::PredicatePlan::new(predicate).unwrap()
        ));
    }
}
