use super::support;
use crate::{context, exec};
use helix_ast::{
    batch as ast_batch, expr as ast_expr, graph as ast_graph, index as ast_index,
    traversal as ast_traversal, value as ast_value,
};

#[test]
fn native_batch_boundary_accepts_single_query() {
    let batch = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![support::query(support::node_source())],
        returns: Vec::new(),
    });

    let (entries, _) = support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::Run(_)
        )
    ));
}

#[test]
fn native_batch_boundary_accepts_catalog_backed_search_roots() {
    let batch = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![support::query(ast_traversal::AstNode::VectorSearchNodes {
            label: "Doc".to_owned(),
            property: "embedding".to_owned(),
            tenant_value: None,
            query_vector: ast_value::PropertyInput::from(ast_value::PropertyValue::F32Array(vec![
                0.1,
            ])),
            k: ast_expr::StreamBound::Literal(10),
        })],
        returns: Vec::new(),
    });

    let (entries, metrics) = support::lower_batch(&batch, &support::search_ctx()).unwrap();
    assert!(metrics.memo_groups > 0);
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::Run(entry)
        ) if matches!(
            entry.root,
            exec::SelectedExecutableRunRoot::Alternative(_)
        )
    ));
}

#[test]
fn native_batch_boundary_accepts_variable_source_roots() {
    let batch = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![support::query(ast_traversal::AstNode::Inject {
            input: None,
            variable: "seed".to_owned(),
        })],
        returns: Vec::new(),
    });

    let (entries, metrics) =
        support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(metrics.memo_groups > 0);
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::Run(entry)
        ) if matches!(
            entry.root,
            exec::SelectedExecutableRunRoot::Alternative(_)
        )
    ));
}

#[test]
fn native_batch_boundary_accepts_index_ddl_roots() {
    let batch = ast_batch::BatchQuery::Write(ast_batch::WriteBatch {
        entries: vec![support::query(ast_traversal::AstNode::CreateIndex {
            spec: ast_index::IndexSpec::node_unique_equality("Person", "email"),
            if_not_exists: true,
        })],
        returns: Vec::new(),
    });

    let (entries, metrics) =
        support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(metrics.memo_groups > 0);
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::Run(entry)
        ) if matches!(
            entry.root,
            exec::SelectedExecutableRunRoot::IndexDdl(_)
        )
    ));
}

#[test]
fn native_batch_boundary_accepts_shortest_path_roots() {
    let batch = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![support::query(ast_traversal::AstNode::ShortestPath {
            source: ast_graph::NodeRef::Ids(vec![1]),
            target: ast_graph::NodeRef::Ids(vec![2]),
            label: Some("KNOWS".to_owned()),
            direction: ast_traversal::ShortestPathDirection::Both,
            max_depth: 4,
        })],
        returns: Vec::new(),
    });

    let (entries, metrics) =
        support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(metrics.memo_groups > 0);
    let exec::SelectedExecutableBatchEntries::Single(
        exec::SelectedInitialExecutableBatchEntry::Run(entry),
    ) = entries
    else {
        panic!("expected single shortest path run entry");
    };
    let exec::SelectedExecutableRunRoot::ShortestPath(root) = entry.root else {
        panic!("expected shortest path selected root");
    };
    assert_eq!(root.plan().source, ast_graph::NodeRef::Ids(vec![1]));
    assert_eq!(root.plan().target, ast_graph::NodeRef::Ids(vec![2]));
    assert_eq!(root.plan().label.as_ref().map(AsRef::as_ref), Some("KNOWS"));
    assert_eq!(
        root.plan().direction,
        ast_traversal::ShortestPathDirection::Both
    );
    assert_eq!(root.plan().max_depth.get(), 4);
}

#[test]
fn native_batch_boundary_accepts_source_mutation_roots() {
    let batch = ast_batch::BatchQuery::Write(ast_batch::WriteBatch {
        entries: vec![support::query(ast_traversal::AstNode::AddN {
            input: None,
            label: "Person".to_owned(),
            properties: Vec::new(),
        })],
        returns: Vec::new(),
    });

    let (entries, metrics) =
        support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(metrics.memo_groups > 0);
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::Run(entry)
        ) if matches!(
            entry.root,
            exec::SelectedExecutableRunRoot::Mutation(_)
        )
    ));
}

#[test]
fn native_batch_boundary_accepts_source_mutation_stream_consumers() {
    let batch = ast_batch::BatchQuery::Write(ast_batch::WriteBatch {
        entries: vec![support::query(ast_traversal::AstNode::Count {
            input: Box::new(ast_traversal::AstNode::Store {
                input: Box::new(ast_traversal::AstNode::AddN {
                    input: None,
                    label: "Person".to_owned(),
                    properties: Vec::new(),
                }),
                name: "created".to_owned(),
            }),
        })],
        returns: Vec::new(),
    });

    let (entries, metrics) =
        support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(metrics.memo_groups > 0);
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::Run(entry)
        ) if matches!(
            entry.root,
            exec::SelectedExecutableRunRoot::Terminal(_)
        )
    ));
}

#[test]
fn native_batch_boundary_accepts_reserved_terminal_chains() {
    let batch = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![support::query(ast_traversal::AstNode::Count {
            input: Box::new(ast_traversal::AstNode::Path {
                input: Box::new(support::node_source()),
            }),
        })],
        returns: Vec::new(),
    });

    let (entries, metrics) =
        support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(metrics.memo_groups > 0);
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::Run(entry)
        ) if matches!(
            entry.root,
            exec::SelectedExecutableRunRoot::Terminal(_)
        )
    ));
}

#[test]
fn native_batch_boundary_accepts_access_expansions() {
    let batch = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![support::query(ast_traversal::AstNode::Out {
            input: Box::new(support::node_source()),
            label: Some("LIKES".to_owned()),
        })],
        returns: Vec::new(),
    });

    let (entries, metrics) =
        support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(metrics.memo_groups > 0);
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::Run(entry)
        ) if matches!(
            entry.root,
            exec::SelectedExecutableRunRoot::Alternative(_)
        )
    ));
}

#[test]
fn native_batch_boundary_accepts_terminal_root_pipelines() {
    let batch = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![support::query(ast_traversal::AstNode::Limit {
            input: Box::new(ast_traversal::AstNode::Count {
                input: Box::new(support::node_source()),
            }),
            count: ast_expr::StreamBound::Literal(10),
        })],
        returns: Vec::new(),
    });

    let (entries, metrics) =
        support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(metrics.memo_groups > 0);
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::Run(entry)
        ) if matches!(
            entry.root,
            exec::SelectedExecutableRunRoot::Pipeline(_)
        )
    ));
}
