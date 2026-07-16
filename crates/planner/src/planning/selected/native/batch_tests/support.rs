use super::super::batch::cascades_batch_entries_from_ast;
use crate::{catalog, context, error, exec};
use helix_ast::{batch, graph, traversal};

pub(super) fn query(root: traversal::AstNode) -> batch::BatchEntry {
    batch::BatchEntry::Query(Box::new(batch::NamedQuery {
        name: Some("items".to_owned()),
        root,
        condition: None,
    }))
}

pub(super) fn conditional_query(
    root: traversal::AstNode,
    condition: batch::BatchCondition,
) -> batch::BatchEntry {
    batch::BatchEntry::Query(Box::new(batch::NamedQuery {
        name: Some("items".to_owned()),
        root,
        condition: Some(condition),
    }))
}

pub(super) fn node_source() -> traversal::AstNode {
    traversal::AstNode::Nodes {
        reference: graph::NodeRef::All,
    }
}

pub(super) fn edge_source() -> traversal::AstNode {
    traversal::AstNode::Edges {
        reference: graph::EdgeRef::All,
    }
}

pub(super) fn lower_batch(
    query: &batch::BatchQuery,
    ctx: &context::PlannerContext,
) -> Result<(exec::SelectedExecutableBatchEntries, exec::PlannerMetrics), error::PlannerError> {
    cascades_batch_entries_from_ast(query, ctx)
}

pub(super) fn selected_group(root: &exec::SelectedExecutableRunRoot) -> usize {
    match root {
        exec::SelectedExecutableRunRoot::Alternative(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::Mutation(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::IndexDdl(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::Branch(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::Repeat(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::ShortestPath(root) => {
            root.provenance().optimizer().group()
        }
        exec::SelectedExecutableRunRoot::Pipeline(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::Terminal(root) => root.provenance().optimizer().group(),
    }
    .get()
}

pub(super) fn search_ctx() -> context::PlannerContext {
    context::PlannerContext {
        indexes: catalog::IndexCatalogSnapshot::default().with_vector(
            catalog::SearchIndexKey::try_new(catalog::ElementKind::Node, "Doc", "embedding")
                .unwrap(),
            catalog::SearchIndexScope::Unscoped,
        ),
        ..context::PlannerContext::default()
    }
}
