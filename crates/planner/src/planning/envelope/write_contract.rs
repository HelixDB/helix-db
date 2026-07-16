//! Write-batch surface validation.
//!
//! Write batches may still contain stream consumers around mutations, but
//! row-local binding capture and binding projection are read-only contracts.

use helix_ast::batch::{BatchEntry, WriteBatch};
use helix_ast::traversal::AstNode;

use crate::error;

pub(super) fn validate_write_batch(batch: &WriteBatch) -> Result<(), error::PlannerError> {
    validate_write_entries(&batch.entries)
}

fn validate_write_entries(entries: &[BatchEntry]) -> Result<(), error::PlannerError> {
    entries.iter().try_for_each(|entry| match entry {
        BatchEntry::Query(query) => validate_write_root(&query.root),
        BatchEntry::ForEach { body, .. } => validate_write_entries(body),
    })
}

fn validate_write_root(root: &AstNode) -> Result<(), error::PlannerError> {
    match root {
        AstNode::Bind { .. } => Err(error::PlannerError::ReadOnlyTraversalInWriteBatch {
            op: error::ReadOnlyWriteOp::Bind,
        }),
        AstNode::ProjectBindings { .. } => {
            Err(error::PlannerError::ReadOnlyTraversalInWriteBatch {
                op: error::ReadOnlyWriteOp::ProjectBindings,
            })
        }
        AstNode::Context
        | AstNode::Nodes { .. }
        | AstNode::NodesWhere { .. }
        | AstNode::Edges { .. }
        | AstNode::EdgesWhere { .. }
        | AstNode::VectorSearchNodes { .. }
        | AstNode::TextSearchNodes { .. }
        | AstNode::VectorSearchEdges { .. }
        | AstNode::TextSearchEdges { .. }
        | AstNode::CreateIndex { .. }
        | AstNode::DropIndex { .. }
        | AstNode::GetIndexOperation { .. }
        | AstNode::RetryIndexOperation { .. }
        | AstNode::AbortIndexOperation { .. }
        | AstNode::ShortestPath { .. }
        | AstNode::AddN { input: None, .. }
        | AstNode::DropEdgeById { input: None, .. }
        | AstNode::Inject { input: None, .. } => Ok(()),
        AstNode::Out { input, .. }
        | AstNode::In { input, .. }
        | AstNode::Both { input, .. }
        | AstNode::OutE { input, .. }
        | AstNode::InE { input, .. }
        | AstNode::BothE { input, .. }
        | AstNode::OutN { input }
        | AstNode::InN { input }
        | AstNode::OtherN { input }
        | AstNode::Has { input, .. }
        | AstNode::HasLabel { input, .. }
        | AstNode::HasKey { input, .. }
        | AstNode::Where { input, .. }
        | AstNode::Dedup { input }
        | AstNode::Within { input, .. }
        | AstNode::Without { input, .. }
        | AstNode::EdgeHas { input, .. }
        | AstNode::EdgeHasLabel { input, .. }
        | AstNode::Limit { input, .. }
        | AstNode::Skip { input, .. }
        | AstNode::Range { input, .. }
        | AstNode::As { input, .. }
        | AstNode::Store { input, .. }
        | AstNode::Select { input, .. }
        | AstNode::Inject {
            input: Some(input), ..
        }
        | AstNode::Count { input }
        | AstNode::Exists { input }
        | AstNode::Id { input }
        | AstNode::Label { input }
        | AstNode::Values { input, .. }
        | AstNode::ValueMap { input, .. }
        | AstNode::Project { input, .. }
        | AstNode::EdgeProperties { input }
        | AstNode::AddN {
            input: Some(input), ..
        }
        | AstNode::AddE { input, .. }
        | AstNode::SetProperty { input, .. }
        | AstNode::RemoveProperty { input, .. }
        | AstNode::Drop { input }
        | AstNode::DropEdge { input, .. }
        | AstNode::DropEdgeLabeled { input, .. }
        | AstNode::DropEdgeById {
            input: Some(input), ..
        }
        | AstNode::OrderBy { input, .. }
        | AstNode::OrderByMultiple { input, .. }
        | AstNode::Group { input, .. }
        | AstNode::GroupCount { input, .. }
        | AstNode::AggregateBy { input, .. }
        | AstNode::Fold { input }
        | AstNode::Unfold { input }
        | AstNode::Path { input }
        | AstNode::SimplePath { input }
        | AstNode::WithSack { input, .. }
        | AstNode::SackSet { input, .. }
        | AstNode::SackAdd { input, .. }
        | AstNode::SackGet { input } => validate_write_root(input),
        AstNode::Repeat { input, config } => {
            validate_write_root(input)?;
            validate_write_root(&config.traversal.root)
        }
        AstNode::Union { input, traversals } | AstNode::Coalesce { input, traversals } => {
            validate_write_root(input)?;
            traversals
                .iter()
                .try_for_each(|traversal| validate_write_root(&traversal.root))
        }
        AstNode::Choose {
            input,
            then_traversal,
            else_traversal,
            ..
        } => {
            validate_write_root(input)?;
            validate_write_root(&then_traversal.root)?;
            else_traversal
                .as_ref()
                .map(|traversal| validate_write_root(&traversal.root))
                .unwrap_or(Ok(()))
        }
        AstNode::Optional { input, traversal } => {
            validate_write_root(input)?;
            validate_write_root(&traversal.root)
        }
    }
}
