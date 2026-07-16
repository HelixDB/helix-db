//! Memo-child extraction for supported root-stream wrappers.
//!
//! Access and variable-source inputs are lowered as parent-local prefixes, so
//! they remain embedded in the parent expression. Recursive stream-producing
//! roots are selected independently and therefore become memo children.

use super::*;

pub(super) fn pipeline_children(pipeline: &RootPipeline) -> Vec<LogicalExpr> {
    root_stream_child(pipeline.input())
}

pub(super) fn reserved_children(reserved: &StreamReserved) -> Vec<LogicalExpr> {
    root_stream_child(reserved.input())
}

pub(super) fn project_children(project: &StreamProject) -> Vec<LogicalExpr> {
    root_stream_child(project.input())
}

pub(super) fn aggregate_children(aggregate: &StreamAggregate) -> Vec<LogicalExpr> {
    root_stream_child(aggregate.input())
}

pub(super) fn variable_write_children(write: &StreamVariableWrite) -> Vec<LogicalExpr> {
    root_stream_child(write.input())
}

fn root_stream_child(input: &RootStream) -> Vec<LogicalExpr> {
    match input {
        RootStream::Access(_) | RootStream::VariableSource(_) => Vec::new(),
        RootStream::Mutation(mutation) => {
            vec![LogicalExpr::RootMutation(mutation.as_ref().clone())]
        }
        RootStream::Branch(branch) => vec![LogicalExpr::RootBranch(branch.as_ref().clone())],
        RootStream::Repeat(repeat) => vec![LogicalExpr::RootRepeat(repeat.as_ref().clone())],
        RootStream::Pipeline(pipeline) => {
            vec![LogicalExpr::RootPipeline(pipeline.as_ref().clone())]
        }
        RootStream::Reserved(reserved) => {
            vec![LogicalExpr::StreamReserved(reserved.as_ref().clone())]
        }
        RootStream::Project(project) => {
            vec![LogicalExpr::StreamProject(project.as_ref().clone())]
        }
        RootStream::Aggregate(aggregate) => {
            vec![LogicalExpr::StreamAggregate(aggregate.as_ref().clone())]
        }
        RootStream::VariableWrite(write) => {
            vec![LogicalExpr::StreamVariableWrite(write.as_ref().clone())]
        }
    }
}
