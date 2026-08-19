//! Memo-child extraction for logical expressions.
//!
//! The logical algebra owns the question of which immediate inputs should
//! become independently selected memo child groups. Parent-local payloads such
//! as access sources or variable-source prefixes stay inside the parent logical
//! expression, so selected reconstruction never receives child plans it will not
//! consume. Each submodule owns one expression family so recursive selected
//! reconstruction can evolve without broadening one large dispatcher.

mod access;
mod control;
mod mutation;
mod root_stream;

use super::*;

impl LogicalExpr {
    /// Logical child expressions that should be represented as memo child groups.
    ///
    /// Leaf expressions and parent-local wrappers return an empty vector.
    /// Recursive executable-root inputs return their immediate selected child
    /// contracts, preserving local payloads in the parent expression while
    /// giving separately planned child roots stable memo-group lineage.
    pub fn memo_children(&self) -> Vec<Self> {
        match self {
            Self::AccessFilter(filter) => access::filter_children(filter),
            Self::AccessWindow(window) => access::window_children(window),
            Self::AccessOrder(order) => access::order_children(order),
            Self::AccessDistinct(distinct) => access::distinct_children(distinct),
            Self::AccessPipeline(pipeline) => access::pipeline_children(pipeline),
            Self::RootPipeline(pipeline) => root_stream::pipeline_children(pipeline),
            Self::StreamReserved(reserved) => root_stream::reserved_children(reserved),
            Self::StreamProject(project) => root_stream::project_children(project),
            Self::StreamCardinality(cardinality) => root_stream::cardinality_children(cardinality),
            Self::StreamAggregate(aggregate) => root_stream::aggregate_children(aggregate),
            Self::StreamVariableWrite(write) => root_stream::variable_write_children(write),
            Self::RootMutation(mutation) => mutation::children(mutation),
            Self::RootBranch(branch) => control::branch_children(branch),
            Self::RootRepeat(repeat) => control::repeat_children(repeat),
            Self::Pure(_)
            | Self::VariableSource(_)
            | Self::PurePipeline(_)
            | Self::FilterChain(_)
            | Self::FilterPushdown(_)
            | Self::AccessPath(_)
            | Self::RootIndexDdl(_)
            | Self::RootShortestPath(_)
            | Self::Barrier(_) => Vec::new(),
        }
    }
}
