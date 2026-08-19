//! Native AST-to-selected-root lowering.
//!
//! This boundary accepts AST shapes that can be represented as complete
//! Cascades logical roots. Recursive branch, repeat, mutation, terminal, and
//! pipeline children are lowered through scoped contract modules so selected
//! execution no longer needs direct native wrapper plans.

mod access;
mod batch;
#[cfg(test)]
mod batch_tests;
mod conditions;
mod context_usage;
mod control_flow;
mod entries;
mod equality_bindings;
mod expansion;
mod family;
mod index_ddl;
mod mutation;
mod names;
mod ordering;
mod pipeline;
#[cfg(test)]
mod pipeline_tests;
mod projection;
mod rejection;
mod reserved;
mod root;
mod root_stream;
mod scope;
mod scoped;
mod shape;
mod shortest_path;
mod source;
mod stream;
mod terminal;
mod variable_source;
mod variables;

pub(in crate::planning) use self::batch::{
    cascades_batch_entries_from_ast, cascades_batch_entries_from_ast_entries,
};
