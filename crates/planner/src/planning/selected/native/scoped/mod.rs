//! Scoped native AST-to-logical-root lowering.
//!
//! This facade keeps scoped native lowering split by contract. `entry` owns the
//! public dispatch order, `root_stream` owns recursive stream normalization, and
//! `pipeline` / `terminal` own the two stream-consuming wrapper families.

mod binding;
mod control_flow;
mod entry;
mod input_mutation;
mod pipeline;
mod root_stream;
mod terminal;
#[cfg(test)]
mod tests;

pub(super) use entry::{scoped_selectable_root_from_ast, ScopedSelectableRoot};

pub(in crate::planning::selected::native) use control_flow::{
    control_flow_from_ast, ControlFlowRoot,
};
pub(in crate::planning::selected::native::scoped) use entry::scoped_required_expr_from_ast;
