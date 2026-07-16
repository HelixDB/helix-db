//! Native AST-to-root-stream normalization.
//!
//! Root-stream normalization is the shared contract boundary for terminals and
//! root pipelines. `ast` recognizes supported AST roots, while `expr` owns the
//! pure logical-expression to root-stream conversion shared by scoped lowering.

mod ast;
mod expr;
#[cfg(test)]
mod tests;

pub(super) use ast::required_root_stream_from_ast;
#[cfg(test)]
pub(super) use ast::{root_stream_from_ast, NativeRootStream};
pub(in crate::planning::selected::native) use expr::root_stream_from_expr;
