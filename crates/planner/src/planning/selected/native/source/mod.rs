//! Native source-shape recognition facade.
//!
//! Source recognition is the lowest native AST boundary above access-path
//! construction. Child modules separate AST shape recognition from validated
//! stream construction so catalog/search lookup logic does not live beside the
//! raw traversal pattern match.

mod ast;
mod lowering;

pub(in crate::planning::selected::native) use self::ast::{NativeSourceAst, NativeSourceAstMatch};
pub(super) use self::lowering::source_stream_from_source;
