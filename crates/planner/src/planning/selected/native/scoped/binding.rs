//! Scoped context bindings.
//!
//! Branch/repeat sub-traversals may start from `AstNode::Context`. This module
//! centralizes the synthetic variable contract used to represent that input.

use crate::{ir, logical};

pub(super) fn context_variable_source() -> logical::VariableSource {
    logical::VariableSource::new(ir::NonEmptyString::from_static("$context"))
}
