//! Native AST selection scope.
//!
//! Query roots and sub-traversal roots have different context semantics. This
//! tiny ADT makes that distinction explicit so top-level `$context` cannot be
//! accidentally planned while branch/repeat bodies can use the parent stream.

/// Scope in which a native AST root is being selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NativeAstScope {
    /// Public query root; implicit context is not bound.
    QueryRoot,
    /// Branch/repeat sub-traversal body; implicit context is the parent row.
    SubTraversal,
}

impl NativeAstScope {
    /// Whether `AstNode::Context` is valid in this scope.
    pub(super) const fn binds_context(self) -> bool {
        matches!(self, Self::SubTraversal)
    }
}
