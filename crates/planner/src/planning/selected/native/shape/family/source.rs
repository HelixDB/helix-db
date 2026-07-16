//! Native access-stream source-shape recognition.

use helix_ast::traversal::AstNode;

use crate::planning::selected::native::source;

/// Native access-stream source-shape recognition result.
pub(super) enum NativeAccessStreamSourceMatch<'a> {
    /// The AST root is a recognized source and carries its typed payload.
    Source(source::NativeSourceAst<'a>),
    /// The AST root is not a source shape.
    NotSource,
}

pub(super) fn source_from_ast(root: &AstNode) -> NativeAccessStreamSourceMatch<'_> {
    match source::NativeSourceAst::from_ast(root) {
        source::NativeSourceAstMatch::Source(source) => {
            NativeAccessStreamSourceMatch::Source(source)
        }
        source::NativeSourceAstMatch::NotSource => NativeAccessStreamSourceMatch::NotSource,
    }
}
