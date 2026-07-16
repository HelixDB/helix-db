//! Native access-stream shape classification facade.
//!
//! Source recognition, wrapper recognition, and the final three-way
//! classification live behind this module so recursive lowering can consume a
//! small ADT instead of re-matching raw AST variants.

mod classify;
mod source;
mod wrapper;

pub(super) use classify::access_stream_shape_from_ast;
pub(super) use wrapper::NativeAccessStreamWrapper;

/// Native access-stream recognition family.
pub(super) enum NativeAccessStreamShape<'a> {
    /// Source roots can start a native access stream.
    Source(crate::planning::selected::native::source::NativeSourceAst<'a>),
    /// Stream wrapper with its recursive input and pending append operation.
    Wrapper(NativeAccessStreamWrapper<'a>),
    /// The AST root is not source-rooted access.
    NotAccessStream,
}
