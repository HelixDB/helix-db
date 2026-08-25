//! Recursive AST wrapper walk for native access streams.

use helix_ast::traversal::AstNode;

use super::family;
use super::result::NativeAccessStreamRoot;
use crate::planning::selected::native::source;
use crate::{context, error};

/// Lower a supported source-rooted AST shape into a native access stream.
pub(in crate::planning::selected::native) fn native_access_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeAccessStreamRoot, error::PlannerError> {
    match family::access_stream_shape_from_ast(root) {
        family::NativeAccessStreamShape::Source(source) => {
            source_access_stream_from_source(ctx, source)
        }
        family::NativeAccessStreamShape::Wrapper(wrapper) => {
            wrapped_access_stream_from_ast(ctx, wrapper)
        }
        family::NativeAccessStreamShape::NotAccessStream => {
            Ok(NativeAccessStreamRoot::NotAccessStream)
        }
    }
}

fn source_access_stream_from_source(
    ctx: &context::PlannerContext,
    source: source::NativeSourceAst<'_>,
) -> Result<NativeAccessStreamRoot, error::PlannerError> {
    source::source_stream_from_source(ctx, source).map(NativeAccessStreamRoot::Stream)
}

fn wrapped_access_stream_from_ast(
    ctx: &context::PlannerContext,
    wrapper: family::NativeAccessStreamWrapper<'_>,
) -> Result<NativeAccessStreamRoot, error::PlannerError> {
    match native_access_stream_from_ast(ctx, wrapper.input())? {
        NativeAccessStreamRoot::Stream(stream) => wrapper
            .into_op()
            .append_to(ctx, stream)
            .map(NativeAccessStreamRoot::Stream),
        NativeAccessStreamRoot::NotAccessStream => Ok(NativeAccessStreamRoot::NotAccessStream),
    }
}
