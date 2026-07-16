//! Native source-to-stream lowering facade.

#[cfg(test)]
mod result;
mod stream;
#[cfg(test)]
mod tests;

#[cfg(test)]
use helix_ast::traversal::AstNode;

use super::ast::NativeSourceAst;
#[cfg(test)]
use super::ast::NativeSourceAstMatch;
use crate::planning::selected::native::stream as native_stream;
use crate::{context, error};

#[cfg(test)]
pub(in crate::planning::selected::native) use result::NativeSourceStreamRoot;

/// Lower an already-recognized source AST shape into a native access stream.
pub(in crate::planning::selected::native) fn source_stream_from_source(
    ctx: &context::PlannerContext,
    source: NativeSourceAst<'_>,
) -> Result<native_stream::NativeAccessStream, error::PlannerError> {
    source.into_stream(ctx)
}

/// Try to lower a supported source AST node into a native access stream.
#[cfg(test)]
pub(in crate::planning::selected::native) fn source_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeSourceStreamRoot, error::PlannerError> {
    match NativeSourceAst::from_ast(root) {
        NativeSourceAstMatch::Source(source) => {
            source_stream_from_source(ctx, source).map(NativeSourceStreamRoot::Source)
        }
        NativeSourceAstMatch::NotSource => Ok(NativeSourceStreamRoot::NotSource),
    }
}
