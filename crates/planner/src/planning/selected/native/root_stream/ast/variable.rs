//! Root-stream recognition for source variable AST roots.

use helix_ast::traversal::AstNode;

use super::NativeRootStream;
use crate::planning::selected::native::variable_source;
use crate::{error, logical};

pub(super) fn variable_source_root_stream_from_ast(
    root: &AstNode,
) -> Result<NativeRootStream, error::PlannerError> {
    match variable_source::native_variable_source_from_ast(root)? {
        variable_source::NativeVariableSourceRoot::Source(source) => {
            return Ok(NativeRootStream::Stream(Box::new(
                logical::RootStream::VariableSource(source),
            )));
        }
        variable_source::NativeVariableSourceRoot::InputConsuming
        | variable_source::NativeVariableSourceRoot::NotVariableSource => {}
    }
    Ok(NativeRootStream::NotRootStream)
}
