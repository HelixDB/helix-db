//! Root-stream recognition for source mutation AST roots.

use helix_ast::traversal::AstNode;

use super::NativeRootStream;
use crate::planning::selected::native::mutation;
use crate::{error, logical};

pub(super) fn source_mutation_root_stream_from_ast(
    root: &AstNode,
) -> Result<NativeRootStream, error::PlannerError> {
    match mutation::native_mutation_from_ast(root)? {
        mutation::NativeMutationRoot::Source(mutation) => {
            return Ok(NativeRootStream::Stream(Box::new(
                logical::RootStream::Mutation(Box::new(mutation)),
            )));
        }
        mutation::NativeMutationRoot::InputConsuming
        | mutation::NativeMutationRoot::NotMutation => {}
    }
    Ok(NativeRootStream::NotRootStream)
}
