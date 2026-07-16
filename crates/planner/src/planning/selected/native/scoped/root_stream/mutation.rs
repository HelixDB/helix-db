//! Scoped root-stream recognition for source and input-consuming mutations.

use helix_ast::traversal::AstNode;

use super::super::input_mutation;
use super::ScopedRootStream;
use crate::planning::selected::native::mutation as source_mutation;
use crate::planning::selected::native::scope::NativeAstScope;
use crate::{context, error, logical};

pub(super) fn mutation_root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedRootStream, error::PlannerError> {
    match source_mutation::native_mutation_from_ast(root)? {
        source_mutation::NativeMutationRoot::Source(mutation) => {
            return Ok(ScopedRootStream::Stream(Box::new(
                logical::RootStream::Mutation(Box::new(mutation)),
            )));
        }
        source_mutation::NativeMutationRoot::InputConsuming
        | source_mutation::NativeMutationRoot::NotMutation => {}
    }
    match input_mutation::input_mutation_from_ast(ctx, root, scope)? {
        input_mutation::InputMutationRoot::Mutation(mutation) => {
            return Ok(ScopedRootStream::Stream(Box::new(
                logical::RootStream::Mutation(Box::new(mutation)),
            )));
        }
        input_mutation::InputMutationRoot::SourceOnly
        | input_mutation::InputMutationRoot::NotMutation => {}
    }
    Ok(ScopedRootStream::NotRootStream)
}
