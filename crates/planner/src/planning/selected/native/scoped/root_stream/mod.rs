//! Scoped root-stream normalization.
//!
//! This contract admits supported roots as stream inputs while honoring scoped
//! context binding and recursively selected branch/repeat/mutation/terminal
//! roots.

mod access_pipeline;
mod context_binding;
mod control_flow;
mod mutation;
mod terminal;
mod variable;

use helix_ast::traversal::AstNode;

use super::super::family;
use super::super::rejection::{self, NativeUnsupportedReason};
use super::super::scope::NativeAstScope;
use crate::{context, error, logical};

/// Scoped root-stream recognition result.
#[derive(Debug)]
pub(super) enum ScopedRootStream {
    /// The AST root is a validated root stream in this scope.
    Stream(Box<logical::RootStream>),
    /// The AST root is not a root stream in this scope.
    NotRootStream,
}

impl ScopedRootStream {
    #[cfg(test)]
    pub(super) fn expect_stream(self, message: &str) -> logical::RootStream {
        match self {
            Self::Stream(stream) => *stream,
            Self::NotRootStream => panic!("{message}"),
        }
    }
}

pub(super) fn root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedRootStream, error::PlannerError> {
    match family::NativeAstFamily::from_ast(root) {
        family::NativeAstFamily::Terminal => {
            terminal::terminal_root_stream_from_ast(ctx, root, scope)
        }
        family::NativeAstFamily::VariableSource => {
            variable::variable_source_root_stream_from_ast(root)
        }
        family::NativeAstFamily::SourceMutation => {
            mutation::mutation_root_stream_from_ast(ctx, root, scope)
        }
        family::NativeAstFamily::Context => context_binding::context_root_stream(scope),
        family::NativeAstFamily::ControlFlow => {
            control_flow::control_flow_root_stream_from_ast(ctx, root, scope)
        }
        family::NativeAstFamily::AccessOrPipeline => {
            access_pipeline::access_or_pipeline_root_stream_from_ast(ctx, root, scope)
        }
        family::NativeAstFamily::IndexDdl | family::NativeAstFamily::ShortestPath => {
            Ok(ScopedRootStream::NotRootStream)
        }
    }
}

pub(super) fn required_root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<logical::RootStream, error::PlannerError> {
    match root_stream_from_ast(ctx, root, scope)? {
        ScopedRootStream::Stream(stream) => Ok(*stream),
        ScopedRootStream::NotRootStream => Err(rejection::unsupported(
            NativeUnsupportedReason::RootStreamInputUnsupported,
        )),
    }
}
