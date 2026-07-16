//! AST root-stream recognition for unscoped selected roots.

mod access_pipeline;
mod control_flow;
mod mutation;
mod terminal;
mod variable;

use helix_ast::traversal::AstNode;

use crate::planning::selected::native::family;
use crate::planning::selected::native::rejection::{self, NativeUnsupportedReason};
use crate::{context, error, logical};

/// Native root-stream recognition result.
#[derive(Debug)]
pub(in crate::planning::selected::native) enum NativeRootStream {
    /// The AST root is a validated root stream.
    Stream(Box<logical::RootStream>),
    /// The AST root is not a root stream.
    NotRootStream,
}

impl NativeRootStream {
    #[cfg(test)]
    pub(super) fn expect_stream(self, message: &str) -> logical::RootStream {
        match self {
            Self::Stream(stream) => *stream,
            Self::NotRootStream => panic!("{message}"),
        }
    }
}

pub(in crate::planning::selected::native) fn root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeRootStream, error::PlannerError> {
    match family::NativeAstFamily::from_ast(root) {
        family::NativeAstFamily::Terminal => terminal::terminal_root_stream_from_ast(ctx, root),
        family::NativeAstFamily::VariableSource => {
            variable::variable_source_root_stream_from_ast(root)
        }
        family::NativeAstFamily::SourceMutation => {
            mutation::source_mutation_root_stream_from_ast(root)
        }
        family::NativeAstFamily::ControlFlow => {
            control_flow::control_flow_root_stream_from_ast(ctx, root)
        }
        family::NativeAstFamily::AccessOrPipeline => {
            access_pipeline::access_or_pipeline_root_stream_from_ast(ctx, root)
        }
        family::NativeAstFamily::IndexDdl
        | family::NativeAstFamily::ShortestPath
        | family::NativeAstFamily::Context => Ok(NativeRootStream::NotRootStream),
    }
}

pub(in crate::planning::selected::native) fn required_root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<logical::RootStream, error::PlannerError> {
    match root_stream_from_ast(ctx, root)? {
        NativeRootStream::Stream(stream) => Ok(*stream),
        NativeRootStream::NotRootStream => Err(rejection::unsupported(
            NativeUnsupportedReason::RootStreamInputUnsupported,
        )),
    }
}
