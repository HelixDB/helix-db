//! Root-family optimizer rules.
//!
//! This facade wires root-only contracts by boundary: stream pipelines,
//! barrier roots, and control-flow roots. Each submodule owns the rule logic
//! for one executable contract surface.

mod barrier;
mod control_flow;
mod pipeline;
mod stream_access;

pub use self::{
    barrier::{
        RootIndexDdlImplementationRule, RootMutationImplementationRule,
        RootShortestPathImplementationRule,
    },
    control_flow::{
        RootBranchImplementationRule, RootControlFlowEmptyRule, RootRepeatImplementationRule,
    },
    pipeline::RootPipelineImplementationRule,
    stream_access::RootStreamAccessRewriteRule,
};
