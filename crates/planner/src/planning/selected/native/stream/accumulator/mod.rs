//! Native access-stream accumulator.
//!
//! The accumulator keeps source-rooted AST lowering honest: a native stream
//! always has one residual-free access path plus zero or more validated stream
//! operators. It lowers to the most specific logical access ADT available.

mod lowering;
mod ops;
mod window;

use crate::{logical, planning};

/// Access-rooted stream assembled directly from AST wrappers.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::planning::selected::native) struct NativeAccessStream {
    access: logical::AccessPath,
    ops: Vec<logical::StreamPipelineOp>,
}

impl NativeAccessStream {
    /// Start a native stream from a residual-free access path.
    pub(in crate::planning::selected::native) fn new(
        access: planning::selected::native::access::NativeAccessPath,
    ) -> Self {
        Self {
            access: access.into_logical(),
            ops: Vec::new(),
        }
    }
}
