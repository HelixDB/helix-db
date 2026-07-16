//! Selected root source/physical compatibility classification.
//!
//! The optimizer returns a source logical expression plus a chosen physical
//! alternative. This module classifies that pair before construction starts, so
//! selected-root builders can assume their logical and physical families match.

mod classify;
mod mismatch;

#[cfg(test)]
mod tests;

use super::terminal::TerminalRootPayload;
use crate::{exec, logical};

pub(super) enum SelectedRootPlanCase<'a> {
    IndexDdl(&'a logical::RootIndexDdl),
    Mutation(&'a logical::RootMutation),
    Branch(&'a logical::RootBranch),
    Repeat(&'a logical::RootRepeat),
    ShortestPath(&'a logical::RootShortestPath),
    Pipeline(&'a logical::RootPipeline),
    Terminal(TerminalRootPayload<'a>),
    GenericAlternative(exec::SelectedExecutableAlternativeFamily),
}
