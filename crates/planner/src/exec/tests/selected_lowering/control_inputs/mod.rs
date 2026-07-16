//! Selected control/root-stream input lowering tests.
//!
//! These modules cover direct control inputs, reserved terminal chains,
//! reserved pipeline chains, and pipeline-to-terminal chains independently.

mod direct;
mod pipeline_terminals;
mod reserved_pipelines;
mod reserved_terminals;

use super::super::*;
