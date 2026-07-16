//! Selected stream-root suffix lowering.
//!
//! Selected child roots own their physical prefixes. This module validates that
//! selected pipeline and terminal roots contain only the parent-local suffix
//! before recursively lowering the child root-stream input.

mod input;
mod pipeline;
mod terminal;
