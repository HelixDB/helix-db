//! Selected stream-terminal executable lowering contracts.
//!
//! These tests are split by root-stream input family so failures point at the
//! contract that changed: access-rooted terminals, reserved terminal sources,
//! reserved terminal chains, and reserved root pipelines.

mod access_terminals;
mod reserved_pipelines;
mod reserved_source;
mod reserved_terminals;
