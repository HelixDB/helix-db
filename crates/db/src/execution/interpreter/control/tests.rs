//! Executable control-flow interpreter integration contracts.
//!
//! Shared subplan builders live in `support`; sibling modules own branch,
//! repeat, and foreach behavior families.

mod branch;
mod foreach;
mod repeat;
mod support;
