//! Executable mutation interpreter integration contracts.
//!
//! Shared plan builders live in `support`; each sibling module owns one runtime
//! behavior family so mutation coverage can grow without one broad test body.

mod graph_lifecycle;
mod support;
