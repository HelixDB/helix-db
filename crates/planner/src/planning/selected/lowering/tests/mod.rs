//! Selected executable reconstruction contract tests.
//!
//! These tests mirror selected-lowering boundaries so recursive memo-child,
//! stream-input, mutation, and branch/repeat behavior can be
//! extended independently.

mod flow;
mod memo_children;
mod memo_context;
mod mutation;
mod roots;
mod stream;
mod support;
