//! Access window, order, and distinct rule contract tests.
//!
//! Child modules keep logical window rewrites, physical window lowering,
//! range-direction proofs, explicit ordering, and distinct handling separate.

mod distinct;
mod order;
mod range_direction;
mod window;
mod window_impl;

use super::*;
