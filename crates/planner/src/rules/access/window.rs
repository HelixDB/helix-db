//! Static access-window rewrite facade.
//!
//! The public rule types stay at `rules::access::window`, while node-specific,
//! edge-specific, and shared search/window proof helpers live in smaller
//! contract modules.

mod contracts;
mod edge;
mod node;
mod rule;
mod shared;
mod support;

pub use rule::{AccessWindowImplementationRule, AccessWindowRule};
