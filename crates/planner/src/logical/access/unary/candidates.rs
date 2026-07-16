//! Scheduler candidate predicates for unary access wrappers.
//!
//! These predicates are deliberately conservative. They may return cheap false
//! positives, but every currently implemented rewrite family must return true
//! so the optimizer schedule can skip impossible rule probes without hiding a
//! valid rewrite.

mod distinct;
mod order;
mod window;

#[cfg(test)]
mod tests;

pub(super) use distinct::distinct_has_noop_candidate;
pub(super) use order::{order_has_order_elision_candidate, order_has_range_direction_candidate};
pub(super) use window::window_has_rewrite_candidate;
