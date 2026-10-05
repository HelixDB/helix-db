//! Executable stream interpreter integration contracts.
//!
//! Shared stream-plan builders live in `support`; sibling modules own bounds/set,
//! terminal, projection/order, aggregate, and dependency behavior families.

mod aggregate;
mod bounds_sets;
mod dependencies;
mod membership;
mod membership_retention;
mod projection_order;
mod record_batches;
mod support;
mod terminals;
