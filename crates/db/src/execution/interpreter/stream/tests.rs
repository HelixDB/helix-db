//! Executable stream interpreter integration contracts.
//!
//! Shared stream-plan builders live in `support`; sibling modules own bounds/set,
//! terminal, projection/order, aggregate, and dependency behavior families.

mod aggregate;
mod bounds_sets;
mod dependencies;
mod projection_order;
mod support;
mod terminals;
