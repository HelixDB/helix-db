//! Selected access-stream wrapper lowering.
//!
//! Each module owns one logical `AccessStream` wrapper family so shape matching,
//! suffix validation, and executable step allocation remain independently
//! testable.

mod dispatch;
mod distinct;
mod filter;
mod order;
mod pipeline;
mod window;
