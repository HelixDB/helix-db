//! Static access-window optimizer rule facade.
//!
//! Exploration, physical seeding, and rewrite outcome contracts live in
//! separate modules so each rule boundary is independently testable.

mod exploration;
mod implementation;
mod rewrite;

pub use exploration::AccessWindowRule;
pub use implementation::AccessWindowImplementationRule;
