//! Access ordering and distinct optimizer rule facade.
//!
//! Range-direction exploration, order elision/seeding, and distinct
//! elision/seeding are separate contract modules behind this facade.

mod distinct;
mod ordering;
mod range_direction;
mod shared;

pub use distinct::{AccessDistinctImplementationRule, AccessDistinctRule};
pub use ordering::{AccessOrderImplementationRule, AccessOrderRule};
pub use range_direction::AccessOrderRangeDirectionRule;
