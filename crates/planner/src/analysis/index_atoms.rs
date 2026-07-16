//! Predicate-to-secondary-index atom analysis facade.
//!
//! Equality, range, and range-value conversion contracts are split so index
//! eligibility rules can evolve independently without widening the analysis API.

mod equality;
mod range;
mod value;

pub(crate) use equality::{equality_atom, EqualityIndexAtom};
pub(crate) use range::{range_atom, RangeIndexAtom};
