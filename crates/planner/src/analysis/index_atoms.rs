//! Predicate-to-secondary-index atom analysis facade.
//!
//! Equality, range, and range-value conversion contracts are split so index
//! eligibility rules can evolve independently without widening the analysis API.

mod candidate;
mod equality;
mod range;
mod value;

pub(super) use candidate::has_candidate;

pub(crate) use equality::{equality_atom, EqualityIndexAtom, EqualityIndexDomain};
pub(crate) use range::{range_atom, RangeIndexAtom};
