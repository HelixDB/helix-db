//! Access-set rule contract tests.
//!
//! The test modules mirror `rules::access::sets` proof families so
//! canonicalization, range tightening, equality/range proofs, contradiction,
//! and subsumption behavior can evolve independently.

mod canonical;
mod contradiction;
mod equality_range_intersection;
mod equality_range_union;
mod range;
mod subsumption;

use super::*;
