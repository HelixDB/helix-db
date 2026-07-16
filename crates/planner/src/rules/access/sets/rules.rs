//! Optimizer rule wrappers for residual-free access set algebra.

mod canonical;
mod contradiction;
mod equality_range;
mod range;
mod subsumption;

pub use self::{
    canonical::AccessSetSimplificationRule,
    contradiction::AccessContradictionRule,
    equality_range::{AccessEqualityRangeIntersectionRule, AccessEqualityRangeUnionRule},
    range::AccessRangeIntersectionRule,
    subsumption::AccessSubsumptionRule,
};
