//! Stable selected-root reconstruction rejection reasons.
//!
//! Selected lowering consumes optimizer output and reconstructs interpreter-facing
//! selected executable roots. Failures here indicate a mismatch between the
//! selected logical/physical contract, memo-child provenance, and reconstructed
//! selected child roots.

mod planner;
mod reason;
mod translation;

pub(in crate::planning::selected) use planner::unsupported;
pub(in crate::planning::selected) use reason::Reason;
pub(in crate::planning::selected) use translation::{
    unsupported_alternative_construction, unsupported_root_construction,
};

#[cfg(test)]
mod tests;
