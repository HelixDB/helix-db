//! Cost contracts and tunable planner cost model.
//!
//! The public API intentionally re-exports narrow submodules so call sites can
//! continue using `crate::cost::CostVector` while the implementation keeps
//! independent contracts for units, vector composition, and experiment
//! profiles.

mod profile;
mod units;
mod vector;

pub use profile::{StorageCostProfile, StorageCostProfileOverrides};
pub use units::{
    ByteEstimate, EstimatedRows, EstimatedRowsAtMost, LatencyEstimate, Selectivity,
    UniqueEqualityRows,
};
pub use vector::CostVector;

#[cfg(test)]
mod tests;
