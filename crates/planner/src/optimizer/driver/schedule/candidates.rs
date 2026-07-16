//! Ordered candidate-rule iteration.

mod features;
mod index;
mod list;
mod merge;

#[cfg(test)]
mod tests;

pub(super) use self::features::FeatureCandidates;
pub(super) use self::index::RuleIndex;
pub(super) use self::list::{CandidateList, CandidateSlice};
pub(in crate::optimizer::driver) use self::merge::RuleCandidates;
