//! Policy-neutral runtime types shared by vector search and mutation.
//!
//! This module owns validated in-memory graph values only. It must not create
//! database keys, inspect raw bytes, or perform I/O; storage and session modules
//! consume these types after their invariants have been established here.

use crate::encoding::NodeId;
use crate::error::HelixDbError;

use super::DistanceScore;

/// Distance-ranked graph candidate with a finite, nonnegative score.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Candidate {
    pub(crate) node_id: NodeId,
    distance: DistanceScore,
}

impl Candidate {
    /// Validates kernel output before it can enter any ordered search structure.
    pub(crate) fn try_new(node_id: NodeId, distance: f32) -> Result<Self, HelixDbError> {
        let distance = DistanceScore::try_new(distance).map_err(|error| {
            HelixDbError::InvariantViolation(format!(
                "vector distance kernel emitted an invalid score: {error}"
            ))
        })?;
        Ok(Self { node_id, distance })
    }

    /// Returns the descriptor-defined f32 score for computation/materialization.
    pub(crate) const fn score(self) -> f32 {
        self.distance.get()
    }

    /// Returns the validated score without reopening the numeric boundary.
    pub(crate) const fn distance(self) -> DistanceScore {
        self.distance
    }
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance && self.node_id == other.node_id
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .cmp(&other.distance)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_rejects_invalid_scores_and_orders_ties_by_node() {
        assert!(Candidate::try_new(1, f32::NAN).is_err());
        assert!(Candidate::try_new(1, -1.0).is_err());
        assert!(Candidate::try_new(1, 1.0).unwrap() < Candidate::try_new(2, 1.0).unwrap());
        assert_eq!(Candidate::try_new(7, 0.25).unwrap().score(), 0.25);
    }
}
