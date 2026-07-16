//! Root-selection summary contracts.

use serde::{Deserialize, Serialize};

use super::SelectionError;
use crate::{cost, ir, memo};

/// Selection failure for one independently optimized root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootSelectionFailure {
    /// Root group whose selected implementation could not be resolved.
    pub root: memo::MemoGroupId,
    /// Typed failure reason.
    pub error: SelectionError,
}

/// Recursive selection summary across all independently optimized roots.
///
/// Incomplete selection carries a non-empty failure list so callers cannot
/// confuse a partial selected-cost sum with a complete executable plan.
///
/// # Examples
///
/// ```
/// use helix_planner::{cost, optimizer};
///
/// let summary = optimizer::RootSelectionSummary::Complete {
///     selected_cost: cost::CostVector::ZERO,
/// };
///
/// assert_eq!(summary.complete_selected_cost(), Some(cost::CostVector::ZERO));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RootSelectionSummary {
    /// Every requested root has a selected implementation.
    Complete {
        /// Serially composed selected root cost.
        selected_cost: cost::CostVector,
    },
    /// At least one requested root has no selected implementation.
    Incomplete {
        /// Cost of roots that did resolve, kept for diagnostics and experiments.
        successful_cost: cost::CostVector,
        /// Non-empty root failure list.
        failures: ir::AtLeast<RootSelectionFailure, 1>,
    },
}

impl RootSelectionSummary {
    /// Return selected cost only when every root has a selected implementation.
    pub const fn complete_selected_cost(&self) -> Option<cost::CostVector> {
        match self {
            Self::Complete { selected_cost } => Some(*selected_cost),
            Self::Incomplete { .. } => None,
        }
    }

    /// Return root-selection failures when selection is incomplete.
    pub fn failures(&self) -> &[RootSelectionFailure] {
        match self {
            Self::Complete { .. } => &[],
            Self::Incomplete { failures, .. } => failures.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_summary_exposes_cost_without_failures() {
        let summary = RootSelectionSummary::Complete {
            selected_cost: cost::CostVector {
                latency: cost::LatencyEstimate::micros(7),
                ..cost::CostVector::ZERO
            },
        };

        assert_eq!(
            summary.complete_selected_cost(),
            Some(cost::CostVector {
                latency: cost::LatencyEstimate::micros(7),
                ..cost::CostVector::ZERO
            })
        );
        assert!(summary.failures().is_empty());
    }

    #[test]
    fn incomplete_summary_hides_complete_cost_and_exposes_failures() {
        let root = memo::MemoGroupId::first();
        let failure = RootSelectionFailure {
            root,
            error: SelectionError::NoPhysicalAlternatives { group: root },
        };
        let summary = RootSelectionSummary::Incomplete {
            successful_cost: cost::CostVector::ZERO,
            failures: ir::AtLeast::<_, 1>::from_one(failure.clone()),
        };

        assert_eq!(summary.complete_selected_cost(), None);
        assert_eq!(summary.failures(), &[failure]);
    }
}
