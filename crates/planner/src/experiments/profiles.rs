//! Deterministic cost-profile comparison fixtures.
//!
//! These fixtures compare planner output under two explicit storage-cost
//! profiles without depending on wall-clock benchmark timing. They are intended
//! for CI-safe experiment coverage: both plans must stay within the same
//! deterministic planner metric thresholds, and callers can inspect the selected
//! costs to decide whether a profile changed planning behavior.

use serde::{Deserialize, Serialize};

use crate::{cost, exec};

use super::{fixtures, metrics};

/// Named storage-cost profile variant used by planner experiments.
///
/// Variants are closed and serde-tagged so experiment fixtures can be committed
/// as stable data while each variant still maps to a fully typed
/// `StorageCostProfile`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CostProfileVariant {
    /// Production default cost profile.
    Default,
    /// Range/equality index scans are relatively expensive.
    ExpensiveRangeScans,
    /// Non-unique equality-index fallbacks are broad when stats are missing.
    BroadEqualityFallback,
}

impl CostProfileVariant {
    /// Materialize this variant as a complete storage-cost profile.
    pub fn storage_profile(self) -> cost::StorageCostProfile {
        match self {
            Self::Default => cost::StorageCostProfile::default(),
            Self::ExpensiveRangeScans => cost::StorageCostProfile {
                range_seek: cost::LatencyEstimate::micros(20_000),
                range_next: cost::LatencyEstimate::micros(250),
                ..cost::StorageCostProfile::default()
            },
            Self::BroadEqualityFallback => cost::StorageCostProfile {
                default_equality_index_rows: cost::EstimatedRows::rows(500),
                ..cost::StorageCostProfile::default()
            },
        }
    }
}

/// Cost-profile comparison over one scalability fixture.
///
/// ```
/// use helix_planner::experiments::{
///     CostProfileComparisonFixture, CostProfileVariant, PlanScalabilityFixture,
///     PlanningScalabilityShape,
/// };
///
/// let fixture = PlanScalabilityFixture::new(
///     PlanningScalabilityShape::ManyAvailableIndexes,
///     64,
/// )
/// .unwrap();
///
/// assert!(CostProfileComparisonFixture::new(
///     fixture,
///     CostProfileVariant::Default,
///     CostProfileVariant::ExpensiveRangeScans,
/// )
/// .is_some());
/// assert!(CostProfileComparisonFixture::new(
///     fixture,
///     CostProfileVariant::Default,
///     CostProfileVariant::Default,
/// )
/// .is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CostProfileComparisonFixture {
    fixture: fixtures::PlanScalabilityFixture,
    baseline: CostProfileVariant,
    candidate: CostProfileVariant,
}

impl CostProfileComparisonFixture {
    /// Build a comparison fixture, rejecting identical profile variants.
    pub fn new(
        fixture: fixtures::PlanScalabilityFixture,
        baseline: CostProfileVariant,
        candidate: CostProfileVariant,
    ) -> Option<Self> {
        if baseline == candidate {
            None
        } else {
            Some(Self {
                fixture,
                baseline,
                candidate,
            })
        }
    }

    /// Scalability fixture planned under both cost profiles.
    pub const fn fixture(self) -> fixtures::PlanScalabilityFixture {
        self.fixture
    }

    /// Baseline profile variant.
    pub const fn baseline(self) -> CostProfileVariant {
        self.baseline
    }

    /// Candidate profile variant.
    pub const fn candidate(self) -> CostProfileVariant {
        self.candidate
    }

    /// Plan both sides and return deterministic comparison metrics.
    pub fn compare(self) -> Result<CostProfileComparison, metrics::PlanningRegressionError> {
        let case = self.fixture.case();
        let baseline_plan = plan_with_profile(&case, self.baseline)?;
        let candidate_plan = plan_with_profile(&case, self.candidate)?;

        Ok(CostProfileComparison {
            fixture: self.fixture,
            baseline: self.baseline,
            candidate: self.candidate,
            baseline_metrics: baseline_plan.metrics().clone(),
            candidate_metrics: candidate_plan.metrics().clone(),
        })
    }
}

fn plan_with_profile(
    case: &fixtures::PlanningScalabilityCase,
    variant: CostProfileVariant,
) -> Result<exec::ExecutablePlan, metrics::PlanningRegressionError> {
    let ctx = case
        .context()
        .clone()
        .with_storage_cost_profile(variant.storage_profile());
    let plan = case.plan_with_context(&ctx)?;
    case.thresholds().check(plan.metrics())?;
    Ok(plan)
}

/// Completed comparison for one fixture and two cost profiles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostProfileComparison {
    fixture: fixtures::PlanScalabilityFixture,
    baseline: CostProfileVariant,
    candidate: CostProfileVariant,
    baseline_metrics: exec::PlannerMetrics,
    candidate_metrics: exec::PlannerMetrics,
}

impl CostProfileComparison {
    /// Scalability fixture planned under both cost profiles.
    pub const fn fixture(&self) -> fixtures::PlanScalabilityFixture {
        self.fixture
    }

    /// Baseline profile variant.
    pub const fn baseline(&self) -> CostProfileVariant {
        self.baseline
    }

    /// Candidate profile variant.
    pub const fn candidate(&self) -> CostProfileVariant {
        self.candidate
    }

    /// Planner metrics for the baseline profile.
    pub const fn baseline_metrics(&self) -> &exec::PlannerMetrics {
        &self.baseline_metrics
    }

    /// Planner metrics for the candidate profile.
    pub const fn candidate_metrics(&self) -> &exec::PlannerMetrics {
        &self.candidate_metrics
    }

    /// Selected root cost for the baseline profile.
    pub const fn baseline_cost(&self) -> cost::CostVector {
        self.baseline_metrics.selected_cost
    }

    /// Selected root cost for the candidate profile.
    pub const fn candidate_cost(&self) -> cost::CostVector {
        self.candidate_metrics.selected_cost
    }

    /// Whether the selected cost changed between the two profiles.
    pub fn selected_cost_changed(&self) -> bool {
        self.baseline_cost() != self.candidate_cost()
    }
}

/// Default cost-profile comparison fixtures shared by tests and CI.
pub fn default_cost_profile_comparison_fixtures() -> Vec<CostProfileComparisonFixture> {
    [
        (
            fixtures::PlanScalabilityFixture::new(
                fixtures::PlanningScalabilityShape::ManyAvailableIndexes,
                64,
            )
            .expect("default comparison fixture scale is positive"),
            CostProfileVariant::Default,
            CostProfileVariant::ExpensiveRangeScans,
        ),
        (
            fixtures::PlanScalabilityFixture::new(
                fixtures::PlanningScalabilityShape::WideBooleanPredicates,
                8,
            )
            .expect("default comparison fixture scale is positive"),
            CostProfileVariant::Default,
            CostProfileVariant::BroadEqualityFallback,
        ),
    ]
    .into_iter()
    .map(|(fixture, baseline, candidate)| {
        CostProfileComparisonFixture::new(fixture, baseline, candidate)
            .expect("default comparison profiles are distinct")
    })
    .collect()
}

/// Plan every default cost-profile comparison fixture.
pub fn compare_default_cost_profiles(
) -> Result<Vec<CostProfileComparison>, metrics::PlanningRegressionError> {
    default_cost_profile_comparison_fixtures()
        .into_iter()
        .map(CostProfileComparisonFixture::compare)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparison_fixture_rejects_identical_profiles() {
        let fixture = fixtures::PlanScalabilityFixture::new(
            fixtures::PlanningScalabilityShape::ManyAvailableIndexes,
            64,
        )
        .unwrap();

        assert!(CostProfileComparisonFixture::new(
            fixture,
            CostProfileVariant::Default,
            CostProfileVariant::Default,
        )
        .is_none());
        assert!(CostProfileComparisonFixture::new(
            fixture,
            CostProfileVariant::Default,
            CostProfileVariant::ExpensiveRangeScans,
        )
        .is_some());
    }

    #[test]
    fn profile_variants_materialize_distinct_knobs() {
        let default = CostProfileVariant::Default.storage_profile();
        let expensive = CostProfileVariant::ExpensiveRangeScans.storage_profile();
        let broad = CostProfileVariant::BroadEqualityFallback.storage_profile();

        assert!(expensive.range_seek > default.range_seek);
        assert!(expensive.range_next > default.range_next);
        assert!(broad.default_equality_index_rows > default.default_equality_index_rows);
    }

    #[test]
    fn default_comparison_fixtures_have_distinct_profiles() {
        let fixtures = default_cost_profile_comparison_fixtures();

        assert_eq!(fixtures.len(), 2);
        assert!(fixtures
            .iter()
            .all(|fixture| fixture.baseline() != fixture.candidate()));
    }

    #[test]
    fn default_cost_profile_comparisons_stay_within_metric_thresholds() {
        let fixtures = default_cost_profile_comparison_fixtures();
        let comparisons = compare_default_cost_profiles().unwrap();

        assert_eq!(comparisons.len(), fixtures.len());
        assert!(comparisons
            .iter()
            .zip(fixtures)
            .all(|(comparison, fixture)| {
                assert_eq!(comparison.fixture(), fixture.fixture());
                assert_eq!(comparison.baseline(), fixture.baseline());
                assert_eq!(comparison.candidate(), fixture.candidate());
                assert!(comparison.selected_cost_changed());
                assert!(comparison.candidate_cost().latency >= comparison.baseline_cost().latency);
                !comparison.baseline_metrics().guardrail_hit
                    && !comparison.candidate_metrics().guardrail_hit
            }));
    }
}
