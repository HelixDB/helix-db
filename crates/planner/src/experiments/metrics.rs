//! Deterministic planner metric regression contracts.

use serde::{Deserialize, Serialize};

use crate::exec::PlannerMetrics;
use crate::{error, properties};

/// Deterministic planner metric threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerMetricThresholds {
    max_memo_groups: properties::PositiveUsize,
    max_memo_exprs: properties::PositiveUsize,
    max_rule_fires: properties::PositiveUsize,
    max_alternatives_considered: properties::PositiveUsize,
}

impl PlannerMetricThresholds {
    /// Build metric thresholds, rejecting any zero limit.
    pub fn new(
        max_memo_groups: usize,
        max_memo_exprs: usize,
        max_rule_fires: usize,
        max_alternatives_considered: usize,
    ) -> Option<Self> {
        Some(Self {
            max_memo_groups: properties::PositiveUsize::new(max_memo_groups)?,
            max_memo_exprs: properties::PositiveUsize::new(max_memo_exprs)?,
            max_rule_fires: properties::PositiveUsize::new(max_rule_fires)?,
            max_alternatives_considered: properties::PositiveUsize::new(
                max_alternatives_considered,
            )?,
        })
    }

    /// Maximum memo groups.
    pub const fn max_memo_groups(self) -> properties::PositiveUsize {
        self.max_memo_groups
    }

    /// Maximum memo expressions.
    pub const fn max_memo_exprs(self) -> properties::PositiveUsize {
        self.max_memo_exprs
    }

    /// Maximum rule fires.
    pub const fn max_rule_fires(self) -> properties::PositiveUsize {
        self.max_rule_fires
    }

    /// Maximum physical alternatives considered.
    pub const fn max_alternatives_considered(self) -> properties::PositiveUsize {
        self.max_alternatives_considered
    }

    /// Check metrics against this threshold set.
    pub fn check(self, metrics: &PlannerMetrics) -> Result<(), PlannerMetricRegression> {
        [
            (
                PlannerMetric::MemoGroups,
                metrics.memo_groups,
                self.max_memo_groups.get(),
            ),
            (
                PlannerMetric::MemoExpressions,
                metrics.memo_exprs,
                self.max_memo_exprs.get(),
            ),
            (
                PlannerMetric::RuleFires,
                metrics.rule_fires,
                self.max_rule_fires.get(),
            ),
            (
                PlannerMetric::AlternativesConsidered,
                metrics.alternatives_considered,
                self.max_alternatives_considered.get(),
            ),
        ]
        .into_iter()
        .find(|(_, actual, limit)| actual > limit)
        .map_or(Ok(()), |(metric, actual, limit)| {
            Err(PlannerMetricRegression {
                metric,
                actual,
                limit,
            })
        })
    }
}

/// Planner metric guarded by deterministic experiment thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannerMetric {
    /// Memo groups explored.
    MemoGroups,
    /// Memo expressions explored.
    MemoExpressions,
    /// Rules fired.
    RuleFires,
    /// Physical alternatives considered.
    AlternativesConsidered,
}

/// A deterministic planner-metric regression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerMetricRegression {
    /// Metric that exceeded its threshold.
    pub metric: PlannerMetric,
    /// Actual metric value.
    pub actual: usize,
    /// Configured threshold.
    pub limit: usize,
}

impl std::fmt::Display for PlannerMetricRegression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} metric regression: actual {} exceeded threshold {}",
            self.metric, self.actual, self.limit
        )
    }
}

impl std::error::Error for PlannerMetricRegression {}

/// Error returned while planning and checking an experiment fixture.
#[derive(Debug)]
pub enum PlanningRegressionError {
    /// Planning failed.
    Planner(error::PlannerError),
    /// Planning succeeded but deterministic metrics exceeded the threshold.
    Metrics(PlannerMetricRegression),
}

impl From<error::PlannerError> for PlanningRegressionError {
    fn from(error: error::PlannerError) -> Self {
        Self::Planner(error)
    }
}

impl From<PlannerMetricRegression> for PlanningRegressionError {
    fn from(error: PlannerMetricRegression) -> Self {
        Self::Metrics(error)
    }
}

impl std::fmt::Display for PlanningRegressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Planner(error) => write!(f, "{error}"),
            Self::Metrics(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for PlanningRegressionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_thresholds_reject_zero_limits() {
        assert!(PlannerMetricThresholds::new(0, 1, 1, 1).is_none());
        assert!(PlannerMetricThresholds::new(1, 0, 1, 1).is_none());
        assert!(PlannerMetricThresholds::new(1, 1, 0, 1).is_none());
        assert!(PlannerMetricThresholds::new(1, 1, 1, 0).is_none());
    }

    #[test]
    fn metric_thresholds_report_first_regression() {
        let thresholds = PlannerMetricThresholds::new(1, 1, 1, 1).unwrap();
        let metrics = PlannerMetrics {
            memo_groups: 2,
            memo_exprs: 3,
            ..PlannerMetrics::default()
        };

        assert_eq!(
            thresholds.check(&metrics),
            Err(PlannerMetricRegression {
                metric: PlannerMetric::MemoGroups,
                actual: 2,
                limit: 1,
            })
        );
    }

    #[test]
    fn planning_regression_error_displays_inner_boundary() {
        let error = PlanningRegressionError::from(PlannerMetricRegression {
            metric: PlannerMetric::RuleFires,
            actual: 5,
            limit: 4,
        });

        assert_eq!(
            error.to_string(),
            "RuleFires metric regression: actual 5 exceeded threshold 4"
        );
    }
}
