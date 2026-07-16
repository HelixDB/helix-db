//! Planner experiment fixtures and deterministic regression thresholds.
//!
//! Criterion benchmarks measure wall-clock time, but CI should not depend on
//! unstable timing. This facade exposes the shared scalability fixture matrix,
//! deterministic metric thresholds, and small benchmark helpers while keeping
//! each experiment contract independently testable.

mod coalescing;
mod defaults;
mod fixtures;
mod metrics;
mod profiles;

pub use self::coalescing::coalescing_keys;
pub use self::defaults::{
    check_default_planning_scalability_fixtures, default_planning_scalability_fixtures,
};
pub use self::fixtures::{
    PlanScalabilityFixture, PlanningScalabilityCase, PlanningScalabilityShape,
    PlanningScalabilityWorkload,
};
pub use self::metrics::{
    PlannerMetric, PlannerMetricRegression, PlannerMetricThresholds, PlanningRegressionError,
};
pub use self::profiles::{
    compare_default_cost_profiles, default_cost_profile_comparison_fixtures, CostProfileComparison,
    CostProfileComparisonFixture, CostProfileVariant,
};
