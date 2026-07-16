//! Deterministic metric thresholds for scalability fixtures.

use crate::properties;

use super::super::metrics;
use super::shape::PlanningScalabilityShape;

pub(super) fn thresholds_for(
    shape: PlanningScalabilityShape,
    scale: properties::PositiveUsize,
) -> metrics::PlannerMetricThresholds {
    let scale = scale.get();
    match shape {
        PlanningScalabilityShape::WideBooleanPredicates => metrics::PlannerMetricThresholds::new(
            scale.saturating_mul(8).saturating_add(128),
            scale.saturating_mul(16).saturating_add(256),
            scale.saturating_mul(64).saturating_add(1_024),
            scale.saturating_mul(8).saturating_add(128),
        ),
        PlanningScalabilityShape::ManyAvailableIndexes => {
            metrics::PlannerMetricThresholds::new(256, 512, 2_048, 256)
        }
        PlanningScalabilityShape::BatchedRootReuse => {
            metrics::PlannerMetricThresholds::new(16, 32, 128, 16)
        }
        PlanningScalabilityShape::ForEachBodyRootReuse => {
            metrics::PlannerMetricThresholds::new(16, 32, 128, 16)
        }
        PlanningScalabilityShape::DeepTraversalChain => metrics::PlannerMetricThresholds::new(
            scale.saturating_mul(8).saturating_add(128),
            scale.saturating_mul(12).saturating_add(256),
            scale.saturating_mul(64).saturating_add(1_024),
            scale.saturating_mul(8).saturating_add(128),
        ),
        PlanningScalabilityShape::ManyMemoAlternatives => metrics::PlannerMetricThresholds::new(
            scale.saturating_mul(12).saturating_add(256),
            scale.saturating_mul(24).saturating_add(512),
            scale.saturating_mul(96).saturating_add(2_048),
            scale.saturating_mul(12).saturating_add(256),
        ),
        PlanningScalabilityShape::OverLimitIndexDisjunction => {
            metrics::PlannerMetricThresholds::new(
                128,
                256,
                scale.saturating_mul(16).saturating_add(1_024),
                128,
            )
        }
        PlanningScalabilityShape::BranchHeavyQueries => metrics::PlannerMetricThresholds::new(
            scale.saturating_mul(16).saturating_add(256),
            scale.saturating_mul(32).saturating_add(512),
            scale.saturating_mul(128).saturating_add(2_048),
            scale.saturating_mul(16).saturating_add(256),
        ),
        PlanningScalabilityShape::OrderedRangeWindowPushdown => {
            metrics::PlannerMetricThresholds::new(
                scale.saturating_mul(16).saturating_add(256),
                scale.saturating_mul(32).saturating_add(512),
                scale.saturating_mul(128).saturating_add(2_048),
                scale.saturating_mul(16).saturating_add(256),
            )
        }
        PlanningScalabilityShape::MutationHeavyBatches => metrics::PlannerMetricThresholds::new(
            scale.saturating_mul(64).saturating_add(512),
            scale.saturating_mul(128).saturating_add(1_024),
            scale.saturating_mul(512).saturating_add(4_096),
            scale.saturating_mul(64).saturating_add(512),
        ),
        PlanningScalabilityShape::SearchIndexDdlWorkloads => metrics::PlannerMetricThresholds::new(
            scale.saturating_mul(48).saturating_add(512),
            scale.saturating_mul(96).saturating_add(1_024),
            scale.saturating_mul(384).saturating_add(4_096),
            scale.saturating_mul(48).saturating_add(512),
        ),
        PlanningScalabilityShape::RuntimeDerivedMixedQueries => {
            metrics::PlannerMetricThresholds::new(
                scale.saturating_mul(96).saturating_add(768),
                scale.saturating_mul(192).saturating_add(1_536),
                scale.saturating_mul(768).saturating_add(6_144),
                scale.saturating_mul(96).saturating_add(768),
            )
        }
    }
    .expect("static threshold formulas are positive")
}
