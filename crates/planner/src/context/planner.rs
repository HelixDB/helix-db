use serde::{Deserialize, Serialize};

use crate::{catalog, cost, feedback};

use super::{OptimizerLimits, ParamBindings, PlannerLimits, StatsSnapshot};

/// Planner input snapshot.
///
/// Build this once per request from database metadata and request parameters.
/// Keep it immutable: optimization passes may build local hash worksets, but
/// the context is the reproducible planning input.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PlannerContext {
    /// Runtime parameters available during planning.
    pub params: ParamBindings,
    /// Available indexes, pre-keyed for O(1) planner lookups.
    pub indexes: catalog::IndexCatalogSnapshot,
    /// Optional statistics used by deterministic heuristics.
    pub stats: StatsSnapshot,
    /// Optional immutable runtime feedback applied to stats at optimization
    /// configuration time.
    #[serde(default)]
    pub runtime_feedback: feedback::RuntimeFeedbackSnapshot,
    /// Tunable storage cost profile used by the modular cost model.
    #[serde(default)]
    pub storage: cost::StorageCostProfile,
    /// Guardrails for planning and execution shape.
    pub limits: PlannerLimits,
    /// Guardrails for Cascades exploration.
    #[serde(default)]
    pub optimizer_limits: OptimizerLimits,
}

impl PlannerContext {
    /// Replace the storage cost profile for this planning request.
    ///
    /// ```
    /// use helix_planner::context::PlannerContext;
    /// use helix_planner::cost::{LatencyEstimate, StorageCostProfile};
    ///
    /// let profile = StorageCostProfile {
    ///     range_next: LatencyEstimate::micros(3),
    ///     ..StorageCostProfile::default()
    /// };
    /// let ctx = PlannerContext::default().with_storage_cost_profile(profile);
    ///
    /// assert_eq!(ctx.storage.range_next.as_micros(), 3);
    /// ```
    pub fn with_storage_cost_profile(mut self, storage: cost::StorageCostProfile) -> Self {
        self.storage = storage;
        self
    }

    /// Apply typed partial storage cost overrides for this planning request.
    ///
    /// ```
    /// use helix_planner::context::PlannerContext;
    /// use helix_planner::cost::{EstimatedRows, StorageCostProfileOverrides};
    ///
    /// let overrides = StorageCostProfileOverrides::from_json_str(
    ///     r#"{"default_equality_index_rows": 7}"#,
    /// )
    /// .unwrap();
    /// let ctx = PlannerContext::default().with_storage_cost_overrides(overrides);
    ///
    /// assert_eq!(ctx.storage.default_equality_index_rows, EstimatedRows::rows(7));
    /// ```
    pub fn with_storage_cost_overrides(
        mut self,
        overrides: cost::StorageCostProfileOverrides,
    ) -> Self {
        self.storage = self.storage.with_overrides(overrides);
        self
    }

    /// Apply JSON storage cost overrides for this planning request.
    ///
    /// ```
    /// use helix_planner::context::PlannerContext;
    ///
    /// let ctx = PlannerContext::default()
    ///     .with_json_storage_cost_overrides(r#"{"range_next": 5}"#)
    ///     .unwrap();
    ///
    /// assert_eq!(ctx.storage.range_next.as_micros(), 5);
    /// ```
    pub fn with_json_storage_cost_overrides(self, input: &str) -> Result<Self, serde_json::Error> {
        Ok(self
            .with_storage_cost_overrides(cost::StorageCostProfileOverrides::from_json_str(input)?))
    }

    /// Attach immutable runtime feedback supplied by the caller.
    ///
    /// ```
    /// use helix_planner::context::PlannerContext;
    /// use helix_planner::feedback::{ObservedRows, RuntimeFeedbackSnapshot};
    /// use helix_planner::ir::NonEmptyString;
    ///
    /// let label = NonEmptyString::new("User").unwrap();
    /// let ctx = PlannerContext::default().with_runtime_feedback(
    ///     RuntimeFeedbackSnapshot::default()
    ///         .with_node_label_cardinality(label.clone(), ObservedRows::rows(3)),
    /// );
    ///
    /// assert_eq!(ctx.effective_stats().node_label_cardinality[&label], 3);
    /// ```
    pub fn with_runtime_feedback(
        mut self,
        runtime_feedback: feedback::RuntimeFeedbackSnapshot,
    ) -> Self {
        self.runtime_feedback = runtime_feedback;
        self
    }

    /// Stats visible to optimizer rules after applying runtime feedback.
    pub fn effective_stats(&self) -> StatsSnapshot {
        self.runtime_feedback.apply_to(self.stats.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_cost_helpers_replace_and_override_request_profile() {
        let profile = cost::StorageCostProfile {
            range_next: cost::LatencyEstimate::micros(3),
            default_equality_index_rows: cost::EstimatedRows::rows(9),
            ..cost::StorageCostProfile::default()
        };
        let overrides = cost::StorageCostProfileOverrides::from_json_str(
            r#"{"range_next": 5, "default_equality_index_rows": 11}"#,
        )
        .unwrap();

        let ctx = PlannerContext::default()
            .with_storage_cost_profile(profile)
            .with_storage_cost_overrides(overrides);

        assert_eq!(ctx.storage.range_next, cost::LatencyEstimate::micros(5));
        assert_eq!(
            ctx.storage.default_equality_index_rows,
            cost::EstimatedRows::rows(11)
        );
    }

    #[test]
    fn json_storage_cost_overrides_reject_invalid_profiles() {
        let ctx = PlannerContext::default()
            .with_json_storage_cost_overrides(r#"{"max_parallel_kv_reads": 2}"#)
            .unwrap();

        assert_eq!(
            ctx.storage.max_parallel_kv_reads,
            crate::properties::PositiveUsize::new(2).unwrap()
        );
        assert!(PlannerContext::default()
            .with_json_storage_cost_overrides(r#"{"max_parallel_kv_reads": 0}"#)
            .is_err());
        assert!(PlannerContext::default()
            .with_json_storage_cost_overrides(r#"{"unknown_cost": 1}"#)
            .is_err());
    }

    #[test]
    fn effective_stats_overlay_runtime_feedback_without_mutating_base_stats() {
        let label = crate::ir::NonEmptyString::new("User").unwrap();
        let base_stats = StatsSnapshot::default().with_node_label_cardinality(label.clone(), 100);
        let ctx = PlannerContext {
            stats: base_stats.clone(),
            ..PlannerContext::default()
        }
        .with_runtime_feedback(
            feedback::RuntimeFeedbackSnapshot::default()
                .with_node_label_cardinality(label.clone(), feedback::ObservedRows::rows(3)),
        );

        let effective = ctx.effective_stats();

        assert_eq!(effective.node_label_cardinality[&label], 3);
        assert_eq!(ctx.stats, base_stats);
    }
}
