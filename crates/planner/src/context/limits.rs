use std::num::NonZeroUsize;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::properties;

/// Planner guardrails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerLimits {
    /// Maximum Boolean index-union branches before a scan fallback.
    pub max_index_union_branches: IndexUnionBranchLimit,
}

impl Default for PlannerLimits {
    fn default() -> Self {
        Self {
            max_index_union_branches: IndexUnionBranchLimit::from_usize(64),
        }
    }
}

/// Cascades optimizer guardrails.
///
/// These limits keep planner work bounded and deterministic while allowing
/// experiments to tune the search envelope. Every limit counts planner work,
/// never elapsed time, so the same query and context always produce the same
/// plan regardless of host speed or load.
///
/// ```
/// use helix_planner::context::OptimizerLimits;
///
/// let limits = OptimizerLimits::default();
/// assert!(limits.memo_groups.get() > 0);
/// assert!(limits.rule_fires.get() > limits.memo_groups.get());
/// assert!(limits.exploration_rule_fires.get() < limits.rule_fires.get());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptimizerLimits {
    /// Maximum memo groups.
    pub memo_groups: properties::PositiveUsize,
    /// Maximum memo expressions.
    pub memo_expressions: properties::PositiveUsize,
    /// Maximum rule fires.
    pub rule_fires: properties::PositiveUsize,
    /// Maximum alternatives retained per group/property requirement.
    pub alternatives_per_group: properties::PositiveUsize,
    /// Rule fires after which optional exploration stops. Implementation
    /// rules and required rewrites keep running for every queued expression,
    /// so each memo group still reaches selection with a physical
    /// alternative. `rule_fires` remains the hard stop.
    pub exploration_rule_fires: properties::PositiveUsize,
}

impl Default for OptimizerLimits {
    fn default() -> Self {
        Self {
            memo_groups: properties::PositiveUsize::at_least_one(10_000),
            memo_expressions: properties::PositiveUsize::at_least_one(100_000),
            rule_fires: properties::PositiveUsize::at_least_one(250_000),
            alternatives_per_group: properties::PositiveUsize::at_least_one(32),
            exploration_rule_fires: properties::PositiveUsize::at_least_one(50_000),
        }
    }
}

/// Boolean index-union branch limit.
///
/// A zero raw value explicitly disables index-union planning; positive values
/// allow unions up to that branch count.
///
/// ```
/// use helix_planner::context::IndexUnionBranchLimit;
/// use std::num::NonZeroUsize;
///
/// assert_eq!(IndexUnionBranchLimit::from_usize(0), IndexUnionBranchLimit::Disabled);
/// assert_eq!(
///     IndexUnionBranchLimit::from_usize(2),
///     IndexUnionBranchLimit::Limited(NonZeroUsize::new(2).unwrap())
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexUnionBranchLimit {
    /// Never plan Boolean index unions.
    Disabled,
    /// Plan Boolean index unions up to this positive branch count.
    Limited(NonZeroUsize),
}

impl IndexUnionBranchLimit {
    /// Interpret a raw limit value.
    pub fn from_usize(value: usize) -> Self {
        match NonZeroUsize::new(value) {
            Some(value) => Self::Limited(value),
            None => Self::Disabled,
        }
    }

    /// Build a positive limited branch count.
    pub fn limited(value: usize) -> Option<Self> {
        NonZeroUsize::new(value).map(Self::Limited)
    }
}

impl Serialize for IndexUnionBranchLimit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(match self {
            Self::Disabled => 0,
            Self::Limited(value) => value.get() as u64,
        })
    }
}

impl<'de> Deserialize<'de> for IndexUnionBranchLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = usize::deserialize(deserializer)?;
        Ok(Self::from_usize(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_planner_limits_enable_bounded_index_unions() {
        assert_eq!(
            PlannerLimits::default().max_index_union_branches,
            IndexUnionBranchLimit::Limited(NonZeroUsize::new(64).unwrap())
        );
    }

    #[test]
    fn optimizer_limits_defaults_are_positive_and_ordered() {
        let limits = OptimizerLimits::default();

        assert!(limits.memo_groups.get() > 0);
        assert!(limits.memo_expressions.get() > limits.memo_groups.get());
        assert!(limits.rule_fires.get() > limits.memo_groups.get());
        assert!(limits.alternatives_per_group.get() > 0);
        assert!(limits.exploration_rule_fires.get() < limits.rule_fires.get());
    }

    #[test]
    fn index_union_branch_limit_serde_uses_raw_limit_values() {
        assert_eq!(
            serde_json::to_string(&IndexUnionBranchLimit::Disabled).unwrap(),
            "0"
        );
        assert_eq!(
            serde_json::to_string(&IndexUnionBranchLimit::limited(3).unwrap()).unwrap(),
            "3"
        );
        assert_eq!(
            serde_json::from_str::<IndexUnionBranchLimit>("0").unwrap(),
            IndexUnionBranchLimit::Disabled
        );
        assert_eq!(
            serde_json::from_str::<IndexUnionBranchLimit>("3").unwrap(),
            IndexUnionBranchLimit::limited(3).unwrap()
        );
        assert!(serde_json::from_str::<IndexUnionBranchLimit>(r#""3""#).is_err());
    }
}
