use serde::{Deserialize, Serialize};

use crate::cost;

/// Planner performance metrics carried with executable plans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerMetrics {
    /// Memo groups explored.
    pub memo_groups: usize,
    /// Memo expressions explored.
    pub memo_exprs: usize,
    /// Rules fired.
    pub rule_fires: usize,
    /// Rule or alternative candidates rejected by guards, dominance, or rule
    /// precondition failures that need traceable reasons.
    pub rejected_alternatives: usize,
    /// Physical alternatives considered.
    pub alternatives_considered: usize,
    /// Cost of the selected root alternative.
    pub selected_cost: cost::CostVector,
    /// Optimization duration in microseconds.
    pub optimization_micros: u64,
    /// Whether any guardrail stopped exploration.
    pub guardrail_hit: bool,
}

impl PlannerMetrics {
    /// Conservative empty metrics.
    pub const fn empty() -> Self {
        Self {
            memo_groups: 0,
            memo_exprs: 0,
            rule_fires: 0,
            rejected_alternatives: 0,
            alternatives_considered: 0,
            selected_cost: cost::CostVector::ZERO,
            optimization_micros: 0,
            guardrail_hit: false,
        }
    }
}

impl Default for PlannerMetrics {
    fn default() -> Self {
        Self::empty()
    }
}
