use serde::{Deserialize, Serialize};

/// Selected access-method counts for one graph element family.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessStatistics {
    /// Full element scans.
    pub all_scans: usize,
    /// Label-scoped scans.
    pub label_scans: usize,
    /// Single or batched point-lookup operations.
    pub point_lookups: usize,
    /// Equality-index lookup operations.
    pub equality_index_lookups: usize,
    /// Range-index scan operations.
    pub range_index_scans: usize,
    /// Range scans traversing their existing physical lane backwards.
    pub reverse_range_index_scans: usize,
    /// Vector-search operations.
    pub vector_searches: usize,
    /// Text-search operations.
    pub text_searches: usize,
    /// Access operations with a planner-proven positive read bound.
    pub bounded_accesses: usize,
    /// Bounded accesses that evaluate at least one runtime expression.
    pub dynamic_bounded_accesses: usize,
}

/// Stable planner work and selected executable-plan shape statistics.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerStatistics {
    /// Memo groups explored.
    pub memo_groups: usize,
    /// Memo expressions explored.
    pub memo_expressions: usize,
    /// Optimizer rules fired.
    pub rules_fired: usize,
    /// Alternatives rejected during optimization.
    pub rejected_alternatives: usize,
    /// Physical alternatives considered.
    pub alternatives_considered: usize,
    /// Optimization duration in microseconds.
    pub optimization_micros: u64,
    /// Whether an optimizer guardrail stopped exploration.
    pub guardrail_hit: bool,
    /// Executable operators, including operators in nested subplans.
    pub total_operators: usize,
    /// Longest selected executable operator path, including nested subplans.
    pub maximum_operator_depth: usize,
    /// Selected node access methods.
    pub node_accesses: AccessStatistics,
    /// Selected edge access methods.
    pub edge_accesses: AccessStatistics,
    /// Set-union merge operators.
    pub unions: usize,
    /// Set-intersection merge operators.
    pub intersections: usize,
    /// Residual predicate filters.
    pub residual_filters: usize,
    /// Explicit sort operators.
    pub explicit_sorts: usize,
    /// Limit operators not fully pushed into access.
    pub limits: usize,
    /// Skip operators.
    pub skips: usize,
    /// Range/slice operators.
    pub ranges: usize,
    /// Graph expansion operators.
    pub expansions: usize,
    /// Branch control-flow operators.
    pub branches: usize,
    /// Repeat control-flow operators.
    pub repeats: usize,
    /// Parameter `ForEach` control-flow operators.
    pub for_each: usize,
}
