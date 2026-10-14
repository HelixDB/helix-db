//! Optimizer exploration guardrail contracts.

use serde::{Deserialize, Serialize};

/// Guardrail that stopped exploration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizerGuardrail {
    /// Memo group limit reached.
    MemoGroups,
    /// Memo expression limit reached.
    MemoExpressions,
    /// Rule-fire limit reached.
    RuleFires,
    /// Physical alternatives-per-group limit reached.
    AlternativesPerGroup,
    /// Exploration rule-fire budget reached; optional exploration stopped
    /// while implementation and required rewrites continued.
    ExplorationBudget,
    /// Memo ownership or identity invariant failed during exploration.
    MemoIntegrity,
}
