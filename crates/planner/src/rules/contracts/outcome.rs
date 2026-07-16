use serde::{Deserialize, Serialize};

use crate::ir;

/// Rule phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleKind {
    /// Logical equivalence exploration.
    Exploration,
    /// Logical-to-physical implementation.
    Implementation,
}

/// Result of trying a rule against an expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleOutcome {
    /// Rule preconditions did not match.
    NotApplicable,
    /// Rule matched but rejected the expression or alternative with a reason.
    Rejected,
    /// Rule produced one or more alternatives.
    Applied,
}

/// Reason a rule matched but intentionally rejected a rewrite or alternative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleRejection {
    /// Human-readable stable reason for traces and tests.
    pub reason: ir::NonEmptyString,
}

impl RuleRejection {
    /// Build a non-empty rejection reason.
    pub fn new(reason: impl Into<String>) -> Option<Self> {
        ir::NonEmptyString::new(reason).map(|reason| Self { reason })
    }
}
