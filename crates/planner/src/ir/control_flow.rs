//! Control-flow executable-IR contract ADTs.
//!
//! Branch and repeat payloads are grouped here because they are semantic
//! barriers in the selected planner and need focused invariants around arity,
//! repeat bounds, and child payload ordering. Public names are re-exported
//! through [`crate::ir`].

use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use super::{AtLeast, PhysicalOp, PredicatePlan};

/// Branch plan.
///
/// The child payload defaults to [`PhysicalOp`] for executable physical plans,
/// but logical optimizer roots use the same arity-preserving shape with
/// logical children. This keeps branch invariants in one ADT while avoiding
/// compatibility physical subtrees in the memo.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchPlan<T = PhysicalOp> {
    /// Union.
    Union(AtLeast<T, 2>),
    /// Conditional branch without an else plan.
    Choose {
        /// Condition.
        condition: PredicatePlan,
        /// Then plan.
        then_plan: Box<T>,
    },
    /// Conditional branch with an else plan.
    ChooseElse {
        /// Condition.
        condition: PredicatePlan,
        /// Then plan.
        then_plan: Box<T>,
        /// Else plan.
        else_plan: Box<T>,
    },
    /// Coalesce.
    Coalesce(AtLeast<T, 1>),
    /// Optional.
    Optional(Box<T>),
}

/// Repeat plan.
///
/// Like [`BranchPlan`], the child payload defaults to [`PhysicalOp`] for the
/// executable IR and is specialized to logical children inside optimizer roots.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepeatPlan<T = PhysicalOp> {
    /// Body plan.
    pub body: Box<T>,
    /// Early stop condition; `max_depth` still applies as a hard guardrail.
    pub stop: RepeatStopPlan,
    /// Emit behavior.
    pub emit: RepeatEmitPlan,
    /// Positive maximum depth.
    pub max_depth: NonZeroUsize,
}

/// Repeat stop behavior before the hard maximum depth is reached.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepeatStopPlan {
    /// No early stop; only `RepeatPlan::max_depth` bounds traversal.
    MaxDepthOnly,
    /// Run a fixed number of iterations.
    Times {
        /// Positive iteration count.
        count: NonZeroUsize,
    },
    /// Stop when the predicate matches.
    Until {
        /// Stop predicate.
        predicate: PredicatePlan,
    },
    /// Stop after a fixed number of iterations or when the predicate matches.
    TimesOrUntil {
        /// Positive iteration count.
        count: NonZeroUsize,
        /// Stop predicate.
        predicate: PredicatePlan,
    },
}

/// Repeat emit behavior.
///
/// Predicate-guarded emission is a separate variant so consumers can match
/// exhaustively without inspecting an optional field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepeatEmitPlan {
    /// Do not emit intermediate results.
    None,
    /// Emit before each iteration.
    Before,
    /// Emit after each iteration.
    After,
    /// Emit matching after states.
    AfterIf {
        /// Predicate for emitted states.
        predicate: PredicatePlan,
    },
    /// Emit before and after each iteration.
    All,
}
