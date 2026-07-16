//! Validated expression and predicate IR contracts.

mod error;
mod plan;
mod predicate;
mod validation;

pub use self::error::{ExprPlanError, NameField, PredicateSetOp};
pub use self::plan::ExprPlan;
pub use self::predicate::PredicatePlan;
