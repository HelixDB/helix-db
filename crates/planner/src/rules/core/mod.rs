//! Core optimizer rule contracts.
//!
//! Core rules are split by optimizer effect:
//!
//! - `exploration` owns logical rewrites that produce more logical
//!   alternatives.
//! - `implementation` owns physical seeding rules that attach delivered
//!   properties and tunable costs.
//!
//! The stable public rule types are re-exported from this facade.

mod exploration;
mod implementation;

pub use exploration::{
    FilterMergeRule, FilterPushdownRule, PurePipelineSimplificationRule,
    StaticPredicateSimplificationRule,
};
pub use implementation::{
    BarrierImplementationRule, FilterImplementationRule, OrderImplementationRule,
    PipelineImplementationRule, SimplifiedPredicateImplementationRule,
    SourceAccessImplementationRule, VariableSourceImplementationRule,
};
