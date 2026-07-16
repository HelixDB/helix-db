//! Logical exploration rules for core optimizer rewrites.

mod filter;
mod pipeline;
mod predicate;

pub use self::{
    filter::{FilterMergeRule, FilterPushdownRule},
    pipeline::PurePipelineSimplificationRule,
    predicate::StaticPredicateSimplificationRule,
};
