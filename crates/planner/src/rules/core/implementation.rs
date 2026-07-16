//! Physical implementation rules for core logical families.

mod barrier;
mod filter;
mod order;
mod pipeline;
mod predicate;
mod source;
mod variable;

pub use self::{
    barrier::BarrierImplementationRule, filter::FilterImplementationRule,
    order::OrderImplementationRule, pipeline::PipelineImplementationRule,
    predicate::SimplifiedPredicateImplementationRule, source::SourceAccessImplementationRule,
    variable::VariableSourceImplementationRule,
};
