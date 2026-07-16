//! Access-rooted stream-pipeline optimizer rule facade.

mod contracts;
mod filter;
mod implementation;
mod order;
mod simplification;
mod support;

pub use self::{
    filter::AccessPipelineFilterRule, implementation::AccessPipelineImplementationRule,
    order::AccessPipelineOrderRule, simplification::AccessPipelineSimplificationRule,
};
