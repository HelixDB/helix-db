//! Access-rooted stream-pipeline optimizer rule facade.

mod contracts;
mod filter;
mod implementation;
mod membership;
mod order;
mod simplification;
mod support;

pub(in crate::rules) use self::membership::membership_rewrite;

pub use self::{
    filter::AccessPipelineFilterRule, implementation::AccessPipelineImplementationRule,
    membership::AccessPipelineMembershipFilterRule, order::AccessPipelineOrderRule,
    simplification::AccessPipelineSimplificationRule,
};
