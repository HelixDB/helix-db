//! Selected access-prefix pipeline matching contracts.
//!
//! Shared ADTs, generic access-prefix matching, and filter-specific suffix
//! matching live in separate modules behind this facade.

mod contracts;
mod filter;
mod prefix;

#[cfg(test)]
mod tests;

pub(in crate::exec::selected::lowering) use contracts::{
    SelectedAccessFilterPipelineMatch, SelectedAccessPipelineMatch,
};
pub(in crate::exec::selected::lowering) use filter::selected_access_filter_pipeline_access;
pub(in crate::exec::selected::lowering) use prefix::selected_access_pipeline_parts;
