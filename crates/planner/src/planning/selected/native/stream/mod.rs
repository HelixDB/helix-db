//! Native access-stream lowering facade.
//!
//! Child modules keep the source-rooted stream accumulator separate from AST
//! bound/range validation so stream composition and payload validation remain
//! independently testable.

mod accumulator;
mod bounds;

pub(super) use self::{
    accumulator::NativeAccessStream,
    bounds::{stream_bound_plan, stream_range_plan},
};

#[cfg(test)]
mod tests;
