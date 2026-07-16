//! Reusable IR invariant wrappers.
//!
//! The public `ir::*` facade re-exports these contracts, while the
//! implementation is split by invariant family so each wrapper can evolve with
//! focused tests and doctests.

mod at_least;
mod element_ids;
mod non_empty_string;

pub use self::{
    at_least::AtLeast,
    element_ids::{ElementIds, ElementIdsError},
    non_empty_string::NonEmptyString,
};

#[cfg(test)]
mod tests;
