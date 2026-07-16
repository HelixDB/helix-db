//! Runtime stream set, merge, and variable-set contracts.

mod distinct;
mod merge;
mod variables;

#[cfg(test)]
mod tests;

use super::*;

#[cfg(test)]
pub(in crate::execution::interpreter::stream) use self::distinct::distinct_rows;
#[cfg(test)]
pub(in crate::execution::interpreter::stream) use self::merge::merge_streams;
#[cfg(test)]
pub(in crate::execution::interpreter::stream) use self::variables::{
    bind_rows, filter_within_rows, filter_without_rows,
};
