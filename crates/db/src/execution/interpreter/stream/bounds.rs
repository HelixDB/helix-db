//! Runtime stream bound and window execution contracts.

mod dispatch;
mod eval;
mod rows;

#[cfg(test)]
mod tests;

use super::*;

#[cfg(test)]
pub(in crate::execution::interpreter::stream) use self::eval::eval_stream_bound;
#[cfg(test)]
pub(in crate::execution::interpreter::stream) use self::rows::{limit_rows, skip_rows, slice_rows};
