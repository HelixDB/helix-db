//! Search-backed executable access contracts.
//!
//! This facade keeps search execution split from runtime definition lookup,
//! tenant/index-name resolution, query input evaluation, and storage calls.

mod definitions;
mod dispatch;
mod generation;
mod input;
mod limits;
mod storage;
mod tenant;

use super::super::*;

#[cfg(test)]
pub(super) use self::input::{db_value_to_query_vector, validate_query_vector};
#[cfg(test)]
pub(super) use self::limits::limited_search_k;
pub(in crate::execution::interpreter::access) use self::limits::SearchReadLimit;
#[cfg(test)]
pub(super) use self::tenant::validate_vector_search_tenant;
