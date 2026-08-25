//! Executable access interpreter integration contracts.
//!
//! Shared executable-plan builders live in `support`; sibling modules own KV,
//! search, expansion, and secondary-index behavior families.

#[cfg(test)]
mod expand;
#[cfg(test)]
mod kv;
#[cfg(test)]
mod search_access;
#[cfg(test)]
mod secondary_indexes;
pub(in crate::execution::interpreter) mod support;
