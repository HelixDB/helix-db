//! Executable access interpreter integration contracts.
//!
//! Shared executable-plan builders live in `support`; sibling modules own KV,
//! search, expansion, and secondary-index behavior families.

mod expand;
mod kv;
mod search_access;
mod secondary_indexes;
mod support;
