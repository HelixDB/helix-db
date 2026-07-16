//! Mutation-time maintenance for physical search indexes.
//!
//! This module is the interpreter boundary between executable graph mutations
//! and physical search-index storage. Mutation execution supplies old/new
//! stored properties; child modules decide which configured indexes are
//! affected and how physical storage is updated.

mod properties;
mod text;
mod vector;

pub(super) use self::text::{TextIndexMaintenanceOutcome, TextPropertyUpdate};
pub(in crate::execution::interpreter) use self::vector::VectorPropertyUpdate;
