//! Index-DDL payload validation facade.
//!
//! Create-time attributes, drop identifiers, and shared raw-name validation
//! live in separate contract modules so DDL lowering can evolve without mixing
//! create-only and drop-only invariants.

mod create;
mod drop;
mod shared;

pub(in crate::planning) use create::index_ddl_create_spec;
pub(in crate::planning) use drop::index_ddl_drop_spec;
