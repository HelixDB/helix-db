//! Index-DDL and mutation executable surface contracts.
//!
//! These tests keep DDL payload validation, mutation payload validation, and
//! mutation-as-root-stream behavior separate so planner changes fail at the
//! contract they actually touch.

mod index_ddl;
mod mutation_payloads;
mod mutation_streams;
