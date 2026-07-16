//! Optimizer contract tests.
//!
//! The test suite mirrors optimizer module boundaries: driver behavior,
//! recursive memo child selection, result ordering/properties, guardrails and
//! configuration, and rule provenance each have an isolated contract module.

mod config;
mod driver;
mod guardrails;
mod memo_children;
mod provenance;
mod result;
mod support;
