//! Root-stream rule contract tests.
//!
//! These modules keep root pipeline, terminal projection, aggregation,
//! variable-write, and reserved-operation rule behavior independently
//! testable while sharing the parent rule-test fixtures.

mod access_rewrite;
mod aggregate;
mod project;
mod reserved;
mod root_pipeline;
mod variable_write;

use super::*;
