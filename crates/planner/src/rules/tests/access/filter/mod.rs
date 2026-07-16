//! Access-filter rule contract tests.
//!
//! Child modules mirror the production `rules::access::filter` boundary so
//! simplification, physical residual-filter seeding, and catalog-backed index
//! rewriting stay independently covered.

mod implementation;
mod index;
mod simplification;

use super::*;
