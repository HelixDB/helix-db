//! Native selected batch-boundary contract tests.
//!
//! Each child module owns one executable-IR contract at the AST batch boundary
//! so unsupported shapes and validation failures stay easy to locate.

mod followups;
mod foreach;
mod roots;
mod support;
mod validation;
