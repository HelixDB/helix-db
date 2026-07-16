//! Planner test manifest.
//!
//! The filesystem is the coverage checklist:
//!
//! - `domains/` covers element families: nodes, edges, and search indexes.
//! - `optimizer/` covers semantic lowering helpers that still exist outside
//!   Cascades rules.
//! - `layering/` covers physical-plan shape, residual wrappers, branch context,
//!   reserved barriers, and planner errors.

mod support;

mod domains;
mod layering;
mod optimizer;
