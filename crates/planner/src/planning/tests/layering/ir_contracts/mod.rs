//! Planner IR algebraic-data-type contract tests.
//!
//! Each child module covers one invariant family so invalid-state wrappers,
//! serde contracts, and proof ADTs can evolve without a monolithic test file.

mod access_batch_trace;
mod batch_stats_repeat;
mod bounds_limits;
mod catalog_index_metadata;
mod collections;
mod expressions_search;
mod index_literals_bounds;
mod index_range_proofs;
mod mutation_projection_predicate;
mod order_variable_ops;

use crate::planning::tests::support::*;
