//! Related integration contracts share one executable. Allocation observations
//! remain local to each test thread; domain modules retain their own fixtures.
//! The distinct live-allocation observer stays in the allocation_bounds target.

#[path = "../../src/analysis/tests/allocations.rs"]
mod allocations;

mod computed_values;
mod evaluation;
mod graph_patterns;
mod graph_requirements;
mod native_scalars;
mod owned_rewrite;
mod pruned_traversal;
mod row_layout;
mod row_pipelines;
mod row_projections;
mod row_windows;
mod schema_snapshots;
mod simple_case;
