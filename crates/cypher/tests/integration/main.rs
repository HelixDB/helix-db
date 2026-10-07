//! Related integration contracts share one executable. Allocation observations
//! remain local to each test thread; domain modules retain their own fixtures.

#[path = "../../../planner/src/analysis/tests/allocations.rs"]
mod allocations;

mod contracts;
mod frontend;
mod graph_bindings;
mod grouping;
mod input_windows;
mod ordering;
mod ordering_aliases;
mod pattern_scopes;
mod punctuation;
mod schema_storage;
mod scopes;
mod simple_case;
mod wildcard_identity;
