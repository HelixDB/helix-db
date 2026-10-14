//! Resident vector-cache ownership, hydration, and commit effects.

#[cfg(feature = "production-coverage")]
#[path = "../../../../tests/production_support/vector/memory_benchmark.rs"]
pub(super) mod benchmark;
pub(super) mod commit;
pub(super) mod hydration;
pub(super) mod part_warm;
pub(super) mod reader_refresh;
pub(super) mod registry;
pub(super) mod store;

#[cfg(test)]
mod node_tests;
