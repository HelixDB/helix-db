//! Element-typed search access builder facade.
//!
//! The facade keeps native selected lowering stable while contract modules own
//! the independently testable parts: wrapper payloads, node builders, edge
//! builders, and tenant-aware index metadata construction.

mod contracts;
mod edge;
mod metadata;
mod node;

pub use contracts::SearchAccessPlan;
pub use edge::{edge_text_search, edge_vector_search};
pub use node::{node_text_search, node_vector_search};

#[cfg(test)]
mod tests;
