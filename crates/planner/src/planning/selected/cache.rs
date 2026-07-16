//! Request-scoped selected-root cache contracts.
//!
//! Cache hits, pending-root batching, and optimized-root consumption live in
//! focused modules behind this facade. All lookups remain collision-safe:
//! digest buckets are accepted only after full logical-root equality.

mod optimized;
mod pending;
mod selected;

pub(super) use optimized::{OptimizedSelectedRunRoots, OptimizedSelectedRunRootsError};
pub(super) use pending::{PendingSelectedRunRoots, SelectedRunRootUse};
pub(super) use selected::{SelectedRunRoot, SelectedRunRootCache};

#[cfg(test)]
mod tests;
