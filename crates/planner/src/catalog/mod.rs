//! Planner catalog contracts.
//!
//! Catalog inputs are immutable snapshots consumed by planning rules. The
//! modules are layered from primitive typed keys through metadata and finally
//! the snapshot builder API:
//!
//! - `element`: node/edge and search-kind enums.
//! - `property`: typed `(label, property)` keys for equality/range indexes.
//! - `search`: typed search-index keys and tenant scoping.
//! - `metadata`: physical index metadata stored in the snapshot.
//! - `snapshot`: the immutable index catalog seen by the optimizer.
//!
//! The public surface is re-exported from this module so callers can continue
//! to depend on `helix_planner::catalog`.

mod element;
mod metadata;
mod property;
mod search;
mod snapshot;

pub use element::{ElementKind, SearchIndexKind};
pub use metadata::{
    EdgeEqualityIndexMeta, EdgeRangeIndexMeta, IndexUniqueness, NodeEqualityIndexMeta,
    NodeRangeIndexMeta, TextIndexMeta, VectorIndexMeta,
};
pub use property::{ScopedPropertyDirectionKey, ScopedPropertyKey};
pub use search::{EdgeSearchIndexKey, NodeSearchIndexKey, SearchIndexKey, SearchIndexScope};
pub use snapshot::IndexCatalogSnapshot;

#[cfg(test)]
mod tests;
