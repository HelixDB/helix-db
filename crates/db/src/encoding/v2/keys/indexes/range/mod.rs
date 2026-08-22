//! Ordered range-index key codecs.

pub(crate) mod edge;
pub(crate) mod node;
pub(crate) mod scans;

pub use edge::EdgeRangeIndexDirection;
pub(crate) use edge::{EdgeRangeIndexKey, GlobalEdgeRangeIndexKey};
pub use node::RangeIndexDirection;
pub(crate) use node::RangeIndexKey;
