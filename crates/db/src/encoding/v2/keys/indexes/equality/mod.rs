//! Equality-index key codecs.

pub(crate) mod edge;
pub(crate) mod node;
pub(crate) mod scans;

pub(crate) use edge::{EdgeDirection, EdgeEqualityIndexKey, GlobalEdgeEqualityIndexKey};
pub(crate) use node::EqualityIndexKey;
