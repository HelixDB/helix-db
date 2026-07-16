//! Graph data types and encoding

pub mod error;
pub mod v1;

pub use v1::keys::EdgeId;
pub use v1::keys::NodeId;

pub use v1::{indexes, keys, property};
