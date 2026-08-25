//! Graph data types and encoding

pub mod error;
#[deprecated(note = "use encoding::v2")]
pub mod v1;
pub mod v2;

pub use v2::keys::{self, indexes};
pub use v2::keys::{EdgeId, NodeId};
pub use v2::values::{self, property};
