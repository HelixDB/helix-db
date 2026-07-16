//! Shared planner IR facade.
//!
//! The concrete contracts live in focused submodules so invariant-heavy ADTs
//! can be tested independently while keeping the stable `ir::*` surface.

mod access;
mod batch;
mod bounds;
mod contracts;
mod control_flow;
mod expr;
mod index;
mod index_ddl;
mod input;
mod mutation;
mod op;
mod order;
mod projection;
mod shortest_path;
mod stream;

pub use access::*;
pub use batch::*;
pub use bounds::*;
pub use contracts::{AtLeast, ElementIds, ElementIdsError, NonEmptyString};
pub use control_flow::*;
pub use expr::*;
pub use index::*;
pub use index_ddl::*;
pub use input::*;
pub use mutation::*;
pub use op::*;
pub use order::*;
pub use projection::*;
pub use shortest_path::*;
pub use stream::*;
