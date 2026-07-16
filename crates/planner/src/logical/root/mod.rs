//! Root-stream, terminal, and root barrier contracts.
//!
//! Root contracts preserve executable payloads for selected Cascades roots.
//! They encode supported root-stream inputs, terminal payloads, mutations, DDL,
//! and traversal control-flow as ADTs rather than compatibility physical trees.

mod barrier;
mod pipeline;
mod shortest_path;
mod stream;
mod terminal;

pub use self::barrier::{RootBranch, RootIndexDdl, RootMutation, RootRepeat};
pub use self::pipeline::RootPipeline;
pub use self::shortest_path::RootShortestPath;
pub use self::stream::RootStream;
pub use self::terminal::{StreamAggregate, StreamProject, StreamReserved, StreamVariableWrite};
