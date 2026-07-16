//! Access-backed logical stream contract facade.
//!
//! Access contracts keep residual-free sources, residual filters, windows,
//! ordering, distinct, and composed stream pipelines explicit. The concrete
//! invariants live in focused modules:
//!
//! - `path`: residual-free node/edge access candidates.
//! - `window`: statically valid access window composition.
//! - `unary`: filter/window/order/distinct wrappers over access paths.
//! - `pipeline`: stream-pipeline operators and canonical non-empty pipelines.
//! - `stream`: selected-lowering access stream union.

mod path;
mod pipeline;
mod stream;
mod unary;
mod window;

pub use path::{AccessPath, AccessSourceKind, EdgeAccessPath, NodeAccessPath};
pub use pipeline::{AccessPipeline, StreamPipelineOp, StreamPipelineOpKind};
pub use stream::AccessStream;
pub use unary::{AccessDistinct, AccessFilter, AccessOrder, AccessWindow};
pub use window::AccessWindowRange;

pub(in crate::logical) use pipeline::{
    combine_effect, pipeline_ops_effect, validate_stream_pipeline_ops,
};
