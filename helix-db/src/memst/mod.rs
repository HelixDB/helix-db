//! MemSt integration: Git-like memory architecture for LLM session management.
//!
//! This module integrates the core of the [`memst`](https://github.com/yfyang86/memst)
//! crate into HelixDB. It provides two main capabilities:
//!
//! 1. **Tiered memory management** ([`memory`]): Working / ShortTerm / LongTerm
//!    lifecycle with promotion, demotion, compaction checkpoints and token budgets.
//! 2. **File and session management** ([`store`]): on-disk session layout with
//!    messages, operation logs, per-tier memory persistence and a manifest index.
//!
//! Supporting modules:
//! - [`types`] - Message, Role, Content, MemoryItem, MemoryTier, SessionMetadata, ...
//! - [`error`] - shared `Result` and `Error` types.
//! - [`objects`] - content-addressable object store (BLAKE3-hashed blobs / trees /
//!   commits / tags) used as the persistent backbone for sessions and memories.

pub mod error;
pub mod memory;
pub mod objects;
pub mod store;
pub mod types;

pub use error::{Error, Result};
pub use memory::{
    CompactionCheckpoint, ConfiguredTokenCounter, LifecycleConfig, MemoryLifecycle, MemoryState,
    SimpleTokenCounter, TokenCounter, Transition, TransitionTrigger,
};
pub use objects::{
    Author, Blob, Commit, CommitMetadata, CommitSource, MemoryScope, ObjectId, ObjectStore, Tag,
    Tree, TreeEntry,
};
pub use store::{MessageIndexEntry, SessionStore};
pub use types::{
    Content, ContentPart, Manifest, MemoryId, MemoryItem, MemoryQuery, MemoryTier, MemoryType,
    Message, Operation, OperationQuery, OperationType, Role, SessionId, SessionMetadata,
    SessionSummary,
};
