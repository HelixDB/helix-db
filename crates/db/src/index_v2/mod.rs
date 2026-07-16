//! Canonical V2 index lifecycle contracts.
//!
//! This module owns the validated logical model that the V2 key/value codecs,
//! catalog loader, DDL repository, and outbox worker share. Runtime index
//! configuration is an adapter into this model; it is not a persistence shape.
//!
//! ```
//! use db::config::SecondaryIndexDefinition;
//! use db::index_v2::{
//!     IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2,
//!     IndexRevision, PhysicalGeneration, ValidatedDynamicIndexDefinition,
//! };
//!
//! let definition = ValidatedDynamicIndexDefinition::try_from(
//!     SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
//! )
//! .unwrap();
//! let operation = IndexOperationId::new_v4();
//! let record = IndexRecordV2::building(
//!     IndexId::new(1).unwrap(),
//!     definition,
//!     IndexRevision::initial(),
//!     PhysicalGeneration::Secondary {
//!         generation: IndexGenerationId::initial(),
//!     },
//!     operation,
//! )
//! .unwrap();
//! assert_eq!(record.index_id().get(), 1);
//! ```

#![deny(missing_docs)]

pub mod blob_publication;
mod catalog;
pub(crate) mod failpoints;
pub(crate) mod lifecycle;
mod metadata;
mod model;
mod operation;
pub(crate) mod outbox;
mod public;
pub(crate) mod read_guard;
pub mod reader_lease;
mod reader_lifecycle;
pub(crate) mod reconciliation;
pub(crate) mod repository;
mod scope_gate;
pub(crate) mod secondary;
pub(crate) mod text;
pub(crate) mod vector;
pub(crate) mod work;
pub(crate) mod worker;

pub(crate) use catalog::*;
pub(crate) use metadata::*;
pub use model::*;
pub(crate) use operation::TEXT_COMPACTION_INPUT_KEY_MAX;
pub use operation::*;
pub use public::*;
pub(crate) use scope_gate::*;
pub use work::{BlobRef, TextPartition};
