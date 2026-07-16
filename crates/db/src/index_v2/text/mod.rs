//! Durable V2 text-index lifecycle boundaries.
//!
//! Text work is split by ownership: upload-intent persistence lives here,
//! object I/O belongs to the publication coordinator, and later build,
//! mutation, manifest, and GC drivers consume only those typed contracts.

mod active_attachment;
pub(crate) mod active_mutation;
mod active_preflight;
pub(crate) mod active_publication;
pub(crate) mod active_request;
pub(crate) mod active_resolution;
mod active_retirement;
pub(crate) mod attachment;
pub(crate) mod blob_gc;
mod build_owner;
mod cleanup;
mod compaction;
pub(crate) mod driver;
mod manifest;
pub(crate) mod mutation;
mod reclaim;
pub(crate) mod reconciliation;
pub(crate) mod serving;
pub(crate) mod upload;
pub(crate) mod upload_queue;
mod validation;
