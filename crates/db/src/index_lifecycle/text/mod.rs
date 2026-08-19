//! Durable V2 text-index lifecycle boundaries.
//!
//! Text construction uploads immutable content-addressed blobs before staging
//! their references in SlateDB. Database transactions remain the sole
//! visibility authority for build, mutation, manifest, and cleanup state.

use std::num::NonZeroUsize;

const ACTIVE_TEXT_DESTINATION_CONCURRENCY: usize = 8;

/// Returns a positive, request-size-independent destination work window.
fn active_text_destination_concurrency(work_items: usize) -> NonZeroUsize {
    NonZeroUsize::new(work_items.clamp(1, ACTIVE_TEXT_DESTINATION_CONCURRENCY))
        .expect("the Active text destination work window is always positive")
}

pub(crate) mod active_batch;
mod active_preflight;
#[cfg(feature = "production-coverage")]
pub(crate) use active_preflight::production_contracts::run as run_active_preflight_contracts;
pub(crate) mod active_publication;
#[cfg(any(test, feature = "production-coverage"))]
mod active_retirement;
pub(crate) mod active_runtime;
#[cfg(feature = "production-coverage")]
pub(crate) use active_retirement::production_contracts::run as run_active_retirement_contracts;
pub(crate) mod attachment;
mod cleanup;
mod compaction;
pub(crate) mod driver;
mod manifest;
pub(crate) mod mutation;
mod projection;
pub(crate) mod serving;
pub(crate) mod statistics;
#[cfg(test)]
mod test_support;
#[cfg(feature = "production-coverage")]
pub(crate) use serving::production_contracts::run as run_serving_contracts;
pub(crate) mod active_compaction;
mod validation;
