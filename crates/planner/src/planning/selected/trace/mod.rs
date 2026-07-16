//! Selected executable-root trace reconstruction.
//!
//! Native selected planning does not build compatibility physical trees, so the
//! executable envelope records selected batch/root provenance directly from the
//! selected executable IR boundary. The submodules separate batch-entry path
//! traversal, selected-root provenance formatting, event construction, and test
//! fixtures so interpreter handoff diagnostics can evolve without one large
//! trace helper accumulating unrelated responsibilities.

mod entries;
mod event;
mod root;

#[cfg(test)]
mod tests;

pub(in crate::planning) use self::entries::append_selected_trace;
