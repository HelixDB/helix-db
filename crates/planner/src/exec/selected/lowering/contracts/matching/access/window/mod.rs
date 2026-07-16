//! Selected access-window physical suffix matching contracts.
//!
//! Matching outcomes and physical suffix recognition stay split so unsupported
//! suffix reasons remain explicit without widening the selected-lowering API.

mod contracts;
mod suffix;

#[cfg(test)]
mod tests;

pub(in crate::exec::selected::lowering) use suffix::selected_access_window_pipeline_matches;
