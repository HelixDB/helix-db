//! Selected ordinary-alternative lowering contracts.
//!
//! Classification, classified dispatch, and post-lowering contract overrides are
//! kept separate so new selected families do not expand one mixed module.

mod classify;
mod dispatch;
mod override_contract;
