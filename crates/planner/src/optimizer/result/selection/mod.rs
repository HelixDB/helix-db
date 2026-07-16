//! Recursive best-plan selection over retained alternatives.
//!
//! Selection accounts for required delivered properties and for child groups
//! that execute as separate selected roots. Error, summary, session, and result
//! extension APIs live in separate modules so each contract stays testable.

mod api;
mod error;
mod session;
mod summary;

pub use self::error::SelectionError;
pub use self::session::SelectionSession;
pub use self::summary::{RootSelectionFailure, RootSelectionSummary};
