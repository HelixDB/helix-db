//! Stream and stream-terminal optimizer rule facade.
//!
//! Stream rules are split by optimizer contract boundary:
//!
//! - `window` owns logical stream-window composition rewrites.
//! - `implementation` owns physical implementation rules for stream operators
//!   and stream terminals.
//!
//! The stable public rule types are re-exported from this facade.

mod implementation;
mod window;

pub use self::{
    implementation::{
        StreamAggregateImplementationRule, StreamImplementationRule,
        StreamProjectImplementationRule, StreamReservedImplementationRule,
        StreamVariableWriteImplementationRule,
    },
    window::StreamCompositionRule,
};
