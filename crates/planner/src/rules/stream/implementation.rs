//! Stream physical implementation rule facade.
//!
//! Terminal seed rules and standalone stream-operator seed rules are separate
//! contract modules behind this stable facade.

mod operators;
mod terminals;

pub use operators::StreamImplementationRule;
pub use terminals::{
    StreamAggregateImplementationRule, StreamProjectImplementationRule,
    StreamReservedImplementationRule, StreamVariableWriteImplementationRule,
};
