//! Language-independent, slot-addressed graph and relational query contracts.
//!
//! Frontends resolve names before constructing a [`Query`]. The validator is
//! the trust boundary: physical planning and execution only accept a validated
//! query. Storage values and frontend syntax do not belong in this module.

mod explain;
pub use explain::*;
mod graph_order;
pub use graph_order::*;
mod aggregation;
pub use aggregation::Accumulator;
mod contracts;
pub use contracts::*;

mod evaluation;
mod expression;
mod graph_values;
mod input_window;
pub use input_window::InputWindow;
mod pipeline;
mod planning;
mod projection;
pub use pipeline::*;
pub use projection::*;
mod query;
mod types;
mod value;
pub use types::ValueType;

pub use evaluation::*;
pub use expression::*;
pub use graph_values::*;
pub use planning::*;
pub use query::*;
pub use value::*;

/// The source location of a diagnostic, measured in UTF-8 bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

/// A stable failure phase, independent of transport and frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorPhase {
    Compile,
    Runtime,
}

/// A query error with machine-readable classification and optional source span.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[error("{category}: {detail}: {message}")]
pub struct QueryError {
    pub category: String,
    pub detail: String,
    pub phase: ErrorPhase,
    pub message: String,
    pub span: Option<Span>,
}

impl QueryError {
    pub fn compile(category: &str, detail: &str, message: impl Into<String>) -> Self {
        Self {
            category: category.into(),
            detail: detail.into(),
            phase: ErrorPhase::Compile,
            message: message.into(),
            span: None,
        }
    }

    pub fn runtime(category: &str, detail: &str, message: impl Into<String>) -> Self {
        Self {
            category: category.into(),
            detail: detail.into(),
            phase: ErrorPhase::Runtime,
            message: message.into(),
            span: None,
        }
    }

    pub fn at(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }

    pub fn unsupported(feature: &str) -> Self {
        Self::compile(
            "UnsupportedFeature",
            feature,
            format!("{feature} is outside the Cypher MVP profile"),
        )
    }
}

pub type Result<T> = std::result::Result<T, QueryError>;

/// Maximum nesting accepted before recursive compilation or evaluation. This
/// bound is exercised on Rust's default test-thread stack as well as workers.
pub const MAX_EXPRESSION_DEPTH: usize = 48;
