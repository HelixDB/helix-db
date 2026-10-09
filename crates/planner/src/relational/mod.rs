//! Language-independent, slot-addressed graph and relational query contracts.
//!
//! Frontends resolve names before constructing a [`Query`]. The validator is
//! the trust boundary: physical planning and execution only accept a validated
//! query. Storage values and frontend syntax do not belong in this module.

pub mod allocation;

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
mod layout;
pub use layout::{RowCell, RowLayout, RowLayoutMode, RowProgram, RowProgramQuery};
mod consumers;
mod pipeline;
mod planning;
mod projection;
mod reference;
pub use pipeline::*;
pub use projection::*;
mod query;
mod types;
mod value;
mod window;
pub use types::ValueType;
pub use window::Window;

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

mod codes;
pub use codes::{category, detail};

/// A stable failure phase, independent of transport and frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorPhase {
    Compile,
    Runtime,
}

impl ErrorPhase {
    /// The phase as it appears in error codes and bodies.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Compile => "compile",
            Self::Runtime => "runtime",
        }
    }
}

/// A query error with machine-readable classification and optional source span.
///
/// `category` and `detail` hold codes from [`category`] and [`detail`].
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

    /// A construct outside the supported profile. `detail` is its code and
    /// `construct` names it for the message, such as `MERGE`.
    pub fn unsupported(detail: &str, construct: impl std::fmt::Display) -> Self {
        Self::compile(
            category::UNSUPPORTED_FEATURE,
            detail,
            format!("the Cypher MVP profile does not support {construct}"),
        )
    }

    /// One code for transports with a single code field, such as the embedded
    /// bindings: `category:phase:detail`.
    ///
    /// ```
    /// use helix_planner::relational::{category, detail, QueryError};
    /// let error = QueryError::compile(category::SYNTAX_ERROR, detail::UNDEFINED_VARIABLE, "x");
    /// assert_eq!(error.code(), "syntax_error:compile:undefined_variable");
    /// ```
    pub fn code(&self) -> String {
        format!("{}:{}:{}", self.category, self.phase.as_str(), self.detail)
    }
}

pub type Result<T> = std::result::Result<T, QueryError>;

/// Maximum nesting accepted before recursive compilation or evaluation. This
/// bound is exercised on Rust's default test-thread stack as well as workers.
pub const MAX_EXPRESSION_DEPTH: usize = 48;

/// Maximum nodes one expression may hold. Planning also spends at most this
/// many nodes, across all of a query's MATCH clauses, on the values it copies
/// while substituting bindings through the clauses after each MATCH.
///
/// ```
/// use helix_planner::relational as r;
/// let list = |nodes: usize| r::Expression::List(vec![r::Expression::Slot(r::Slot(0)); nodes - 1]);
/// list(r::MAX_EXPRESSION_NODES).validate_shape().unwrap();
/// assert!(list(r::MAX_EXPRESSION_NODES + 1).validate_shape().is_err());
/// ```
pub const MAX_EXPRESSION_NODES: usize = 200_000;

mod program;
pub use program::*;
mod selection;
pub use selection::*;
