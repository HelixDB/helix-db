//! Transport contract for the Cypher API.
//!
//! Every server that accepts Cypher, whether the standalone server or a gateway
//! in front of database nodes, serves these paths and reports a [`QueryError`]
//! with the same status and body. Clients see one API wherever they connect.
use crate::QueryError;
/// The codes a [`QueryError`] reports, in the lower snake case of every
/// HelixDB error code.
pub use helix_planner::relational::{category, detail};
use helix_planner::relational::{ErrorPhase, Span};
use serde::Serialize;

/// Executes one statement. The body is a [`crate::request::Request`].
pub const HTTP_PATH: &str = "/v2/cypher";
/// Plans one statement without executing it. The body is a [`crate::request::Request`].
pub const HTTP_EXPLAIN_PATH: &str = "/v2/cypher/explain";

/// How a transport reports a [`QueryError`].
///
/// Transports match every variant, so a new class cannot silently be reported
/// as another one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// The statement or its parameters are invalid; resending it unchanged fails again.
    InvalidQuery,
    /// A per-query resource budget was exceeded. A modifying statement committed nothing.
    ResourceLimit,
    /// A modifying statement reached a reader or a warm-only request.
    WriterRequired,
    /// The planner broke one of its own invariants.
    Internal,
}

impl ErrorClass {
    /// Classify an error by its stable category.
    ///
    /// ```
    /// use helix_cypher::api::ErrorClass;
    /// let error = helix_cypher::compile("RETURN (").unwrap_err();
    /// assert_eq!(ErrorClass::of(&error), ErrorClass::InvalidQuery);
    /// assert_eq!(ErrorClass::of(&error).http_status(), 400);
    /// ```
    pub fn of(error: &QueryError) -> Self {
        match error.category.as_str() {
            category::RESOURCE_LIMIT => Self::ResourceLimit,
            category::ACCESS_MODE_ERROR => Self::WriterRequired,
            category::INTERNAL_PLANNER_ERROR => Self::Internal,
            _ => Self::InvalidQuery,
        }
    }

    /// The status every Cypher HTTP endpoint returns for this class.
    pub const fn http_status(self) -> u16 {
        match self {
            Self::InvalidQuery => 400,
            Self::ResourceLimit => 429,
            Self::WriterRequired => 503,
            Self::Internal => 500,
        }
    }
}

/// The JSON body every Cypher HTTP endpoint returns for a [`QueryError`]. The
/// gRPC methods carry the same body in their status details.
///
/// ```
/// use helix_cypher::api::{category, detail};
/// let error = helix_cypher::QueryError::compile(
///     category::SYNTAX_ERROR,
///     detail::UNEXPECTED_END,
///     "unexpected end",
/// );
/// let body = serde_json::to_value(helix_cypher::api::ErrorBody::from(&error))?;
/// assert_eq!(body, serde_json::json!({
///     "error": "syntax_error",
///     "msg": "unexpected end",
///     "details": {"detail": "unexpected_end", "phase": "compile", "span": null},
/// }));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Serialize)]
pub struct ErrorBody<'a> {
    error: &'a str,
    msg: &'a str,
    details: ErrorDetails<'a>,
}

#[derive(Debug, Serialize)]
struct ErrorDetails<'a> {
    detail: &'a str,
    phase: ErrorPhase,
    span: Option<Span>,
}

impl<'a> From<&'a QueryError> for ErrorBody<'a> {
    fn from(error: &'a QueryError) -> Self {
        Self {
            error: &error.category,
            msg: &error.message,
            details: ErrorDetails {
                detail: &error.detail,
                phase: error.phase,
                span: error.span,
            },
        }
    }
}
