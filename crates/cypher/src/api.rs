//! Transport contract for the Cypher API.
//!
//! Every server that accepts Cypher, whether the standalone server or a gateway
//! in front of database nodes, serves these paths and reports a [`QueryError`]
//! with the same status and body. Clients see one API wherever they connect.
use crate::QueryError;
use helix_planner::relational::{ErrorPhase, Span};
use serde::Serialize;
use std::fmt;

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
            "ResourceLimit" => Self::ResourceLimit,
            "AccessModeError" => Self::WriterRequired,
            "InternalPlannerError" => Self::Internal,
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

/// A [`QueryError`]'s classification in the code style of every HelixDB error:
/// lower snake case, like the native `index_not_found`.
///
/// The planner keeps upper camel case categories and details, the spelling the
/// openCypher TCK uses. Every transport converts them here, so a client sees
/// one spelling wherever it connects.
///
/// ```
/// let error = helix_cypher::QueryError::runtime("ResourceLimit", "MemoryLimit", "over budget");
/// let code = helix_cypher::api::ErrorCode::from(&error);
/// assert_eq!(code.category, "resource_limit");
/// assert_eq!(code.detail, "memory_limit");
/// assert_eq!(code.to_string(), "resource_limit:runtime:memory_limit");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorCode {
    pub category: String,
    pub phase: ErrorPhase,
    pub detail: String,
}

impl From<&QueryError> for ErrorCode {
    fn from(error: &QueryError) -> Self {
        Self {
            category: snake_case(&error.category),
            phase: error.phase,
            detail: snake_case(&error.detail),
        }
    }
}

/// `category:phase:detail`, for transports with a single code field, such as the
/// embedded bindings.
impl fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let phase = match self.phase {
            ErrorPhase::Compile => "compile",
            ErrorPhase::Runtime => "runtime",
        };
        write!(formatter, "{}:{phase}:{}", self.category, self.detail)
    }
}

/// Converts `UpperCamelCase` to `lower_snake_case`. An acronym stays one word
/// (`IDOverflow` becomes `id_overflow`), and lower snake case input is unchanged.
fn snake_case(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    chars.iter().enumerate().fold(
        String::with_capacity(name.len() + 8),
        |mut snake, (index, &c)| {
            let starts_word = c.is_ascii_uppercase()
                && index > 0
                && (chars[index - 1].is_ascii_lowercase()
                    || chars[index - 1].is_ascii_digit()
                    || chars.get(index + 1).is_some_and(char::is_ascii_lowercase));
            if starts_word && !snake.ends_with('_') {
                snake.push('_');
            }
            snake.push(c.to_ascii_lowercase());
            snake
        },
    )
}

/// The JSON body every Cypher HTTP endpoint returns for a [`QueryError`]. The
/// gRPC methods carry the same body in their status details.
///
/// ```
/// let error = helix_cypher::QueryError::compile("SyntaxError", "UnexpectedEnd", "unexpected end");
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
    error: String,
    msg: &'a str,
    details: ErrorDetails,
}

#[derive(Debug, Serialize)]
struct ErrorDetails {
    detail: String,
    phase: ErrorPhase,
    span: Option<Span>,
}

impl<'a> From<&'a QueryError> for ErrorBody<'a> {
    fn from(error: &'a QueryError) -> Self {
        let code = ErrorCode::from(error);
        Self {
            error: code.category,
            msg: &error.message,
            details: ErrorDetails {
                detail: code.detail,
                phase: code.phase,
                span: error.span,
            },
        }
    }
}
