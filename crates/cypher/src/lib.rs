//! Cypher frontend: text → syntax → resolved shared relational query.
//!
//! This crate never reads storage or emits the native traversal AST.

mod bind;
mod lexer;
mod parser;
pub mod syntax;

pub use helix_planner::relational::{QueryError, Span};

/// Parse a single statement without assigning meaning to variable names.
pub fn parse(text: &str) -> Result<syntax::Statement, QueryError> {
    if text.len() > 16 * 1024 * 1024 {
        return Err(QueryError::compile(
            "ResourceLimit",
            "QueryTooLarge",
            "query text exceeds 16 MiB",
        ));
    }
    parser::parse(text)
}

/// Resolve names, validate capabilities, and build the common planner input.
pub fn resolve(
    statement: &syntax::Statement,
) -> Result<helix_planner::relational::Query, QueryError> {
    bind::resolve(statement)
}

/// Compile Cypher into the shared logical query contract.
///
/// ```
/// let query = helix_cypher::compile("RETURN 1 AS answer")?;
/// assert_eq!(query.returns()[0].0, "answer");
/// # Ok::<(), helix_cypher::QueryError>(())
/// ```
pub fn compile(text: &str) -> Result<helix_planner::relational::Query, QueryError> {
    resolve(&parse(text)?)
}
