//! Cypher frontend: text → syntax → resolved shared relational query.
//!
//! This crate never reads storage or emits the native traversal AST. It also
//! owns the Cypher API contract ([`request`] and [`api`]) so that every
//! transport, including ones without a database, decodes, routes and reports
//! errors identically.

pub mod api;
mod bind;
mod lexer;
mod parser;
pub mod request;
pub mod syntax;

use helix_planner::relational::{category, detail};
pub use helix_planner::relational::{QueryError, Span};

/// Parse a single statement without assigning meaning to variable names.
/// The returned syntax owns its names and literal values.
///
/// ```
/// let statement = {
///     let text = String::from("RETURN 'owned' AS value");
///     helix_cypher::parse(&text)?
/// };
/// assert_eq!(statement.clauses.len(), 1);
/// # Ok::<(), helix_cypher::QueryError>(())
/// ```
pub fn parse(text: &str) -> Result<syntax::Statement, QueryError> {
    if text.len() > 16 * 1024 * 1024 {
        return Err(QueryError::compile(
            category::RESOURCE_LIMIT,
            detail::QUERY_TOO_LARGE,
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

/// A statement's shape without its values, for telemetry: every string,
/// number and boolean literal becomes `?`, comments are dropped, and the
/// remaining tokens (keywords, names, parameters and punctuation) are joined by
/// single spaces.
///
/// ```
/// let shape = helix_cypher::redact_literals(
///     "MATCH (p:Person {name:'Ada'}) /* note */ WHERE p.age > 30 AND p.vip = True RETURN p.name, $limit",
/// )?;
/// assert_eq!(
///     shape,
///     "MATCH ( p : Person { name : ? } ) WHERE p . age > ? AND p . vip = ? RETURN p . name , $limit",
/// );
/// # Ok::<(), helix_cypher::QueryError>(())
/// ```
pub fn redact_literals(text: &str) -> Result<String, QueryError> {
    Ok(lexer::lex(text)?
        .iter()
        .filter_map(|token| match &token.kind {
            lexer::Kind::String(_) | lexer::Kind::Number(_) => Some("?"),
            // Boolean literals lex as words in any letter case.
            lexer::Kind::Word(word)
                if word.eq_ignore_ascii_case("true") || word.eq_ignore_ascii_case("false") =>
            {
                Some("?")
            }
            lexer::Kind::Word(_)
            | lexer::Kind::Escaped(_)
            | lexer::Kind::Parameter(_)
            | lexer::Kind::Symbol(_)
            | lexer::Kind::Pattern(_) => Some(&text[token.span.start..token.span.end]),
            lexer::Kind::End => None,
        })
        .collect::<Vec<_>>()
        .join(" "))
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
