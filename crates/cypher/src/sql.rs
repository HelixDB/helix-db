//! SQL frontend: one SQL statement → Cypher [`syntax`] → the shared planner.
//!
//! This is a Rust port of the SQL-to-Cypher translator in
//! [neo4j-jdbc](https://github.com/neo4j/neo4j-jdbc) (`neo4j-jdbc-translator/impl`,
//! Apache-2.0), adapted to HelixDB's Cypher profile. It parses SQL with
//! [`sqlparser`] and builds the same syntax tree the Cypher parser builds, so
//! binding, planning and execution are shared, and every error span points into
//! the SQL text. Translation never reads storage: tables map to labels and
//! joins to relationships by the rules below, not by a schema.
//!
//! # Tables and columns
//!
//! * A table is a node label, after [`Mappings::tables`]. Its Cypher variable is
//!   the lowercased alias, or the lowercased table name: `FROM Person p` matches
//!   `(p:Person)`, and `FROM Person` matches `(person:Person)`.
//! * A table named `Start_TYPE_End`, such as `Person_ACTED_IN_Movie`, is a
//!   relationship table: `(_start:Person)-[person_acted_in_movie:ACTED_IN]->(_end:Movie)`.
//!   Its columns are relationship properties; qualify a column with `Person`
//!   or `_start` (`Movie` or `_end`) to read the start (end) node.
//! * A column is a property: `p.name` reads `p.name`, and an unqualified column
//!   needs exactly one table in scope. Selecting a table alias, such as
//!   `SELECT p FROM Person p`, returns the node itself, `p.*` returns
//!   `properties(p)`, and the virtual column `v$id` returns `id(p)`.
//!
//! # Joins
//!
//! A join chain is read left to right without consulting a schema:
//!
//! * `A a JOIN B b ON b.id = a.b_id` is `(a:A)-[b_id:B_ID]->(b:B)`. The type is
//!   the second column of the `=`, uppercased, unless [`Mappings::join_columns`]
//!   names one; the relationship points away from the table that column belongs to.
//! * `A a JOIN B b USING (col)` is `(a:A)-[col:COL]->(b:B)`, and
//!   `A NATURAL JOIN B` is `(a:A)-->(b:B)`.
//! * In a chain of three tables the middle one is the relationship:
//!   `A a JOIN R r ON .. JOIN B b ON ..` is `(a:A)-[r:R]->(b:B)`, and longer
//!   chains continue in pairs. The direction comes from the last `ON`.
//! * Every `ON` must compare two columns with `=`; filters belong in `WHERE`.
//!   `CROSS JOIN` and a comma start a separate pattern. Outer joins are rejected.
//!
//! # Statements
//!
//! `SELECT` becomes `MATCH .. WHERE .. RETURN`, with `GROUP BY` and `HAVING`
//! through an intermediate `WITH` when Cypher's implicit grouping would differ.
//! `INSERT` becomes `CREATE` (several rows `UNWIND` a list), `UPDATE` becomes
//! `MATCH .. SET`, `DELETE` becomes `MATCH .. DELETE`, and `TRUNCATE` becomes
//! `MATCH .. DETACH DELETE`. An `INSERT` into a relationship table connects
//! existing nodes found by the start and end columns. `?` placeholders become
//! `$1`, `$2`, … in written order, and `:name` or `$name` become `$name`.
//!
//! ```
//! use helix_cypher::sql;
//! let statement = sql::translate(
//!     "SELECT p.name, m.title FROM people p JOIN movies m ON m.id = p.directed WHERE p.born > ?",
//!     &sql::Mappings::default()
//!         .table("people", "Person")
//!         .table("movies", "Movie"),
//! )?;
//! assert_eq!(
//!     statement.to_string(),
//!     "MATCH (p:Person)-[directed:DIRECTED]->(m:Movie) WHERE p.born > $1 RETURN p.name, m.title",
//! );
//! # Ok::<(), helix_cypher::QueryError>(())
//! ```
use crate::syntax::{self, Expr, ExprKind};
use helix_planner::relational::{self as r, category, detail, QueryError, Span};
use sqlparser::{ast, dialect::GenericDialect, parser::Parser, tokenizer};
use std::collections::BTreeMap;

mod dml;
mod expr;
pub mod request;
mod select;
mod tables;

/// How SQL names map onto the graph. Keys match case-insensitively.
///
/// ```
/// let mappings: helix_cypher::sql::Mappings = serde_json::from_str(
///     r#"{"tables": {"people": "Person"}, "join_columns": {"people.movie_id": "DIRECTED"}}"#,
/// )?;
/// assert_eq!(mappings, helix_cypher::sql::Mappings::default()
///     .table("people", "Person")
///     .join_column("people.movie_id", "DIRECTED"));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Mappings {
    /// Table name → node label, or relationship type for a join's middle table.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tables: BTreeMap<String, String>,
    /// `table.column` of a join's `ON` column → relationship type.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub join_columns: BTreeMap<String, String>,
}

impl Mappings {
    /// Map a table to a label or relationship type.
    pub fn table(mut self, table: impl Into<String>, label: impl Into<String>) -> Self {
        self.tables.insert(table.into(), label.into());
        self
    }

    /// Map a join column, written `table.column`, to a relationship type.
    pub fn join_column(mut self, column: impl Into<String>, kind: impl Into<String>) -> Self {
        self.join_columns.insert(column.into(), kind.into());
        self
    }

    fn label(&self, table: &str) -> String {
        lookup(&self.tables, table).unwrap_or(table).to_owned()
    }

    fn join_type(&self, table: &str, column: &str) -> Option<&str> {
        lookup(&self.join_columns, &format!("{table}.{column}"))
    }
}

fn lookup<'a>(map: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    map.iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

/// Translate one SQL statement into Cypher syntax. The result binds and plans
/// like parsed Cypher; [`syntax::Statement`]'s `Display` prints it as Cypher.
///
/// ```
/// let error = helix_cypher::sql::translate("SELECT 1 UNION SELECT 2", &Default::default()).unwrap_err();
/// assert_eq!(error.detail, helix_cypher::api::detail::UNION);
/// ```
pub fn translate(text: &str, mappings: &Mappings) -> Result<syntax::Statement, QueryError> {
    if text.len() > crate::MAX_QUERY_BYTES {
        return Err(QueryError::compile(
            category::RESOURCE_LIMIT,
            detail::QUERY_TOO_LARGE,
            "query text exceeds 16 MiB",
        ));
    }
    // sqlparser materializes every token, one per whitespace character, so
    // bound them before tokenizing.
    if token_estimate(text) > MAX_TOKENS {
        return Err(QueryError::compile(
            category::RESOURCE_LIMIT,
            detail::TOO_MANY_TOKENS,
            "SQL text exceeds the token budget",
        ));
    }
    let lines = Lines::new(text);
    let dialect = GenericDialect {};
    let tokens = tokenizer::Tokenizer::new(&dialect, text)
        .tokenize_with_location()
        .map_err(|error| {
            let failure = QueryError::compile(
                category::SYNTAX_ERROR,
                detail::UNEXPECTED_SYNTAX,
                error.message,
            );
            match lines.offset(error.location) {
                Some(offset) => failure.at(Span {
                    start: offset,
                    end: offset,
                }),
                None => failure,
            }
        })?;
    // Operators chain left-deep in sqlparser's tree, which it then walks and
    // drops recursively; bounding them bounds that recursion.
    if operators(&tokens) > MAX_OPERATORS {
        return Err(QueryError::compile(
            category::RESOURCE_LIMIT,
            detail::EXPRESSION_DEPTH,
            format!("SQL statement uses more than {MAX_OPERATORS} operators"),
        ));
    }
    let positional = tokens
        .iter()
        .filter(|token| matches!(&token.token, tokenizer::Token::Placeholder(p) if p == "?"))
        .filter_map(|token| lines.offset(token.span.start))
        .collect();
    let statements = Parser::new(&dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
        .map_err(|error| parse_error(&lines, error))?;
    let translator = Translator {
        lines,
        mappings,
        positional,
        depth: std::cell::Cell::new(0),
    };
    match statements.as_slice() {
        [statement] => translator.statement(statement),
        [] => Err(QueryError::compile(
            category::SYNTAX_ERROR,
            detail::UNEXPECTED_END,
            "expected a SQL statement",
        )),
        [_, second, ..] => Err(QueryError::compile(
            category::UNSUPPORTED_FEATURE,
            detail::MULTIPLE_STATEMENTS,
            "SQL translation accepts one statement per request",
        )
        .at(translator.at(second))),
    }
}

/// Upper bound on the tokens sqlparser produces after estimating, matching
/// the Cypher lexer's budget for non-whitespace tokens.
const MAX_TOKENS: usize = 1_000_000;
/// Binary and postfix operators per statement.
const MAX_OPERATORS: usize = 1024;

/// An overestimate of sqlparser's token count without tokenizing: every
/// whitespace or punctuation character is a token, as is every run of word
/// characters, quoted text or comment.
fn token_estimate(text: &str) -> usize {
    let mut characters = text.chars().peekable();
    let mut count = 0;
    while let Some(c) = characters.next() {
        count += 1;
        match c {
            '\'' | '"' | '`' => {
                // A doubled quote continues the text; counting it as a new
                // token only overestimates.
                characters.by_ref().find(|&next| next == c);
            }
            '-' if characters.peek() == Some(&'-') => {
                characters.by_ref().find(|&next| next == '\n');
            }
            '/' if characters.peek() == Some(&'*') => {
                characters.next();
                let mut previous = ' ';
                characters
                    .by_ref()
                    .find(|&next| std::mem::replace(&mut previous, next) == '*' && next == '/');
            }
            c if c.is_alphanumeric() || c == '_' => {
                while characters
                    .next_if(|next| next.is_alphanumeric() || *next == '_')
                    .is_some()
                {}
            }
            _ => {}
        }
    }
    count
}

/// Binary and postfix operator tokens. A sign after an operator, an opening
/// bracket or a comma is unary: the parser bounds those by its depth limit.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "sqlparser's token set is open-ended; every other symbol counts as an operator"
)]
fn operators(tokens: &[tokenizer::TokenWithSpan]) -> usize {
    use tokenizer::Token as T;
    let mut previous_operand = false;
    tokens
        .iter()
        .filter(|token| !matches!(token.token, T::Whitespace(_)))
        .filter(|token| {
            let (operand, operator) = match &token.token {
                T::Word(word) => {
                    let keyword = [
                        "AND", "OR", "XOR", "IS", "LIKE", "ILIKE", "IN", "BETWEEN", "COLLATE",
                    ]
                    .iter()
                    .any(|k| word.value.eq_ignore_ascii_case(k))
                        && word.quote_style.is_none();
                    (!keyword, keyword)
                }
                T::Number(..)
                | T::Char(_)
                | T::SingleQuotedString(_)
                | T::DoubleQuotedString(_)
                | T::TripleSingleQuotedString(_)
                | T::TripleDoubleQuotedString(_)
                | T::DollarQuotedString(_)
                | T::NationalStringLiteral(_)
                | T::EscapedStringLiteral(_)
                | T::UnicodeStringLiteral(_)
                | T::HexStringLiteral(_)
                | T::Placeholder(_)
                | T::RParen
                | T::RBracket
                | T::RBrace => (true, false),
                T::Comma
                | T::LParen
                | T::LBracket
                | T::LBrace
                | T::Period
                | T::SemiColon
                | T::Colon
                | T::EOF => (false, false),
                T::Plus | T::Minus => (false, previous_operand),
                _ => (false, true),
            };
            previous_operand = operand;
            operator
        })
        .count()
}

/// Translate, resolve and validate one SQL statement for the planner.
///
/// ```
/// let query = helix_cypher::sql::compile("SELECT 1 AS answer", &Default::default())?;
/// assert_eq!(query.returns()[0].0, "answer");
/// # Ok::<(), helix_cypher::QueryError>(())
/// ```
pub fn compile(
    text: &str,
    mappings: &Mappings,
) -> Result<helix_planner::relational::Query, QueryError> {
    crate::resolve(&translate(text, mappings)?)
}

fn parse_error(lines: &Lines<'_>, error: sqlparser::parser::ParserError) -> QueryError {
    use sqlparser::parser::ParserError as E;
    let message = match error {
        E::RecursionLimitExceeded => {
            return QueryError::compile(
                category::RESOURCE_LIMIT,
                detail::EXPRESSION_DEPTH,
                "SQL nesting exceeds the parser's depth limit",
            );
        }
        E::TokenizerError(message) | E::ParserError(message) => message,
    };
    let error = QueryError::compile(category::SYNTAX_ERROR, detail::UNEXPECTED_SYNTAX, &message);
    // sqlparser reports where it stopped as a trailing "Line: L, Column: C".
    let location = message.rsplit_once("Line: ").and_then(|(_, rest)| {
        let (line, column) = rest.split_once(", Column: ")?;
        let column: String = column.chars().take_while(char::is_ascii_digit).collect();
        Some(tokenizer::Location {
            line: line.parse().ok()?,
            column: column.parse().ok()?,
        })
    });
    match location.and_then(|location| lines.offset(location)) {
        Some(offset) => error.at(Span {
            start: offset,
            end: offset,
        }),
        None => error,
    }
}

/// Byte offsets of line starts, for converting sqlparser's 1-based character
/// locations into the byte spans every Cypher error reports.
struct Lines<'a> {
    text: &'a str,
    starts: Vec<usize>,
}

impl<'a> Lines<'a> {
    fn new(text: &'a str) -> Self {
        let starts = std::iter::once(0)
            .chain(text.match_indices('\n').map(|(index, _)| index + 1))
            .collect();
        Self { text, starts }
    }

    fn offset(&self, location: tokenizer::Location) -> Option<usize> {
        let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
        let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
        let start = *self.starts.get(line)?;
        let rest = &self.text[start..];
        Some(
            start
                + rest
                    .char_indices()
                    .nth(column)
                    .map_or(rest.len(), |(index, _)| index),
        )
    }
}

/// Per-statement translation state. It borrows the source for spans and the
/// mappings for names, and owns nothing that outlives one statement.
struct Translator<'a> {
    lines: Lines<'a>,
    mappings: &'a Mappings,
    /// Byte offsets of every `?` placeholder in written order, so that
    /// translation order cannot renumber them.
    positional: Vec<usize>,
    /// Current expression nesting, bounded before Cypher's own limit applies.
    depth: std::cell::Cell<usize>,
}

impl Translator<'_> {
    fn span(&self, span: tokenizer::Span) -> Option<Span> {
        Some(Span {
            start: self.lines.offset(span.start)?,
            end: self.lines.offset(span.end)?,
        })
    }

    /// The span of a node, or the whole statement when sqlparser has none.
    fn at(&self, node: &impl ast::Spanned) -> Span {
        self.span(node.span()).unwrap_or(Span {
            start: 0,
            end: self.lines.text.len(),
        })
    }

    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "sqlparser's statement set is open-ended; every other kind is unsupported"
    )]
    fn statement(&self, statement: &ast::Statement) -> Result<syntax::Statement, QueryError> {
        match statement {
            ast::Statement::Query(query) => self.query(query),
            ast::Statement::Insert(insert) => self.insert(insert),
            ast::Statement::Update(update) => self.update(update),
            ast::Statement::Delete(delete) => self.delete(delete),
            ast::Statement::Truncate(truncate) => self.truncate(truncate),
            other => Err(unsupported(
                format!("the statement `{}`", first_words(&other.to_string())),
                self.at(other),
            )),
        }
    }
}

fn first_words(text: &str) -> String {
    text.split_whitespace()
        .take(3)
        .collect::<Vec<_>>()
        .join(" ")
}

/// A SQL construct outside the translation, reported as an unsupported feature.
fn unsupported(construct: impl std::fmt::Display, span: Span) -> QueryError {
    QueryError::compile(
        category::UNSUPPORTED_FEATURE,
        detail::SQL_CONSTRUCT,
        format!("SQL translation does not support {construct}"),
    )
    .at(span)
}

/// A statement whose meaning the translation cannot determine.
fn invalid(detail: &str, message: impl Into<String>, span: Span) -> QueryError {
    QueryError::compile(category::SYNTAX_ERROR, detail, message).at(span)
}

fn node(kind: ExprKind, span: Span) -> Result<Expr, QueryError> {
    Expr::new(kind, span)
}

fn variable(name: &str, span: Span) -> Result<Expr, QueryError> {
    node(ExprKind::Variable(name.to_owned()), span)
}

fn literal(value: r::Value, span: Span) -> Result<Expr, QueryError> {
    node(ExprKind::Literal(value), span)
}

fn call(name: &str, arguments: Vec<Expr>, span: Span) -> Result<Expr, QueryError> {
    node(
        ExprKind::Call {
            name: name.to_owned(),
            arguments,
            distinct: false,
            star: false,
        },
        span,
    )
}

fn property(value: Expr, key: &str, span: Span) -> Result<Expr, QueryError> {
    node(ExprKind::Property(Box::new(value), key.to_owned()), span)
}

fn binary(operator: r::Binary, left: Expr, right: Expr, span: Span) -> Result<Expr, QueryError> {
    node(
        ExprKind::Binary(operator, Box::new(left), Box::new(right)),
        span,
    )
}

/// A projection item whose column is named like the Cypher text of `expression`.
fn item(expression: Expr, alias: Option<String>) -> syntax::Item {
    syntax::Item::Expression {
        text: expression.to_string(),
        expression,
        alias,
    }
}

#[cfg(test)]
mod tests;
