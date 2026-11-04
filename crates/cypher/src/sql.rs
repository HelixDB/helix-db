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
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};

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
}

/// Entries by lowercased key, for case-insensitive lookups in constant time,
/// as unquoted SQL names compare. Of keys that differ only in case, the first
/// wins.
fn lowercased<K: AsRef<str>, V>(entries: impl IntoIterator<Item = (K, V)>) -> HashMap<String, V> {
    let mut lowered = HashMap::new();
    for (key, value) in entries {
        lowered.entry(key.as_ref().to_lowercase()).or_insert(value);
    }
    lowered
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
        labels: lowercased(mappings.tables.iter().map(|(k, v)| (k, v.as_str()))),
        join_types: lowercased(mappings.join_columns.iter().map(|(k, v)| (k, v.as_str()))),
        positional,
        depth: Cell::new(0),
        copied: Cell::new(0),
        printed: Cell::new(0),
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

/// Bound on [`token_estimate`], about 44 MB of sqlparser tokens at most.
const MAX_TOKENS: usize = 500_000;
/// Binary and postfix operators per statement.
const MAX_OPERATORS: usize = 1024;

/// An upper bound on sqlparser's token count, computed without tokenizing.
/// Every character is a token except within a run of letters, ASCII digits
/// and underscores, which counts once plus once more wherever digits start or
/// stop, as in `0a`. sqlparser only ends tokens at such places, so no quoting
/// or comment can make this undercount; quoted text and comments simply count
/// as their words and symbols. Large values belong in parameters, which are
/// not tokenized.
fn token_estimate(text: &str) -> usize {
    let word = |c: &char| c.is_alphabetic() || c.is_ascii_digit() || *c == '_';
    let mut characters = text.chars().peekable();
    let mut count = 0;
    while let Some(c) = characters.next() {
        count += 1;
        let mut digit = c.is_ascii_digit();
        if word(&c) {
            while let Some(next) = characters.next_if(word) {
                if next.is_ascii_digit() != digit {
                    digit = !digit;
                    count += 1;
                }
            }
        }
    }
    count
}

/// Binary and postfix operator tokens. Every unquoted keyword counts, except
/// literals and the clause keywords that never join two expressions, so an
/// infix keyword such as `OVERLAPS`, including one a newer sqlparser adds,
/// is counted by default; a keyword used as a column name only overcounts.
/// A sign counts unless it follows an opening bracket, a comma or a symbolic
/// operator, where it can only be unary and the parser bounds it by its depth
/// limit. Any word may be a column name, so a sign after one always counts.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "sqlparser's token and keyword sets are open-ended; every other one counts as an operator"
)]
fn operators(tokens: &[tokenizer::TokenWithSpan]) -> usize {
    use sqlparser::keywords::Keyword as K;
    use tokenizer::Token as T;
    let mut previous_operand = false;
    tokens
        .iter()
        .filter(|token| !matches!(token.token, T::Whitespace(_)))
        .filter(|token| {
            let (operand, operator) = match &token.token {
                T::Word(word) if word.quote_style.is_none() => match word.keyword {
                    K::NoKeyword | K::NULL | K::TRUE | K::FALSE => (true, false),
                    K::SELECT
                    | K::FROM
                    | K::WHERE
                    | K::GROUP
                    | K::BY
                    | K::HAVING
                    | K::ORDER
                    | K::ASC
                    | K::DESC
                    | K::NULLS
                    | K::LAST
                    | K::LIMIT
                    | K::OFFSET
                    | K::FETCH
                    | K::FIRST
                    | K::NEXT
                    | K::ROW
                    | K::ROWS
                    | K::ONLY
                    | K::TOP
                    | K::DISTINCT
                    | K::AS
                    | K::ON
                    | K::USING
                    | K::JOIN
                    | K::INNER
                    | K::CROSS
                    | K::NATURAL
                    | K::INSERT
                    | K::INTO
                    | K::VALUES
                    | K::UPDATE
                    | K::SET
                    | K::DELETE
                    | K::TRUNCATE
                    | K::TABLE
                    | K::RETURNING
                    | K::CASE
                    | K::WHEN
                    | K::THEN
                    | K::ELSE
                    | K::END
                    | K::CAST => (true, false),
                    _ => (true, true),
                },
                T::Word(_) => (true, false),
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

/// Converts sqlparser's 1-based line and character locations into the byte
/// offsets every Cypher error span reports, in constant time per location.
struct Lines<'a> {
    text: &'a str,
    /// Byte offset of each line start.
    starts: Vec<usize>,
    /// For text with multibyte characters only.
    characters: Option<Characters>,
}

/// Character positions of non-ASCII text: the character index of each line
/// start, and the byte offset of every [`Characters::STRIDE`]th character, so
/// a lookup walks at most one stride.
struct Characters {
    lines: Vec<usize>,
    marks: Vec<usize>,
}

impl Characters {
    const STRIDE: usize = 64;
}

impl<'a> Lines<'a> {
    fn new(text: &'a str) -> Self {
        let starts = std::iter::once(0)
            .chain(text.match_indices('\n').map(|(index, _)| index + 1))
            .collect();
        let characters = (!text.is_ascii()).then(|| {
            let (mut lines, mut marks) = (vec![0], Vec::new());
            for (index, (offset, c)) in text.char_indices().enumerate() {
                if index % Characters::STRIDE == 0 {
                    marks.push(offset);
                }
                if c == '\n' {
                    lines.push(index + 1);
                }
            }
            Characters { lines, marks }
        });
        Self {
            text,
            starts,
            characters,
        }
    }

    fn offset(&self, location: tokenizer::Location) -> Option<usize> {
        let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
        let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
        let start = *self.starts.get(line)?;
        let offset = match &self.characters {
            None => start.saturating_add(column),
            Some(characters) => {
                let index = characters.lines.get(line)? + column;
                let mark = characters
                    .marks
                    .get(index / Characters::STRIDE)
                    .copied()
                    .unwrap_or(self.text.len());
                let rest = &self.text[mark..];
                mark + rest
                    .char_indices()
                    .nth(index % Characters::STRIDE)
                    .map_or(rest.len(), |(offset, _)| offset)
            }
        };
        Some(offset.min(self.text.len()))
    }
}

/// Per-statement translation state. It borrows the source for spans and the
/// mappings for names, and owns nothing that outlives one statement.
struct Translator<'a> {
    lines: Lines<'a>,
    /// [`Mappings::tables`] by lowercased table name.
    labels: HashMap<String, &'a str>,
    /// [`Mappings::join_columns`] by lowercased `table.column`.
    join_types: HashMap<String, &'a str>,
    /// Byte offsets of every `?` placeholder in written order, so that
    /// translation order cannot renumber them.
    positional: Vec<usize>,
    /// Current expression nesting, bounded before Cypher's own limit applies.
    depth: Cell<usize>,
    /// Weight of the expressions copied so far; see [`Translator::copy`].
    copied: Cell<usize>,
    /// Bytes printed so far to compare expressions; see [`Translator::text`].
    printed: Cell<usize>,
}

/// Bound on the weight of copied expressions per statement. A node weighs
/// [`NODE_WEIGHT`] and each byte of its Cypher text one more, so copies hold
/// about 16 MB at most, whether their nodes are many or carry long names and
/// literals.
const MAX_COPIED_WEIGHT: usize = 16 << 20;
/// About the memory of one expression node.
const NODE_WEIGHT: usize = 64;
/// Bound on the Cypher text printed per statement to compare expressions.
const MAX_PRINTED_BYTES: usize = 64 << 20;

impl Translator<'_> {
    fn label(&self, table: &str) -> String {
        self.labels
            .get(&table.to_lowercase())
            .map_or_else(|| table.to_owned(), |label| (*label).to_owned())
    }

    fn join_type(&self, table: &str, column: &str) -> Option<&str> {
        self.join_types
            .get(&format!("{table}.{column}").to_lowercase())
            .copied()
    }

    /// Copy a translated expression. SQL forms that repeat an operand, such
    /// as `BETWEEN`, `NULLIF` and references to an alias or select-list
    /// position, copy it, so nesting them could otherwise grow the tree
    /// exponentially in the text's length, and repeating a reference to a
    /// long literal would copy it each time. The copies share one budget.
    fn copy(&self, expression: &Expr) -> Result<Expr, QueryError> {
        struct Length(usize);
        impl std::fmt::Write for Length {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                self.0 += text.len();
                Ok(())
            }
        }
        let mut nodes = 0;
        expr::visit(expression, &mut |_| nodes += 1);
        let mut length = Length(0);
        std::fmt::write(&mut length, format_args!("{expression}"))
            .expect("counting text cannot fail");
        let copied = self.copied.get() + nodes * NODE_WEIGHT + length.0;
        if copied > MAX_COPIED_WEIGHT {
            return Err(QueryError::compile(
                category::RESOURCE_LIMIT,
                detail::QUERY_TOO_LARGE,
                "the SQL statement translates to too large a Cypher statement",
            )
            .at(expression.span));
        }
        self.copied.set(copied);
        Ok(expression.clone())
    }

    /// An expression's Cypher text, by which grouping compares expressions:
    /// printing is canonical and injective. Comparing each subexpression of a
    /// deep expression prints its innermost parts once per level, so all of
    /// this printing shares one budget.
    fn text(&self, expression: &Expr) -> Result<String, QueryError> {
        let text = expression.to_string();
        let printed = self.printed.get() + text.len();
        if printed > MAX_PRINTED_BYTES {
            return Err(QueryError::compile(
                category::RESOURCE_LIMIT,
                detail::QUERY_TOO_LARGE,
                "the SQL statement's grouped expressions are too large to compare",
            )
            .at(expression.span));
        }
        self.printed.set(printed);
        Ok(text)
    }

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

fn variable(name: &str, span: Span) -> Result<Expr, QueryError> {
    Expr::new(ExprKind::Variable(name.to_owned()), span)
}

fn literal(value: r::Value, span: Span) -> Result<Expr, QueryError> {
    Expr::new(ExprKind::Literal(value), span)
}

fn call(name: &str, arguments: Vec<Expr>, span: Span) -> Result<Expr, QueryError> {
    Expr::new(
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
    Expr::new(ExprKind::Property(Box::new(value), key.to_owned()), span)
}

fn binary(operator: r::Binary, left: Expr, right: Expr, span: Span) -> Result<Expr, QueryError> {
    Expr::new(
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
