//! Canonical Cypher text for [`crate::syntax`] trees.
//!
//! Printing is the inverse of parsing up to spans, the source text of
//! projection items, and redundant parentheses: [`crate::parse`] of printed
//! text yields a statement that prints identically. Names that are not plain
//! identifiers, or that collide with a keyword, are quoted with backticks, and
//! subexpressions are parenthesized only where the parser's precedence would
//! otherwise regroup them.
//!
//! ```
//! let statement = helix_cypher::parse("match (p:Person) where (p.age > 1) return p.name as `the name`")?;
//! assert_eq!(
//!     statement.to_string(),
//!     "MATCH (p:Person) WHERE p.age > 1 RETURN p.name AS `the name`",
//! );
//! # Ok::<(), helix_cypher::QueryError>(())
//! ```
use crate::syntax::{self, ExprKind};
use helix_planner::relational as r;
use std::fmt::{self, Display, Formatter, Write};

impl Display for syntax::Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.clauses
            .iter()
            .enumerate()
            .try_for_each(|(index, clause)| {
                if index > 0 {
                    f.write_char(' ')?;
                }
                write!(f, "{clause}")
            })
    }
}

impl Display for syntax::Clause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            syntax::Clause::Match {
                patterns,
                optional,
                predicate,
            } => {
                if *optional {
                    f.write_str("OPTIONAL ")?;
                }
                f.write_str("MATCH ")?;
                list(f, patterns)?;
                predicate
                    .iter()
                    .try_for_each(|predicate| write!(f, " WHERE {predicate}"))?;
                Ok(())
            }
            syntax::Clause::Project {
                returning,
                items,
                distinct,
                ordering,
                skip,
                limit,
                predicate,
            } => {
                f.write_str(if *returning { "RETURN " } else { "WITH " })?;
                if *distinct {
                    f.write_str("DISTINCT ")?;
                }
                list(f, items)?;
                if !ordering.is_empty() {
                    f.write_str(" ORDER BY ")?;
                    ordering.iter().enumerate().try_for_each(
                        |(index, (expression, descending))| {
                            if index > 0 {
                                f.write_str(", ")?;
                            }
                            write!(f, "{expression}")?;
                            if *descending {
                                f.write_str(" DESC")?;
                            }
                            Ok(())
                        },
                    )?;
                }
                skip.iter().try_for_each(|skip| write!(f, " SKIP {skip}"))?;
                limit
                    .iter()
                    .try_for_each(|limit| write!(f, " LIMIT {limit}"))?;
                predicate
                    .iter()
                    .try_for_each(|predicate| write!(f, " WHERE {predicate}"))?;
                Ok(())
            }
            syntax::Clause::Unwind { expression, name } => {
                write!(f, "UNWIND {expression} AS {}", Name(name))
            }
            syntax::Clause::Create(patterns) => {
                f.write_str("CREATE ")?;
                list(f, patterns)
            }
            syntax::Clause::Set(assignments) => {
                f.write_str("SET ")?;
                list(f, assignments)
            }
            syntax::Clause::Remove(expressions) => {
                f.write_str("REMOVE ")?;
                list(f, expressions)
            }
            syntax::Clause::Delete {
                expressions,
                detach,
            } => {
                if *detach {
                    f.write_str("DETACH ")?;
                }
                f.write_str("DELETE ")?;
                list(f, expressions)
            }
        }
    }
}

impl Display for syntax::Assignment {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            syntax::Assignment::Property(target, value) => write!(f, "{target} = {value}"),
            syntax::Assignment::Replace(name, value) => write!(f, "{} = {value}", Name(name)),
            syntax::Assignment::Extend(name, value) => write!(f, "{} += {value}", Name(name)),
        }
    }
}

impl Display for syntax::Item {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            syntax::Item::Wildcard => f.write_char('*'),
            syntax::Item::Named { expression, .. } => write!(f, "{expression}"),
            syntax::Item::Expression {
                expression, alias, ..
            } => {
                write!(f, "{expression}")?;
                alias
                    .iter()
                    .try_for_each(|alias| write!(f, " AS {}", Name(alias)))
            }
        }
    }
}

impl Display for syntax::Pattern {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.name
            .iter()
            .try_for_each(|name| write!(f, "{} = ", Name(name)))?;
        let mut nodes = self.nodes.iter();
        nodes
            .next()
            .iter()
            .try_for_each(|first| write!(f, "{first}"))?;
        self.relationships
            .iter()
            .zip(nodes)
            .try_for_each(|(relationship, node)| write!(f, "{relationship}{node}"))
    }
}

impl Display for syntax::Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_char('(')?;
        self.name
            .iter()
            .try_for_each(|name| write!(f, "{}", Name(name)))?;
        self.labels
            .iter()
            .try_for_each(|label| write!(f, ":{}", Name(label)))?;
        if self.has_properties {
            if self.name.is_some() || !self.labels.is_empty() {
                f.write_char(' ')?;
            }
            properties(f, &self.properties)?;
        }
        f.write_char(')')
    }
}

impl Display for syntax::Relationship {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self.direction {
            r::Direction::Incoming => "<-",
            r::Direction::Outgoing | r::Direction::Undirected => "-",
        })?;
        if self.name.is_some() || !self.types.is_empty() || !self.properties.is_empty() {
            f.write_char('[')?;
            self.name
                .iter()
                .try_for_each(|name| write!(f, "{}", Name(name)))?;
            self.types
                .iter()
                .enumerate()
                .try_for_each(|(index, kind)| {
                    f.write_str(if index == 0 { ":" } else { "|" })?;
                    write!(f, "{}", Name(kind))
                })?;
            if !self.properties.is_empty() {
                if self.name.is_some() || !self.types.is_empty() {
                    f.write_char(' ')?;
                }
                properties(f, &self.properties)?;
            }
            f.write_char(']')?;
        }
        f.write_str(match self.direction {
            r::Direction::Outgoing => "->",
            r::Direction::Incoming | r::Direction::Undirected => "-",
        })
    }
}

impl Display for syntax::Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use r::Unary as U;
        match self.kind() {
            ExprKind::Literal(value) => write!(f, "{}", Literal(value)),
            ExprKind::Variable(name) => write!(f, "{}", Name(name)),
            ExprKind::Parameter(name) => {
                if !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    write!(f, "${name}")
                } else {
                    write!(f, "${}", Quoted(name))
                }
            }
            // Only the named variables survive parsing; an anonymous node pads
            // the pattern so that it still reads as a relationship pattern.
            ExprKind::PatternPredicate(names) => {
                let padding = 2_usize.saturating_sub(names.len());
                names
                    .iter()
                    .map(|name| Some(Name(name)))
                    .chain(std::iter::repeat_with(|| None).take(padding))
                    .enumerate()
                    .try_for_each(|(index, name)| {
                        if index > 0 {
                            f.write_str("--")?;
                        }
                        match name {
                            Some(name) => write!(f, "({name})"),
                            None => f.write_str("()"),
                        }
                    })
            }
            ExprKind::Property(value, name) => {
                // `1.x` would lex as one malformed number.
                let number = matches!(
                    value.kind(),
                    ExprKind::Literal(r::Value::Integer(_) | r::Value::Float(_))
                );
                operand(f, value, number || precedence(value.kind()) < POSTFIX)?;
                write!(f, ".{}", Name(name))
            }
            ExprKind::Index(value, index) => {
                operand(f, value, precedence(value.kind()) < POSTFIX)?;
                write!(f, "[{index}]")
            }
            ExprKind::Slice { value, start, end } => {
                operand(f, value, precedence(value.kind()) < POSTFIX)?;
                f.write_char('[')?;
                start.iter().try_for_each(|start| write!(f, "{start}"))?;
                f.write_str("..")?;
                end.iter().try_for_each(|end| write!(f, "{end}"))?;
                f.write_char(']')
            }
            ExprKind::Unary(U::Not, value) => {
                f.write_str("NOT ")?;
                operand(f, value, precedence(value.kind()) < COMPARISON)
            }
            ExprKind::Unary(operator @ (U::Negate | U::Positive), value) => {
                f.write_char(if *operator == U::Negate { '-' } else { '+' })?;
                // A sign directly before a number, or before another sign,
                // would lex as part of that literal.
                let text = value.to_string();
                if precedence(value.kind()) < SIGN
                    || text
                        .starts_with(|c: char| c.is_ascii_digit() || matches!(c, '-' | '+' | '.'))
                {
                    write!(f, "({text})")
                } else {
                    f.write_str(&text)
                }
            }
            ExprKind::Unary(operator @ (U::IsNull | U::IsNotNull), value) => {
                operand(f, value, precedence(value.kind()) < MEMBERSHIP)?;
                f.write_str(if *operator == U::IsNull {
                    " IS NULL"
                } else {
                    " IS NOT NULL"
                })
            }
            ExprKind::Binary(operator, left, right) => {
                let own = binary_precedence(*operator);
                let comparison = own == COMPARISON;
                operand(
                    f,
                    left,
                    precedence(left.kind()) < own
                        || is_not(left)
                        || comparison && precedence(left.kind()) == COMPARISON,
                )?;
                write!(f, " {} ", binary_symbol(*operator))?;
                operand(f, right, precedence(right.kind()) <= own || is_not(right))
            }
            ExprKind::Connective(connective, operands) => {
                let own = connective_precedence(*connective);
                let word = match connective {
                    r::Connective::And => " AND ",
                    r::Connective::Or => " OR ",
                    r::Connective::Xor => " XOR ",
                };
                operands.iter().enumerate().try_for_each(|(index, value)| {
                    if index > 0 {
                        f.write_str(word)?;
                    }
                    operand(f, value, precedence(value.kind()) <= own)
                })
            }
            ExprKind::Call {
                name,
                arguments,
                distinct,
                star,
            } => {
                write!(f, "{}(", Name(name))?;
                if *distinct {
                    f.write_str("DISTINCT ")?;
                }
                if *star {
                    f.write_char('*')?;
                }
                list(f, arguments)?;
                f.write_char(')')
            }
            ExprKind::List(values) => {
                f.write_char('[')?;
                // `[name IN` opens a list comprehension, so a first value
                // that prints that way is parenthesized.
                let [first, rest @ ..] = values.as_slice() else {
                    return f.write_char(']');
                };
                let text = first.to_string();
                let mut words = text.split_whitespace();
                let comprehension = words.next().is_some_and(|word| {
                    word.starts_with(|c: char| c.is_alphabetic() || c == '_')
                        && word.chars().all(|c| c.is_alphanumeric() || c == '_')
                }) && words
                    .next()
                    .is_some_and(|word| word.eq_ignore_ascii_case("IN"));
                if comprehension {
                    write!(f, "({text})")?;
                } else {
                    f.write_str(&text)?;
                }
                rest.iter().try_for_each(|value| write!(f, ", {value}"))?;
                f.write_char(']')
            }
            ExprKind::Map(entries) => properties(f, entries),
            ExprKind::Case {
                operand: subject,
                branches,
                otherwise,
            } => {
                f.write_str("CASE")?;
                subject
                    .iter()
                    .try_for_each(|subject| write!(f, " {subject}"))?;
                branches
                    .iter()
                    .try_for_each(|(when, then)| write!(f, " WHEN {when} THEN {then}"))?;
                otherwise
                    .iter()
                    .try_for_each(|otherwise| write!(f, " ELSE {otherwise}"))?;
                f.write_str(" END")
            }
            ExprKind::HasLabel(value, label) => {
                operand(f, value, precedence(value.kind()) < POSTFIX)?;
                write!(f, ":{}", Name(label))
            }
        }
    }
}

// Binding strength, mirroring the parser: a connective's operands bind
// tighter than it, `NOT` takes a comparison, and postfix access binds tightest.
const COMPARISON: u8 = 4;
const MEMBERSHIP: u8 = 5;
const SIGN: u8 = 9;
const POSTFIX: u8 = 10;
const ATOM: u8 = 11;

fn precedence(kind: &ExprKind) -> u8 {
    match kind {
        ExprKind::Connective(connective, _) => connective_precedence(*connective),
        ExprKind::Unary(r::Unary::Not, _) => COMPARISON,
        ExprKind::Unary(r::Unary::IsNull | r::Unary::IsNotNull, _) => MEMBERSHIP,
        ExprKind::Unary(r::Unary::Negate | r::Unary::Positive, _) | ExprKind::HasLabel(..) => SIGN,
        ExprKind::Binary(operator, ..) => binary_precedence(*operator),
        ExprKind::Property(..) | ExprKind::Index(..) | ExprKind::Slice { .. } => POSTFIX,
        // A negative number lexes with its sign, so it groups like a negation.
        ExprKind::Literal(r::Value::Integer(value)) if *value < 0 => SIGN,
        ExprKind::Literal(r::Value::Float(value)) if value.is_sign_negative() => SIGN,
        ExprKind::Literal(_)
        | ExprKind::Variable(_)
        | ExprKind::Parameter(_)
        | ExprKind::PatternPredicate(_)
        | ExprKind::Call { .. }
        | ExprKind::List(_)
        | ExprKind::Map(_)
        | ExprKind::Case { .. } => ATOM,
    }
}

fn connective_precedence(connective: r::Connective) -> u8 {
    match connective {
        r::Connective::Or => 1,
        r::Connective::Xor => 2,
        r::Connective::And => 3,
    }
}

fn binary_precedence(operator: r::Binary) -> u8 {
    use r::Binary as B;
    match operator {
        B::Equal | B::NotEqual | B::Less | B::LessEqual | B::Greater | B::GreaterEqual => {
            COMPARISON
        }
        B::In | B::StartsWith | B::EndsWith | B::Contains => MEMBERSHIP,
        B::Add | B::Subtract => 6,
        B::Multiply | B::Divide | B::Modulo => 7,
        B::Power => 8,
    }
}

fn binary_symbol(operator: r::Binary) -> &'static str {
    use r::Binary as B;
    match operator {
        B::Add => "+",
        B::Subtract => "-",
        B::Multiply => "*",
        B::Divide => "/",
        B::Modulo => "%",
        B::Power => "^",
        B::Equal => "=",
        B::NotEqual => "<>",
        B::Less => "<",
        B::LessEqual => "<=",
        B::Greater => ">",
        B::GreaterEqual => ">=",
        B::In => "IN",
        B::StartsWith => "STARTS WITH",
        B::EndsWith => "ENDS WITH",
        B::Contains => "CONTAINS",
    }
}

/// `NOT` reads a whole comparison as its operand, so it is parenthesized
/// wherever it is the operand of another operator.
fn is_not(value: &syntax::Expr) -> bool {
    matches!(value.kind(), ExprKind::Unary(r::Unary::Not, _))
}

fn operand(f: &mut Formatter<'_>, value: &syntax::Expr, parenthesize: bool) -> fmt::Result {
    if parenthesize {
        write!(f, "({value})")
    } else {
        write!(f, "{value}")
    }
}

fn list<T: Display>(f: &mut Formatter<'_>, values: &[T]) -> fmt::Result {
    values.iter().enumerate().try_for_each(|(index, value)| {
        if index > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{value}")
    })
}

fn properties(f: &mut Formatter<'_>, entries: &[(String, syntax::Expr)]) -> fmt::Result {
    f.write_char('{')?;
    entries
        .iter()
        .enumerate()
        .try_for_each(|(index, (key, value))| {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}: {value}", Name(key))
        })?;
    f.write_char('}')
}

/// A symbolic name, label, type or key: bare when it lexes as one ordinary
/// word, otherwise quoted.
struct Name<'a>(&'a str);

impl Display for Name<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut characters = self.0.chars();
        let plain = characters
            .next()
            .is_some_and(|c| c.is_alphabetic() || c == '_')
            && characters.all(|c| c.is_alphanumeric() || c == '_')
            && !RESERVED
                .iter()
                .any(|word| self.0.eq_ignore_ascii_case(word));
        if plain {
            f.write_str(self.0)
        } else {
            write!(f, "{}", Quoted(self.0))
        }
    }
}

/// A backtick-quoted name; a backtick inside it is doubled.
struct Quoted<'a>(&'a str);

impl Display for Quoted<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_char('`')?;
        self.0.chars().try_for_each(|c| {
            if c == '`' {
                f.write_str("``")
            } else {
                f.write_char(c)
            }
        })?;
        f.write_char('`')
    }
}

/// Words that the parser reads as keywords or literals in some position.
/// Quoting a name that matches one is always safe.
const RESERVED: &[&str] = &[
    "ALL",
    "AND",
    "AS",
    "ASC",
    "ASCENDING",
    "BY",
    "CALL",
    "CASE",
    "CONTAINS",
    "CREATE",
    "DELETE",
    "DESC",
    "DESCENDING",
    "DETACH",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "ENDS",
    "FALSE",
    "FOREACH",
    "IN",
    "IS",
    "LIMIT",
    "LOAD",
    "MATCH",
    "MERGE",
    "NOT",
    "NULL",
    "OPTIONAL",
    "OR",
    "ORDER",
    "REMOVE",
    "RETURN",
    "SET",
    "SKIP",
    "STARTS",
    "THEN",
    "TRUE",
    "UNION",
    "UNWIND",
    "WHEN",
    "WHERE",
    "WITH",
    "XOR",
];

struct Literal<'a>(&'a r::Value);

impl Display for Literal<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            r::Value::Null => f.write_str("NULL"),
            r::Value::Boolean(value) => f.write_str(if *value { "TRUE" } else { "FALSE" }),
            r::Value::Integer(value) => write!(f, "{value}"),
            // Debug formatting keeps a decimal point or exponent, so the text
            // lexes back as a float with the same value.
            r::Value::Float(value) if value.is_finite() => write!(f, "{value:?}"),
            // Literals are finite; these spell the same value as an expression.
            r::Value::Float(value) if value.is_nan() => f.write_str("(0.0 / 0.0)"),
            r::Value::Float(value) if *value > 0.0 => f.write_str("(1.0 / 0.0)"),
            r::Value::Float(_) => f.write_str("(-1.0 / 0.0)"),
            r::Value::String(value) => {
                f.write_char('\'')?;
                value.chars().try_for_each(|c| match c {
                    '\'' => f.write_str("\\'"),
                    '\\' => f.write_str("\\\\"),
                    '\n' => f.write_str("\\n"),
                    '\r' => f.write_str("\\r"),
                    '\t' => f.write_str("\\t"),
                    c if c.is_control() => write!(f, "\\u{:04X}", u32::from(c)),
                    c => f.write_char(c),
                })?;
                f.write_char('\'')
            }
            r::Value::List(values) => {
                f.write_char('[')?;
                values.iter().enumerate().try_for_each(|(index, value)| {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{}", Literal(value))
                })?;
                f.write_char(']')
            }
            r::Value::Map(entries) => {
                f.write_char('{')?;
                entries
                    .iter()
                    .enumerate()
                    .try_for_each(|(index, (key, value))| {
                        if index > 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{}: {}", Name(key), Literal(value))
                    })?;
                f.write_char('}')
            }
            // Graph values never appear in statement text.
            r::Value::Entity(_) | r::Value::Path(_) => f.write_str("NULL"),
        }
    }
}

#[cfg(test)]
mod tests;
