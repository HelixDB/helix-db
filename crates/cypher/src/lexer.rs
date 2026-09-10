use helix_planner::relational::{QueryError, Result, Span};

/// Unicode alternatives in the pinned grammar are valid only in patterns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternPunctuation {
    LeftArrow,
    RightArrow,
    Dash,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    Word(String),
    Escaped(String),
    String(String),
    Number(String),
    Parameter(String),
    Symbol(&'static str),
    Pattern(PatternPunctuation),
    End,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: Kind,
    pub span: Span,
}

pub fn lex(source: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < source.len() {
        let c = source[i..].chars().next().expect("character boundary");
        if c.is_whitespace() {
            i += c.len_utf8();
            continue;
        }
        if source[i..].starts_with("//") {
            i += source[i..].find('\n').unwrap_or(source.len() - i);
            continue;
        }
        if source[i..].starts_with("/*") {
            let Some(end) = source[i + 2..].find("*/") else {
                return Err(error(i, source.len(), "unterminated comment"));
            };
            i += 2 + end + 2;
            continue;
        }
        let start = i;
        let quoted_parameter = c == '$' && source[i + 1..].starts_with('`');
        let kind = if c == '\'' || c == '"' || c == '`' || quoted_parameter {
            let quote = if quoted_parameter { '`' } else { c };
            if quoted_parameter {
                i += 1;
            }
            i += quote.len_utf8();
            let mut value = String::new();
            let mut closed = false;
            while i < source.len() {
                let ch = source[i..].chars().next().expect("character boundary");
                i += ch.len_utf8();
                if ch == quote {
                    if quote == '`' && source[i..].starts_with('`') {
                        value.push('`');
                        i += 1;
                        continue;
                    }
                    closed = true;
                    break;
                }
                if ch == '\\' && quote != '`' {
                    let Some(escape) = source[i..].chars().next() else {
                        break;
                    };
                    i += escape.len_utf8();
                    match escape {
                        'n' => value.push('\n'),
                        'r' => value.push('\r'),
                        't' => value.push('\t'),
                        'b' => value.push('\u{8}'),
                        'f' => value.push('\u{c}'),
                        '\\' | '\'' | '"' => value.push(escape),
                        'u' | 'U' => {
                            let len = if escape == 'u' { 4 } else { 8 };
                            let Some(hex) = source.get(i..i + len) else {
                                return Err(QueryError::compile(
                                    "SyntaxError",
                                    "InvalidUnicodeLiteral",
                                    "incomplete Unicode escape",
                                )
                                .at(Span { start, end: i }));
                            };
                            let scalar = u32::from_str_radix(hex, 16)
                                .ok()
                                .and_then(char::from_u32)
                                .ok_or_else(|| {
                                    QueryError::compile(
                                        "SyntaxError",
                                        "InvalidUnicodeLiteral",
                                        "invalid Unicode scalar",
                                    )
                                    .at(Span {
                                        start,
                                        end: i + len,
                                    })
                                })?;
                            value.push(scalar);
                            i += len;
                        }
                        _ => return Err(error(start, i, "invalid string escape")),
                    }
                } else {
                    value.push(ch);
                }
            }
            if !closed {
                return Err(error(start, i, "unterminated quoted value"));
            }
            if quoted_parameter {
                if value.is_empty() {
                    return Err(error(start, i, "expected parameter name"));
                }
                Kind::Parameter(value)
            } else if quote == '`' {
                Kind::Escaped(value)
            } else {
                Kind::String(value)
            }
        } else if c == '$' {
            i += 1;
            let beginning = i;
            while let Some(ch) = source[i..].chars().next() {
                if ch.is_alphanumeric() || ch == '_' {
                    i += ch.len_utf8();
                } else {
                    break;
                }
            }
            if i == beginning {
                return Err(error(start, i, "expected parameter name"));
            }
            Kind::Parameter(source[beginning..i].to_owned())
        } else if c.is_ascii_digit()
            || c == '.' && source[i + 1..].starts_with(|ch: char| ch.is_ascii_digit())
        {
            i += 1;
            while i < source.len() {
                let b = source.as_bytes()[i];
                if b.is_ascii_alphanumeric()
                    || b == b'.' && !source[i..].starts_with("..")
                    || (b == b'+' || b == b'-') && matches!(source.as_bytes()[i - 1], b'e' | b'E')
                {
                    i += 1;
                } else {
                    break;
                }
            }
            Kind::Number(source[start..i].to_owned())
        } else if c.is_alphabetic() || c == '_' {
            i += c.len_utf8();
            while let Some(ch) = source[i..].chars().next() {
                if ch.is_alphanumeric() || ch == '_' {
                    i += ch.len_utf8();
                } else {
                    break;
                }
            }
            Kind::Word(source[start..i].to_owned())
        } else {
            let pattern = match c {
                '\u{27e8}' | '\u{3008}' | '\u{fe64}' | '\u{ff1c}' => {
                    Some(PatternPunctuation::LeftArrow)
                }
                '\u{27e9}' | '\u{3009}' | '\u{fe65}' | '\u{ff1e}' => {
                    Some(PatternPunctuation::RightArrow)
                }
                '\u{00ad}' | '\u{2010}' | '\u{2011}' | '\u{2012}' | '\u{2013}' | '\u{2014}'
                | '\u{2015}' | '\u{2212}' | '\u{fe58}' | '\u{fe63}' | '\u{ff0d}' => {
                    Some(PatternPunctuation::Dash)
                }
                _ => None,
            };
            match pattern {
                Some(pattern) => {
                    i += c.len_utf8();
                    Kind::Pattern(pattern)
                }
                None => {
                    let symbol = [
                        "<=", ">=", "<>", "!=", "+=", "..", "(", ")", "[", "]", "{", "}", ",", ".",
                        ":", ";", "+", "-", "*", "/", "%", "^", "=", "<", ">", "|",
                    ]
                    .into_iter()
                    .find(|s| source[i..].starts_with(s))
                    .ok_or_else(|| {
                        QueryError::compile(
                            "SyntaxError",
                            if c.is_ascii() {
                                "UnexpectedSyntax"
                            } else {
                                "InvalidUnicodeCharacter"
                            },
                            "unexpected character",
                        )
                        .at(Span {
                            start,
                            end: start + c.len_utf8(),
                        })
                    })?;
                    i += symbol.len();
                    Kind::Symbol(symbol)
                }
            }
        };
        tokens.push(Token {
            kind,
            span: Span { start, end: i },
        });
        if tokens.len() > 200_000 {
            return Err(QueryError::compile(
                "ResourceLimit",
                "TooManyTokens",
                "query exceeds the token budget",
            ));
        }
    }
    tokens.push(Token {
        kind: Kind::End,
        span: Span { start: i, end: i },
    });
    Ok(tokens)
}

fn error(start: usize, end: usize, message: &str) -> QueryError {
    QueryError::compile("SyntaxError", "UnexpectedSyntax", message).at(Span { start, end })
}
