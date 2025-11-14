use crate::helixc::parser::Rule;
use std::fmt::{Display, Formatter};

#[derive(Clone)]
pub enum ParserError {
    ParseError(String),
    LexError(String),
    ParamDoesNotMatchSchema(String),
}

impl Display for ParserError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::ParseError(e) => write!(f, "Parse error: {e}"),
            Self::LexError(e) => write!(f, "Lex error: {e}"),
            Self::ParamDoesNotMatchSchema(p) => {
                write!(f, "Parameter with name: {p} does not exist in the schema")
            }
        }
    }
}

impl From<pest::error::Error<Rule>> for ParserError {
    fn from(e: pest::error::Error<Rule>) -> Self {
        Self::ParseError(e.to_string())
    }
}

impl From<String> for ParserError {
    fn from(e: String) -> Self {
        Self::LexError(e)
    }
}

impl From<&'static str> for ParserError {
    fn from(e: &'static str) -> Self {
        Self::LexError(e.to_string())
    }
}

impl std::fmt::Debug for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ParseError(e) => write!(f, "Parse error: {e}"),
            Self::LexError(e) => write!(f, "Lex error: {e}"),
            Self::ParamDoesNotMatchSchema(p) => {
                write!(f, "Parameter with name: {p} does not exist in the schema")
            }
        }
    }
}