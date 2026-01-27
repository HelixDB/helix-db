use crate::helixc::parser::Rule;
use pest::{Position, iterators::Pair};

#[derive(Debug, Clone)]
pub struct Loc {
    pub filepath: Option<String>,
    pub start: Span,
    pub end: Span,
    pub span: String,
}

#[derive(Debug, Clone, Copy)]
pub struct Span {
    pub line: usize,
    pub column: usize,
    pub byte_offset: usize,
}

impl Span {
    pub fn new(line: usize, column: usize) -> Self {
        Self {
            line,
            column: column + 1,
            byte_offset: 0,
        }
    }

    pub fn from_pos(pos: &Position) -> Self {
        let (line, column) = pos.line_col();
        Self {
            line,
            column: column + 1,
            byte_offset: pos.pos(),
        }
    }
}

impl Loc {
    pub fn new(filepath: Option<String>, start: Span, end: Span, span: String) -> Self {
        Self {
            filepath,
            start,
            end,
            span,
        }
    }

    pub fn empty() -> Self {
        Self::new(None, Span::new(1, 1), Span::new(1, 1), "".to_string())
    }

    /// Returns the byte range for use with ariadne.
    pub fn byte_range(&self) -> std::ops::Range<usize> {
        self.start.byte_offset..self.end.byte_offset
    }
}

pub trait HasLoc {
    fn loc(&self) -> Loc;

    fn loc_with_filepath(&self, filepath: String) -> Loc {
        Loc::new(
            Some(filepath),
            self.loc().start,
            self.loc().end,
            self.loc().span,
        )
    }
}
impl<'a> HasLoc for Pair<'a, Rule> {
    fn loc(&self) -> Loc {
        Loc::new(
            None,
            Span::from_pos(&self.as_span().start_pos()),
            Span::from_pos(&self.as_span().end_pos()),
            self.as_span().as_str().to_string(),
        )
    }
}
