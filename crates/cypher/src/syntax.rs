//! Source-shaped syntax. Names stay strings until semantic resolution.
use helix_planner::relational::{self as r, Span};

#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    pub clauses: Vec<Clause>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    Match {
        patterns: Vec<Pattern>,
        optional: bool,
        predicate: Option<Expr>,
    },
    Project {
        returning: bool,
        items: Vec<Item>,
        distinct: bool,
        ordering: Vec<(Expr, bool)>,
        skip: Option<Expr>,
        limit: Option<Expr>,
        predicate: Option<Expr>,
    },
    Unwind {
        expression: Expr,
        name: String,
    },
    Create(Vec<Pattern>),
    Set(Vec<Assignment>),
    Remove(Vec<Expr>),
    Delete {
        expressions: Vec<Expr>,
        detach: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Assignment {
    Property(Expr, Expr),
    Replace(String, Expr),
    Extend(String, Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Wildcard,
    Expression {
        expression: Expr,
        alias: Option<String>,
        text: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub name: Option<String>,
    pub nodes: Vec<Node>,
    pub relationships: Vec<Relationship>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub has_properties: bool,
    pub name: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Relationship {
    pub name: Option<String>,
    pub types: Vec<String>,
    pub direction: r::Direction,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Expr {
    pub(crate) kind: ExprKind,
    pub span: Span,
    depth: usize,
}

impl Expr {
    /// Inspect the syntax without invalidating its checked nesting depth.
    pub fn kind(&self) -> &ExprKind {
        &self.kind
    }
    pub(crate) fn new(kind: ExprKind, span: Span) -> r::Result<Self> {
        let depth = 1 + match &kind {
            ExprKind::Literal(_)
            | ExprKind::Variable(_)
            | ExprKind::Parameter(_)
            | ExprKind::PatternPredicate => 0,
            ExprKind::Property(x, _) | ExprKind::Unary(_, x) | ExprKind::HasLabel(x, _) => x.depth,
            ExprKind::Index(a, b) | ExprKind::Binary(_, a, b) => a.depth.max(b.depth),
            ExprKind::Slice { value, start, end } => start
                .iter()
                .chain(end.iter())
                .map(|x| x.depth)
                .fold(value.depth, usize::max),
            ExprKind::Call { arguments, .. } | ExprKind::List(arguments) => {
                arguments.iter().map(|x| x.depth).max().unwrap_or(0)
            }
            ExprKind::Map(values) => values.iter().map(|(_, x)| x.depth).max().unwrap_or(0),
            ExprKind::Case {
                operand,
                branches,
                otherwise,
            } => operand
                .iter()
                .chain(otherwise.iter())
                .map(|x| x.depth)
                .chain(branches.iter().flat_map(|(a, b)| [a.depth, b.depth]))
                .max()
                .unwrap_or(0),
        };
        if depth > r::MAX_EXPRESSION_DEPTH {
            return Err(r::QueryError::compile(
                "ResourceLimit",
                "ExpressionDepth",
                "expression tree exceeds 48 levels",
            )
            .at(span));
        }
        Ok(Self { kind, span, depth })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExprKind {
    Literal(r::Value),
    Variable(String),
    Parameter(String),
    /// Recognized graph-pattern syntax. Binding rejects scalar use and reports
    /// the deferred capability in boolean contexts; it never enters the planner.
    PatternPredicate,
    Property(Box<Expr>, String),
    Index(Box<Expr>, Box<Expr>),
    Slice {
        value: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },
    Unary(r::Unary, Box<Expr>),
    Binary(r::Binary, Box<Expr>, Box<Expr>),
    Call {
        name: String,
        arguments: Vec<Expr>,
        distinct: bool,
        star: bool,
    },
    List(Vec<Expr>),
    Map(Vec<(String, Expr)>),
    Case {
        operand: Option<Box<Expr>>,
        branches: Vec<(Expr, Expr)>,
        otherwise: Option<Box<Expr>>,
    },
    HasLabel(Box<Expr>, String),
}
