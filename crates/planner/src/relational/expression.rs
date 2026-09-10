use super::Value;
use std::collections::BTreeSet;

/// An index into a query's binding catalog. Runtime rows use slots, never names.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Slot(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Unary {
    Not,
    Negate,
    Positive,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Binary {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
    Xor,
    In,
    StartsWith,
    EndsWith,
    Contains,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Function {
    Id,
    Type,
    Labels,
    Properties,
    Keys,
    Size,
    Length,
    Nodes,
    Relationships,
    Head,
    Last,
    Coalesce,
    ToString,
    ToInteger,
    ToFloat,
    ToBoolean,
    Exists,
    Abs,
    Range,
    Reverse,
    Trim,
    Ltrim,
    Rtrim,
    ToLower,
    ToUpper,
    Substring,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Aggregate {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

/// Frontend-independent expression shape. The operation and literal domains
/// encode semantic differences (for example native two-valued comparisons and
/// Cypher null propagation), without retaining either frontend's syntax tree.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ScalarExpression<L, U, B, F> {
    Literal(L),
    Slot(Slot),
    Parameter(String),
    Property(Box<Self>, String),
    Index(Box<Self>, Box<Self>),
    Slice {
        value: Box<Self>,
        start: Option<Box<Self>>,
        end: Option<Box<Self>>,
    },
    Unary(U, Box<Self>),
    Binary(B, Box<Self>, Box<Self>),
    Function(F, Vec<Self>),
    Aggregate {
        function: Aggregate,
        argument: Option<Box<Self>>,
        distinct: bool,
    },
    List(Vec<Self>),
    Map(Vec<(String, Self)>),
    Case {
        branches: Vec<(Self, Self)>,
        otherwise: Box<Self>,
    },
    HasLabel(Slot, String),
}

/// Resolved expressions with Cypher's graph-value and scalar semantics.
pub type Expression = ScalarExpression<Value, Unary, Binary, Function>;

impl Expression {
    /// Slots requiring graph hydration. Identity, paths, and ordinary scalar
    /// operations need no property reads; dynamic property access is conservative.
    pub fn graph_requirements(&self, out: &mut std::collections::BTreeMap<Slot, PropertyDemand>) {
        self.visit(&mut |expression| {
            let (slots, demand) = match expression {
                Self::Property(value, key) => (
                    value.slots(),
                    PropertyDemand::Keys(BTreeSet::from([key.clone()])),
                ),
                Self::HasLabel(slot, _) => (
                    BTreeSet::from([*slot]),
                    PropertyDemand::Keys(BTreeSet::new()),
                ),
                Self::Index(value, index)
                    if !matches!(index.as_ref(), Self::Literal(Value::Integer(_))) =>
                {
                    (value.slots(), PropertyDemand::All)
                }
                Self::Function(Function::Labels | Function::Type, args) => (
                    args.iter().flat_map(Self::slots).collect(),
                    PropertyDemand::Keys(BTreeSet::new()),
                ),
                Self::Function(Function::Properties | Function::Keys, args) => (
                    args.iter().flat_map(Self::slots).collect(),
                    PropertyDemand::All,
                ),
                _ => return,
            };
            for slot in slots {
                out.entry(slot).or_default().merge(&demand);
            }
        });
    }

    /// Check shape before recursive visitors or evaluation cross the plan boundary.
    pub fn validate_shape(&self) -> super::Result<()> {
        let mut pending = vec![(self, 0_usize)];
        let mut count = 0;
        while let Some((expression, depth)) = pending.pop() {
            count += 1;
            if depth >= super::MAX_EXPRESSION_DEPTH || count > 200_000 {
                return Err(super::QueryError::compile(
                    "ResourceLimit",
                    "ExpressionDepth",
                    "expression exceeds validated structural limits",
                ));
            }
            let mut child = |e| pending.push((e, depth + 1));
            match expression {
                Self::Literal(value) => value.validate_shape()?,
                Self::Slot(_) | Self::Parameter(_) | Self::HasLabel(..) => {}
                Self::Property(x, _) | Self::Unary(_, x) => child(x.as_ref()),
                Self::Index(a, b) | Self::Binary(_, a, b) => {
                    child(a.as_ref());
                    child(b.as_ref());
                }
                Self::Slice { value, start, end } => {
                    child(value.as_ref());
                    for e in start.iter().chain(end.iter()) {
                        child(e.as_ref());
                    }
                }
                Self::List(xs) => xs.iter().for_each(child),
                Self::Map(xs) => xs.iter().for_each(|(_, e)| child(e)),
                Self::Case {
                    branches,
                    otherwise,
                } => {
                    child(otherwise.as_ref());
                    for (a, b) in branches {
                        child(a);
                        child(b);
                    }
                }
                Self::Function(function, args) => {
                    let valid = match function {
                        Function::Coalesce => !args.is_empty(),
                        Function::Range | Function::Substring => (2..=3).contains(&args.len()),
                        _ => args.len() == 1,
                    };
                    if !valid {
                        return Err(super::QueryError::compile(
                            "InternalPlannerError",
                            "FunctionArity",
                            "resolved function has incompatible argument count",
                        ));
                    }
                    args.iter().for_each(child);
                }
                Self::Aggregate {
                    function,
                    argument,
                    distinct,
                } => {
                    if argument.is_none() && (*function != Aggregate::Count || *distinct) {
                        return Err(super::QueryError::compile(
                            "InternalPlannerError",
                            "AggregateArity",
                            "only count(*) permits no argument",
                        ));
                    }
                    if let Some(argument) = argument {
                        child(argument.as_ref());
                    }
                }
            }
        }
        Ok(())
    }
}

impl<L, U, B, F> ScalarExpression<L, U, B, F> {
    pub fn visit(&self, f: &mut impl FnMut(&Self)) {
        f(self);
        match self {
            Self::Property(x, _) | Self::Unary(_, x) => x.visit(f),
            Self::Index(a, b) | Self::Binary(_, a, b) => {
                a.visit(f);
                b.visit(f);
            }
            Self::Slice { value, start, end } => {
                value.visit(f);
                for x in start.iter().chain(end.iter()) {
                    x.visit(f);
                }
            }
            Self::Function(_, xs) | Self::List(xs) => xs.iter().for_each(|x| x.visit(f)),
            Self::Aggregate { argument, .. } => {
                if let Some(x) = argument {
                    x.visit(f);
                }
            }
            Self::Map(xs) => xs.iter().for_each(|(_, x)| x.visit(f)),
            Self::Case {
                branches,
                otherwise,
            } => {
                for (a, b) in branches {
                    a.visit(f);
                    b.visit(f);
                }
                otherwise.visit(f);
            }
            Self::Literal(_) | Self::Slot(_) | Self::Parameter(_) | Self::HasLabel(_, _) => {}
        }
    }

    pub fn slots(&self) -> BTreeSet<Slot> {
        let mut slots = BTreeSet::new();
        self.visit(&mut |x| {
            if let Self::Slot(s) | Self::HasLabel(s, _) = x {
                slots.insert(*s);
            }
        });
        slots
    }

    pub fn has_aggregate(&self) -> bool {
        let mut found = false;
        self.visit(&mut |x| {
            found |= matches!(x, Self::Aggregate { .. });
        });
        found
    }
}

impl<L: Clone, U: Copy, B: Copy, F: Clone> ScalarExpression<L, U, B, F> {
    /// Rewrite resolved expressions without reconstructing frontend syntax.
    /// Returning a replacement stops traversal into that subtree.
    pub fn rewrite(
        &self,
        replace: &mut impl FnMut(&Self) -> super::Result<Option<Self>>,
    ) -> super::Result<Self> {
        if let Some(value) = replace(self)? {
            return Ok(value);
        }
        let mut rewrite = |value: &Self| value.rewrite(replace);
        Ok(match self {
            Self::Literal(_) | Self::Slot(_) | Self::Parameter(_) | Self::HasLabel(..) => {
                self.clone()
            }
            Self::Property(value, key) => Self::Property(Box::new(rewrite(value)?), key.clone()),
            Self::Index(a, b) => Self::Index(Box::new(rewrite(a)?), Box::new(rewrite(b)?)),
            Self::Slice { value, start, end } => Self::Slice {
                value: Box::new(rewrite(value)?),
                start: start
                    .as_ref()
                    .map(|e| rewrite(e).map(Box::new))
                    .transpose()?,
                end: end.as_ref().map(|e| rewrite(e).map(Box::new)).transpose()?,
            },
            Self::Unary(op, x) => Self::Unary(*op, Box::new(rewrite(x)?)),
            Self::Binary(op, a, b) => {
                Self::Binary(*op, Box::new(rewrite(a)?), Box::new(rewrite(b)?))
            }
            Self::Function(f, args) => Self::Function(
                f.clone(),
                args.iter().map(rewrite).collect::<super::Result<_>>()?,
            ),
            Self::List(xs) => Self::List(xs.iter().map(rewrite).collect::<super::Result<_>>()?),
            Self::Map(xs) => Self::Map(
                xs.iter()
                    .map(|(k, v)| Ok((k.clone(), rewrite(v)?)))
                    .collect::<super::Result<_>>()?,
            ),
            Self::Case {
                branches,
                otherwise,
            } => {
                let branches = branches
                    .iter()
                    .map(|(a, b)| Ok((rewrite(a)?, rewrite(b)?)))
                    .collect::<super::Result<_>>()?;
                Self::Case {
                    branches,
                    otherwise: Box::new(rewrite(otherwise)?),
                }
            }
            Self::Aggregate {
                function,
                argument,
                distinct,
            } => Self::Aggregate {
                function: *function,
                argument: argument
                    .as_ref()
                    .map(|e| rewrite(e).map(Box::new))
                    .transpose()?,
                distinct: *distinct,
            },
        })
    }
}

/// Property demand permits late hydration without conflating a missing key
/// with a key that was never requested. Metadata is always decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyDemand {
    Keys(BTreeSet<String>),
    All,
}

impl Default for PropertyDemand {
    fn default() -> Self {
        Self::Keys(BTreeSet::new())
    }
}

impl PropertyDemand {
    pub fn merge(&mut self, other: &Self) {
        match (&mut *self, other) {
            (Self::All, _) => {}
            (_, Self::All) => *self = Self::All,
            (Self::Keys(keys), Self::Keys(other)) => keys.extend(other.iter().cloned()),
        }
    }
    pub fn contains(&self, key: &str) -> bool {
        match self {
            Self::All => true,
            Self::Keys(keys) => keys.contains(key),
        }
    }
}
