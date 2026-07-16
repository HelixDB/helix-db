use serde::{Deserialize, Serialize};

use crate::value::{PropertyInput, PropertyValue};
/// Computed expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expr {
    /// Property reference.
    Property(String),
    /// Current element ID.
    Id,
    /// Current UTC timestamp in milliseconds.
    Timestamp,
    /// Current typed datetime.
    DateTimeNow,
    /// Literal value.
    Constant(PropertyValue),
    /// Runtime parameter reference.
    Param(String),
    /// Addition.
    Add { left: Box<Expr>, right: Box<Expr> },
    /// Subtraction.
    Sub { left: Box<Expr>, right: Box<Expr> },
    /// Multiplication.
    Mul { left: Box<Expr>, right: Box<Expr> },
    /// Division.
    Div { left: Box<Expr>, right: Box<Expr> },
    /// Modulo.
    Mod { left: Box<Expr>, right: Box<Expr> },
    /// Numeric negation.
    Neg { expr: Box<Expr> },
    /// Conditional expression.
    Case {
        /// Ordered predicate/expression branches.
        when_then: Vec<WhenThen>,
        /// Optional fallback expression.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        else_expr: Option<Box<Expr>>,
    },
}

/// One conditional expression branch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhenThen {
    /// Condition to test.
    pub when: Predicate,
    /// Expression returned when `when` matches.
    pub then: Expr,
}

impl Expr {
    /// Create a property reference expression.
    pub fn prop(name: impl Into<String>) -> Self {
        Self::Property(name.into())
    }

    /// Create a literal expression.
    pub fn val(value: impl Into<PropertyValue>) -> Self {
        Self::Constant(value.into())
    }

    /// Create an ID expression.
    pub fn id() -> Self {
        Self::Id
    }

    /// Create a timestamp expression.
    pub fn timestamp() -> Self {
        Self::Timestamp
    }

    /// Create a datetime expression.
    pub fn datetime() -> Self {
        Self::DateTimeNow
    }

    /// Create a parameter reference expression.
    pub fn param(name: impl Into<String>) -> Self {
        Self::Param(name.into())
    }

    /// Addition.
    pub fn add_expr(self, other: Expr) -> Self {
        Self::Add {
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// Subtraction.
    pub fn sub_expr(self, other: Expr) -> Self {
        Self::Sub {
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// Multiplication.
    pub fn mul_expr(self, other: Expr) -> Self {
        Self::Mul {
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// Division.
    pub fn div_expr(self, other: Expr) -> Self {
        Self::Div {
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// Modulo.
    pub fn modulo(self, other: Expr) -> Self {
        Self::Mod {
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// Negation.
    pub fn neg_expr(self) -> Self {
        Self::Neg {
            expr: Box::new(self),
        }
    }

    /// Backwards-compatible addition builder.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: Expr) -> Self {
        self.add_expr(other)
    }

    /// Backwards-compatible subtraction builder.
    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, other: Expr) -> Self {
        self.sub_expr(other)
    }

    /// Backwards-compatible multiplication builder.
    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, other: Expr) -> Self {
        self.mul_expr(other)
    }

    /// Backwards-compatible division builder.
    #[allow(clippy::should_implement_trait)]
    pub fn div(self, other: Expr) -> Self {
        self.div_expr(other)
    }

    /// Backwards-compatible negation builder.
    #[allow(clippy::should_implement_trait)]
    pub fn neg(self) -> Self {
        self.neg_expr()
    }

    /// Create a conditional expression.
    pub fn case(when_then: Vec<(Predicate, Expr)>, else_expr: Option<Expr>) -> Self {
        Self::Case {
            when_then: when_then
                .into_iter()
                .map(|(when, then)| WhenThen { when, then })
                .collect(),
            else_expr: else_expr.map(Box::new),
        }
    }
}
/// A non-negative stream bound.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamBound {
    /// Literal bound.
    Literal(usize),
    /// Runtime expression bound.
    Expr(Expr),
}

impl StreamBound {
    /// Create a literal bound.
    pub fn literal(value: usize) -> Self {
        Self::Literal(value)
    }

    /// Create an expression bound.
    pub fn expr(expr: Expr) -> Self {
        Self::Expr(expr)
    }
}

impl From<usize> for StreamBound {
    fn from(value: usize) -> Self {
        Self::Literal(value)
    }
}

impl From<u32> for StreamBound {
    fn from(value: u32) -> Self {
        Self::Literal(value as usize)
    }
}

impl From<u16> for StreamBound {
    fn from(value: u16) -> Self {
        Self::Literal(value as usize)
    }
}

impl From<u8> for StreamBound {
    fn from(value: u8) -> Self {
        Self::Literal(value as usize)
    }
}

impl From<i64> for StreamBound {
    fn from(value: i64) -> Self {
        if value >= 0 {
            Self::Literal(value as usize)
        } else {
            Self::Expr(Expr::val(value))
        }
    }
}

impl From<i32> for StreamBound {
    fn from(value: i32) -> Self {
        if value >= 0 {
            Self::Literal(value as usize)
        } else {
            Self::Expr(Expr::val(value))
        }
    }
}

impl From<Expr> for StreamBound {
    fn from(value: Expr) -> Self {
        Self::Expr(value)
    }
}

/// Comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    /// Equal.
    Eq,
    /// Not equal.
    Neq,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Gte,
    /// Less than.
    Lt,
    /// Less than or equal.
    Lte,
}

/// Predicate expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Predicate {
    /// Equality comparison.
    Eq { left: Expr, right: Expr },
    /// Inequality comparison.
    Neq { left: Expr, right: Expr },
    /// Greater-than comparison.
    Gt { left: Expr, right: Expr },
    /// Greater-than-or-equal comparison.
    Gte { left: Expr, right: Expr },
    /// Less-than comparison.
    Lt { left: Expr, right: Expr },
    /// Less-than-or-equal comparison.
    Lte { left: Expr, right: Expr },
    /// Inclusive range comparison.
    Between { value: Expr, min: Expr, max: Expr },
    /// Property exists.
    HasKey { property: String },
    /// Property is null or missing.
    IsNull { property: String },
    /// Property exists and is not null.
    IsNotNull { property: String },
    /// String starts with prefix.
    StartsWith { value: Expr, prefix: Expr },
    /// String ends with suffix.
    EndsWith { value: Expr, suffix: Expr },
    /// String contains substring.
    Contains { value: Expr, substring: Expr },
    /// Value is in a list.
    IsIn { value: Expr, values: Expr },
    /// Logical AND.
    And { predicates: Vec<Predicate> },
    /// Logical OR.
    Or { predicates: Vec<Predicate> },
    /// Logical NOT.
    Not { predicate: Box<Predicate> },
    /// Explicit expression comparison.
    Compare {
        /// Left expression.
        left: Expr,
        /// Operator.
        op: CompareOp,
        /// Right expression.
        right: Expr,
    },
}

/// Source predicates are intentionally the same AST shape as normal predicates.
pub type SourcePredicate = Predicate;

impl Predicate {
    /// Create an equality predicate.
    pub fn eq(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        Self::Eq {
            left: Expr::prop(property),
            right: value.into().into_expr(),
        }
    }

    /// Create a not-equals predicate.
    pub fn neq(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        Self::Neq {
            left: Expr::prop(property),
            right: value.into().into_expr(),
        }
    }

    /// Create a greater-than predicate.
    pub fn gt(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        Self::Gt {
            left: Expr::prop(property),
            right: value.into().into_expr(),
        }
    }

    /// Create a greater-than-or-equal predicate.
    pub fn gte(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        Self::Gte {
            left: Expr::prop(property),
            right: value.into().into_expr(),
        }
    }

    /// Create a less-than predicate.
    pub fn lt(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        Self::Lt {
            left: Expr::prop(property),
            right: value.into().into_expr(),
        }
    }

    /// Create a less-than-or-equal predicate.
    pub fn lte(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        Self::Lte {
            left: Expr::prop(property),
            right: value.into().into_expr(),
        }
    }

    /// Create a between predicate.
    pub fn between(
        property: impl Into<String>,
        min: impl Into<PropertyInput>,
        max: impl Into<PropertyInput>,
    ) -> Self {
        Self::Between {
            value: Expr::prop(property),
            min: min.into().into_expr(),
            max: max.into().into_expr(),
        }
    }

    /// Create a has-key predicate.
    pub fn has_key(property: impl Into<String>) -> Self {
        Self::HasKey {
            property: property.into(),
        }
    }

    /// Create an is-null predicate.
    pub fn is_null(property: impl Into<String>) -> Self {
        Self::IsNull {
            property: property.into(),
        }
    }

    /// Create an is-not-null predicate.
    pub fn is_not_null(property: impl Into<String>) -> Self {
        Self::IsNotNull {
            property: property.into(),
        }
    }

    /// Create a starts-with predicate.
    pub fn starts_with(property: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self::StartsWith {
            value: Expr::prop(property),
            prefix: Expr::val(prefix.into()),
        }
    }

    /// Create an ends-with predicate.
    pub fn ends_with(property: impl Into<String>, suffix: impl Into<String>) -> Self {
        Self::EndsWith {
            value: Expr::prop(property),
            suffix: Expr::val(suffix.into()),
        }
    }

    /// Create a contains predicate.
    pub fn contains(property: impl Into<String>, substring: impl Into<String>) -> Self {
        Self::Contains {
            value: Expr::prop(property),
            substring: Expr::val(substring.into()),
        }
    }

    /// Create a parameterized contains predicate.
    pub fn contains_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Self::Contains {
            value: Expr::prop(property),
            substring: Expr::param(param_name),
        }
    }

    /// Create an IN predicate.
    pub fn is_in(property: impl Into<String>, values: impl Into<PropertyValue>) -> Self {
        Self::IsIn {
            value: Expr::prop(property),
            values: Expr::val(values.into()),
        }
    }

    /// Create an IN predicate from an expression.
    pub fn is_in_expr(property: impl Into<String>, values: Expr) -> Self {
        Self::IsIn {
            value: Expr::prop(property),
            values,
        }
    }

    /// Create a parameterized IN predicate.
    pub fn is_in_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Self::is_in_expr(property, Expr::param(param_name))
    }

    /// Combine predicates with AND.
    pub fn and(predicates: Vec<Predicate>) -> Self {
        Self::And { predicates }
    }

    /// Combine predicates with OR.
    pub fn or(predicates: Vec<Predicate>) -> Self {
        Self::Or { predicates }
    }

    /// Negate a predicate.
    #[allow(clippy::should_implement_trait)]
    pub fn not(predicate: Predicate) -> Self {
        Self::Not {
            predicate: Box::new(predicate),
        }
    }

    /// Create an expression comparison predicate.
    pub fn compare(left: Expr, op: CompareOp, right: Expr) -> Self {
        Self::Compare { left, op, right }
    }

    /// Create a parameterized equality predicate.
    pub fn eq_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Self::eq(property, Expr::param(param_name))
    }

    /// Create a parameterized not-equals predicate.
    pub fn neq_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Self::neq(property, Expr::param(param_name))
    }

    /// Create a parameterized greater-than predicate.
    pub fn gt_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Self::gt(property, Expr::param(param_name))
    }

    /// Create a parameterized greater-than-or-equal predicate.
    pub fn gte_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Self::gte(property, Expr::param(param_name))
    }

    /// Create a parameterized less-than predicate.
    pub fn lt_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Self::lt(property, Expr::param(param_name))
    }

    /// Create a parameterized less-than-or-equal predicate.
    pub fn lte_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Self::lte(property, Expr::param(param_name))
    }
}
