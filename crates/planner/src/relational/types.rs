//! Static value domains. `Any` means data-dependent, rather than silently
//! coercing an incompatible known type. Nullability is tracked by row bindings.
use super::{Aggregate, Binary, Binding, Expression, Function, QueryError, Result, Unary, Value};
use crate::relational::{category, detail};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ValueType {
    Any,
    Null,
    Boolean,
    Integer,
    Float,
    String,
    DateTime,
    List,
    Map,
    Node,
    Relationship,
    Path,
}

impl Expression {
    pub fn value_type(&self, bindings: &[Binding]) -> Result<ValueType> {
        use ValueType as T;
        let require = |actual: T, accepted: &[T], category: &str| -> Result<()> {
            if matches!(actual, T::Any | T::Null) || accepted.contains(&actual) {
                Ok(())
            } else {
                Err(QueryError::compile(
                    category,
                    detail::INVALID_ARGUMENT_TYPE,
                    format!("expected {accepted:?}, received {actual:?}"),
                ))
            }
        };
        let infer = |e: &Expression| e.value_type(bindings);
        Ok(match self {
            Self::Literal(value) => T::of(value),
            Self::Slot(slot) => {
                bindings
                    .get(slot.0 as usize)
                    .ok_or_else(|| {
                        QueryError::compile(
                            category::INTERNAL_PLANNER_ERROR,
                            detail::INVALID_SLOT,
                            "slot exceeds binding catalog",
                        )
                    })?
                    .value_type
            }
            Self::Parameter(_) => T::Any,
            Self::Property(value, key) => match infer(value)? {
                T::DateTime => {
                    super::DatetimeField::from_key(key).ok_or_else(|| {
                        QueryError::compile(
                            category::TYPE_ERROR,
                            detail::INVALID_ARGUMENT_TYPE,
                            format!("a datetime has no property `{key}`"),
                        )
                    })?;
                    T::Integer
                }
                value => {
                    require(
                        value,
                        &[T::Map, T::Node, T::Relationship, T::DateTime],
                        category::TYPE_ERROR,
                    )?;
                    T::Any
                }
            },
            Self::Index(value, index) => {
                require(
                    infer(value)?,
                    &[T::List, T::Map, T::Node, T::Relationship],
                    category::TYPE_ERROR,
                )?;
                infer(index)?;
                T::Any
            }
            Self::Slice { value, start, end } => {
                require(infer(value)?, &[T::List], category::SYNTAX_ERROR)?;
                for bound in start.iter().chain(end.iter()) {
                    require(infer(bound)?, &[T::Integer], category::SYNTAX_ERROR)?;
                }
                T::List
            }
            Self::Unary(op, x) => {
                let t = infer(x)?;
                match op {
                    Unary::Not => {
                        require(t, &[T::Boolean], category::SYNTAX_ERROR)?;
                        T::Boolean
                    }
                    Unary::Negate | Unary::Positive => {
                        require(t, &[T::Integer, T::Float], category::SYNTAX_ERROR)?;
                        t
                    }
                    Unary::IsNull | Unary::IsNotNull => T::Boolean,
                }
            }
            Self::Binary(Binary::In, a, list) => {
                let a = infer(a)?;
                // Each member of a list written out is compared with `a`.
                match list.as_ref() {
                    Self::List(members) => {
                        for member in members {
                            comparable(a, infer(member)?)?;
                        }
                    }
                    Self::Literal(Value::List(members)) => {
                        for member in members {
                            comparable(a, T::of(member))?;
                        }
                    }
                    list => require(infer(list)?, &[T::List], category::SYNTAX_ERROR)?,
                }
                T::Boolean
            }
            Self::Binary(op, a, b) => {
                let a = infer(a)?;
                let b = infer(b)?;
                match op {
                    Binary::Equal
                    | Binary::NotEqual
                    | Binary::Less
                    | Binary::LessEqual
                    | Binary::Greater
                    | Binary::GreaterEqual
                    | Binary::StartsWith
                    | Binary::EndsWith
                    | Binary::Contains => {
                        comparable(a, b)?;
                        T::Boolean
                    }
                    Binary::In => unreachable!("matched above"),
                    Binary::Add if a == T::List || b == T::List => T::List,
                    Binary::Add if a == T::String || b == T::String => T::String,
                    Binary::Add
                    | Binary::Subtract
                    | Binary::Multiply
                    | Binary::Divide
                    | Binary::Modulo
                    | Binary::Power => {
                        require(a, &[T::Integer, T::Float], category::SYNTAX_ERROR)?;
                        require(b, &[T::Integer, T::Float], category::SYNTAX_ERROR)?;
                        if a == T::Float || b == T::Float || *op == Binary::Power {
                            T::Float
                        } else if a == T::Integer && b == T::Integer {
                            T::Integer
                        } else {
                            T::Any
                        }
                    }
                }
            }
            Self::Connective(_, operands) => {
                let [first, second, rest @ ..] = operands.as_ref() else {
                    unreachable!("a connective has at least two operands");
                };
                // As the left-associated chain did, infer the first two
                // operands before checking either, then each later one in turn.
                let (first, second) = (infer(first)?, infer(second)?);
                require(first, &[T::Boolean], category::SYNTAX_ERROR)?;
                require(second, &[T::Boolean], category::SYNTAX_ERROR)?;
                for operand in rest {
                    require(infer(operand)?, &[T::Boolean], category::SYNTAX_ERROR)?;
                }
                T::Boolean
            }
            Self::List(xs) => {
                for x in xs {
                    infer(x)?;
                }
                T::List
            }
            Self::Map(xs) => {
                for (_, x) in xs {
                    infer(x)?;
                }
                T::Map
            }
            Self::HasLabel(slot, _) => {
                require(
                    infer(&Self::Slot(*slot))?,
                    &[T::Node],
                    category::SYNTAX_ERROR,
                )?;
                T::Boolean
            }
            Self::Case {
                branches,
                otherwise,
            } => {
                let mut result = infer(otherwise)?;
                for (predicate, value) in branches {
                    require(infer(predicate)?, &[T::Boolean], category::SYNTAX_ERROR)?;
                    let t = infer(value)?;
                    if result == T::Null {
                        result = t;
                    } else if t != T::Null && t != result {
                        result = T::Any;
                    }
                }
                result
            }
            Self::SimpleCase(case) => {
                let operand = infer(&case.operand)?;
                let mut result = infer(&case.otherwise)?;
                for (comparison, value) in &case.branches {
                    comparable(operand, infer(comparison)?)?;
                    let value_type = infer(value)?;
                    if result == T::Null {
                        result = value_type;
                    } else if value_type != T::Null && value_type != result {
                        result = T::Any;
                    }
                }
                result
            }
            Self::Aggregate {
                function, argument, ..
            } => {
                if let Some(argument) = argument {
                    infer(argument)?;
                }
                match function {
                    Aggregate::Count => T::Integer,
                    Aggregate::Avg => T::Float,
                    Aggregate::Collect => T::List,
                    Aggregate::Sum | Aggregate::Min | Aggregate::Max => T::Any,
                }
            }
            Self::Function(function, args) => {
                let types = args.iter().map(infer).collect::<Result<Vec<_>>>()?;
                let first = types.first().copied().unwrap_or(T::Any);
                match function {
                    Function::Id => {
                        require(first, &[T::Node, T::Relationship], category::SYNTAX_ERROR)?;
                        T::Integer
                    }
                    Function::Type => {
                        require(first, &[T::Relationship], category::SYNTAX_ERROR)?;
                        T::String
                    }
                    Function::Labels => {
                        require(first, &[T::Node], category::TYPE_ERROR)?;
                        T::List
                    }
                    Function::Properties => {
                        require(
                            first,
                            &[T::Node, T::Relationship, T::Map],
                            category::SYNTAX_ERROR,
                        )?;
                        T::Map
                    }
                    Function::Keys => {
                        require(
                            first,
                            &[T::Node, T::Relationship, T::Map],
                            category::SYNTAX_ERROR,
                        )?;
                        T::List
                    }
                    Function::Nodes | Function::Relationships => {
                        require(first, &[T::Path], category::SYNTAX_ERROR)?;
                        T::List
                    }
                    Function::Length => {
                        require(first, &[T::Path], category::SYNTAX_ERROR)?;
                        T::Integer
                    }
                    Function::Size => {
                        require(first, &[T::List, T::String], category::SYNTAX_ERROR)?;
                        T::Integer
                    }
                    Function::Head | Function::Last => {
                        require(first, &[T::List], category::SYNTAX_ERROR)?;
                        T::Any
                    }
                    Function::Coalesce => T::Any,
                    Function::ToString => T::String,
                    Function::ToInteger => T::Integer,
                    Function::ToFloat => T::Float,
                    Function::ToBoolean | Function::Exists => T::Boolean,
                    Function::Abs => first,
                    Function::Range => T::List,
                    Function::Reverse => first,
                    Function::Trim
                    | Function::Ltrim
                    | Function::Rtrim
                    | Function::ToLower
                    | Function::ToUpper
                    | Function::Substring => T::String,
                    Function::DateTime => {
                        require(
                            first,
                            &[T::String, T::DateTime, T::Map],
                            category::TYPE_ERROR,
                        )?;
                        T::DateTime
                    }
                    Function::Timestamp => T::Integer,
                    // Any argument converts or is null.
                    Function::ToStringOrNull => T::String,
                    Function::ToIntegerOrNull => T::Integer,
                    Function::ToFloatOrNull => T::Float,
                    Function::ToBooleanOrNull => T::Boolean,
                    Function::DateTimeOrNull => T::DateTime,
                    Function::RequireDateTime => {
                        require(first, &[T::DateTime], category::TYPE_ERROR)?;
                        T::DateTime
                    }
                }
            }
        })
    }
}

impl ValueType {
    /// The type of a value, as of a literal holding it.
    ///
    /// ```
    /// use helix_planner::relational as r;
    /// assert_eq!(r::ValueType::of(&r::Value::DateTime(0)), r::ValueType::DateTime);
    /// assert_eq!(r::ValueType::of(&r::Value::List(vec![])), r::ValueType::List);
    /// ```
    pub fn of(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Boolean(_) => Self::Boolean,
            Value::Integer(_) => Self::Integer,
            Value::Float(_) => Self::Float,
            Value::String(_) => Self::String,
            Value::DateTime(_) => Self::DateTime,
            Value::List(_) => Self::List,
            Value::Map(_) => Self::Map,
            Value::Entity(super::Entity::Node(_)) => Self::Node,
            Value::Entity(super::Entity::Relationship(_)) => Self::Relationship,
            Value::Path(_) => Self::Path,
        }
    }
}

/// Fails with the compile-time type error of comparing operands known to be a
/// datetime and text, in either order: a comparison, a text match, an IN
/// member or a simple CASE's WHEN value. Only these fail: at run time the pair
/// compares as unknown, so whether a row matches never depends on an index.
pub(super) fn comparable(a: ValueType, b: ValueType) -> Result<()> {
    use ValueType as T;
    if !matches!((a, b), (T::DateTime, T::String) | (T::String, T::DateTime)) {
        return Ok(());
    }
    Err(QueryError::compile(
        category::TYPE_ERROR,
        detail::DATETIME_TEXT_COMPARISON,
        "cannot compare a datetime with text; read the text as a datetime with \
         datetime('…') in Cypher or TIMESTAMP '…' in SQL",
    ))
}
