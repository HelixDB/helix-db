//! Static value domains. `Any` means data-dependent, rather than silently
//! coercing an incompatible known type. Nullability is tracked by row bindings.
use super::{Aggregate, Binary, Binding, Expression, Function, QueryError, Result, Unary, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ValueType {
    Any,
    Null,
    Boolean,
    Integer,
    Float,
    String,
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
                    "InvalidArgumentType",
                    format!("expected {accepted:?}, received {actual:?}"),
                ))
            }
        };
        let infer = |e: &Expression| e.value_type(bindings);
        Ok(match self {
            Self::Literal(value) => match value {
                Value::Null => T::Null,
                Value::Boolean(_) => T::Boolean,
                Value::Integer(_) => T::Integer,
                Value::Float(_) => T::Float,
                Value::String(_) => T::String,
                Value::List(_) => T::List,
                Value::Map(_) => T::Map,
                Value::Entity(super::Entity::Node(_)) => T::Node,
                Value::Entity(super::Entity::Relationship(_)) => T::Relationship,
                Value::Path(_) => T::Path,
            },
            Self::Slot(slot) => {
                bindings
                    .get(slot.0 as usize)
                    .ok_or_else(|| {
                        QueryError::compile(
                            "InternalPlannerError",
                            "InvalidSlot",
                            "slot exceeds binding catalog",
                        )
                    })?
                    .value_type
            }
            Self::Parameter(_) => T::Any,
            Self::Property(value, _) => {
                require(
                    infer(value)?,
                    &[T::Map, T::Node, T::Relationship],
                    "TypeError",
                )?;
                T::Any
            }
            Self::Index(value, index) => {
                require(
                    infer(value)?,
                    &[T::List, T::Map, T::Node, T::Relationship],
                    "TypeError",
                )?;
                infer(index)?;
                T::Any
            }
            Self::Slice { value, start, end } => {
                require(infer(value)?, &[T::List], "SyntaxError")?;
                for bound in start.iter().chain(end.iter()) {
                    require(infer(bound)?, &[T::Integer], "SyntaxError")?;
                }
                T::List
            }
            Self::Unary(op, x) => {
                let t = infer(x)?;
                match op {
                    Unary::Not => {
                        require(t, &[T::Boolean], "SyntaxError")?;
                        T::Boolean
                    }
                    Unary::Negate | Unary::Positive => {
                        require(t, &[T::Integer, T::Float], "SyntaxError")?;
                        t
                    }
                    Unary::IsNull | Unary::IsNotNull => T::Boolean,
                }
            }
            Self::Binary(op, a, b) => {
                let a = infer(a)?;
                let b = infer(b)?;
                match op {
                    Binary::And | Binary::Or | Binary::Xor => {
                        require(a, &[T::Boolean], "SyntaxError")?;
                        require(b, &[T::Boolean], "SyntaxError")?;
                        T::Boolean
                    }
                    Binary::In => {
                        require(b, &[T::List], "SyntaxError")?;
                        T::Boolean
                    }
                    Binary::StartsWith | Binary::EndsWith | Binary::Contains => T::Boolean,
                    Binary::Equal
                    | Binary::NotEqual
                    | Binary::Less
                    | Binary::LessEqual
                    | Binary::Greater
                    | Binary::GreaterEqual => T::Boolean,
                    Binary::Add if a == T::List || b == T::List => T::List,
                    Binary::Add if a == T::String || b == T::String => T::String,
                    Binary::Add
                    | Binary::Subtract
                    | Binary::Multiply
                    | Binary::Divide
                    | Binary::Modulo
                    | Binary::Power => {
                        require(a, &[T::Integer, T::Float], "SyntaxError")?;
                        require(b, &[T::Integer, T::Float], "SyntaxError")?;
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
                require(infer(&Self::Slot(*slot))?, &[T::Node], "SyntaxError")?;
                T::Boolean
            }
            Self::Case {
                branches,
                otherwise,
            } => {
                let mut result = infer(otherwise)?;
                for (predicate, value) in branches {
                    require(infer(predicate)?, &[T::Boolean], "SyntaxError")?;
                    let t = infer(value)?;
                    if result == T::Null {
                        result = t;
                    } else if t != T::Null && t != result {
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
                        require(first, &[T::Node, T::Relationship], "SyntaxError")?;
                        T::Integer
                    }
                    Function::Type => {
                        require(first, &[T::Relationship], "SyntaxError")?;
                        T::String
                    }
                    Function::Labels => {
                        require(first, &[T::Node], "TypeError")?;
                        T::List
                    }
                    Function::Properties => {
                        require(first, &[T::Node, T::Relationship, T::Map], "SyntaxError")?;
                        T::Map
                    }
                    Function::Keys => {
                        require(first, &[T::Node, T::Relationship, T::Map], "SyntaxError")?;
                        T::List
                    }
                    Function::Nodes | Function::Relationships => {
                        require(first, &[T::Path], "SyntaxError")?;
                        T::List
                    }
                    Function::Length => {
                        require(first, &[T::Path], "SyntaxError")?;
                        T::Integer
                    }
                    Function::Size => {
                        require(first, &[T::List, T::String], "SyntaxError")?;
                        T::Integer
                    }
                    Function::Head | Function::Last => {
                        require(first, &[T::List], "SyntaxError")?;
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
                }
            }
        })
    }
}
