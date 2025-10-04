use core::fmt;
use std::{collections::HashMap, fmt::Display};

use crate::helixc::generator::{traversal_steps::Traversal, utils::GeneratedValue};

pub struct ReturnValue {
    pub value: ReturnValueExpr,
    pub return_type: ReturnType,
}
impl Display for ReturnValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.return_type {
            ReturnType::Literal(name) => {
                if let GeneratedValue::Aggregate(name) = name {
                    writeln!(
                        f,
                        "    return_vals.insert({}.to_string(), ReturnValue::from({}));",
                        name, self.value
                    )
                } else {
                    writeln!(
                        f,
                        "    return_vals.insert({}.to_string(), ReturnValue::from(Value::from({})));",
                        name, self.value
                    )
                }
            }
            ReturnType::NamedLiteral(name) => {
                writeln!(
                    f,
                    "    return_vals.insert({}.to_string(), ReturnValue::from(Value::from({})));",
                    name, self.value
                )
            }
            ReturnType::NamedExpr(name) => {
                writeln!(
                    f,
                    "    return_vals.insert({}.to_string(), ReturnValue::from_traversal_value_array_with_mixin({}, remapping_vals.borrow_mut()));",
                    name, self.value
                )
            }
            ReturnType::SingleExpr(name) => {
                writeln!(
                    f,
                    "    return_vals.insert({}.to_string(), ReturnValue::from_traversal_value_with_mixin({}, remapping_vals.borrow_mut()));",
                    name, self.value
                )
            }
            ReturnType::UnnamedExpr => {
                writeln!(
                    f,
                    "    return_vals.insert(\"data\".to_string(), ReturnValue::from_traversal_value_array_with_mixin({}, remapping_vals.borrow_mut()));",
                    self.value
                )
            }
            ReturnType::HashMap => {
                writeln!(
                    f,
                    "    return_vals.insert(\"data\".to_string(), ReturnValue::from({}));",
                    self.value
                )
            }
            ReturnType::Array => {
                writeln!(
                    f,
                    "    return_vals.insert(\"data\".to_string(), ReturnValue::from({}));",
                    self.value
                )
            }
            ReturnType::Aggregate(name) => {
                writeln!(
                    f,
                    "    return_vals.insert({}.to_string(), ReturnValue::from({}));",
                    name, self.value
                )
            }
        }
    }
}

impl ReturnValue {
    pub fn get_name(&self) -> String {
        match &self.return_type {
            ReturnType::Literal(name) => name.inner().inner().to_string(),
            ReturnType::NamedLiteral(name) => name.inner().inner().to_string(),
            ReturnType::NamedExpr(name) => name.inner().inner().to_string(),
            ReturnType::SingleExpr(name) => name.inner().inner().to_string(),
            ReturnType::UnnamedExpr => unimplemented!(),
            ReturnType::HashMap => unimplemented!(),
            ReturnType::Array => unimplemented!(),
            ReturnType::Aggregate(name) => name.to_string(),
        }
    }

    pub fn new_literal(name: GeneratedValue, value: GeneratedValue) -> Self {
        Self {
            value: ReturnValueExpr::Value(value.clone()),
            return_type: ReturnType::Literal(name),
        }
    }
    pub fn new_named_literal(name: GeneratedValue, value: GeneratedValue) -> Self {
        Self {
            value: ReturnValueExpr::Value(value.clone()),
            return_type: ReturnType::NamedLiteral(name),
        }
    }
    pub fn new_named(name: GeneratedValue, value: ReturnValueExpr) -> Self {
        Self {
            value,
            return_type: ReturnType::NamedExpr(name),
        }
    }
    pub fn new_single_named(name: GeneratedValue, value: ReturnValueExpr) -> Self {
        Self {
            value,
            return_type: ReturnType::SingleExpr(name),
        }
    }
    pub fn new_unnamed(value: ReturnValueExpr) -> Self {
        Self {
            value,
            return_type: ReturnType::UnnamedExpr,
        }
    }
    pub fn new_array(values: Vec<ReturnValueExpr>) -> Self {
        Self {
            value: ReturnValueExpr::Array(values),
            return_type: ReturnType::Array,
        }
    }
    pub fn new_object(values: HashMap<String, ReturnValueExpr>) -> Self {
        Self {
            value: ReturnValueExpr::Object(values),
            return_type: ReturnType::HashMap,
        }
    }
    pub fn new_aggregate(name: GeneratedValue, value: GeneratedValue) -> Self {
        Self {
            value: ReturnValueExpr::Value(value.clone()),
            return_type: ReturnType::Aggregate(name),
        }
    }
    pub fn new_aggregate_traversal(name: GeneratedValue, value: ReturnValueExpr) -> Self {
        Self {
            value,
            return_type: ReturnType::Aggregate(name),
        }
    }
}

#[derive(Clone)]
pub enum ReturnType {
    Literal(GeneratedValue),
    NamedLiteral(GeneratedValue),
    NamedExpr(GeneratedValue),
    SingleExpr(GeneratedValue),
    UnnamedExpr,
    HashMap,
    Array,
    Aggregate(GeneratedValue),
}
#[derive(Clone)]
pub enum ReturnValueExpr {
    Traversal(Traversal),
    Identifier(GeneratedValue),
    Value(GeneratedValue),
    Array(Vec<ReturnValueExpr>),
    Object(HashMap<String, ReturnValueExpr>),
}
impl Display for ReturnValueExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReturnValueExpr::Traversal(traversal) => write!(f, "{traversal}"),
            ReturnValueExpr::Identifier(identifier) => write!(f, "{identifier}"),
            ReturnValueExpr::Value(value) => write!(f, "{value}"),
            ReturnValueExpr::Array(values) => {
                write!(f, "vec![")?;
                // if traversal then use the other from functions
                for value in values {
                    write!(f, "ReturnValue::from({value}),")?;
                }
                write!(f, "]")
            }
            ReturnValueExpr::Object(values) => {
                write!(f, "HashMap::from([")?;
                // if traversal then use the other from functions
                for (key, value) in values {
                    write!(f, "(String::from(\"{key}\"), ReturnValue::from({value})),")?;
                }
                write!(f, "])")
            }
        }
    }
}
