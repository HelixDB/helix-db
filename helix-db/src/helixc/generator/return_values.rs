use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::{traversal_steps::Traversal, utils::GeneratedValue};


pub struct ReturnValue {
    pub value: ReturnValueExpr,
    pub return_type: ReturnType,
}
impl Display for ReturnValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.return_type {
            ReturnType::Literal(name) => {
                writeln!(
                    f,
                    "    return_vals.insert({}.to_string(), ReturnValue::from(Value::from({})));",
                    name, self.value
                )
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
                    "    return_vals.insert({}.to_string(), ReturnValue::from_traversal_value_array_with_mixin({}.clone(), remapping_vals.borrow_mut()));",
                    name, self.value
                )
            }
            ReturnType::SingleExpr(name) => {
                writeln!(
                    f,
                    "    return_vals.insert({}.to_string(), ReturnValue::from_traversal_value_with_mixin({}.clone(), remapping_vals.borrow_mut()));",
                    name, self.value
                )
            }
            ReturnType::UnnamedExpr => {
                write!(f, "// need to implement unnamed return value\n todo!()")?;
                panic!("Unnamed return value is not supported");
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
            ReturnType::UnnamedExpr => todo!(),
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
}

#[derive(Clone)]
pub enum ReturnType {
    Literal(GeneratedValue),
    NamedLiteral(GeneratedValue),
    NamedExpr(GeneratedValue),
    SingleExpr(GeneratedValue),
    UnnamedExpr,
}
#[derive(Clone)]
pub enum ReturnValueExpr {
    Traversal(Traversal),
    Identifier(GeneratedValue),
    Value(GeneratedValue),
}
impl Display for ReturnValueExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReturnValueExpr::Traversal(traversal) => write!(f, "{traversal}"),
            ReturnValueExpr::Identifier(identifier) => write!(f, "{identifier}"),
            ReturnValueExpr::Value(value) => write!(f, "{value}"),
        }
    }
}
