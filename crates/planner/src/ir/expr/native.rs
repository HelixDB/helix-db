//! Native frontend adapter for the shared scalar expression structure. Native
//! literal widths and two-valued comparison semantics are deliberately retained.
use crate::relational::{ScalarExpression, Slot};
use helix_ast::{expr, value};

pub type Expression = ScalarExpression<value::PropertyValue, Unary, Binary, Function>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unary {
    Negate,
    Not,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Binary {
    FloatAdd,
    FloatSubtract,
    FloatMultiply,
    FloatDivide,
    IntegerRemainder,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    StartsWith,
    EndsWith,
    Contains,
    MembershipOrScalarEquality,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Function {
    SaturatingId,
    Timestamp,
    DateTimeNow,
    HasProperty(String),
    /// Evaluate value once; evaluate the upper bound only if the lower bound passes.
    Between,
    All,
    Any,
}

/// The native row's current element is the adapter's implicit input slot.
pub const CURRENT: Slot = Slot(0);

pub(super) fn expression(input: &expr::Expr) -> Expression {
    match input {
        expr::Expr::Property(key) => {
            Expression::Property(Box::new(Expression::Slot(CURRENT)), key.clone())
        }
        expr::Expr::Id => {
            Expression::Function(Function::SaturatingId, vec![Expression::Slot(CURRENT)])
        }
        expr::Expr::Timestamp => Expression::Function(Function::Timestamp, Vec::new()),
        expr::Expr::DateTimeNow => Expression::Function(Function::DateTimeNow, Vec::new()),
        expr::Expr::Constant(value) => Expression::Literal(value.clone()),
        expr::Expr::Param(name) => Expression::Parameter(name.clone()),
        expr::Expr::Add { left, right } => binary(Binary::FloatAdd, left, right),
        expr::Expr::Sub { left, right } => binary(Binary::FloatSubtract, left, right),
        expr::Expr::Mul { left, right } => binary(Binary::FloatMultiply, left, right),
        expr::Expr::Div { left, right } => binary(Binary::FloatDivide, left, right),
        expr::Expr::Mod { left, right } => binary(Binary::IntegerRemainder, left, right),
        expr::Expr::Neg { expr } => Expression::Unary(Unary::Negate, Box::new(expression(expr))),
        expr::Expr::Case {
            when_then,
            else_expr,
        } => Expression::Case {
            branches: when_then
                .iter()
                .map(|branch| (predicate(&branch.when), expression(&branch.then)))
                .collect(),
            otherwise: Box::new(
                else_expr
                    .as_deref()
                    .map(expression)
                    .unwrap_or(Expression::Literal(value::PropertyValue::Null)),
            ),
        },
    }
}

fn binary(operation: Binary, left: &expr::Expr, right: &expr::Expr) -> Expression {
    Expression::Binary(
        operation,
        Box::new(expression(left)),
        Box::new(expression(right)),
    )
}

pub(super) fn predicate(input: &expr::Predicate) -> Expression {
    match input {
        expr::Predicate::Eq { left, right } => binary(Binary::Equal, left, right),
        expr::Predicate::Neq { left, right } => binary(Binary::NotEqual, left, right),
        expr::Predicate::Lt { left, right } => binary(Binary::Less, left, right),
        expr::Predicate::Lte { left, right } => binary(Binary::LessEqual, left, right),
        expr::Predicate::Gt { left, right } => binary(Binary::Greater, left, right),
        expr::Predicate::Gte { left, right } => binary(Binary::GreaterEqual, left, right),
        expr::Predicate::Compare { left, op, right } => binary(
            match op {
                expr::CompareOp::Eq => Binary::Equal,
                expr::CompareOp::Neq => Binary::NotEqual,
                expr::CompareOp::Lt => Binary::Less,
                expr::CompareOp::Lte => Binary::LessEqual,
                expr::CompareOp::Gt => Binary::Greater,
                expr::CompareOp::Gte => Binary::GreaterEqual,
            },
            left,
            right,
        ),
        expr::Predicate::StartsWith { value, prefix } => binary(Binary::StartsWith, value, prefix),
        expr::Predicate::EndsWith { value, suffix } => binary(Binary::EndsWith, value, suffix),
        expr::Predicate::Contains { value, substring } => {
            binary(Binary::Contains, value, substring)
        }
        expr::Predicate::IsIn { value, values } => {
            binary(Binary::MembershipOrScalarEquality, value, values)
        }
        expr::Predicate::HasKey { property } => Expression::Function(
            Function::HasProperty(property.clone()),
            vec![Expression::Slot(CURRENT)],
        ),
        expr::Predicate::IsNull { property } => Expression::Unary(
            Unary::IsNull,
            Box::new(Expression::Property(
                Box::new(Expression::Slot(CURRENT)),
                property.clone(),
            )),
        ),
        expr::Predicate::IsNotNull { property } => Expression::Unary(
            Unary::IsNotNull,
            Box::new(Expression::Property(
                Box::new(Expression::Slot(CURRENT)),
                property.clone(),
            )),
        ),
        expr::Predicate::Not { predicate: inner } => {
            Expression::Unary(Unary::Not, Box::new(predicate(inner)))
        }
        expr::Predicate::Between { value, min, max } => Expression::Function(
            Function::Between,
            vec![expression(value), expression(min), expression(max)],
        ),
        expr::Predicate::And { predicates } | expr::Predicate::Or { predicates } => {
            let operation = if matches!(input, expr::Predicate::And { .. }) {
                Function::All
            } else {
                Function::Any
            };
            Expression::Function(operation, predicates.iter().map(predicate).collect())
        }
    }
}
