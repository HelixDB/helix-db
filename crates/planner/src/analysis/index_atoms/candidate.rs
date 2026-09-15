//! Borrowed atom eligibility for scheduling. Deferred errors remain candidates
//! so authoritative rules retain their existing diagnostic and evaluation order.

use helix_ast::{expr, value::PropertyValue};

use crate::ir;

pub(in crate::analysis) fn has_candidate(predicate: &expr::Predicate) -> bool {
    use expr::{CompareOp, Expr, Predicate};
    match predicate {
        Predicate::Eq { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Eq,
            right,
        } => match (left, right) {
            (Expr::Property(_), Expr::Constant(value))
            | (Expr::Constant(value), Expr::Property(_)) => {
                ir::SecondaryIndexLiteral::validate_value(value).is_ok()
            }
            (Expr::Property(_), _) | (_, Expr::Property(_)) => true,
            _ => false,
        },
        Predicate::IsIn {
            value: Expr::Property(_),
            values,
        } => match values {
            Expr::Constant(PropertyValue::Array(values)) => values
                .iter()
                .all(|value| ir::SecondaryIndexLiteral::validate_value(value).is_ok()),
            Expr::Constant(value) => ir::SecondaryIndexLiteral::validate_value(value).is_ok(),
            _ => true,
        },
        Predicate::Gt { left, right }
        | Predicate::Gte { left, right }
        | Predicate::Lt { left, right }
        | Predicate::Lte { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte,
            right,
        } => {
            let other = match (left, right) {
                (Expr::Property(_), other) | (other, Expr::Property(_)) => other,
                _ => return false,
            };
            !matches!(RangeCandidate::new(other), RangeCandidate::Rejected)
        }
        Predicate::Between {
            value: Expr::Property(_),
            min,
            max,
        } => {
            let lower = RangeCandidate::new(min);
            match lower {
                RangeCandidate::Rejected => return false,
                RangeCandidate::DeferredError => return true,
                RangeCandidate::Runtime | RangeCandidate::Literal(_) => {}
            }
            match (lower, RangeCandidate::new(max)) {
                (_, RangeCandidate::Rejected) => false,
                (_, RangeCandidate::DeferredError) => true,
                (RangeCandidate::Literal(lower), RangeCandidate::Literal(upper)) => {
                    lower.compare(upper).is_some_and(|order| order.is_le())
                }
                _ => true,
            }
        }
        Predicate::Neq { .. }
        | Predicate::Between { .. }
        | Predicate::HasKey { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::StartsWith { .. }
        | Predicate::EndsWith { .. }
        | Predicate::Contains { .. }
        | Predicate::IsIn { .. }
        | Predicate::And { .. }
        | Predicate::Or { .. }
        | Predicate::Not { .. }
        | Predicate::Compare {
            op: CompareOp::Neq, ..
        } => false,
    }
}

enum RangeCandidate<'a> {
    Rejected,
    DeferredError,
    Runtime,
    Literal(ir::RangeLiteralRef<'a>),
}

impl<'a> RangeCandidate<'a> {
    fn new(value: &'a expr::Expr) -> Self {
        match value {
            expr::Expr::Constant(value) => {
                ir::RangeLiteralRef::new(value).map_or(Self::Rejected, Self::Literal)
            }
            expr::Expr::Param(name) if !name.is_empty() => Self::Runtime,
            _ => Self::DeferredError,
        }
    }
}
