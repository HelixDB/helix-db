//! Predicate-to-literal extraction contracts for scalar contradiction proofs.

use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;

use super::values::literal_collection_values;
use crate::ir::{BoundInclusivity, RangeIndexLiteral};

/// Extract a finite, reflexive literal collection from a property `IN` predicate.
///
/// The returned values are deduplicated and preserve first-seen order. Dynamic
/// collections, non-collection literals, and collections containing non-reflexive
/// float values return `None`.
pub(super) fn literal_in_values(predicate: &Predicate) -> Option<(String, Vec<PropertyValue>)> {
    let Predicate::IsIn {
        value: Expr::Property(property),
        values: Expr::Constant(values),
    } = predicate
    else {
        return None;
    };
    literal_collection_values(values).map(|values| (property.clone(), values))
}

pub(super) fn nullability_constraint(
    predicate: &Predicate,
) -> Option<(String, NullabilityConstraint)> {
    match predicate {
        Predicate::IsNull { property } => {
            Some((property.clone(), NullabilityConstraint::NullOrMissing))
        }
        Predicate::IsNotNull { property } => {
            Some((property.clone(), NullabilityConstraint::NonNull))
        }
        _ => None,
    }
}

pub(super) fn equality_literal(predicate: &Predicate) -> Option<(String, PropertyValue)> {
    match predicate {
        Predicate::Eq { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Eq,
            right,
        } => property_literal_value(left, right),
        _ => None,
    }
}

pub(super) fn inequality_literal(predicate: &Predicate) -> Option<(String, PropertyValue)> {
    match predicate {
        Predicate::Neq { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Neq,
            right,
        } => property_literal_value(left, right),
        _ => None,
    }
}

pub(super) fn range_bound_literal(
    predicate: &Predicate,
) -> Option<(String, BoundKind, LiteralBound)> {
    match predicate {
        Predicate::Gt { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Gt,
            right,
        } => range_bound_from_compare(
            left,
            right,
            PropertyBoundSide::LowerWhenLeft,
            BoundInclusivity::Exclusive,
        ),
        Predicate::Gte { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Gte,
            right,
        } => range_bound_from_compare(
            left,
            right,
            PropertyBoundSide::LowerWhenLeft,
            BoundInclusivity::Inclusive,
        ),
        Predicate::Lt { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Lt,
            right,
        } => range_bound_from_compare(
            left,
            right,
            PropertyBoundSide::UpperWhenLeft,
            BoundInclusivity::Exclusive,
        ),
        Predicate::Lte { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Lte,
            right,
        } => range_bound_from_compare(
            left,
            right,
            PropertyBoundSide::UpperWhenLeft,
            BoundInclusivity::Inclusive,
        ),
        _ => None,
    }
}

pub(super) fn between_literal_bounds(
    predicate: &Predicate,
) -> Option<(String, LiteralBound, LiteralBound)> {
    let Predicate::Between { value, min, max } = predicate else {
        return None;
    };
    let (Expr::Property(property), Expr::Constant(min), Expr::Constant(max)) = (value, min, max)
    else {
        return None;
    };
    Some((
        property.clone(),
        LiteralBound::new(min.clone(), BoundInclusivity::Inclusive)?,
        LiteralBound::new(max.clone(), BoundInclusivity::Inclusive)?,
    ))
}

fn property_literal_value(left: &Expr, right: &Expr) -> Option<(String, PropertyValue)> {
    match (left, right) {
        (Expr::Property(property), Expr::Constant(value))
        | (Expr::Constant(value), Expr::Property(property)) => {
            Some((property.clone(), value.clone()))
        }
        _ => None,
    }
}

fn range_bound_from_compare(
    left: &Expr,
    right: &Expr,
    left_property_bound: PropertyBoundSide,
    inclusivity: BoundInclusivity,
) -> Option<(String, BoundKind, LiteralBound)> {
    if let (Expr::Property(property), Expr::Constant(value)) = (left, right) {
        let bound = LiteralBound::new(value.clone(), inclusivity)?;
        return Some((
            property.clone(),
            match left_property_bound {
                PropertyBoundSide::LowerWhenLeft => BoundKind::Lower,
                PropertyBoundSide::UpperWhenLeft => BoundKind::Upper,
            },
            bound,
        ));
    }
    if let (Expr::Constant(value), Expr::Property(property)) = (left, right) {
        let bound = LiteralBound::new(value.clone(), inclusivity)?;
        return Some((
            property.clone(),
            match left_property_bound {
                PropertyBoundSide::LowerWhenLeft => BoundKind::Upper,
                PropertyBoundSide::UpperWhenLeft => BoundKind::Lower,
            },
            bound,
        ));
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BoundKind {
    Lower,
    Upper,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NullabilityConstraint {
    NullOrMissing,
    NonNull,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct LiteralBound {
    value: PropertyValue,
    inclusivity: BoundInclusivity,
}

impl LiteralBound {
    fn new(value: PropertyValue, inclusivity: BoundInclusivity) -> Option<Self> {
        RangeIndexLiteral::try_from_property_value(value.clone())?;
        Some(Self { value, inclusivity })
    }

    pub(super) fn value(&self) -> &PropertyValue {
        &self.value
    }

    pub(super) fn is_inclusive(&self) -> bool {
        self.inclusivity == BoundInclusivity::Inclusive
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropertyBoundSide {
    LowerWhenLeft,
    UpperWhenLeft,
}
