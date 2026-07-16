//! Range-index atom extraction.

use helix_ast::expr::{CompareOp, Expr, Predicate};

use super::value::{expr_range_index_value, RangeIndexValueAtom};
use crate::error::PlannerError;
use crate::ir::{BoundInclusivity, IndexBetweenRange, IndexBound, IndexRange};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RangeIndexAtom {
    Atom { property: String, range: IndexRange },
    NotIndexable,
}

pub(crate) fn range_atom(predicate: &Predicate) -> Result<RangeIndexAtom, PlannerError> {
    match predicate {
        Predicate::Gt { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Gt,
            right,
        } => range_from_compare(
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
        } => range_from_compare(
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
        } => range_from_compare(
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
        } => range_from_compare(
            left,
            right,
            PropertyBoundSide::UpperWhenLeft,
            BoundInclusivity::Inclusive,
        ),
        Predicate::Between { value, min, max } => between_range(value, min, max),
        Predicate::Eq { .. }
        | Predicate::Neq { .. }
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
            op: CompareOp::Eq | CompareOp::Neq,
            ..
        } => Ok(RangeIndexAtom::NotIndexable),
    }
}

fn between_range(value: &Expr, min: &Expr, max: &Expr) -> Result<RangeIndexAtom, PlannerError> {
    let Expr::Property(property) = value else {
        return Ok(RangeIndexAtom::NotIndexable);
    };
    let lower = match expr_range_index_value(min)? {
        RangeIndexValueAtom::Value(value) => IndexBound::Inclusive(value),
        RangeIndexValueAtom::NotIndexable => return Ok(RangeIndexAtom::NotIndexable),
    };
    let upper = match expr_range_index_value(max)? {
        RangeIndexValueAtom::Value(value) => IndexBound::Inclusive(value),
        RangeIndexValueAtom::NotIndexable => return Ok(RangeIndexAtom::NotIndexable),
    };
    let Some(range) = IndexBetweenRange::new(lower, upper).map(IndexRange::Between) else {
        return Ok(RangeIndexAtom::NotIndexable);
    };
    Ok(RangeIndexAtom::Atom {
        property: property.clone(),
        range,
    })
}

fn range_from_compare(
    left: &Expr,
    right: &Expr,
    left_property_bound: PropertyBoundSide,
    inclusivity: BoundInclusivity,
) -> Result<RangeIndexAtom, PlannerError> {
    if let Expr::Property(property) = left {
        let RangeIndexValueAtom::Value(value) = expr_range_index_value(right)? else {
            return Ok(RangeIndexAtom::NotIndexable);
        };
        let bound = bound(value, inclusivity);
        return Ok(RangeIndexAtom::Atom {
            property: property.clone(),
            range: match left_property_bound {
                PropertyBoundSide::LowerWhenLeft => IndexRange::Lower { lower: bound },
                PropertyBoundSide::UpperWhenLeft => IndexRange::Upper { upper: bound },
            },
        });
    }
    if let Expr::Property(property) = right {
        let RangeIndexValueAtom::Value(value) = expr_range_index_value(left)? else {
            return Ok(RangeIndexAtom::NotIndexable);
        };
        let bound = bound(value, inclusivity);
        return Ok(RangeIndexAtom::Atom {
            property: property.clone(),
            range: match left_property_bound {
                PropertyBoundSide::LowerWhenLeft => IndexRange::Upper { upper: bound },
                PropertyBoundSide::UpperWhenLeft => IndexRange::Lower { lower: bound },
            },
        });
    }
    Ok(RangeIndexAtom::NotIndexable)
}

fn bound(value: crate::ir::RangeIndexValue, inclusivity: BoundInclusivity) -> IndexBound {
    match inclusivity {
        BoundInclusivity::Inclusive => IndexBound::Inclusive(value),
        BoundInclusivity::Exclusive => IndexBound::Exclusive(value),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropertyBoundSide {
    LowerWhenLeft,
    UpperWhenLeft,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::RangeIndexValue;
    use helix_ast::value::PropertyValue;

    fn int(value: i64) -> Expr {
        Expr::Constant(PropertyValue::from(value))
    }

    #[test]
    fn reversed_comparisons_flip_range_bound_side() {
        assert_eq!(
            range_atom(&Predicate::Compare {
                left: int(18),
                op: CompareOp::Gt,
                right: Expr::Property("age".to_owned()),
            })
            .unwrap(),
            RangeIndexAtom::Atom {
                property: "age".to_owned(),
                range: IndexRange::Upper {
                    upper: IndexBound::Exclusive(
                        RangeIndexValue::literal(PropertyValue::from(18)).unwrap(),
                    ),
                },
            }
        );
        assert_eq!(
            range_atom(&Predicate::Compare {
                left: int(18),
                op: CompareOp::Lte,
                right: Expr::Property("age".to_owned()),
            })
            .unwrap(),
            RangeIndexAtom::Atom {
                property: "age".to_owned(),
                range: IndexRange::Lower {
                    lower: IndexBound::Inclusive(
                        RangeIndexValue::literal(PropertyValue::from(18)).unwrap(),
                    ),
                },
            }
        );
    }

    #[test]
    fn between_rejects_inverted_static_ranges_without_error() {
        assert_eq!(
            range_atom(&Predicate::Between {
                value: Expr::Property("age".to_owned()),
                min: int(40),
                max: int(18),
            })
            .unwrap(),
            RangeIndexAtom::NotIndexable
        );
    }
}
