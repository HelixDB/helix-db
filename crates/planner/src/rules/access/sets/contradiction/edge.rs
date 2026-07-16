//! Edge access contradiction proofs.

use super::super::*;

pub(super) fn has_static_contradiction(plan: &ir::EdgeAccessPlan) -> bool {
    match plan {
        ir::EdgeAccessPlan::Intersect(plans) => plans.iter().enumerate().any(|(index, left)| {
            is_statically_empty(left.as_ref())
                || plans
                    .iter()
                    .skip(index + 1)
                    .any(|right| plans_contradict(left.as_ref(), right.as_ref()))
        }),
        _ => false,
    }
}

fn is_statically_empty(plan: &ir::EdgeAccessPlan) -> bool {
    match plan {
        ir::EdgeAccessPlan::Empty => true,
        ir::EdgeAccessPlan::Intersect(_) => has_static_contradiction(plan),
        ir::EdgeAccessPlan::Union(plans) => plans
            .iter()
            .all(|child| is_statically_empty(child.as_ref())),
        _ => false,
    }
}

fn plans_contradict(left: &ir::EdgeAccessPlan, right: &ir::EdgeAccessPlan) -> bool {
    if is_statically_empty(left) || is_statically_empty(right) {
        return true;
    }
    match (left, right) {
        (ir::EdgeAccessPlan::Union(children), other) => children
            .iter()
            .all(|child| plans_contradict(child.as_ref(), other)),
        (other, ir::EdgeAccessPlan::Union(children)) => children
            .iter()
            .all(|child| plans_contradict(other, child.as_ref())),
        (ir::EdgeAccessPlan::Intersect(children), other) => children
            .iter()
            .any(|child| plans_contradict(child.as_ref(), other)),
        (other, ir::EdgeAccessPlan::Intersect(children)) => children
            .iter()
            .any(|child| plans_contradict(other, child.as_ref())),
        _ => {
            literal_equalities_conflict(left, right)
                || literal_equality_range_conflict(left, right)
                || literal_equality_range_conflict(right, left)
        }
    }
}

fn literal_equalities_conflict(left: &ir::EdgeAccessPlan, right: &ir::EdgeAccessPlan) -> bool {
    let Some((left_key, left_value)) = literal_equality_parts(left) else {
        return false;
    };
    let Some((right_key, right_value)) = literal_equality_parts(right) else {
        return false;
    };
    left_key == right_key && left_value != right_value
}

fn literal_equality_range_conflict(
    equality: &ir::EdgeAccessPlan,
    range: &ir::EdgeAccessPlan,
) -> bool {
    let Some((equality_key, value)) = literal_equality_parts(equality) else {
        return false;
    };
    let Some((range_key, range)) = range_index_parts(range) else {
        return false;
    };
    range_key.label == equality_key.label
        && range_key.property == equality_key.property
        && range.excludes_secondary_literal(value)
}

fn literal_equality_parts(
    plan: &ir::EdgeAccessPlan,
) -> Option<(&catalog::ScopedPropertyKey, &ir::SecondaryIndexLiteral)> {
    match plan {
        ir::EdgeAccessPlan::EqualityIndex {
            key,
            value: ir::IndexValue::Literal(value),
            ..
        } => Some((key, value)),
        _ => None,
    }
}

fn range_index_parts(
    plan: &ir::EdgeAccessPlan,
) -> Option<(&catalog::ScopedPropertyDirectionKey, &ir::IndexRange)> {
    match plan {
        ir::EdgeAccessPlan::RangeIndex { key, range, .. } => Some((key, range)),
        _ => None,
    }
}
