//! Literal `IN` index-plan extraction.

use super::limits::max_index_union_branches;
use super::property::{access_index_property, AccessIndexProperty};
use super::types::{
    AccessFilterIndexAtom, AccessFilterIndexAtoms, AccessFilterIndexBranches,
    AccessFilterIndexPlan, AccessFilterIndexPlanMatch, AccessFilterIndexPlanRejection,
};
use crate::{analysis, context, ir};

pub(super) fn literal_in_index_plan(
    predicate: &helix_ast::expr::Predicate,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterIndexPlanMatch {
    let Some((property, values)) = analysis::literal_in_values(predicate) else {
        return AccessFilterIndexPlanMatch::NotIndexable(
            AccessFilterIndexPlanRejection::NotIndexCandidate,
        );
    };
    let property = match access_index_property(property) {
        AccessIndexProperty::Indexable(property) => property,
        AccessIndexProperty::NotIndexable(_) => {
            return AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::PropertyNotIndexable,
            );
        }
    };
    let branches = values
        .into_iter()
        .map(|value| {
            let value = ir::SecondaryIndexLiteral::new(value)
                .map_err(|_| AccessFilterIndexPlanRejection::LiteralValueNotIndexable)?;
            AccessFilterIndexAtoms::new(vec![AccessFilterIndexAtom::Equality {
                property: property.clone(),
                value: ir::IndexValue::Literal(value),
            }])
            .map_err(|_| AccessFilterIndexPlanRejection::EmptyIndexAtoms)
        })
        .collect::<Result<Vec<_>, _>>();
    let mut branches = match branches {
        Ok(branches) => branches,
        Err(reason) => return AccessFilterIndexPlanMatch::NotIndexable(reason),
    };
    match branches.len() {
        0 => AccessFilterIndexPlanMatch::NotIndexable(
            AccessFilterIndexPlanRejection::EmptyIndexAtoms,
        ),
        1 => match branches.pop() {
            Some(branch) => {
                AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Conjunction(branch))
            }
            None => AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::EmptyIndexAtoms,
            ),
        },
        len => match max_index_union_branches(planner_limits) {
            None => AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::BranchLimitDisabled,
            ),
            Some(max_branches) if len <= max_branches => {
                match AccessFilterIndexBranches::new(branches) {
                    Ok(branches) => AccessFilterIndexPlanMatch::Planned(
                        AccessFilterIndexPlan::Disjunction(branches),
                    ),
                    Err(_) => AccessFilterIndexPlanMatch::NotIndexable(
                        AccessFilterIndexPlanRejection::TooFewIndexBranches,
                    ),
                }
            }
            Some(_) => AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::BranchLimitExceeded,
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limited(branches: usize) -> context::PlannerLimits {
        context::PlannerLimits {
            max_index_union_branches: context::IndexUnionBranchLimit::limited(branches).unwrap(),
        }
    }

    #[test]
    fn literal_in_plan_uses_singleton_conjunction_before_branch_limits() {
        let predicate = helix_ast::expr::Predicate::is_in(
            "age",
            helix_ast::value::PropertyValue::I64Array(vec![42]),
        );
        assert!(matches!(
            literal_in_index_plan(&predicate, &limited(1)),
            AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Conjunction(_))
        ));
    }

    #[test]
    fn literal_in_plan_respects_union_branch_limit_and_property_contracts() {
        let predicate = helix_ast::expr::Predicate::is_in(
            "age",
            helix_ast::value::PropertyValue::I64Array(vec![1, 2]),
        );
        assert!(matches!(
            literal_in_index_plan(&predicate, &limited(2)),
            AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Disjunction(branches))
                if branches.as_ref().len() == 2
        ));
        assert_eq!(
            literal_in_index_plan(&predicate, &limited(1)),
            AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::BranchLimitExceeded
            )
        );

        let scoped = helix_ast::expr::Predicate::is_in(
            "$label",
            helix_ast::value::PropertyValue::StringArray(vec!["User".to_owned()]),
        );
        assert_eq!(
            literal_in_index_plan(&scoped, &limited(2)),
            AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::PropertyNotIndexable
            )
        );
    }
}
