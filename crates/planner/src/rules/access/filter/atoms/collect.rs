//! Predicate-to-index-atom collection.

use super::property::{access_index_property, AccessIndexProperty};
use super::types::{
    AccessEqualityDomain, AccessFilterIndexAtom, AccessFilterIndexAtoms,
    AccessFilterIndexPlanRejection,
};
use crate::{analysis, ir};

pub(super) fn access_filter_index_atoms(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    planner_limits: &crate::context::PlannerLimits,
) -> Result<AccessFilterIndexAtoms, AccessFilterIndexPlanRejection> {
    let mut atoms = Vec::new();
    collect_access_filter_index_atoms(predicate, label, planner_limits, &mut atoms)?;
    AccessFilterIndexAtoms::new(atoms).map_err(|_| AccessFilterIndexPlanRejection::EmptyIndexAtoms)
}

fn collect_access_filter_index_atoms(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    planner_limits: &crate::context::PlannerLimits,
    atoms: &mut Vec<AccessFilterIndexAtom>,
) -> Result<(), AccessFilterIndexPlanRejection> {
    if super::super::labels::label_equality_matches(predicate, label) {
        return Ok(());
    }
    if analysis::label_equality_atom(predicate).is_some() {
        return Err(AccessFilterIndexPlanRejection::LabelScopeMismatch);
    }
    match predicate {
        helix_ast::expr::Predicate::And { predicates } => {
            predicates.iter().try_for_each(|predicate| {
                collect_access_filter_index_atoms(predicate, label, planner_limits, atoms)
            })
        }
        predicate => {
            let atom = access_filter_index_atom(predicate, planner_limits)?;
            atoms.push(atom);
            Ok(())
        }
    }
}

fn access_filter_index_atom(
    predicate: &helix_ast::expr::Predicate,
    planner_limits: &crate::context::PlannerLimits,
) -> Result<AccessFilterIndexAtom, AccessFilterIndexPlanRejection> {
    if let Ok(analysis::EqualityIndexAtom::Atom { property, domain }) =
        analysis::equality_atom(predicate)
    {
        let domain = match domain {
            analysis::EqualityIndexDomain::Empty => {
                return Err(AccessFilterIndexPlanRejection::EmptyIndexAtoms);
            }
            analysis::EqualityIndexDomain::One(value) => AccessEqualityDomain::One(value),
            analysis::EqualityIndexDomain::Many(values) => {
                let Some(max_branches) = super::limits::max_index_union_branches(planner_limits)
                else {
                    return Err(AccessFilterIndexPlanRejection::BranchLimitDisabled);
                };
                if values.len() > max_branches {
                    return Err(AccessFilterIndexPlanRejection::BranchLimitExceeded);
                }
                AccessEqualityDomain::Many(values)
            }
            analysis::EqualityIndexDomain::RuntimeSet(param) => {
                let Some(max_branches) = super::limits::max_index_union_branches(planner_limits)
                else {
                    return Err(AccessFilterIndexPlanRejection::BranchLimitDisabled);
                };
                AccessEqualityDomain::Runtime(ir::RuntimeEqualitySet::new(
                    param,
                    std::num::NonZeroUsize::new(max_branches)
                        .expect("enabled index-union limit is positive"),
                ))
            }
        };
        return match access_index_property(property) {
            AccessIndexProperty::Indexable(property) => {
                Ok(AccessFilterIndexAtom::Equality { property, domain })
            }
            AccessIndexProperty::NotIndexable(_) => {
                Err(AccessFilterIndexPlanRejection::PropertyNotIndexable)
            }
        };
    }
    if let Ok(analysis::RangeIndexAtom::Atom { property, range }) = analysis::range_atom(predicate)
    {
        return match access_index_property(property) {
            AccessIndexProperty::Indexable(property) => {
                Ok(AccessFilterIndexAtom::Range { property, range })
            }
            AccessIndexProperty::NotIndexable(_) => {
                Err(AccessFilterIndexPlanRejection::PropertyNotIndexable)
            }
        };
    }
    Err(AccessFilterIndexPlanRejection::NotIndexCandidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user_label() -> ir::NonEmptyString {
        ir::NonEmptyString::new("User").unwrap()
    }

    #[test]
    fn collection_ignores_matching_label_scope_and_keeps_index_atoms() {
        let predicate = helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("$label", "User"),
            helix_ast::expr::Predicate::gte("age", 21),
        ]);

        let atoms = access_filter_index_atoms(
            &predicate,
            &user_label(),
            &crate::context::PlannerLimits::default(),
        )
        .unwrap();

        assert!(matches!(
            atoms.as_ref(),
            [AccessFilterIndexAtom::Range { property, .. }] if property.as_ref() == "age"
        ));
    }

    #[test]
    fn collection_reports_label_scope_property_and_candidate_rejections() {
        let conflicting_label = helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("$label", "Admin"),
            helix_ast::expr::Predicate::gte("age", 21),
        ]);
        assert_eq!(
            access_filter_index_atoms(
                &conflicting_label,
                &user_label(),
                &crate::context::PlannerLimits::default(),
            ),
            Err(AccessFilterIndexPlanRejection::LabelScopeMismatch)
        );

        let dotted_property = helix_ast::expr::Predicate::eq("profile.age", 21);
        assert_eq!(
            access_filter_index_atoms(
                &dotted_property,
                &user_label(),
                &crate::context::PlannerLimits::default(),
            ),
            Err(AccessFilterIndexPlanRejection::PropertyNotIndexable)
        );

        let not_candidate = helix_ast::expr::Predicate::contains("bio", "rust");
        assert_eq!(
            access_filter_index_atoms(
                &not_candidate,
                &user_label(),
                &crate::context::PlannerLimits::default(),
            ),
            Err(AccessFilterIndexPlanRejection::NotIndexCandidate)
        );
    }
}
