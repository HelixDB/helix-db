//! Predicate-analysis contract facade.
//!
//! This module is intentionally small. The planner consumes predicate facts
//! through stable contract functions while the analysis internals live in
//! focused modules:
//!
//! - `labels`: label-scope proofs.
//! - `prune`: static branch pruning.
//! - `scalar`: scalar tautology/contradiction and finite literal-set proofs.
//! - `index_atoms`: equality and range atoms suitable for secondary indexes.

mod index_atoms;
mod labels;
mod prune;
mod scalar;

#[cfg(test)]
mod tests;

pub(crate) use self::index_atoms::{equality_atom, range_atom, EqualityIndexAtom, RangeIndexAtom};
pub(crate) use self::labels::{label_equality_atom, label_scope, FeasibleLabelScope, LabelScope};
pub(crate) use self::prune::{prune_statically_impossible_branches, PrunedPredicate};
pub(crate) use self::scalar::{
    literal_in_values, predicate_is_statically_tautological,
    scalar_property_conjunction_is_impossible,
};

/// Whether a predicate is tautological for rows whose label is already known.
///
/// This is a pure predicate-shape proof used by access-filter simplification
/// scheduling and the simplification rule itself. It intentionally does not
/// inspect catalog metadata or storage state.
pub(crate) fn predicate_is_tautological_for_label(
    predicate: &helix_ast::expr::Predicate,
    label: &crate::ir::NonEmptyString,
) -> bool {
    predicate_is_statically_tautological(predicate)
        || label_equality_atom(predicate).as_deref() == Some(label.as_ref())
        || match predicate {
            helix_ast::expr::Predicate::And { predicates } => predicates
                .iter()
                .all(|predicate| predicate_is_tautological_for_label(predicate, label)),
            helix_ast::expr::Predicate::Or { predicates } => predicates
                .iter()
                .any(|predicate| predicate_is_tautological_for_label(predicate, label)),
            helix_ast::expr::Predicate::Eq { .. }
            | helix_ast::expr::Predicate::Neq { .. }
            | helix_ast::expr::Predicate::Gt { .. }
            | helix_ast::expr::Predicate::Gte { .. }
            | helix_ast::expr::Predicate::Lt { .. }
            | helix_ast::expr::Predicate::Lte { .. }
            | helix_ast::expr::Predicate::Between { .. }
            | helix_ast::expr::Predicate::HasKey { .. }
            | helix_ast::expr::Predicate::IsNull { .. }
            | helix_ast::expr::Predicate::IsNotNull { .. }
            | helix_ast::expr::Predicate::StartsWith { .. }
            | helix_ast::expr::Predicate::EndsWith { .. }
            | helix_ast::expr::Predicate::Contains { .. }
            | helix_ast::expr::Predicate::IsIn { .. }
            | helix_ast::expr::Predicate::Not { .. }
            | helix_ast::expr::Predicate::Compare { .. } => false,
        }
}

/// Whether a predicate contains at least one atom that could be backed by an
/// equality/range secondary index after catalog lookup.
///
/// This is a necessary-shape check for optimizer scheduling only. The access
/// filter index rule remains the authoritative boundary for property
/// validation, branch limits, catalog lookup, and full predicate coverage.
pub(crate) fn predicate_has_index_atom_candidate(predicate: &helix_ast::expr::Predicate) -> bool {
    if literal_in_values(predicate).is_some() {
        return true;
    }
    match predicate {
        helix_ast::expr::Predicate::And { predicates }
        | helix_ast::expr::Predicate::Or { predicates } => {
            predicates.iter().any(predicate_has_index_atom_candidate)
        }
        predicate => {
            matches!(
                equality_atom(predicate),
                Ok(EqualityIndexAtom::Atom { .. }) | Err(_)
            ) || matches!(
                range_atom(predicate),
                Ok(RangeIndexAtom::Atom { .. }) | Err(_)
            )
        }
    }
}

#[cfg(test)]
mod access_filter_candidate_tests {
    use super::*;
    use crate::ir;

    fn user_label() -> ir::NonEmptyString {
        ir::NonEmptyString::new("User").unwrap()
    }

    #[test]
    fn predicate_tautology_for_label_covers_boolean_and_label_shapes() {
        assert!(predicate_is_tautological_for_label(
            &helix_ast::expr::Predicate::eq("$label", "User"),
            &user_label()
        ));
        assert!(predicate_is_tautological_for_label(
            &helix_ast::expr::Predicate::or(vec![
                helix_ast::expr::Predicate::eq("$label", "User"),
                helix_ast::expr::Predicate::contains("bio", "rust"),
            ]),
            &user_label()
        ));
        assert!(!predicate_is_tautological_for_label(
            &helix_ast::expr::Predicate::and(vec![
                helix_ast::expr::Predicate::eq("$label", "User"),
                helix_ast::expr::Predicate::contains("bio", "rust"),
            ]),
            &user_label()
        ));
    }

    #[test]
    fn predicate_index_atom_candidate_tracks_secondary_index_shapes() {
        assert!(predicate_has_index_atom_candidate(
            &helix_ast::expr::Predicate::eq("age", 42)
        ));
        assert!(predicate_has_index_atom_candidate(
            &helix_ast::expr::Predicate::gte("age", 21)
        ));
        assert!(predicate_has_index_atom_candidate(
            &helix_ast::expr::Predicate::is_in(
                "age",
                helix_ast::value::PropertyValue::I64Array(vec![1, 2])
            )
        ));
        assert!(predicate_has_index_atom_candidate(
            &helix_ast::expr::Predicate::or(vec![
                helix_ast::expr::Predicate::contains("bio", "rust"),
                helix_ast::expr::Predicate::eq("age", 42),
            ])
        ));
        assert!(!predicate_has_index_atom_candidate(
            &helix_ast::expr::Predicate::contains("bio", "rust")
        ));
    }
}
