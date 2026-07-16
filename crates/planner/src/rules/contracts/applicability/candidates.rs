//! Candidate predicates for scheduler-specific rule applicability.
//!
//! These helpers are the shared contract between rule metadata, focused rule
//! tests, and the compiled optimizer schedule. Keeping them outside the enum
//! definition makes it clear which predicates are shape prefilters rather than
//! rule implementations.

use super::super::super::access;
use crate::{analysis, ir, logical};

pub(crate) fn access_path_has_set_canonicalization_candidate(access: &logical::AccessPath) -> bool {
    access.has_set_canonicalization_candidate()
}

pub(crate) fn access_path_has_set_subsumption_candidate(access: &logical::AccessPath) -> bool {
    access.has_set_subsumption_candidate()
}

pub(crate) fn access_path_has_range_intersection_candidate(
    access_path: &logical::AccessPath,
) -> bool {
    access::access_path_has_range_intersection_proof_candidate(access_path)
}

pub(crate) fn access_path_has_equality_range_intersection_candidate(
    access_path: &logical::AccessPath,
) -> bool {
    access::access_path_has_equality_range_intersection_proof_candidate(access_path)
}

pub(crate) fn access_path_has_equality_range_union_candidate(
    access_path: &logical::AccessPath,
) -> bool {
    access::access_path_has_equality_range_union_proof_candidate(access_path)
}

pub(crate) fn access_path_has_contradiction_candidate(access_path: &logical::AccessPath) -> bool {
    access::access_path_has_contradiction_proof_candidate(access_path)
}

pub(crate) fn root_branch_has_empty_input(branch: &logical::RootBranch) -> bool {
    logical_expr_is_direct_empty_access(branch.input())
}

pub(crate) fn root_repeat_has_empty_input(repeat: &logical::RootRepeat) -> bool {
    logical_expr_is_direct_empty_access(repeat.input())
}

fn logical_expr_is_direct_empty_access(expr: &logical::LogicalExpr) -> bool {
    matches!(expr, logical::LogicalExpr::AccessPath(access) if access.is_direct_empty())
}

pub(crate) fn access_filter_has_simplification_candidate(filter: &logical::AccessFilter) -> bool {
    let predicate = filter.predicate().as_ref();
    filter.access().is_direct_empty()
        || analysis::predicate_is_statically_tautological(predicate)
        || analysis::scalar_property_conjunction_is_impossible(predicate)
        || filter.access().common_label().is_some_and(|access_label| {
            analysis::predicate_is_tautological_for_label(predicate, access_label)
                || matches!(
                    analysis::label_scope(predicate),
                    Ok(analysis::LabelScope::Impossible)
                        | Ok(analysis::LabelScope::Feasible(
                            analysis::FeasibleLabelScope::Scoped(_)
                        ))
                )
        })
}

pub(crate) fn access_filter_has_index_candidate(filter: &logical::AccessFilter) -> bool {
    let Ok(analysis::PrunedPredicate::Feasible { predicate, label }) =
        analysis::prune_statically_impossible_branches(filter.predicate().as_ref())
    else {
        return false;
    };
    access_filter_has_usable_label(filter.access().common_label(), &label)
        && analysis::predicate_has_index_atom_candidate(&predicate)
}

fn access_filter_has_usable_label(
    access_label: Option<&ir::NonEmptyString>,
    predicate_label: &analysis::FeasibleLabelScope,
) -> bool {
    match (access_label, predicate_label) {
        (Some(access_label), analysis::FeasibleLabelScope::Scoped(predicate_label)) => {
            access_label == predicate_label
        }
        (Some(_), analysis::FeasibleLabelScope::Unscoped)
        | (None, analysis::FeasibleLabelScope::Scoped(_)) => true,
        (None, analysis::FeasibleLabelScope::Unscoped) => false,
    }
}
