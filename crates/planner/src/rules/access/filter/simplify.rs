use super::super::sources::{
    access_path_common_label, access_path_is_direct_empty, empty_access_path_like,
};
use super::AccessFilterRewrite;
use crate::{analysis, ir, logical};

pub(in crate::rules) fn simplify_access_filter(
    filter: &logical::AccessFilter,
) -> AccessFilterRewrite {
    if access_path_is_direct_empty(filter.access())
        || analysis::predicate_is_statically_tautological(filter.predicate().as_ref())
    {
        AccessFilterRewrite::Rewritten(filter.access().clone())
    } else if analysis::scalar_property_conjunction_is_impossible(filter.predicate().as_ref()) {
        AccessFilterRewrite::Rewritten(empty_access_path_like(filter.access()))
    } else if let Some(access_label) = access_path_common_label(filter.access()) {
        simplify_access_filter_for_label(filter, access_label)
    } else {
        AccessFilterRewrite::NotApplicable
    }
}

fn simplify_access_filter_for_label(
    filter: &logical::AccessFilter,
    access_label: &ir::NonEmptyString,
) -> AccessFilterRewrite {
    if predicate_is_tautological_for_label(filter.predicate().as_ref(), access_label) {
        return AccessFilterRewrite::Rewritten(filter.access().clone());
    }
    match analysis::label_scope(filter.predicate().as_ref()) {
        Ok(analysis::LabelScope::Impossible) => {
            AccessFilterRewrite::Rewritten(empty_access_path_like(filter.access()))
        }
        Ok(analysis::LabelScope::Feasible(analysis::FeasibleLabelScope::Scoped(
            predicate_label,
        ))) if predicate_label != *access_label => {
            AccessFilterRewrite::Rewritten(empty_access_path_like(filter.access()))
        }
        Ok(analysis::LabelScope::Feasible(
            analysis::FeasibleLabelScope::Scoped(_) | analysis::FeasibleLabelScope::Unscoped,
        ))
        | Err(_) => AccessFilterRewrite::NotApplicable,
    }
}

fn predicate_is_tautological_for_label(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
) -> bool {
    analysis::predicate_is_tautological_for_label(predicate, label)
}
