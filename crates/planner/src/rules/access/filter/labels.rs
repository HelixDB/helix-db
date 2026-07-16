use crate::{analysis, ir};

pub(super) fn access_filter_label(
    access_label: Option<&ir::NonEmptyString>,
    predicate_label: &analysis::FeasibleLabelScope,
) -> Option<ir::NonEmptyString> {
    match (access_label, predicate_label) {
        (Some(access_label), analysis::FeasibleLabelScope::Scoped(predicate_label))
            if access_label == predicate_label =>
        {
            Some(access_label.clone())
        }
        (Some(_), analysis::FeasibleLabelScope::Scoped(_)) => None,
        (Some(access_label), analysis::FeasibleLabelScope::Unscoped) => Some(access_label.clone()),
        (None, analysis::FeasibleLabelScope::Scoped(predicate_label)) => {
            Some(predicate_label.clone())
        }
        (None, analysis::FeasibleLabelScope::Unscoped) => None,
    }
}

pub(super) fn label_equality_matches(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
) -> bool {
    analysis::label_equality_atom(predicate).as_deref() == Some(label.as_ref())
}
