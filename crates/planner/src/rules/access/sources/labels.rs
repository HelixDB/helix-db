use crate::{ir, logical};

pub(in crate::rules::access) fn access_path_common_label(
    access: &logical::AccessPath,
) -> Option<&ir::NonEmptyString> {
    access.common_label()
}

pub(in crate::rules::access) fn node_source_common_label(
    source: &ir::NodeAccessSourcePlan,
) -> Option<&ir::NonEmptyString> {
    source.common_label()
}

pub(in crate::rules::access) fn edge_source_common_label(
    source: &ir::EdgeAccessSourcePlan,
) -> Option<&ir::NonEmptyString> {
    source.common_label()
}
