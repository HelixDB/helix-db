use crate::{ir, logical};

/// Conversion from a node/edge access plan into a residual-free logical access
/// path.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access) enum AccessPathFromPlan {
    /// The plan is not a residual-free source and must not enter `AccessPath`.
    NotResidualFree,
    /// The plan has been validated as a residual-free logical access path.
    Access(logical::AccessPath),
}

pub(in crate::rules::access) fn empty_access_path_like(
    access: &logical::AccessPath,
) -> logical::AccessPath {
    access.empty_like()
}

pub(in crate::rules::access) fn node_access_path_from_plan(
    plan: ir::NodeAccessPlan,
) -> AccessPathFromPlan {
    ir::NodeAccessSourcePlan::new(plan).map_or(AccessPathFromPlan::NotResidualFree, |source| {
        AccessPathFromPlan::Access(logical::AccessPath::Node(logical::NodeAccessPath::new(
            source,
        )))
    })
}

pub(in crate::rules::access) fn edge_access_path_from_plan(
    plan: ir::EdgeAccessPlan,
) -> AccessPathFromPlan {
    ir::EdgeAccessSourcePlan::new(plan).map_or(AccessPathFromPlan::NotResidualFree, |source| {
        AccessPathFromPlan::Access(logical::AccessPath::Edge(logical::EdgeAccessPath::new(
            source,
        )))
    })
}

pub(in crate::rules::access) fn access_path_is_direct_empty(access: &logical::AccessPath) -> bool {
    access.is_direct_empty()
}
