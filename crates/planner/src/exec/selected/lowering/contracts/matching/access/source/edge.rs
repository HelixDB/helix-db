//! Selected edge access-source matching.

use crate::{ir, physical};

use super::{SelectedAccessShapeMatch, SelectedAccessShapeMismatch};

pub(in crate::exec::selected::lowering) fn selected_edge_access_matches(
    plan: &ir::EdgeAccessPlan,
    access: &physical::PhysicalAccess,
) -> bool {
    selected_edge_access_match(plan, access).is_matched()
}

pub(super) fn selected_edge_access_match(
    plan: &ir::EdgeAccessPlan,
    access: &physical::PhysicalAccess,
) -> SelectedAccessShapeMatch {
    match (plan, access) {
        (ir::EdgeAccessPlan::Empty, physical::PhysicalAccess::Empty)
        | (
            ir::EdgeAccessPlan::FromParam { .. } | ir::EdgeAccessPlan::FromVar { .. },
            physical::PhysicalAccess::RuntimeInput,
        )
        | (ir::EdgeAccessPlan::LabelScan { .. }, physical::PhysicalAccess::LabelScan)
        | (ir::EdgeAccessPlan::EqualityIndex { .. }, physical::PhysicalAccess::EqualityIndex)
        | (ir::EdgeAccessPlan::RangeIndex { .. }, physical::PhysicalAccess::RangeIndex)
        | (ir::EdgeAccessPlan::VectorSearch { .. }, physical::PhysicalAccess::VectorSearch)
        | (ir::EdgeAccessPlan::TextSearch { .. }, physical::PhysicalAccess::TextSearch)
        | (ir::EdgeAccessPlan::Intersect(_), physical::PhysicalAccess::SetIntersection)
        | (ir::EdgeAccessPlan::Union(_), physical::PhysicalAccess::SetUnion) => {
            SelectedAccessShapeMatch::Matched
        }
        (
            ir::EdgeAccessPlan::PointIds { .. },
            physical::PhysicalAccess::PointReads { .. } | physical::PhysicalAccess::Kv(_),
        )
        | (ir::EdgeAccessPlan::AllScan, physical::PhysicalAccess::Kv(_)) => {
            SelectedAccessShapeMatch::Matched
        }
        (ir::EdgeAccessPlan::ScanThenFilter { .. }, _) => SelectedAccessShapeMatch::NotMatched(
            SelectedAccessShapeMismatch::ResidualFilterRequiresPipeline,
        ),
        (
            ir::EdgeAccessPlan::Empty
            | ir::EdgeAccessPlan::FromParam { .. }
            | ir::EdgeAccessPlan::FromVar { .. }
            | ir::EdgeAccessPlan::AllScan
            | ir::EdgeAccessPlan::PointIds { .. }
            | ir::EdgeAccessPlan::LabelScan { .. }
            | ir::EdgeAccessPlan::EqualityIndex { .. }
            | ir::EdgeAccessPlan::RangeIndex { .. }
            | ir::EdgeAccessPlan::VectorSearch { .. }
            | ir::EdgeAccessPlan::TextSearch { .. }
            | ir::EdgeAccessPlan::Intersect(_)
            | ir::EdgeAccessPlan::Union(_),
            _,
        ) => SelectedAccessShapeMatch::NotMatched(
            SelectedAccessShapeMismatch::PhysicalAccessFamilyMismatch,
        ),
    }
}
