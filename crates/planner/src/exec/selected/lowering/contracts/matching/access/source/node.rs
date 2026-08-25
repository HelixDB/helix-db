//! Selected node access-source matching.

use crate::{exec, ir, physical};

use super::{SelectedAccessShapeMatch, SelectedAccessShapeMismatch};

pub(in crate::exec::selected::lowering) fn selected_node_access_matches(
    plan: &ir::NodeAccessPlan,
    access: &physical::PhysicalAccess,
) -> bool {
    selected_node_access_match(plan, access).is_matched()
}

pub(super) fn selected_node_access_match(
    plan: &ir::NodeAccessPlan,
    access: &physical::PhysicalAccess,
) -> SelectedAccessShapeMatch {
    match (plan, access) {
        (
            ir::NodeAccessPlan::EqualityIndex { index, key, value },
            physical::PhysicalAccess::NodeExact(exact),
        ) if exact.as_ref()
            == &exec::ExecNodeAccessPlan::exact_equality(
                index.clone(),
                key.clone(),
                value.clone(),
            ) =>
        {
            SelectedAccessShapeMatch::Matched
        }
        (
            ir::NodeAccessPlan::Union(_) | ir::NodeAccessPlan::Intersect(_),
            physical::PhysicalAccess::NodeExact(exact),
        ) if exec::node_secondary_set(plan).is_some_and(|set| {
            exact.as_ref() == &exec::ExecNodeAccessPlan::SecondarySet { set }
        }) =>
        {
            SelectedAccessShapeMatch::Matched
        }
        (ir::NodeAccessPlan::Empty, physical::PhysicalAccess::Empty)
        | (
            ir::NodeAccessPlan::FromParam { .. } | ir::NodeAccessPlan::FromVar { .. },
            physical::PhysicalAccess::RuntimeInput,
        )
        | (ir::NodeAccessPlan::LabelScan { .. }, physical::PhysicalAccess::LabelScan)
        | (ir::NodeAccessPlan::RangeIndex { .. }, physical::PhysicalAccess::RangeIndex)
        | (ir::NodeAccessPlan::VectorSearch { .. }, physical::PhysicalAccess::VectorSearch)
        | (ir::NodeAccessPlan::TextSearch { .. }, physical::PhysicalAccess::TextSearch) => {
            SelectedAccessShapeMatch::Matched
        }
        (ir::NodeAccessPlan::Intersect(_), physical::PhysicalAccess::SetIntersection)
            if exec::node_secondary_set(plan).is_none() =>
        {
            SelectedAccessShapeMatch::Matched
        }
        (ir::NodeAccessPlan::Union(_), physical::PhysicalAccess::SetUnion)
            if exec::node_secondary_set(plan).is_none() =>
        {
            SelectedAccessShapeMatch::Matched
        }
        (
            ir::NodeAccessPlan::PointIds { .. },
            physical::PhysicalAccess::PointReads { .. } | physical::PhysicalAccess::Kv(_),
        )
        | (ir::NodeAccessPlan::AllScan, physical::PhysicalAccess::Kv(_)) => {
            SelectedAccessShapeMatch::Matched
        }
        (ir::NodeAccessPlan::ScanThenFilter { .. }, _) => SelectedAccessShapeMatch::NotMatched(
            SelectedAccessShapeMismatch::ResidualFilterRequiresPipeline,
        ),
        (
            ir::NodeAccessPlan::Empty
            | ir::NodeAccessPlan::FromParam { .. }
            | ir::NodeAccessPlan::FromVar { .. }
            | ir::NodeAccessPlan::AllScan
            | ir::NodeAccessPlan::PointIds { .. }
            | ir::NodeAccessPlan::LabelScan { .. }
            | ir::NodeAccessPlan::EqualityIndex { .. }
            | ir::NodeAccessPlan::RangeIndex { .. }
            | ir::NodeAccessPlan::VectorSearch { .. }
            | ir::NodeAccessPlan::TextSearch { .. }
            | ir::NodeAccessPlan::Intersect(_)
            | ir::NodeAccessPlan::Union(_),
            _,
        ) => SelectedAccessShapeMatch::NotMatched(
            SelectedAccessShapeMismatch::PhysicalAccessFamilyMismatch,
        ),
    }
}
