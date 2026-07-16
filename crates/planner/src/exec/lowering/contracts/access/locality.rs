//! Access key-locality inference.

use crate::{ir, properties};

pub(super) fn access_key_locality_from_node_access(
    plan: &ir::NodeAccessPlan,
) -> properties::KeyLocality {
    match plan {
        ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::EqualityIndex { .. }
        | ir::NodeAccessPlan::RangeIndex { .. } => properties::KeyLocality::Close,
        ir::NodeAccessPlan::Empty
        | ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::VectorSearch { .. }
        | ir::NodeAccessPlan::TextSearch { .. }
        | ir::NodeAccessPlan::PointIds { .. }
        | ir::NodeAccessPlan::Intersect(_)
        | ir::NodeAccessPlan::Union(_)
        | ir::NodeAccessPlan::ScanThenFilter { .. } => properties::KeyLocality::Unknown,
    }
}

pub(super) fn access_key_locality_from_edge_access(
    plan: &ir::EdgeAccessPlan,
) -> properties::KeyLocality {
    match plan {
        ir::EdgeAccessPlan::LabelScan { .. }
        | ir::EdgeAccessPlan::EqualityIndex { .. }
        | ir::EdgeAccessPlan::RangeIndex { .. } => properties::KeyLocality::Close,
        ir::EdgeAccessPlan::Empty
        | ir::EdgeAccessPlan::FromParam { .. }
        | ir::EdgeAccessPlan::FromVar { .. }
        | ir::EdgeAccessPlan::AllScan
        | ir::EdgeAccessPlan::VectorSearch { .. }
        | ir::EdgeAccessPlan::TextSearch { .. }
        | ir::EdgeAccessPlan::PointIds { .. }
        | ir::EdgeAccessPlan::Intersect(_)
        | ir::EdgeAccessPlan::Union(_)
        | ir::EdgeAccessPlan::ScanThenFilter { .. } => properties::KeyLocality::Unknown,
    }
}
