//! Edge source access physical-contract adapter.

use crate::{catalog, context, cost, exec, ir, properties};

use super::super::contract::AccessPhysicalContract;
use super::shared;

pub(in crate::rules) fn edge_access_contract(
    plan: &ir::EdgeAccessPlan,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> AccessPhysicalContract {
    shared::access_contract::<EdgeAccessFamily>(plan, storage, stats)
}

struct EdgeAccessFamily;

impl shared::AccessSourceFamily for EdgeAccessFamily {
    type Plan = ir::EdgeAccessPlan;

    fn element() -> properties::ElementKind {
        properties::ElementKind::Edge
    }

    fn point_keyspace() -> exec::ElementKeyspace {
        exec::ElementKeyspace::EdgeEndpoints
    }

    fn all_scan_keyspace() -> exec::ElementKeyspace {
        exec::ElementKeyspace::EdgeEndpoints
    }

    fn source_parts(plan: &Self::Plan) -> shared::AccessSourceParts<'_, Self::Plan> {
        match plan {
            ir::EdgeAccessPlan::Empty => shared::AccessSourceParts::Empty,
            ir::EdgeAccessPlan::PointIds { ids } => shared::AccessSourceParts::PointIds(ids),
            ir::EdgeAccessPlan::FromParam { .. } | ir::EdgeAccessPlan::FromVar { .. } => {
                shared::AccessSourceParts::RuntimeInput
            }
            ir::EdgeAccessPlan::AllScan => shared::AccessSourceParts::AllScan,
            ir::EdgeAccessPlan::LabelScan { label } => {
                shared::AccessSourceParts::LabelScan { label }
            }
            ir::EdgeAccessPlan::EqualityIndex { key, .. } => {
                shared::AccessSourceParts::EqualityIndex {
                    key,
                    kind: shared::EqualityIndexKind::NonUnique,
                }
            }
            ir::EdgeAccessPlan::RangeIndex { key, .. } => {
                shared::AccessSourceParts::RangeIndex { key }
            }
            ir::EdgeAccessPlan::VectorSearch { k, .. } => {
                shared::AccessSourceParts::VectorSearch { k }
            }
            ir::EdgeAccessPlan::TextSearch { k, .. } => shared::AccessSourceParts::TextSearch { k },
            ir::EdgeAccessPlan::Intersect(plans) => shared::AccessSourceParts::Intersect(
                plans.iter().map(|plan| plan.as_ref()).collect(),
            ),
            ir::EdgeAccessPlan::Union(plans) => {
                shared::AccessSourceParts::Union(plans.iter().map(|plan| plan.as_ref()).collect())
            }
            ir::EdgeAccessPlan::ScanThenFilter {
                source,
                residual: _,
            } => shared::AccessSourceParts::ScanThenFilter {
                source: source.as_ref(),
            },
        }
    }

    fn label_cardinality(
        stats: &context::StatsSnapshot,
        label: &ir::NonEmptyString,
    ) -> Option<u64> {
        stats.edge_label_cardinality.get(label).copied()
    }

    fn equality_cardinality(
        stats: &context::StatsSnapshot,
        key: &catalog::ScopedPropertyKey,
    ) -> Option<u64> {
        stats.edge_eq_cardinality.get(key).copied()
    }

    fn range_cardinality(
        stats: &context::StatsSnapshot,
        key: &catalog::ScopedPropertyDirectionKey,
    ) -> Option<u64> {
        stats.edge_range_cardinality.get(key).copied()
    }
}
