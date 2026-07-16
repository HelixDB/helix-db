//! Node source access physical-contract adapter.

use crate::{catalog, context, cost, exec, ir, properties};

use super::super::contract::AccessPhysicalContract;
use super::shared;

pub(in crate::rules) fn node_access_contract(
    plan: &ir::NodeAccessPlan,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> AccessPhysicalContract {
    shared::access_contract::<NodeAccessFamily>(plan, storage, stats)
}

struct NodeAccessFamily;

impl shared::AccessSourceFamily for NodeAccessFamily {
    type Plan = ir::NodeAccessPlan;

    fn element() -> properties::ElementKind {
        properties::ElementKind::Node
    }

    fn point_keyspace() -> exec::ElementKeyspace {
        exec::ElementKeyspace::NodeProperty
    }

    fn all_scan_keyspace() -> exec::ElementKeyspace {
        exec::ElementKeyspace::NodeProperty
    }

    fn source_parts(plan: &Self::Plan) -> shared::AccessSourceParts<'_, Self::Plan> {
        match plan {
            ir::NodeAccessPlan::Empty => shared::AccessSourceParts::Empty,
            ir::NodeAccessPlan::PointIds { ids } => shared::AccessSourceParts::PointIds(ids),
            ir::NodeAccessPlan::FromParam { .. } | ir::NodeAccessPlan::FromVar { .. } => {
                shared::AccessSourceParts::RuntimeInput
            }
            ir::NodeAccessPlan::AllScan => shared::AccessSourceParts::AllScan,
            ir::NodeAccessPlan::LabelScan { label } => {
                shared::AccessSourceParts::LabelScan { label }
            }
            ir::NodeAccessPlan::EqualityIndex { index, key, .. } => {
                shared::AccessSourceParts::EqualityIndex {
                    key,
                    kind: match index.uniqueness {
                        catalog::IndexUniqueness::Unique => shared::EqualityIndexKind::Unique,
                        catalog::IndexUniqueness::NonUnique => shared::EqualityIndexKind::NonUnique,
                    },
                }
            }
            ir::NodeAccessPlan::RangeIndex { key, .. } => {
                shared::AccessSourceParts::RangeIndex { key }
            }
            ir::NodeAccessPlan::VectorSearch { k, .. } => {
                shared::AccessSourceParts::VectorSearch { k }
            }
            ir::NodeAccessPlan::TextSearch { k, .. } => shared::AccessSourceParts::TextSearch { k },
            ir::NodeAccessPlan::Intersect(plans) => shared::AccessSourceParts::Intersect(
                plans.iter().map(|plan| plan.as_ref()).collect(),
            ),
            ir::NodeAccessPlan::Union(plans) => {
                shared::AccessSourceParts::Union(plans.iter().map(|plan| plan.as_ref()).collect())
            }
            ir::NodeAccessPlan::ScanThenFilter {
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
        stats.node_label_cardinality.get(label).copied()
    }

    fn equality_cardinality(
        stats: &context::StatsSnapshot,
        key: &catalog::ScopedPropertyKey,
    ) -> Option<u64> {
        stats.node_eq_cardinality.get(key).copied()
    }

    fn range_cardinality(
        stats: &context::StatsSnapshot,
        key: &catalog::ScopedPropertyDirectionKey,
    ) -> Option<u64> {
        stats.node_range_cardinality.get(key).copied()
    }
}
