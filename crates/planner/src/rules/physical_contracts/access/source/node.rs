//! Node source access physical-contract adapter.

use crate::{catalog, context, cost, exec, ir, properties};

use super::super::contract::AccessPhysicalContract;
use super::shared;

pub(in crate::rules) fn node_access_contract(
    plan: &ir::NodeAccessPlan,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> AccessPhysicalContract {
    let contract = shared::access_contract::<NodeAccessFamily>(plan, storage, stats);
    let exact = match plan {
        ir::NodeAccessPlan::Union(_) | ir::NodeAccessPlan::Intersect(_) => {
            exec::node_secondary_set(plan).map(|set| exec::ExecNodeAccessPlan::SecondarySet { set })
        }
        ir::NodeAccessPlan::Empty
        | ir::NodeAccessPlan::PointIds { .. }
        | ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::EqualityIndex { .. }
        | ir::NodeAccessPlan::RangeIndex { .. }
        | ir::NodeAccessPlan::VectorSearch { .. }
        | ir::NodeAccessPlan::TextSearch { .. }
        | ir::NodeAccessPlan::ScanThenFilter { .. } => None,
    };
    match exact {
        Some(exact) => {
            contract.with_access(crate::physical::PhysicalAccess::NodeExact(Box::new(exact)))
        }
        None => contract,
    }
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
            ir::NodeAccessPlan::EqualityIndex { index, key, value } => {
                shared::AccessSourceParts::EqualityIndex {
                    access: crate::physical::PhysicalAccess::NodeExact(Box::new(
                        exec::ExecNodeAccessPlan::exact_equality(
                            index.clone(),
                            key.clone(),
                            value.clone(),
                        ),
                    )),
                    index_id: &index.index_id,
                    key,
                    kind: match index.uniqueness {
                        catalog::IndexUniqueness::Unique => shared::EqualityIndexKind::Unique,
                        catalog::IndexUniqueness::NonUnique => shared::EqualityIndexKind::NonUnique,
                    },
                    semantics: value.semantics(),
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
