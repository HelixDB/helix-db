//! Exact deferred-index visibility requirements for executable operations.
//!
//! Graph rows are staged eagerly in the request transaction. Topology,
//! secondary, vector, and text maintenance may be retained in family-local
//! runtimes, so only operations that consume one of those physical families
//! request its flush.

use helix_planner::exec;

/// One deferred physical family that an operation may need to observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeferredMutationFamily {
    Topology,
    Secondary,
    Vector,
    Text,
}

/// Closed set of deferred families required before one executable operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::execution::interpreter) struct RequiredMutationVisibility(u8);

impl RequiredMutationVisibility {
    const SECONDARY: u8 = 1 << 0;
    const VECTOR: u8 = 1 << 1;
    const TEXT: u8 = 1 << 2;
    const TOPOLOGY: u8 = 1 << 3;

    const NONE: Self = Self(0);
    const ALL: Self = Self(Self::TOPOLOGY | Self::SECONDARY | Self::VECTOR | Self::TEXT);

    const fn one(family: DeferredMutationFamily) -> Self {
        match family {
            DeferredMutationFamily::Topology => Self(Self::TOPOLOGY),
            DeferredMutationFamily::Secondary => Self(Self::SECONDARY),
            DeferredMutationFamily::Vector => Self(Self::VECTOR),
            DeferredMutationFamily::Text => Self(Self::TEXT),
        }
    }

    /// Returns whether this operation requires the selected family.
    pub(super) const fn contains(self, family: DeferredMutationFamily) -> bool {
        self.0 & Self::one(family).0 != 0
    }

    /// Returns whether no deferred physical state is observable by this operation.
    pub(super) const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Returns the conservative requirement used by explicit test barriers.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(super) const fn all() -> Self {
        Self::ALL
    }
}

/// Classifies the physical visibility required by one executable operation.
pub(in crate::execution::interpreter) fn required_for(
    op: &exec::ExecOp,
) -> RequiredMutationVisibility {
    match op {
        exec::ExecOp::Access { plan } => required_for_access(plan),
        exec::ExecOp::VectorSearch { .. } => {
            RequiredMutationVisibility::one(DeferredMutationFamily::Vector)
        }
        exec::ExecOp::TextSearch { .. } => {
            RequiredMutationVisibility::one(DeferredMutationFamily::Text)
        }
        exec::ExecOp::Count { .. }
        | exec::ExecOp::KvRead(_)
        | exec::ExecOp::Reserved { .. }
        | exec::ExecOp::Barrier { .. }
        | exec::ExecOp::IndexDdl { .. } => RequiredMutationVisibility::ALL,
        exec::ExecOp::Expand { .. } | exec::ExecOp::ShortestPath { .. } => {
            RequiredMutationVisibility::one(DeferredMutationFamily::Topology)
        }
        exec::ExecOp::Filter { .. }
        | exec::ExecOp::Limit { .. }
        | exec::ExecOp::Skip { .. }
        | exec::ExecOp::Range { .. }
        | exec::ExecOp::Distinct
        | exec::ExecOp::Order { .. }
        | exec::ExecOp::Project { .. }
        | exec::ExecOp::Aggregate { .. }
        | exec::ExecOp::Variable { .. }
        | exec::ExecOp::Branch { .. }
        | exec::ExecOp::Repeat { .. }
        | exec::ExecOp::Mutation { .. }
        | exec::ExecOp::Merge { .. }
        | exec::ExecOp::ForEach { .. }
        | exec::ExecOp::Noop => RequiredMutationVisibility::NONE,
    }
}

fn required_for_access(plan: &exec::ExecAccessPlan) -> RequiredMutationVisibility {
    match plan {
        exec::ExecAccessPlan::Limited(plan) => required_for_access(plan.source()),
        exec::ExecAccessPlan::Node(plan) => match plan {
            exec::ExecNodeAccessPlan::Bitmap { .. }
            | exec::ExecNodeAccessPlan::Unique { .. }
            | exec::ExecNodeAccessPlan::DynamicEquality { .. }
            | exec::ExecNodeAccessPlan::DynamicMembership { .. }
            | exec::ExecNodeAccessPlan::RangeIndex { .. }
            | exec::ExecNodeAccessPlan::SecondarySet { .. } => {
                RequiredMutationVisibility::one(DeferredMutationFamily::Secondary)
            }
            exec::ExecNodeAccessPlan::VectorSearch { .. } => {
                RequiredMutationVisibility::one(DeferredMutationFamily::Vector)
            }
            exec::ExecNodeAccessPlan::TextSearch { .. } => {
                RequiredMutationVisibility::one(DeferredMutationFamily::Text)
            }
            exec::ExecNodeAccessPlan::Empty
            | exec::ExecNodeAccessPlan::FromParam { .. }
            | exec::ExecNodeAccessPlan::FromVar { .. }
            | exec::ExecNodeAccessPlan::AllScan
            | exec::ExecNodeAccessPlan::AuthoritativeScan { .. } => {
                RequiredMutationVisibility::NONE
            }
            exec::ExecNodeAccessPlan::LabelScan { .. } => {
                RequiredMutationVisibility::one(DeferredMutationFamily::Topology)
            }
        },
        exec::ExecAccessPlan::Edge(plan) => match plan {
            exec::ExecEdgeAccessPlan::Bitmap { .. }
            | exec::ExecEdgeAccessPlan::DynamicEquality { .. }
            | exec::ExecEdgeAccessPlan::DynamicMembership { .. }
            | exec::ExecEdgeAccessPlan::RangeIndex { .. }
            | exec::ExecEdgeAccessPlan::SecondarySet { .. } => {
                RequiredMutationVisibility::one(DeferredMutationFamily::Secondary)
            }
            exec::ExecEdgeAccessPlan::VectorSearch { .. } => {
                RequiredMutationVisibility::one(DeferredMutationFamily::Vector)
            }
            exec::ExecEdgeAccessPlan::TextSearch { .. } => {
                RequiredMutationVisibility::one(DeferredMutationFamily::Text)
            }
            exec::ExecEdgeAccessPlan::Empty
            | exec::ExecEdgeAccessPlan::FromParam { .. }
            | exec::ExecEdgeAccessPlan::FromVar { .. }
            | exec::ExecEdgeAccessPlan::AllScan
            | exec::ExecEdgeAccessPlan::AuthoritativeScan { .. } => {
                RequiredMutationVisibility::NONE
            }
            exec::ExecEdgeAccessPlan::LabelScan { .. } => {
                RequiredMutationVisibility::one(DeferredMutationFamily::Topology)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_only_operations_require_no_deferred_family() {
        for op in [
            exec::ExecOp::Noop,
            exec::ExecOp::Distinct,
            exec::ExecOp::Access {
                plan: Box::new(exec::ExecAccessPlan::Node(
                    exec::ExecNodeAccessPlan::AllScan,
                )),
            },
            exec::ExecOp::Access {
                plan: Box::new(exec::ExecAccessPlan::Edge(
                    exec::ExecEdgeAccessPlan::AllScan,
                )),
            },
        ] {
            assert_eq!(required_for(&op), RequiredMutationVisibility::NONE);
        }
    }

    #[test]
    fn search_accesses_require_only_their_physical_family() {
        let secondary = exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::exact_equality(
                    helix_planner::catalog::NodeEqualityIndexMeta::try_new("user-email").unwrap(),
                    helix_planner::catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                    helix_planner::ir::IndexValue::Literal(
                        helix_planner::ir::SecondaryIndexLiteral::new(
                            helix_ast::value::PropertyValue::from("a@example.com"),
                        )
                        .unwrap(),
                    ),
                ),
            )),
        };
        let required = required_for(&secondary);
        assert!(required.contains(DeferredMutationFamily::Secondary));
        assert!(!required.contains(DeferredMutationFamily::Vector));
        assert!(!required.contains(DeferredMutationFamily::Text));

        let vector = required_for(&exec::ExecOp::VectorSearch {
            plan: Box::new(helix_planner::ir::RestrictedVectorSearchPlan::Nodes {
                key: helix_planner::catalog::NodeSearchIndexKey::try_new("Doc", "embedding")
                    .unwrap(),
                index: helix_planner::ir::SearchIndexPlan {
                    index_id: helix_planner::ir::NonEmptyString::new("doc-vector").unwrap(),
                    tenant: helix_planner::ir::SearchTenantPlan::Unscoped,
                },
                query_vector: helix_planner::ir::VectorQueryInputPlan::new(
                    helix_ast::value::PropertyInput::from(vec![1.0_f32, 0.0]),
                )
                .unwrap(),
                k: helix_planner::ir::SearchLimitPlan::Literal(
                    std::num::NonZeroUsize::new(10).unwrap(),
                ),
            }),
        });
        assert!(!vector.contains(DeferredMutationFamily::Secondary));
        assert!(vector.contains(DeferredMutationFamily::Vector));
        assert!(!vector.contains(DeferredMutationFamily::Text));
    }

    #[test]
    fn explicit_barrier_requires_every_deferred_family() {
        let required = required_for(&exec::ExecOp::Barrier {
            name: helix_planner::ir::NonEmptyString::new("visible").unwrap(),
        });
        for family in [
            DeferredMutationFamily::Topology,
            DeferredMutationFamily::Secondary,
            DeferredMutationFamily::Vector,
            DeferredMutationFamily::Text,
        ] {
            assert!(required.contains(family));
        }
    }
}
