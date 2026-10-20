//! Exact deferred-index visibility requirements for executable operations.
//!
//! Graph rows are staged eagerly in the request transaction. Topology and
//! secondary maintenance may be retained in family-local runtimes, so only
//! operations that consume one of those physical families request its flush.
//! Vector and text maintenance is queued, never staged physically; searches
//! overlay the transaction's own queued state directly and need no flush.

use helix_planner::exec;

/// One deferred physical family that an operation may need to observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeferredMutationFamily {
    Topology,
    Secondary,
}

/// Closed set of deferred families required before one executable operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::execution::interpreter) struct RequiredMutationVisibility(u8);

impl RequiredMutationVisibility {
    const SECONDARY: u8 = 1 << 0;
    const TOPOLOGY: u8 = 1 << 1;

    const NONE: Self = Self(0);
    const ALL: Self = Self(Self::TOPOLOGY | Self::SECONDARY);
    /// A secondary read that may also read a label bitmap: an index
    /// membership, or an equality whose null or unencodable value is answered
    /// by the label rows outside the equality lane.
    const SECONDARY_WITH_LABELS: Self = Self(Self::SECONDARY | Self::TOPOLOGY);

    const fn one(family: DeferredMutationFamily) -> Self {
        match family {
            DeferredMutationFamily::Topology => Self(Self::TOPOLOGY),
            DeferredMutationFamily::Secondary => Self(Self::SECONDARY),
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
        exec::ExecOp::Count { plan } => required_for_count(plan),
        exec::ExecOp::KvRead(_)
        | exec::ExecOp::Reserved { .. }
        | exec::ExecOp::Barrier { .. }
        | exec::ExecOp::IndexDdl { .. } => RequiredMutationVisibility::ALL,
        exec::ExecOp::Expand { .. } | exec::ExecOp::ShortestPath { .. } => {
            RequiredMutationVisibility::one(DeferredMutationFamily::Topology)
        }
        // Membership reads the secondary set and the node-label bitmap.
        exec::ExecOp::IndexMembership { .. } => RequiredMutationVisibility::SECONDARY_WITH_LABELS,
        exec::ExecOp::Branch { plan } => {
            let subplan = |plan: &exec::ExecutableSubplan| {
                plan.steps()
                    .iter()
                    .fold(RequiredMutationVisibility::NONE, |mask, step| {
                        RequiredMutationVisibility(mask.0 | required_for(&step.op).0)
                    })
            };
            match plan {
                exec::ExecBranchPlan::Union(branches) => branches
                    .as_ref()
                    .iter()
                    .fold(RequiredMutationVisibility::NONE, |mask, plan| {
                        RequiredMutationVisibility(mask.0 | subplan(plan).0)
                    }),
                exec::ExecBranchPlan::Coalesce(branches) => branches
                    .as_ref()
                    .iter()
                    .fold(RequiredMutationVisibility::NONE, |mask, plan| {
                        RequiredMutationVisibility(mask.0 | subplan(plan).0)
                    }),
                exec::ExecBranchPlan::Optional(plan)
                | exec::ExecBranchPlan::Choose {
                    then_plan: plan, ..
                } => subplan(plan),
                exec::ExecBranchPlan::ChooseElse {
                    then_plan,
                    else_plan,
                    ..
                } => RequiredMutationVisibility(subplan(then_plan).0 | subplan(else_plan).0),
            }
        }
        exec::ExecOp::Repeat { plan } => plan
            .body
            .steps()
            .iter()
            .fold(RequiredMutationVisibility::NONE, |mask, step| {
                RequiredMutationVisibility(mask.0 | required_for(&step.op).0)
            }),
        exec::ExecOp::VectorSearch { .. }
        | exec::ExecOp::TextSearch { .. }
        | exec::ExecOp::Filter { .. }
        | exec::ExecOp::Limit { .. }
        | exec::ExecOp::Skip { .. }
        | exec::ExecOp::Range { .. }
        | exec::ExecOp::Distinct
        | exec::ExecOp::Order { .. }
        | exec::ExecOp::Project { .. }
        | exec::ExecOp::Aggregate { .. }
        | exec::ExecOp::Variable { .. }
        | exec::ExecOp::Mutation { .. }
        | exec::ExecOp::Merge { .. }
        | exec::ExecOp::ForEach { .. }
        | exec::ExecOp::Noop => RequiredMutationVisibility::NONE,
    }
}

/// A row pipeline may suspend several MATCH operators while borrowing one read
/// view. Flush their selected physical families before opening any continuation.
/// No mutation is allowed inside such a pipeline.
pub(in crate::execution::interpreter) fn required_for_pattern(
    plan: &helix_planner::relational::MatchPlan,
) -> RequiredMutationVisibility {
    use helix_planner::relational::MatchStep;
    plan.steps
        .iter()
        .fold(RequiredMutationVisibility::NONE, |mut required, step| {
            match step {
                MatchStep::Scan(slot) | MatchStep::HashJoin { slot, .. } => {
                    let source = plan
                        .sources
                        .iter()
                        .find(|source| source.slot == *slot)
                        .expect("planned scan has a source");
                    for step in source.access.steps() {
                        required.0 |= required_for(&step.op).0;
                    }
                }
                MatchStep::IndexLookup(lookup) => {
                    required.0 |= RequiredMutationVisibility::SECONDARY;
                    let source = plan
                        .sources
                        .iter()
                        .find(|source| source.slot == lookup.slot)
                        .expect("planned lookup has a fallback source");
                    for step in source.access.steps() {
                        required.0 |= required_for(&step.op).0;
                    }
                }
                // Edge label bitmaps and adjacency are both topology rows.
                MatchStep::Expand { .. } | MatchStep::ScanRelationship { .. } => {
                    required.0 |= RequiredMutationVisibility::TOPOLOGY;
                }
            }
            required
        })
}

/// Counts of a constant, a request parameter, a materialized variable or the
/// rows of the step they consume read no storage, so they flush nothing.
/// Flushing before them would re-merge every shared row a preceding mutation
/// touched, once per count (`add_n(..).count()` repeated in one batch).
/// Every other count reads storage and keeps the conservative barrier.
fn required_for_count(plan: &exec::ExecCountPlan) -> RequiredMutationVisibility {
    match plan {
        exec::ExecCountPlan::Constant(_)
        | exec::ExecCountPlan::RuntimeInput { .. }
        | exec::ExecCountPlan::InputRows { .. }
        | exec::ExecCountPlan::InputScalars { .. } => RequiredMutationVisibility::NONE,
        exec::ExecCountPlan::NodeBitmap(_)
        | exec::ExecCountPlan::EdgeBitmap(_)
        | exec::ExecCountPlan::NodeUnique(_)
        | exec::ExecCountPlan::NodeRange(_)
        | exec::ExecCountPlan::EdgeRange(_)
        | exec::ExecCountPlan::NodeAuthoritativeScan(_)
        | exec::ExecCountPlan::EdgeAuthoritativeScan(_)
        | exec::ExecCountPlan::NodePointReads { .. }
        | exec::ExecCountPlan::EdgePointReads { .. }
        | exec::ExecCountPlan::NodeRuntimeInput { .. }
        | exec::ExecCountPlan::EdgeRuntimeInput { .. }
        | exec::ExecCountPlan::NodeFullScan { .. }
        | exec::ExecCountPlan::EdgeFullScan { .. }
        | exec::ExecCountPlan::NodeLabelBitmap { .. }
        | exec::ExecCountPlan::EdgeLabelBitmap { .. }
        | exec::ExecCountPlan::NodeVectorSearch(_)
        | exec::ExecCountPlan::EdgeVectorSearch(_)
        | exec::ExecCountPlan::NodeTextSearch(_)
        | exec::ExecCountPlan::EdgeTextSearch(_)
        | exec::ExecCountPlan::NodeDynamicEquality(_)
        | exec::ExecCountPlan::EdgeDynamicEquality(_)
        | exec::ExecCountPlan::NodeDynamicMembership(_)
        | exec::ExecCountPlan::EdgeDynamicMembership(_)
        | exec::ExecCountPlan::Stream(_) => RequiredMutationVisibility::ALL,
    }
}

fn required_for_access(plan: &exec::ExecAccessPlan) -> RequiredMutationVisibility {
    match plan {
        exec::ExecAccessPlan::Limited(plan) => required_for_access(plan.source()),
        exec::ExecAccessPlan::Node(plan) => match plan {
            exec::ExecNodeAccessPlan::Bitmap { .. }
            | exec::ExecNodeAccessPlan::Unique { .. }
            | exec::ExecNodeAccessPlan::RangeIndex { .. } => {
                RequiredMutationVisibility::one(DeferredMutationFamily::Secondary)
            }
            exec::ExecNodeAccessPlan::DynamicEquality { .. }
            | exec::ExecNodeAccessPlan::DynamicMembership { .. }
            | exec::ExecNodeAccessPlan::SecondarySet { .. }
            | exec::ExecNodeAccessPlan::AuthoritativeScan {
                predicate: exec::ExecNodeAuthoritativeScanPredicate::NullEquality { .. },
            } => RequiredMutationVisibility::SECONDARY_WITH_LABELS,
            exec::ExecNodeAccessPlan::VectorSearch { .. }
            | exec::ExecNodeAccessPlan::TextSearch { .. }
            | exec::ExecNodeAccessPlan::Empty
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
            | exec::ExecEdgeAccessPlan::RangeIndex { .. } => {
                RequiredMutationVisibility::one(DeferredMutationFamily::Secondary)
            }
            exec::ExecEdgeAccessPlan::DynamicEquality { .. }
            | exec::ExecEdgeAccessPlan::DynamicMembership { .. }
            | exec::ExecEdgeAccessPlan::SecondarySet { .. }
            | exec::ExecEdgeAccessPlan::AuthoritativeScan {
                predicate: exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { .. },
            } => RequiredMutationVisibility::SECONDARY_WITH_LABELS,
            exec::ExecEdgeAccessPlan::VectorSearch { .. }
            | exec::ExecEdgeAccessPlan::TextSearch { .. }
            | exec::ExecEdgeAccessPlan::Empty
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
    fn secondary_accesses_flush_secondary_and_searches_flush_nothing() {
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
        assert!(!required.contains(DeferredMutationFamily::Topology));

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
        // Searches overlay transaction-local queued state instead of flushing.
        assert_eq!(vector, RequiredMutationVisibility::NONE);
    }

    #[test]
    fn index_membership_requires_secondary_and_label_topology() {
        let key = helix_planner::catalog::ScopedPropertyKey::try_new("Item", "kind").unwrap();
        let plan = helix_planner::ir::NodeIndexMembershipPlan::new(
            helix_planner::ir::NodeAccessSourcePlan::new(
                helix_planner::ir::NodeAccessPlan::EqualityIndex {
                    index: helix_planner::catalog::IndexCatalogSnapshot::default()
                        .with_node_eq(key.clone())
                        .node_eq[&key]
                        .clone(),
                    key,
                    value: helix_planner::ir::IndexValue::Literal(
                        helix_planner::ir::SecondaryIndexLiteral::new(
                            helix_ast::value::PropertyValue::from("B"),
                        )
                        .unwrap(),
                    ),
                },
            )
            .unwrap(),
            helix_planner::ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("kind", "B"))
                .unwrap(),
            None,
        )
        .unwrap();
        let required = required_for(&exec::ExecOp::IndexMembership {
            plan: Box::new(exec::ExecNodeIndexMembershipPlan::from(&plan)),
        });

        assert!(required.contains(DeferredMutationFamily::Secondary));
        assert!(required.contains(DeferredMutationFamily::Topology));
        assert_eq!(
            required_for(&exec::ExecOp::Filter {
                predicate: plan.predicate().clone(),
            }),
            RequiredMutationVisibility::NONE
        );
    }

    #[test]
    fn counts_of_held_rows_flush_nothing_and_storage_counts_flush_everything() {
        let window = exec::ExecCountWindowPlan::identity;
        let name = || helix_planner::ir::NonEmptyString::new("held").unwrap();
        let count = |plan| {
            required_for(&exec::ExecOp::Count {
                plan: Box::new(plan),
            })
        };
        for plan in [
            exec::ExecCountPlan::Constant(3),
            exec::ExecCountPlan::InputRows { window: window() },
            exec::ExecCountPlan::InputScalars { window: window() },
            exec::ExecCountPlan::RuntimeInput {
                input: exec::ExecRuntimeInputPlan::Param(name()),
                window: window(),
            },
            exec::ExecCountPlan::RuntimeInput {
                input: exec::ExecRuntimeInputPlan::Variable(name()),
                window: window(),
            },
        ] {
            assert_eq!(count(plan), RequiredMutationVisibility::NONE);
        }
        for plan in [
            exec::ExecCountPlan::NodeFullScan { window: window() },
            exec::ExecCountPlan::EdgeFullScan { window: window() },
            exec::ExecCountPlan::NodeLabelBitmap {
                label: name(),
                window: window(),
            },
            exec::ExecCountPlan::EdgeLabelBitmap {
                label: name(),
                window: window(),
            },
            exec::ExecCountPlan::NodeRuntimeInput {
                input: exec::ExecRuntimeInputPlan::Variable(name()),
                window: window(),
            },
        ] {
            assert_eq!(count(plan), RequiredMutationVisibility::ALL);
        }
    }

    #[test]
    fn explicit_barrier_requires_every_deferred_family() {
        let required = required_for(&exec::ExecOp::Barrier {
            name: helix_planner::ir::NonEmptyString::new("visible").unwrap(),
        });
        for family in [
            DeferredMutationFamily::Topology,
            DeferredMutationFamily::Secondary,
        ] {
            assert!(required.contains(family));
        }
    }
}
