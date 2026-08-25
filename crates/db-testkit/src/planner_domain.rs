//! Finite planner configuration domain and deterministic property checks.
//!
//! The domain deliberately separates semantic workload shape from the mutable
//! planner context. [`NormalizedPlannerCase::pairwise`] covers every pair of
//! axis values for pull requests; [`NormalizedPlannerCase::complete`] returns
//! the finite cross-product for deterministic nightly sharding.

use std::collections::BTreeSet;

use helix_ast::batch::BatchQuery;
use helix_ast::query::QueryValue;
use helix_ast::{batch, expr, index, traversal, value};
use helix_planner::catalog;
use helix_planner::context;
use helix_planner::cost;
use helix_planner::experiments::{
    PlanScalabilityFixture, PlanningScalabilityShape, PlanningScalabilityWorkload,
};
use helix_planner::feedback;
use helix_planner::ir;
use helix_planner::properties;
use serde::{Deserialize, Serialize};

use crate::{Result, TestkitError};

/// Every production scalability workload family used as a normalized seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannerShape {
    /// Wide conjunctive indexed predicates.
    WideBooleanPredicates,
    /// Large catalogs with few relevant indexes.
    ManyAvailableIndexes,
    /// Repeated native roots in one batch.
    BatchedRootReuse,
    /// Repeated native roots inside `for_each`.
    ForEachBodyRootReuse,
    /// Long traversal chains.
    DeepTraversalChain,
    /// Many alternatives in a memo family.
    ManyMemoAlternatives,
    /// Disjunction exceeding the index-union limit.
    OverLimitIndexDisjunction,
    /// High branch fanout.
    BranchHeavyQueries,
    /// Ordered range windows.
    OrderedRangeWindowPushdown,
    /// Mixed graph mutations.
    MutationHeavyBatches,
    /// Secondary, vector, and text DDL.
    SearchIndexDdlWorkloads,
    /// Query-service-derived mixed requests.
    RuntimeDerivedMixedQueries,
    /// Same-index bitmap batches intersected with a different equality index.
    CardinalityBitmapSets,
    /// Verified range drivers with bitmap membership and normalized windows.
    CardinalityRangesAndWindows,
    /// Unique equality and explicit authoritative null cardinality.
    CardinalityUniqueAndNull,
    /// Late-bound node and edge equality inside `foreach`.
    CardinalityLateBoundFilters,
}

impl PlannerShape {
    /// Complete closed shape domain.
    pub const ALL: [Self; 16] = [
        Self::WideBooleanPredicates,
        Self::ManyAvailableIndexes,
        Self::BatchedRootReuse,
        Self::ForEachBodyRootReuse,
        Self::DeepTraversalChain,
        Self::ManyMemoAlternatives,
        Self::OverLimitIndexDisjunction,
        Self::BranchHeavyQueries,
        Self::OrderedRangeWindowPushdown,
        Self::MutationHeavyBatches,
        Self::SearchIndexDdlWorkloads,
        Self::RuntimeDerivedMixedQueries,
        Self::CardinalityBitmapSets,
        Self::CardinalityRangesAndWindows,
        Self::CardinalityUniqueAndNull,
        Self::CardinalityLateBoundFilters,
    ];

    const fn production(self) -> Option<PlanningScalabilityShape> {
        Some(match self {
            Self::WideBooleanPredicates => PlanningScalabilityShape::WideBooleanPredicates,
            Self::ManyAvailableIndexes => PlanningScalabilityShape::ManyAvailableIndexes,
            Self::BatchedRootReuse => PlanningScalabilityShape::BatchedRootReuse,
            Self::ForEachBodyRootReuse => PlanningScalabilityShape::ForEachBodyRootReuse,
            Self::DeepTraversalChain => PlanningScalabilityShape::DeepTraversalChain,
            Self::ManyMemoAlternatives => PlanningScalabilityShape::ManyMemoAlternatives,
            Self::OverLimitIndexDisjunction => PlanningScalabilityShape::OverLimitIndexDisjunction,
            Self::BranchHeavyQueries => PlanningScalabilityShape::BranchHeavyQueries,
            Self::OrderedRangeWindowPushdown => {
                PlanningScalabilityShape::OrderedRangeWindowPushdown
            }
            Self::MutationHeavyBatches => PlanningScalabilityShape::MutationHeavyBatches,
            Self::SearchIndexDdlWorkloads => PlanningScalabilityShape::SearchIndexDdlWorkloads,
            Self::RuntimeDerivedMixedQueries => {
                PlanningScalabilityShape::RuntimeDerivedMixedQueries
            }
            Self::CardinalityBitmapSets
            | Self::CardinalityRangesAndWindows
            | Self::CardinalityUniqueAndNull
            | Self::CardinalityLateBoundFilters => return None,
        })
    }
}

/// Representative partitions for positive, otherwise-unbounded fixture scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScaleClass {
    /// Smallest accepted scale.
    Singleton,
    /// Small multi-item case.
    Pair,
    /// Ordinary production-like case.
    Ordinary,
    /// Largest bounded pull-request case; larger scales remain benchmark gates.
    PullRequestMaximum,
}

impl ScaleClass {
    /// Complete scale partition.
    pub const ALL: [Self; 4] = [
        Self::Singleton,
        Self::Pair,
        Self::Ordinary,
        Self::PullRequestMaximum,
    ];

    /// Concrete representative.
    pub const fn value(self) -> usize {
        match self {
            Self::Singleton => 1,
            Self::Pair => 2,
            Self::Ordinary => 8,
            Self::PullRequestMaximum => 16,
        }
    }
}

/// Boolean/positive boundary classes for index-union planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnionLimitClass {
    /// Explicitly disabled zero state.
    Disabled,
    /// Smallest positive state.
    Singleton,
    /// Ordinary bounded state.
    Ordinary,
    /// Maximum value accepted by the serialized contract.
    MaximumAccepted,
}

impl UnionLimitClass {
    /// Complete union-limit domain.
    pub const ALL: [Self; 4] = [
        Self::Disabled,
        Self::Singleton,
        Self::Ordinary,
        Self::MaximumAccepted,
    ];

    fn production(self) -> context::IndexUnionBranchLimit {
        match self {
            Self::Disabled => context::IndexUnionBranchLimit::Disabled,
            Self::Singleton => context::IndexUnionBranchLimit::from_usize(1),
            Self::Ordinary => context::IndexUnionBranchLimit::from_usize(64),
            Self::MaximumAccepted => context::IndexUnionBranchLimit::from_usize(usize::MAX),
        }
    }
}

/// Representative optimizer guardrail configurations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizerLimitClass {
    /// All guardrails at their smallest valid value.
    Minimum,
    /// Production defaults.
    Default,
    /// Large but bounded values used to distinguish guardrail behavior.
    Large,
}

impl OptimizerLimitClass {
    /// Complete optimizer-limit partition.
    pub const ALL: [Self; 3] = [Self::Minimum, Self::Default, Self::Large];

    fn apply(self, ctx: &mut context::PlannerContext) {
        match self {
            Self::Minimum => {
                let one = properties::PositiveUsize::at_least_one(1);
                ctx.optimizer_limits = context::OptimizerLimits {
                    memo_groups: one,
                    memo_expressions: one,
                    rule_fires: one,
                    alternatives_per_group: one,
                    optimization_micros: one,
                };
            }
            Self::Default => {}
            Self::Large => {
                ctx.optimizer_limits = context::OptimizerLimits {
                    memo_groups: properties::PositiveUsize::at_least_one(100_000),
                    memo_expressions: properties::PositiveUsize::at_least_one(1_000_000),
                    rule_fires: properties::PositiveUsize::at_least_one(2_500_000),
                    alternatives_per_group: properties::PositiveUsize::at_least_one(128),
                    optimization_micros: properties::PositiveUsize::at_least_one(2_000_000),
                };
            }
        }
    }
}

/// Optional planner-context field partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionalContextClass {
    /// No optional overlay.
    Absent,
    /// A property-compatible parameter is present.
    PropertyParameter,
    /// A JSON-compatible parameter is present.
    QueryParameter,
    /// Immutable runtime feedback is present.
    RuntimeFeedback,
}

impl OptionalContextClass {
    /// Complete optional-field partition.
    pub const ALL: [Self; 4] = [
        Self::Absent,
        Self::PropertyParameter,
        Self::QueryParameter,
        Self::RuntimeFeedback,
    ];

    fn apply(self, ctx: &mut context::PlannerContext) {
        let name = ir::NonEmptyString::new("normalized_limit")
            .expect("normalized parameter name is non-empty");
        match self {
            Self::Absent => {}
            Self::PropertyParameter => {
                ctx.params = ctx.params.clone().with_value(name, 7_i64);
            }
            Self::QueryParameter => {
                ctx.params = ctx
                    .params
                    .clone()
                    .with_query_value(name, QueryValue::String("seven".to_string()));
            }
            Self::RuntimeFeedback => {
                let label = ir::NonEmptyString::new("User")
                    .expect("normalized feedback label is non-empty");
                ctx.runtime_feedback = feedback::RuntimeFeedbackSnapshot::default()
                    .with_node_label_cardinality(label, feedback::ObservedRows::rows(17));
            }
        }
    }
}

/// Representative storage cost profiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageCostClass {
    /// Production defaults.
    Default,
    /// Range operations are relatively cheap.
    RangePreferred,
    /// Range operations are relatively expensive.
    RangePenalized,
}

impl StorageCostClass {
    /// Complete storage-cost partition.
    pub const ALL: [Self; 3] = [Self::Default, Self::RangePreferred, Self::RangePenalized];

    fn apply(self, ctx: &mut context::PlannerContext) {
        match self {
            Self::Default => {}
            Self::RangePreferred => {
                ctx.storage.range_next = cost::LatencyEstimate::micros(1);
            }
            Self::RangePenalized => {
                ctx.storage.range_next = cost::LatencyEstimate::micros(100);
            }
        }
    }
}

/// One canonical point in the finite planner configuration domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NormalizedPlannerCase {
    /// Semantic workload family.
    pub shape: PlannerShape,
    /// Representative workload scale.
    pub scale: ScaleClass,
    /// Index-union boundary.
    pub union_limit: UnionLimitClass,
    /// Optimizer guardrail boundary.
    pub optimizer_limits: OptimizerLimitClass,
    /// Optional context field boundary.
    pub optional_context: OptionalContextClass,
    /// Storage cost boundary.
    pub storage_cost: StorageCostClass,
}

impl NormalizedPlannerCase {
    const DEFAULT: Self = Self {
        shape: PlannerShape::WideBooleanPredicates,
        scale: ScaleClass::Ordinary,
        union_limit: UnionLimitClass::Ordinary,
        optimizer_limits: OptimizerLimitClass::Default,
        optional_context: OptionalContextClass::Absent,
        storage_cost: StorageCostClass::Default,
    };

    /// Builds the production query and immutable context for this domain point.
    pub fn inputs(self) -> (BatchQuery, context::PlannerContext) {
        let (query, mut ctx) = match self.shape.production() {
            Some(shape) => {
                let fixture = PlanScalabilityFixture::new(shape, self.scale.value())
                    .expect("normalized scale classes are positive")
                    .case();
                let query = match fixture.workload() {
                    PlanningScalabilityWorkload::Read(batch) => BatchQuery::Read(batch.clone()),
                    PlanningScalabilityWorkload::Write(batch) => BatchQuery::Write(batch.clone()),
                };
                (query, fixture.context().clone())
            }
            None => cardinality_inputs(self.shape, self.scale),
        };
        ctx.limits.max_index_union_branches = self.union_limit.production();
        self.optimizer_limits.apply(&mut ctx);
        self.optional_context.apply(&mut ctx);
        self.storage_cost.apply(&mut ctx);
        (query, ctx)
    }

    /// Deterministic pairwise covering set for pull requests.
    ///
    /// Each pair of axes and each pair of values on those axes appears in at
    /// least one returned case. Exact duplicates are removed without relying
    /// on randomized construction.
    pub fn pairwise() -> Vec<Self> {
        let axes = [
            PlannerShape::ALL.len(),
            ScaleClass::ALL.len(),
            UnionLimitClass::ALL.len(),
            OptimizerLimitClass::ALL.len(),
            OptionalContextClass::ALL.len(),
            StorageCostClass::ALL.len(),
        ];
        let mut cases = BTreeSet::new();
        for left_axis in 0..axes.len() {
            for right_axis in left_axis + 1..axes.len() {
                for left_value in 0..axes[left_axis] {
                    for right_value in 0..axes[right_axis] {
                        let mut indexes = [0; 6];
                        indexes[0] = PlannerShape::ALL
                            .iter()
                            .position(|value| *value == Self::DEFAULT.shape)
                            .expect("default shape belongs to domain");
                        indexes[1] = ScaleClass::ALL
                            .iter()
                            .position(|value| *value == Self::DEFAULT.scale)
                            .expect("default scale belongs to domain");
                        indexes[2] = UnionLimitClass::ALL
                            .iter()
                            .position(|value| *value == Self::DEFAULT.union_limit)
                            .expect("default union limit belongs to domain");
                        indexes[3] = OptimizerLimitClass::ALL
                            .iter()
                            .position(|value| *value == Self::DEFAULT.optimizer_limits)
                            .expect("default optimizer limit belongs to domain");
                        indexes[4] = OptionalContextClass::ALL
                            .iter()
                            .position(|value| *value == Self::DEFAULT.optional_context)
                            .expect("default optional context belongs to domain");
                        indexes[5] = StorageCostClass::ALL
                            .iter()
                            .position(|value| *value == Self::DEFAULT.storage_cost)
                            .expect("default storage cost belongs to domain");
                        indexes[left_axis] = left_value;
                        indexes[right_axis] = right_value;
                        cases.insert(Self::from_indexes(indexes));
                    }
                }
            }
        }
        cases.into_iter().collect()
    }

    /// Complete finite cross-product for deterministic nightly sharding.
    pub fn complete() -> Vec<Self> {
        let mut cases = Vec::new();
        for shape in PlannerShape::ALL {
            for scale in ScaleClass::ALL {
                for union_limit in UnionLimitClass::ALL {
                    for optimizer_limits in OptimizerLimitClass::ALL {
                        for optional_context in OptionalContextClass::ALL {
                            for storage_cost in StorageCostClass::ALL {
                                cases.push(Self {
                                    shape,
                                    scale,
                                    union_limit,
                                    optimizer_limits,
                                    optional_context,
                                    storage_cost,
                                });
                            }
                        }
                    }
                }
            }
        }
        cases
    }

    /// Checks parse/serde stability, deterministic planning, executable-plan
    /// validation, and deterministic typed rejection for this case.
    pub fn check(self) -> Result<PlannerCaseOutcome> {
        let (query, ctx) = self.inputs();
        check_query(&query, &ctx)
    }

    fn from_indexes(indexes: [usize; 6]) -> Self {
        Self {
            shape: PlannerShape::ALL[indexes[0]],
            scale: ScaleClass::ALL[indexes[1]],
            union_limit: UnionLimitClass::ALL[indexes[2]],
            optimizer_limits: OptimizerLimitClass::ALL[indexes[3]],
            optional_context: OptionalContextClass::ALL[indexes[4]],
            storage_cost: StorageCostClass::ALL[indexes[5]],
        }
    }
}

fn cardinality_inputs(
    shape: PlannerShape,
    scale: ScaleClass,
) -> (BatchQuery, context::PlannerContext) {
    let node_status = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
    let node_tier = catalog::ScopedPropertyKey::try_new("User", "tier").unwrap();
    let edge_status = catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
    let mut indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_eq(node_status.clone())
        .with_node_eq(node_tier)
        .with_edge_eq(edge_status.clone());
    let stats = context::StatsSnapshot::default()
        .with_node_label_cardinality(ir::NonEmptyString::new("User").unwrap(), 1_000_000)
        .with_edge_label_cardinality(ir::NonEmptyString::new("FOLLOWS").unwrap(), 2_000_000);

    let query = match shape {
        PlannerShape::CardinalityBitmapSets => {
            let values = (0..scale.value().max(2))
                .map(|index| expr::Predicate::eq("status", format!("state-{index}")))
                .collect();
            let node_predicate = expr::Predicate::and(vec![
                expr::Predicate::or(values),
                expr::Predicate::eq("tier", "gold"),
            ]);
            let edge_predicate = expr::Predicate::or(vec![
                expr::Predicate::eq("status", "open"),
                expr::Predicate::eq("status", "closed"),
            ]);
            BatchQuery::Read(
                batch::read_batch()
                    .var_as(
                        "node_count",
                        traversal::g()
                            .n_with_label_where("User", node_predicate)
                            .count(),
                    )
                    .var_as(
                        "edge_count",
                        traversal::g()
                            .e_with_label_where("FOLLOWS", edge_predicate)
                            .count(),
                    )
                    .returning(["node_count", "edge_count"]),
            )
        }
        PlannerShape::CardinalityRangesAndWindows => {
            let direction = if scale.value().is_multiple_of(2) {
                index::RangeIndexDirection::Asc
            } else {
                index::RangeIndexDirection::Desc
            };
            indexes = indexes.with_node_range(
                catalog::ScopedPropertyDirectionKey::try_new("User", "age", direction).unwrap(),
            );
            let predicate = expr::Predicate::and(vec![
                expr::Predicate::gte("age", 18_i64),
                expr::Predicate::lt("age", 90_i64),
                expr::Predicate::eq("status", "active"),
            ]);
            BatchQuery::Read(
                batch::read_batch()
                    .var_as(
                        "result",
                        traversal::g()
                            .n_with_label_where("User", predicate)
                            .order_by(
                                "age",
                                match direction {
                                    index::RangeIndexDirection::Asc => traversal::Order::Asc,
                                    index::RangeIndexDirection::Desc => traversal::Order::Desc,
                                },
                            )
                            .skip(1usize)
                            .limit(scale.value())
                            .count(),
                    )
                    .returning(["result"]),
            )
        }
        PlannerShape::CardinalityUniqueAndNull => {
            let email = catalog::ScopedPropertyKey::try_new("User", "email").unwrap();
            indexes = indexes.with_node_eq(email.clone());
            indexes
                .node_eq
                .get_mut(&email)
                .expect("unique email index was inserted")
                .uniqueness = catalog::IndexUniqueness::Unique;
            BatchQuery::Read(
                batch::read_batch()
                    .var_as(
                        "unique_count",
                        traversal::g()
                            .n_with_label_where(
                                "User",
                                expr::Predicate::eq("email", "user@example.com"),
                            )
                            .count(),
                    )
                    .var_as(
                        "null_count",
                        traversal::g()
                            .n_with_label_where(
                                "User",
                                expr::Predicate::eq("status", value::PropertyValue::Null),
                            )
                            .count(),
                    )
                    .returning(["unique_count", "null_count"]),
            )
        }
        PlannerShape::CardinalityLateBoundFilters => {
            let audit_event = catalog::ScopedPropertyKey::try_new("Audit", "event_id").unwrap();
            let mention_event =
                catalog::ScopedPropertyKey::try_new("MENTIONS", "event_id").unwrap();
            indexes = indexes
                .with_node_eq(audit_event)
                .with_edge_eq(mention_event);
            let body = batch::write_batch()
                .var_as(
                    "nodes",
                    traversal::g()
                        .n_with_label_where(
                            "Audit",
                            expr::Predicate::and(vec![
                                expr::Predicate::eq_param("event_id", "event_id"),
                                expr::Predicate::contains("message", "accepted"),
                            ]),
                        )
                        .count(),
                )
                .var_as(
                    "edges",
                    traversal::g()
                        .e_with_label_where(
                            "MENTIONS",
                            expr::Predicate::eq_param("event_id", "event_id"),
                        )
                        .count(),
                );
            return (
                BatchQuery::Write(batch::write_batch().for_each_param("events", body)),
                context::PlannerContext {
                    indexes,
                    stats: stats
                        .with_node_label_cardinality(
                            ir::NonEmptyString::new("Audit").unwrap(),
                            500_000,
                        )
                        .with_edge_label_cardinality(
                            ir::NonEmptyString::new("MENTIONS").unwrap(),
                            500_000,
                        ),
                    ..context::PlannerContext::default()
                },
            );
        }
        PlannerShape::WideBooleanPredicates
        | PlannerShape::ManyAvailableIndexes
        | PlannerShape::BatchedRootReuse
        | PlannerShape::ForEachBodyRootReuse
        | PlannerShape::DeepTraversalChain
        | PlannerShape::ManyMemoAlternatives
        | PlannerShape::OverLimitIndexDisjunction
        | PlannerShape::BranchHeavyQueries
        | PlannerShape::OrderedRangeWindowPushdown
        | PlannerShape::MutationHeavyBatches
        | PlannerShape::SearchIndexDdlWorkloads
        | PlannerShape::RuntimeDerivedMixedQueries => {
            unreachable!("production scalability shapes use their existing fixtures")
        }
    };
    (
        query,
        context::PlannerContext {
            indexes,
            stats,
            ..context::PlannerContext::default()
        },
    )
}

/// Checks one externally supplied AST/context pair with the same properties as
/// the normalized domain.
pub fn check_query(
    query: &BatchQuery,
    ctx: &context::PlannerContext,
) -> Result<PlannerCaseOutcome> {
    let query_json = serde_json::to_vec(query)?;
    let decoded_query = serde_json::from_slice::<BatchQuery>(&query_json)?;
    if &decoded_query != query {
        return Err(TestkitError::Planner(
            "query serialization changed normalized meaning".to_string(),
        ));
    }
    let context_json = serde_json::to_vec(ctx)?;
    let decoded_context = serde_json::from_slice::<context::PlannerContext>(&context_json)?;
    if &decoded_context != ctx {
        return Err(TestkitError::Planner(
            "planner context serialization changed normalized meaning".to_string(),
        ));
    }

    let first = helix_planner::planning::plan(&decoded_query, &decoded_context);
    let second = helix_planner::planning::plan(&decoded_query, &decoded_context);
    match (first, second) {
        (Ok(first), Ok(second)) => {
            let first_fingerprint = semantic_plan_json(&first)?;
            let second_fingerprint = semantic_plan_json(&second)?;
            if first_fingerprint != second_fingerprint {
                return Err(TestkitError::Planner(
                    "same normalized input produced different executable plans".to_string(),
                ));
            }
            let round_trip = serde_json::from_value::<helix_planner::exec::ExecutablePlan>(
                serde_json::to_value(&first)?,
            )?;
            if semantic_plan_json(&round_trip)? != first_fingerprint {
                return Err(TestkitError::Planner(
                    "executable plan serialization changed meaning".to_string(),
                ));
            }
            Ok(PlannerCaseOutcome::Planned {
                semantic_plan: first_fingerprint,
            })
        }
        (Err(first), Err(second)) if first == second => Ok(PlannerCaseOutcome::Rejected {
            typed_error: format!("{first:?}"),
        }),
        (Err(first), Err(second)) => Err(TestkitError::Planner(format!(
            "same normalized input produced different typed errors: {first:?} versus {second:?}"
        ))),
        (first, second) => Err(TestkitError::Planner(format!(
            "same normalized input changed success state: {first:?} versus {second:?}"
        ))),
    }
}

/// Deterministic result of one normalized planner property check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum PlannerCaseOutcome {
    /// The case produced a validated executable plan.
    Planned {
        /// JSON fingerprint with non-semantic wall-clock metrics removed.
        semantic_plan: serde_json::Value,
    },
    /// The case was deterministically rejected at a typed planner boundary.
    Rejected {
        /// Stable debug form retaining the concrete error variant.
        typed_error: String,
    },
}

fn semantic_plan_json(plan: &helix_planner::exec::ExecutablePlan) -> Result<serde_json::Value> {
    let mut value = serde_json::to_value(plan)?;
    let Some(metrics) = value
        .as_object_mut()
        .and_then(|plan| plan.get_mut("metrics"))
        .and_then(serde_json::Value::as_object_mut)
    else {
        return Err(TestkitError::Planner(
            "serialized executable plan omitted metrics".to_string(),
        ));
    };
    let Some(_) = metrics.remove("optimization_micros") else {
        return Err(TestkitError::Planner(
            "serialized executable plan omitted optimization duration".to_string(),
        ));
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairwise_domain_covers_every_axis_value_pair() {
        let cases = NormalizedPlannerCase::pairwise();
        let indexes = cases
            .iter()
            .map(|case| {
                [
                    PlannerShape::ALL
                        .iter()
                        .position(|value| value == &case.shape)
                        .unwrap(),
                    ScaleClass::ALL
                        .iter()
                        .position(|value| value == &case.scale)
                        .unwrap(),
                    UnionLimitClass::ALL
                        .iter()
                        .position(|value| value == &case.union_limit)
                        .unwrap(),
                    OptimizerLimitClass::ALL
                        .iter()
                        .position(|value| value == &case.optimizer_limits)
                        .unwrap(),
                    OptionalContextClass::ALL
                        .iter()
                        .position(|value| value == &case.optional_context)
                        .unwrap(),
                    StorageCostClass::ALL
                        .iter()
                        .position(|value| value == &case.storage_cost)
                        .unwrap(),
                ]
            })
            .collect::<Vec<_>>();
        let levels = [PlannerShape::ALL.len(), 4, 4, 3, 4, 3];
        for left_axis in 0..levels.len() {
            for right_axis in left_axis + 1..levels.len() {
                for left_value in 0..levels[left_axis] {
                    for right_value in 0..levels[right_axis] {
                        assert!(indexes.iter().any(|case| {
                            case[left_axis] == left_value && case[right_axis] == right_value
                        }));
                    }
                }
            }
        }
    }

    #[test]
    fn complete_domain_has_exact_cartesian_cardinality_and_unique_cases() {
        let cases = NormalizedPlannerCase::complete();
        assert_eq!(cases.len(), PlannerShape::ALL.len() * 4 * 4 * 3 * 4 * 3);
        assert_eq!(
            cases.iter().copied().collect::<BTreeSet<_>>().len(),
            cases.len()
        );
    }

    #[test]
    fn representative_normalized_cases_are_deterministic_and_round_trip() {
        for shape in PlannerShape::ALL {
            let outcome = NormalizedPlannerCase {
                shape,
                ..NormalizedPlannerCase::DEFAULT
            }
            .check()
            .unwrap();
            assert!(matches!(
                outcome,
                PlannerCaseOutcome::Planned { .. } | PlannerCaseOutcome::Rejected { .. }
            ));
        }
    }

    #[test]
    fn cardinality_shapes_reach_validated_executable_plans() {
        for shape in [
            PlannerShape::CardinalityBitmapSets,
            PlannerShape::CardinalityRangesAndWindows,
            PlannerShape::CardinalityUniqueAndNull,
            PlannerShape::CardinalityLateBoundFilters,
        ] {
            let outcome = NormalizedPlannerCase {
                shape,
                ..NormalizedPlannerCase::DEFAULT
            }
            .check()
            .unwrap();
            assert!(
                matches!(outcome, PlannerCaseOutcome::Planned { .. }),
                "cardinality corpus shape {shape:?} must reach executable-plan validation: {outcome:?}"
            );
        }
    }

    #[test]
    fn pairwise_pull_request_domain_passes_all_planner_properties() {
        for case in NormalizedPlannerCase::pairwise() {
            case.check()
                .unwrap_or_else(|error| panic!("normalized planner case {case:?} failed: {error}"));
        }
    }

    #[test]
    fn zero_fixture_scale_is_the_explicit_invalid_partition() {
        assert!(
            PlanScalabilityFixture::new(PlanningScalabilityShape::WideBooleanPredicates, 0,)
                .is_none()
        );
    }
}
