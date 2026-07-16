//! Closed production rule inventory.

mod inventory;
mod names;
mod serde_impl;

/// Closed inventory of production optimizer rule IDs.
///
/// ```
/// use helix_planner::rules::{KnownRuleId, RuleId};
///
/// assert_eq!(
///     RuleId::known(KnownRuleId::FilterPushdown).as_ref(),
///     "filter_pushdown"
/// );
/// assert_eq!(
///     RuleId::new("filter_pushdown"),
///     Some(RuleId::known(KnownRuleId::FilterPushdown))
/// );
/// assert!(KnownRuleId::ALL
///     .iter()
///     .any(|id| id.as_ref() == "seed_access_path"));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KnownRuleId {
    /// Transpose filters below safe pure stream-preserving operators.
    FilterPushdown,
    /// Simplify no-op and idempotent side-effect-free pipeline operators.
    PurePipelineSimplification,
    /// Simplify statically decidable residual filters.
    StaticPredicateSimplification,
    /// Merge adjacent residual filters into one conjunctive predicate.
    FilterMerge,
    /// Implement side-effect-free logical pipelines.
    SeedPurePipeline,
    /// Implement variable source injections.
    SeedVariableSource,
    /// Implement static predicate rewrite outcomes.
    SeedSimplifiedPredicate,
    /// Implement logical element sources as LSM range access.
    SeedSourceAccess,
    /// Implement logical residual filters.
    SeedFilter,
    /// Implement logical order as an explicit sort.
    SeedOrder,
    /// Implement effectful logical barriers.
    SeedBarrier,
    /// Implement residual-free access paths.
    SeedAccessPath,
    /// Apply static windows directly to access paths.
    AccessWindow,
    /// Implement direct windows over access paths.
    SeedAccessWindow,
    /// Simplify residual filters over access.
    AccessFilterSimplification,
    /// Explore catalog-backed indexes for access filters.
    AccessFilterIndex,
    /// Implement residual access filters.
    SeedAccessFilter,
    /// Simplify residual-free access sets.
    AccessSetSimplification,
    /// Merge same-key range intersections.
    AccessRangeIntersection,
    /// Restrict equality branches by same-property ranges.
    AccessEqualityRangeIntersection,
    /// Remove equality union branches covered by ranges.
    AccessEqualityRangeUnion,
    /// Collapse contradictory residual-free access intersections.
    AccessContradiction,
    /// Remove access sources covered by wider sources.
    AccessSubsumption,
    /// Rewrite ordered range access to an opposite-direction index.
    AccessOrderRangeDirection,
    /// Elide ordering when access already delivers it.
    AccessOrder,
    /// Implement direct access ordering.
    SeedAccessOrder,
    /// Elide distinct when access is duplicate-free.
    AccessDistinct,
    /// Implement direct access distinct.
    SeedAccessDistinct,
    /// Rewrite a leading access-pipeline filter.
    AccessPipelineFilter,
    /// Rewrite or elide access-pipeline ordering.
    AccessPipelineOrder,
    /// Simplify access-rooted pipelines.
    AccessPipelineSimplification,
    /// Implement access-rooted pipelines.
    SeedAccessPipeline,
    /// Implement root stream pipelines.
    SeedRootPipeline,
    /// Implement root mutations.
    SeedRootMutation,
    /// Implement root index DDL.
    SeedRootIndexDdl,
    /// Implement root shortest path.
    SeedRootShortestPath,
    /// Collapse root control flow with statically empty input.
    RootControlFlowEmpty,
    /// Push access-filter rewrites through root-stream wrappers.
    RootStreamAccessRewrite,
    /// Implement root branch control flow.
    SeedRootBranch,
    /// Implement root repeat control flow.
    SeedRootRepeat,
    /// Implement reserved stream terminals.
    SeedStreamReserved,
    /// Implement projection stream terminals.
    SeedStreamProject,
    /// Implement aggregate stream terminals.
    SeedStreamAggregate,
    /// Implement state-writing variable terminals.
    SeedStreamVariableWrite,
    /// Compose adjacent static stream-window operators.
    StreamWindowComposition,
    /// Implement ordinary stream operators.
    SeedStream,
}
