//! Scalability fixture families.

use serde::{Deserialize, Serialize};

/// Scalability fixture family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanningScalabilityShape {
    /// Wide conjunctive predicates over many indexed properties.
    WideBooleanPredicates,
    /// Many catalog indexes where only a small subset should be relevant.
    ManyAvailableIndexes,
    /// Many batch entries that reuse one native root.
    BatchedRootReuse,
    /// Many `ForEach` body entries that reuse one native root.
    ForEachBodyRootReuse,
    /// Long source-rooted traversal chains.
    DeepTraversalChain,
    /// Many viable indexed alternatives inside one memo group family.
    ManyMemoAlternatives,
    /// Wide indexed disjunction forced past the configured union branch limit.
    OverLimitIndexDisjunction,
    /// Root-level branch fanout over selected child subplans.
    BranchHeavyQueries,
    /// Many ordered range-index roots with semantic range suffixes and read caps.
    OrderedRangeWindowPushdown,
    /// Many write entries mixing source mutations, indexed updates, and edge writes.
    MutationHeavyBatches,
    /// Many secondary, vector-search, and text-search index DDL entries.
    SearchIndexDdlWorkloads,
    /// Mixed read/write/search/variable batches shaped like query-service traffic.
    RuntimeDerivedMixedQueries,
}
