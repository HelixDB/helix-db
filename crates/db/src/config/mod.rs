//! Configuration for Helix database

pub(crate) mod cache;
pub(crate) mod db;
mod definition_differences;
pub(crate) mod indexes;
pub(crate) mod runtime_catalog;
pub(crate) mod search_index_backfill;
pub(crate) mod secondary_backfill;
pub(crate) mod utils;

pub use crate::index_v2::ValidatedDynamicIndexDefinition;
pub use cache::{
    CacheConfig, CacheMode, ObjectStoreWarmLevel, SimHasherCacheSettings, SlateHybridCacheConfig,
    SlateObjectStoreCacheSettings, SlateRuntimeConfig, VectorMemoryBudget,
    VectorMemoryHydrationMode, VectorMemorySettings, DEFAULT_VECTOR_MEMORY_BUDGET_BYTES,
};
pub use db::{DbConfig, EdgeEncoding, EdgeUpdatePolicy, HelixConfig, OpenAttribution};
pub use definition_differences::{DefinitionDifference, NonEmptyDefinitionDifferences};
pub use indexes::{
    is_scoped_secondary_index_property, scoped_secondary_index_property,
    split_scoped_secondary_index_property, IndexConfig, RangeIndexDirection, RuntimeIndexCatalog,
    SecondaryIndexDefinition, SecondaryIndexElementType, SecondaryIndexKind, TextAnalyzerKind,
    TextElementType, TextIndexDefinition, VectorElementType, VectorIndexDefinition,
};
pub use search_index_backfill::{
    ActiveTextMutationLimits, SearchIndexBackfillLimitError, SearchIndexBackfillLimits,
    SearchIndexBatchLimits, TextBackfillCompactionLimits, TextBuildArtifactLimits,
};
pub use secondary_backfill::{
    SecondaryBackfillActiveIntervalMillis, SecondaryBackfillBatchRows,
    SecondaryBackfillIdleIntervalMillis, SecondaryBackfillTuning, SecondaryBackfillWorkerMode,
};
pub use utils::{ConfigError, ConfigResult, DiskCacheConfig, NonEmptyPathBuf};

#[cfg(test)]
pub(crate) mod tests;
