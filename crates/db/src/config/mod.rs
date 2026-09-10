//! Configuration for Helix database

pub(crate) mod cache;
pub(crate) mod db;
mod definition_differences;
mod index_lifecycle_throughput;
pub(crate) mod indexes;
mod migrations;
pub(crate) mod runtime_catalog;
pub(crate) mod search_index_backfill;
pub(crate) mod secondary_index_lifecycle;
pub(crate) mod utils;

pub use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
pub use cache::{
    CacheConfig, CacheMode, CacheWarmMode, FtsHybridCacheConfig, FtsMemoryCacheConfig,
    FtsWarmConfig, ObjectStoreWarmLevel, SimHasherCacheSettings, SlateHybridCacheConfig,
    SlateMemoryCacheConfig, SlateObjectStoreCacheSettings, SlateRuntimeConfig, SlateWarmConfig,
    VectorMemoryBudget, VectorMemoryHydrationMode, VectorMemorySettings,
    DEFAULT_VECTOR_MEMORY_BUDGET_BYTES,
};
pub use db::{
    DbConfig, EdgeEncoding, EdgeUpdatePolicy, HelixConfig, OpenAttribution, QueryTelemetry,
};
pub use definition_differences::{DefinitionDifference, NonEmptyDefinitionDifferences};
pub use index_lifecycle_throughput::{
    IndexLifecycleConcurrency, IndexLifecycleScanTuning, IndexLifecycleThroughputTuning,
    IndexLifecycleThroughputTuningError,
};
pub(crate) use indexes::RuntimeIndexCatalog;
pub use indexes::{
    is_scoped_secondary_index_property, scoped_secondary_index_property,
    split_scoped_secondary_index_property, RangeIndexDirection, SecondaryIndexDefinition,
    SecondaryIndexElementType, SecondaryIndexKind, TextAnalyzerKind, TextElementType,
    TextIndexDefinition, VectorElementType, VectorIndexDefinition,
};
pub use migrations::{
    MigrationActiveIntervalMillis, MigrationBatchBytes, MigrationBatchRows,
    MigrationIdleIntervalMillis, MigrationTuning, MigrationWorkerMode,
};
pub use search_index_backfill::{
    ActiveTextMutationLimits, SearchIndexBackfillLimitError, SearchIndexBackfillLimits,
    SearchIndexBatchLimits, TextBackfillCompactionLimits, TextBuildArtifactLimits,
};
pub use secondary_index_lifecycle::{
    SecondaryIndexLifecycleActiveIntervalMillis, SecondaryIndexLifecycleBatchRows,
    SecondaryIndexLifecycleCatchUpTailDelayMillis, SecondaryIndexLifecycleIdleIntervalMillis,
    SecondaryIndexLifecycleTuning, SecondaryIndexLifecycleWorkerMode,
};
pub use utils::{ConfigError, ConfigResult, DiskCacheConfig, NonEmptyPathBuf};

#[cfg(test)]
pub(crate) mod tests;
