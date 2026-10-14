//! Feature-gated vector benchmark support.

mod batch;
mod telemetry;

pub use batch::{
    VectorBatchBenchmarkCacheLimits, VectorBatchBenchmarkCase, VectorBatchBenchmarkFixture,
    VectorBatchBenchmarkMetric, VectorBatchBenchmarkSample, VectorBatchBenchmarkWorkload,
};
pub(crate) use telemetry::{
    observe_retained_payload, record_cache_stats, record_delete, record_dirty_neighbor_flush,
    record_multi_get, record_point_get, record_put, record_scan, reset, snapshot,
    VectorMutationBenchmarkTelemetry,
};
