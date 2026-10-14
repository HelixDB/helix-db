//! Benchmark-build instrumentation (feature `async-index-benchmark`).
//!
//! Process-wide counters for storage I/O that the product build does not
//! collect: object-store HTTP attempts below `object_store`'s retry loop and
//! SlateDB's own recorder (flushes, compaction bytes, per-attempt object-store
//! requests by component). A benchmark process opens one database, so both
//! are process-global; they are cumulative and never reset.

pub(crate) mod io;

use std::sync::{Arc, OnceLock};

use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};

static STORAGE_METRICS: OnceLock<Arc<DefaultMetricsRecorder>> = OnceLock::new();

/// Returns the recorder every benchmark-build SlateDB handle reports into.
pub(crate) fn storage_recorder() -> Arc<DefaultMetricsRecorder> {
    Arc::clone(STORAGE_METRICS.get_or_init(|| Arc::new(DefaultMetricsRecorder::new())))
}

/// Returns cumulative object-store connector counters grouped by service and
/// method. Byte counts are request bodies offered and response bodies
/// delivered to the client, not network-wire bytes.
pub fn connector_counters() -> serde_json::Value {
    io::snapshot()
}

/// Returns every SlateDB metric recorded so far as
/// `[{name, labels, value}]`, with histograms expanded to their buckets.
pub fn storage_metrics() -> serde_json::Value {
    let Some(recorder) = STORAGE_METRICS.get() else {
        return serde_json::Value::Array(Vec::new());
    };
    recorder
        .snapshot()
        .all()
        .iter()
        .map(|metric| {
            let value = match &metric.value {
                MetricValue::Counter(value) => serde_json::json!(value),
                MetricValue::Gauge(value) | MetricValue::UpDownCounter(value) => {
                    serde_json::json!(value)
                }
                MetricValue::Histogram {
                    count,
                    sum,
                    min,
                    max,
                    boundaries,
                    bucket_counts,
                } => serde_json::json!({
                    "count": count,
                    "sum": sum,
                    "min": min,
                    "max": max,
                    "boundaries": boundaries,
                    "bucket_counts": bucket_counts,
                }),
            };
            serde_json::json!({
                "name": metric.name,
                "labels": metric.labels,
                "value": value,
            })
        })
        .collect()
}
