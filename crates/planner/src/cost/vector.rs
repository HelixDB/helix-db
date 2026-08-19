use serde::{Deserialize, Serialize};

use super::units::{ByteEstimate, LatencyEstimate};

/// Multi-dimensional cost used by the optimizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostVector {
    /// Estimated critical-path latency.
    pub latency: LatencyEstimate,
    /// Object-store or LSM read operations.
    pub object_reads: u64,
    /// Authoritative graph-property reads used to verify index candidates.
    #[serde(default)]
    pub authoritative_graph_reads: u64,
    /// `multi_get` calls.
    pub multi_get_calls: u64,
    /// Range scan seek operations.
    pub range_seeks: u64,
    /// Range scan next/row steps.
    pub range_nexts: u64,
    /// CPU work in abstract units.
    pub cpu_units: u64,
    /// Estimated bytes read or materialized.
    pub bytes: ByteEstimate,
    /// Estimated peak memory held while this work executes.
    pub peak_memory: ByteEstimate,
    /// Maximum parallel width.
    pub parallel_width: usize,
}

impl CostVector {
    /// Zero cost.
    pub const ZERO: Self = Self {
        latency: LatencyEstimate::ZERO,
        object_reads: 0,
        authoritative_graph_reads: 0,
        multi_get_calls: 0,
        range_seeks: 0,
        range_nexts: 0,
        cpu_units: 0,
        bytes: ByteEstimate::ZERO,
        peak_memory: ByteEstimate::ZERO,
        parallel_width: 1,
    };

    /// Serial composition.
    pub fn serial(self, rhs: Self) -> Self {
        Self {
            latency: self.latency.saturating_add(rhs.latency),
            object_reads: self.object_reads.saturating_add(rhs.object_reads),
            authoritative_graph_reads: self
                .authoritative_graph_reads
                .saturating_add(rhs.authoritative_graph_reads),
            multi_get_calls: self.multi_get_calls.saturating_add(rhs.multi_get_calls),
            range_seeks: self.range_seeks.saturating_add(rhs.range_seeks),
            range_nexts: self.range_nexts.saturating_add(rhs.range_nexts),
            cpu_units: self.cpu_units.saturating_add(rhs.cpu_units),
            bytes: self.bytes.saturating_add(rhs.bytes),
            peak_memory: self.peak_memory.max(rhs.peak_memory),
            parallel_width: self.parallel_width.max(rhs.parallel_width),
        }
    }

    /// Saturating multiplication by a repeat count.
    pub fn saturating_mul(self, rhs: u64) -> Self {
        Self {
            latency: self.latency.saturating_mul(rhs),
            object_reads: self.object_reads.saturating_mul(rhs),
            authoritative_graph_reads: self.authoritative_graph_reads.saturating_mul(rhs),
            multi_get_calls: self.multi_get_calls.saturating_mul(rhs),
            range_seeks: self.range_seeks.saturating_mul(rhs),
            range_nexts: self.range_nexts.saturating_mul(rhs),
            cpu_units: self.cpu_units.saturating_mul(rhs),
            bytes: self.bytes.saturating_mul(rhs),
            peak_memory: self.peak_memory,
            parallel_width: self.parallel_width,
        }
    }
}
