//! Cost formulas derived from `StorageCostProfile`.

use crate::properties::{KeyLocality, PositiveUsize};

use super::{
    parallel, ByteEstimate, CostVector, EstimatedRows, LatencyEstimate, StorageCostProfile,
    UniqueEqualityRows,
};

impl StorageCostProfile {
    /// Estimate rows for a unique equality lookup.
    ///
    /// Stats are clamped to the unique-index upper bound, while missing stats
    /// use the profile's bounded fallback knob.
    ///
    /// ```
    /// use helix_planner::cost::{EstimatedRows, StorageCostProfile, UniqueEqualityRows};
    ///
    /// let profile = StorageCostProfile {
    ///     default_unique_equality_rows: UniqueEqualityRows::ZERO,
    ///     ..StorageCostProfile::default()
    /// };
    ///
    /// assert_eq!(profile.unique_equality_rows(None), EstimatedRows::ZERO);
    /// assert_eq!(profile.unique_equality_rows(Some(10)), EstimatedRows::rows(1));
    /// ```
    pub fn unique_equality_rows(&self, cardinality: Option<u64>) -> EstimatedRows {
        cardinality.map_or(self.default_unique_equality_rows.estimated_rows(), |rows| {
            UniqueEqualityRows::clamp(EstimatedRows::rows(rows)).estimated_rows()
        })
    }

    /// Estimate rows for a non-unique equality lookup.
    ///
    /// Missing stats use the profile's tunable equality-index fallback instead
    /// of assuming equality behaves like a point lookup.
    pub fn equality_index_rows(&self, cardinality: Option<u64>) -> EstimatedRows {
        cardinality.map_or(self.default_equality_index_rows, EstimatedRows::rows)
    }

    /// Cost independent point gets as serial work.
    pub fn point_gets(&self, keys: PositiveUsize) -> CostVector {
        let key_count = keys.get() as u64;
        CostVector {
            latency: self
                .object_get_latency
                .saturating_add(self.sstable_filter_probe)
                .saturating_mul(key_count),
            object_reads: key_count,
            bytes: ByteEstimate::bytes(
                self.default_key_read_bytes
                    .as_bytes()
                    .saturating_mul(key_count),
            ),
            ..CostVector::ZERO
        }
    }

    /// Cost one `multi_get` batch.
    pub fn multi_get(&self, keys: PositiveUsize, locality: KeyLocality) -> CostVector {
        let key_count = keys.get() as u64;
        let locality_penalty = match locality {
            KeyLocality::Close => LatencyEstimate::ZERO,
            KeyLocality::Unknown | KeyLocality::Sparse => self.sstable_filter_probe,
        };
        CostVector {
            latency: self.multi_get_setup.saturating_add(
                self.multi_get_per_key
                    .saturating_add(locality_penalty)
                    .saturating_mul(key_count),
            ),
            object_reads: key_count,
            multi_get_calls: 1,
            bytes: ByteEstimate::bytes(
                self.default_key_read_bytes
                    .as_bytes()
                    .saturating_mul(key_count),
            ),
            ..CostVector::ZERO
        }
    }

    /// Cost one range scan.
    pub fn range_scan(&self, estimated_rows: EstimatedRows) -> CostVector {
        let rows = estimated_rows.as_rows();
        CostVector {
            latency: self
                .range_seek
                .saturating_add(self.range_next.saturating_mul(rows)),
            range_seeks: 1,
            range_nexts: rows,
            bytes: ByteEstimate::bytes(self.default_key_read_bytes.as_bytes().saturating_mul(rows)),
            ..CostVector::ZERO
        }
    }

    /// Cost an equality-index lookup over an estimated result count.
    pub fn equality_index_lookup(&self, estimated_rows: EstimatedRows) -> CostVector {
        self.range_scan(estimated_rows)
    }

    /// Cost residual predicate evaluation for a row estimate.
    pub fn predicate_eval(&self, rows: EstimatedRows) -> CostVector {
        let rows = rows.as_rows();
        CostVector {
            latency: self.cpu_predicate_eval.saturating_mul(rows),
            cpu_units: rows,
            ..CostVector::ZERO
        }
    }

    /// Cost a generic streaming operator over a row estimate.
    pub fn stream_operator(&self, rows: EstimatedRows) -> CostVector {
        let rows = rows.as_rows();
        CostVector {
            latency: self.stream_operator_eval.saturating_mul(rows),
            cpu_units: rows,
            ..CostVector::ZERO
        }
    }

    /// Cost an explicit sort/materialization operator over a row estimate.
    pub fn explicit_sort(&self, rows: EstimatedRows) -> CostVector {
        let rows = rows.as_rows();
        CostVector {
            latency: self
                .sort_setup
                .saturating_add(self.sort_per_row.saturating_mul(rows)),
            cpu_units: rows,
            bytes: self.default_materialized_row_bytes.saturating_mul(rows),
            peak_memory: self.default_materialized_row_bytes.saturating_mul(rows),
            ..CostVector::ZERO
        }
    }

    /// Cost a side-effect or materialization barrier.
    pub fn barrier(&self) -> CostVector {
        CostVector {
            latency: self.barrier_overhead,
            cpu_units: 1,
            ..CostVector::ZERO
        }
    }

    /// Cost injecting a variable/source state into a stream.
    pub fn source_inject(&self) -> CostVector {
        CostVector {
            latency: self.source_inject_overhead,
            cpu_units: 1,
            ..CostVector::ZERO
        }
    }

    /// Cost the executable wrapper around one `ForEach` body subplan.
    pub fn foreach_wrapper(&self) -> CostVector {
        CostVector {
            latency: self.foreach_overhead,
            cpu_units: 1,
            ..CostVector::ZERO
        }
    }

    /// Cost scheduling a parallel executable step with the selected width.
    pub fn parallel_task_overhead(&self, parallel_width: PositiveUsize) -> CostVector {
        CostVector {
            latency: self
                .task_overhead
                .saturating_mul(parallel_width.get() as u64),
            parallel_width: parallel_width.get(),
            ..CostVector::ZERO
        }
    }

    /// Cost parallel execution with bounded concurrency.
    pub fn parallel(&self, children: &[CostVector], max_concurrency: PositiveUsize) -> CostVector {
        let mut total = CostVector::ZERO;
        let mut critical_path = LatencyEstimate::ZERO;
        for child in children {
            total = CostVector {
                latency: LatencyEstimate::ZERO,
                ..total
            }
            .serial(*child);
            critical_path = critical_path.max(child.latency);
        }
        let parallel_width = children.len().min(max_concurrency.get()).max(1);
        let peak_memory = parallel::bounded_peak_memory(children, max_concurrency);
        CostVector {
            latency: critical_path.saturating_add(
                self.parallel_task_overhead(PositiveUsize::at_least_one(parallel_width))
                    .latency,
            ),
            peak_memory,
            parallel_width,
            ..total
        }
    }

    /// Batch size used by multi-get coalescing for a locality class.
    pub const fn multi_get_batch_size(&self, locality: KeyLocality) -> PositiveUsize {
        match locality {
            KeyLocality::Close => self.close_key_multi_get_batch,
            KeyLocality::Unknown | KeyLocality::Sparse => self.sparse_key_multi_get_batch,
        }
    }
}
