//! Cost formulas derived from `StorageCostProfile`.

use helix_ast::expr::Predicate;

use crate::cost::{MembershipStream, RECORD_BATCH_ROWS};
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

    /// Cost a label bitmap followed by graph-row reads and row construction.
    ///
    /// Label access uses the shared equality bitmap, then checks each graph
    /// row before emitting it. Unlike managed equality access, these IDs do
    /// not bypass graph-row reads. Charge the row read/decode budget even
    /// when statistics are missing; residual predicates are charged separately.
    ///
    /// ```
    /// use helix_planner::cost::{EstimatedRows, StorageCostProfile};
    /// let profile = StorageCostProfile::default();
    /// let cost = profile.label_scan(EstimatedRows::rows(1000));
    /// assert_eq!(cost.object_reads, 1001);
    /// assert_eq!(cost.authoritative_graph_reads, 1000);
    /// assert_eq!(cost.range_nexts, 0);
    /// ```
    pub fn label_scan(&self, rows: EstimatedRows) -> CostVector {
        self.bitmap_equality_lookup(rows)
            .serial(self.authoritative_verification(rows))
            .serial(self.secondary_row_materialization(rows))
    }

    /// Cost an element-ID scan followed by graph-row reads and row construction.
    /// This is distinct from an index range scan, whose driver may defer graph
    /// verification until after membership filtering.
    pub fn element_scan(&self, rows: EstimatedRows) -> CostVector {
        self.range_scan(rows)
            .serial(self.authoritative_verification(rows))
            .serial(self.secondary_row_materialization(rows))
    }

    /// Estimate traversal independently of the physical index direction.
    /// This charges every estimated visited entry, never just the output limit.
    pub fn ordered_range_scan(
        &self,
        rows: EstimatedRows,
        iteration: crate::ir::RangeScanIteration,
    ) -> CostVector {
        let mut cost = self.range_scan(rows);
        if iteration == crate::ir::RangeScanIteration::Reverse {
            let extra = u128::from(self.reverse_range_per_1000.as_micros())
                * u128::from(rows.as_rows())
                / 1_000;
            cost.latency = cost.latency.saturating_add(LatencyEstimate::micros(
                extra.min(u128::from(u64::MAX)) as u64,
            ));
        }
        cost
    }

    /// Cost property sort-key reads followed by the existing comparator sort.
    /// Property decoding uses the same per-row estimate as authoritative checks.
    pub fn property_sort(&self, rows: EstimatedRows) -> CostVector {
        self.authoritative_verification(rows)
            .serial(self.explicit_sort(rows))
    }

    /// Cost an equality-index lookup over an estimated result count.
    pub fn equality_index_lookup(&self, estimated_rows: EstimatedRows) -> CostVector {
        self.bitmap_equality_lookup(estimated_rows)
            .serial(self.secondary_row_materialization(estimated_rows))
    }

    /// Cost one V4 non-unique equality bitmap point read and decode.
    pub fn bitmap_equality_lookup(&self, estimated_rows: EstimatedRows) -> CostVector {
        let rows = estimated_rows.as_rows();
        CostVector {
            latency: self
                .object_get_latency
                .saturating_add(self.sstable_filter_probe)
                .saturating_add(self.bitmap_decode_per_id.saturating_mul(rows)),
            object_reads: 1,
            cpu_units: rows,
            bytes: self.default_bitmap_id_bytes.saturating_mul(rows),
            peak_memory: self.default_bitmap_id_bytes.saturating_mul(rows),
            ..CostVector::ZERO
        }
    }

    /// Cost one close-key `multi_get` and decode for batched V4 bitmaps.
    pub fn bitmap_equality_batch(
        &self,
        values: PositiveUsize,
        estimated_rows: EstimatedRows,
    ) -> CostVector {
        let rows = estimated_rows.as_rows();
        self.multi_get(values, KeyLocality::Close)
            .serial(CostVector {
                latency: self.bitmap_decode_per_id.saturating_mul(rows),
                cpu_units: rows,
                bytes: self.default_bitmap_id_bytes.saturating_mul(rows),
                peak_memory: self.default_bitmap_id_bytes.saturating_mul(rows),
                ..CostVector::ZERO
            })
    }

    /// Cost a unique-owner point read plus authoritative property verification.
    pub fn unique_equality_lookup(&self, estimated_rows: EstimatedRows) -> CostVector {
        self.point_gets(PositiveUsize::at_least_one(1))
            .serial(self.authoritative_verification(estimated_rows))
    }

    /// Cost a unique owner multi-get and same-snapshot authoritative checks.
    ///
    /// ```
    /// use helix_planner::{cost, properties};
    /// let profile = cost::StorageCostProfile::default();
    /// let batch = profile.unique_equality_batch(
    ///     properties::PositiveUsize::at_least_one(5), cost::EstimatedRows::rows(5));
    /// assert_eq!(batch.multi_get_calls, 1);
    /// assert_eq!(batch.authoritative_graph_reads, 5);
    /// assert_eq!(batch.object_reads, 10);
    /// ```
    pub fn unique_equality_batch(&self, values: PositiveUsize, rows: EstimatedRows) -> CostVector {
        self.multi_get(values, KeyLocality::Close)
            .serial(self.authoritative_verification(rows))
            .serial(self.secondary_set_operation(rows))
    }

    /// Cost an authoritative graph scan used for null equality.
    pub fn null_equality_scan(&self, scanned_rows: EstimatedRows) -> CostVector {
        self.range_scan(scanned_rows)
            .serial(self.predicate_eval(scanned_rows))
            .serial(CostVector {
                authoritative_graph_reads: scanned_rows.as_rows(),
                ..CostVector::ZERO
            })
    }

    /// Cost a V2 range scan plus authoritative verification of every candidate.
    pub fn secondary_range_lookup(&self, estimated_rows: EstimatedRows) -> CostVector {
        self.range_scan(estimated_rows)
            .serial(self.authoritative_verification(estimated_rows))
    }

    /// Cost authoritative graph-property verification for candidate IDs.
    pub fn authoritative_verification(&self, estimated_rows: EstimatedRows) -> CostVector {
        let rows = estimated_rows.as_rows();
        CostVector {
            latency: self.authoritative_verify_per_id.saturating_mul(rows),
            object_reads: rows,
            authoritative_graph_reads: rows,
            cpu_units: rows,
            bytes: self.default_key_read_bytes.saturating_mul(rows),
            ..CostVector::ZERO
        }
    }

    /// Cost a bitmap/set operation over candidate IDs.
    pub fn secondary_set_operation(&self, estimated_rows: EstimatedRows) -> CostVector {
        let rows = estimated_rows.as_rows();
        CostVector {
            latency: self.secondary_set_per_id.saturating_mul(rows),
            cpu_units: rows,
            peak_memory: self.default_bitmap_id_bytes.saturating_mul(rows),
            ..CostVector::ZERO
        }
    }

    /// Cost constructing verified result rows after all ID-set work is complete.
    pub fn secondary_row_materialization(&self, estimated_rows: EstimatedRows) -> CostVector {
        let rows = estimated_rows.as_rows();
        CostVector {
            latency: self
                .secondary_row_materialization_per_id
                .saturating_mul(rows),
            cpu_units: rows,
            bytes: self.default_materialized_row_bytes.saturating_mul(rows),
            peak_memory: self.default_materialized_row_bytes.saturating_mul(rows),
            ..CostVector::ZERO
        }
    }

    /// Cost a residual filter whose predicate reads each row's stored record.
    ///
    /// Every input row pays one authoritative record read and decode before
    /// the predicate is evaluated, so the cost grows with the input stream.
    ///
    /// ```
    /// use helix_planner::cost::{EstimatedRows, StorageCostProfile};
    /// let profile = StorageCostProfile::default();
    /// let cost = profile.stored_predicate_filter(EstimatedRows::rows(1_000));
    /// assert_eq!(cost.authoritative_graph_reads, 1_000);
    /// assert_eq!(cost.object_reads, 1_000);
    /// ```
    pub fn stored_predicate_filter(&self, rows: EstimatedRows) -> CostVector {
        self.authoritative_verification(rows)
            .serial(self.predicate_eval(rows))
    }

    /// Cost a row-preserving node index membership filter by what the interpreter
    /// does for `stream`.
    ///
    /// The interpreter evaluates a stream of at most [`RECORD_BATCH_ROWS`] node
    /// rows row by row, exactly like the per-row filter over `predicate`, and
    /// never reads the set. Past one batch it reads the secondary set once per
    /// request, concurrently with the `label_domain` bitmap for an unscoped
    /// predicate, probes it once per row, and evaluates only rows of other labels.
    ///
    /// Row by row, membership does the work of
    /// [`residual_filter`](Self::residual_filter) over `predicate`, the price of
    /// the filter it replaces, so both sides of the choice count the same blob
    /// reads and predicate leaves.
    ///
    /// * Within one batch, or an empty stream: the filter's work plus the set
    ///   read. Membership can only match the filter there, so it never wins.
    /// * An unbounded stream estimated within one batch: the filter's work less
    ///   one predicate-leaf evaluation. At the estimate both plans do the same
    ///   work, but only the membership stops reading records if the stream
    ///   outgrows the estimate. The credit is the smallest unit of that work: it
    ///   settles this tie under every profile, since it always lowers the CPU
    ///   units, and never outweighs a record read, such as the second read of a
    ///   kept row by a residual filter behind the membership.
    /// * Past one batch: the set reads plus one probe per row. An unscoped
    ///   predicate rewrites only when exactly one label's index answers it, so its
    ///   rows are priced as that label's. A row of another label costs one probe
    ///   more than the filter, after reads made at most once per request.
    ///
    /// ```
    /// use helix_ast::expr::Predicate;
    /// use helix_planner::cost::{
    ///     EstimatedRows, MembershipStream, RecordBatchRows, StorageCostProfile,
    /// };
    /// let profile = StorageCostProfile::default();
    /// let set = profile.bitmap_equality_lookup(EstimatedRows::rows(10));
    /// let label = profile.bitmap_equality_lookup(EstimatedRows::rows(1_000));
    /// let kind = Predicate::eq("kind", "B");
    /// let f = |n| profile.residual_filter(&kind, EstimatedRows::rows(n));
    /// let unbounded = |n| MembershipStream::MayExceedOneBatch(EstimatedRows::rows(n));
    ///
    /// for label_domain in [None, Some(label)] {
    ///     // An unbounded stream estimated within one batch: one leaf evaluation less.
    ///     let cost = profile.index_membership_filter(&kind, set, label_domain, unbounded(10));
    ///     assert_eq!(
    ///         cost.latency.as_micros(),
    ///         f(10).latency.as_micros() - profile.cpu_predicate_eval.as_micros()
    ///     );
    ///     assert_eq!(cost.object_reads, 10);
    ///     assert_eq!(cost.cpu_units + 1, f(10).cpu_units);
    ///
    ///     // A stream proven to fit in one batch, or an empty one, keeps the filter.
    ///     let bounded = MembershipStream::WithinOneBatch(RecordBatchRows::at_most(10));
    ///     let cost = profile.index_membership_filter(&kind, set, label_domain, bounded);
    ///     assert!(cost.latency > f(10).latency);
    ///     let cost = profile.index_membership_filter(&kind, set, label_domain, unbounded(0));
    ///     assert!(cost.latency > f(0).latency);
    ///
    ///     // Past one batch the set reads amortize over the stream.
    ///     let cost = profile.index_membership_filter(&kind, set, label_domain, unbounded(1_000));
    ///     assert!(cost.latency < f(1_000).latency);
    /// }
    ///
    /// let unscoped = profile.index_membership_filter(&kind, set, Some(label), unbounded(1_000));
    /// assert_eq!(unscoped.object_reads, 2);
    /// assert_eq!(unscoped.authoritative_graph_reads, 0);
    ///
    /// // Just past one batch the set read outweighs the record reads it saves.
    /// let scoped = profile.index_membership_filter(&kind, set, None, unbounded(257));
    /// assert_eq!(scoped.latency.as_micros(), 5_317);
    /// assert_eq!(f(257).latency.as_micros(), 2_827);
    ///
    /// // Every leaf of a label-scoped predicate is evaluated row by row.
    /// let scoped = Predicate::and(vec![Predicate::eq("$label", "Attribute"), kind]);
    /// let rows = EstimatedRows::rows(10);
    /// let bounded = MembershipStream::WithinOneBatch(RecordBatchRows::at_most(10));
    /// assert_eq!(
    ///     profile.index_membership_filter(&scoped, set, None, bounded),
    ///     profile.residual_filter(&scoped, rows).serial(set)
    /// );
    /// ```
    pub fn index_membership_filter(
        &self,
        predicate: &Predicate,
        set: CostVector,
        label_domain: Option<CostVector>,
        stream: MembershipStream,
    ) -> CostVector {
        match stream {
            MembershipStream::MayExceedOneBatch(rows) if rows.as_rows() > RECORD_BATCH_ROWS => {
                label_domain
                    .map_or(set, |label| {
                        self.parallel(&[set, label], PositiveUsize::at_least_one(2))
                    })
                    .serial(self.secondary_set_operation(rows))
            }
            MembershipStream::MayExceedOneBatch(rows) => match rows.as_rows() {
                0 => set,
                _ => self.residual_filter_less_leaves(predicate, rows, 1),
            },
            MembershipStream::WithinOneBatch(rows) => self
                .residual_filter(predicate, rows.estimated_rows())
                .serial(set),
        }
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
