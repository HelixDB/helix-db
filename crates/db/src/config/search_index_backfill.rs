//! Validated runtime resource policy for text and vector backfill.
//!
//! [`SearchIndexBackfillLimits`] groups common source/transaction limits with
//! text artifact and compaction budgets. Nested types keep unrelated resources
//! out of one flat constructor and reject contradictory vector transaction
//! limits before a builder reads source data. Edge property batches remain one
//! row until storage can stop on returned bytes; accepting a larger count today
//! would claim a memory bound that SlateDB's current `multi_get` cannot prove.
//!
//! These limits are runtime policy only. They are never serialized into vector
//! rows, text manifests/splits/live state, or canonical lifecycle records, so
//! changing them does not alter an on-disk format.
//!
//! Builders obtain one validated policy from [`crate::config::DbConfig`] and
//! use its nested budgets when admitting source rows, staging index writes,
//! emitting V2 text build artifacts, and compacting those artifacts.
//! Keeping validation here prevents each worker from interpreting an
//! internally inconsistent set of independent integer settings.
//!
//! # Usage
//!
//! ```
//! use db::config::{DbConfig, SearchIndexBackfillLimits};
//!
//! let limits = SearchIndexBackfillLimits::default();
//! let config = DbConfig::new().with_search_index_backfill_limits(limits);
//!
//! assert_eq!(config.search_index_backfill(), limits);
//! ```

use std::num::{NonZeroU64, NonZeroUsize};

const DEFAULT_BATCH_ENTITIES: usize = 512;
const DEFAULT_INPUT_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_OUTPUT_OPERATIONS: u64 = 4_096;
const DEFAULT_OUTPUT_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_SINGLE_VECTOR_OUTPUT_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_TEXT_ARTIFACT_ENTRIES: usize = 512;
const DEFAULT_TEXT_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024;
const DEFAULT_TEXT_COMPACTION_FAN_IN: usize = 32;
const DEFAULT_TEXT_COMPACTION_INPUT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_TEXT_COMPACTION_TEMP_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_TEXT_COMPACTION_OUTPUT_BLOB_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_TEXT_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;

/// Positive common source and transaction limits for text/vector builders.
///
/// A worker uses the entity and input-byte limits while reading a source batch,
/// then uses the operation and output-byte limits before staging that batch's
/// transaction. The single-vector limit lets vector builders fail one entity
/// explicitly when an indivisible HNSW insertion cannot fit the transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SearchIndexBatchLimits {
    max_entities: NonZeroUsize,
    max_input_bytes: NonZeroU64,
    max_output_operations: NonZeroU64,
    max_output_bytes: NonZeroU64,
    max_single_vector_output_bytes: NonZeroU64,
}

impl SearchIndexBatchLimits {
    /// Constructs common limits and rejects an impossible per-vector ceiling.
    ///
    /// Call this at configuration time, before a lifecycle worker reads source
    /// data. The returned value guarantees that any vector accepted by the
    /// single-vector ceiling can also fit the configured transaction ceiling.
    pub fn try_new(
        max_entities: NonZeroUsize,
        max_input_bytes: NonZeroU64,
        max_output_operations: NonZeroU64,
        max_output_bytes: NonZeroU64,
        max_single_vector_output_bytes: NonZeroU64,
    ) -> Result<Self, SearchIndexBackfillLimitError> {
        if max_single_vector_output_bytes > max_output_bytes {
            return Err(
                SearchIndexBackfillLimitError::SingleVectorExceedsTransactionBytes {
                    single_vector: max_single_vector_output_bytes,
                    transaction: max_output_bytes,
                },
            );
        }
        Ok(Self {
            max_entities,
            max_input_bytes,
            max_output_operations,
            max_output_bytes,
            max_single_vector_output_bytes,
        })
    }

    /// Returns the maximum decoded source entities retained in one batch.
    pub const fn max_entities(self) -> NonZeroUsize {
        self.max_entities
    }

    /// Returns the maximum decoded source bytes retained in one batch.
    pub const fn max_input_bytes(self) -> NonZeroU64 {
        self.max_input_bytes
    }

    /// Returns the maximum puts/deletes staged by one transaction.
    pub const fn max_output_operations(self) -> NonZeroU64 {
        self.max_output_operations
    }

    /// Returns the maximum encoded bytes staged by one transaction.
    pub const fn max_output_bytes(self) -> NonZeroU64 {
        self.max_output_bytes
    }

    /// Returns the complete output cap for one atomic vector insertion.
    pub const fn max_single_vector_output_bytes(self) -> NonZeroU64 {
        self.max_single_vector_output_bytes
    }
}

/// Positive bounds for one V2 text build-artifact batch.
///
/// Text builders use these limits only while a generation is `Building`; the
/// individually keyed artifact rows are temporary lifecycle records and remain
/// independent of the prepared manifest root/pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TextBuildArtifactLimits {
    max_entries: NonZeroUsize,
    max_bytes: NonZeroU64,
}

impl TextBuildArtifactLimits {
    /// Constructs a complete positive artifact-page budget.
    ///
    /// Use this value when deciding whether another individually keyed artifact
    /// can be admitted to the current bounded batch.
    pub const fn new(max_entries: NonZeroUsize, max_bytes: NonZeroU64) -> Self {
        Self {
            max_entries,
            max_bytes,
        }
    }

    /// Returns the maximum split references retained by one artifact batch.
    pub const fn max_entries(self) -> NonZeroUsize {
        self.max_entries
    }

    /// Returns the maximum encoded bytes retained by one artifact batch.
    pub const fn max_bytes(self) -> NonZeroU64 {
        self.max_bytes
    }
}

/// Positive resource bounds for one text build compaction pass.
///
/// The text lifecycle worker applies these independent ceilings when selecting
/// compaction inputs, reserving temporary disk, writing an immutable blob, and
/// encoding one bounded V2 manifest page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TextBackfillCompactionLimits {
    max_fan_in: NonZeroUsize,
    max_input_bytes: NonZeroU64,
    max_temporary_disk_bytes: NonZeroU64,
    max_output_blob_bytes: NonZeroU64,
    max_manifest_bytes: NonZeroU64,
}

impl TextBackfillCompactionLimits {
    /// Constructs one complete positive compaction and final-manifest budget.
    ///
    /// This constructor records positive resource ceilings. Cross-budget
    /// relationships that depend on the transaction budget are checked by
    /// [`SearchIndexBackfillLimits::try_new`].
    pub const fn new(
        max_fan_in: NonZeroUsize,
        max_input_bytes: NonZeroU64,
        max_temporary_disk_bytes: NonZeroU64,
        max_output_blob_bytes: NonZeroU64,
        max_manifest_bytes: NonZeroU64,
    ) -> Self {
        Self {
            max_fan_in,
            max_input_bytes,
            max_temporary_disk_bytes,
            max_output_blob_bytes,
            max_manifest_bytes,
        }
    }

    /// Returns the maximum split/blob inputs merged by one pass.
    pub const fn max_fan_in(self) -> NonZeroUsize {
        self.max_fan_in
    }

    /// Returns the maximum input bytes read by one pass.
    pub const fn max_input_bytes(self) -> NonZeroU64 {
        self.max_input_bytes
    }

    /// Returns the maximum pass-owned temporary disk bytes.
    pub const fn max_temporary_disk_bytes(self) -> NonZeroU64 {
        self.max_temporary_disk_bytes
    }

    /// Returns the maximum immutable output blob size.
    pub const fn max_output_blob_bytes(self) -> NonZeroU64 {
        self.max_output_blob_bytes
    }

    /// Returns the hard encoded V2 manifest-page ceiling.
    pub const fn max_manifest_bytes(self) -> NonZeroU64 {
        self.max_manifest_bytes
    }
}

/// Complete validated limits for streaming text/vector lifecycle work.
///
/// Store this policy in [`crate::config::DbConfig`] and share it across text and
/// vector lifecycle drivers. This is deliberately a runtime contract: it
/// controls work admission without being encoded in lifecycle or index rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SearchIndexBackfillLimits {
    batch: SearchIndexBatchLimits,
    edge_property_read_batch: NonZeroUsize,
    text_artifacts: TextBuildArtifactLimits,
    text_compaction: TextBackfillCompactionLimits,
}

/// Complete runtime-only limits for one request-owned Active text mutation.
///
/// The view reuses the transaction-output policy plus the immutable split,
/// manifest-page, and compaction-input ceilings already required by text
/// lifecycle work. Active attachment may read a maximum-size manifest page once
/// for planning and again for post-publication revalidation, so its input budget
/// cannot be the smaller one-pass batch-input ceiling. This policy is never
/// serialized into an index row or split, so changing it does not change the
/// on-disk format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActiveTextMutationLimits {
    max_input_bytes: NonZeroU64,
    batch: SearchIndexBatchLimits,
    max_split_bytes: NonZeroU64,
    max_manifest_page_bytes: NonZeroU64,
}

impl ActiveTextMutationLimits {
    /// Projects the exact Active-request limits from one validated backfill policy.
    pub const fn from_backfill(limits: SearchIndexBackfillLimits) -> Self {
        Self {
            max_input_bytes: limits.text_compaction.max_input_bytes,
            batch: limits.batch,
            max_split_bytes: limits.text_compaction.max_output_blob_bytes,
            max_manifest_page_bytes: limits.text_compaction.max_manifest_bytes,
        }
    }

    /// Returns the maximum serialized bytes read by one text mutation plan.
    pub const fn max_input_bytes(self) -> NonZeroU64 {
        self.max_input_bytes
    }

    /// Returns the maximum database writes across the preflighted request.
    pub const fn max_output_operations(self) -> NonZeroU64 {
        self.batch.max_output_operations
    }

    /// Returns the maximum serialized key/value bytes written across that request.
    pub const fn max_output_bytes(self) -> NonZeroU64 {
        self.batch.max_output_bytes
    }

    /// Returns the maximum immutable split payload admitted before publication.
    pub const fn max_split_bytes(self) -> NonZeroU64 {
        self.max_split_bytes
    }

    /// Returns the hard encoded V2 manifest-page value ceiling.
    pub const fn max_manifest_page_bytes(self) -> NonZeroU64 {
        self.max_manifest_page_bytes
    }
}

impl SearchIndexBackfillLimits {
    /// Combines all budgets and validates their cross-component constraints.
    ///
    /// The edge-property width is currently required to be one because the
    /// underlying `multi_get` API cannot stop after a returned-byte ceiling.
    /// Artifact pages and the final current-format text manifest must also fit
    /// the transaction that publishes them. Rejecting those configurations
    /// here keeps workers from starting work that cannot be committed safely.
    pub fn try_new(
        batch: SearchIndexBatchLimits,
        edge_property_read_batch: NonZeroUsize,
        text_artifacts: TextBuildArtifactLimits,
        text_compaction: TextBackfillCompactionLimits,
    ) -> Result<Self, SearchIndexBackfillLimitError> {
        if edge_property_read_batch != NonZeroUsize::MIN {
            return Err(SearchIndexBackfillLimitError::EdgeReadIsNotByteBounded {
                requested_rows: edge_property_read_batch,
            });
        }
        if text_artifacts.max_bytes() > batch.max_output_bytes() {
            return Err(
                SearchIndexBackfillLimitError::ArtifactPageExceedsTransactionBytes {
                    artifact_page: text_artifacts.max_bytes(),
                    transaction: batch.max_output_bytes(),
                },
            );
        }
        if text_compaction.max_manifest_bytes() > batch.max_output_bytes() {
            return Err(
                SearchIndexBackfillLimitError::ManifestExceedsTransactionBytes {
                    manifest: text_compaction.max_manifest_bytes(),
                    transaction: batch.max_output_bytes(),
                },
            );
        }
        Ok(Self {
            batch,
            edge_property_read_batch,
            text_artifacts,
            text_compaction,
        })
    }

    /// Returns the source-admission and transaction-staging limits used by both
    /// text and vector builders.
    pub const fn batch(self) -> SearchIndexBatchLimits {
        self.batch
    }

    /// Returns the edge-property read width used to preserve the input-byte
    /// bound until storage provides a byte-stopping multi-read API.
    pub const fn edge_property_read_batch(self) -> NonZeroUsize {
        self.edge_property_read_batch
    }

    /// Returns the limits used to page temporary V2 text build-artifact rows.
    pub const fn text_artifacts(self) -> TextBuildArtifactLimits {
        self.text_artifacts
    }

    /// Returns the limits used for bounded text compaction and V2 manifest-page
    /// preparation.
    pub const fn text_compaction(self) -> TextBackfillCompactionLimits {
        self.text_compaction
    }

    /// Returns the runtime-only request admission view for Active text updates.
    pub const fn active_text_mutation(self) -> ActiveTextMutationLimits {
        ActiveTextMutationLimits::from_backfill(self)
    }
}

impl Default for SearchIndexBackfillLimits {
    fn default() -> Self {
        Self::try_new(
            SearchIndexBatchLimits::try_new(
                NonZeroUsize::new(DEFAULT_BATCH_ENTITIES).expect("default entities are positive"),
                NonZeroU64::new(DEFAULT_INPUT_BYTES).expect("default input bytes are positive"),
                NonZeroU64::new(DEFAULT_OUTPUT_OPERATIONS)
                    .expect("default output operations are positive"),
                NonZeroU64::new(DEFAULT_OUTPUT_BYTES).expect("default output bytes are positive"),
                NonZeroU64::new(DEFAULT_SINGLE_VECTOR_OUTPUT_BYTES)
                    .expect("default vector output bytes are positive"),
            )
            .expect("default vector output fits its transaction"),
            NonZeroUsize::MIN,
            TextBuildArtifactLimits::new(
                NonZeroUsize::new(DEFAULT_TEXT_ARTIFACT_ENTRIES)
                    .expect("default artifact entries are positive"),
                NonZeroU64::new(DEFAULT_TEXT_ARTIFACT_BYTES)
                    .expect("default artifact bytes are positive"),
            ),
            TextBackfillCompactionLimits::new(
                NonZeroUsize::new(DEFAULT_TEXT_COMPACTION_FAN_IN)
                    .expect("default compaction fan-in is positive"),
                NonZeroU64::new(DEFAULT_TEXT_COMPACTION_INPUT_BYTES)
                    .expect("default compaction input is positive"),
                NonZeroU64::new(DEFAULT_TEXT_COMPACTION_TEMP_BYTES)
                    .expect("default compaction temporary bytes are positive"),
                NonZeroU64::new(DEFAULT_TEXT_COMPACTION_OUTPUT_BLOB_BYTES)
                    .expect("default compaction output is positive"),
                NonZeroU64::new(DEFAULT_TEXT_MANIFEST_BYTES)
                    .expect("default manifest bytes are positive"),
            ),
        )
        .expect("default search-index backfill limits are consistent")
    }
}

/// Contradictory search-index backfill policy rejected before source reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SearchIndexBackfillLimitError {
    /// Current edge batching cannot stop after a returned-byte ceiling.
    #[error(
        "search-index edge property batch {requested_rows} exceeds the byte-bounded current mode of one row"
    )]
    EdgeReadIsNotByteBounded {
        /// Requested property rows materialized by one read.
        requested_rows: NonZeroUsize,
    },
    /// One vector invariant cannot have a larger cap than its transaction.
    #[error(
        "single-vector output cap {single_vector} exceeds transaction output cap {transaction}"
    )]
    SingleVectorExceedsTransactionBytes {
        /// Configured one-vector encoded output cap.
        single_vector: NonZeroU64,
        /// Configured whole-transaction encoded output cap.
        transaction: NonZeroU64,
    },
    /// An artifact page cannot fit the transaction intended to publish it.
    #[error("text artifact-page cap {artifact_page} exceeds transaction output cap {transaction}")]
    ArtifactPageExceedsTransactionBytes {
        /// Configured artifact-page encoded byte cap.
        artifact_page: NonZeroU64,
        /// Configured whole-transaction encoded output cap.
        transaction: NonZeroU64,
    },
    /// A current manifest cannot fit the transaction intended to publish it.
    #[error("text manifest cap {manifest} exceeds transaction output cap {transaction}")]
    ManifestExceedsTransactionBytes {
        /// Configured current-manifest encoded byte cap.
        manifest: NonZeroU64,
        /// Configured whole-transaction encoded output cap.
        transaction: NonZeroU64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    fn nzu(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn batch() -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(nzu(8), nz(100), nz(20), nz(80), nz(60)).unwrap()
    }

    fn artifacts(bytes: u64) -> TextBuildArtifactLimits {
        TextBuildArtifactLimits::new(nzu(4), nz(bytes))
    }

    fn compaction(manifest: u64) -> TextBackfillCompactionLimits {
        TextBackfillCompactionLimits::new(nzu(2), nz(100), nz(100), nz(100), nz(manifest))
    }

    #[test]
    fn defaults_are_positive_consistent_and_evidence_starting_points() {
        let limits = SearchIndexBackfillLimits::default();
        assert_eq!(limits.batch().max_entities().get(), 512);
        assert_eq!(limits.batch().max_input_bytes().get(), 8 * 1024 * 1024);
        assert_eq!(limits.edge_property_read_batch(), NonZeroUsize::MIN);
        assert!(limits.text_artifacts().max_bytes() <= limits.batch().max_output_bytes());
        assert!(limits.text_compaction().max_manifest_bytes() <= limits.batch().max_output_bytes());
        assert_eq!(
            limits.active_text_mutation().max_input_bytes(),
            limits.text_compaction().max_input_bytes()
        );
        assert_eq!(
            limits.active_text_mutation().max_output_bytes(),
            limits.batch().max_output_bytes()
        );
    }

    #[test]
    fn contradictory_limits_fail_before_backfill_work() {
        assert!(matches!(
            SearchIndexBatchLimits::try_new(nzu(1), nz(1), nz(1), nz(10), nz(11)),
            Err(SearchIndexBackfillLimitError::SingleVectorExceedsTransactionBytes { .. })
        ));
        assert!(matches!(
            SearchIndexBackfillLimits::try_new(batch(), nzu(2), artifacts(20), compaction(20)),
            Err(SearchIndexBackfillLimitError::EdgeReadIsNotByteBounded { .. })
        ));
        assert!(matches!(
            SearchIndexBackfillLimits::try_new(batch(), nzu(1), artifacts(81), compaction(20)),
            Err(SearchIndexBackfillLimitError::ArtifactPageExceedsTransactionBytes { .. })
        ));
        assert!(matches!(
            SearchIndexBackfillLimits::try_new(batch(), nzu(1), artifacts(20), compaction(81)),
            Err(SearchIndexBackfillLimitError::ManifestExceedsTransactionBytes { .. })
        ));
    }
}
