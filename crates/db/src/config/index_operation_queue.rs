//! Runtime policy for queued asynchronous vector/text index operations.
//!
//! Admission limits apply per logical index, aggregated across its
//! generations. They are never serialized, so changing them does not alter
//! the persisted queue format.
//!
//! # Usage
//!
//! ```
//! use std::num::NonZeroU64;
//! use std::time::Duration;
//!
//! use db::config::{DbConfig, IndexOperationQueueTuning, IndexOperationQueueTuningError};
//!
//! let tuning = IndexOperationQueueTuning::default()
//!     .with_max_retained_bytes(NonZeroU64::new(64 * 1024 * 1024).unwrap())
//!     .unwrap()
//!     .with_max_members(NonZeroU64::new(10_000).unwrap())
//!     .with_recovery_sweep_interval(Duration::from_millis(250))
//!     .unwrap();
//! let config = DbConfig::new().with_index_operation_queue_tuning(tuning);
//! assert_eq!(config.index_operation_queue(), tuning);
//! assert_eq!(IndexOperationQueueTuning::default().max_members().get(), 250_000);
//!
//! // Strong vector searches decode and score at most this much unpublished
//! // work before failing with retryable backpressure.
//! let strong = IndexOperationQueueTuning::default()
//!     .with_strong_vector_search_max_pending_bytes(NonZeroU64::new(64 << 20).unwrap());
//! assert_eq!(strong.strong_vector_search_max_pending_bytes().get(), 64 << 20);
//! assert_eq!(
//!     IndexOperationQueueTuning::default()
//!         .strong_vector_search_max_pending_bytes()
//!         .get(),
//!     IndexOperationQueueTuning::DEFAULT_STRONG_VECTOR_SEARCH_MAX_PENDING_BYTES
//! );
//!
//! // A larger retained-byte ceiling could let one queue value outgrow the
//! // longest value storage can encode.
//! let largest = IndexOperationQueueTuning::MAX_RETAINED_BYTES;
//! assert_eq!(largest, 2_437_684_127);
//! assert!(IndexOperationQueueTuning::default()
//!     .with_max_retained_bytes(NonZeroU64::new(largest).unwrap())
//!     .is_ok());
//! assert_eq!(
//!     IndexOperationQueueTuning::default()
//!         .with_max_retained_bytes(NonZeroU64::new(largest + 1).unwrap()),
//!     Err(IndexOperationQueueTuningError::RetainedBytesAboveQueueValueLimit {
//!         requested: largest + 1,
//!     })
//! );
//! ```

use std::num::NonZeroU64;
use std::time::Duration;

const DEFAULT_MAX_RETAINED_BYTES: u64 = 1_000_000_000;
const DEFAULT_MAX_MEMBERS: u64 = 250_000;
const DEFAULT_MAX_OPERAND_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_RECOVERY_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const EVENTUAL_SEARCH_SOURCE_INPUT_BYTES: u64 = 128 * 1024 * 1024;
const _: () = assert!(DEFAULT_MAX_RETAINED_BYTES <= IndexOperationQueueTuning::MAX_RETAINED_BYTES);

/// Invalid queue policy rejected before a database opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOperationQueueTuningError {
    /// The periodic recovery sweep must run.
    ZeroRecoverySweepInterval,
    /// The retained-byte ceiling exceeds
    /// [`IndexOperationQueueTuning::MAX_RETAINED_BYTES`].
    RetainedBytesAboveQueueValueLimit {
        /// The rejected ceiling.
        requested: u64,
    },
}

impl core::fmt::Display for IndexOperationQueueTuningError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ZeroRecoverySweepInterval => {
                formatter.write_str("index operation queue recovery sweep interval must be nonzero")
            }
            Self::RetainedBytesAboveQueueValueLimit { requested } => write!(
                formatter,
                "index operation queue max retained bytes {requested} exceed the queue value \
                 limit {}",
                IndexOperationQueueTuning::MAX_RETAINED_BYTES
            ),
        }
    }
}

impl std::error::Error for IndexOperationQueueTuningError {}

/// Storage layout of index-operation queues.
///
/// `Map` is the product layout: one merge-backed value per generation. `Rows`
/// stores one row per operation and acknowledges by deletion; it exists as the
/// baseline for queue-layout benchmarks and runs the same producer, publisher,
/// and search overlays. Only test and `async-index-benchmark` builds can
/// select it (`IndexOperationQueueTuning::with_layout`), so every product
/// handle runs `Map`.
///
/// A database must reopen with the layout that wrote its queues. Writer opens
/// fail closed on the other layout's queues. In builds that can select `Rows`,
/// reader opens also scan every tenant scope for them; that check sees only
/// the queues present at open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum QueueLayout {
    /// One merge-backed operation map per scope, index, and generation.
    #[default]
    Map,
    /// One row per operation, acknowledged by deletion.
    Rows,
}

/// Complete runtime policy for the immutable index-operation queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexOperationQueueTuning {
    max_retained_bytes: NonZeroU64,
    max_members: NonZeroU64,
    max_operand_bytes: NonZeroU64,
    recovery_sweep_interval: Duration,
    layout: QueueLayout,
    /// Open with automatic publication paused so tests drive it explicitly.
    #[cfg(test)]
    start_paused: bool,
    /// Per-search source-input budget for eventual pending overlays.
    eventual_search_budget: u64,
    /// Most committed pending vector work one strong search decodes and
    /// scores.
    strong_vector_search_max_pending_bytes: NonZeroU64,
}

impl Default for IndexOperationQueueTuning {
    fn default() -> Self {
        Self {
            max_retained_bytes: NonZeroU64::new(DEFAULT_MAX_RETAINED_BYTES)
                .expect("default retained-byte limit is nonzero"),
            max_members: NonZeroU64::new(DEFAULT_MAX_MEMBERS)
                .expect("default member limit is nonzero"),
            max_operand_bytes: NonZeroU64::new(DEFAULT_MAX_OPERAND_BYTES)
                .expect("default operand limit is nonzero"),
            recovery_sweep_interval: DEFAULT_RECOVERY_SWEEP_INTERVAL,
            layout: QueueLayout::Map,
            #[cfg(test)]
            start_paused: false,
            eventual_search_budget: EVENTUAL_SEARCH_SOURCE_INPUT_BYTES,
            strong_vector_search_max_pending_bytes: NonZeroU64::new(
                Self::DEFAULT_STRONG_VECTOR_SEARCH_MAX_PENDING_BYTES,
            )
            .expect("default strong vector search bound is nonzero"),
        }
    }
}

impl IndexOperationQueueTuning {
    /// Largest accepted [retained-byte ceiling](Self::max_retained_bytes),
    /// about 2.27 GiB.
    ///
    /// Storage encodes a value's length in 32 bits, and one generation's
    /// queue value can hold, besides its outstanding operations, a 16-byte
    /// acknowledgement for each operation that was outstanding before it.
    /// A larger ceiling could let such a value outgrow that length.
    pub const MAX_RETAINED_BYTES: u64 =
        crate::encoding::v2::values::indexes::operation_queue::MAX_RETAINED_BYTES;

    /// Default [strong vector search bound](Self::strong_vector_search_max_pending_bytes):
    /// 512 MiB.
    pub const DEFAULT_STRONG_VECTOR_SEARCH_MAX_PENDING_BYTES: u64 = 512 * 1024 * 1024;

    /// Returns the retained-operation byte ceiling per logical index.
    pub const fn max_retained_bytes(self) -> NonZeroU64 {
        self.max_retained_bytes
    }

    /// Returns the distinct pending entity/generation member ceiling per logical index.
    pub const fn max_members(self) -> NonZeroU64 {
        self.max_members
    }

    /// Returns the requested ceiling for one transaction's operand per queue key.
    ///
    /// Open further clamps it to the configured write-ahead-log entry bound.
    pub const fn max_operand_bytes(self) -> NonZeroU64 {
        self.max_operand_bytes
    }

    /// Returns the periodic recovery sweep interval.
    pub const fn recovery_sweep_interval(self) -> Duration {
        self.recovery_sweep_interval
    }

    /// Returns the queue storage layout.
    pub const fn layout(self) -> QueueLayout {
        self.layout
    }

    /// Selects the queue storage layout (benchmark baseline: `Rows`).
    ///
    /// Test and `async-index-benchmark` builds only: product builds always
    /// run [`QueueLayout::Map`], so their handles cannot disagree on layout.
    #[cfg(any(test, feature = "async-index-benchmark"))]
    pub const fn with_layout(mut self, layout: QueueLayout) -> Self {
        self.layout = layout;
        self
    }

    /// Replaces the retained-operation byte ceiling; a ceiling above
    /// [`Self::MAX_RETAINED_BYTES`] is rejected.
    pub const fn with_max_retained_bytes(
        mut self,
        bytes: NonZeroU64,
    ) -> Result<Self, IndexOperationQueueTuningError> {
        if bytes.get() > Self::MAX_RETAINED_BYTES {
            return Err(
                IndexOperationQueueTuningError::RetainedBytesAboveQueueValueLimit {
                    requested: bytes.get(),
                },
            );
        }
        self.max_retained_bytes = bytes;
        Ok(self)
    }

    /// Replaces the distinct pending member ceiling.
    pub const fn with_max_members(mut self, members: NonZeroU64) -> Self {
        self.max_members = members;
        self
    }

    /// Replaces the per-transaction operand ceiling.
    pub const fn with_max_operand_bytes(mut self, bytes: NonZeroU64) -> Self {
        self.max_operand_bytes = bytes;
        self
    }

    /// Replaces the periodic recovery sweep interval.
    pub fn with_recovery_sweep_interval(
        mut self,
        interval: Duration,
    ) -> Result<Self, IndexOperationQueueTuningError> {
        if interval.is_zero() {
            return Err(IndexOperationQueueTuningError::ZeroRecoverySweepInterval);
        }
        self.recovery_sweep_interval = interval;
        Ok(self)
    }

    /// Opens with automatic publication paused so tests drive it explicitly.
    #[cfg(test)]
    pub(crate) const fn with_publication_paused_for_tests(mut self) -> Self {
        self.start_paused = true;
        self
    }

    /// Shrinks the eventual overlay budget so tests reach its boundary cheaply.
    #[cfg(test)]
    pub(crate) const fn with_eventual_search_budget_for_tests(mut self, bytes: u64) -> Self {
        self.eventual_search_budget = bytes;
        self
    }

    /// Returns the per-search eventual overlay budget (128 MiB outside tests).
    pub(crate) const fn eventual_search_budget(self) -> u64 {
        self.eventual_search_budget
    }

    /// Returns the most committed but unpublished vector work, in retained
    /// bytes of each pending entity's latest operation, that one strong
    /// vector search decodes and scores exactly.
    ///
    /// A strong vector search, in a read or write request, whose index has
    /// more committed pending work fails with retryable `index_backpressure`
    /// (`pending_vector_bytes`) instead of decoding it, and succeeds once the
    /// index worker has published enough of it. A write request's own
    /// changes never count. Eventual searches are unaffected: they overlay
    /// the oldest work within their own budget. A strong search still reads
    /// the stored queue before it can tell, so the bound caps the decoding
    /// and exact scoring that follow, not that read.
    pub const fn strong_vector_search_max_pending_bytes(self) -> NonZeroU64 {
        self.strong_vector_search_max_pending_bytes
    }

    /// Replaces the [strong vector search bound](Self::strong_vector_search_max_pending_bytes).
    pub const fn with_strong_vector_search_max_pending_bytes(mut self, bytes: NonZeroU64) -> Self {
        self.strong_vector_search_max_pending_bytes = bytes;
        self
    }

    /// Returns whether automatic publication starts paused.
    #[cfg(test)]
    pub(crate) const fn starts_paused(self) -> bool {
        self.start_paused
    }

    /// Returns the operand ceiling after applying the WAL replay entry bound.
    ///
    /// SlateDB writes one merged entry per key per transaction and rejects a
    /// WAL whose decoded block cannot fit half of the replay working memory
    /// (`min(max_inflight_bytes / 4, 64 MiB) / 2`). A single-entry block needs
    /// its encoded and decoded copies, so an operand must stay below a quarter
    /// of that working memory, less a small key/framing allowance.
    pub(crate) fn effective_operand_bytes(self, wal_replay_max_inflight_bytes: usize) -> u64 {
        const MAX_DECODE_WORKING_BYTES: u64 = 64 * 1024 * 1024;
        const ENTRY_FRAMING_ALLOWANCE: u64 = 64 * 1024;
        let working = (u64::try_from(wal_replay_max_inflight_bytes).unwrap_or(u64::MAX) / 4)
            .min(MAX_DECODE_WORKING_BYTES);
        let block = working / 2;
        let entry = (block / 2).saturating_sub(ENTRY_FRAMING_ALLOWANCE);
        self.max_operand_bytes.get().min(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operand_ceiling_respects_the_wal_replay_entry_bound() {
        let tuning = IndexOperationQueueTuning::default();
        // Default SlateDB replay memory (256 MiB) permits the 8 MiB default.
        assert_eq!(
            tuning.effective_operand_bytes(256 * 1024 * 1024),
            8 * 1024 * 1024
        );
        // Small replay budgets clamp the operand below the configured ceiling.
        assert_eq!(
            tuning.effective_operand_bytes(23 * 1024 * 1024),
            (23 * 1024 * 1024 / 4 / 2 / 2) - 64 * 1024
        );
        assert_eq!(
            IndexOperationQueueTuning::default().with_recovery_sweep_interval(Duration::ZERO),
            Err(IndexOperationQueueTuningError::ZeroRecoverySweepInterval)
        );
    }
}
