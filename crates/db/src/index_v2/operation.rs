//! Valid-by-construction operation, progress, and execution-state contracts.

use std::num::{NonZeroU32, NonZeroU64};

use bytes::Bytes;

use super::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexIdentity,
    IndexIdentityFamily, IndexOperationId, IndexOperationRevision, IndexRevision,
    IndexV2ModelError, WriterEpoch,
};

/// Maximum encoded complete-key cursor length.
pub const INDEX_CURSOR_MAX_LEN: usize = 1024 * 1024;

/// Maximum exact artifact keys retained by one compaction upload checkpoint.
///
/// Runtime compaction may choose a smaller fan-in. This persistence ceiling
/// prevents a configured fan-in from creating an unbounded operation value.
pub(crate) const TEXT_COMPACTION_INPUT_KEY_MAX: usize = 1024;

/// Failure to construct an operation whose closed fields disagree.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IndexOperationModelError {
    /// A cursor exceeds the frozen bound.
    #[error("operation cursor is {actual} bytes; maximum is {maximum}")]
    OversizedCursor {
        /// Actual byte length.
        actual: usize,
        /// Frozen maximum.
        maximum: usize,
    },
    /// A claim sequence is zero.
    #[error("operation claim sequence must be non-zero")]
    ZeroClaimSequence,
    /// Progress family disagrees with the operation family.
    #[error("operation progress family does not match operation family")]
    ProgressFamilyMismatch,
    /// Build/drop kind disagrees with progress.
    #[error("operation progress does not match operation kind")]
    ProgressKindMismatch,
    /// Completion outcome disagrees with build/drop kind.
    #[error("operation completion outcome does not match operation kind")]
    CompletionKindMismatch,
    /// Terminal build outcome disagrees with construction/abort progress.
    #[error("operation completion outcome does not match build progress mode")]
    CompletionProgressMismatch,
    /// Logical identity family disagrees with the physical operation family.
    #[error("operation identity does not match operation family")]
    IdentityFamilyMismatch,
    /// A blocker payload violates its specific invariant.
    #[error("invalid blocker payload: {0}")]
    InvalidBlocker(&'static str),
    /// A text upload checkpoint cannot name one exact bounded source batch.
    #[error("invalid text build upload progress: {0}")]
    InvalidTextBuildUploadProgress(&'static str),
    /// A text manifest validation checkpoint cannot represent a bounded scan state.
    #[error("invalid text manifest validation progress: {0}")]
    InvalidTextManifestValidationProgress(&'static str),
    /// A durable operation transition was requested from the wrong state.
    #[error("illegal operation transition from {from} using {transition}")]
    IllegalExecutionTransition {
        /// Current execution-state name.
        from: &'static str,
        /// Requested transition name.
        transition: &'static str,
    },
}

/// Complete typed key used by a bounded resume-after scan.
///
/// The owning scoped repository additionally validates these bytes through the
/// exact `encoding/v1` key parser for its known scope. Keeping scope outside the
/// value prevents a cursor from carrying an independently variable tenant.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexCursor(Bytes);

impl IndexCursor {
    /// Bounds cursor bytes before any allocation or persistence.
    pub fn try_new(bytes: Bytes) -> Result<Self, IndexOperationModelError> {
        if bytes.len() > INDEX_CURSOR_MAX_LEN {
            return Err(IndexOperationModelError::OversizedCursor {
                actual: bytes.len(),
                maximum: INDEX_CURSOR_MAX_LEN,
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the complete encoded key.
    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

/// Monotonic counters retained across bounded operation steps.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct OperationCounters {
    /// Authoritative source entities visited.
    pub entities: u64,
    /// Source bytes consumed.
    pub input_bytes: u64,
    /// Physical write/delete operations staged.
    pub output_operations: u64,
    /// Physical output bytes staged.
    pub output_bytes: u64,
}

/// Inclusive source bound plus strict resume-after cursor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceScanProgress {
    /// Inclusive typed source upper bound captured at operation creation.
    pub inclusive_upper_bound: IndexCursor,
    /// Last completed key; the next step resumes strictly after it.
    pub cursor: Option<IndexCursor>,
    /// Cumulative bounded-work counters.
    pub counters: OperationCounters,
}

/// Exact text source batch waiting for one durable child upload to reconcile.
///
/// The private fields make the operation-to-intent relationship closed: a
/// checkpoint always retains its pre-upload source position, the last source
/// key admitted by the batch, the counters to publish after attachment, the
/// exact typed artifact key, and the child intent ID. The upload and operation
/// rows are committed together; recovery therefore point-reads this identity
/// and never scans a generation for an unknown child.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TextBuildUploadProgress {
    source: SourceScanProgress,
    completed_cursor: IndexCursor,
    completed_counters: OperationCounters,
    artifact_key: IndexCursor,
    intent_id: super::TextUploadIntentId,
}

impl TextBuildUploadProgress {
    /// Validates one non-empty source advance and its single artifact write.
    pub fn try_new(
        source: SourceScanProgress,
        completed_cursor: IndexCursor,
        completed_counters: OperationCounters,
        artifact_key: IndexCursor,
        intent_id: super::TextUploadIntentId,
    ) -> Result<Self, IndexOperationModelError> {
        if source
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.as_bytes() >= completed_cursor.as_bytes())
        {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "completed cursor must advance strictly beyond the source cursor",
            ));
        }
        if completed_cursor.as_bytes() > source.inclusive_upper_bound.as_bytes() {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "completed cursor exceeds the inclusive source upper bound",
            ));
        }
        if completed_counters.entities <= source.counters.entities {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "completed entity counter must advance for a non-empty source batch",
            ));
        }
        if completed_counters.input_bytes <= source.counters.input_bytes {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "completed input-byte counter must advance for a non-empty source batch",
            ));
        }
        let expected_output_operations = source.counters.output_operations.checked_add(1).ok_or(
            IndexOperationModelError::InvalidTextBuildUploadProgress(
                "output operation counter is exhausted",
            ),
        )?;
        if completed_counters.output_operations != expected_output_operations {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "completed output-operation counter must advance by exactly one artifact",
            ));
        }
        if completed_counters.output_bytes <= source.counters.output_bytes {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "completed output-byte counter must advance for a non-empty artifact",
            ));
        }
        Ok(Self {
            source,
            completed_cursor,
            completed_counters,
            artifact_key,
            intent_id,
        })
    }

    /// Borrows the source checkpoint retained for retry after non-publication.
    pub const fn source(&self) -> &SourceScanProgress {
        &self.source
    }

    /// Borrows the last authoritative source key admitted by this batch.
    pub const fn completed_cursor(&self) -> &IndexCursor {
        &self.completed_cursor
    }

    /// Returns the counters that become visible after artifact attachment.
    pub const fn completed_counters(&self) -> OperationCounters {
        self.completed_counters
    }

    /// Borrows the exact scoped V2 artifact key expected from the child upload.
    pub const fn artifact_key(&self) -> &IndexCursor {
        &self.artifact_key
    }

    /// Returns the exact child upload intent; recovery never scans by owner.
    pub const fn intent_id(&self) -> super::TextUploadIntentId {
        self.intent_id
    }

    /// Converts a reconciled child into the next ordinary source checkpoint.
    pub fn completed_source(&self) -> SourceScanProgress {
        SourceScanProgress {
            inclusive_upper_bound: self.source.inclusive_upper_bound.clone(),
            cursor: Some(self.completed_cursor.clone()),
            counters: self.completed_counters,
        }
    }
}

/// Exact text catch-up entity waiting for one durable child upload to reconcile.
///
/// Catch-up consumes a coalesced delta before its child publishes. The private
/// fields retain the exact delta key so terminal non-publication can restore the
/// work item without scanning or guessing, while successful publication resumes
/// the prefix from its canonical beginning to observe concurrently coalesced
/// mutations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TextCatchUpUploadProgress {
    catch_up: PrefixScanProgress,
    delta_key: IndexCursor,
    completed_counters: OperationCounters,
    artifact_key: IndexCursor,
    intent_id: super::TextUploadIntentId,
}

impl TextCatchUpUploadProgress {
    /// Validates one catch-up entity and its exact artifact upload checkpoint.
    pub fn try_new(
        catch_up: PrefixScanProgress,
        delta_key: IndexCursor,
        completed_counters: OperationCounters,
        artifact_key: IndexCursor,
        intent_id: super::TextUploadIntentId,
    ) -> Result<Self, IndexOperationModelError> {
        if catch_up.cursor.is_some() {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "text catch-up must restart from the coalesced delta prefix",
            ));
        }
        let expected_entities = catch_up.counters.entities.checked_add(1).ok_or(
            IndexOperationModelError::InvalidTextBuildUploadProgress(
                "text catch-up entity counter is exhausted",
            ),
        )?;
        if completed_counters.entities != expected_entities {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "completed catch-up entity counter must advance by exactly one",
            ));
        }
        if completed_counters.input_bytes <= catch_up.counters.input_bytes {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "completed catch-up input-byte counter must advance",
            ));
        }
        if completed_counters.output_operations <= catch_up.counters.output_operations {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "completed catch-up output-operation counter must advance",
            ));
        }
        if completed_counters.output_bytes <= catch_up.counters.output_bytes {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "completed catch-up output-byte counter must advance",
            ));
        }
        Ok(Self {
            catch_up,
            delta_key,
            completed_counters,
            artifact_key,
            intent_id,
        })
    }

    /// Borrows the prefix checkpoint retained before consuming the delta.
    pub const fn catch_up(&self) -> &PrefixScanProgress {
        &self.catch_up
    }

    /// Borrows the exact coalesced delta restored after non-publication.
    pub const fn delta_key(&self) -> &IndexCursor {
        &self.delta_key
    }

    /// Returns the counters retained after the catch-up entity is applied.
    pub const fn completed_counters(&self) -> OperationCounters {
        self.completed_counters
    }

    /// Borrows the exact scoped V2 artifact expected from the child upload.
    pub const fn artifact_key(&self) -> &IndexCursor {
        &self.artifact_key
    }

    /// Returns the exact child upload intent; recovery never scans by owner.
    pub const fn intent_id(&self) -> super::TextUploadIntentId {
        self.intent_id
    }

    /// Converts a reconciled child into the next coalesced-prefix checkpoint.
    pub fn completed_catch_up(&self) -> PrefixScanProgress {
        PrefixScanProgress {
            cursor: None,
            counters: self.completed_counters,
        }
    }
}

/// Exact text compaction batch waiting for one durable child upload.
///
/// Compaction cannot reuse [`TextBuildUploadProgress`] because its source
/// cursor is exclusively a kind-`0x0C` partition scan, and it cannot reuse
/// [`TextCatchUpUploadProgress`] because that checkpoint owns one exact
/// kind-`0x03` delta. This variant retains the bounded, sorted source artifact
/// set and child identity required to retire inputs only after the replacement
/// artifact is durably attached.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TextCompactionUploadProgress {
    compact: PrefixScanProgress,
    input_artifact_keys: Vec<IndexCursor>,
    completed_counters: OperationCounters,
    artifact_key: IndexCursor,
    intent_id: super::TextUploadIntentId,
}

impl TextCompactionUploadProgress {
    /// Maximum exact input artifacts retained by one durable checkpoint.
    pub const MAX_INPUT_ARTIFACTS: usize = TEXT_COMPACTION_INPUT_KEY_MAX;

    /// Validates one useful bounded compaction and its exact replacement child.
    pub fn try_new(
        compact: PrefixScanProgress,
        input_artifact_keys: Vec<IndexCursor>,
        completed_counters: OperationCounters,
        artifact_key: IndexCursor,
        intent_id: super::TextUploadIntentId,
    ) -> Result<Self, IndexOperationModelError> {
        if !(2..=TEXT_COMPACTION_INPUT_KEY_MAX).contains(&input_artifact_keys.len()) {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "text compaction must retain a bounded useful input set",
            ));
        }
        if input_artifact_keys
            .windows(2)
            .any(|keys| keys[0].as_bytes() >= keys[1].as_bytes())
        {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "text compaction input artifact keys must be strictly sorted",
            ));
        }
        if input_artifact_keys.iter().any(|key| key == &artifact_key) {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "text compaction replacement artifact must not overwrite an input",
            ));
        }
        let retained_key_bytes = input_artifact_keys
            .iter()
            .try_fold(artifact_key.as_bytes().len(), |total, key| {
                total.checked_add(key.as_bytes().len())
            });
        if retained_key_bytes.is_none_or(|bytes| bytes > INDEX_CURSOR_MAX_LEN) {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "text compaction retained artifact keys exceed the operation bound",
            ));
        }
        if completed_counters.entities < compact.counters.entities {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "text compaction entity counter cannot move backwards",
            ));
        }
        if completed_counters.input_bytes <= compact.counters.input_bytes {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "text compaction input-byte counter must advance",
            ));
        }
        if completed_counters.output_operations <= compact.counters.output_operations {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "text compaction output-operation counter must advance",
            ));
        }
        if completed_counters.output_bytes <= compact.counters.output_bytes {
            return Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                "text compaction output-byte counter must advance",
            ));
        }
        Ok(Self {
            compact,
            input_artifact_keys,
            completed_counters,
            artifact_key,
            intent_id,
        })
    }

    /// Borrows the compaction prefix checkpoint retained for retry.
    pub const fn compact(&self) -> &PrefixScanProgress {
        &self.compact
    }

    /// Borrows the exact sorted input artifacts retired after attachment.
    pub fn input_artifact_keys(&self) -> &[IndexCursor] {
        &self.input_artifact_keys
    }

    /// Returns counters published after input retirement succeeds.
    pub const fn completed_counters(&self) -> OperationCounters {
        self.completed_counters
    }

    /// Borrows the exact replacement artifact produced by the child upload.
    pub const fn artifact_key(&self) -> &IndexCursor {
        &self.artifact_key
    }

    /// Returns the exact child intent; recovery never scans by owner.
    pub const fn intent_id(&self) -> super::TextUploadIntentId {
        self.intent_id
    }

    /// Resumes the same partition after replacement and input retirement.
    pub fn completed_compaction(&self) -> PrefixScanProgress {
        PrefixScanProgress {
            cursor: self.compact.cursor.clone(),
            counters: self.completed_counters,
        }
    }
}

/// Prefix scan with strict resume-after cursor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrefixScanProgress {
    /// Last completed key.
    pub cursor: Option<IndexCursor>,
    /// Cumulative counters.
    pub counters: OperationCounters,
}

/// Incomplete proof for one non-empty text manifest partition.
///
/// A completed partition is never persisted: once its declared page and split
/// counts match, validation drops this accumulator and retains only the last
/// complete page key as the resume cursor. This makes a persisted accumulator
/// mean exactly one thing—the next page of the same root is still required.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TextManifestPartitionValidation {
    partition_fingerprint: [u8; 32],
    root_revision: super::TextManifestRevision,
    page_count: NonZeroU32,
    split_count: NonZeroU64,
    next_page: NonZeroU32,
    observed_split_count: NonZeroU64,
}

impl TextManifestPartitionValidation {
    /// Constructs one incomplete, internally consistent partition proof.
    pub fn try_new(
        partition_fingerprint: [u8; 32],
        root_revision: super::TextManifestRevision,
        page_count: u32,
        split_count: u64,
        next_page: u32,
        observed_split_count: u64,
    ) -> Result<Self, IndexOperationModelError> {
        let Some(page_count) = NonZeroU32::new(page_count) else {
            return Err(
                IndexOperationModelError::InvalidTextManifestValidationProgress(
                    "partition page count must be non-zero",
                ),
            );
        };
        let Some(split_count) = NonZeroU64::new(split_count) else {
            return Err(
                IndexOperationModelError::InvalidTextManifestValidationProgress(
                    "partition split count must be non-zero",
                ),
            );
        };
        let Some(next_page) = NonZeroU32::new(next_page) else {
            return Err(
                IndexOperationModelError::InvalidTextManifestValidationProgress(
                    "incomplete partition must have consumed at least page zero",
                ),
            );
        };
        let Some(observed_split_count) = NonZeroU64::new(observed_split_count) else {
            return Err(
                IndexOperationModelError::InvalidTextManifestValidationProgress(
                    "an observed non-empty page must contribute a split",
                ),
            );
        };
        let minimum_root_revision = u64::from(page_count.get()) + 1;
        let Some(remaining_pages) = page_count.get().checked_sub(next_page.get()) else {
            return Err(
                IndexOperationModelError::InvalidTextManifestValidationProgress(
                    "next page exceeds the root page count",
                ),
            );
        };
        if remaining_pages == 0
            || observed_split_count.get() < u64::from(next_page.get())
            || observed_split_count.get() > split_count.get()
            || observed_split_count
                .get()
                .saturating_add(u64::from(remaining_pages))
                > split_count.get()
            || root_revision.get() != minimum_root_revision
        {
            return Err(
                IndexOperationModelError::InvalidTextManifestValidationProgress(
                    "partition counts, next page, split total, and root revision disagree",
                ),
            );
        }
        Ok(Self {
            partition_fingerprint,
            root_revision,
            page_count,
            split_count,
            next_page,
            observed_split_count,
        })
    }

    /// Returns the exact partition fingerprint being validated.
    pub const fn partition_fingerprint(&self) -> &[u8; 32] {
        &self.partition_fingerprint
    }

    /// Returns the immutable root revision observed before page validation.
    pub const fn root_revision(&self) -> super::TextManifestRevision {
        self.root_revision
    }

    /// Returns the root's declared non-zero page count.
    pub const fn page_count(&self) -> u32 {
        self.page_count.get()
    }

    /// Returns the root's declared non-zero split count.
    pub const fn split_count(&self) -> u64 {
        self.split_count.get()
    }

    /// Returns the next contiguous page number required from this partition.
    pub const fn next_page(&self) -> u32 {
        self.next_page.get()
    }

    /// Returns the exact number of split entries observed so far.
    pub const fn observed_split_count(&self) -> u64 {
        self.observed_split_count.get()
    }
}

/// Bounded page-lane checkpoint for pre-activation manifest validation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TextManifestPageValidationProgress {
    cursor: Option<IndexCursor>,
    partition: Option<TextManifestPartitionValidation>,
    counters: OperationCounters,
}

impl TextManifestPageValidationProgress {
    /// Starts page validation before the first generation-qualified page key.
    pub const fn initial(counters: OperationCounters) -> Self {
        Self {
            cursor: None,
            partition: None,
            counters,
        }
    }

    /// Constructs a resumable page checkpoint with at most one incomplete root.
    pub fn try_new(
        cursor: Option<IndexCursor>,
        partition: Option<TextManifestPartitionValidation>,
        counters: OperationCounters,
    ) -> Result<Self, IndexOperationModelError> {
        if partition.is_some() && cursor.is_none() {
            return Err(
                IndexOperationModelError::InvalidTextManifestValidationProgress(
                    "an incomplete partition requires its last complete page cursor",
                ),
            );
        }
        Ok(Self {
            cursor,
            partition,
            counters,
        })
    }

    /// Borrows the last completely validated page key.
    pub const fn cursor(&self) -> Option<&IndexCursor> {
        self.cursor.as_ref()
    }

    /// Returns the incomplete partition proof, when the next page is required.
    pub const fn partition(&self) -> Option<&TextManifestPartitionValidation> {
        self.partition.as_ref()
    }

    /// Returns cumulative operation counters.
    pub const fn counters(&self) -> OperationCounters {
        self.counters
    }
}

/// Closed validation lane between manifest construction and activation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TextManifestValidationProgress {
    /// Validate every page, root relationship, split count, reachability row, and blob.
    Pages(TextManifestPageValidationProgress),
    /// Validate every root, including valid empty partitions and page-less corruption.
    Roots(PrefixScanProgress),
    /// Reject unfinished build uploads and validate every reclaimable intent candidate.
    UploadIntents(PrefixScanProgress),
}

impl TextManifestValidationProgress {
    /// Starts the bounded proof at the manifest-page lane.
    pub const fn initial(counters: OperationCounters) -> Self {
        Self::Pages(TextManifestPageValidationProgress::initial(counters))
    }

    /// Returns cumulative counters independent of the current validation lane.
    pub const fn counters(&self) -> OperationCounters {
        match self {
            Self::Pages(progress) => progress.counters(),
            Self::Roots(progress) | Self::UploadIntents(progress) => progress.counters,
        }
    }
}

/// Step whose state is fully represented by counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct NoCursorProgress {
    /// Cumulative counters.
    pub counters: OperationCounters,
}

/// Cross-process drain step.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct DrainProgress {
    /// Persisted drain epoch once acquired.
    pub drain_epoch: Option<u64>,
    /// Cumulative counters.
    pub counters: OperationCounters,
}

/// Reachability/GC step linked to an optional durable run.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GcProgress {
    /// Assigned GC run when materialized.
    pub gc_run_id: Option<super::BlobGcRunId>,
    /// Candidate enumeration cursor.
    pub candidate_cursor: Option<IndexCursor>,
    /// Stage-specific GC cursor.
    pub stage_cursor: Option<IndexCursor>,
    /// Cumulative counters.
    pub counters: OperationCounters,
}

/// Secondary build stage with its only legal payload shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SecondaryBuildStage {
    /// Scan authoritative graph rows into hidden secondary entries.
    Scan(SourceScanProgress),
    /// Apply coalesced mutations that raced the source scan.
    CatchUp(PrefixScanProgress),
    /// Validate hidden entries before activation.
    Validate(PrefixScanProgress),
    /// Publish the validated hidden generation.
    Activate(NoCursorProgress),
}

/// Vector build stage with its only legal payload shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VectorBuildStage {
    /// Scan authoritative graph rows into a hidden HNSW generation.
    Scan(SourceScanProgress),
    /// Apply coalesced mutations that raced the source scan.
    CatchUp(PrefixScanProgress),
    /// Validate the complete physical descriptor and graph rows.
    ValidateDescriptor(PrefixScanProgress),
    /// Publish the validated hidden generation.
    Activate(NoCursorProgress),
}

/// Text build stage with its only legal payload shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TextBuildStage {
    /// Scan authoritative graph rows and stage partition-qualified entity state.
    ScanSource(SourceScanProgress),
    /// Scan staged entity state in partition order and construct bounded splits.
    ScanPartitions(SourceScanProgress),
    /// One exact source batch whose upload intent is still the retry anchor.
    AwaitUpload(TextBuildUploadProgress),
    /// Apply coalesced mutations that raced the source scan.
    CatchUp(PrefixScanProgress),
    /// One exact catch-up delta whose upload intent is still the retry anchor.
    AwaitCatchUpUpload(TextCatchUpUploadProgress),
    /// Compact bounded staged split sets.
    Compact(PrefixScanProgress),
    /// One bounded artifact merge whose replacement upload is still pending.
    AwaitCompactionUpload(TextCompactionUploadProgress),
    /// Construct canonical manifest pages and roots for every partition.
    PrepareManifests(PrefixScanProgress),
    /// Bounded physical and publication proof before canonical activation.
    ValidateManifests(TextManifestValidationProgress),
    /// Publish the validated hidden generation.
    Activate(NoCursorProgress),
}

/// Secondary cleanup stages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SecondaryCleanupProgress {
    /// Start the exact generation reader drain.
    BeginDrain(DrainProgress),
    /// Delete all owned secondary entry rows.
    DeleteEntries(PrefixScanProgress),
    /// Delete coalesced mutation rows.
    DeleteDeltas(PrefixScanProgress),
    /// Close the proven drained generation.
    FinishDrain(DrainProgress),
    /// Commit terminal catalog and operation state.
    Finalize(NoCursorProgress),
}

/// Vector cleanup stages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VectorCleanupProgress {
    /// Start the exact generation reader drain.
    BeginDrain(DrainProgress),
    /// Retire and clear the exact resident vector snapshot.
    RetireCache(NoCursorProgress),
    /// Delete all owned physical vector row families.
    DeletePhysical(PrefixScanProgress),
    /// Delete coalesced mutation rows.
    DeleteDeltas(PrefixScanProgress),
    /// Close the proven drained generation.
    FinishDrain(DrainProgress),
    /// Commit terminal catalog and operation state.
    Finalize(NoCursorProgress),
}

/// Text cleanup stages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TextCleanupProgress {
    /// Start the exact generation reader drain.
    BeginDrain(DrainProgress),
    /// Materialize the immutable candidate set for owned blobs.
    PrepareCandidates(PrefixScanProgress),
    /// Acquire delete fences for the exact candidate set.
    AcquireDeleteFences(GcProgress),
    /// Retire canonical manifest owners and references.
    RetireManifest(GcProgress),
    /// Retire build-artifact owners and references.
    RetireArtifacts(GcProgress),
    /// Retire upload-intent owners and references.
    RetireUploadIntents(GcProgress),
    /// Prove global unreachability through the durable GC passes.
    MarkReachability(GcProgress),
    /// Delete fenced blobs proven globally unreachable.
    DeleteBlobs(GcProgress),
    /// Delete generation-qualified entity state.
    DeleteEntityState(PrefixScanProgress),
    /// Close the proven drained generation.
    FinishDrain(DrainProgress),
    /// Commit terminal catalog and operation state.
    Finalize(NoCursorProgress),
}

/// A secondary BUILD is either constructing or running the family's cleanup
/// state machine. The variant owns the only legal stage ADT for that mode.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SecondaryBuildProgress {
    /// Hidden secondary construction.
    Constructing(SecondaryBuildStage),
    /// Cleanup of an unactivated secondary generation.
    Aborting(SecondaryCleanupProgress),
}

/// A vector BUILD is either constructing or running the family's cleanup
/// state machine. The variant owns the only legal stage ADT for that mode.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VectorBuildProgress {
    /// Hidden vector construction.
    Constructing(VectorBuildStage),
    /// Cleanup of an unactivated vector generation.
    Aborting(VectorCleanupProgress),
}

/// A text BUILD is either constructing or running the family's cleanup state
/// machine. The variant owns the only legal stage ADT for that mode.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TextBuildProgress {
    /// Hidden text construction.
    Constructing(TextBuildStage),
    /// Cleanup of an unactivated text generation.
    Aborting(TextCleanupProgress),
}

/// Operation progress is family- and kind-typed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexOperationProgress {
    /// Secondary BUILD construction or abort cleanup.
    SecondaryBuild(SecondaryBuildProgress),
    /// Vector BUILD construction or abort cleanup.
    VectorBuild(VectorBuildProgress),
    /// Text BUILD construction or abort cleanup.
    TextBuild(TextBuildProgress),
    /// DROP cleanup for an activated secondary generation.
    SecondaryCleanup(SecondaryCleanupProgress),
    /// DROP cleanup for an activated vector generation.
    VectorCleanup(VectorCleanupProgress),
    /// DROP cleanup for an activated text generation.
    TextCleanup(TextCleanupProgress),
}

impl IndexOperationProgress {
    /// Returns the physical family lane.
    pub const fn family(&self) -> IndexOperationFamily {
        match self {
            Self::SecondaryBuild(_) | Self::SecondaryCleanup(_) => IndexOperationFamily::Secondary,
            Self::VectorBuild(_) | Self::VectorCleanup(_) => IndexOperationFamily::Vector,
            Self::TextBuild(_) | Self::TextCleanup(_) => IndexOperationFamily::Text,
        }
    }

    /// Returns whether progress belongs to BUILD or DROP.
    pub const fn kind(&self) -> IndexOperationKind {
        match self {
            Self::SecondaryBuild(_) | Self::VectorBuild(_) | Self::TextBuild(_) => {
                IndexOperationKind::Build
            }
            Self::SecondaryCleanup(_) | Self::VectorCleanup(_) | Self::TextCleanup(_) => {
                IndexOperationKind::Drop
            }
        }
    }

    /// Returns the construction/abort mode for a build operation.
    pub const fn is_constructing_build(&self) -> bool {
        match self {
            Self::SecondaryBuild(SecondaryBuildProgress::Constructing(_))
            | Self::VectorBuild(VectorBuildProgress::Constructing(_))
            | Self::TextBuild(TextBuildProgress::Constructing(_)) => true,
            Self::SecondaryBuild(SecondaryBuildProgress::Aborting(_))
            | Self::VectorBuild(VectorBuildProgress::Aborting(_))
            | Self::TextBuild(TextBuildProgress::Aborting(_))
            | Self::SecondaryCleanup(_)
            | Self::VectorCleanup(_)
            | Self::TextCleanup(_) => false,
        }
    }

    /// Returns true only for a BUILD already converted to cleanup.
    pub const fn is_aborting_build(&self) -> bool {
        match self {
            Self::SecondaryBuild(SecondaryBuildProgress::Aborting(_))
            | Self::VectorBuild(VectorBuildProgress::Aborting(_))
            | Self::TextBuild(TextBuildProgress::Aborting(_)) => true,
            Self::SecondaryBuild(SecondaryBuildProgress::Constructing(_))
            | Self::VectorBuild(VectorBuildProgress::Constructing(_))
            | Self::TextBuild(TextBuildProgress::Constructing(_))
            | Self::SecondaryCleanup(_)
            | Self::VectorCleanup(_)
            | Self::TextCleanup(_) => false,
        }
    }

    /// Validates every complete resume key owned by this progress variant.
    ///
    /// The caller supplies scope-aware `encoding/v1` parsing because scope is
    /// deliberately not duplicated inside persisted cursor bytes.
    pub(crate) fn cursors_are_valid(&self, mut validate: impl FnMut(&IndexCursor) -> bool) -> bool {
        let source_is_valid =
            |progress: &SourceScanProgress, validate: &mut dyn FnMut(&IndexCursor) -> bool| {
                validate(&progress.inclusive_upper_bound)
                    && progress.cursor.as_ref().is_none_or(validate)
            };
        let prefix_is_valid =
            |progress: &PrefixScanProgress, validate: &mut dyn FnMut(&IndexCursor) -> bool| {
                progress.cursor.as_ref().is_none_or(validate)
            };
        let gc_is_valid =
            |progress: &GcProgress, validate: &mut dyn FnMut(&IndexCursor) -> bool| {
                progress
                    .candidate_cursor
                    .as_ref()
                    .is_none_or(&mut *validate)
                    && progress.stage_cursor.as_ref().is_none_or(validate)
            };
        let acquire_gc_is_valid =
            |progress: &GcProgress, validate: &mut dyn FnMut(&IndexCursor) -> bool| {
                gc_is_valid(progress, validate)
                    && (progress.gc_run_id.is_none()
                        || (progress.candidate_cursor.is_some() && progress.stage_cursor.is_none()))
            };
        let assigned_gc_is_valid =
            |progress: &GcProgress, validate: &mut dyn FnMut(&IndexCursor) -> bool| {
                progress.gc_run_id.is_some()
                    && progress.candidate_cursor.is_some()
                    && gc_is_valid(progress, validate)
            };
        match self {
            Self::SecondaryBuild(SecondaryBuildProgress::Constructing(stage)) => match stage {
                SecondaryBuildStage::Scan(progress) => source_is_valid(progress, &mut validate),
                SecondaryBuildStage::CatchUp(progress)
                | SecondaryBuildStage::Validate(progress) => {
                    prefix_is_valid(progress, &mut validate)
                }
                SecondaryBuildStage::Activate(_) => true,
            },
            Self::SecondaryBuild(SecondaryBuildProgress::Aborting(progress))
            | Self::SecondaryCleanup(progress) => match progress {
                SecondaryCleanupProgress::DeleteEntries(progress)
                | SecondaryCleanupProgress::DeleteDeltas(progress) => {
                    prefix_is_valid(progress, &mut validate)
                }
                SecondaryCleanupProgress::BeginDrain(_)
                | SecondaryCleanupProgress::FinishDrain(_)
                | SecondaryCleanupProgress::Finalize(_) => true,
            },
            Self::VectorBuild(VectorBuildProgress::Constructing(stage)) => match stage {
                VectorBuildStage::Scan(progress) => source_is_valid(progress, &mut validate),
                VectorBuildStage::CatchUp(progress)
                | VectorBuildStage::ValidateDescriptor(progress) => {
                    prefix_is_valid(progress, &mut validate)
                }
                VectorBuildStage::Activate(_) => true,
            },
            Self::VectorBuild(VectorBuildProgress::Aborting(progress))
            | Self::VectorCleanup(progress) => match progress {
                VectorCleanupProgress::DeletePhysical(progress)
                | VectorCleanupProgress::DeleteDeltas(progress) => {
                    prefix_is_valid(progress, &mut validate)
                }
                VectorCleanupProgress::BeginDrain(_)
                | VectorCleanupProgress::RetireCache(_)
                | VectorCleanupProgress::FinishDrain(_)
                | VectorCleanupProgress::Finalize(_) => true,
            },
            Self::TextBuild(TextBuildProgress::Constructing(stage)) => match stage {
                TextBuildStage::ScanSource(progress) | TextBuildStage::ScanPartitions(progress) => {
                    source_is_valid(progress, &mut validate)
                }
                TextBuildStage::AwaitUpload(progress) => {
                    source_is_valid(progress.source(), &mut validate)
                        && validate(progress.completed_cursor())
                        && validate(progress.artifact_key())
                }
                TextBuildStage::AwaitCatchUpUpload(progress) => {
                    prefix_is_valid(progress.catch_up(), &mut validate)
                        && validate(progress.delta_key())
                        && validate(progress.artifact_key())
                }
                TextBuildStage::AwaitCompactionUpload(progress) => {
                    prefix_is_valid(progress.compact(), &mut validate)
                        && progress.input_artifact_keys().iter().all(&mut validate)
                        && validate(progress.artifact_key())
                }
                TextBuildStage::CatchUp(progress)
                | TextBuildStage::Compact(progress)
                | TextBuildStage::PrepareManifests(progress) => {
                    prefix_is_valid(progress, &mut validate)
                }
                TextBuildStage::ValidateManifests(progress) => match progress {
                    TextManifestValidationProgress::Pages(progress) => {
                        progress.cursor().is_none_or(&mut validate)
                    }
                    TextManifestValidationProgress::Roots(progress)
                    | TextManifestValidationProgress::UploadIntents(progress) => {
                        prefix_is_valid(progress, &mut validate)
                    }
                },
                TextBuildStage::Activate(_) => true,
            },
            Self::TextBuild(TextBuildProgress::Aborting(progress))
            | Self::TextCleanup(progress) => match progress {
                TextCleanupProgress::PrepareCandidates(progress)
                | TextCleanupProgress::DeleteEntityState(progress) => {
                    prefix_is_valid(progress, &mut validate)
                }
                TextCleanupProgress::AcquireDeleteFences(progress) => {
                    acquire_gc_is_valid(progress, &mut validate)
                }
                TextCleanupProgress::RetireManifest(progress)
                | TextCleanupProgress::RetireArtifacts(progress)
                | TextCleanupProgress::RetireUploadIntents(progress)
                | TextCleanupProgress::MarkReachability(progress)
                | TextCleanupProgress::DeleteBlobs(progress) => {
                    assigned_gc_is_valid(progress, &mut validate)
                }
                TextCleanupProgress::BeginDrain(_)
                | TextCleanupProgress::FinishDrain(_)
                | TextCleanupProgress::Finalize(_) => true,
            },
        }
    }
}

/// Public operation kind.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexOperationKind {
    /// Construct and activate a new generation.
    Build = 0x01,
    /// Retire and remove an activated generation.
    Drop = 0x02,
}

/// Physical family driver selected by an operation.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexOperationFamily {
    /// Secondary equality or range index driver.
    Secondary = 0x01,
    /// Vector HNSW index driver.
    Vector = 0x02,
    /// Text index driver.
    Text = 0x03,
}

impl IndexOperationFamily {
    const fn owns_identity(self, identity: &IndexIdentity) -> bool {
        matches!(
            (self, identity.family()),
            (
                Self::Secondary,
                IndexIdentityFamily::SecondaryEquality | IndexIdentityFamily::SecondaryRange
            ) | (Self::Vector, IndexIdentityFamily::Vector)
                | (Self::Text, IndexIdentityFamily::Text)
        )
    }
}

/// Non-zero claim sequence scoped by a writer epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClaimSequence(NonZeroU64);

impl ClaimSequence {
    /// Validates a claim sequence.
    pub fn new(value: u64) -> Result<Self, IndexOperationModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(IndexOperationModelError::ZeroClaimSequence)
    }

    /// Returns the raw sequence.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Durable worker claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperationClaim {
    /// Fenced writer epoch.
    pub writer_epoch: WriterEpoch,
    /// Monotonic sequence within that epoch.
    pub sequence: ClaimSequence,
}

/// Typed blocker whose variants own their exact payload.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexOperationBlocker {
    /// An authoritative entity cannot be decoded for the requested index.
    InvalidSourceData {
        /// Kind of malformed source entity.
        entity_kind: IndexElementKind,
        /// Identity of the malformed source entity.
        entity_id: IndexEntityId,
    },
    /// Two source entities violate a unique-secondary constraint.
    UniquenessViolation {
        /// First entity observed for the duplicated value.
        first_entity_id: IndexEntityId,
        /// Conflicting entity observed for the duplicated value.
        second_entity_id: IndexEntityId,
    },
    /// One entity cannot fit within an atomic build transaction.
    OversizedEntity {
        /// Kind of oversized source entity.
        entity_kind: IndexElementKind,
        /// Identity of the oversized source entity.
        entity_id: IndexEntityId,
        /// Measured encoded size or operation count.
        observed: u64,
        /// Configured maximum for the measured resource.
        limit: u64,
    },
    /// One text partition cannot fit the current manifest limits.
    ManifestLimit {
        /// Partition whose manifest exceeded its limit.
        partition: super::TextPartition,
        /// Measured encoded manifest resource.
        observed: u64,
        /// Configured maximum for that resource.
        limit: u64,
    },
    /// No valid reader-lease coordinator is available for this topology.
    ReaderCoordinationUnavailable,
    /// Text work requires object storage that is not configured.
    ObjectStoreConfigurationUnavailable,
    /// Persisted state violates a lifecycle invariant.
    InvariantViolation,
    /// No valid blob-publication coordinator is available for this topology.
    BlobPublicationCoordinationUnavailable,
    /// A content-addressed text object disagrees with its declared blob.
    ///
    /// The intent ID is the exact O(1) coupling used to requeue the blocked
    /// upload with this operation; retry never scans a generation for a child.
    BlobPublicationMismatch {
        /// Exact upload intent whose declared and observed blobs disagree.
        intent_id: super::TextUploadIntentId,
    },
}

impl IndexOperationBlocker {
    /// Validates size-limit payload ordering.
    pub fn validate(&self) -> Result<(), IndexOperationModelError> {
        match self {
            Self::OversizedEntity {
                observed, limit, ..
            }
            | Self::ManifestLimit {
                observed, limit, ..
            } if observed <= limit => Err(IndexOperationModelError::InvalidBlocker(
                "observed size must exceed limit",
            )),
            Self::InvalidSourceData { .. }
            | Self::UniquenessViolation { .. }
            | Self::OversizedEntity { .. }
            | Self::ManifestLimit { .. }
            | Self::ReaderCoordinationUnavailable
            | Self::ObjectStoreConfigurationUnavailable
            | Self::InvariantViolation
            | Self::BlobPublicationCoordinationUnavailable
            | Self::BlobPublicationMismatch { .. } => Ok(()),
        }
    }
}

/// Build completion outcome.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuildOperationOutcome {
    /// The generation reached canonical Active publication.
    Succeeded = 0x01,
    /// The hidden generation was fully aborted and cleaned.
    Aborted = 0x02,
}

/// Kind-specific terminal outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexOperationOutcome {
    /// Terminal outcome of a BUILD operation.
    Build(BuildOperationOutcome),
    /// An activated generation was fully dropped and cleaned.
    DropSucceeded,
}

impl IndexOperationOutcome {
    const fn kind(self) -> IndexOperationKind {
        match self {
            Self::Build(_) => IndexOperationKind::Build,
            Self::DropSucceeded => IndexOperationKind::Drop,
        }
    }
}

/// Durable scheduling/execution state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexOperationExecutionState {
    /// Runnable work awaiting its eligibility time.
    Queued {
        /// Earliest retry time in Unix milliseconds.
        not_before_unix_millis: Option<u64>,
    },
    /// Work exclusively owned by one fenced writer claim.
    Claimed(OperationClaim),
    /// Work stopped at a typed operator-remediable or safety boundary.
    Blocked(IndexOperationBlocker),
    /// Immutable terminal operation outcome.
    Completed(IndexOperationOutcome),
}

impl IndexOperationExecutionState {
    /// Returns a stable name for repository diagnostics.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Queued { .. } => "queued",
            Self::Claimed(_) => "claimed",
            Self::Blocked(_) => "blocked",
            Self::Completed(_) => "completed",
        }
    }
}

/// Canonical durable index operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexOperationRecord {
    operation_id: IndexOperationId,
    index_id: IndexId,
    identity: IndexIdentity,
    generation: IndexGenerationId,
    index_record_revision: IndexRevision,
    operation_revision: IndexOperationRevision,
    kind: IndexOperationKind,
    family: IndexOperationFamily,
    progress: IndexOperationProgress,
    attempt: u32,
    execution_state: IndexOperationExecutionState,
}

impl IndexOperationRecord {
    /// Validates every cross-field operation invariant.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        operation_id: IndexOperationId,
        index_id: IndexId,
        identity: IndexIdentity,
        generation: IndexGenerationId,
        index_record_revision: IndexRevision,
        operation_revision: IndexOperationRevision,
        kind: IndexOperationKind,
        family: IndexOperationFamily,
        progress: IndexOperationProgress,
        attempt: u32,
        execution_state: IndexOperationExecutionState,
    ) -> Result<Self, IndexOperationModelError> {
        if progress.family() != family {
            return Err(IndexOperationModelError::ProgressFamilyMismatch);
        }
        if progress.kind() != kind {
            return Err(IndexOperationModelError::ProgressKindMismatch);
        }
        if !family.owns_identity(&identity) {
            return Err(IndexOperationModelError::IdentityFamilyMismatch);
        }
        if let IndexOperationExecutionState::Completed(outcome) = execution_state
            && outcome.kind() != kind
        {
            return Err(IndexOperationModelError::CompletionKindMismatch);
        }
        if let IndexOperationExecutionState::Completed(outcome) = execution_state {
            let progress_matches = match outcome {
                IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded) => {
                    progress.is_constructing_build()
                }
                IndexOperationOutcome::Build(BuildOperationOutcome::Aborted) => {
                    progress.is_aborting_build()
                }
                IndexOperationOutcome::DropSucceeded => {
                    matches!(
                        progress,
                        IndexOperationProgress::SecondaryCleanup(_)
                            | IndexOperationProgress::VectorCleanup(_)
                            | IndexOperationProgress::TextCleanup(_)
                    )
                }
            };
            if !progress_matches {
                return Err(IndexOperationModelError::CompletionProgressMismatch);
            }
        }
        if let IndexOperationExecutionState::Blocked(blocker) = &execution_state {
            blocker.validate()?;
        }
        Ok(Self {
            operation_id,
            index_id,
            identity,
            generation,
            index_record_revision,
            operation_revision,
            kind,
            family,
            progress,
            attempt,
            execution_state,
        })
    }

    /// Returns the UUID used by the scoped record and global runnable pointer.
    pub const fn operation_id(&self) -> IndexOperationId {
        self.operation_id
    }

    /// Returns the logical index that owns this operation.
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Returns the identity needed to point-read the canonical index record.
    pub const fn identity(&self) -> &IndexIdentity {
        &self.identity
    }

    /// Returns the one physical generation this operation may mutate.
    pub const fn generation(&self) -> IndexGenerationId {
        self.generation
    }

    /// Returns the canonical index-record revision expected by this operation.
    pub const fn index_record_revision(&self) -> IndexRevision {
        self.index_record_revision
    }

    /// Returns the operation revision used for exact compare-and-swap updates.
    pub const fn operation_revision(&self) -> IndexOperationRevision {
        self.operation_revision
    }

    /// Returns whether the operation builds or drops a generation.
    pub const fn kind(&self) -> IndexOperationKind {
        self.kind
    }

    /// Returns the family driver allowed to execute the operation.
    pub const fn family(&self) -> IndexOperationFamily {
        self.family
    }

    /// Borrows the family- and stage-typed bounded progress checkpoint.
    pub const fn progress(&self) -> &IndexOperationProgress {
        &self.progress
    }

    /// Returns the persisted transient-failure attempt counter.
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Borrows the queue, claim, blocker, or terminal state and its typed payload.
    pub const fn execution_state(&self) -> &IndexOperationExecutionState {
        &self.execution_state
    }

    /// Acquires or replaces a repository-authorized durable claim.
    ///
    /// The repository proves whether a queued, prior-writer, or supervised
    /// same-writer claim may be replaced before calling this method.
    pub(crate) fn claim(&self, claim: OperationClaim) -> Result<Self, IndexOperationModelError> {
        if !matches!(
            self.execution_state,
            IndexOperationExecutionState::Queued { .. } | IndexOperationExecutionState::Claimed(_)
        ) {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "claim",
            });
        }
        self.next(
            self.index_record_revision,
            self.progress.clone(),
            self.attempt.saturating_add(1),
            IndexOperationExecutionState::Claimed(claim),
        )
    }

    /// Persists a successful bounded checkpoint and releases its claim.
    pub(crate) fn progressed(
        &self,
        progress: IndexOperationProgress,
    ) -> Result<Self, IndexOperationModelError> {
        if !matches!(
            self.execution_state,
            IndexOperationExecutionState::Claimed(_)
        ) {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "progress",
            });
        }
        self.next(
            self.index_record_revision,
            progress,
            self.attempt,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Releases a claim after a transient failure with a durable retry time.
    pub(crate) fn transient_failure(
        &self,
        not_before_unix_millis: u64,
    ) -> Result<Self, IndexOperationModelError> {
        if !matches!(
            self.execution_state,
            IndexOperationExecutionState::Claimed(_)
        ) {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "transient_failure",
            });
        }
        self.next(
            self.index_record_revision,
            self.progress.clone(),
            self.attempt,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: Some(not_before_unix_millis),
            },
        )
    }

    /// Persists a typed blocker and removes this operation from runnable work.
    pub(crate) fn block(
        &self,
        blocker: IndexOperationBlocker,
    ) -> Result<Self, IndexOperationModelError> {
        if !matches!(
            self.execution_state,
            IndexOperationExecutionState::Claimed(_)
        ) {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "block",
            });
        }
        self.next(
            self.index_record_revision,
            self.progress.clone(),
            self.attempt,
            IndexOperationExecutionState::Blocked(blocker),
        )
    }

    /// Blocks runnable build work on one exact mismatched publication intent.
    ///
    /// A child upload may discover corruption while this operation is queued
    /// or concurrently claimed. The repository commits this transition with
    /// the matching blocked upload and removes both runnable pointers, so the
    /// losing operation delivery cannot checkpoint an older revision.
    pub(crate) fn block_for_blob_mismatch(
        &self,
        intent_id: super::TextUploadIntentId,
    ) -> Result<Self, IndexOperationModelError> {
        if self.kind != IndexOperationKind::Build
            || self.family != IndexOperationFamily::Text
            || !self.progress.is_constructing_build()
            || !matches!(
                self.execution_state,
                IndexOperationExecutionState::Queued { .. }
                    | IndexOperationExecutionState::Claimed(_)
            )
        {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "block_for_blob_mismatch",
            });
        }
        self.next(
            self.index_record_revision,
            self.progress.clone(),
            self.attempt,
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::BlobPublicationMismatch {
                intent_id,
            }),
        )
    }

    /// Persists a terminal outcome linked to the next canonical revision.
    pub(crate) fn complete(
        &self,
        outcome: IndexOperationOutcome,
        index_record_revision: IndexRevision,
    ) -> Result<Self, IndexOperationModelError> {
        if !matches!(
            self.execution_state,
            IndexOperationExecutionState::Claimed(_)
        ) {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "complete",
            });
        }
        self.next(
            index_record_revision,
            self.progress.clone(),
            self.attempt,
            IndexOperationExecutionState::Completed(outcome),
        )
    }

    /// Requeues the exact blocked checkpoint without modifying physical state.
    pub(crate) fn retry(&self) -> Result<Self, IndexOperationModelError> {
        if !matches!(
            self.execution_state,
            IndexOperationExecutionState::Blocked(_)
        ) {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "retry",
            });
        }
        self.next(
            self.index_record_revision,
            self.progress.clone(),
            self.attempt,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Converts a constructing BUILD into the family's initial cleanup
    /// checkpoint while invalidating any queued delay or worker claim.
    pub(crate) fn begin_abort(
        &self,
        index_record_revision: IndexRevision,
    ) -> Result<Self, IndexOperationModelError> {
        let progress = match &self.progress {
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(_)) => {
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
                    SecondaryCleanupProgress::BeginDrain(DrainProgress::default()),
                ))
            }
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(_)) => {
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                    VectorCleanupProgress::BeginDrain(DrainProgress::default()),
                ))
            }
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(_)) => {
                IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                    TextCleanupProgress::BeginDrain(DrainProgress::default()),
                ))
            }
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(_))
            | IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(_))
            | IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(_))
            | IndexOperationProgress::SecondaryCleanup(_)
            | IndexOperationProgress::VectorCleanup(_)
            | IndexOperationProgress::TextCleanup(_) => {
                return Err(IndexOperationModelError::IllegalExecutionTransition {
                    from: self.execution_state.name(),
                    transition: "begin_abort",
                });
            }
        };
        if matches!(
            self.execution_state,
            IndexOperationExecutionState::Completed(_)
        ) {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "begin_abort",
            });
        }
        self.next(
            index_record_revision,
            progress,
            self.attempt,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    fn next(
        &self,
        index_record_revision: IndexRevision,
        progress: IndexOperationProgress,
        attempt: u32,
        execution_state: IndexOperationExecutionState,
    ) -> Result<Self, IndexOperationModelError> {
        Self::try_new(
            self.operation_id,
            self.index_id,
            self.identity.clone(),
            self.generation,
            index_record_revision,
            self.operation_revision.checked_next()?,
            self.kind,
            self.family,
            progress,
            attempt,
            execution_state,
        )
    }
}

impl From<IndexV2ModelError> for IndexOperationModelError {
    fn from(_value: IndexV2ModelError) -> Self {
        Self::InvalidBlocker("nested V2 model validation failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_v2::IndexComponent;

    fn identity() -> IndexIdentity {
        IndexIdentity::new(
            IndexIdentityFamily::SecondaryEquality,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "User").unwrap(),
            IndexComponent::try_new("property", "email").unwrap(),
        )
    }

    fn source_scan() -> SourceScanProgress {
        SourceScanProgress {
            inclusive_upper_bound: IndexCursor::try_new(Bytes::from_static(b"\x02upper")).unwrap(),
            cursor: None,
            counters: OperationCounters::default(),
        }
    }

    #[test]
    fn family_kind_and_completion_mismatches_are_unrepresentable_records() {
        let base = (
            IndexOperationId::from_bytes([1; 16]).unwrap(),
            IndexId::initial(),
            identity(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
        );
        let progress = IndexOperationProgress::SecondaryBuild(
            SecondaryBuildProgress::Constructing(SecondaryBuildStage::Scan(source_scan())),
        );
        assert!(matches!(
            IndexOperationRecord::try_new(
                base.0,
                base.1,
                base.2.clone(),
                base.3,
                base.4,
                base.5,
                IndexOperationKind::Build,
                IndexOperationFamily::Vector,
                progress.clone(),
                0,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            ),
            Err(IndexOperationModelError::ProgressFamilyMismatch)
        ));
        assert!(matches!(
            IndexOperationRecord::try_new(
                base.0,
                base.1,
                base.2,
                base.3,
                base.4,
                base.5,
                IndexOperationKind::Build,
                IndexOperationFamily::Secondary,
                progress,
                0,
                IndexOperationExecutionState::Completed(IndexOperationOutcome::DropSucceeded),
            ),
            Err(IndexOperationModelError::CompletionKindMismatch)
        ));

        let constructing = IndexOperationProgress::SecondaryBuild(
            SecondaryBuildProgress::Constructing(SecondaryBuildStage::Scan(source_scan())),
        );
        assert!(matches!(
            IndexOperationRecord::try_new(
                base.0,
                base.1,
                identity(),
                base.3,
                base.4,
                base.5,
                IndexOperationKind::Build,
                IndexOperationFamily::Secondary,
                constructing,
                0,
                IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                    BuildOperationOutcome::Aborted,
                )),
            ),
            Err(IndexOperationModelError::CompletionProgressMismatch)
        ));
    }

    #[test]
    fn stage_payload_shape_is_owned_by_each_stage_variant() {
        let stages = [
            SecondaryBuildStage::Scan(source_scan()),
            SecondaryBuildStage::CatchUp(PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            }),
            SecondaryBuildStage::Validate(PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            }),
            SecondaryBuildStage::Activate(NoCursorProgress::default()),
        ];
        assert_eq!(stages.len(), 4);
    }

    #[test]
    fn text_build_upload_progress_names_one_exact_non_empty_batch() {
        let source = SourceScanProgress {
            inclusive_upper_bound: IndexCursor::try_new(Bytes::from_static(b"\x04upper")).unwrap(),
            cursor: Some(IndexCursor::try_new(Bytes::from_static(b"\x01start")).unwrap()),
            counters: OperationCounters {
                entities: 1,
                input_bytes: 2,
                output_operations: 3,
                output_bytes: 4,
            },
        };
        let completed_cursor = IndexCursor::try_new(Bytes::from_static(b"\x02completed")).unwrap();
        let completed_counters = OperationCounters {
            entities: 2,
            input_bytes: 3,
            output_operations: 4,
            output_bytes: 5,
        };
        let artifact_key = IndexCursor::try_new(Bytes::from_static(b"\x05artifact")).unwrap();
        let intent_id = super::super::TextUploadIntentId::from_bytes([11; 16]).unwrap();

        let progress = TextBuildUploadProgress::try_new(
            source.clone(),
            completed_cursor.clone(),
            completed_counters,
            artifact_key.clone(),
            intent_id,
        )
        .unwrap();

        assert_eq!(progress.source(), &source);
        assert_eq!(progress.completed_cursor(), &completed_cursor);
        assert_eq!(progress.completed_counters(), completed_counters);
        assert_eq!(progress.artifact_key(), &artifact_key);
        assert_eq!(progress.intent_id(), intent_id);
        assert_eq!(
            progress.completed_source(),
            SourceScanProgress {
                inclusive_upper_bound: source.inclusive_upper_bound.clone(),
                cursor: Some(completed_cursor),
                counters: completed_counters,
            }
        );
    }

    #[test]
    fn text_build_upload_progress_rejects_every_invalid_batch_shape() {
        let source_counters = OperationCounters {
            entities: 1,
            input_bytes: 2,
            output_operations: 3,
            output_bytes: 4,
        };
        let source = SourceScanProgress {
            inclusive_upper_bound: IndexCursor::try_new(Bytes::from_static(b"\x04upper")).unwrap(),
            cursor: Some(IndexCursor::try_new(Bytes::from_static(b"\x01start")).unwrap()),
            counters: source_counters,
        };
        let completed_cursor = IndexCursor::try_new(Bytes::from_static(b"\x02completed")).unwrap();
        let completed_counters = OperationCounters {
            entities: 2,
            input_bytes: 3,
            output_operations: 4,
            output_bytes: 5,
        };
        let artifact_key = IndexCursor::try_new(Bytes::from_static(b"\x05artifact")).unwrap();
        let intent_id = super::super::TextUploadIntentId::from_bytes([12; 16]).unwrap();
        let assert_invalid = |source, completed_cursor, completed_counters, expected_message| {
            assert_eq!(
                TextBuildUploadProgress::try_new(
                    source,
                    completed_cursor,
                    completed_counters,
                    artifact_key.clone(),
                    intent_id,
                ),
                Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                    expected_message,
                ))
            );
        };

        assert_invalid(
            source.clone(),
            source.cursor.clone().unwrap(),
            completed_counters,
            "completed cursor must advance strictly beyond the source cursor",
        );
        assert_invalid(
            SourceScanProgress {
                inclusive_upper_bound: source.cursor.clone().unwrap(),
                cursor: None,
                counters: source_counters,
            },
            completed_cursor.clone(),
            completed_counters,
            "completed cursor exceeds the inclusive source upper bound",
        );
        assert_invalid(
            source.clone(),
            completed_cursor.clone(),
            OperationCounters {
                entities: source_counters.entities,
                ..completed_counters
            },
            "completed entity counter must advance for a non-empty source batch",
        );
        assert_invalid(
            source.clone(),
            completed_cursor.clone(),
            OperationCounters {
                input_bytes: source_counters.input_bytes,
                ..completed_counters
            },
            "completed input-byte counter must advance for a non-empty source batch",
        );
        assert_invalid(
            source.clone(),
            completed_cursor.clone(),
            OperationCounters {
                output_operations: source_counters.output_operations,
                ..completed_counters
            },
            "completed output-operation counter must advance by exactly one artifact",
        );
        assert_invalid(
            SourceScanProgress {
                counters: OperationCounters {
                    output_operations: u64::MAX,
                    ..source_counters
                },
                ..source.clone()
            },
            completed_cursor.clone(),
            OperationCounters {
                output_operations: u64::MAX,
                ..completed_counters
            },
            "output operation counter is exhausted",
        );
        assert_invalid(
            source,
            completed_cursor,
            OperationCounters {
                output_bytes: source_counters.output_bytes,
                ..completed_counters
            },
            "completed output-byte counter must advance for a non-empty artifact",
        );
    }

    #[test]
    fn text_catch_up_upload_progress_names_one_exact_delta() {
        let catch_up = PrefixScanProgress {
            cursor: None,
            counters: OperationCounters {
                entities: 1,
                input_bytes: 2,
                output_operations: 3,
                output_bytes: 4,
            },
        };
        let delta_key = IndexCursor::try_new(Bytes::from_static(b"delta")).unwrap();
        let artifact_key = IndexCursor::try_new(Bytes::from_static(b"artifact")).unwrap();
        let completed_counters = OperationCounters {
            entities: 2,
            input_bytes: 3,
            output_operations: 5,
            output_bytes: 6,
        };
        let intent_id = super::super::TextUploadIntentId::from_bytes([13; 16]).unwrap();

        let progress = TextCatchUpUploadProgress::try_new(
            catch_up.clone(),
            delta_key.clone(),
            completed_counters,
            artifact_key.clone(),
            intent_id,
        )
        .unwrap();

        assert_eq!(progress.catch_up(), &catch_up);
        assert_eq!(progress.delta_key(), &delta_key);
        assert_eq!(progress.completed_counters(), completed_counters);
        assert_eq!(progress.artifact_key(), &artifact_key);
        assert_eq!(progress.intent_id(), intent_id);
        assert_eq!(
            progress.completed_catch_up(),
            PrefixScanProgress {
                cursor: None,
                counters: completed_counters,
            }
        );
    }

    #[test]
    fn text_catch_up_upload_progress_rejects_invalid_checkpoint_shapes() {
        let counters = OperationCounters {
            entities: 1,
            input_bytes: 2,
            output_operations: 3,
            output_bytes: 4,
        };
        let completed = OperationCounters {
            entities: 2,
            input_bytes: 3,
            output_operations: 4,
            output_bytes: 5,
        };
        let delta_key = IndexCursor::try_new(Bytes::from_static(b"delta")).unwrap();
        let artifact_key = IndexCursor::try_new(Bytes::from_static(b"artifact")).unwrap();
        let intent_id = super::super::TextUploadIntentId::from_bytes([14; 16]).unwrap();
        let assert_invalid = |catch_up, completed_counters, expected| {
            assert_eq!(
                TextCatchUpUploadProgress::try_new(
                    catch_up,
                    delta_key.clone(),
                    completed_counters,
                    artifact_key.clone(),
                    intent_id,
                ),
                Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                    expected,
                ))
            );
        };

        assert_invalid(
            PrefixScanProgress {
                cursor: Some(IndexCursor::try_new(Bytes::from_static(b"cursor")).unwrap()),
                counters,
            },
            completed,
            "text catch-up must restart from the coalesced delta prefix",
        );
        assert_invalid(
            PrefixScanProgress {
                cursor: None,
                counters: OperationCounters {
                    entities: u64::MAX,
                    ..counters
                },
            },
            OperationCounters {
                entities: u64::MAX,
                ..completed
            },
            "text catch-up entity counter is exhausted",
        );
        for (completed_counters, expected) in [
            (
                OperationCounters {
                    entities: counters.entities,
                    ..completed
                },
                "completed catch-up entity counter must advance by exactly one",
            ),
            (
                OperationCounters {
                    input_bytes: counters.input_bytes,
                    ..completed
                },
                "completed catch-up input-byte counter must advance",
            ),
            (
                OperationCounters {
                    output_operations: counters.output_operations,
                    ..completed
                },
                "completed catch-up output-operation counter must advance",
            ),
            (
                OperationCounters {
                    output_bytes: counters.output_bytes,
                    ..completed
                },
                "completed catch-up output-byte counter must advance",
            ),
        ] {
            assert_invalid(
                PrefixScanProgress {
                    cursor: None,
                    counters,
                },
                completed_counters,
                expected,
            );
        }
    }

    #[test]
    fn text_compaction_upload_progress_names_one_exact_bounded_merge() {
        let compact = PrefixScanProgress {
            cursor: Some(IndexCursor::try_new(Bytes::from_static(b"partition-before")).unwrap()),
            counters: OperationCounters {
                entities: 2,
                input_bytes: 3,
                output_operations: 4,
                output_bytes: 5,
            },
        };
        let inputs = vec![
            IndexCursor::try_new(Bytes::from_static(b"artifact-a")).unwrap(),
            IndexCursor::try_new(Bytes::from_static(b"artifact-b")).unwrap(),
        ];
        let completed = OperationCounters {
            entities: 4,
            input_bytes: 6,
            output_operations: 7,
            output_bytes: 8,
        };
        let output = IndexCursor::try_new(Bytes::from_static(b"artifact-c")).unwrap();
        let intent_id = super::super::TextUploadIntentId::from_bytes([15; 16]).unwrap();

        let progress = TextCompactionUploadProgress::try_new(
            compact.clone(),
            inputs.clone(),
            completed,
            output.clone(),
            intent_id,
        )
        .unwrap();

        assert_eq!(progress.compact(), &compact);
        assert_eq!(progress.input_artifact_keys(), inputs);
        assert_eq!(progress.completed_counters(), completed);
        assert_eq!(progress.artifact_key(), &output);
        assert_eq!(progress.intent_id(), intent_id);
        assert_eq!(
            progress.completed_compaction(),
            PrefixScanProgress {
                cursor: compact.cursor,
                counters: completed,
            }
        );
    }

    #[test]
    fn text_compaction_upload_progress_rejects_every_invalid_shape() {
        let compact_counters = OperationCounters {
            entities: 2,
            input_bytes: 3,
            output_operations: 4,
            output_bytes: 5,
        };
        let compact = PrefixScanProgress {
            cursor: None,
            counters: compact_counters,
        };
        let completed = OperationCounters {
            entities: 3,
            input_bytes: 4,
            output_operations: 5,
            output_bytes: 6,
        };
        let first = IndexCursor::try_new(Bytes::from_static(b"artifact-a")).unwrap();
        let second = IndexCursor::try_new(Bytes::from_static(b"artifact-b")).unwrap();
        let output = IndexCursor::try_new(Bytes::from_static(b"artifact-c")).unwrap();
        let intent_id = super::super::TextUploadIntentId::from_bytes([16; 16]).unwrap();
        let assert_invalid = |inputs, completed_counters, output_key, expected| {
            assert_eq!(
                TextCompactionUploadProgress::try_new(
                    compact.clone(),
                    inputs,
                    completed_counters,
                    output_key,
                    intent_id,
                ),
                Err(IndexOperationModelError::InvalidTextBuildUploadProgress(
                    expected,
                ))
            );
        };

        assert_invalid(
            vec![first.clone()],
            completed,
            output.clone(),
            "text compaction must retain a bounded useful input set",
        );
        assert_invalid(
            vec![second.clone(), first.clone()],
            completed,
            output.clone(),
            "text compaction input artifact keys must be strictly sorted",
        );
        assert_invalid(
            vec![first.clone(), second.clone()],
            completed,
            second.clone(),
            "text compaction replacement artifact must not overwrite an input",
        );
        let large_first =
            IndexCursor::try_new(Bytes::from(vec![1; INDEX_CURSOR_MAX_LEN / 2 + 1])).unwrap();
        let large_second =
            IndexCursor::try_new(Bytes::from(vec![2; INDEX_CURSOR_MAX_LEN / 2 + 1])).unwrap();
        assert_invalid(
            vec![large_first, large_second],
            completed,
            output.clone(),
            "text compaction retained artifact keys exceed the operation bound",
        );
        for (completed_counters, expected) in [
            (
                OperationCounters {
                    entities: compact_counters.entities - 1,
                    ..completed
                },
                "text compaction entity counter cannot move backwards",
            ),
            (
                OperationCounters {
                    input_bytes: compact_counters.input_bytes,
                    ..completed
                },
                "text compaction input-byte counter must advance",
            ),
            (
                OperationCounters {
                    output_operations: compact_counters.output_operations,
                    ..completed
                },
                "text compaction output-operation counter must advance",
            ),
            (
                OperationCounters {
                    output_bytes: compact_counters.output_bytes,
                    ..completed
                },
                "text compaction output-byte counter must advance",
            ),
        ] {
            assert_invalid(
                vec![first.clone(), second.clone()],
                completed_counters,
                output.clone(),
                expected,
            );
        }
        assert_invalid(
            (0..=TEXT_COMPACTION_INPUT_KEY_MAX)
                .map(|ordinal| {
                    IndexCursor::try_new(Bytes::from(format!("artifact-{ordinal:04}"))).unwrap()
                })
                .collect(),
            completed,
            output,
            "text compaction must retain a bounded useful input set",
        );
    }

    #[test]
    fn cursor_claim_and_blocker_bounds_fail_closed() {
        assert!(IndexCursor::try_new(Bytes::from(vec![0; INDEX_CURSOR_MAX_LEN + 1])).is_err());
        assert!(ClaimSequence::new(0).is_err());
        assert!(IndexOperationBlocker::OversizedEntity {
            entity_kind: IndexElementKind::Node,
            entity_id: IndexEntityId::initial(),
            observed: 10,
            limit: 10,
        }
        .validate()
        .is_err());
    }

    #[test]
    fn execution_transitions_require_claims_and_revision_every_result() {
        let operation_id = IndexOperationId::from_bytes([8; 16]).unwrap();
        let progress =
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Activate(NoCursorProgress::default()),
            ));
        let queued = IndexOperationRecord::try_new(
            operation_id,
            IndexId::initial(),
            identity(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            progress.clone(),
            u32::MAX,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        assert!(matches!(
            queued.block(IndexOperationBlocker::InvariantViolation),
            Err(IndexOperationModelError::IllegalExecutionTransition { .. })
        ));
        assert!(matches!(
            queued.retry(),
            Err(IndexOperationModelError::IllegalExecutionTransition { .. })
        ));

        let claimed = queued
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([9; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            })
            .unwrap();
        assert_eq!(claimed.attempt(), u32::MAX);
        assert_eq!(
            claimed.operation_revision(),
            IndexOperationRevision::new(2).unwrap()
        );
        let progressed = claimed.progressed(progress.clone()).unwrap();
        assert!(matches!(
            progressed.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None
            }
        ));

        let claimed = progressed
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([9; 16]).unwrap(),
                sequence: ClaimSequence::new(2).unwrap(),
            })
            .unwrap();
        let delayed = claimed.transient_failure(123).unwrap();
        assert!(matches!(
            delayed.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: Some(123)
            }
        ));

        let claimed = delayed
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([9; 16]).unwrap(),
                sequence: ClaimSequence::new(3).unwrap(),
            })
            .unwrap();
        let blocked = claimed
            .block(IndexOperationBlocker::InvariantViolation)
            .unwrap();
        let retried = blocked.retry().unwrap();
        let claimed = retried
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([9; 16]).unwrap(),
                sequence: ClaimSequence::new(4).unwrap(),
            })
            .unwrap();
        let completed = claimed
            .complete(
                IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
                IndexRevision::new(2).unwrap(),
            )
            .unwrap();
        assert!(matches!(
            completed.execution_state(),
            IndexOperationExecutionState::Completed(_)
        ));
        assert!(matches!(
            completed.claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([9; 16]).unwrap(),
                sequence: ClaimSequence::new(5).unwrap(),
            }),
            Err(IndexOperationModelError::IllegalExecutionTransition { .. })
        ));
    }

    #[test]
    fn text_manifest_validation_progress_rejects_partial_or_completed_accumulators() {
        let valid = TextManifestPartitionValidation::try_new(
            [7; 32],
            super::super::TextManifestRevision::new(4).unwrap(),
            3,
            5,
            2,
            3,
        )
        .unwrap();
        assert_eq!(valid.partition_fingerprint(), &[7; 32]);
        assert_eq!(valid.root_revision().get(), 4);
        assert_eq!(valid.page_count(), 3);
        assert_eq!(valid.split_count(), 5);
        assert_eq!(valid.next_page(), 2);
        assert_eq!(valid.observed_split_count(), 3);

        for invalid in [
            TextManifestPartitionValidation::try_new(
                [7; 32],
                super::super::TextManifestRevision::initial(),
                0,
                1,
                1,
                1,
            ),
            TextManifestPartitionValidation::try_new(
                [7; 32],
                super::super::TextManifestRevision::new(4).unwrap(),
                3,
                0,
                1,
                1,
            ),
            TextManifestPartitionValidation::try_new(
                [7; 32],
                super::super::TextManifestRevision::new(4).unwrap(),
                3,
                5,
                0,
                1,
            ),
            TextManifestPartitionValidation::try_new(
                [7; 32],
                super::super::TextManifestRevision::new(4).unwrap(),
                3,
                5,
                1,
                0,
            ),
            TextManifestPartitionValidation::try_new(
                [7; 32],
                super::super::TextManifestRevision::new(3).unwrap(),
                3,
                5,
                1,
                1,
            ),
            TextManifestPartitionValidation::try_new(
                [7; 32],
                super::super::TextManifestRevision::new(4).unwrap(),
                3,
                5,
                3,
                5,
            ),
            TextManifestPartitionValidation::try_new(
                [7; 32],
                super::super::TextManifestRevision::new(4).unwrap(),
                3,
                5,
                4,
                5,
            ),
            TextManifestPartitionValidation::try_new(
                [7; 32],
                super::super::TextManifestRevision::new(4).unwrap(),
                3,
                5,
                2,
                1,
            ),
            TextManifestPartitionValidation::try_new(
                [7; 32],
                super::super::TextManifestRevision::new(4).unwrap(),
                3,
                5,
                1,
                6,
            ),
            TextManifestPartitionValidation::try_new(
                [7; 32],
                super::super::TextManifestRevision::new(4).unwrap(),
                3,
                4,
                1,
                3,
            ),
        ] {
            assert!(matches!(
                invalid,
                Err(IndexOperationModelError::InvalidTextManifestValidationProgress(_))
            ));
        }

        let counters = OperationCounters {
            entities: 1,
            input_bytes: 2,
            output_operations: 3,
            output_bytes: 4,
        };
        let initial = TextManifestValidationProgress::initial(counters);
        assert_eq!(initial.counters(), counters);
        assert!(matches!(
            initial,
            TextManifestValidationProgress::Pages(TextManifestPageValidationProgress {
                cursor: None,
                partition: None,
                ..
            })
        ));
        assert!(matches!(
            TextManifestPageValidationProgress::try_new(None, Some(valid), counters),
            Err(IndexOperationModelError::InvalidTextManifestValidationProgress(_))
        ));
        let page_cursor = IndexCursor::try_new(Bytes::from_static(b"manifest-page")).unwrap();
        let resumable =
            TextManifestPageValidationProgress::try_new(Some(page_cursor), Some(valid), counters)
                .unwrap();
        assert!(resumable.cursor().is_some());
        assert_eq!(resumable.partition(), Some(&valid));
        assert_eq!(resumable.counters(), counters);
        for progress in [
            TextManifestValidationProgress::Roots(PrefixScanProgress {
                cursor: None,
                counters,
            }),
            TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
                cursor: None,
                counters,
            }),
        ] {
            assert_eq!(progress.counters(), counters);
        }
        let prefix_cursor = IndexCursor::try_new(Bytes::from_static(b"prefix")).unwrap();
        for progress in [
            TextManifestValidationProgress::Pages(resumable),
            TextManifestValidationProgress::Roots(PrefixScanProgress {
                cursor: Some(prefix_cursor.clone()),
                counters,
            }),
            TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
                cursor: Some(prefix_cursor),
                counters,
            }),
        ] {
            let progress = IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ValidateManifests(progress),
            ));
            assert!(progress.cursors_are_valid(|_| true));
            assert!(!progress.cursors_are_valid(|_| false));
        }
    }

    #[test]
    fn text_owner_retirement_requires_an_assigned_run_and_candidate_boundary() {
        let cursor = IndexCursor::try_new(Bytes::from_static(b"candidate-boundary")).unwrap();
        let run_id = super::super::BlobGcRunId::from_bytes([41; 16]).unwrap();
        let assigned = GcProgress {
            gc_run_id: Some(run_id),
            candidate_cursor: Some(cursor.clone()),
            stage_cursor: None,
            counters: OperationCounters::default(),
        };
        for progress in [
            TextCleanupProgress::RetireManifest(assigned.clone()),
            TextCleanupProgress::RetireArtifacts(assigned.clone()),
            TextCleanupProgress::RetireUploadIntents(assigned.clone()),
            TextCleanupProgress::MarkReachability(assigned.clone()),
            TextCleanupProgress::DeleteBlobs(assigned.clone()),
        ] {
            assert!(
                IndexOperationProgress::TextCleanup(progress).cursors_are_valid(|_| true),
                "assigned cleanup progress retains both durable identities"
            );
        }
        for progress in [
            GcProgress {
                gc_run_id: None,
                candidate_cursor: Some(cursor.clone()),
                stage_cursor: None,
                counters: OperationCounters::default(),
            },
            GcProgress {
                gc_run_id: Some(run_id),
                candidate_cursor: None,
                stage_cursor: None,
                counters: OperationCounters::default(),
            },
        ] {
            assert!(
                !IndexOperationProgress::TextCleanup(TextCleanupProgress::RetireManifest(progress))
                    .cursors_are_valid(|_| true)
            );
        }
        assert!(
            !IndexOperationProgress::TextCleanup(TextCleanupProgress::AcquireDeleteFences(
                GcProgress {
                    gc_run_id: Some(run_id),
                    candidate_cursor: Some(cursor.clone()),
                    stage_cursor: Some(cursor),
                    counters: OperationCounters::default(),
                }
            ))
            .cursors_are_valid(|_| true)
        );
    }

    #[test]
    fn blob_mismatch_blocks_only_runnable_constructing_text_builds() {
        let intent_id = super::super::TextUploadIntentId::from_bytes([31; 16]).unwrap();
        let queued = IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([30; 16]).unwrap(),
            IndexId::initial(),
            IndexIdentity::new(
                IndexIdentityFamily::Text,
                IndexElementKind::Node,
                IndexComponent::try_new("label", "Document").unwrap(),
                IndexComponent::try_new("property", "body").unwrap(),
            ),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Compact(PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                }),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        let blocked = queued.block_for_blob_mismatch(intent_id).unwrap();
        assert_eq!(
            blocked.operation_revision(),
            IndexOperationRevision::new(2).unwrap()
        );
        assert!(matches!(
            blocked.execution_state(),
            IndexOperationExecutionState::Blocked(
                IndexOperationBlocker::BlobPublicationMismatch {
                    intent_id: blocked_intent_id,
                }
            ) if *blocked_intent_id == intent_id
        ));
        assert!(matches!(
            blocked.block_for_blob_mismatch(intent_id),
            Err(IndexOperationModelError::IllegalExecutionTransition { .. })
        ));

        let claimed = queued
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([32; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            })
            .unwrap();
        assert!(claimed.block_for_blob_mismatch(intent_id).is_ok());

        let secondary = IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([33; 16]).unwrap(),
            IndexId::initial(),
            identity(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Activate(NoCursorProgress::default()),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        assert!(matches!(
            secondary.block_for_blob_mismatch(intent_id),
            Err(IndexOperationModelError::IllegalExecutionTransition { .. })
        ));
    }
}
