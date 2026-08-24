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
    /// A private queue schedule disagrees with its public execution state.
    #[error("invalid operation queue schedule: {0}")]
    InvalidQueueSchedule(&'static str),
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
/// exact `encoding/v2` key parser for its known scope. Keeping scope outside the
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
            || root_revision.get() < minimum_root_revision
        {
            return Err(
                IndexOperationModelError::InvalidTextManifestValidationProgress(
                    "partition counts, next page, split total, or minimum root revision disagree",
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
    /// Validate every page, root relationship, split count, and blob.
    Pages(TextManifestPageValidationProgress),
    /// Validate every root, including valid empty partitions and page-less corruption.
    Roots(PrefixScanProgress),
    /// Validate every entity state against its exact owning root revision.
    EntityStates(PrefixScanProgress),
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
            Self::Roots(progress) | Self::EntityStates(progress) => progress.counters,
        }
    }
}

/// Step whose state is fully represented by counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct NoCursorProgress {
    /// Cumulative counters.
    pub counters: OperationCounters,
}

/// Stable physical-lane order for validating one adopted legacy vector namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LegacyVectorValidationLane {
    /// Default vector index keyspace containing metadata and the transaction guard.
    Core,
    /// Vector-hot keyspace containing upper rows, layer-0 neighbors, and SimHash.
    Hot,
    /// Layer-0 keyspace containing payloads, candidates, and reverse locators.
    Layer0,
}

impl LegacyVectorValidationLane {
    /// Returns the next physical lane, or `None` after layer zero.
    pub const fn next(self) -> Option<Self> {
        match self {
            Self::Core => Some(Self::Hot),
            Self::Hot => Some(Self::Layer0),
            Self::Layer0 => None,
        }
    }
}

/// Typed checkpoint for bounded validation of legacy vector rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LegacyVectorValidationProgress {
    /// Physical lane currently being validated.
    pub lane: LegacyVectorValidationLane,
    /// Last complete physical key validated in this lane.
    pub cursor: Option<IndexCursor>,
    /// Cumulative validation and directory-write counters.
    pub counters: OperationCounters,
}

impl LegacyVectorValidationProgress {
    /// Starts validation at the core lane without a cursor.
    pub const fn initial() -> Self {
        Self {
            lane: LegacyVectorValidationLane::Core,
            cursor: None,
            counters: OperationCounters {
                entities: 0,
                input_bytes: 0,
                output_operations: 0,
                output_bytes: 0,
            },
        }
    }
}

/// Typed checkpoint for proving one newly adopted SimHash directory complete.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LegacyVectorDirectoryValidationProgress {
    /// Last complete compact directory key validated.
    pub cursor: Option<IndexCursor>,
    /// Exact marker-write count produced from canonical vector rows.
    pub expected_markers: u64,
    /// Exact marker count validated through the current cursor.
    pub verified_markers: u64,
    /// Cumulative adoption counters. Directory validation adds only reads.
    pub counters: OperationCounters,
}

impl LegacyVectorDirectoryValidationProgress {
    /// Starts compact validation against the completed canonical backfill.
    pub const fn initial(expected_markers: u64, counters: OperationCounters) -> Self {
        Self {
            cursor: None,
            expected_markers,
            verified_markers: 0,
            counters,
        }
    }
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
    /// Validate an unchanged, hash-derived pre-V2 physical namespace.
    AdoptLegacy(LegacyVectorValidationProgress),
    /// Validate the compact directory produced while adopting legacy vectors.
    ValidateAdoptedDirectory(LegacyVectorDirectoryValidationProgress),
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
    /// Apply coalesced mutations that raced the source scan.
    CatchUp(PrefixScanProgress),
    /// Compact bounded staged split sets.
    Compact(PrefixScanProgress),
    /// Construct canonical manifest pages and roots for every partition.
    PrepareManifests(PrefixScanProgress),
    /// Bounded physical proof before canonical activation.
    ValidateManifests(TextManifestValidationProgress),
    /// Publish the validated hidden generation.
    Activate(NoCursorProgress),
}

/// Secondary cleanup stages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SecondaryCleanupProgress {
    /// Delete all owned secondary entry rows.
    DeleteEntries(PrefixScanProgress),
    /// Delete coalesced mutation rows.
    DeleteDeltas(PrefixScanProgress),
    /// Commit terminal catalog and operation state.
    Finalize(NoCursorProgress),
}

/// Vector cleanup stages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VectorCleanupProgress {
    /// Retire and clear the exact resident vector snapshot.
    RetireCache(NoCursorProgress),
    /// Delete all owned physical vector row families.
    DeletePhysical(PrefixScanProgress),
    /// Delete coalesced mutation rows.
    DeleteDeltas(PrefixScanProgress),
    /// Commit terminal catalog and operation state.
    Finalize(NoCursorProgress),
}

/// Text cleanup stages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TextCleanupProgress {
    /// Delete every generation-qualified metadata row without deleting blobs.
    DeleteMetadata(PrefixScanProgress),
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
    /// The caller supplies scope-aware `encoding/v2` parsing because scope is
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
                SecondaryCleanupProgress::Finalize(_) => true,
            },
            Self::VectorBuild(VectorBuildProgress::Constructing(stage)) => match stage {
                VectorBuildStage::AdoptLegacy(progress) => {
                    progress.cursor.as_ref().is_none_or(&mut validate)
                }
                VectorBuildStage::ValidateAdoptedDirectory(progress) => {
                    progress.cursor.as_ref().is_none_or(&mut validate)
                        && progress.verified_markers <= progress.expected_markers
                }
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
                VectorCleanupProgress::RetireCache(_) | VectorCleanupProgress::Finalize(_) => true,
            },
            Self::TextBuild(TextBuildProgress::Constructing(stage)) => match stage {
                TextBuildStage::ScanSource(progress) | TextBuildStage::ScanPartitions(progress) => {
                    source_is_valid(progress, &mut validate)
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
                    | TextManifestValidationProgress::EntityStates(progress) => {
                        prefix_is_valid(progress, &mut validate)
                    }
                },
                TextBuildStage::Activate(_) => true,
            },
            Self::TextBuild(TextBuildProgress::Aborting(progress))
            | Self::TextCleanup(progress) => match progress {
                TextCleanupProgress::DeleteMetadata(progress) => {
                    prefix_is_valid(progress, &mut validate)
                }
                TextCleanupProgress::Finalize(_) => true,
            },
        }
    }

    fn try_map_cursors<E>(
        &mut self,
        map: &mut impl FnMut(&IndexCursor) -> Result<IndexCursor, E>,
    ) -> Result<(), E> {
        fn map_optional<E>(
            cursor: &mut Option<IndexCursor>,
            map: &mut impl FnMut(&IndexCursor) -> Result<IndexCursor, E>,
        ) -> Result<(), E> {
            let Some(current) = cursor.as_ref() else {
                return Ok(());
            };
            *cursor = Some(map(current)?);
            Ok(())
        }

        fn map_source<E>(
            progress: &mut SourceScanProgress,
            map: &mut impl FnMut(&IndexCursor) -> Result<IndexCursor, E>,
        ) -> Result<(), E> {
            progress.inclusive_upper_bound = map(&progress.inclusive_upper_bound)?;
            map_optional(&mut progress.cursor, map)
        }

        fn map_prefix<E>(
            progress: &mut PrefixScanProgress,
            map: &mut impl FnMut(&IndexCursor) -> Result<IndexCursor, E>,
        ) -> Result<(), E> {
            map_optional(&mut progress.cursor, map)
        }

        match self {
            Self::SecondaryBuild(SecondaryBuildProgress::Constructing(stage)) => match stage {
                SecondaryBuildStage::Scan(progress) => map_source(progress, map)?,
                SecondaryBuildStage::CatchUp(progress)
                | SecondaryBuildStage::Validate(progress) => map_prefix(progress, map)?,
                SecondaryBuildStage::Activate(_) => {}
            },
            Self::SecondaryBuild(SecondaryBuildProgress::Aborting(progress))
            | Self::SecondaryCleanup(progress) => match progress {
                SecondaryCleanupProgress::DeleteEntries(progress)
                | SecondaryCleanupProgress::DeleteDeltas(progress) => map_prefix(progress, map)?,
                SecondaryCleanupProgress::Finalize(_) => {}
            },
            Self::VectorBuild(VectorBuildProgress::Constructing(stage)) => match stage {
                VectorBuildStage::AdoptLegacy(progress) => {
                    map_optional(&mut progress.cursor, map)?;
                }
                VectorBuildStage::ValidateAdoptedDirectory(progress) => {
                    map_optional(&mut progress.cursor, map)?;
                }
                VectorBuildStage::Scan(progress) => map_source(progress, map)?,
                VectorBuildStage::CatchUp(progress)
                | VectorBuildStage::ValidateDescriptor(progress) => map_prefix(progress, map)?,
                VectorBuildStage::Activate(_) => {}
            },
            Self::VectorBuild(VectorBuildProgress::Aborting(progress))
            | Self::VectorCleanup(progress) => match progress {
                VectorCleanupProgress::DeletePhysical(progress)
                | VectorCleanupProgress::DeleteDeltas(progress) => map_prefix(progress, map)?,
                VectorCleanupProgress::RetireCache(_) | VectorCleanupProgress::Finalize(_) => {}
            },
            Self::TextBuild(TextBuildProgress::Constructing(stage)) => match stage {
                TextBuildStage::ScanSource(progress) | TextBuildStage::ScanPartitions(progress) => {
                    map_source(progress, map)?
                }
                TextBuildStage::CatchUp(progress)
                | TextBuildStage::Compact(progress)
                | TextBuildStage::PrepareManifests(progress) => map_prefix(progress, map)?,
                TextBuildStage::ValidateManifests(progress) => match progress {
                    TextManifestValidationProgress::Pages(progress) => {
                        map_optional(&mut progress.cursor, map)?;
                    }
                    TextManifestValidationProgress::Roots(progress)
                    | TextManifestValidationProgress::EntityStates(progress) => {
                        map_prefix(progress, map)?;
                    }
                },
                TextBuildStage::Activate(_) => {}
            },
            Self::TextBuild(TextBuildProgress::Aborting(progress))
            | Self::TextCleanup(progress) => match progress {
                TextCleanupProgress::DeleteMetadata(progress) => map_prefix(progress, map)?,
                TextCleanupProgress::Finalize(_) => {}
            },
        }
        Ok(())
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
    /// Text work requires object storage that is not configured.
    ObjectStoreConfigurationUnavailable,
    /// Persisted state violates a lifecycle invariant.
    InvariantViolation,
    /// A persisted legacy vector namespace failed structural validation.
    InvalidLegacyPhysical,
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
            | Self::ObjectStoreConfigurationUnavailable
            | Self::InvariantViolation
            | Self::InvalidLegacyPhysical => Ok(()),
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

/// Private cause carried by one queued operation.
///
/// The public execution state intentionally projects every variant as queued.
/// Persistence retains the cause so blocking startup can distinguish ordinary
/// delayed progress from a failure emitted by its own worker epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IndexOperationQueueSchedule {
    Immediate,
    DelayedAfterProgress {
        not_before_unix_millis: u64,
    },
    DelayedAfterTransientFailure {
        not_before_unix_millis: u64,
        failed_writer_epoch: WriterEpoch,
    },
}

impl IndexOperationQueueSchedule {
    pub(crate) const fn not_before_unix_millis(self) -> Option<u64> {
        match self {
            Self::Immediate => None,
            Self::DelayedAfterProgress {
                not_before_unix_millis,
            }
            | Self::DelayedAfterTransientFailure {
                not_before_unix_millis,
                ..
            } => Some(not_before_unix_millis),
        }
    }

    pub(crate) fn transient_failure_from(self, writer_epoch: WriterEpoch) -> bool {
        matches!(
            self,
            Self::DelayedAfterTransientFailure {
                failed_writer_epoch,
                ..
            } if failed_writer_epoch == writer_epoch
        )
    }

    pub(crate) fn is_eligible_for(self, writer_epoch: WriterEpoch, now_unix_millis: u64) -> bool {
        match self {
            Self::Immediate => true,
            Self::DelayedAfterProgress {
                not_before_unix_millis,
            } => not_before_unix_millis <= now_unix_millis,
            Self::DelayedAfterTransientFailure {
                not_before_unix_millis,
                failed_writer_epoch,
            } => failed_writer_epoch != writer_epoch || not_before_unix_millis <= now_unix_millis,
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
    queue_schedule: Option<IndexOperationQueueSchedule>,
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
        let queue_schedule = match &execution_state {
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            } => Some(IndexOperationQueueSchedule::Immediate),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: Some(not_before_unix_millis),
            } => Some(IndexOperationQueueSchedule::DelayedAfterProgress {
                not_before_unix_millis: *not_before_unix_millis,
            }),
            IndexOperationExecutionState::Claimed(_)
            | IndexOperationExecutionState::Blocked(_)
            | IndexOperationExecutionState::Completed(_) => None,
        };
        Self::try_new_with_queue_schedule(
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
            queue_schedule,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new_with_queue_schedule(
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
        queue_schedule: Option<IndexOperationQueueSchedule>,
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
        match (&execution_state, queue_schedule) {
            (
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis,
                },
                Some(schedule),
            ) if *not_before_unix_millis == schedule.not_before_unix_millis() => {}
            (IndexOperationExecutionState::Queued { .. }, None) => {
                return Err(IndexOperationModelError::InvalidQueueSchedule(
                    "queued operation has no schedule",
                ));
            }
            (IndexOperationExecutionState::Queued { .. }, Some(_)) => {
                return Err(IndexOperationModelError::InvalidQueueSchedule(
                    "queued deadline disagrees with schedule",
                ));
            }
            (
                IndexOperationExecutionState::Claimed(_)
                | IndexOperationExecutionState::Blocked(_)
                | IndexOperationExecutionState::Completed(_),
                None,
            ) => {}
            (
                IndexOperationExecutionState::Claimed(_)
                | IndexOperationExecutionState::Blocked(_)
                | IndexOperationExecutionState::Completed(_),
                Some(_),
            ) => {
                return Err(IndexOperationModelError::InvalidQueueSchedule(
                    "non-queued operation has a schedule",
                ));
            }
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
            queue_schedule,
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

    pub(crate) const fn queue_schedule(&self) -> Option<IndexOperationQueueSchedule> {
        self.queue_schedule
    }

    /// Rewrites every complete cursor without changing operation revisions or
    /// execution state. This is reserved for blocking physical-key migrations.
    pub(crate) fn try_map_cursors<E>(
        &self,
        mut map: impl FnMut(&IndexCursor) -> Result<IndexCursor, E>,
    ) -> Result<Self, E> {
        let mut mapped = self.clone();
        mapped.progress.try_map_cursors(&mut map)?;
        Ok(mapped)
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

    /// Persists a successful checkpoint and releases its claim after a deadline.
    pub(crate) fn progressed_after(
        &self,
        progress: IndexOperationProgress,
        not_before_unix_millis: u64,
    ) -> Result<Self, IndexOperationModelError> {
        if !matches!(
            self.execution_state,
            IndexOperationExecutionState::Claimed(_)
        ) {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "progress_after",
            });
        }
        self.next(
            self.index_record_revision,
            progress,
            self.attempt,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: Some(not_before_unix_millis),
            },
        )
    }

    /// Releases a claim after a transient failure with a durable retry time.
    pub(crate) fn transient_failure(
        &self,
        not_before_unix_millis: u64,
    ) -> Result<Self, IndexOperationModelError> {
        let IndexOperationExecutionState::Claimed(claim) = self.execution_state else {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "transient_failure",
            });
        };
        self.next_with_queue_schedule(
            self.index_record_revision,
            self.progress.clone(),
            self.attempt,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: Some(not_before_unix_millis),
            },
            Some(IndexOperationQueueSchedule::DelayedAfterTransientFailure {
                not_before_unix_millis,
                failed_writer_epoch: claim.writer_epoch,
            }),
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

    /// Rebinds one retained successful build to an atomically published index revision.
    pub(crate) fn rebind_completed_index_revision(
        &self,
        index_record_revision: IndexRevision,
    ) -> Result<Self, IndexOperationModelError> {
        if self.kind != IndexOperationKind::Build
            || self.family != IndexOperationFamily::Vector
            || !matches!(
                self.execution_state,
                IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                    BuildOperationOutcome::Succeeded
                ))
            )
        {
            return Err(IndexOperationModelError::IllegalExecutionTransition {
                from: self.execution_state.name(),
                transition: "rebind_completed_index_revision",
            });
        }
        self.next(
            index_record_revision,
            self.progress.clone(),
            self.attempt,
            self.execution_state.clone(),
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
                    SecondaryCleanupProgress::DeleteEntries(PrefixScanProgress {
                        cursor: None,
                        counters: OperationCounters::default(),
                    }),
                ))
            }
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(_)) => {
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                    VectorCleanupProgress::RetireCache(NoCursorProgress::default()),
                ))
            }
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(_)) => {
                IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                    TextCleanupProgress::DeleteMetadata(PrefixScanProgress {
                        cursor: None,
                        counters: OperationCounters::default(),
                    }),
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
        let queue_schedule = match &execution_state {
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            } => Some(IndexOperationQueueSchedule::Immediate),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: Some(not_before_unix_millis),
            } => Some(IndexOperationQueueSchedule::DelayedAfterProgress {
                not_before_unix_millis: *not_before_unix_millis,
            }),
            IndexOperationExecutionState::Claimed(_)
            | IndexOperationExecutionState::Blocked(_)
            | IndexOperationExecutionState::Completed(_) => None,
        };
        self.next_with_queue_schedule(
            index_record_revision,
            progress,
            attempt,
            execution_state,
            queue_schedule,
        )
    }

    fn next_with_queue_schedule(
        &self,
        index_record_revision: IndexRevision,
        progress: IndexOperationProgress,
        attempt: u32,
        execution_state: IndexOperationExecutionState,
        queue_schedule: Option<IndexOperationQueueSchedule>,
    ) -> Result<Self, IndexOperationModelError> {
        Self::try_new_with_queue_schedule(
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
            queue_schedule,
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
    use crate::index_lifecycle::{IndexComponent, TextManifestRevision};

    fn cursor(value: u8) -> IndexCursor {
        IndexCursor::try_new(Bytes::from(vec![value])).unwrap()
    }

    fn source() -> SourceScanProgress {
        SourceScanProgress {
            inclusive_upper_bound: cursor(1),
            cursor: Some(cursor(2)),
            counters: OperationCounters::default(),
        }
    }

    fn prefix() -> PrefixScanProgress {
        PrefixScanProgress {
            cursor: Some(cursor(3)),
            counters: OperationCounters::default(),
        }
    }

    fn identity(family: IndexIdentityFamily) -> IndexIdentity {
        IndexIdentity::new(
            family,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "Document").unwrap(),
            IndexComponent::try_new("property", "value").unwrap(),
        )
    }

    fn identity_family(family: IndexOperationFamily) -> IndexIdentityFamily {
        match family {
            IndexOperationFamily::Secondary => IndexIdentityFamily::SecondaryEquality,
            IndexOperationFamily::Vector => IndexIdentityFamily::Vector,
            IndexOperationFamily::Text => IndexIdentityFamily::Text,
        }
    }

    fn record(
        progress: IndexOperationProgress,
        execution_state: IndexOperationExecutionState,
    ) -> IndexOperationRecord {
        let family = progress.family();
        IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([7; 16]).unwrap(),
            IndexId::new(2).unwrap(),
            identity(identity_family(family)),
            IndexGenerationId::new(3).unwrap(),
            IndexRevision::new(4).unwrap(),
            IndexOperationRevision::new(5).unwrap(),
            progress.kind(),
            family,
            progress,
            6,
            execution_state,
        )
        .unwrap()
    }

    fn secondary_constructing() -> IndexOperationProgress {
        IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
            SecondaryBuildStage::Scan(source()),
        ))
    }

    fn vector_constructing() -> IndexOperationProgress {
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::Scan(source()),
        ))
    }

    fn text_constructing() -> IndexOperationProgress {
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanSource(source()),
        ))
    }

    fn claim(writer: u8, sequence: u64) -> OperationClaim {
        OperationClaim {
            writer_epoch: WriterEpoch::from_bytes([writer; 16]).unwrap(),
            sequence: ClaimSequence::new(sequence).unwrap(),
        }
    }

    #[test]
    fn cursor_and_manifest_checkpoints_reject_every_invalid_boundary() {
        assert_eq!(
            IndexCursor::try_new(Bytes::new()).unwrap().as_bytes().len(),
            0
        );
        assert_eq!(
            IndexCursor::try_new(Bytes::from(vec![0; INDEX_CURSOR_MAX_LEN + 1])),
            Err(IndexOperationModelError::OversizedCursor {
                actual: INDEX_CURSOR_MAX_LEN + 1,
                maximum: INDEX_CURSOR_MAX_LEN,
            })
        );

        let invalid = [
            (4, 0, 3, 1, 1, "partition page count must be non-zero"),
            (4, 3, 0, 1, 1, "partition split count must be non-zero"),
            (
                4,
                3,
                3,
                0,
                1,
                "incomplete partition must have consumed at least page zero",
            ),
            (
                4,
                3,
                3,
                1,
                0,
                "an observed non-empty page must contribute a split",
            ),
            (4, 3, 3, 4, 3, "next page exceeds the root page count"),
            (
                4,
                3,
                3,
                3,
                3,
                "partition counts, next page, split total, or minimum root revision disagree",
            ),
            (
                4,
                3,
                3,
                2,
                1,
                "partition counts, next page, split total, or minimum root revision disagree",
            ),
            (
                4,
                3,
                2,
                1,
                3,
                "partition counts, next page, split total, or minimum root revision disagree",
            ),
            (
                4,
                3,
                2,
                1,
                1,
                "partition counts, next page, split total, or minimum root revision disagree",
            ),
            (
                3,
                3,
                3,
                1,
                1,
                "partition counts, next page, split total, or minimum root revision disagree",
            ),
        ];
        for (revision, pages, splits, next_page, observed, reason) in invalid {
            assert_eq!(
                TextManifestPartitionValidation::try_new(
                    [9; 32],
                    TextManifestRevision::new(revision).unwrap(),
                    pages,
                    splits,
                    next_page,
                    observed,
                ),
                Err(IndexOperationModelError::InvalidTextManifestValidationProgress(reason))
            );
        }

        let partition = TextManifestPartitionValidation::try_new(
            [9; 32],
            TextManifestRevision::new(4).unwrap(),
            3,
            4,
            2,
            2,
        )
        .unwrap();
        assert_eq!(partition.partition_fingerprint(), &[9; 32]);
        assert_eq!(partition.root_revision().get(), 4);
        assert_eq!(partition.page_count(), 3);
        assert_eq!(partition.split_count(), 4);
        assert_eq!(partition.next_page(), 2);
        assert_eq!(partition.observed_split_count(), 2);

        let counters = OperationCounters {
            entities: 1,
            input_bytes: 2,
            output_operations: 3,
            output_bytes: 4,
        };
        assert_eq!(
            TextManifestPageValidationProgress::try_new(None, Some(partition), counters),
            Err(
                IndexOperationModelError::InvalidTextManifestValidationProgress(
                    "an incomplete partition requires its last complete page cursor"
                )
            )
        );
        let pages =
            TextManifestPageValidationProgress::try_new(Some(cursor(8)), Some(partition), counters)
                .unwrap();
        assert_eq!(pages.cursor().unwrap().as_bytes().as_ref(), &[8]);
        assert!(pages.partition().is_some());
        assert_eq!(pages.counters(), counters);
        let initial = TextManifestPageValidationProgress::initial(counters);
        assert!(initial.cursor().is_none());
        assert!(initial.partition().is_none());

        for progress in [
            TextManifestValidationProgress::initial(counters),
            TextManifestValidationProgress::Roots(PrefixScanProgress {
                cursor: None,
                counters,
            }),
            TextManifestValidationProgress::EntityStates(PrefixScanProgress {
                cursor: None,
                counters,
            }),
        ] {
            assert_eq!(progress.counters(), counters);
        }
    }

    #[test]
    fn progress_variants_validate_and_map_only_their_encoded_cursors() {
        let pages = TextManifestPageValidationProgress::try_new(
            Some(cursor(4)),
            None,
            OperationCounters::default(),
        )
        .unwrap();
        let legacy = LegacyVectorValidationProgress {
            lane: LegacyVectorValidationLane::Hot,
            cursor: Some(cursor(5)),
            counters: OperationCounters::default(),
        };
        let directory = LegacyVectorDirectoryValidationProgress {
            cursor: Some(cursor(6)),
            expected_markers: 2,
            verified_markers: 1,
            counters: OperationCounters::default(),
        };
        let none = NoCursorProgress::default();
        let cases = vec![
            (secondary_constructing(), 2, true, false),
            (
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::CatchUp(prefix()),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::Validate(prefix()),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::Activate(none),
                )),
                0,
                true,
                false,
            ),
            (
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
                    SecondaryCleanupProgress::DeleteEntries(prefix()),
                )),
                1,
                false,
                true,
            ),
            (
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
                    SecondaryCleanupProgress::DeleteDeltas(prefix()),
                )),
                1,
                false,
                true,
            ),
            (
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
                    SecondaryCleanupProgress::Finalize(none),
                )),
                0,
                false,
                true,
            ),
            (
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteEntries(
                    prefix(),
                )),
                1,
                false,
                false,
            ),
            (
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteDeltas(
                    prefix(),
                )),
                1,
                false,
                false,
            ),
            (
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::Finalize(none)),
                0,
                false,
                false,
            ),
            (
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::AdoptLegacy(legacy.clone()),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::ValidateAdoptedDirectory(directory.clone()),
                )),
                1,
                true,
                false,
            ),
            (vector_constructing(), 2, true, false),
            (
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::CatchUp(prefix()),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::ValidateDescriptor(prefix()),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::Activate(none),
                )),
                0,
                true,
                false,
            ),
            (
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                    VectorCleanupProgress::RetireCache(none),
                )),
                0,
                false,
                true,
            ),
            (
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                    VectorCleanupProgress::DeletePhysical(prefix()),
                )),
                1,
                false,
                true,
            ),
            (
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                    VectorCleanupProgress::DeleteDeltas(prefix()),
                )),
                1,
                false,
                true,
            ),
            (
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                    VectorCleanupProgress::Finalize(none),
                )),
                0,
                false,
                true,
            ),
            (
                IndexOperationProgress::VectorCleanup(VectorCleanupProgress::RetireCache(none)),
                0,
                false,
                false,
            ),
            (
                IndexOperationProgress::VectorCleanup(VectorCleanupProgress::DeletePhysical(
                    prefix(),
                )),
                1,
                false,
                false,
            ),
            (
                IndexOperationProgress::VectorCleanup(
                    VectorCleanupProgress::DeleteDeltas(prefix()),
                ),
                1,
                false,
                false,
            ),
            (
                IndexOperationProgress::VectorCleanup(VectorCleanupProgress::Finalize(none)),
                0,
                false,
                false,
            ),
            (text_constructing(), 2, true, false),
            (
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ScanPartitions(source()),
                )),
                2,
                true,
                false,
            ),
            (
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::CatchUp(prefix()),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::Compact(prefix()),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::PrepareManifests(prefix()),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(TextManifestValidationProgress::Pages(pages)),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(TextManifestValidationProgress::Roots(
                        prefix(),
                    )),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(
                        TextManifestValidationProgress::EntityStates(prefix()),
                    ),
                )),
                1,
                true,
                false,
            ),
            (
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::Activate(none),
                )),
                0,
                true,
                false,
            ),
            (
                IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                    TextCleanupProgress::DeleteMetadata(prefix()),
                )),
                1,
                false,
                true,
            ),
            (
                IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                    TextCleanupProgress::Finalize(none),
                )),
                0,
                false,
                true,
            ),
            (
                IndexOperationProgress::TextCleanup(TextCleanupProgress::DeleteMetadata(prefix())),
                1,
                false,
                false,
            ),
            (
                IndexOperationProgress::TextCleanup(TextCleanupProgress::Finalize(none)),
                0,
                false,
                false,
            ),
        ];

        for (mut progress, expected_cursors, constructing, aborting) in cases {
            assert!(progress.cursors_are_valid(|_| true));
            assert_eq!(progress.is_constructing_build(), constructing);
            assert_eq!(progress.is_aborting_build(), aborting);
            let mut mapped = 0;
            progress
                .try_map_cursors(&mut |_| {
                    mapped += 1;
                    Ok::<_, ()>(cursor(99))
                })
                .unwrap();
            assert_eq!(mapped, expected_cursors);
        }

        let mut no_cursor = IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress::initial()),
        ));
        no_cursor
            .try_map_cursors(&mut |_| -> Result<_, ()> { unreachable!() })
            .unwrap();
        assert!(no_cursor.cursors_are_valid(|_| false));

        let invalid_directory = IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Constructing(VectorBuildStage::ValidateAdoptedDirectory(
                LegacyVectorDirectoryValidationProgress {
                    cursor: None,
                    expected_markers: 1,
                    verified_markers: 2,
                    counters: OperationCounters::default(),
                },
            )),
        );
        assert!(!invalid_directory.cursors_are_valid(|_| true));

        let mut failing = secondary_constructing();
        assert_eq!(
            failing.try_map_cursors(&mut |_| Err::<IndexCursor, _>("map failed")),
            Err("map failed")
        );
        assert!(!secondary_constructing().cursors_are_valid(|_| false));
    }

    #[test]
    fn claim_blocker_execution_and_schedule_contracts_cover_every_variant() {
        assert_eq!(
            ClaimSequence::new(0),
            Err(IndexOperationModelError::ZeroClaimSequence)
        );
        assert_eq!(ClaimSequence::new(9).unwrap().get(), 9);
        assert_eq!(
            LegacyVectorValidationProgress::initial().lane,
            LegacyVectorValidationLane::Core
        );
        assert_eq!(
            LegacyVectorValidationLane::Core.next(),
            Some(LegacyVectorValidationLane::Hot)
        );
        assert_eq!(
            LegacyVectorValidationLane::Hot.next(),
            Some(LegacyVectorValidationLane::Layer0)
        );
        assert_eq!(LegacyVectorValidationLane::Layer0.next(), None);
        assert_eq!(
            LegacyVectorDirectoryValidationProgress::initial(3, OperationCounters::default())
                .expected_markers,
            3
        );

        let size_blockers = [
            IndexOperationBlocker::OversizedEntity {
                entity_kind: IndexElementKind::Node,
                entity_id: IndexEntityId::new(1),
                observed: 4,
                limit: 4,
            },
            IndexOperationBlocker::ManifestLimit {
                partition: super::super::TextPartition::Unpartitioned,
                observed: 3,
                limit: 4,
            },
        ];
        for blocker in size_blockers {
            assert_eq!(
                blocker.validate(),
                Err(IndexOperationModelError::InvalidBlocker(
                    "observed size must exceed limit"
                ))
            );
        }
        for blocker in [
            IndexOperationBlocker::InvalidSourceData {
                entity_kind: IndexElementKind::Node,
                entity_id: IndexEntityId::new(1),
            },
            IndexOperationBlocker::UniquenessViolation {
                first_entity_id: IndexEntityId::new(1),
                second_entity_id: IndexEntityId::new(2),
            },
            IndexOperationBlocker::OversizedEntity {
                entity_kind: IndexElementKind::Edge,
                entity_id: IndexEntityId::new(3),
                observed: 5,
                limit: 4,
            },
            IndexOperationBlocker::ManifestLimit {
                partition: super::super::TextPartition::Unpartitioned,
                observed: 5,
                limit: 4,
            },
            IndexOperationBlocker::ObjectStoreConfigurationUnavailable,
            IndexOperationBlocker::InvariantViolation,
            IndexOperationBlocker::InvalidLegacyPhysical,
        ] {
            assert_eq!(blocker.validate(), Ok(()));
        }

        for (state, name) in [
            (
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
                "queued",
            ),
            (
                IndexOperationExecutionState::Claimed(claim(1, 1)),
                "claimed",
            ),
            (
                IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvariantViolation),
                "blocked",
            ),
            (
                IndexOperationExecutionState::Completed(IndexOperationOutcome::DropSucceeded),
                "completed",
            ),
        ] {
            assert_eq!(state.name(), name);
        }

        let first_writer = WriterEpoch::from_bytes([1; 16]).unwrap();
        let second_writer = WriterEpoch::from_bytes([2; 16]).unwrap();
        let immediate = IndexOperationQueueSchedule::Immediate;
        let progress = IndexOperationQueueSchedule::DelayedAfterProgress {
            not_before_unix_millis: 10,
        };
        let failure = IndexOperationQueueSchedule::DelayedAfterTransientFailure {
            not_before_unix_millis: 10,
            failed_writer_epoch: first_writer,
        };
        assert_eq!(immediate.not_before_unix_millis(), None);
        assert_eq!(progress.not_before_unix_millis(), Some(10));
        assert_eq!(failure.not_before_unix_millis(), Some(10));
        assert!(!immediate.transient_failure_from(first_writer));
        assert!(failure.transient_failure_from(first_writer));
        assert!(!failure.transient_failure_from(second_writer));
        assert!(immediate.is_eligible_for(first_writer, 0));
        assert!(!progress.is_eligible_for(first_writer, 9));
        assert!(progress.is_eligible_for(first_writer, 10));
        assert!(!failure.is_eligible_for(first_writer, 9));
        assert!(failure.is_eligible_for(first_writer, 10));
        assert!(failure.is_eligible_for(second_writer, 0));
    }

    #[test]
    fn record_constructor_rejects_every_cross_field_mismatch() {
        let progress = secondary_constructing();
        let base = |kind, family, identity_family, execution_state, schedule| {
            IndexOperationRecord::try_new_with_queue_schedule(
                IndexOperationId::from_bytes([7; 16]).unwrap(),
                IndexId::new(2).unwrap(),
                identity(identity_family),
                IndexGenerationId::new(3).unwrap(),
                IndexRevision::new(4).unwrap(),
                IndexOperationRevision::new(5).unwrap(),
                kind,
                family,
                progress.clone(),
                0,
                execution_state,
                schedule,
            )
        };
        let queued = IndexOperationExecutionState::Queued {
            not_before_unix_millis: None,
        };
        assert_eq!(
            base(
                IndexOperationKind::Build,
                IndexOperationFamily::Vector,
                IndexIdentityFamily::Vector,
                queued.clone(),
                Some(IndexOperationQueueSchedule::Immediate),
            ),
            Err(IndexOperationModelError::ProgressFamilyMismatch)
        );
        assert_eq!(
            base(
                IndexOperationKind::Drop,
                IndexOperationFamily::Secondary,
                IndexIdentityFamily::SecondaryEquality,
                queued.clone(),
                Some(IndexOperationQueueSchedule::Immediate),
            ),
            Err(IndexOperationModelError::ProgressKindMismatch)
        );
        assert_eq!(
            base(
                IndexOperationKind::Build,
                IndexOperationFamily::Secondary,
                IndexIdentityFamily::Text,
                queued.clone(),
                Some(IndexOperationQueueSchedule::Immediate),
            ),
            Err(IndexOperationModelError::IdentityFamilyMismatch)
        );
        assert_eq!(
            base(
                IndexOperationKind::Build,
                IndexOperationFamily::Secondary,
                IndexIdentityFamily::SecondaryRange,
                IndexOperationExecutionState::Completed(IndexOperationOutcome::DropSucceeded),
                None,
            ),
            Err(IndexOperationModelError::CompletionKindMismatch)
        );
        assert_eq!(
            base(
                IndexOperationKind::Build,
                IndexOperationFamily::Secondary,
                IndexIdentityFamily::SecondaryEquality,
                IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                    BuildOperationOutcome::Aborted,
                )),
                None,
            ),
            Err(IndexOperationModelError::CompletionProgressMismatch)
        );
        assert_eq!(
            base(
                IndexOperationKind::Build,
                IndexOperationFamily::Secondary,
                IndexIdentityFamily::SecondaryEquality,
                IndexOperationExecutionState::Blocked(IndexOperationBlocker::OversizedEntity {
                    entity_kind: IndexElementKind::Node,
                    entity_id: IndexEntityId::new(1),
                    observed: 1,
                    limit: 1,
                }),
                None,
            ),
            Err(IndexOperationModelError::InvalidBlocker(
                "observed size must exceed limit"
            ))
        );
        assert_eq!(
            base(
                IndexOperationKind::Build,
                IndexOperationFamily::Secondary,
                IndexIdentityFamily::SecondaryEquality,
                queued.clone(),
                None,
            ),
            Err(IndexOperationModelError::InvalidQueueSchedule(
                "queued operation has no schedule"
            ))
        );
        assert_eq!(
            base(
                IndexOperationKind::Build,
                IndexOperationFamily::Secondary,
                IndexIdentityFamily::SecondaryEquality,
                queued,
                Some(IndexOperationQueueSchedule::DelayedAfterProgress {
                    not_before_unix_millis: 1,
                }),
            ),
            Err(IndexOperationModelError::InvalidQueueSchedule(
                "queued deadline disagrees with schedule"
            ))
        );
        assert_eq!(
            base(
                IndexOperationKind::Build,
                IndexOperationFamily::Secondary,
                IndexIdentityFamily::SecondaryEquality,
                IndexOperationExecutionState::Claimed(claim(1, 1)),
                Some(IndexOperationQueueSchedule::Immediate),
            ),
            Err(IndexOperationModelError::InvalidQueueSchedule(
                "non-queued operation has a schedule"
            ))
        );

        let aborting = IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
            SecondaryCleanupProgress::Finalize(NoCursorProgress::default()),
        ));
        let completed_abort = record(
            aborting,
            IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Aborted,
            )),
        );
        assert!(matches!(
            completed_abort.execution_state(),
            IndexOperationExecutionState::Completed(_)
        ));
        let completed_drop = record(
            IndexOperationProgress::TextCleanup(TextCleanupProgress::Finalize(
                NoCursorProgress::default(),
            )),
            IndexOperationExecutionState::Completed(IndexOperationOutcome::DropSucceeded),
        );
        assert_eq!(completed_drop.kind(), IndexOperationKind::Drop);
    }

    #[test]
    fn operation_transitions_preserve_contracts_and_reject_wrong_states() {
        let queued = record(
            secondary_constructing(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: Some(10),
            },
        );
        assert_eq!(queued.operation_id().as_bytes(), &[7; 16]);
        assert_eq!(queued.index_id().get(), 2);
        assert_eq!(
            queued.identity().family(),
            IndexIdentityFamily::SecondaryEquality
        );
        assert_eq!(queued.generation().get(), 3);
        assert_eq!(queued.index_record_revision().get(), 4);
        assert_eq!(queued.operation_revision().get(), 5);
        assert_eq!(queued.family(), IndexOperationFamily::Secondary);
        assert!(queued.progress().is_constructing_build());
        assert_eq!(queued.attempt(), 6);
        assert_eq!(
            queued.queue_schedule().unwrap().not_before_unix_millis(),
            Some(10)
        );
        let mut mapped_calls = 0;
        let mapped = queued
            .try_map_cursors(|_| {
                mapped_calls += 1;
                Ok::<_, ()>(cursor(42))
            })
            .unwrap();
        assert_eq!(mapped_calls, 2);
        assert_eq!(mapped.operation_revision(), queued.operation_revision());

        let claimed = queued.claim(claim(1, 1)).unwrap();
        assert_eq!(claimed.attempt(), 7);
        assert_eq!(claimed.operation_revision().get(), 6);
        let replaced = claimed.claim(claim(2, 2)).unwrap();
        assert_eq!(replaced.attempt(), 8);

        let progressed = claimed.progressed(secondary_constructing()).unwrap();
        assert!(matches!(
            progressed.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None
            }
        ));
        let progressed_after = claimed
            .progressed_after(secondary_constructing(), 20)
            .unwrap();
        assert_eq!(
            progressed_after.queue_schedule(),
            Some(IndexOperationQueueSchedule::DelayedAfterProgress {
                not_before_unix_millis: 20
            })
        );
        let failed = claimed.transient_failure(30).unwrap();
        assert!(failed
            .queue_schedule()
            .unwrap()
            .transient_failure_from(WriterEpoch::from_bytes([1; 16]).unwrap()));

        let blocked = claimed
            .block(IndexOperationBlocker::InvariantViolation)
            .unwrap();
        assert!(matches!(
            blocked.execution_state(),
            IndexOperationExecutionState::Blocked(_)
        ));
        assert!(matches!(
            blocked.retry().unwrap().execution_state(),
            IndexOperationExecutionState::Queued { .. }
        ));
        let completed = claimed
            .complete(
                IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
                IndexRevision::new(8).unwrap(),
            )
            .unwrap();
        assert_eq!(completed.index_record_revision().get(), 8);

        for transition in [
            queued.progressed(secondary_constructing()),
            queued.progressed_after(secondary_constructing(), 1),
            queued.transient_failure(1),
            queued.block(IndexOperationBlocker::InvariantViolation),
            queued.complete(
                IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
                IndexRevision::new(9).unwrap(),
            ),
            queued.retry(),
        ] {
            assert!(matches!(
                transition,
                Err(IndexOperationModelError::IllegalExecutionTransition { .. })
            ));
        }
        assert!(matches!(
            blocked.claim(claim(1, 2)),
            Err(IndexOperationModelError::IllegalExecutionTransition { .. })
        ));

        for progress in [
            secondary_constructing(),
            vector_constructing(),
            text_constructing(),
        ] {
            let aborted = record(
                progress,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .begin_abort(IndexRevision::new(10).unwrap())
            .unwrap();
            assert!(aborted.progress().is_aborting_build());
            assert_eq!(aborted.index_record_revision().get(), 10);
        }
        let already_aborting = record(
            IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                TextCleanupProgress::Finalize(NoCursorProgress::default()),
            )),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        );
        assert!(matches!(
            already_aborting.begin_abort(IndexRevision::new(10).unwrap()),
            Err(IndexOperationModelError::IllegalExecutionTransition { .. })
        ));
        assert!(matches!(
            completed.begin_abort(IndexRevision::new(10).unwrap()),
            Err(IndexOperationModelError::IllegalExecutionTransition { .. })
        ));

        let vector_completed = record(
            vector_constructing(),
            IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Succeeded,
            )),
        );
        assert_eq!(
            vector_completed
                .rebind_completed_index_revision(IndexRevision::new(11).unwrap())
                .unwrap()
                .index_record_revision()
                .get(),
            11
        );
        let queued_vector = record(
            vector_constructing(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        );
        assert!(matches!(
            queued_vector.rebind_completed_index_revision(IndexRevision::new(11).unwrap()),
            Err(IndexOperationModelError::IllegalExecutionTransition { .. })
        ));
        assert!(matches!(
            completed.rebind_completed_index_revision(IndexRevision::new(11).unwrap()),
            Err(IndexOperationModelError::IllegalExecutionTransition { .. })
        ));

        let saturated = IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([7; 16]).unwrap(),
            IndexId::new(2).unwrap(),
            identity(IndexIdentityFamily::SecondaryEquality),
            IndexGenerationId::new(3).unwrap(),
            IndexRevision::new(4).unwrap(),
            IndexOperationRevision::new(u64::MAX).unwrap(),
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            secondary_constructing(),
            u32::MAX,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        assert!(matches!(
            saturated.claim(claim(1, 1)),
            Err(IndexOperationModelError::InvalidBlocker(
                "nested V2 model validation failed"
            ))
        ));
        assert_eq!(
            IndexOperationModelError::from(IndexId::new(0).unwrap_err()),
            IndexOperationModelError::InvalidBlocker("nested V2 model validation failed")
        );
    }
}
