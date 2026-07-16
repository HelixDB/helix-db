//! Stable public receipts and operation-status projections.
//!
//! Durable operation records contain worker claims, retry deadlines, and raw
//! resume cursors. This module is the one-way contract boundary that projects
//! those records into the deliberately smaller JSON/SDK lifecycle API.
//!
//! ```
//! use db::index_v2::{IndexDdlReceipt, IndexGenerationId, IndexId, IndexOperationId};
//!
//! let receipt = IndexDdlReceipt::Accepted {
//!     operation_id: IndexOperationId::from_bytes([7; 16]).unwrap(),
//!     index_id: IndexId::new(42).unwrap(),
//!     generation: IndexGenerationId::new(3).unwrap(),
//! };
//! let json = serde_json::to_value(receipt).unwrap();
//! assert_eq!(json["kind"], "accepted");
//! assert_eq!(json["index_id"], "42");
//! assert_eq!(json["generation"], "3");
//! ```

use serde::{Serialize, Serializer};

use super::{
    BuildOperationOutcome, IndexGenerationId, IndexId, IndexOperationBlocker,
    IndexOperationExecutionState, IndexOperationFamily, IndexOperationId, IndexOperationKind,
    IndexOperationOutcome, IndexOperationProgress, IndexOperationRecord, NoCursorProgress,
    OperationCounters, PrefixScanProgress, SecondaryBuildProgress, SecondaryBuildStage,
    SecondaryCleanupProgress, SourceScanProgress, TextBuildProgress, TextBuildStage,
    TextCleanupProgress, VectorBuildProgress, VectorBuildStage, VectorCleanupProgress,
};

/// Result of a CREATE or DROP lifecycle request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IndexDdlReceipt {
    /// A new durable operation was atomically accepted.
    Accepted {
        /// Stable operation identity used by status/control APIs.
        #[serde(with = "operation_id_string")]
        operation_id: IndexOperationId,
        /// Logical index identity.
        #[serde(with = "index_id_string")]
        index_id: IndexId,
        /// Physical generation affected by the operation.
        #[serde(with = "generation_string")]
        generation: IndexGenerationId,
    },
    /// The request converged on an already-running operation.
    ExistingOperation {
        /// Stable operation identity used by status/control APIs.
        #[serde(with = "operation_id_string")]
        operation_id: IndexOperationId,
    },
    /// `IF NOT EXISTS` converged on an identical active index.
    AlreadyActive {
        /// Logical index identity.
        #[serde(with = "index_id_string")]
        index_id: IndexId,
        /// Active physical generation.
        #[serde(with = "generation_string")]
        generation: IndexGenerationId,
    },
}

/// Public BUILD/DROP operation kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicIndexOperationKind {
    /// Build a hidden generation and activate it.
    Build,
    /// Drain and remove an active generation.
    Drop,
}

/// Public physical family name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicIndexFamily {
    /// Equality/range secondary indexes.
    Secondary,
    /// Vector indexes.
    Vector,
    /// Text indexes.
    Text,
}

/// Stable blocker code exposed to clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexOperationBlockerCode {
    /// Authoritative source data cannot satisfy the family contract.
    InvalidSourceData,
    /// A unique index found conflicting entities.
    UniquenessViolation,
    /// One entity exceeds a configured bounded-step limit.
    OversizedEntity,
    /// A text manifest exceeds its configured bound.
    ManifestLimit,
    /// Generation reader coordination is unavailable.
    ReaderCoordinationUnavailable,
    /// Text object-store configuration is unavailable.
    ObjectStoreConfigurationUnavailable,
    /// Blob publication coordination is unavailable.
    BlobPublicationCoordinationUnavailable,
    /// An internal lifecycle invariant could not be proven.
    InvariantViolation,
}

/// Monotonic bounded-work counters safe for public progress reporting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct IndexOperationPublicProgress {
    /// Authoritative entities visited.
    #[serde(with = "u64_string")]
    pub entities: u64,
    /// Source bytes consumed.
    #[serde(with = "u64_string")]
    pub input_bytes: u64,
    /// Physical operations staged.
    #[serde(with = "u64_string")]
    pub output_operations: u64,
    /// Physical output bytes staged.
    #[serde(with = "u64_string")]
    pub output_bytes: u64,
}

/// Stable public lifecycle stage serialized in snake case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexOperationStage {
    /// Scan authoritative source entities.
    Scan,
    /// Scan staged text entities in partition order.
    ScanPartitions,
    /// Wait for one exact text build upload intent to attach or retry.
    AwaitUpload,
    /// Apply mutation deltas captured during the scan.
    CatchUp,
    /// Validate secondary ownership and uniqueness.
    Validate,
    /// Validate vector physical metadata against the descriptor.
    ValidateDescriptor,
    /// Compact text split state.
    Compact,
    /// Construct bounded text manifest pages.
    PrepareManifests,
    /// Validate manifest topology, blob publication, and remaining build ownership.
    ValidateManifests,
    /// Publish the hidden generation as active.
    Activate,
    /// Begin reader drain for ordinary DROP cleanup.
    BeginDrain,
    /// Delete secondary generation entries.
    DeleteEntries,
    /// Retire a vector generation from memory.
    RetireCache,
    /// Delete vector physical rows.
    DeletePhysical,
    /// Delete retained mutation deltas.
    DeleteDeltas,
    /// Materialize bounded text blob candidates.
    PrepareCandidates,
    /// Acquire text blob deletion fences.
    AcquireDeleteFences,
    /// Retire text manifests.
    RetireManifest,
    /// Retire text generation artifacts.
    RetireArtifacts,
    /// Retire text upload intents.
    RetireUploadIntents,
    /// Prove global blob reachability.
    MarkReachability,
    /// Delete globally unreachable text blobs.
    DeleteBlobs,
    /// Delete text entity live state.
    DeleteEntityState,
    /// Finish ordinary DROP reader drain.
    FinishDrain,
    /// Finalize ordinary DROP cleanup.
    Finalize,
    /// Begin reader drain for BUILD abort cleanup.
    AbortingBeginDrain,
    /// Delete secondary entries during BUILD abort cleanup.
    AbortingDeleteEntries,
    /// Retire vector memory during BUILD abort cleanup.
    AbortingRetireCache,
    /// Delete vector rows during BUILD abort cleanup.
    AbortingDeletePhysical,
    /// Delete retained deltas during BUILD abort cleanup.
    AbortingDeleteDeltas,
    /// Prepare text candidates during BUILD abort cleanup.
    AbortingPrepareCandidates,
    /// Acquire text deletion fences during BUILD abort cleanup.
    AbortingAcquireDeleteFences,
    /// Retire text manifests during BUILD abort cleanup.
    AbortingRetireManifest,
    /// Retire text artifacts during BUILD abort cleanup.
    AbortingRetireArtifacts,
    /// Retire text upload intents during BUILD abort cleanup.
    AbortingRetireUploadIntents,
    /// Prove text reachability during BUILD abort cleanup.
    AbortingMarkReachability,
    /// Delete text blobs during BUILD abort cleanup.
    AbortingDeleteBlobs,
    /// Delete text entity state during BUILD abort cleanup.
    AbortingDeleteEntityState,
    /// Finish reader drain during BUILD abort cleanup.
    AbortingFinishDrain,
    /// Finalize BUILD abort cleanup.
    AbortingFinalize,
}

/// Fields shared by every public operation-status variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexOperationStatusCommon {
    /// Stable operation identity.
    #[serde(with = "operation_id_string")]
    pub operation_id: IndexOperationId,
    /// Logical index identity.
    #[serde(with = "index_id_string")]
    pub index_id: IndexId,
    /// Physical generation affected by the operation.
    #[serde(with = "generation_string")]
    pub generation: IndexGenerationId,
    /// BUILD or DROP.
    pub operation_kind: PublicIndexOperationKind,
    /// Physical family.
    pub family: PublicIndexFamily,
    /// Frozen family stage serialized in snake case.
    pub stage: IndexOperationStage,
    /// Number of claims attempted.
    pub attempt: u32,
    /// Monotonic bounded-work progress.
    pub progress: IndexOperationPublicProgress,
}

/// Public operation status serialized at the JSON/SDK boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IndexOperationStatus {
    /// Runnable, including delayed retry work.
    Queued {
        /// Common operation fields.
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
    /// Claimed by a fenced worker.
    Running {
        /// Common operation fields.
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
    /// Requires an explicit retry or abort command.
    Blocked {
        /// Common operation fields.
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
        /// Stable machine-readable blocker.
        blocker_code: IndexOperationBlockerCode,
        /// Optional non-contractual diagnostic.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    /// Build or drop succeeded.
    Succeeded {
        /// Common operation fields.
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
    /// A build was explicitly aborted and cleaned up.
    Aborted {
        /// Common operation fields.
        #[serde(flatten)]
        common: IndexOperationStatusCommon,
    },
}

impl IndexOperationStatus {
    /// Projects a durable record without exposing claims, deadlines, or cursors.
    pub fn from_record(record: &IndexOperationRecord) -> Self {
        let (stage, counters) = public_progress(record.progress());
        let common = IndexOperationStatusCommon {
            operation_id: record.operation_id(),
            index_id: record.index_id(),
            generation: record.generation(),
            operation_kind: match record.kind() {
                IndexOperationKind::Build => PublicIndexOperationKind::Build,
                IndexOperationKind::Drop => PublicIndexOperationKind::Drop,
            },
            family: match record.family() {
                IndexOperationFamily::Secondary => PublicIndexFamily::Secondary,
                IndexOperationFamily::Vector => PublicIndexFamily::Vector,
                IndexOperationFamily::Text => PublicIndexFamily::Text,
            },
            stage,
            attempt: record.attempt(),
            progress: counters.into(),
        };
        match record.execution_state() {
            IndexOperationExecutionState::Queued { .. } => Self::Queued { common },
            IndexOperationExecutionState::Claimed(_) => Self::Running { common },
            IndexOperationExecutionState::Blocked(blocker) => Self::Blocked {
                common,
                blocker_code: blocker.into(),
                message: None,
            },
            IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Aborted,
            )) => Self::Aborted { common },
            IndexOperationExecutionState::Completed(
                IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded)
                | IndexOperationOutcome::DropSucceeded,
            ) => Self::Succeeded { common },
        }
    }

    /// Borrows fields present in every status variant.
    pub const fn common(&self) -> &IndexOperationStatusCommon {
        match self {
            Self::Queued { common }
            | Self::Running { common }
            | Self::Blocked { common, .. }
            | Self::Succeeded { common }
            | Self::Aborted { common } => common,
        }
    }
}

impl From<OperationCounters> for IndexOperationPublicProgress {
    fn from(value: OperationCounters) -> Self {
        Self {
            entities: value.entities,
            input_bytes: value.input_bytes,
            output_operations: value.output_operations,
            output_bytes: value.output_bytes,
        }
    }
}

impl From<&IndexOperationBlocker> for IndexOperationBlockerCode {
    fn from(value: &IndexOperationBlocker) -> Self {
        match value {
            IndexOperationBlocker::InvalidSourceData { .. } => Self::InvalidSourceData,
            IndexOperationBlocker::UniquenessViolation { .. } => Self::UniquenessViolation,
            IndexOperationBlocker::OversizedEntity { .. } => Self::OversizedEntity,
            IndexOperationBlocker::ManifestLimit { .. } => Self::ManifestLimit,
            IndexOperationBlocker::ReaderCoordinationUnavailable => {
                Self::ReaderCoordinationUnavailable
            }
            IndexOperationBlocker::ObjectStoreConfigurationUnavailable => {
                Self::ObjectStoreConfigurationUnavailable
            }
            IndexOperationBlocker::BlobPublicationCoordinationUnavailable => {
                Self::BlobPublicationCoordinationUnavailable
            }
            IndexOperationBlocker::InvariantViolation
            | IndexOperationBlocker::BlobPublicationMismatch { .. } => Self::InvariantViolation,
        }
    }
}

fn public_progress(progress: &IndexOperationProgress) -> (IndexOperationStage, OperationCounters) {
    match progress {
        IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(stage)) => {
            match stage {
                SecondaryBuildStage::Scan(value) => {
                    (IndexOperationStage::Scan, source_counters(value))
                }
                SecondaryBuildStage::CatchUp(value) => {
                    (IndexOperationStage::CatchUp, prefix_counters(value))
                }
                SecondaryBuildStage::Validate(value) => {
                    (IndexOperationStage::Validate, prefix_counters(value))
                }
                SecondaryBuildStage::Activate(value) => {
                    (IndexOperationStage::Activate, no_cursor_counters(value))
                }
            }
        }
        IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(stage)) => {
            secondary_cleanup_progress(stage, CleanupProjection::Abort)
        }
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(stage)) => {
            match stage {
                VectorBuildStage::Scan(value) => {
                    (IndexOperationStage::Scan, source_counters(value))
                }
                VectorBuildStage::CatchUp(value) => {
                    (IndexOperationStage::CatchUp, prefix_counters(value))
                }
                VectorBuildStage::ValidateDescriptor(value) => (
                    IndexOperationStage::ValidateDescriptor,
                    prefix_counters(value),
                ),
                VectorBuildStage::Activate(value) => {
                    (IndexOperationStage::Activate, no_cursor_counters(value))
                }
            }
        }
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(stage)) => {
            vector_cleanup_progress(stage, CleanupProjection::Abort)
        }
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(stage)) => match stage {
            TextBuildStage::ScanSource(value) => {
                (IndexOperationStage::Scan, source_counters(value))
            }
            TextBuildStage::ScanPartitions(value) => {
                (IndexOperationStage::ScanPartitions, source_counters(value))
            }
            TextBuildStage::AwaitUpload(value) => {
                (IndexOperationStage::AwaitUpload, value.completed_counters())
            }
            TextBuildStage::AwaitCatchUpUpload(value) => {
                (IndexOperationStage::AwaitUpload, value.completed_counters())
            }
            TextBuildStage::AwaitCompactionUpload(value) => {
                (IndexOperationStage::AwaitUpload, value.completed_counters())
            }
            TextBuildStage::CatchUp(value) => {
                (IndexOperationStage::CatchUp, prefix_counters(value))
            }
            TextBuildStage::Compact(value) => {
                (IndexOperationStage::Compact, prefix_counters(value))
            }
            TextBuildStage::PrepareManifests(value) => (
                IndexOperationStage::PrepareManifests,
                prefix_counters(value),
            ),
            TextBuildStage::ValidateManifests(value) => {
                (IndexOperationStage::ValidateManifests, value.counters())
            }
            TextBuildStage::Activate(value) => {
                (IndexOperationStage::Activate, no_cursor_counters(value))
            }
        },
        IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(stage)) => {
            text_cleanup_progress(stage, CleanupProjection::Abort)
        }
        IndexOperationProgress::SecondaryCleanup(stage) => {
            secondary_cleanup_progress(stage, CleanupProjection::Drop)
        }
        IndexOperationProgress::VectorCleanup(stage) => {
            vector_cleanup_progress(stage, CleanupProjection::Drop)
        }
        IndexOperationProgress::TextCleanup(stage) => {
            text_cleanup_progress(stage, CleanupProjection::Drop)
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CleanupProjection {
    Drop,
    Abort,
}

fn secondary_cleanup_progress(
    stage: &SecondaryCleanupProgress,
    projection: CleanupProjection,
) -> (IndexOperationStage, OperationCounters) {
    match (projection, stage) {
        (CleanupProjection::Drop, SecondaryCleanupProgress::BeginDrain(value)) => {
            (IndexOperationStage::BeginDrain, value.counters)
        }
        (CleanupProjection::Drop, SecondaryCleanupProgress::DeleteEntries(value)) => {
            (IndexOperationStage::DeleteEntries, value.counters)
        }
        (CleanupProjection::Drop, SecondaryCleanupProgress::DeleteDeltas(value)) => {
            (IndexOperationStage::DeleteDeltas, value.counters)
        }
        (CleanupProjection::Drop, SecondaryCleanupProgress::FinishDrain(value)) => {
            (IndexOperationStage::FinishDrain, value.counters)
        }
        (CleanupProjection::Drop, SecondaryCleanupProgress::Finalize(value)) => {
            (IndexOperationStage::Finalize, value.counters)
        }
        (CleanupProjection::Abort, SecondaryCleanupProgress::BeginDrain(value)) => {
            (IndexOperationStage::AbortingBeginDrain, value.counters)
        }
        (CleanupProjection::Abort, SecondaryCleanupProgress::DeleteEntries(value)) => {
            (IndexOperationStage::AbortingDeleteEntries, value.counters)
        }
        (CleanupProjection::Abort, SecondaryCleanupProgress::DeleteDeltas(value)) => {
            (IndexOperationStage::AbortingDeleteDeltas, value.counters)
        }
        (CleanupProjection::Abort, SecondaryCleanupProgress::FinishDrain(value)) => {
            (IndexOperationStage::AbortingFinishDrain, value.counters)
        }
        (CleanupProjection::Abort, SecondaryCleanupProgress::Finalize(value)) => {
            (IndexOperationStage::AbortingFinalize, value.counters)
        }
    }
}

fn vector_cleanup_progress(
    stage: &VectorCleanupProgress,
    projection: CleanupProjection,
) -> (IndexOperationStage, OperationCounters) {
    match (projection, stage) {
        (CleanupProjection::Drop, VectorCleanupProgress::BeginDrain(value)) => {
            (IndexOperationStage::BeginDrain, value.counters)
        }
        (CleanupProjection::Drop, VectorCleanupProgress::RetireCache(value)) => {
            (IndexOperationStage::RetireCache, value.counters)
        }
        (CleanupProjection::Drop, VectorCleanupProgress::DeletePhysical(value)) => {
            (IndexOperationStage::DeletePhysical, value.counters)
        }
        (CleanupProjection::Drop, VectorCleanupProgress::DeleteDeltas(value)) => {
            (IndexOperationStage::DeleteDeltas, value.counters)
        }
        (CleanupProjection::Drop, VectorCleanupProgress::FinishDrain(value)) => {
            (IndexOperationStage::FinishDrain, value.counters)
        }
        (CleanupProjection::Drop, VectorCleanupProgress::Finalize(value)) => {
            (IndexOperationStage::Finalize, value.counters)
        }
        (CleanupProjection::Abort, VectorCleanupProgress::BeginDrain(value)) => {
            (IndexOperationStage::AbortingBeginDrain, value.counters)
        }
        (CleanupProjection::Abort, VectorCleanupProgress::RetireCache(value)) => {
            (IndexOperationStage::AbortingRetireCache, value.counters)
        }
        (CleanupProjection::Abort, VectorCleanupProgress::DeletePhysical(value)) => {
            (IndexOperationStage::AbortingDeletePhysical, value.counters)
        }
        (CleanupProjection::Abort, VectorCleanupProgress::DeleteDeltas(value)) => {
            (IndexOperationStage::AbortingDeleteDeltas, value.counters)
        }
        (CleanupProjection::Abort, VectorCleanupProgress::FinishDrain(value)) => {
            (IndexOperationStage::AbortingFinishDrain, value.counters)
        }
        (CleanupProjection::Abort, VectorCleanupProgress::Finalize(value)) => {
            (IndexOperationStage::AbortingFinalize, value.counters)
        }
    }
}

fn text_cleanup_progress(
    stage: &TextCleanupProgress,
    projection: CleanupProjection,
) -> (IndexOperationStage, OperationCounters) {
    match (projection, stage) {
        (CleanupProjection::Drop, TextCleanupProgress::BeginDrain(value)) => {
            (IndexOperationStage::BeginDrain, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::PrepareCandidates(value)) => {
            (IndexOperationStage::PrepareCandidates, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::AcquireDeleteFences(value)) => {
            (IndexOperationStage::AcquireDeleteFences, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::RetireManifest(value)) => {
            (IndexOperationStage::RetireManifest, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::RetireArtifacts(value)) => {
            (IndexOperationStage::RetireArtifacts, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::RetireUploadIntents(value)) => {
            (IndexOperationStage::RetireUploadIntents, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::MarkReachability(value)) => {
            (IndexOperationStage::MarkReachability, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::DeleteBlobs(value)) => {
            (IndexOperationStage::DeleteBlobs, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::DeleteEntityState(value)) => {
            (IndexOperationStage::DeleteEntityState, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::FinishDrain(value)) => {
            (IndexOperationStage::FinishDrain, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::Finalize(value)) => {
            (IndexOperationStage::Finalize, value.counters)
        }
        (CleanupProjection::Abort, TextCleanupProgress::BeginDrain(value)) => {
            (IndexOperationStage::AbortingBeginDrain, value.counters)
        }
        (CleanupProjection::Abort, TextCleanupProgress::PrepareCandidates(value)) => (
            IndexOperationStage::AbortingPrepareCandidates,
            value.counters,
        ),
        (CleanupProjection::Abort, TextCleanupProgress::AcquireDeleteFences(value)) => (
            IndexOperationStage::AbortingAcquireDeleteFences,
            value.counters,
        ),
        (CleanupProjection::Abort, TextCleanupProgress::RetireManifest(value)) => {
            (IndexOperationStage::AbortingRetireManifest, value.counters)
        }
        (CleanupProjection::Abort, TextCleanupProgress::RetireArtifacts(value)) => {
            (IndexOperationStage::AbortingRetireArtifacts, value.counters)
        }
        (CleanupProjection::Abort, TextCleanupProgress::RetireUploadIntents(value)) => (
            IndexOperationStage::AbortingRetireUploadIntents,
            value.counters,
        ),
        (CleanupProjection::Abort, TextCleanupProgress::MarkReachability(value)) => (
            IndexOperationStage::AbortingMarkReachability,
            value.counters,
        ),
        (CleanupProjection::Abort, TextCleanupProgress::DeleteBlobs(value)) => {
            (IndexOperationStage::AbortingDeleteBlobs, value.counters)
        }
        (CleanupProjection::Abort, TextCleanupProgress::DeleteEntityState(value)) => (
            IndexOperationStage::AbortingDeleteEntityState,
            value.counters,
        ),
        (CleanupProjection::Abort, TextCleanupProgress::FinishDrain(value)) => {
            (IndexOperationStage::AbortingFinishDrain, value.counters)
        }
        (CleanupProjection::Abort, TextCleanupProgress::Finalize(value)) => {
            (IndexOperationStage::AbortingFinalize, value.counters)
        }
    }
}

const fn source_counters(progress: &SourceScanProgress) -> OperationCounters {
    progress.counters
}

const fn prefix_counters(progress: &PrefixScanProgress) -> OperationCounters {
    progress.counters
}

const fn no_cursor_counters(progress: &NoCursorProgress) -> OperationCounters {
    progress.counters
}

mod operation_id_string {
    use super::*;

    pub(super) fn serialize<S>(value: &IndexOperationId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.as_uuid().to_string())
    }
}

macro_rules! decimal_id_serde {
    ($module:ident, $ty:ty) => {
        mod $module {
            use super::*;

            pub(super) fn serialize<S>(value: &$ty, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&value.get().to_string())
            }
        }
    };
}

decimal_id_serde!(index_id_string, IndexId);
decimal_id_serde!(generation_string, IndexGenerationId);

mod u64_string {
    use super::*;

    pub(super) fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::index_v2::{
        ClaimSequence, DrainProgress, GcProgress, IndexComponent, IndexCursor, IndexElementKind,
        IndexEntityId, IndexIdentity, IndexIdentityFamily, IndexOperationRevision, IndexRevision,
        OperationClaim, TextBuildUploadProgress, TextCatchUpUploadProgress,
        TextCompactionUploadProgress, TextManifestValidationProgress, TextPartition, WriterEpoch,
    };

    fn family_record(
        identity_family: IndexIdentityFamily,
        kind: IndexOperationKind,
        family: IndexOperationFamily,
        progress: IndexOperationProgress,
        state: IndexOperationExecutionState,
    ) -> IndexOperationRecord {
        IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([7; 16]).unwrap(),
            IndexId::new(42).unwrap(),
            IndexIdentity::new(
                identity_family,
                IndexElementKind::Node,
                IndexComponent::try_new("label", "User").unwrap(),
                IndexComponent::try_new("property", "email").unwrap(),
            ),
            IndexGenerationId::new(3).unwrap(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            kind,
            family,
            progress,
            2,
            state,
        )
        .unwrap()
    }

    fn record(state: IndexOperationExecutionState) -> IndexOperationRecord {
        family_record(
            IndexIdentityFamily::SecondaryEquality,
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Scan(SourceScanProgress {
                    inclusive_upper_bound: IndexCursor::try_new(Bytes::from_static(b"upper"))
                        .unwrap(),
                    cursor: None,
                    counters: OperationCounters {
                        entities: 9,
                        input_bytes: 10,
                        output_operations: 11,
                        output_bytes: 12,
                    },
                }),
            )),
            state,
        )
    }

    fn aborted_record() -> IndexOperationRecord {
        family_record(
            IndexIdentityFamily::SecondaryEquality,
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
                SecondaryCleanupProgress::Finalize(NoCursorProgress::default()),
            )),
            IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Aborted,
            )),
        )
    }

    #[test]
    fn public_status_hides_internal_claim_and_uses_frozen_wire_scalars() {
        let status =
            IndexOperationStatus::from_record(&record(IndexOperationExecutionState::Queued {
                not_before_unix_millis: Some(u64::MAX),
            }));
        assert_eq!(status.common().stage, IndexOperationStage::Scan);
        let json = serde_json::to_value(status).unwrap();

        assert_eq!(json["status"], "queued");
        assert_eq!(json["operation_id"], "07070707-0707-0707-0707-070707070707");
        assert_eq!(json["index_id"], "42");
        assert_eq!(json["generation"], "3");
        assert_eq!(json["progress"]["entities"], "9");
        assert!(json.get("not_before_unix_millis").is_none());
    }

    #[test]
    fn running_blocked_and_aborted_statuses_use_closed_public_tags() {
        let running_status = IndexOperationStatus::from_record(&record(
            IndexOperationExecutionState::Claimed(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([8; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            }),
        ));
        assert_eq!(running_status.common().attempt, 2);
        let running = serde_json::to_value(running_status).unwrap();
        assert_eq!(running["status"], "running");
        assert!(running.get("writer_epoch").is_none());

        let blocked_status = IndexOperationStatus::from_record(&record(
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::UniquenessViolation {
                first_entity_id: IndexEntityId::new(1),
                second_entity_id: IndexEntityId::new(2),
            }),
        ));
        assert_eq!(blocked_status.common().attempt, 2);
        let blocked = serde_json::to_value(blocked_status).unwrap();
        assert_eq!(blocked["status"], "blocked");
        assert_eq!(blocked["blocker_code"], "uniqueness_violation");
        assert!(blocked.get("message").is_none());

        let aborted_status = IndexOperationStatus::from_record(&aborted_record());
        assert_eq!(aborted_status.common().attempt, 2);
        let aborted = serde_json::to_value(aborted_status).unwrap();
        assert_eq!(aborted["status"], "aborted");
        assert_eq!(aborted["stage"], "aborting_finalize");
    }

    #[test]
    fn succeeded_statuses_cover_build_drop_and_every_family() {
        let vector_build = family_record(
            IndexIdentityFamily::Vector,
            IndexOperationKind::Build,
            IndexOperationFamily::Vector,
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::Activate(NoCursorProgress::default()),
            )),
            IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Succeeded,
            )),
        );
        let vector_status = IndexOperationStatus::from_record(&vector_build);
        assert_eq!(
            vector_status.common().operation_kind,
            PublicIndexOperationKind::Build
        );
        assert_eq!(vector_status.common().family, PublicIndexFamily::Vector);
        assert_eq!(
            serde_json::to_value(vector_status).unwrap()["status"],
            "succeeded"
        );

        let text_drop = family_record(
            IndexIdentityFamily::Text,
            IndexOperationKind::Drop,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextCleanup(TextCleanupProgress::Finalize(
                NoCursorProgress::default(),
            )),
            IndexOperationExecutionState::Completed(IndexOperationOutcome::DropSucceeded),
        );
        let text_status = IndexOperationStatus::from_record(&text_drop);
        assert_eq!(
            text_status.common().operation_kind,
            PublicIndexOperationKind::Drop
        );
        assert_eq!(text_status.common().family, PublicIndexFamily::Text);
        assert_eq!(
            serde_json::to_value(text_status).unwrap()["status"],
            "succeeded"
        );
    }

    #[test]
    fn every_internal_blocker_maps_to_one_public_code() {
        let blockers = [
            (
                IndexOperationBlocker::InvalidSourceData {
                    entity_kind: IndexElementKind::Node,
                    entity_id: IndexEntityId::new(1),
                },
                IndexOperationBlockerCode::InvalidSourceData,
            ),
            (
                IndexOperationBlocker::UniquenessViolation {
                    first_entity_id: IndexEntityId::new(1),
                    second_entity_id: IndexEntityId::new(2),
                },
                IndexOperationBlockerCode::UniquenessViolation,
            ),
            (
                IndexOperationBlocker::OversizedEntity {
                    entity_kind: IndexElementKind::Edge,
                    entity_id: IndexEntityId::new(1),
                    observed: 2,
                    limit: 1,
                },
                IndexOperationBlockerCode::OversizedEntity,
            ),
            (
                IndexOperationBlocker::ManifestLimit {
                    partition: TextPartition::Unpartitioned,
                    observed: 2,
                    limit: 1,
                },
                IndexOperationBlockerCode::ManifestLimit,
            ),
            (
                IndexOperationBlocker::ReaderCoordinationUnavailable,
                IndexOperationBlockerCode::ReaderCoordinationUnavailable,
            ),
            (
                IndexOperationBlocker::ObjectStoreConfigurationUnavailable,
                IndexOperationBlockerCode::ObjectStoreConfigurationUnavailable,
            ),
            (
                IndexOperationBlocker::BlobPublicationCoordinationUnavailable,
                IndexOperationBlockerCode::BlobPublicationCoordinationUnavailable,
            ),
            (
                IndexOperationBlocker::InvariantViolation,
                IndexOperationBlockerCode::InvariantViolation,
            ),
            (
                IndexOperationBlocker::BlobPublicationMismatch {
                    intent_id: crate::index_v2::TextUploadIntentId::from_bytes([1; 16]).unwrap(),
                },
                IndexOperationBlockerCode::InvariantViolation,
            ),
        ];

        for (blocker, expected) in blockers {
            assert_eq!(IndexOperationBlockerCode::from(&blocker), expected);
        }
    }

    #[test]
    fn every_typed_progress_stage_maps_to_its_public_stage_and_counters() {
        let counters = OperationCounters {
            entities: 1,
            input_bytes: 2,
            output_operations: 3,
            output_bytes: 4,
        };
        let source = SourceScanProgress {
            inclusive_upper_bound: IndexCursor::try_new(Bytes::from_static(b"upper")).unwrap(),
            cursor: None,
            counters,
        };
        let prefix = PrefixScanProgress {
            cursor: None,
            counters,
        };
        let no_cursor = NoCursorProgress { counters };
        let drain = DrainProgress {
            drain_epoch: Some(1),
            counters,
        };
        let gc = GcProgress {
            gc_run_id: Some(crate::index_v2::BlobGcRunId::from_bytes([3; 16]).unwrap()),
            candidate_cursor: Some(
                IndexCursor::try_new(Bytes::from_static(b"candidate-boundary")).unwrap(),
            ),
            stage_cursor: None,
            counters,
        };
        let awaiting_upload = TextBuildUploadProgress::try_new(
            SourceScanProgress {
                inclusive_upper_bound: IndexCursor::try_new(Bytes::from_static(b"upper")).unwrap(),
                cursor: None,
                counters: OperationCounters {
                    entities: 0,
                    input_bytes: 1,
                    output_operations: 2,
                    output_bytes: 3,
                },
            },
            IndexCursor::try_new(Bytes::from_static(b"middle")).unwrap(),
            counters,
            IndexCursor::try_new(Bytes::from_static(b"artifact")).unwrap(),
            crate::index_v2::TextUploadIntentId::from_bytes([2; 16]).unwrap(),
        )
        .unwrap();
        let awaiting_catch_up_upload = TextCatchUpUploadProgress::try_new(
            PrefixScanProgress {
                cursor: None,
                counters: OperationCounters {
                    entities: 0,
                    input_bytes: 1,
                    output_operations: 2,
                    output_bytes: 3,
                },
            },
            IndexCursor::try_new(Bytes::from_static(b"delta")).unwrap(),
            counters,
            IndexCursor::try_new(Bytes::from_static(b"catch-up-artifact")).unwrap(),
            crate::index_v2::TextUploadIntentId::from_bytes([3; 16]).unwrap(),
        )
        .unwrap();
        let awaiting_compaction_upload = TextCompactionUploadProgress::try_new(
            PrefixScanProgress {
                cursor: None,
                counters: OperationCounters {
                    entities: 1,
                    input_bytes: 1,
                    output_operations: 2,
                    output_bytes: 3,
                },
            },
            vec![
                IndexCursor::try_new(Bytes::from_static(b"artifact-a")).unwrap(),
                IndexCursor::try_new(Bytes::from_static(b"artifact-b")).unwrap(),
            ],
            counters,
            IndexCursor::try_new(Bytes::from_static(b"artifact-c")).unwrap(),
            crate::index_v2::TextUploadIntentId::from_bytes([4; 16]).unwrap(),
        )
        .unwrap();
        let assert_progress = |progress, expected_stage| {
            let (stage, actual_counters) = public_progress(&progress);
            assert_eq!(stage, expected_stage);
            assert_eq!(actual_counters, counters);
        };

        for (progress, expected_stage) in [
            (
                SecondaryBuildStage::Scan(source.clone()),
                IndexOperationStage::Scan,
            ),
            (
                SecondaryBuildStage::CatchUp(prefix.clone()),
                IndexOperationStage::CatchUp,
            ),
            (
                SecondaryBuildStage::Validate(prefix.clone()),
                IndexOperationStage::Validate,
            ),
            (
                SecondaryBuildStage::Activate(no_cursor),
                IndexOperationStage::Activate,
            ),
        ] {
            assert_progress(
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    progress,
                )),
                expected_stage,
            );
        }
        for (progress, drop_stage, abort_stage) in [
            (
                SecondaryCleanupProgress::BeginDrain(drain),
                IndexOperationStage::BeginDrain,
                IndexOperationStage::AbortingBeginDrain,
            ),
            (
                SecondaryCleanupProgress::DeleteEntries(prefix.clone()),
                IndexOperationStage::DeleteEntries,
                IndexOperationStage::AbortingDeleteEntries,
            ),
            (
                SecondaryCleanupProgress::DeleteDeltas(prefix.clone()),
                IndexOperationStage::DeleteDeltas,
                IndexOperationStage::AbortingDeleteDeltas,
            ),
            (
                SecondaryCleanupProgress::FinishDrain(drain),
                IndexOperationStage::FinishDrain,
                IndexOperationStage::AbortingFinishDrain,
            ),
            (
                SecondaryCleanupProgress::Finalize(no_cursor),
                IndexOperationStage::Finalize,
                IndexOperationStage::AbortingFinalize,
            ),
        ] {
            assert_progress(
                IndexOperationProgress::SecondaryCleanup(progress.clone()),
                drop_stage,
            );
            assert_progress(
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(progress)),
                abort_stage,
            );
        }

        for (progress, expected_stage) in [
            (
                VectorBuildStage::Scan(source.clone()),
                IndexOperationStage::Scan,
            ),
            (
                VectorBuildStage::CatchUp(prefix.clone()),
                IndexOperationStage::CatchUp,
            ),
            (
                VectorBuildStage::ValidateDescriptor(prefix.clone()),
                IndexOperationStage::ValidateDescriptor,
            ),
            (
                VectorBuildStage::Activate(no_cursor),
                IndexOperationStage::Activate,
            ),
        ] {
            assert_progress(
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(progress)),
                expected_stage,
            );
        }
        for (progress, drop_stage, abort_stage) in [
            (
                VectorCleanupProgress::BeginDrain(drain),
                IndexOperationStage::BeginDrain,
                IndexOperationStage::AbortingBeginDrain,
            ),
            (
                VectorCleanupProgress::RetireCache(no_cursor),
                IndexOperationStage::RetireCache,
                IndexOperationStage::AbortingRetireCache,
            ),
            (
                VectorCleanupProgress::DeletePhysical(prefix.clone()),
                IndexOperationStage::DeletePhysical,
                IndexOperationStage::AbortingDeletePhysical,
            ),
            (
                VectorCleanupProgress::DeleteDeltas(prefix.clone()),
                IndexOperationStage::DeleteDeltas,
                IndexOperationStage::AbortingDeleteDeltas,
            ),
            (
                VectorCleanupProgress::FinishDrain(drain),
                IndexOperationStage::FinishDrain,
                IndexOperationStage::AbortingFinishDrain,
            ),
            (
                VectorCleanupProgress::Finalize(no_cursor),
                IndexOperationStage::Finalize,
                IndexOperationStage::AbortingFinalize,
            ),
        ] {
            assert_progress(
                IndexOperationProgress::VectorCleanup(progress.clone()),
                drop_stage,
            );
            assert_progress(
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(progress)),
                abort_stage,
            );
        }

        for (progress, expected_stage) in [
            (
                TextBuildStage::ScanSource(source.clone()),
                IndexOperationStage::Scan,
            ),
            (
                TextBuildStage::ScanPartitions(source),
                IndexOperationStage::ScanPartitions,
            ),
            (
                TextBuildStage::AwaitUpload(awaiting_upload),
                IndexOperationStage::AwaitUpload,
            ),
            (
                TextBuildStage::AwaitCatchUpUpload(awaiting_catch_up_upload),
                IndexOperationStage::AwaitUpload,
            ),
            (
                TextBuildStage::AwaitCompactionUpload(awaiting_compaction_upload),
                IndexOperationStage::AwaitUpload,
            ),
            (
                TextBuildStage::CatchUp(prefix.clone()),
                IndexOperationStage::CatchUp,
            ),
            (
                TextBuildStage::Compact(prefix.clone()),
                IndexOperationStage::Compact,
            ),
            (
                TextBuildStage::PrepareManifests(prefix.clone()),
                IndexOperationStage::PrepareManifests,
            ),
            (
                TextBuildStage::ValidateManifests(TextManifestValidationProgress::initial(
                    counters,
                )),
                IndexOperationStage::ValidateManifests,
            ),
            (
                TextBuildStage::Activate(no_cursor),
                IndexOperationStage::Activate,
            ),
        ] {
            assert_progress(
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(progress)),
                expected_stage,
            );
        }
        for (progress, drop_stage, abort_stage) in [
            (
                TextCleanupProgress::BeginDrain(drain),
                IndexOperationStage::BeginDrain,
                IndexOperationStage::AbortingBeginDrain,
            ),
            (
                TextCleanupProgress::PrepareCandidates(prefix.clone()),
                IndexOperationStage::PrepareCandidates,
                IndexOperationStage::AbortingPrepareCandidates,
            ),
            (
                TextCleanupProgress::AcquireDeleteFences(gc.clone()),
                IndexOperationStage::AcquireDeleteFences,
                IndexOperationStage::AbortingAcquireDeleteFences,
            ),
            (
                TextCleanupProgress::RetireManifest(gc.clone()),
                IndexOperationStage::RetireManifest,
                IndexOperationStage::AbortingRetireManifest,
            ),
            (
                TextCleanupProgress::RetireArtifacts(gc.clone()),
                IndexOperationStage::RetireArtifacts,
                IndexOperationStage::AbortingRetireArtifacts,
            ),
            (
                TextCleanupProgress::RetireUploadIntents(gc.clone()),
                IndexOperationStage::RetireUploadIntents,
                IndexOperationStage::AbortingRetireUploadIntents,
            ),
            (
                TextCleanupProgress::MarkReachability(gc.clone()),
                IndexOperationStage::MarkReachability,
                IndexOperationStage::AbortingMarkReachability,
            ),
            (
                TextCleanupProgress::DeleteBlobs(gc),
                IndexOperationStage::DeleteBlobs,
                IndexOperationStage::AbortingDeleteBlobs,
            ),
            (
                TextCleanupProgress::DeleteEntityState(prefix),
                IndexOperationStage::DeleteEntityState,
                IndexOperationStage::AbortingDeleteEntityState,
            ),
            (
                TextCleanupProgress::FinishDrain(drain),
                IndexOperationStage::FinishDrain,
                IndexOperationStage::AbortingFinishDrain,
            ),
            (
                TextCleanupProgress::Finalize(no_cursor),
                IndexOperationStage::Finalize,
                IndexOperationStage::AbortingFinalize,
            ),
        ] {
            assert_progress(
                IndexOperationProgress::TextCleanup(progress.clone()),
                drop_stage,
            );
            assert_progress(
                IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(progress)),
                abort_stage,
            );
        }
    }

    #[test]
    fn every_public_stage_has_one_frozen_snake_case_tag() {
        let stages = [
            (IndexOperationStage::Scan, "scan"),
            (IndexOperationStage::ScanPartitions, "scan_partitions"),
            (IndexOperationStage::CatchUp, "catch_up"),
            (IndexOperationStage::Validate, "validate"),
            (
                IndexOperationStage::ValidateDescriptor,
                "validate_descriptor",
            ),
            (IndexOperationStage::Compact, "compact"),
            (IndexOperationStage::PrepareManifests, "prepare_manifests"),
            (IndexOperationStage::ValidateManifests, "validate_manifests"),
            (IndexOperationStage::Activate, "activate"),
            (IndexOperationStage::BeginDrain, "begin_drain"),
            (IndexOperationStage::DeleteEntries, "delete_entries"),
            (IndexOperationStage::RetireCache, "retire_cache"),
            (IndexOperationStage::DeletePhysical, "delete_physical"),
            (IndexOperationStage::DeleteDeltas, "delete_deltas"),
            (IndexOperationStage::PrepareCandidates, "prepare_candidates"),
            (
                IndexOperationStage::AcquireDeleteFences,
                "acquire_delete_fences",
            ),
            (IndexOperationStage::RetireManifest, "retire_manifest"),
            (IndexOperationStage::RetireArtifacts, "retire_artifacts"),
            (
                IndexOperationStage::RetireUploadIntents,
                "retire_upload_intents",
            ),
            (IndexOperationStage::MarkReachability, "mark_reachability"),
            (IndexOperationStage::DeleteBlobs, "delete_blobs"),
            (
                IndexOperationStage::DeleteEntityState,
                "delete_entity_state",
            ),
            (IndexOperationStage::FinishDrain, "finish_drain"),
            (IndexOperationStage::Finalize, "finalize"),
            (
                IndexOperationStage::AbortingBeginDrain,
                "aborting_begin_drain",
            ),
            (
                IndexOperationStage::AbortingDeleteEntries,
                "aborting_delete_entries",
            ),
            (
                IndexOperationStage::AbortingRetireCache,
                "aborting_retire_cache",
            ),
            (
                IndexOperationStage::AbortingDeletePhysical,
                "aborting_delete_physical",
            ),
            (
                IndexOperationStage::AbortingDeleteDeltas,
                "aborting_delete_deltas",
            ),
            (
                IndexOperationStage::AbortingPrepareCandidates,
                "aborting_prepare_candidates",
            ),
            (
                IndexOperationStage::AbortingAcquireDeleteFences,
                "aborting_acquire_delete_fences",
            ),
            (
                IndexOperationStage::AbortingRetireManifest,
                "aborting_retire_manifest",
            ),
            (
                IndexOperationStage::AbortingRetireArtifacts,
                "aborting_retire_artifacts",
            ),
            (
                IndexOperationStage::AbortingRetireUploadIntents,
                "aborting_retire_upload_intents",
            ),
            (
                IndexOperationStage::AbortingMarkReachability,
                "aborting_mark_reachability",
            ),
            (
                IndexOperationStage::AbortingDeleteBlobs,
                "aborting_delete_blobs",
            ),
            (
                IndexOperationStage::AbortingDeleteEntityState,
                "aborting_delete_entity_state",
            ),
            (
                IndexOperationStage::AbortingFinishDrain,
                "aborting_finish_drain",
            ),
            (IndexOperationStage::AbortingFinalize, "aborting_finalize"),
        ];

        for (stage, expected) in stages {
            assert_eq!(serde_json::to_value(stage).unwrap(), expected);
        }
    }

    #[test]
    fn every_receipt_variant_uses_the_frozen_tag_and_decimal_ids() {
        let operation_id = IndexOperationId::from_bytes([7; 16]).unwrap();
        let receipts = [
            IndexDdlReceipt::Accepted {
                operation_id,
                index_id: IndexId::new(u64::MAX).unwrap(),
                generation: IndexGenerationId::new(u64::MAX).unwrap(),
            },
            IndexDdlReceipt::ExistingOperation { operation_id },
            IndexDdlReceipt::AlreadyActive {
                index_id: IndexId::new(u64::MAX).unwrap(),
                generation: IndexGenerationId::new(u64::MAX).unwrap(),
            },
        ];
        let json = receipts
            .into_iter()
            .map(|receipt| serde_json::to_value(receipt).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(json[0]["kind"], "accepted");
        assert_eq!(json[1]["kind"], "existing_operation");
        assert_eq!(json[2]["kind"], "already_active");
        assert_eq!(json[0]["index_id"], u64::MAX.to_string());
    }
}
