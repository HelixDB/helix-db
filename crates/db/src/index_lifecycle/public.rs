//! Stable public receipts and operation-status projections.
//!
//! Durable operation records contain worker claims, retry deadlines, and raw
//! resume cursors. This module is the one-way contract boundary that projects
//! those records into the deliberately smaller JSON/SDK lifecycle API.
//!
//! ```
//! use db::index_lifecycle::{IndexDdlReceipt, IndexGenerationId, IndexId, IndexOperationId};
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

impl IndexDdlReceipt {
    #[cfg(any(test, feature = "bench-internals"))]
    pub(crate) const fn operation_id(&self) -> Option<IndexOperationId> {
        match self {
            Self::Accepted { operation_id, .. } | Self::ExistingOperation { operation_id } => {
                Some(*operation_id)
            }
            Self::AlreadyActive { .. } => None,
        }
    }
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
    /// Text object-store configuration is unavailable.
    ObjectStoreConfigurationUnavailable,
    /// An internal lifecycle invariant could not be proven.
    InvariantViolation,
    /// A legacy vector namespace failed structural validation.
    InvalidLegacyPhysical,
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
    /// Apply mutation deltas captured during the scan.
    CatchUp,
    /// Validate secondary ownership and uniqueness.
    Validate,
    /// Validate vector physical metadata against the descriptor.
    ValidateDescriptor,
    /// Validate an unchanged legacy vector namespace before ownership transfer.
    ValidateLegacyPhysical,
    /// Compact text split state.
    Compact,
    /// Construct bounded text manifest pages.
    PrepareManifests,
    /// Validate manifest topology and uploaded blob metadata.
    ValidateManifests,
    /// Publish the hidden generation as active.
    Activate,
    /// Delete secondary generation entries.
    DeleteEntries,
    /// Retire a vector generation from memory.
    RetireCache,
    /// Delete vector physical rows.
    DeletePhysical,
    /// Delete retained mutation deltas.
    DeleteDeltas,
    /// Delete text generation metadata while retaining immutable blobs.
    DeleteMetadata,
    /// Finalize ordinary DROP cleanup.
    Finalize,
    /// Delete secondary entries during BUILD abort cleanup.
    AbortingDeleteEntries,
    /// Retire vector memory during BUILD abort cleanup.
    AbortingRetireCache,
    /// Delete vector rows during BUILD abort cleanup.
    AbortingDeletePhysical,
    /// Delete retained deltas during BUILD abort cleanup.
    AbortingDeleteDeltas,
    /// Delete text metadata during BUILD abort cleanup.
    AbortingDeleteMetadata,
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
            IndexOperationBlocker::ObjectStoreConfigurationUnavailable => {
                Self::ObjectStoreConfigurationUnavailable
            }
            IndexOperationBlocker::InvariantViolation => Self::InvariantViolation,
            IndexOperationBlocker::InvalidLegacyPhysical => Self::InvalidLegacyPhysical,
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
                VectorBuildStage::AdoptLegacy(value) => {
                    (IndexOperationStage::ValidateLegacyPhysical, value.counters)
                }
                VectorBuildStage::ValidateAdoptedDirectory(value) => {
                    (IndexOperationStage::ValidateLegacyPhysical, value.counters)
                }
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
        (CleanupProjection::Drop, SecondaryCleanupProgress::DeleteEntries(value)) => {
            (IndexOperationStage::DeleteEntries, value.counters)
        }
        (CleanupProjection::Drop, SecondaryCleanupProgress::DeleteDeltas(value)) => {
            (IndexOperationStage::DeleteDeltas, value.counters)
        }
        (CleanupProjection::Drop, SecondaryCleanupProgress::Finalize(value)) => {
            (IndexOperationStage::Finalize, value.counters)
        }
        (CleanupProjection::Abort, SecondaryCleanupProgress::DeleteEntries(value)) => {
            (IndexOperationStage::AbortingDeleteEntries, value.counters)
        }
        (CleanupProjection::Abort, SecondaryCleanupProgress::DeleteDeltas(value)) => {
            (IndexOperationStage::AbortingDeleteDeltas, value.counters)
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
        (CleanupProjection::Drop, VectorCleanupProgress::RetireCache(value)) => {
            (IndexOperationStage::RetireCache, value.counters)
        }
        (CleanupProjection::Drop, VectorCleanupProgress::DeletePhysical(value)) => {
            (IndexOperationStage::DeletePhysical, value.counters)
        }
        (CleanupProjection::Drop, VectorCleanupProgress::DeleteDeltas(value)) => {
            (IndexOperationStage::DeleteDeltas, value.counters)
        }
        (CleanupProjection::Drop, VectorCleanupProgress::Finalize(value)) => {
            (IndexOperationStage::Finalize, value.counters)
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
        (CleanupProjection::Drop, TextCleanupProgress::DeleteMetadata(value)) => {
            (IndexOperationStage::DeleteMetadata, value.counters)
        }
        (CleanupProjection::Drop, TextCleanupProgress::Finalize(value)) => {
            (IndexOperationStage::Finalize, value.counters)
        }
        (CleanupProjection::Abort, TextCleanupProgress::DeleteMetadata(value)) => {
            (IndexOperationStage::AbortingDeleteMetadata, value.counters)
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
