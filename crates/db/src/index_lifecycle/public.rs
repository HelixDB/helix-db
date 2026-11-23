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

/// Bounded-work counters safe for public progress reporting.
///
/// They grow while one writer runs the operation. A writer reports the steps
/// it has applied, and progressed steps do not wait for their WAL upload, so
/// after a crash or failover the counters can fall back to the last durable
/// step, which the build then redoes. Readers report only durable steps.
///
/// A vector BUILD that links its graph in memory counts here only as it
/// stores the graph; what it does before that is in
/// [`IndexOperationStatusCommon::vector_bulk_build`]. When such an attempt is
/// abandoned, after a restart for one, its rows are deleted and the counters
/// start again from zero with the next attempt or with the serial scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct IndexOperationPublicProgress {
    /// Source entities read. A build counts each entity its source scan
    /// reads and each entity it re-reads to apply a write made during the
    /// build; validation and other passes over entities already counted add
    /// bytes and operations, never entities, so a build over `N` entities
    /// with no concurrent writes reports `N`. Cleanup counts the rows it
    /// removes.
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

/// How a vector BUILD constructs its graph.
///
/// ```
/// use db::index_lifecycle::{VectorBuildPath, VectorSerialBuildReason};
///
/// let path = VectorBuildPath::Serial(VectorSerialBuildReason::OverMemoryBudget);
/// assert_eq!(
///     serde_json::to_value(path).unwrap(),
///     serde_json::json!({"path": "serial", "reason": "over_memory_budget"})
/// );
/// assert_eq!(
///     serde_json::to_value(VectorBuildPath::Bulk).unwrap(),
///     serde_json::json!({"path": "bulk"})
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "path", content = "reason", rename_all = "snake_case")]
pub enum VectorBuildPath {
    /// The whole graph is linked in memory and written once.
    Bulk,
    /// Vectors are inserted one at a time.
    Serial(VectorSerialBuildReason),
}

/// Why a vector BUILD inserts serially instead of bulk building.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorSerialBuildReason {
    /// Bulk builds are turned off.
    BulkBuildsDisabled,
    /// The index keeps one graph per tenant.
    TenantPartitioned,
    /// The memory the process may give bulk builds cannot hold the source's
    /// vectors and their graph.
    OverMemoryBudget,
    /// The source holds fewer vectors than a bulk build is started for.
    TooFewVectors,
    /// As many bulk attempts were started for this build as one build gets,
    /// and none stored its graph.
    EarlierAttemptsFailed,
    /// A bulk attempt failed in a way a serial build does not, which is a
    /// defect of the bulk path, or could not start because storage refused
    /// the upload of its count: what it stored was deleted and the build
    /// went on serially at once.
    BulkAttemptFailed,
    /// So many source rows changed after a bulk attempt read them that
    /// taking them out of its graph would have cost more than a serial
    /// build: what the attempt stored was deleted and the build went on
    /// serially at once.
    TooManyRowsChanged,
    /// A serial scan that an earlier process started is continuing.
    ResumedSerialScan,
}

/// What a vector BUILD on the bulk path is doing.
///
/// `bulk_attempt` is stored with the operation, so every process reports it.
/// It is not the operation's `attempt`, which counts retried steps.
/// The phase is known only to the writer process running the attempt.
///
/// ```
/// use db::index_lifecycle::{VectorBulkBuildPhase, VectorBulkBuildStatus, VectorBulkBuildWait};
///
/// let reading = VectorBulkBuildStatus {
///     bulk_attempt: 2,
///     bulk_attempt_limit: 3,
///     phase: Some(VectorBulkBuildPhase::Reading { members_read: 1_000, members: 250_000 }),
/// };
/// assert_eq!(
///     serde_json::to_value(reading).unwrap(),
///     serde_json::json!({
///         "bulk_attempt": 2,
///         "bulk_attempt_limit": 3,
///         "phase": "reading",
///         "members_read": "1000",
///         "members": "250000",
///     })
/// );
/// let waiting = VectorBulkBuildStatus {
///     bulk_attempt: 1,
///     bulk_attempt_limit: 3,
///     phase: Some(VectorBulkBuildPhase::Waiting { waiting_for: VectorBulkBuildWait::Memory }),
/// };
/// assert_eq!(
///     serde_json::to_value(waiting).unwrap(),
///     serde_json::json!({
///         "bulk_attempt": 1,
///         "bulk_attempt_limit": 3,
///         "phase": "waiting",
///         "waiting_for": "memory",
///     })
/// );
/// // A process that does not run the attempt knows its count alone.
/// let elsewhere = VectorBulkBuildStatus { bulk_attempt: 1, bulk_attempt_limit: 3, phase: None };
/// assert_eq!(
///     serde_json::to_value(elsewhere).unwrap(),
///     serde_json::json!({"bulk_attempt": 1, "bulk_attempt_limit": 3})
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct VectorBulkBuildStatus {
    /// Which bulk attempt of the build this is, counted from one since the
    /// operation was last at the serial scan: one more than the attempts
    /// that ended without storing their graph (see
    /// [`super::BulkBuildAttempts`]). Of a build that still waits to start,
    /// the attempt it will start.
    pub bulk_attempt: u8,
    /// Attempts after which the build scans serially.
    pub bulk_attempt_limit: u8,
    /// What the attempt is doing, where this process runs it.
    #[serde(flatten)]
    pub phase: Option<VectorBulkBuildPhase>,
}

/// One phase of a bulk attempt, with how far it has got.
///
/// An attempt reads its label, links what it read, and writes the graph;
/// before it reads it may wait, and whatever an attempt that did not finish
/// left stored is deleted before the next one starts. The stored progress
/// counters ([`IndexOperationPublicProgress`]) move only while it writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum VectorBulkBuildPhase {
    /// The attempt has not started: the build waits, holding no memory and
    /// taking no step.
    Waiting {
        /// What it waits for.
        waiting_for: VectorBulkBuildWait,
    },
    /// The source rows of the label's members are read into memory.
    Reading {
        /// Rows read so far. Members whose row is gone are not counted, so
        /// the read can end below `members`.
        #[serde(with = "u64_string")]
        members_read: u64,
        /// Members of the label the attempt reads.
        #[serde(with = "u64_string")]
        members: u64,
    },
    /// The graph is linked in memory.
    Linking {
        /// Vectors inserted into the graph so far.
        #[serde(with = "u64_string")]
        vectors_linked: u64,
        /// Vectors the read found.
        #[serde(with = "u64_string")]
        vectors: u64,
    },
    /// The graph is written, in source order.
    Writing {
        /// Graph nodes stored, or left out because their entity changed
        /// since it was read.
        #[serde(with = "u64_string")]
        nodes_written: u64,
        /// Nodes of the graph.
        #[serde(with = "u64_string")]
        nodes: u64,
        /// Whether the last step wrote nothing because the write buffer
        /// held too much unflushed, its own rows or other writers'.
        paused_for_write_buffer: bool,
    },
    /// Rows an attempt that did not finish left stored are deleted.
    Discarding {
        /// Rows this process has deleted so far.
        #[serde(with = "u64_string")]
        rows_deleted: u64,
    },
}

/// What a bulk attempt waits for before it starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorBulkBuildWait {
    /// Another build reads or links: one does at a time.
    Turn,
    /// Memory that running bulk builds hold and will release.
    Memory,
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
    /// Claims that have tried the current checkpoint: `1` while a healthy
    /// operation runs. Every step that makes progress resets it, so it grows only
    /// while consecutive attempts fail to make progress.
    pub attempt: u32,
    /// Bounded-work progress; see [`IndexOperationPublicProgress`] for when
    /// it can fall back.
    pub progress: IndexOperationPublicProgress,
    /// How a vector BUILD constructs its graph.
    ///
    /// `bulk` at stage `scan` is stored with the operation, so every process
    /// reports it, for as long as the build is on the bulk path: a release
    /// from before bulk builds cannot open the database as a writer until no
    /// operation reports the two together. A writer reports them for as long
    /// as that is what a restart would find, which is a moment longer than
    /// its own steps have left the bulk path: until the step that left it is
    /// uploaded. Every other value is known only to the writer process
    /// running the build: absent for other operations, before that process
    /// has chosen, on readers, and once the process has restarted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_build_path: Option<VectorBuildPath>,
    /// What a vector BUILD on the bulk path is doing: present while the
    /// operation is stored on that path, and while this process's build
    /// waits to start its first attempt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bulk_build: Option<VectorBulkBuildStatus>,
}

/// Whether `record` is a vector BUILD at its bulk stage, the one stage a
/// release from before bulk builds cannot read.
pub(crate) fn at_bulk_stage(record: &IndexOperationRecord) -> bool {
    matches!(
        record.progress(),
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::BulkBuild(_)
        ))
    )
}

/// Whether `record` is a vector BUILD that an abort took off its
/// construction, at whatever stage it stood: the one way a build leaves the
/// bulk stage without a step of the process that runs it.
pub(crate) fn is_aborting_vector_build(record: &IndexOperationRecord) -> bool {
    matches!(
        record.progress(),
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(_))
    )
}

/// The record an operation's status is projected from.
///
/// `applied` is what this process's last committed step stored, and
/// `durable` what a restart would find: a progressed step does not wait for
/// its WAL upload. A writer reports `applied`, with one exception. While
/// `durable` is still at a vector BUILD's bulk stage and `applied` has left
/// it, the status is `durable`'s: that the bulk stage is stored decides
/// whether an older release can open the database, so no status may say it
/// is gone before it is gone from what such a release would read.
///
/// A process that stepped the operation at the bulk stage can be ahead in
/// that way, so such a process reads `durable` until it has read the two
/// records equal off the stage
/// ([`crate::index_lifecycle::vector::VectorBuildReports::bulk_stage_sightings`]).
/// So can any writer once the build is aborted, which takes it off the stage
/// without a step: `durable` is read for every status of a vector build
/// that is aborting or was aborted ([`is_aborting_vector_build`]).
pub(crate) fn status_record<'a>(
    applied: &'a IndexOperationRecord,
    durable: Option<&'a IndexOperationRecord>,
) -> &'a IndexOperationRecord {
    match durable {
        Some(durable) if at_bulk_stage(durable) && !at_bulk_stage(applied) => durable,
        Some(_) | None => applied,
    }
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
        // The bulk attempts a record at the bulk stage counts.
        let bulk_attempts = match record.progress() {
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::BulkBuild(progress),
            )) => Some(progress.attempts),
            IndexOperationProgress::VectorBuild(_)
            | IndexOperationProgress::VectorCleanup(_)
            | IndexOperationProgress::SecondaryBuild(_)
            | IndexOperationProgress::SecondaryCleanup(_)
            | IndexOperationProgress::TextBuild(_)
            | IndexOperationProgress::TextCleanup(_) => None,
        };
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
            vector_build_path: bulk_attempts.map(|_| VectorBuildPath::Bulk),
            vector_bulk_build: bulk_attempts.map(|attempts| VectorBulkBuildStatus {
                bulk_attempt: attempts.get(),
                bulk_attempt_limit: super::BulkBuildAttempts::LIMIT,
                phase: None,
            }),
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

    /// Attaches what this process knows of the operation's vector build
    /// beside its record: the path it chose, and the phase of the bulk
    /// attempt it runs.
    ///
    /// A stored bulk build stays `bulk` until its record leaves the bulk
    /// path, whatever the process has chosen since. A stored serial scan is
    /// never reported as `bulk`: an attempt the process remembers is over. So
    /// stage `scan` with path `bulk` is reported exactly while the bulk stage
    /// is stored.
    ///
    /// The phase goes to the attempt the record counts, except that a build
    /// that waits to start has ended that attempt and reports the one it
    /// will start, the next. Of a record at the serial scan only a build
    /// that waits to start is reported, as its first attempt; any other
    /// phase the process remembers there is over.
    #[must_use]
    pub fn with_vector_build_report(
        mut self,
        path: Option<VectorBuildPath>,
        phase: Option<VectorBulkBuildPhase>,
    ) -> Self {
        let (Self::Queued { common }
        | Self::Running { common }
        | Self::Blocked { common, .. }
        | Self::Succeeded { common }
        | Self::Aborted { common }) = &mut self;
        let serial_scan = common.stage == IndexOperationStage::Scan;
        common.vector_build_path = common
            .vector_build_path
            .or(path.filter(|path| !(serial_scan && *path == VectorBuildPath::Bulk)));
        common.vector_bulk_build = match (common.vector_bulk_build, phase) {
            // The attempt the record counts is over, and its rows are gone.
            (Some(stored), Some(phase @ VectorBulkBuildPhase::Waiting { .. })) => {
                Some(VectorBulkBuildStatus {
                    bulk_attempt: stored.bulk_attempt.saturating_add(1),
                    phase: Some(phase),
                    ..stored
                })
            }
            (Some(stored), phase) => Some(VectorBulkBuildStatus { phase, ..stored }),
            (None, Some(phase @ VectorBulkBuildPhase::Waiting { .. })) if serial_scan => {
                Some(VectorBulkBuildStatus {
                    bulk_attempt: super::BulkBuildAttempts::FIRST.get(),
                    bulk_attempt_limit: super::BulkBuildAttempts::LIMIT,
                    phase: Some(phase),
                })
            }
            (None, Some(_) | None) => None,
        };
        self
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
                // The public stage is the scan's; `vector_build_path` tells
                // the two apart.
                VectorBuildStage::BulkBuild(value) => {
                    (IndexOperationStage::Scan, source_counters(&value.scan))
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
