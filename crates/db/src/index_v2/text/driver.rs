//! Bounded V2 text-index build driver.
//!
//! Text construction starts with a durable two-pass source boundary. The
//! `ScanSource` pass reads authoritative graph property rows and stages only
//! typed, generation-qualified
//! [`TextEntityStateValue`](crate::index_v2::work::TextEntityStateValue)
//! records. Those
//! keys sort by partition fingerprint before entity identity, so the later
//! `ScanPartitions` pass can build bounded multi-document splits even when
//! tenant values are arbitrarily interleaved in graph-ID order.
//!
//! The driver owns no database handle. Source staging borrows the repository
//! transaction supplied by the outbox dispatcher. Partition construction uses
//! a short-lived read snapshot, drops it before CPU-heavy split construction,
//! and retains only the publication coordinator service needed to reserve and
//! publish the exact child outbox. Holding that service in an `Arc` is required
//! by the parent-owned worker's object-safe driver lifetime and is acyclic: the
//! coordinator owns only object-store state, never this driver or the database.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::config::{SearchIndexBatchLimits, TextBackfillCompactionLimits};
use crate::encoding::property;
use crate::encoding::v1::keys::index_v2::{
    GlobalIndexV2Key, IndexEntity, IndexEntityStateKey, IndexV2Key, IndexV2RecordKind,
    PartitionFingerprint, TextBuildArtifactKey, TextEntityStateKey, TextIntentOwnedKey,
    TextManifestRootKey,
};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, GlobalKeyKind, Key, KeyPrefix};
use crate::encoding::v1::values::index_v2::{
    decode_index_record, decode_metadata_value, decode_work_value, encode_work_value,
    IndexV2WorkValue,
};
use crate::error::{HelixDbError, Result};
use crate::index_v2::blob_publication::{
    BlobPermitReleaseAuthority, BlobPublicationCoordinator, BlobPublicationError,
    BlobReferenceGuard,
};
use crate::index_v2::outbox::{
    IndexOperationDriver, IndexOperationStepPermit, IndexOperationStepResult,
    PreparedIndexOperationStep, PreparedStepCommitResolution,
};
use crate::index_v2::reader_lease::IndexLeaseCoordinator;
use crate::index_v2::text::upload::{
    self, PreparedTextUploadIntent, PreparedUploadObservation, PreparedUploadStageOutcome,
};
use crate::index_v2::work::{
    self, AppliedEntityStateValue, AppliedFamilyState, CoalescedBuildDeltaValue,
    TextEntityStateValue, TextPartition,
};
use crate::index_v2::{
    BuildOperationOutcome, IndexCursor, IndexElementKind, IndexEntityId, IndexOperationBlocker,
    IndexOperationExecutionState, IndexOperationFamily, IndexOperationOutcome,
    IndexOperationProgress, IndexOperationRecord, IndexRecordV2, IndexV2MetadataValue,
    OperationCounters, PrefixScanProgress, SourceScanProgress, TextBuildProgress, TextBuildStage,
    TextBuildUploadProgress, TextCatchUpUploadProgress, TextCompactionUploadProgress,
    TextLogicalVersion, TextManifestValidationProgress, TextUploadIntentId,
    ValidatedDynamicIndexDefinition, ValidatedTextIndexDefinition,
};

/// Runtime availability of the publication service required by partition work.
enum TextPublicationRuntime {
    /// Source-only tests may exercise `ScanSource` without claiming upload readiness.
    Unavailable,
    /// Complete object/publication dependencies used by split-producing work.
    Installed(TextPublicationDependencies),
}

/// Inseparable runtime services required by partition, catch-up, and compaction I/O.
///
/// Keeping the object store, its database namespace, coordinator, and validated
/// compaction policy in one installed variant prevents a driver from claiming
/// compaction readiness with only some of the resources needed to finish it.
struct TextPublicationDependencies {
    coordinator: Arc<dyn BlobPublicationCoordinator>,
    object_store: Arc<dyn ObjectStore>,
    db_path: String,
    compaction_limits: TextBackfillCompactionLimits,
    gc_gate: crate::search::text::BlobGcGate,
}

/// Process-local authority retained across one text cleanup transaction.
///
/// Candidate preparation needs only exact-scope mutation exclusion. Once a
/// run reaches `FencesClosed`, owner retirement and reachability/disposition
/// also retain the exclusive blob gate, preventing a same-writer publication
/// from crossing the transaction snapshot.
enum TextCleanupPermit {
    ScopeOnly {
        _scope: tokio::sync::OwnedRwLockWriteGuard<()>,
    },
    ScopeAndBlobDeletion {
        _scope: tokio::sync::OwnedRwLockWriteGuard<()>,
        _blobs: crate::search::text::BlobDeletionPermit,
    },
}

/// Family driver for durable text build checkpoints.
///
/// The outbox repository owns transaction creation and commits. This driver
/// stages only family-specific rows and returns the next closed progress ADT.
pub(crate) struct TextIndexDriver {
    scope_gates: Arc<crate::index_v2::IndexScopeGates>,
    publication: TextPublicationRuntime,
    reader_leases: Option<Arc<dyn IndexLeaseCoordinator>>,
}

impl core::fmt::Debug for TextIndexDriver {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TextIndexDriver")
            .field(
                "publication_installed",
                &matches!(self.publication, TextPublicationRuntime::Installed(_)),
            )
            .field("reader_coordination", &self.reader_leases.is_some())
            .finish()
    }
}

impl Default for TextIndexDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl TextIndexDriver {
    /// Constructs a source-only driver that cannot prepare partition uploads.
    ///
    /// Production capability installation must use
    /// [`Self::with_publication_runtime`]. Keeping the unavailable state
    /// explicit prevents a test-only driver from performing uncoordinated I/O.
    pub(crate) fn new() -> Self {
        Self {
            scope_gates: Arc::new(crate::index_v2::IndexScopeGates::default()),
            publication: TextPublicationRuntime::Unavailable,
            reader_leases: None,
        }
    }

    /// Constructs an isolated complete runtime for text family unit tests.
    #[cfg(test)]
    pub(crate) fn with_publication_runtime(
        coordinator: Arc<dyn BlobPublicationCoordinator>,
        object_store: Arc<dyn ObjectStore>,
        db_path: impl Into<String>,
        compaction_limits: TextBackfillCompactionLimits,
        gc_gate: crate::search::text::BlobGcGate,
    ) -> Self {
        Self::with_lifecycle_runtime(
            Arc::new(crate::index_v2::IndexScopeGates::default()),
            coordinator,
            object_store,
            db_path,
            compaction_limits,
            gc_gate,
            Some(Arc::new(
                crate::index_v2::reader_lease::ProcessLocalIndexLeaseCoordinator::new(
                    crate::index_v2::reader_lease::ReaderLeaseTiming::default(),
                ),
            )),
        )
    }

    /// Constructs a complete text driver sharing the production mutation gate.
    pub(crate) fn with_lifecycle_runtime(
        scope_gates: Arc<crate::index_v2::IndexScopeGates>,
        coordinator: Arc<dyn BlobPublicationCoordinator>,
        object_store: Arc<dyn ObjectStore>,
        db_path: impl Into<String>,
        compaction_limits: TextBackfillCompactionLimits,
        gc_gate: crate::search::text::BlobGcGate,
        reader_leases: Option<Arc<dyn IndexLeaseCoordinator>>,
    ) -> Self {
        Self {
            scope_gates,
            publication: TextPublicationRuntime::Installed(TextPublicationDependencies {
                coordinator,
                object_store,
                db_path: db_path.into(),
                compaction_limits,
                gc_gate,
            }),
            reader_leases,
        }
    }
}

/// Closed text preparation consumed by exactly one repository dispatch.
pub(crate) enum PreparedTextOperationStep {
    /// A pre-read selected a repository-only transition or blocker.
    Repository(Box<PreparedTextRepositoryStep>),
    /// One generation-owner batch under the recovered immutable fence set.
    CleanupOwner {
        prepared: Box<super::cleanup::PreparedFencedOwnerRetirement>,
        _permit: Box<dyn IndexOperationStepPermit>,
    },
    /// One externally normalized upload intent under both cleanup gates.
    CleanupIntent {
        prepared: Box<super::cleanup::PreparedUploadIntentRetirement>,
        _permit: Box<dyn IndexOperationStepPermit>,
    },
    /// One partition split and its reserved durable upload child.
    PartitionUpload(Box<PreparedTextBuildUpload>),
    /// One catch-up entity and its reserved durable upload child.
    CatchUpUpload(Box<PreparedTextBuildUpload>),
    /// One bounded compaction replacement and its reserved durable upload child.
    CompactionUpload(Box<PreparedTextBuildUpload>),
    /// One all-stale compaction whose exact inputs can retire without a child.
    CompactionRetirement(Box<PreparedTextCompactionRetirement>),
    /// One range-validated manifest exhaustion or blocker transition.
    ManifestRepository(Box<PreparedTextManifestRepositoryStep>),
    /// One reference-fenced artifact-to-manifest-page relocation.
    ManifestPage(Box<PreparedTextManifestPage>),
    /// One range-fenced pre-activation validation checkpoint.
    Validation(Box<PreparedTextValidationStep>),
}

/// Closed validation preparation with exactly the authority its lane requires.
pub(crate) enum PreparedTextValidationStep {
    /// Root, upload-intent, exhaustion, or invariant-blocker validation.
    Database {
        source_operation: IndexOperationRecord,
        prepared: super::validation::PreparedDatabaseValidation,
    },
    /// Page validation retaining the local and coordinator reference guards.
    Page {
        source_operation: IndexOperationRecord,
        prepared: super::validation::PreparedPageValidation,
        _local_gate: crate::search::text::BlobPublicationPermit,
        _references: Vec<BlobReferenceGuard>,
    },
}

/// Repository-only text result prepared without an external reservation.
pub(crate) struct PreparedTextRepositoryStep {
    source_operation: IndexOperationRecord,
    expected_reads: Vec<PreparedTextExpectedRead>,
    writes: Vec<PreparedTextWrite>,
    result: IndexOperationStepResult,
}

/// Exact build upload retained across its atomic operation/child commit.
pub(crate) struct PreparedTextBuildUpload {
    coordinator: Arc<dyn BlobPublicationCoordinator>,
    source_operation: IndexOperationRecord,
    expected_operation: IndexOperationRecord,
    progress: IndexOperationProgress,
    artifact_key: Bytes,
    expected_reads: Vec<PreparedTextExpectedRead>,
    lifecycle_writes: Vec<PreparedTextWrite>,
    intent: PreparedTextUploadIntent,
    payload: Bytes,
}

/// Exact all-stale input retirement retained across repository dispatch.
pub(crate) struct PreparedTextCompactionRetirement {
    source_operation: IndexOperationRecord,
    expected_reads: Vec<PreparedTextExpectedRead>,
    input_artifact_keys: Vec<IndexCursor>,
    progress: IndexOperationProgress,
}

/// Manifest result whose source range must remain exact through commit.
pub(crate) struct PreparedTextManifestRepositoryStep {
    source_operation: IndexOperationRecord,
    range: super::manifest::PreparedArtifactRange,
    expected_reads: Vec<PreparedTextExpectedRead>,
    result: IndexOperationStepResult,
}

/// Exact manifest page retained with local and coordinator reference authority.
pub(crate) struct PreparedTextManifestPage {
    source_operation: IndexOperationRecord,
    prepared: super::manifest::PreparedManifestPage,
    progress: IndexOperationProgress,
    _local_gate: crate::search::text::BlobPublicationPermit,
    _references: Vec<BlobReferenceGuard>,
}

/// Exact row observation that prevents a prepared catch-up split from going stale.
#[derive(Clone)]
struct PreparedTextExpectedRead {
    key: Bytes,
    value: Option<Bytes>,
}

/// Typed operation-owned write staged only with the matching upload child.
#[derive(Clone)]
enum PreparedTextWrite {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

/// Exact observed state and optional creation of one canonical empty manifest.
#[derive(Clone)]
struct PreparedEmptyManifestRoot {
    observation: PreparedTextExpectedRead,
    write: Option<(Bytes, Bytes)>,
}

impl PreparedEmptyManifestRoot {
    /// Returns whether this step must create the canonical empty value.
    const fn requires_creation(&self) -> bool {
        self.write.is_some()
    }

    /// Returns bytes read while proving the root absent or exactly empty.
    fn input_bytes(&self) -> u64 {
        u64::try_from(
            self.observation
                .key
                .len()
                .saturating_add(self.observation.value.as_ref().map_or(0, Bytes::len)),
        )
        .unwrap_or(u64::MAX)
    }

    /// Returns the one optional root-creation operation.
    const fn output_operations(&self) -> u64 {
        if self.write.is_some() {
            1
        } else {
            0
        }
    }

    /// Returns exact encoded bytes written by optional root creation.
    fn output_bytes(&self) -> u64 {
        self.write.as_ref().map_or(0, |(key, value)| {
            u64::try_from(key.len().saturating_add(value.len())).unwrap_or(u64::MAX)
        })
    }

    /// Separates the retained read from the optional atomic write.
    fn into_parts(self) -> (PreparedTextExpectedRead, Option<PreparedTextWrite>) {
        (
            self.observation,
            self.write
                .map(|(key, value)| PreparedTextWrite::Put { key, value }),
        )
    }
}

/// Point-observes one partition root and prepares its canonical empty value.
///
/// This boundary is used only before manifest paging begins. An existing root
/// must therefore be the exact initial empty value; a partially populated root
/// indicates a stage/ownership violation rather than an idempotent replay.
async fn prepare_empty_manifest_root(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    partition: TextPartition,
) -> Result<PreparedEmptyManifestRoot> {
    let key = scoped_index_key(
        scope,
        IndexV2Key::TextManifestRoot(TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: partition.fingerprint(),
        }),
    );
    let value = transaction.get(&key).await?;
    let empty = work::TextManifestRootValue::empty(
        operation.index_id(),
        operation.generation(),
        partition.clone(),
    );
    let Some(observed_value) = value.as_ref() else {
        return Ok(PreparedEmptyManifestRoot {
            observation: PreparedTextExpectedRead {
                key: key.clone(),
                value: None,
            },
            write: Some((
                key,
                encode_work_value(&IndexV2WorkValue::TextManifestRoot(empty)),
            )),
        });
    };
    let IndexV2WorkValue::TextManifestRoot(root) = decode_work_value(observed_value)? else {
        return Err(corruption(
            "text empty-manifest root key contains another work value",
        ));
    };
    if root != empty {
        return Err(corruption(
            "text partition root is not its exact initial empty manifest",
        ));
    }
    Ok(PreparedEmptyManifestRoot {
        observation: PreparedTextExpectedRead { key, value },
        write: None,
    })
}

impl PreparedTextOperationStep {
    /// Stages only the transition already authorized by this preparation.
    pub(crate) async fn stage(
        &self,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Result<IndexOperationStepResult> {
        match self {
            Self::Repository(prepared) => {
                if operation != &prepared.source_operation {
                    return Err(corruption(
                        "prepared text repository step no longer matches its claimed operation",
                    ));
                }
                for expected in &prepared.expected_reads {
                    if transaction.get(&expected.key).await? != expected.value {
                        return Ok(IndexOperationStepResult::TransientFailure);
                    }
                }
                for write in &prepared.writes {
                    match write {
                        PreparedTextWrite::Put { key, value } => transaction.put(key, value)?,
                        PreparedTextWrite::Delete { key } => transaction.delete(key)?,
                    }
                }
                Ok(prepared.result.clone())
            }
            Self::CleanupOwner { prepared, .. } => {
                prepared.stage(transaction, scope, operation).await
            }
            Self::CleanupIntent { prepared, .. } => {
                prepared.stage(transaction, scope, operation).await
            }
            Self::PartitionUpload(prepared)
            | Self::CatchUpUpload(prepared)
            | Self::CompactionUpload(prepared) => {
                if operation != &prepared.source_operation {
                    return Err(corruption(
                        "prepared text partition upload no longer matches its claimed operation",
                    ));
                }
                if transaction.get(&prepared.artifact_key).await?.is_some() {
                    return Err(corruption(
                        "prepared text partition upload targets an occupied artifact key",
                    ));
                }
                for expected in &prepared.expected_reads {
                    if transaction.get(&expected.key).await? != expected.value {
                        return Ok(IndexOperationStepResult::TransientFailure);
                    }
                }
                match upload::stage_prepared_upload(transaction, scope, &prepared.intent).await? {
                    PreparedUploadStageOutcome::Staged => {
                        for write in &prepared.lifecycle_writes {
                            match write {
                                PreparedTextWrite::Put { key, value } => {
                                    transaction.put(key, value)?;
                                }
                                PreparedTextWrite::Delete { key } => transaction.delete(key)?,
                            }
                        }
                        Ok(IndexOperationStepResult::Progressed(
                            prepared.progress.clone(),
                        ))
                    }
                    PreparedUploadStageOutcome::IdentifierCollision => {
                        Ok(IndexOperationStepResult::TransientFailure)
                    }
                    PreparedUploadStageOutcome::AlreadyDurable => Err(corruption(
                        "prepared text child is durable before its owning operation checkpoint",
                    )),
                }
            }
            Self::CompactionRetirement(prepared) => {
                if operation != &prepared.source_operation {
                    return Err(corruption(
                        "prepared text compaction retirement no longer matches its claimed operation",
                    ));
                }
                for expected in &prepared.expected_reads {
                    if transaction.get(&expected.key).await? != expected.value {
                        return Ok(IndexOperationStepResult::TransientFailure);
                    }
                }
                super::compaction::stage_input_retirement(
                    transaction,
                    scope,
                    operation,
                    &prepared.input_artifact_keys,
                )
                .await?;
                Ok(IndexOperationStepResult::Progressed(
                    prepared.progress.clone(),
                ))
            }
            Self::ManifestRepository(prepared) => {
                if operation != &prepared.source_operation {
                    return Err(corruption(
                        "prepared text manifest result no longer matches its claimed operation",
                    ));
                }
                if !prepared.range.is_current(transaction).await? {
                    return Ok(IndexOperationStepResult::TransientFailure);
                }
                for expected in &prepared.expected_reads {
                    if transaction.get(&expected.key).await? != expected.value {
                        return Ok(IndexOperationStepResult::TransientFailure);
                    }
                }
                Ok(prepared.result.clone())
            }
            Self::ManifestPage(prepared) => {
                if operation != &prepared.source_operation {
                    return Err(corruption(
                        "prepared text manifest page no longer matches its claimed operation",
                    ));
                }
                if !prepared.prepared.stage(transaction).await? {
                    return Ok(IndexOperationStepResult::TransientFailure);
                }
                Ok(IndexOperationStepResult::Progressed(
                    prepared.progress.clone(),
                ))
            }
            Self::Validation(prepared) => match prepared.as_ref() {
                PreparedTextValidationStep::Database {
                    source_operation,
                    prepared,
                } => {
                    if operation != source_operation {
                        return Err(corruption(
                            "prepared text validation no longer matches its claimed operation",
                        ));
                    }
                    prepared.stage(transaction).await
                }
                PreparedTextValidationStep::Page {
                    source_operation,
                    prepared,
                    ..
                } => {
                    if operation != source_operation {
                        return Err(corruption(
                            "prepared text page validation no longer matches its claimed operation",
                        ));
                    }
                    prepared.stage(transaction).await
                }
            },
        }
    }

    /// Releases only a reservation proven not to have entered the outbox.
    pub(crate) async fn discard(self) -> Result<()> {
        let prepared = match self {
            Self::Repository(_)
            | Self::CleanupOwner { .. }
            | Self::CleanupIntent { .. }
            | Self::CompactionRetirement(_)
            | Self::ManifestRepository(_)
            | Self::ManifestPage(_)
            | Self::Validation(_) => {
                return Ok(());
            }
            Self::PartitionUpload(prepared)
            | Self::CatchUpUpload(prepared)
            | Self::CompactionUpload(prepared) => prepared,
        };
        let prepared = *prepared;
        let permit = prepared.intent.permit();
        prepared
            .coordinator
            .release(
                &permit,
                BlobPermitReleaseAuthority::definitive_non_publication(permit.id()),
            )
            .await
            .map_err(coordinator_error)
    }

    /// Resolves a commit error from the exact operation and complete child triple.
    pub(crate) async fn resolve_commit_error(
        &self,
        db: &Db,
        scope: DataScope,
    ) -> Result<PreparedStepCommitResolution> {
        let prepared = match self {
            Self::Repository(_)
            | Self::CleanupOwner { .. }
            | Self::CleanupIntent { .. }
            | Self::CompactionRetirement(_)
            | Self::ManifestRepository(_)
            | Self::ManifestPage(_)
            | Self::Validation(_) => {
                return Ok(PreparedStepCommitResolution::Ordinary);
            }
            Self::PartitionUpload(prepared)
            | Self::CatchUpUpload(prepared)
            | Self::CompactionUpload(prepared) => prepared,
        };
        let operation = super::super::outbox::read_operation(
            db,
            scope,
            prepared.source_operation.operation_id(),
        )
        .await?;
        let child = upload::observe_prepared_upload(db, scope, &prepared.intent).await?;
        if operation.as_ref() == Some(&prepared.expected_operation)
            && child == PreparedUploadObservation::Exact
        {
            return Ok(PreparedStepCommitResolution::Committed);
        }
        if operation.as_ref() == Some(&prepared.source_operation)
            && matches!(
                child,
                PreparedUploadObservation::Absent | PreparedUploadObservation::IdentifierCollision
            )
        {
            return Ok(PreparedStepCommitResolution::NotCommitted);
        }
        Err(corruption(
            "text partition operation/child commit outcome is partial or disagrees",
        ))
    }

    /// Submits object I/O only after the exact operation/child commit is durable.
    pub(crate) async fn after_commit(self) {
        let prepared = match self {
            Self::Repository(_)
            | Self::CleanupOwner { .. }
            | Self::CleanupIntent { .. }
            | Self::CompactionRetirement(_)
            | Self::ManifestRepository(_)
            | Self::ManifestPage(_)
            | Self::Validation(_) => return,
            Self::PartitionUpload(prepared)
            | Self::CatchUpUpload(prepared)
            | Self::CompactionUpload(prepared) => prepared,
        };
        let prepared = *prepared;
        if let Err(error) = prepared
            .coordinator
            .publish(&prepared.intent.permit(), prepared.payload)
            .await
        {
            tracing::warn!(
                intent_id = %prepared.intent.value().intent_id.as_uuid(),
                error = %error,
                "durable text partition upload publication remains queued for reconciliation"
            );
        }
    }
}

#[async_trait]
impl IndexOperationDriver for TextIndexDriver {
    fn family(&self) -> IndexOperationFamily {
        IndexOperationFamily::Text
    }

    async fn acquire_step_permit(
        &self,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Result<Box<dyn IndexOperationStepPermit>> {
        let needs_exclusive = matches!(
            operation.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(_))
                | IndexOperationProgress::TextCleanup(_)
        );
        if needs_exclusive {
            // Reference retirement changes the set protected by the root pass
            // and therefore owns the blob gate. `MarkReachability` and
            // `DeleteBlobs` only observe or hand off root-owned work; making
            // them reacquire that gate would deadlock the single worker lane.
            let needs_blob_deletion = matches!(
                operation.progress(),
                IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                    crate::index_v2::TextCleanupProgress::RetireManifest(_)
                        | crate::index_v2::TextCleanupProgress::RetireArtifacts(_)
                        | crate::index_v2::TextCleanupProgress::RetireUploadIntents(_)
                )) | IndexOperationProgress::TextCleanup(
                    crate::index_v2::TextCleanupProgress::RetireManifest(_)
                        | crate::index_v2::TextCleanupProgress::RetireArtifacts(_)
                        | crate::index_v2::TextCleanupProgress::RetireUploadIntents(_)
                )
            );
            if needs_blob_deletion {
                let TextPublicationRuntime::Installed(runtime) = &self.publication else {
                    return Err(HelixDbError::IndexLifecycleUnavailable {
                        family: crate::error::IndexFamily::Text,
                        reason: crate::error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
                    });
                };
                let scope_permit = self.scope_gates.exclusive_permit(scope).await;
                return Ok(Box::new(TextCleanupPermit::ScopeAndBlobDeletion {
                    _scope: scope_permit,
                    _blobs: runtime.gc_gate.acquire_deletion().await,
                }));
            }
            return Ok(Box::new(TextCleanupPermit::ScopeOnly {
                _scope: self.scope_gates.exclusive_permit(scope).await,
            }));
        }
        Ok(Box::new(()))
    }

    async fn prepare_step(
        &self,
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<PreparedIndexOperationStep> {
        let progress = operation.progress();
        if let Some(prepared) = crate::index_v2::reader_lifecycle::prepare_reader_lifecycle_step(
            self.reader_leases.as_ref(),
            scope,
            operation,
        )
        .await
        {
            let permit = self.acquire_step_permit(scope, operation).await?;
            return Ok(PreparedIndexOperationStep::reader_lifecycle(
                self.family(),
                permit,
                prepared,
            ));
        }
        let owner_retirement = match progress {
            IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                crate::index_v2::TextCleanupProgress::RetireManifest(progress),
            )) => Some((
                super::cleanup::FencedOwnerRetirementProgress::Manifest(progress.clone()),
                true,
            )),
            IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                crate::index_v2::TextCleanupProgress::RetireArtifacts(progress),
            )) => Some((
                super::cleanup::FencedOwnerRetirementProgress::Artifacts(progress.clone()),
                true,
            )),
            IndexOperationProgress::TextCleanup(
                crate::index_v2::TextCleanupProgress::RetireManifest(progress),
            ) => Some((
                super::cleanup::FencedOwnerRetirementProgress::Manifest(progress.clone()),
                false,
            )),
            IndexOperationProgress::TextCleanup(
                crate::index_v2::TextCleanupProgress::RetireArtifacts(progress),
            ) => Some((
                super::cleanup::FencedOwnerRetirementProgress::Artifacts(progress.clone()),
                false,
            )),
            IndexOperationProgress::SecondaryBuild(_)
            | IndexOperationProgress::VectorBuild(_)
            | IndexOperationProgress::TextBuild(_)
            | IndexOperationProgress::SecondaryCleanup(_)
            | IndexOperationProgress::VectorCleanup(_)
            | IndexOperationProgress::TextCleanup(_) => None,
        };
        if let Some((progress, aborting)) = owner_retirement {
            let TextPublicationRuntime::Installed(runtime) = &self.publication else {
                return Err(HelixDbError::IndexLifecycleUnavailable {
                    family: crate::error::IndexFamily::Text,
                    reason: crate::error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
                });
            };
            let permit = self.acquire_step_permit(scope, operation).await?;
            let prepared = super::cleanup::prepare_fenced_owner_retirement(
                db,
                scope,
                operation,
                progress,
                aborting,
                limits,
                runtime.coordinator.as_ref(),
            )
            .await?;
            return Ok(PreparedIndexOperationStep::text(
                PreparedTextOperationStep::CleanupOwner {
                    prepared: Box::new(prepared),
                    _permit: permit,
                },
            ));
        }
        let intent_retirement = match progress {
            IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(
                crate::index_v2::TextCleanupProgress::RetireUploadIntents(progress),
            )) => Some((progress, true)),
            IndexOperationProgress::TextCleanup(
                crate::index_v2::TextCleanupProgress::RetireUploadIntents(progress),
            ) => Some((progress, false)),
            IndexOperationProgress::SecondaryBuild(_)
            | IndexOperationProgress::VectorBuild(_)
            | IndexOperationProgress::TextBuild(_)
            | IndexOperationProgress::SecondaryCleanup(_)
            | IndexOperationProgress::VectorCleanup(_)
            | IndexOperationProgress::TextCleanup(_) => None,
        };
        if let Some((progress, aborting)) = intent_retirement {
            let TextPublicationRuntime::Installed(runtime) = &self.publication else {
                return Err(HelixDbError::IndexLifecycleUnavailable {
                    family: crate::error::IndexFamily::Text,
                    reason: crate::error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
                });
            };
            let permit = self.acquire_step_permit(scope, operation).await?;
            let prepared = super::cleanup::prepare_upload_intent_retirement(
                db,
                scope,
                operation,
                progress,
                aborting,
                limits,
                runtime.coordinator.as_ref(),
            )
            .await?;
            return Ok(PreparedIndexOperationStep::text(
                PreparedTextOperationStep::CleanupIntent {
                    prepared: Box::new(prepared),
                    _permit: permit,
                },
            ));
        }
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(stage)) = progress
        else {
            let permit = self.acquire_step_permit(scope, operation).await?;
            return Ok(PreparedIndexOperationStep::driver_owned(
                IndexOperationFamily::Text,
                permit,
            ));
        };
        match stage {
            TextBuildStage::ScanPartitions(progress) => {
                let TextPublicationRuntime::Installed(runtime) = &self.publication else {
                    return Ok(PreparedIndexOperationStep::text(
                        PreparedTextOperationStep::Repository(Box::new(
                            PreparedTextRepositoryStep {
                                source_operation: operation.clone(),
                                expected_reads: Vec::new(),
                                writes: Vec::new(),
                                result: IndexOperationStepResult::TransientFailure,
                            },
                        )),
                    ));
                };
                let step = prepare_partition_step(
                    db,
                    scope,
                    operation,
                    progress,
                    limits,
                    Arc::clone(&runtime.coordinator),
                )
                .await?;
                Ok(PreparedIndexOperationStep::text(step))
            }
            TextBuildStage::CatchUp(progress) => {
                let TextPublicationRuntime::Installed(runtime) = &self.publication else {
                    return Ok(PreparedIndexOperationStep::driver_owned(
                        IndexOperationFamily::Text,
                        Box::new(()),
                    ));
                };
                let Some(step) = prepare_catch_up_step(
                    db,
                    scope,
                    operation,
                    progress,
                    limits,
                    Arc::clone(&runtime.coordinator),
                )
                .await?
                else {
                    return Ok(PreparedIndexOperationStep::driver_owned(
                        IndexOperationFamily::Text,
                        Box::new(()),
                    ));
                };
                Ok(PreparedIndexOperationStep::text(step))
            }
            TextBuildStage::Compact(progress) => {
                let TextPublicationRuntime::Installed(runtime) = &self.publication else {
                    return Ok(PreparedIndexOperationStep::text(
                        PreparedTextOperationStep::Repository(Box::new(
                            PreparedTextRepositoryStep {
                                source_operation: operation.clone(),
                                expected_reads: Vec::new(),
                                writes: Vec::new(),
                                result: IndexOperationStepResult::TransientFailure,
                            },
                        )),
                    ));
                };
                let step = prepare_compaction_step(db, scope, operation, progress, limits, runtime)
                    .await?;
                Ok(PreparedIndexOperationStep::text(step))
            }
            TextBuildStage::PrepareManifests(progress) => {
                let TextPublicationRuntime::Installed(runtime) = &self.publication else {
                    return Ok(PreparedIndexOperationStep::text(
                        PreparedTextOperationStep::Repository(Box::new(
                            PreparedTextRepositoryStep {
                                source_operation: operation.clone(),
                                expected_reads: Vec::new(),
                                writes: Vec::new(),
                                result: IndexOperationStepResult::TransientFailure,
                            },
                        )),
                    ));
                };
                let step =
                    prepare_manifest_step(db, scope, operation, progress, limits, runtime).await?;
                Ok(PreparedIndexOperationStep::text(step))
            }
            TextBuildStage::ValidateManifests(progress) => {
                let TextPublicationRuntime::Installed(runtime) = &self.publication else {
                    return Ok(PreparedIndexOperationStep::text(
                        PreparedTextOperationStep::Repository(Box::new(
                            PreparedTextRepositoryStep {
                                source_operation: operation.clone(),
                                expected_reads: Vec::new(),
                                writes: Vec::new(),
                                result: IndexOperationStepResult::TransientFailure,
                            },
                        )),
                    ));
                };
                let step = prepare_validation_step(db, scope, operation, progress, limits, runtime)
                    .await?;
                Ok(PreparedIndexOperationStep::text(step))
            }
            TextBuildStage::ScanSource(_)
            | TextBuildStage::AwaitUpload(_)
            | TextBuildStage::AwaitCatchUpUpload(_)
            | TextBuildStage::AwaitCompactionUpload(_)
            | TextBuildStage::Activate(_) => Ok(PreparedIndexOperationStep::driver_owned(
                IndexOperationFamily::Text,
                Box::new(()),
            )),
        }
    }

    async fn step(
        &self,
        _db: &slatedb::Db,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<IndexOperationStepResult> {
        let record = load_operation_index(transaction, scope, operation).await?;
        let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
            return Err(corruption("text operation loaded another family"));
        };
        match operation.progress() {
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ScanSource(progress),
            )) => scan_source(transaction, scope, operation, definition, progress, limits).await,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::AwaitUpload(progress),
            )) => reconcile_await_upload(transaction, scope, operation, progress).await,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::AwaitCatchUpUpload(progress),
            )) => reconcile_await_catch_up_upload(transaction, scope, operation, progress).await,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::AwaitCompactionUpload(progress),
            )) => reconcile_await_compaction_upload(transaction, scope, operation, progress).await,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(progress),
            )) => catch_up(transaction, scope, operation, definition, progress, limits).await,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Activate(progress),
            )) => activate(transaction, scope, operation, progress.counters).await,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ScanPartitions(_)
                | TextBuildStage::Compact(_)
                | TextBuildStage::PrepareManifests(_)
                | TextBuildStage::ValidateManifests(_),
            )) => Ok(IndexOperationStepResult::TransientFailure),
            IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(progress)) => {
                super::cleanup::step_cleanup(transaction, scope, operation, progress, true, limits)
                    .await
            }
            IndexOperationProgress::TextCleanup(progress) => {
                super::cleanup::step_cleanup(transaction, scope, operation, progress, false, limits)
                    .await
            }
            IndexOperationProgress::SecondaryBuild(_)
            | IndexOperationProgress::VectorBuild(_)
            | IndexOperationProgress::SecondaryCleanup(_)
            | IndexOperationProgress::VectorCleanup(_) => {
                Err(corruption("text driver received another family"))
            }
        }
    }
}

/// Returns whether a queued `AwaitUpload` operation must remain unclaimed.
///
/// Claiming an operation increments its revision. A durable child names the
/// queued `AwaitUpload` revision exactly, so the operation worker must leave
/// that revision untouched until upload reconciliation removes both child
/// anchors. Partial anchors or ownership disagreement are corruption rather
/// than permission to claim and infer progress from blob presence.
pub(crate) async fn await_upload_child_is_pending(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Result<bool> {
    let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(stage)) =
        operation.progress()
    else {
        return Ok(false);
    };
    let (intent_id, artifact_cursor) = match stage {
        TextBuildStage::AwaitUpload(progress) => (progress.intent_id(), progress.artifact_key()),
        TextBuildStage::AwaitCatchUpUpload(progress) => {
            (progress.intent_id(), progress.artifact_key())
        }
        TextBuildStage::AwaitCompactionUpload(progress) => {
            (progress.intent_id(), progress.artifact_key())
        }
        TextBuildStage::ScanSource(_)
        | TextBuildStage::ScanPartitions(_)
        | TextBuildStage::CatchUp(_)
        | TextBuildStage::Compact(_)
        | TextBuildStage::PrepareManifests(_)
        | TextBuildStage::ValidateManifests(_)
        | TextBuildStage::Activate(_) => return Ok(false),
    };
    if !matches!(
        operation.execution_state(),
        IndexOperationExecutionState::Queued { .. }
    ) {
        return Err(corruption(
            "text AwaitUpload child gate requires an exact queued operation",
        ));
    }

    let intent_key = scoped_index_key(
        scope,
        IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            intent_id,
        }),
    );
    let pointer_key = Key::Global {
        kind: GlobalKeyKind::IndexV2(GlobalIndexV2Key::UploadPointer(intent_id)),
    }
    .to_bytes();
    let intent_value = transaction.get(intent_key).await?;
    let pointer_value = transaction.get(pointer_key).await?;
    let (intent_value, pointer_value) = match (intent_value, pointer_value) {
        (Some(intent_value), Some(pointer_value)) => (intent_value, pointer_value),
        (None, None) => return Ok(false),
        (Some(_), None) | (None, Some(_)) => {
            return Err(corruption(
                "text AwaitUpload child intent and pointer are not an atomic pair",
            ));
        }
    };
    let IndexV2WorkValue::TextUploadIntent(intent) = decode_work_value(&intent_value)? else {
        return Err(corruption(
            "text AwaitUpload child key contains another work value",
        ));
    };
    let IndexV2MetadataValue::UploadQueuePointer(pointer) = decode_metadata_value(&pointer_value)?
    else {
        return Err(corruption(
            "text AwaitUpload child pointer contains another metadata value",
        ));
    };
    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(artifact_key)),
        ..
    } = Key::parse_from_slice(scope, artifact_cursor.as_bytes())?
    else {
        return Err(corruption(
            "text AwaitUpload checkpoint contains another artifact key kind",
        ));
    };
    let work::TextUploadOwner::Build {
        operation_id,
        expected_operation_revision,
    } = intent.owner
    else {
        return Err(corruption(
            "text AwaitUpload child is owned by an active mutation",
        ));
    };
    let work::TextUploadAttachment::BuildArtifact {
        artifact_ordinal,
        split,
    } = intent.attachment
    else {
        return Err(corruption(
            "text AwaitUpload child targets a manifest destination",
        ));
    };
    if pointer.scope != scope
        || pointer.index_id != operation.index_id()
        || pointer.generation != operation.generation()
        || pointer.record_revision != intent.revision
        || intent.intent_id != intent_id
        || intent.index_id != operation.index_id()
        || &intent.identity != operation.identity()
        || intent.generation != operation.generation()
        || operation_id != operation.operation_id()
        || expected_operation_revision != operation.operation_revision()
        || artifact_key.root.index_id != operation.index_id()
        || artifact_key.root.generation != operation.generation()
        || artifact_key.root.partition != intent.partition.fingerprint()
        || artifact_key.ordinal != artifact_ordinal
        || intent.blob != split.blob()
        || matches!(intent.work_state, work::TextUploadWorkState::Blocked(_))
    {
        return Err(corruption(
            "text AwaitUpload operation, child, pointer, and artifact owner disagree",
        ));
    }
    Ok(true)
}

/// Resolves an upload-free wait checkpoint from its one persisted artifact key.
///
/// Child presence after claim is corruption because the pre-claim gate must
/// preserve the child-owned operation revision. Exact artifact ownership
/// advances to the completed kind-`0x0C` cursor; exact absence retries from the
/// retained pre-upload cursor without scanning the generation for evidence.
async fn reconcile_await_upload(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &TextBuildUploadProgress,
) -> Result<IndexOperationStepResult> {
    let intent_key = scoped_index_key(
        scope,
        IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            intent_id: progress.intent_id(),
        }),
    );
    let pointer_key = Key::Global {
        kind: GlobalKeyKind::IndexV2(GlobalIndexV2Key::UploadPointer(progress.intent_id())),
    }
    .to_bytes();
    if transaction.get(intent_key).await?.is_some() || transaction.get(pointer_key).await?.is_some()
    {
        return Err(corruption(
            "text AwaitUpload operation was claimed while its child remained durable",
        ));
    }

    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(artifact_key)),
        ..
    } = Key::parse_from_slice(scope, progress.artifact_key().as_bytes())?
    else {
        return Err(corruption(
            "text AwaitUpload checkpoint contains another artifact key kind",
        ));
    };
    let Some(value) = transaction.get(progress.artifact_key().as_bytes()).await? else {
        return Ok(progressed_build(TextBuildStage::ScanPartitions(
            progress.source().clone(),
        )));
    };
    let IndexV2WorkValue::TextBuildArtifact(artifact) = decode_work_value(&value)? else {
        return Err(corruption(
            "text AwaitUpload artifact key contains another work value",
        ));
    };
    if artifact.index_id != operation.index_id()
        || artifact.generation != operation.generation()
        || artifact.partition.fingerprint() != artifact_key.root.partition
        || artifact.artifact_ordinal != artifact_key.ordinal
        || artifact.source_intent_id != progress.intent_id()
    {
        return Err(corruption(
            "text AwaitUpload artifact disagrees with its exact checkpoint owner",
        ));
    }
    Ok(progressed_build(TextBuildStage::ScanPartitions(
        progress.completed_source(),
    )))
}

/// Resolves one upload-free catch-up wait from its exact delta and artifact keys.
///
/// The catch-up transaction has already applied entity state and removed the
/// coalesced delta atomically with the child. Definitive child absence restores
/// that exact delta before resuming. A matching artifact keeps the applied state
/// and resumes from the prefix beginning so mutations coalesced after the child
/// commit remain observable.
async fn reconcile_await_catch_up_upload(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &TextCatchUpUploadProgress,
) -> Result<IndexOperationStepResult> {
    let intent_key = scoped_index_key(
        scope,
        IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            intent_id: progress.intent_id(),
        }),
    );
    let pointer_key = Key::Global {
        kind: GlobalKeyKind::IndexV2(GlobalIndexV2Key::UploadPointer(progress.intent_id())),
    }
    .to_bytes();
    if transaction.get(intent_key).await?.is_some() || transaction.get(pointer_key).await?.is_some()
    {
        return Err(corruption(
            "text AwaitCatchUpUpload operation was claimed while its child remained durable",
        ));
    }

    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::BuildDelta(delta_key)),
        ..
    } = Key::parse_from_slice(scope, progress.delta_key().as_bytes())?
    else {
        return Err(corruption(
            "text AwaitCatchUpUpload checkpoint contains another delta key kind",
        ));
    };
    if delta_key.index_id != operation.index_id()
        || delta_key.generation != operation.generation()
        || delta_key.entity.kind != operation.identity().element_kind()
    {
        return Err(corruption(
            "text AwaitCatchUpUpload delta disagrees with its operation owner",
        ));
    }
    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(artifact_key)),
        ..
    } = Key::parse_from_slice(scope, progress.artifact_key().as_bytes())?
    else {
        return Err(corruption(
            "text AwaitCatchUpUpload checkpoint contains another artifact key kind",
        ));
    };
    let Some(value) = transaction.get(progress.artifact_key().as_bytes()).await? else {
        let value = encode_work_value(&IndexV2WorkValue::CoalescedBuildDelta(
            CoalescedBuildDeltaValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                entity_kind: delta_key.entity.kind,
                entity_id: delta_key.entity.id,
            },
        ));
        match transaction.get(progress.delta_key().as_bytes()).await? {
            Some(existing) if existing != value => {
                return Err(corruption(
                    "text catch-up retry found a disagreeing coalesced delta",
                ));
            }
            Some(_) => {}
            None => transaction.put(progress.delta_key().as_bytes(), value)?,
        }
        return Ok(progressed_build(TextBuildStage::CatchUp(
            progress.completed_catch_up(),
        )));
    };
    let IndexV2WorkValue::TextBuildArtifact(artifact) = decode_work_value(&value)? else {
        return Err(corruption(
            "text AwaitCatchUpUpload artifact key contains another work value",
        ));
    };
    if artifact.index_id != operation.index_id()
        || artifact.generation != operation.generation()
        || artifact.partition.fingerprint() != artifact_key.root.partition
        || artifact.artifact_ordinal != artifact_key.ordinal
        || artifact.source_intent_id != progress.intent_id()
    {
        return Err(corruption(
            "text AwaitCatchUpUpload artifact disagrees with its exact checkpoint owner",
        ));
    }
    Ok(progressed_build(TextBuildStage::CatchUp(
        progress.completed_catch_up(),
    )))
}

/// Resolves one upload-free compaction checkpoint and retires its exact inputs.
///
/// Definitive non-publication leaves every input owner/reference untouched and
/// retries from the retained compaction prefix. A matching replacement first
/// proves its exact artifact ownership, then atomically materializes candidates
/// and removes only the retained input artifacts plus reachability rows.
async fn reconcile_await_compaction_upload(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &TextCompactionUploadProgress,
) -> Result<IndexOperationStepResult> {
    let intent_key = scoped_index_key(
        scope,
        IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            intent_id: progress.intent_id(),
        }),
    );
    let pointer_key = Key::Global {
        kind: GlobalKeyKind::IndexV2(GlobalIndexV2Key::UploadPointer(progress.intent_id())),
    }
    .to_bytes();
    if transaction.get(intent_key).await?.is_some() || transaction.get(pointer_key).await?.is_some()
    {
        return Err(corruption(
            "text AwaitCompactionUpload operation was claimed while its child remained durable",
        ));
    }

    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(output_key)),
        ..
    } = Key::parse_from_slice(scope, progress.artifact_key().as_bytes())?
    else {
        return Err(corruption(
            "text AwaitCompactionUpload checkpoint contains another output key kind",
        ));
    };
    let Some(output_value) = transaction.get(progress.artifact_key().as_bytes()).await? else {
        return Ok(progressed_build(TextBuildStage::Compact(
            progress.compact().clone(),
        )));
    };
    let IndexV2WorkValue::TextBuildArtifact(output) = decode_work_value(&output_value)? else {
        return Err(corruption(
            "text AwaitCompactionUpload output key contains another work value",
        ));
    };
    let Some(first_input) = progress.input_artifact_keys().first() else {
        return Err(corruption(
            "text AwaitCompactionUpload lost its useful input set",
        ));
    };
    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(first_input_key)),
        ..
    } = Key::parse_from_slice(scope, first_input.as_bytes())?
    else {
        return Err(corruption(
            "text AwaitCompactionUpload contains another input key kind",
        ));
    };
    if output.index_id != operation.index_id()
        || output.generation != operation.generation()
        || output.partition.fingerprint() != output_key.root.partition
        || output_key.root.partition != first_input_key.root.partition
        || output.artifact_ordinal != output_key.ordinal
        || output.source_intent_id != progress.intent_id()
    {
        return Err(corruption(
            "text AwaitCompactionUpload replacement disagrees with its exact owner",
        ));
    }
    super::compaction::stage_input_retirement(
        transaction,
        scope,
        operation,
        progress.input_artifact_keys(),
    )
    .await?;
    Ok(progressed_build(TextBuildStage::Compact(
        progress.completed_compaction(),
    )))
}

/// One admitted partition run materialized from a short-lived read snapshot.
struct PartitionDocuments {
    empty_root: PreparedEmptyManifestRoot,
    partition: TextPartition,
    documents: Vec<crate::search::text::TextDocumentInput>,
    completed_cursor: IndexCursor,
    completed_counters: OperationCounters,
}

/// Closed partition-scan decision with all writes needed by its transition.
enum PartitionScanSelection {
    Repository {
        empty_root: Option<PreparedEmptyManifestRoot>,
        result: IndexOperationStepResult,
    },
    Upload(PartitionDocuments),
}

/// Closed resume target selected before constructing one immutable split.
enum PreparedTextUploadSource {
    Partition {
        progress: SourceScanProgress,
        completed_cursor: IndexCursor,
    },
    CatchUp {
        progress: PrefixScanProgress,
        delta_key: IndexCursor,
    },
}

/// Complete split input whose reads/writes remain bound to one operation claim.
struct PreparedTextSplitInput {
    partition: TextPartition,
    documents: Vec<crate::search::text::TextDocumentInput>,
    completed_counters: OperationCounters,
    source: PreparedTextUploadSource,
    expected_reads: Vec<PreparedTextExpectedRead>,
    lifecycle_writes: Vec<PreparedTextWrite>,
}

/// Prepares one partition-ordered step without retaining a database snapshot.
async fn prepare_partition_step(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
    coordinator: Arc<dyn BlobPublicationCoordinator>,
) -> Result<PreparedTextOperationStep> {
    let IndexOperationExecutionState::Claimed(_) = operation.execution_state() else {
        return Err(corruption(
            "text partition preparation requires an exact claimed operation",
        ));
    };
    let snapshot = db.begin(IsolationLevel::Snapshot).await?;
    let record = load_operation_index(&snapshot, scope, operation).await?;
    let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
        return Err(corruption(
            "text partition preparation loaded another family",
        ));
    };
    let prepared =
        scan_partition_documents(&snapshot, scope, operation, definition, progress, limits).await?;
    drop(snapshot);

    let documents = match prepared {
        PartitionScanSelection::Repository { empty_root, result } => {
            let (expected_reads, writes) = match empty_root {
                Some(root) => {
                    let (read, write) = root.into_parts();
                    (vec![read], write.into_iter().collect())
                }
                None => (Vec::new(), Vec::new()),
            };
            return Ok(PreparedTextOperationStep::Repository(Box::new(
                PreparedTextRepositoryStep {
                    source_operation: operation.clone(),
                    expected_reads,
                    writes,
                    result,
                },
            )));
        }
        PartitionScanSelection::Upload(documents) => documents,
    };
    let (root_read, root_write) = documents.empty_root.into_parts();
    prepare_build_upload(
        operation,
        scope,
        definition,
        limits,
        coordinator,
        PreparedTextSplitInput {
            partition: documents.partition,
            documents: documents.documents,
            completed_counters: documents.completed_counters,
            source: PreparedTextUploadSource::Partition {
                progress: progress.clone(),
                completed_cursor: documents.completed_cursor,
            },
            expected_reads: vec![root_read],
            lifecycle_writes: root_write.into_iter().collect(),
        },
    )
    .await
}

/// Constructs and reserves one exact split without retaining a database view.
async fn prepare_build_upload(
    operation: &IndexOperationRecord,
    scope: DataScope,
    definition: &ValidatedTextIndexDefinition,
    limits: SearchIndexBatchLimits,
    coordinator: Arc<dyn BlobPublicationCoordinator>,
    input: PreparedTextSplitInput,
) -> Result<PreparedTextOperationStep> {
    let IndexOperationExecutionState::Claimed(claim) = operation.execution_state() else {
        return Err(corruption(
            "text split preparation requires an exact claimed operation",
        ));
    };
    let unpublished =
        crate::search::text::build_documents_as_split(&definition.to_runtime(), &input.documents)?
            .ok_or_else(|| corruption("non-empty text build batch produced no split"))?;
    let (payload, runtime_split) = unpublished.into_parts();
    let payload_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    if payload_bytes > limits.max_output_bytes().get() {
        return Ok(PreparedTextOperationStep::Repository(Box::new(
            PreparedTextRepositoryStep {
                source_operation: operation.clone(),
                expected_reads: input.expected_reads.clone(),
                writes: Vec::new(),
                result: IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                    partition: input.partition,
                    observed: payload_bytes,
                    limit: limits.max_output_bytes().get(),
                }),
            },
        )));
    }

    let split = work::SplitRef::try_new(
        work::BlobRef::new(runtime_split.blob.sha256, runtime_split.blob.size_bytes),
        runtime_split.footer_offset,
        runtime_split.footer_len,
        runtime_split.hotcache_len,
        runtime_split.total_size_bytes,
    )
    .map_err(work_error)?;
    let artifact_ordinal = match u32::try_from(input.completed_counters.output_operations) {
        Ok(ordinal) => ordinal,
        Err(_) => {
            return Ok(PreparedTextOperationStep::Repository(Box::new(
                PreparedTextRepositoryStep {
                    source_operation: operation.clone(),
                    expected_reads: input.expected_reads.clone(),
                    writes: Vec::new(),
                    result: IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::ManifestLimit {
                            partition: input.partition,
                            observed: input.completed_counters.output_operations,
                            limit: u64::from(u32::MAX),
                        },
                    ),
                },
            )));
        }
    };
    let intent_id = TextUploadIntentId::new_v4();
    let artifact_owner = TextBuildArtifactKey {
        root: TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: input.partition.fingerprint(),
        },
        ordinal: artifact_ordinal,
    };
    let artifact_key = scoped_index_key(scope, IndexV2Key::TextBuildArtifact(artifact_owner));
    let artifact_value = encode_work_value(&IndexV2WorkValue::TextBuildArtifact(
        work::TextBuildArtifactValue {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: input.partition.clone(),
            artifact_ordinal,
            split,
            source_intent_id: intent_id,
        },
    ));
    let artifact_bytes =
        u64::try_from(artifact_key.len().saturating_add(artifact_value.len())).unwrap_or(u64::MAX);
    if artifact_bytes > limits.max_output_bytes().get() {
        return Ok(PreparedTextOperationStep::Repository(Box::new(
            PreparedTextRepositoryStep {
                source_operation: operation.clone(),
                expected_reads: input.expected_reads.clone(),
                writes: Vec::new(),
                result: IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                    partition: input.partition,
                    observed: artifact_bytes,
                    limit: limits.max_output_bytes().get(),
                }),
            },
        )));
    }
    let completed_counters = OperationCounters {
        entities: input.completed_counters.entities,
        input_bytes: input.completed_counters.input_bytes,
        output_operations: checked_add(
            input.completed_counters.output_operations,
            1,
            "cumulative output operations",
        )?,
        output_bytes: checked_add(
            input.completed_counters.output_bytes,
            artifact_bytes,
            "cumulative output bytes",
        )?,
    };
    let is_catch_up = matches!(&input.source, PreparedTextUploadSource::CatchUp { .. });
    let artifact_cursor = IndexCursor::try_new(artifact_key.clone()).map_err(operation_error)?;
    let next_stage = match &input.source {
        PreparedTextUploadSource::Partition {
            progress,
            completed_cursor,
        } => TextBuildStage::AwaitUpload(
            TextBuildUploadProgress::try_new(
                progress.clone(),
                completed_cursor.clone(),
                completed_counters,
                artifact_cursor,
                intent_id,
            )
            .map_err(operation_error)?,
        ),
        PreparedTextUploadSource::CatchUp {
            progress,
            delta_key,
        } => TextBuildStage::AwaitCatchUpUpload(
            TextCatchUpUploadProgress::try_new(
                progress.clone(),
                delta_key.clone(),
                completed_counters,
                artifact_cursor,
                intent_id,
            )
            .map_err(operation_error)?,
        ),
    };
    let next_progress =
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(next_stage));
    let expected_operation = operation
        .progressed(next_progress.clone())
        .map_err(operation_error)?;
    let owner = work::TextUploadOwner::Build {
        operation_id: operation.operation_id(),
        expected_operation_revision: expected_operation.operation_revision(),
    };
    let attachment = work::TextUploadAttachment::BuildArtifact {
        artifact_ordinal,
        split,
    };
    let spec = work::TextUploadSpec::try_new(
        operation.index_id(),
        operation.identity().clone(),
        operation.generation(),
        input.partition,
        split.blob(),
        owner,
        attachment,
    )
    .map_err(work_error)?;
    let permit = coordinator
        .reserve(split.blob(), intent_id, claim.writer_epoch)
        .await
        .map_err(coordinator_error)?;
    let intent = PreparedTextUploadIntent::from_spec(intent_id, permit, spec);
    let prepared = Box::new(PreparedTextBuildUpload {
        coordinator,
        source_operation: operation.clone(),
        expected_operation,
        progress: next_progress,
        artifact_key,
        expected_reads: input.expected_reads,
        lifecycle_writes: input.lifecycle_writes,
        intent,
        payload,
    });
    Ok(if is_catch_up {
        PreparedTextOperationStep::CatchUpUpload(prepared)
    } else {
        PreparedTextOperationStep::PartitionUpload(prepared)
    })
}

/// Prepares one bounded compaction decision without retaining a database view.
///
/// Artifact selection and entity-version resolution use separate short-lived
/// snapshots around object materialization. Their exact observations are held
/// until repository dispatch, so a concurrent artifact/state change yields a
/// transient retry before either source retirement or child creation commits.
async fn prepare_compaction_step(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    batch_limits: SearchIndexBatchLimits,
    runtime: &TextPublicationDependencies,
) -> Result<PreparedTextOperationStep> {
    let IndexOperationExecutionState::Claimed(claim) = operation.execution_state() else {
        return Err(corruption(
            "text compaction preparation requires an exact claimed operation",
        ));
    };
    let snapshot = db.begin(IsolationLevel::Snapshot).await?;
    let record = load_operation_index(&snapshot, scope, operation).await?;
    let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
        return Err(corruption(
            "text compaction preparation loaded another family",
        ));
    };
    let definition = definition.clone();
    let selection = super::compaction::select_artifacts(
        &snapshot,
        scope,
        operation,
        progress,
        batch_limits,
        runtime.compaction_limits,
    )
    .await?;
    drop(snapshot);

    let selected = match selection {
        super::compaction::ArtifactSelection::Exhausted => {
            return Ok(PreparedTextOperationStep::Repository(Box::new(
                PreparedTextRepositoryStep {
                    source_operation: operation.clone(),
                    expected_reads: Vec::new(),
                    writes: Vec::new(),
                    result: progressed_build(TextBuildStage::PrepareManifests(
                        PrefixScanProgress {
                            cursor: None,
                            counters: progress.counters,
                        },
                    )),
                },
            )));
        }
        super::compaction::ArtifactSelection::Advance {
            cursor,
            observation,
        } => {
            return Ok(PreparedTextOperationStep::Repository(Box::new(
                PreparedTextRepositoryStep {
                    source_operation: operation.clone(),
                    expected_reads: vec![PreparedTextExpectedRead {
                        key: observation.key,
                        value: observation.value,
                    }],
                    writes: Vec::new(),
                    result: progressed_build(TextBuildStage::Compact(PrefixScanProgress {
                        cursor: Some(cursor),
                        counters: progress.counters,
                    })),
                },
            )));
        }
        super::compaction::ArtifactSelection::Compact(selected) => selected,
    };

    let physical_index_name = format!(
        "v2-text-{}-{}-{:02x?}",
        operation.index_id().get(),
        operation.generation().get(),
        selected.partition.fingerprint().as_bytes(),
    );
    let prepared = crate::search::text::compaction::prepare_text_build_compaction(
        &runtime.object_store,
        &runtime.db_path,
        &definition.to_runtime(),
        &physical_index_name,
        &selected.split_refs,
        runtime.compaction_limits,
    )
    .await
    .map_err(compaction_error)?;
    if prepared.input_bytes().get() != selected.input_blob_bytes {
        return Err(corruption(
            "text compaction materialization disagrees with selected input bytes",
        ));
    }

    let snapshot = db.begin(IsolationLevel::Snapshot).await?;
    let current_record = load_operation_index(&snapshot, scope, operation).await?;
    if current_record.definition() != &ValidatedDynamicIndexDefinition::Text(definition.clone()) {
        return Err(corruption(
            "text compaction definition changed within one operation revision",
        ));
    }
    let resolved = super::compaction::resolve_live_versions(
        &snapshot,
        scope,
        operation,
        &selected.partition,
        prepared.document_versions(),
    )
    .await?;
    drop(snapshot);

    let mut expected_reads = selected
        .observations
        .into_iter()
        .chain(resolved.observations)
        .map(|observation| PreparedTextExpectedRead {
            key: observation.key,
            value: observation.value,
        })
        .collect::<Vec<_>>();
    let unpublished = match prepared.finish(resolved.live_versions).await {
        Ok(unpublished) => unpublished,
        Err(crate::search::text::compaction::TextBuildCompactionError::OutputBlobExceeded {
            required,
            limit,
        }) => {
            return Ok(PreparedTextOperationStep::Repository(Box::new(
                PreparedTextRepositoryStep {
                    source_operation: operation.clone(),
                    expected_reads,
                    writes: Vec::new(),
                    result: IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::ManifestLimit {
                            partition: selected.partition,
                            observed: required.get(),
                            limit: limit.get(),
                        },
                    ),
                },
            )));
        }
        Err(error) => return Err(compaction_error(error)),
    };
    let completed_input_bytes = checked_add(
        progress.counters.input_bytes,
        selected.input_blob_bytes,
        "compaction input bytes",
    )?;
    let completed_retirement_operations = checked_add(
        progress.counters.output_operations,
        selected.retirement_output_operations,
        "compaction retirement operations",
    )?;
    let completed_retirement_bytes = checked_add(
        progress.counters.output_bytes,
        selected.retirement_output_bytes,
        "compaction retirement bytes",
    )?;
    let Some(unpublished) = unpublished else {
        let next_progress = IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::Compact(PrefixScanProgress {
                cursor: progress.cursor.clone(),
                counters: OperationCounters {
                    entities: progress.counters.entities,
                    input_bytes: completed_input_bytes,
                    output_operations: completed_retirement_operations,
                    output_bytes: completed_retirement_bytes,
                },
            }),
        ));
        return Ok(PreparedTextOperationStep::CompactionRetirement(Box::new(
            PreparedTextCompactionRetirement {
                source_operation: operation.clone(),
                expected_reads,
                input_artifact_keys: selected.artifact_keys,
                progress: next_progress,
            },
        )));
    };

    let (payload, runtime_split) = unpublished.into_parts();
    let split = work::SplitRef::try_new(
        work::BlobRef::new(runtime_split.blob.sha256, runtime_split.blob.size_bytes),
        runtime_split.footer_offset,
        runtime_split.footer_len,
        runtime_split.hotcache_len,
        runtime_split.total_size_bytes,
    )
    .map_err(work_error)?;
    let artifact_ordinal = match u32::try_from(progress.counters.output_operations) {
        Ok(ordinal) => ordinal,
        Err(_) => {
            return Ok(PreparedTextOperationStep::Repository(Box::new(
                PreparedTextRepositoryStep {
                    source_operation: operation.clone(),
                    expected_reads,
                    writes: Vec::new(),
                    result: IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::ManifestLimit {
                            partition: selected.partition,
                            observed: progress.counters.output_operations,
                            limit: u64::from(u32::MAX),
                        },
                    ),
                },
            )));
        }
    };
    let intent_id = TextUploadIntentId::new_v4();
    let artifact_owner = TextBuildArtifactKey {
        root: TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: selected.partition.fingerprint(),
        },
        ordinal: artifact_ordinal,
    };
    let artifact_key = scoped_index_key(scope, IndexV2Key::TextBuildArtifact(artifact_owner));
    let artifact_value = encode_work_value(&IndexV2WorkValue::TextBuildArtifact(
        work::TextBuildArtifactValue {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: selected.partition.clone(),
            artifact_ordinal,
            split,
            source_intent_id: intent_id,
        },
    ));
    let artifact_bytes =
        u64::try_from(artifact_key.len().saturating_add(artifact_value.len())).unwrap_or(u64::MAX);
    if artifact_bytes > batch_limits.max_output_bytes().get() {
        return Ok(PreparedTextOperationStep::Repository(Box::new(
            PreparedTextRepositoryStep {
                source_operation: operation.clone(),
                expected_reads,
                writes: Vec::new(),
                result: IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                    partition: selected.partition,
                    observed: artifact_bytes,
                    limit: batch_limits.max_output_bytes().get(),
                }),
            },
        )));
    }
    let completed_counters = OperationCounters {
        entities: progress.counters.entities,
        input_bytes: completed_input_bytes,
        output_operations: checked_add(
            completed_retirement_operations,
            1,
            "compaction replacement operation",
        )?,
        output_bytes: checked_add(
            completed_retirement_bytes,
            artifact_bytes,
            "compaction replacement bytes",
        )?,
    };
    let artifact_cursor = IndexCursor::try_new(artifact_key.clone()).map_err(operation_error)?;
    let next_stage = TextBuildStage::AwaitCompactionUpload(
        TextCompactionUploadProgress::try_new(
            progress.clone(),
            selected.artifact_keys,
            completed_counters,
            artifact_cursor,
            intent_id,
        )
        .map_err(operation_error)?,
    );
    let next_progress =
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(next_stage));
    let expected_operation = operation
        .progressed(next_progress.clone())
        .map_err(operation_error)?;
    let owner = work::TextUploadOwner::Build {
        operation_id: operation.operation_id(),
        expected_operation_revision: expected_operation.operation_revision(),
    };
    let attachment = work::TextUploadAttachment::BuildArtifact {
        artifact_ordinal,
        split,
    };
    let spec = work::TextUploadSpec::try_new(
        operation.index_id(),
        operation.identity().clone(),
        operation.generation(),
        selected.partition,
        split.blob(),
        owner,
        attachment,
    )
    .map_err(work_error)?;
    let permit = runtime
        .coordinator
        .reserve(split.blob(), intent_id, claim.writer_epoch)
        .await
        .map_err(coordinator_error)?;
    let intent = PreparedTextUploadIntent::from_spec(intent_id, permit, spec);
    expected_reads.shrink_to_fit();
    Ok(PreparedTextOperationStep::CompactionUpload(Box::new(
        PreparedTextBuildUpload {
            coordinator: Arc::clone(&runtime.coordinator),
            source_operation: operation.clone(),
            expected_operation,
            progress: next_progress,
            artifact_key,
            expected_reads,
            lifecycle_writes: Vec::new(),
            intent,
            payload,
        },
    )))
}

/// Converts physical compaction failures into retryable I/O or durable corruption.
fn compaction_error(
    error: crate::search::text::compaction::TextBuildCompactionError,
) -> HelixDbError {
    match error {
        crate::search::text::compaction::TextBuildCompactionError::Database(error) => error,
        error @ (crate::search::text::compaction::TextBuildCompactionError::TooFewInputSplits
        | crate::search::text::compaction::TextBuildCompactionError::FanInExceeded { .. }
        | crate::search::text::compaction::TextBuildCompactionError::InputSplitBytesEmpty
        | crate::search::text::compaction::TextBuildCompactionError::InputBytesExceeded { .. }
        | crate::search::text::compaction::TextBuildCompactionError::TemporaryDiskExceeded {
            ..
        }
        | crate::search::text::compaction::TextBuildCompactionError::OutputBlobEmpty
        | crate::search::text::compaction::TextBuildCompactionError::OutputBlobExceeded { .. }
        | crate::search::text::compaction::TextBuildCompactionError::DuplicateDocumentVersion {
            ..
        }
        | crate::search::text::compaction::TextBuildCompactionError::MeasurementOverflow) => {
            corruption(format!("invalid text compaction input or capacity: {error}"))
        }
    }
}

/// Prepares one bounded artifact-to-manifest-page relocation.
///
/// Database selection completes before acquiring external reference authority.
/// The local shared gate is acquired before coordinator guards and all of them
/// remain inside the returned closed step until commit, abort, or commit-error
/// resolution drops it.
async fn prepare_manifest_step(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    batch_limits: SearchIndexBatchLimits,
    runtime: &TextPublicationDependencies,
) -> Result<PreparedTextOperationStep> {
    let snapshot = db.begin(IsolationLevel::Snapshot).await?;
    let selection = super::manifest::select_page(
        &snapshot,
        scope,
        operation,
        progress,
        batch_limits,
        runtime.compaction_limits,
    )
    .await?;
    drop(snapshot);
    let prepared = match selection {
        super::manifest::ManifestSelection::Exhausted(range) => {
            return Ok(PreparedTextOperationStep::ManifestRepository(Box::new(
                PreparedTextManifestRepositoryStep {
                    source_operation: operation.clone(),
                    range,
                    expected_reads: Vec::new(),
                    result: progressed_build(TextBuildStage::ValidateManifests(
                        TextManifestValidationProgress::initial(progress.counters),
                    )),
                },
            )));
        }
        super::manifest::ManifestSelection::Blocked {
            blocker,
            range,
            observations,
        } => {
            return Ok(PreparedTextOperationStep::ManifestRepository(Box::new(
                PreparedTextManifestRepositoryStep {
                    source_operation: operation.clone(),
                    range,
                    expected_reads: observations
                        .into_iter()
                        .map(|observation| PreparedTextExpectedRead {
                            key: observation.key,
                            value: observation.value,
                        })
                        .collect(),
                    result: IndexOperationStepResult::Blocked(blocker),
                },
            )));
        }
        super::manifest::ManifestSelection::Page(prepared) => prepared,
    };

    let local_gate = runtime.gc_gate.acquire_publication().await;
    let mut references = Vec::with_capacity(prepared.blobs().len());
    for blob in prepared.blobs() {
        match runtime.coordinator.validate_reference(*blob).await {
            Ok(reference) if reference.blob() == *blob => references.push(reference),
            Ok(_) => {
                return Err(corruption(
                    "text manifest coordinator guard names another blob",
                ));
            }
            Err(
                BlobPublicationError::DeleteFenceClosed
                | BlobPublicationError::PublicationOutcomeAmbiguous(_)
                | BlobPublicationError::ObjectStore(_)
                | BlobPublicationError::CoordinatorUnavailable(_),
            ) => {
                return Ok(PreparedTextOperationStep::Repository(Box::new(
                    PreparedTextRepositoryStep {
                        source_operation: operation.clone(),
                        expected_reads: Vec::new(),
                        writes: Vec::new(),
                        result: IndexOperationStepResult::TransientFailure,
                    },
                )));
            }
            Err(
                BlobPublicationError::ReferenceAbsent | BlobPublicationError::ReferenceMismatch,
            ) => {
                return Ok(PreparedTextOperationStep::Repository(Box::new(
                    PreparedTextRepositoryStep {
                        source_operation: operation.clone(),
                        expected_reads: Vec::new(),
                        writes: Vec::new(),
                        result: IndexOperationStepResult::Blocked(
                            IndexOperationBlocker::InvariantViolation,
                        ),
                    },
                )));
            }
            Err(error) => return Err(coordinator_error(error)),
        }
    }

    let completed_counters = OperationCounters {
        entities: progress.counters.entities,
        input_bytes: checked_add(
            progress.counters.input_bytes,
            prepared.input_bytes(),
            "manifest input bytes",
        )?,
        output_operations: checked_add(
            progress.counters.output_operations,
            prepared.output_operations(),
            "manifest output operations",
        )?,
        output_bytes: checked_add(
            progress.counters.output_bytes,
            prepared.output_bytes(),
            "manifest output bytes",
        )?,
    };
    let next_progress = IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
        TextBuildStage::PrepareManifests(PrefixScanProgress {
            cursor: Some(prepared.completed_cursor().clone()),
            counters: completed_counters,
        }),
    ));
    Ok(PreparedTextOperationStep::ManifestPage(Box::new(
        PreparedTextManifestPage {
            source_operation: operation.clone(),
            prepared,
            progress: next_progress,
            _local_gate: local_gate,
            _references: references,
        },
    )))
}

/// Prepares one bounded page/root/upload-intent validation checkpoint.
///
/// Database selection finishes under a short snapshot. Page work then acquires
/// the local publication gate, coordinator reference guards, and exact object
/// metadata before returning a closed step that retains all authority through
/// the serializable checkpoint commit.
async fn prepare_validation_step(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &TextManifestValidationProgress,
    limits: SearchIndexBatchLimits,
    runtime: &TextPublicationDependencies,
) -> Result<PreparedTextOperationStep> {
    let snapshot = db.begin(IsolationLevel::Snapshot).await?;
    let record = load_operation_index(&snapshot, scope, operation).await?;
    let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
        return Err(corruption(
            "text manifest validation loaded another definition family",
        ));
    };
    let selection =
        super::validation::select(&snapshot, scope, operation, definition, progress, limits)
            .await?;
    drop(snapshot);
    let prepared = match selection {
        super::validation::ValidationSelection::Database(prepared) => {
            return Ok(PreparedTextOperationStep::Validation(Box::new(
                PreparedTextValidationStep::Database {
                    source_operation: operation.clone(),
                    prepared,
                },
            )));
        }
        super::validation::ValidationSelection::Page(prepared) => prepared,
    };

    let local_gate = runtime.gc_gate.acquire_publication().await;
    let mut references = Vec::with_capacity(prepared.blobs().len());
    let mut external_result = None;
    for blob in prepared.blobs().iter().copied() {
        match runtime.coordinator.validate_reference(blob).await {
            Ok(reference) if reference.blob() == blob => references.push(reference),
            Ok(_) => {
                return Err(corruption(
                    "text validation coordinator guard names another blob",
                ));
            }
            Err(
                BlobPublicationError::DeleteFenceClosed
                | BlobPublicationError::PublicationOutcomeAmbiguous(_)
                | BlobPublicationError::ObjectStore(_)
                | BlobPublicationError::CoordinatorUnavailable(_),
            ) => {
                external_result = Some(IndexOperationStepResult::TransientFailure);
                break;
            }
            Err(
                BlobPublicationError::ReferenceAbsent | BlobPublicationError::ReferenceMismatch,
            ) => {
                external_result = Some(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
                break;
            }
            Err(error) => return Err(coordinator_error(error)),
        }
        let location = crate::search::text::blob_object_store_path(&runtime.db_path, *blob.hash());
        match runtime.object_store.head(&location).await {
            Ok(metadata) if metadata.size == blob.size() => {}
            Ok(_) | Err(slatedb::object_store::Error::NotFound { .. }) => {
                external_result = Some(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
                break;
            }
            Err(_) => {
                external_result = Some(IndexOperationStepResult::TransientFailure);
                break;
            }
        }
    }
    if let Some(result) = external_result {
        return Ok(PreparedTextOperationStep::Validation(Box::new(
            PreparedTextValidationStep::Database {
                source_operation: operation.clone(),
                prepared: prepared.into_database_with_result(result),
            },
        )));
    }
    Ok(PreparedTextOperationStep::Validation(Box::new(
        PreparedTextValidationStep::Page {
            source_operation: operation.clone(),
            prepared,
            _local_gate: local_gate,
            _references: references,
        },
    )))
}

/// Rechecks late work in the same transaction that canonically activates text.
async fn activate(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    counters: OperationCounters,
) -> Result<IndexOperationStepResult> {
    if generation_has_rows(transaction, scope, IndexV2RecordKind::BuildDelta, operation).await? {
        return Ok(progressed_build(TextBuildStage::CatchUp(
            PrefixScanProgress {
                cursor: None,
                counters,
            },
        )));
    }
    if generation_has_rows(
        transaction,
        scope,
        IndexV2RecordKind::TextBuildArtifact,
        operation,
    )
    .await?
    {
        return Ok(progressed_build(TextBuildStage::PrepareManifests(
            PrefixScanProgress {
                cursor: None,
                counters,
            },
        )));
    }
    if generation_has_rows(
        transaction,
        scope,
        IndexV2RecordKind::TextUploadIntent,
        operation,
    )
    .await?
    {
        return Ok(progressed_build(TextBuildStage::ValidateManifests(
            TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
                cursor: None,
                counters,
            }),
        )));
    }
    if generation_has_rows(
        transaction,
        scope,
        IndexV2RecordKind::ActiveMutationCommitProof,
        operation,
    )
    .await?
    {
        return Ok(IndexOperationStepResult::Blocked(
            IndexOperationBlocker::InvariantViolation,
        ));
    }
    Ok(IndexOperationStepResult::Completed(
        IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
    ))
}

/// One point-read catch-up decision with no partially representable outcome.
enum TextCatchUpPlanRead {
    Exhausted,
    Blocked(IndexOperationBlocker),
    Planned(TextCatchUpEntityPlan),
}

/// Exact empty roots required by one authoritative catch-up transition.
enum PreparedCatchUpManifestRoots {
    None,
    One(PreparedEmptyManifestRoot),
    Move {
        previous: PreparedEmptyManifestRoot,
        current: PreparedEmptyManifestRoot,
    },
}

impl PreparedCatchUpManifestRoots {
    /// Returns the zero, one, or two distinct roots as borrowed slots.
    fn as_refs(&self) -> [Option<&PreparedEmptyManifestRoot>; 2] {
        match self {
            Self::None => [None, None],
            Self::One(root) => [Some(root), None],
            Self::Move { previous, current } => [Some(previous), Some(current)],
        }
    }

    /// Returns bytes read while proving every required partition root.
    fn input_bytes(&self) -> u64 {
        self.as_refs()
            .into_iter()
            .flatten()
            .fold(0_u64, |bytes, root| {
                bytes.saturating_add(root.input_bytes())
            })
    }

    /// Separates retained root observations from their optional atomic writes.
    fn into_parts(self) -> (Vec<PreparedTextExpectedRead>, Vec<PreparedTextWrite>) {
        let roots = match self {
            Self::None => [None, None],
            Self::One(root) => [Some(root), None],
            Self::Move { previous, current } => [Some(previous), Some(current)],
        };
        roots.into_iter().flatten().fold(
            (Vec::new(), Vec::new()),
            |(mut reads, mut writes), root| {
                let (read, write) = root.into_parts();
                reads.push(read);
                writes.extend(write);
                (reads, writes)
            },
        )
    }
}

/// Exact authoritative entity transition staged with at most one child split.
struct TextCatchUpEntityPlan {
    entity: IndexEntity,
    delta_key: IndexCursor,
    expected_reads: Vec<PreparedTextExpectedRead>,
    writes: Vec<PreparedTextWrite>,
    document: Option<(TextPartition, crate::search::text::TextDocumentInput)>,
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
}

/// Exact state-row observation for one entity in one text partition.
///
/// Catch-up reads at most the previously applied partition and the current
/// authoritative partition. Retaining absence as a variant prevents a move to
/// a previously used partition from resetting its logical version below an
/// older tombstone.
#[derive(Clone)]
enum ObservedTextEntityState {
    Absent {
        partition: TextPartition,
        key: Bytes,
    },
    Present {
        partition: TextPartition,
        key: Bytes,
        value: Bytes,
        logical_version: TextLogicalVersion,
        live: bool,
    },
}

/// Point-reads and validates one exact generation/partition entity-state row.
async fn read_catch_up_entity_state(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    entity: IndexEntity,
    partition: TextPartition,
) -> Result<ObservedTextEntityState> {
    let key = scoped_index_key(
        scope,
        IndexV2Key::TextEntityState(TextEntityStateKey {
            root: TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            },
            entity,
        }),
    );
    let Some(value) = transaction.get(&key).await? else {
        return Ok(ObservedTextEntityState::Absent { partition, key });
    };
    let IndexV2WorkValue::TextEntityState(state) = decode_work_value(&value)? else {
        return Err(corruption(
            "text catch-up entity-state key contains another value kind",
        ));
    };
    if state.index_id != operation.index_id()
        || state.generation != operation.generation()
        || state.partition != partition
        || state.entity_kind != entity.kind
        || state.entity_id != entity.id
    {
        return Err(corruption("text catch-up entity-state ownership mismatch"));
    }
    Ok(ObservedTextEntityState::Present {
        partition,
        key,
        value,
        logical_version: state.logical_version,
        live: state.live,
    })
}

/// Prepares a live catch-up entity; repository-only outcomes run in its transaction.
async fn prepare_catch_up_step(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
    coordinator: Arc<dyn BlobPublicationCoordinator>,
) -> Result<Option<PreparedTextOperationStep>> {
    let snapshot = db.begin(IsolationLevel::Snapshot).await?;
    let record = load_operation_index(&snapshot, scope, operation).await?;
    let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
        return Err(corruption(
            "text catch-up preparation loaded another family",
        ));
    };
    let plan = plan_next_catch_up(&snapshot, scope, operation, definition, progress).await?;
    drop(snapshot);
    let TextCatchUpPlanRead::Planned(plan) = plan else {
        return Ok(None);
    };
    let Some((partition, document)) = plan.document else {
        return Ok(None);
    };
    if plan.input_bytes > limits.max_input_bytes().get()
        || plan.output_operations > limits.max_output_operations().get()
        || plan.output_bytes > limits.max_output_bytes().get()
    {
        return Ok(None);
    }
    let completed_counters = OperationCounters {
        entities: checked_add(progress.counters.entities, 1, "catch-up entities")?,
        input_bytes: checked_add(
            progress.counters.input_bytes,
            plan.input_bytes,
            "catch-up input bytes",
        )?,
        output_operations: checked_add(
            progress.counters.output_operations,
            plan.output_operations,
            "catch-up output operations",
        )?,
        output_bytes: checked_add(
            progress.counters.output_bytes,
            plan.output_bytes,
            "catch-up output bytes",
        )?,
    };
    prepare_build_upload(
        operation,
        scope,
        definition,
        limits,
        coordinator,
        PreparedTextSplitInput {
            partition,
            documents: vec![document],
            completed_counters,
            source: PreparedTextUploadSource::CatchUp {
                progress: progress.clone(),
                delta_key: plan.delta_key,
            },
            expected_reads: plan.expected_reads,
            lifecycle_writes: plan.writes,
        },
    )
    .await
    .map(Some)
}

/// Applies one no-upload delta or hands a live entity back to split preparation.
async fn catch_up(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedTextIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let plan = match plan_next_catch_up(transaction, scope, operation, definition, progress).await?
    {
        TextCatchUpPlanRead::Exhausted => {
            return Ok(progressed_build(TextBuildStage::Compact(
                PrefixScanProgress {
                    cursor: None,
                    counters: progress.counters,
                },
            )));
        }
        TextCatchUpPlanRead::Blocked(blocker) => {
            return Ok(IndexOperationStepResult::Blocked(blocker));
        }
        TextCatchUpPlanRead::Planned(plan) => plan,
    };
    if plan.input_bytes > limits.max_input_bytes().get() {
        return Ok(IndexOperationStepResult::Blocked(
            IndexOperationBlocker::OversizedEntity {
                entity_kind: plan.entity.kind,
                entity_id: plan.entity.id,
                observed: plan.input_bytes,
                limit: limits.max_input_bytes().get(),
            },
        ));
    }
    if plan.output_operations > limits.max_output_operations().get() {
        return Ok(IndexOperationStepResult::Blocked(
            IndexOperationBlocker::OversizedEntity {
                entity_kind: plan.entity.kind,
                entity_id: plan.entity.id,
                observed: plan.output_operations,
                limit: limits.max_output_operations().get(),
            },
        ));
    }
    if plan.output_bytes > limits.max_output_bytes().get() {
        return Ok(IndexOperationStepResult::Blocked(
            IndexOperationBlocker::OversizedEntity {
                entity_kind: plan.entity.kind,
                entity_id: plan.entity.id,
                observed: plan.output_bytes,
                limit: limits.max_output_bytes().get(),
            },
        ));
    }
    if plan.document.is_some() {
        return Ok(IndexOperationStepResult::TransientFailure);
    }
    for write in plan.writes {
        match write {
            PreparedTextWrite::Put { key, value } => transaction.put(key, value)?,
            PreparedTextWrite::Delete { key } => transaction.delete(key)?,
        }
    }
    Ok(progressed_build(TextBuildStage::CatchUp(
        PrefixScanProgress {
            cursor: None,
            counters: OperationCounters {
                entities: checked_add(progress.counters.entities, 1, "catch-up entities")?,
                input_bytes: checked_add(
                    progress.counters.input_bytes,
                    plan.input_bytes,
                    "catch-up input bytes",
                )?,
                output_operations: checked_add(
                    progress.counters.output_operations,
                    plan.output_operations,
                    "catch-up output operations",
                )?,
                output_bytes: checked_add(
                    progress.counters.output_bytes,
                    plan.output_bytes,
                    "catch-up output bytes",
                )?,
            },
        },
    )))
}

/// Point-reads one coalesced delta and derives its complete typed state transition.
async fn plan_next_catch_up(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedTextIndexDefinition,
    progress: &PrefixScanProgress,
) -> Result<TextCatchUpPlanRead> {
    if progress.cursor.is_some() {
        return Err(corruption(
            "text catch-up progress must restart from the coalesced delta prefix",
        ));
    }
    let prefix = Key::data_prefix(
        scope,
        IndexV2Key::generation_prefix(
            IndexV2RecordKind::BuildDelta,
            operation.index_id(),
            operation.generation(),
        ),
    );
    let mut rows = transaction.scan_prefix(&prefix, ..).await?;
    let Some(row) = rows.next().await? else {
        return Ok(TextCatchUpPlanRead::Exhausted);
    };
    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::BuildDelta(delta_key)),
        ..
    } = Key::parse_from_slice(scope, &row.key)?
    else {
        return Err(corruption(
            "text build-delta prefix yielded another key kind",
        ));
    };
    let IndexV2WorkValue::CoalescedBuildDelta(delta) = decode_work_value(&row.value)? else {
        return Err(corruption(
            "text build-delta key contains another value kind",
        ));
    };
    if delta_key.index_id != operation.index_id()
        || delta_key.generation != operation.generation()
        || delta_key.entity.kind != definition.element_kind()
        || delta.index_id != operation.index_id()
        || delta.generation != operation.generation()
        || delta.entity_kind != delta_key.entity.kind
        || delta.entity_id != delta_key.entity.id
    {
        return Err(corruption("text build-delta ownership mismatch"));
    }
    let entity = delta_key.entity;
    let applied_key = scoped_index_key(
        scope,
        IndexV2Key::AppliedState(IndexEntityStateKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        }),
    );
    let applied_value = transaction.get(&applied_key).await?;
    let previous = match applied_value.as_ref() {
        Some(value) => {
            let IndexV2WorkValue::AppliedEntityState(applied) = decode_work_value(value)? else {
                return Err(corruption(
                    "text applied-state key contains another value kind",
                ));
            };
            if applied.index_id != operation.index_id()
                || applied.generation != operation.generation()
                || applied.entity_kind != entity.kind
                || applied.entity_id != entity.id
            {
                return Err(corruption("text applied-state ownership mismatch"));
            }
            let AppliedFamilyState::Text(previous) = applied.state else {
                return Err(corruption(
                    "text generation contains another applied-state family",
                ));
            };
            previous
        }
        None => None,
    };
    let graph_key = authoritative_property_key(scope, entity);
    let graph_value = transaction.get(&graph_key).await?;
    let current = 'current: {
        let Some(value) = graph_value.as_ref() else {
            break 'current None;
        };
        let properties = match property::decode_properties(value) {
            Ok(properties) => properties,
            Err(_) => {
                return Ok(TextCatchUpPlanRead::Blocked(
                    IndexOperationBlocker::InvalidSourceData {
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                    },
                ));
            }
        };
        let label_matches = properties.iter().any(|property| {
            property.name == "$label"
                && property.value.as_str() == Some(definition.label().as_str())
        });
        if !label_matches {
            break 'current None;
        }
        let Some(indexed_property) = properties
            .iter()
            .find(|property| property.name == definition.property().as_str())
        else {
            break 'current None;
        };
        let text = match crate::search::text::normalize_indexed_text_value(&indexed_property.value)
        {
            Ok(Some(text)) => text,
            Ok(None) => break 'current None,
            Err(_) => {
                return Ok(TextCatchUpPlanRead::Blocked(
                    IndexOperationBlocker::InvalidSourceData {
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                    },
                ));
            }
        };
        let partition = match definition.tenant_property() {
            None => TextPartition::Unpartitioned,
            Some(tenant_property) => {
                let Some(tenant_value) = properties
                    .iter()
                    .find(|property| property.name == tenant_property.as_str())
                    .and_then(|property| {
                        crate::search::text::normalize_tenant_value(&property.value)
                    })
                else {
                    return Ok(TextCatchUpPlanRead::Blocked(
                        IndexOperationBlocker::InvalidSourceData {
                            entity_kind: entity.kind,
                            entity_id: entity.id,
                        },
                    ));
                };
                let Ok(partition) = TextPartition::try_tenant_value(
                    property::encode_index_partition_value(tenant_value),
                ) else {
                    return Ok(TextCatchUpPlanRead::Blocked(
                        IndexOperationBlocker::InvalidSourceData {
                            entity_kind: entity.kind,
                            entity_id: entity.id,
                        },
                    ));
                };
                partition
            }
        };
        break 'current Some((partition, text));
    };
    let previous_state = match previous.as_ref() {
        Some((partition, _)) => Some(
            read_catch_up_entity_state(transaction, scope, operation, entity, partition.clone())
                .await?,
        ),
        None => None,
    };
    let current_state = match current.as_ref() {
        Some((partition, _))
            if previous
                .as_ref()
                .is_some_and(|(previous_partition, _)| previous_partition == partition) =>
        {
            previous_state.clone()
        }
        Some((partition, _)) => Some(
            read_catch_up_entity_state(transaction, scope, operation, entity, partition.clone())
                .await?,
        ),
        None => None,
    };
    let empty_manifest_roots = match (previous.as_ref(), current.as_ref()) {
        (None, None) => PreparedCatchUpManifestRoots::None,
        (Some((previous_partition, _)), Some((current_partition, _)))
            if previous_partition != current_partition =>
        {
            PreparedCatchUpManifestRoots::Move {
                previous: prepare_empty_manifest_root(
                    transaction,
                    scope,
                    operation,
                    previous_partition.clone(),
                )
                .await?,
                current: prepare_empty_manifest_root(
                    transaction,
                    scope,
                    operation,
                    current_partition.clone(),
                )
                .await?,
            }
        }
        (Some((partition, _)), _) | (None, Some((partition, _))) => {
            PreparedCatchUpManifestRoots::One(
                prepare_empty_manifest_root(transaction, scope, operation, partition.clone())
                    .await?,
            )
        }
    };
    build_text_catch_up_plan(
        operation,
        entity,
        row.key,
        row.value,
        applied_key,
        applied_value,
        graph_key,
        graph_value,
        previous,
        current,
        previous_state,
        current_state,
        empty_manifest_roots,
    )
}

/// Materializes the closed live/dead transition selected by authoritative state.
#[allow(
    clippy::too_many_arguments,
    reason = "the plan binds all three exact reads and the complete entity transition"
)]
fn build_text_catch_up_plan(
    operation: &IndexOperationRecord,
    entity: IndexEntity,
    delta_key: Bytes,
    delta_value: Bytes,
    applied_key: Bytes,
    applied_value: Option<Bytes>,
    graph_key: Bytes,
    graph_value: Option<Bytes>,
    previous: Option<(TextPartition, TextLogicalVersion)>,
    current: Option<(TextPartition, String)>,
    previous_state: Option<ObservedTextEntityState>,
    current_state: Option<ObservedTextEntityState>,
    empty_manifest_roots: PreparedCatchUpManifestRoots,
) -> Result<TextCatchUpPlanRead> {
    let state_row = |key, partition: &TextPartition, logical_version, live| {
        let value = encode_work_value(&IndexV2WorkValue::TextEntityState(TextEntityStateValue {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: partition.clone(),
            entity_kind: entity.kind,
            entity_id: entity.id,
            logical_version,
            live,
        }));
        PreparedTextWrite::Put { key, value }
    };
    let applied_row = |partition: TextPartition, logical_version| PreparedTextWrite::Put {
        key: applied_key.clone(),
        value: encode_work_value(&IndexV2WorkValue::AppliedEntityState(
            AppliedEntityStateValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                entity_kind: entity.kind,
                entity_id: entity.id,
                state: AppliedFamilyState::Text(Some((partition, logical_version))),
            },
        )),
    };
    let mut writes = vec![PreparedTextWrite::Delete {
        key: delta_key.clone(),
    }];
    let mut expected_state_reads = Vec::with_capacity(2);
    for observed in [previous_state.as_ref(), current_state.as_ref()]
        .into_iter()
        .flatten()
    {
        let (key, value) = match observed {
            ObservedTextEntityState::Absent { key, .. } => (key, None),
            ObservedTextEntityState::Present { key, value, .. } => (key, Some(value.clone())),
        };
        if !expected_state_reads
            .iter()
            .any(|read: &PreparedTextExpectedRead| read.key == *key)
        {
            expected_state_reads.push(PreparedTextExpectedRead {
                key: key.clone(),
                value,
            });
        }
    }
    let document = match (previous, current) {
        (None, None) => {
            if previous_state.is_some() || current_state.is_some() {
                return Err(corruption(
                    "absent text applied state retained a partition-state observation",
                ));
            }
            None
        }
        (Some((previous_partition, previous_version)), None) => {
            let previous_key = match previous_state.as_ref() {
                Some(ObservedTextEntityState::Present {
                    partition,
                    key,
                    logical_version,
                    ..
                }) if partition == &previous_partition && *logical_version == previous_version => {
                    key.clone()
                }
                Some(ObservedTextEntityState::Absent { .. })
                | Some(ObservedTextEntityState::Present { .. })
                | None => {
                    return Err(corruption(
                        "text applied state disagrees with its previous partition state",
                    ));
                }
            };
            let Ok(next_version) = previous_version.checked_next() else {
                return Ok(TextCatchUpPlanRead::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
            };
            writes.push(state_row(
                previous_key,
                &previous_partition,
                next_version,
                false,
            ));
            writes.push(applied_row(previous_partition, next_version));
            None
        }
        (None, Some((partition, text))) => {
            let state_key = match current_state.as_ref() {
                Some(ObservedTextEntityState::Absent {
                    partition: observed_partition,
                    key,
                }) if observed_partition == &partition => key.clone(),
                Some(ObservedTextEntityState::Present { .. })
                | Some(ObservedTextEntityState::Absent { .. })
                | None => {
                    return Err(corruption(
                        "new text applied state found an existing or mismatched partition state",
                    ));
                }
            };
            let version = TextLogicalVersion::initial();
            writes.push(state_row(state_key, &partition, version, true));
            writes.push(applied_row(partition.clone(), version));
            Some((
                partition,
                crate::search::text::TextDocumentInput::new(entity.id.get(), text)
                    .with_logical_version(version.get()),
            ))
        }
        (Some((previous_partition, previous_version)), Some((partition, text)))
            if previous_partition == partition =>
        {
            let state_key = match previous_state.as_ref() {
                Some(ObservedTextEntityState::Present {
                    partition: observed_partition,
                    key,
                    logical_version,
                    ..
                }) if observed_partition == &partition && *logical_version == previous_version => {
                    key.clone()
                }
                Some(ObservedTextEntityState::Absent { .. })
                | Some(ObservedTextEntityState::Present { .. })
                | None => {
                    return Err(corruption(
                        "text applied state disagrees with its current partition state",
                    ));
                }
            };
            let Ok(next_version) = previous_version.checked_next() else {
                return Ok(TextCatchUpPlanRead::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
            };
            writes.push(state_row(state_key, &partition, next_version, true));
            writes.push(applied_row(partition.clone(), next_version));
            Some((
                partition,
                crate::search::text::TextDocumentInput::new(entity.id.get(), text)
                    .with_logical_version(next_version.get()),
            ))
        }
        (Some((previous_partition, previous_version)), Some((partition, text))) => {
            let previous_key = match previous_state.as_ref() {
                Some(ObservedTextEntityState::Present {
                    partition: observed_partition,
                    key,
                    logical_version,
                    ..
                }) if observed_partition == &previous_partition
                    && *logical_version == previous_version =>
                {
                    key.clone()
                }
                Some(ObservedTextEntityState::Absent { .. })
                | Some(ObservedTextEntityState::Present { .. })
                | None => {
                    return Err(corruption(
                        "text applied state disagrees with its moved-from partition state",
                    ));
                }
            };
            let Ok(dead_version) = previous_version.checked_next() else {
                return Ok(TextCatchUpPlanRead::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
            };
            let (current_key, live_version) = match current_state.as_ref() {
                Some(ObservedTextEntityState::Absent {
                    partition: observed_partition,
                    key,
                }) if observed_partition == &partition => {
                    (key.clone(), TextLogicalVersion::initial())
                }
                Some(ObservedTextEntityState::Present {
                    partition: observed_partition,
                    key,
                    logical_version,
                    live: false,
                    ..
                }) if observed_partition == &partition => {
                    let Ok(next_version) = logical_version.checked_next() else {
                        return Ok(TextCatchUpPlanRead::Blocked(
                            IndexOperationBlocker::InvariantViolation,
                        ));
                    };
                    (key.clone(), next_version)
                }
                Some(ObservedTextEntityState::Present { live: true, .. })
                | Some(ObservedTextEntityState::Present { .. })
                | Some(ObservedTextEntityState::Absent { .. })
                | None => {
                    return Err(corruption(
                        "text move destination contains live or mismatched partition state",
                    ));
                }
            };
            writes.push(state_row(
                previous_key,
                &previous_partition,
                dead_version,
                false,
            ));
            writes.push(state_row(current_key, &partition, live_version, true));
            writes.push(applied_row(partition.clone(), live_version));
            Some((
                partition,
                crate::search::text::TextDocumentInput::new(entity.id.get(), text)
                    .with_logical_version(live_version.get()),
            ))
        }
    };
    let root_input_bytes = empty_manifest_roots.input_bytes();
    let (root_reads, root_writes) = empty_manifest_roots.into_parts();
    writes.extend(root_writes);
    let input_bytes = u64::try_from(
        delta_key
            .len()
            .saturating_add(delta_value.len())
            .saturating_add(applied_key.len())
            .saturating_add(applied_value.as_ref().map_or(0, Bytes::len))
            .saturating_add(graph_key.len())
            .saturating_add(graph_value.as_ref().map_or(0, Bytes::len))
            .saturating_add(expected_state_reads.iter().fold(0_usize, |bytes, read| {
                bytes
                    .saturating_add(read.key.len())
                    .saturating_add(read.value.as_ref().map_or(0, Bytes::len))
            })),
    )
    .unwrap_or(u64::MAX)
    .saturating_add(root_input_bytes);
    let output_operations = u64::try_from(writes.len()).unwrap_or(u64::MAX);
    let output_bytes = writes.iter().fold(0_u64, |bytes, write| {
        let write_bytes = match write {
            PreparedTextWrite::Put { key, value } => key.len().saturating_add(value.len()),
            PreparedTextWrite::Delete { key } => key.len(),
        };
        bytes.saturating_add(u64::try_from(write_bytes).unwrap_or(u64::MAX))
    });
    let mut expected_reads = vec![
        PreparedTextExpectedRead {
            key: delta_key.clone(),
            value: Some(delta_value),
        },
        PreparedTextExpectedRead {
            key: applied_key,
            value: applied_value,
        },
        PreparedTextExpectedRead {
            key: graph_key,
            value: graph_value,
        },
    ];
    expected_reads.extend(expected_state_reads);
    expected_reads.extend(root_reads);
    Ok(TextCatchUpPlanRead::Planned(TextCatchUpEntityPlan {
        entity,
        delta_key: IndexCursor::try_new(delta_key).map_err(operation_error)?,
        expected_reads,
        writes,
        document,
        input_bytes,
        output_operations,
        output_bytes,
    }))
}

/// Reads one bounded contiguous kind-`0x0C` partition run and its graph rows.
///
/// Every admitted partition carries its exact empty-root observation. Upload
/// selections require that root, while repository-only progress may create the
/// canonical unpartitioned root even when the authoritative source is empty.
async fn scan_partition_documents(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedTextIndexDefinition,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
) -> Result<PartitionScanSelection> {
    let expected_upper =
        initial_partition_scan(operation, scope, progress.counters)?.inclusive_upper_bound;
    if progress.inclusive_upper_bound != expected_upper {
        return Err(corruption(
            "text partition scan does not retain its exact maximal generation key",
        ));
    }
    let prefix = Key::data_prefix(
        scope,
        IndexV2Key::generation_prefix(
            IndexV2RecordKind::TextEntityState,
            operation.index_id(),
            operation.generation(),
        ),
    );
    let start = cursor_suffix(&prefix, progress.cursor.as_ref())?;
    let upper = cursor_suffix(&prefix, Some(&progress.inclusive_upper_bound))?
        .ok_or_else(|| corruption("text partition upper bound is absent"))?;
    let start = start.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::Included(upper)))
        .await?;
    let mut partition = None::<TextPartition>;
    let mut documents = Vec::new();
    let mut completed_cursor = progress.cursor.clone();
    let mut batch_entities = 0_usize;
    let mut batch_input_bytes = 0_u64;
    let mut empty_root = None::<PreparedEmptyManifestRoot>;
    let mut exhausted = true;

    while batch_entities < limits.max_entities().get() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let (key, state) = decode_entity_state(scope, &row.key, &row.value, operation)?;
        let row_partition = state.partition.clone();
        match &partition {
            Some(current) if current.fingerprint() != key.root.partition => {
                exhausted = false;
                break;
            }
            Some(current) if current != &row_partition => {
                return Err(corruption(
                    "text partition fingerprint collision would merge canonical tenants",
                ));
            }
            Some(_) => {}
            None => {
                partition = Some(row_partition.clone());
                let root = prepare_empty_manifest_root(
                    transaction,
                    scope,
                    operation,
                    row_partition.clone(),
                )
                .await?;
                let root_input_bytes = root.input_bytes();
                if root_input_bytes > limits.max_input_bytes().get() {
                    return Ok(PartitionScanSelection::Repository {
                        empty_root: None,
                        result: IndexOperationStepResult::Blocked(
                            IndexOperationBlocker::ManifestLimit {
                                partition: row_partition,
                                observed: root_input_bytes,
                                limit: limits.max_input_bytes().get(),
                            },
                        ),
                    });
                }
                let root_output_operations = root.output_operations();
                if root_output_operations > limits.max_output_operations().get() {
                    return Ok(PartitionScanSelection::Repository {
                        empty_root: None,
                        result: IndexOperationStepResult::Blocked(
                            IndexOperationBlocker::ManifestLimit {
                                partition: row_partition,
                                observed: root_output_operations,
                                limit: limits.max_output_operations().get(),
                            },
                        ),
                    });
                }
                let root_output_bytes = root.output_bytes();
                if root_output_bytes > limits.max_output_bytes().get() {
                    return Ok(PartitionScanSelection::Repository {
                        empty_root: None,
                        result: IndexOperationStepResult::Blocked(
                            IndexOperationBlocker::ManifestLimit {
                                partition: row_partition,
                                observed: root_output_bytes,
                                limit: limits.max_output_bytes().get(),
                            },
                        ),
                    });
                }
                if root.requires_creation() {
                    let seed_input_bytes = root_input_bytes.saturating_add(
                        u64::try_from(row.key.len().saturating_add(row.value.len()))
                            .unwrap_or(u64::MAX),
                    );
                    if seed_input_bytes > limits.max_input_bytes().get() {
                        return Ok(PartitionScanSelection::Repository {
                            empty_root: None,
                            result: IndexOperationStepResult::Blocked(
                                IndexOperationBlocker::ManifestLimit {
                                    partition: row_partition,
                                    observed: seed_input_bytes,
                                    limit: limits.max_input_bytes().get(),
                                },
                            ),
                        });
                    }
                    let counters = OperationCounters {
                        entities: progress.counters.entities,
                        input_bytes: checked_add(
                            progress.counters.input_bytes,
                            seed_input_bytes,
                            "empty-root input bytes",
                        )?,
                        output_operations: checked_add(
                            progress.counters.output_operations,
                            root_output_operations,
                            "empty-root output operations",
                        )?,
                        output_bytes: checked_add(
                            progress.counters.output_bytes,
                            root_output_bytes,
                            "empty-root output bytes",
                        )?,
                    };
                    return Ok(PartitionScanSelection::Repository {
                        empty_root: Some(root),
                        result: progressed_build(TextBuildStage::ScanPartitions(
                            SourceScanProgress {
                                inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
                                cursor: progress.cursor.clone(),
                                counters,
                            },
                        )),
                    });
                }
                batch_input_bytes = root_input_bytes;
                empty_root = Some(root);
            }
        }

        let graph_key = authoritative_property_key(scope, key.entity);
        let graph_value = transaction.get(&graph_key).await?;
        let input_bytes = u64::try_from(
            row.key
                .len()
                .saturating_add(row.value.len())
                .saturating_add(graph_key.len())
                .saturating_add(graph_value.as_ref().map_or(0, Bytes::len)),
        )
        .unwrap_or(u64::MAX);
        let admitted_input_bytes = batch_input_bytes.saturating_add(input_bytes);
        if admitted_input_bytes > limits.max_input_bytes().get() {
            if batch_entities == 0 {
                let blocker = if input_bytes > limits.max_input_bytes().get() {
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: key.entity.kind,
                        entity_id: key.entity.id,
                        observed: input_bytes,
                        limit: limits.max_input_bytes().get(),
                    }
                } else {
                    IndexOperationBlocker::ManifestLimit {
                        partition: row_partition,
                        observed: admitted_input_bytes,
                        limit: limits.max_input_bytes().get(),
                    }
                };
                return Ok(PartitionScanSelection::Repository {
                    empty_root: None,
                    result: IndexOperationStepResult::Blocked(blocker),
                });
            }
            exhausted = false;
            break;
        }

        let document = match graph_value {
            Some(value) if state.live => {
                let properties = match property::decode_properties(&value) {
                    Ok(properties) => properties,
                    Err(_) => {
                        return Ok(PartitionScanSelection::Repository {
                            empty_root: None,
                            result: invalid_source(key.entity.kind, key.entity.id),
                        });
                    }
                };
                text_document(definition, &properties, &state)?
            }
            Some(_) | None => None,
        };
        if let Some(document) = document {
            documents.push(document);
        }
        batch_entities = batch_entities
            .checked_add(1)
            .ok_or_else(|| corruption("text partition batch entity count overflowed"))?;
        batch_input_bytes = checked_add(
            batch_input_bytes,
            input_bytes,
            "partition batch input bytes",
        )?;
        completed_cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
    }
    if batch_entities == limits.max_entities().get() {
        exhausted = false;
    }

    if partition.is_none() && definition.tenant_property().is_none() {
        let root = prepare_empty_manifest_root(
            transaction,
            scope,
            operation,
            TextPartition::Unpartitioned,
        )
        .await?;
        let root_input_bytes = root.input_bytes();
        if root_input_bytes > limits.max_input_bytes().get() {
            return Ok(PartitionScanSelection::Repository {
                empty_root: None,
                result: IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                    partition: TextPartition::Unpartitioned,
                    observed: root_input_bytes,
                    limit: limits.max_input_bytes().get(),
                }),
            });
        }
        let root_output_operations = root.output_operations();
        if root_output_operations > limits.max_output_operations().get() {
            return Ok(PartitionScanSelection::Repository {
                empty_root: None,
                result: IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                    partition: TextPartition::Unpartitioned,
                    observed: root_output_operations,
                    limit: limits.max_output_operations().get(),
                }),
            });
        }
        let root_output_bytes = root.output_bytes();
        if root_output_bytes > limits.max_output_bytes().get() {
            return Ok(PartitionScanSelection::Repository {
                empty_root: None,
                result: IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                    partition: TextPartition::Unpartitioned,
                    observed: root_output_bytes,
                    limit: limits.max_output_bytes().get(),
                }),
            });
        }
        if root.requires_creation() {
            let counters = OperationCounters {
                entities: progress.counters.entities,
                input_bytes: checked_add(
                    progress.counters.input_bytes,
                    root_input_bytes,
                    "empty-root input bytes",
                )?,
                output_operations: checked_add(
                    progress.counters.output_operations,
                    root_output_operations,
                    "empty-root output operations",
                )?,
                output_bytes: checked_add(
                    progress.counters.output_bytes,
                    root_output_bytes,
                    "empty-root output bytes",
                )?,
            };
            return Ok(PartitionScanSelection::Repository {
                empty_root: Some(root),
                result: progressed_build(TextBuildStage::ScanPartitions(SourceScanProgress {
                    inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
                    cursor: progress.cursor.clone(),
                    counters,
                })),
            });
        }
        batch_input_bytes = root_input_bytes;
        empty_root = Some(root);
    }

    let completed_counters = OperationCounters {
        entities: checked_add(
            progress.counters.entities,
            batch_entities as u64,
            "cumulative entities",
        )?,
        input_bytes: checked_add(
            progress.counters.input_bytes,
            batch_input_bytes,
            "cumulative input bytes",
        )?,
        output_operations: progress.counters.output_operations,
        output_bytes: progress.counters.output_bytes,
    };
    let Some(completed_cursor) = completed_cursor else {
        return Ok(PartitionScanSelection::Repository {
            empty_root,
            result: progressed_build(TextBuildStage::CatchUp(PrefixScanProgress {
                cursor: None,
                counters: completed_counters,
            })),
        });
    };
    if documents.is_empty() {
        let next = if exhausted {
            TextBuildStage::CatchUp(PrefixScanProgress {
                cursor: None,
                counters: completed_counters,
            })
        } else {
            TextBuildStage::ScanPartitions(SourceScanProgress {
                inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
                cursor: Some(completed_cursor),
                counters: completed_counters,
            })
        };
        return Ok(PartitionScanSelection::Repository {
            empty_root,
            result: progressed_build(next),
        });
    }
    let Some(partition) = partition else {
        return Err(corruption(
            "non-empty text partition documents have no canonical partition",
        ));
    };
    let Some(empty_root) = empty_root else {
        return Err(corruption(
            "non-empty text partition documents have no empty manifest root",
        ));
    };
    Ok(PartitionScanSelection::Upload(PartitionDocuments {
        empty_root,
        partition,
        documents,
        completed_cursor,
        completed_counters,
    }))
}

/// Decodes and cross-checks one generation-qualified text entity-state row.
fn decode_entity_state(
    scope: DataScope,
    key: &[u8],
    value: &[u8],
    operation: &IndexOperationRecord,
) -> Result<(TextEntityStateKey, TextEntityStateValue)> {
    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::TextEntityState(key)),
        ..
    } = Key::parse_from_slice(scope, key)?
    else {
        return Err(corruption(
            "text entity-state prefix yielded another key kind",
        ));
    };
    let IndexV2WorkValue::TextEntityState(state) = decode_work_value(value)? else {
        return Err(corruption(
            "text entity-state key contains another value kind",
        ));
    };
    if key.root.index_id != operation.index_id()
        || key.root.generation != operation.generation()
        || key.root.partition != state.partition.fingerprint()
        || state.index_id != operation.index_id()
        || state.generation != operation.generation()
        || key.entity.kind != state.entity_kind
        || key.entity.id != state.entity_id
    {
        return Err(corruption("text entity-state key/value ownership mismatch"));
    }
    Ok((key, state))
}

/// Builds one document only when current graph state still owns this partition.
fn text_document(
    definition: &ValidatedTextIndexDefinition,
    properties: &[property::Property],
    state: &TextEntityStateValue,
) -> Result<Option<crate::search::text::TextDocumentInput>> {
    let label_matches = properties.iter().any(|property| {
        property.name == "$label" && property.value.as_str() == Some(definition.label().as_str())
    });
    if !label_matches {
        return Ok(None);
    }
    let Some(indexed_property) = properties
        .iter()
        .find(|property| property.name == definition.property().as_str())
    else {
        return Ok(None);
    };
    let text = match crate::search::text::normalize_indexed_text_value(&indexed_property.value) {
        Ok(Some(text)) => text,
        Ok(None) => return Ok(None),
        Err(_) => return Ok(None),
    };
    let current_partition = match definition.tenant_property() {
        None => TextPartition::Unpartitioned,
        Some(tenant_property) => {
            let Some(tenant_value) = properties
                .iter()
                .find(|property| property.name == tenant_property.as_str())
                .and_then(|property| crate::search::text::normalize_tenant_value(&property.value))
            else {
                return Ok(None);
            };
            let Ok(partition) = TextPartition::try_tenant_value(
                property::encode_index_partition_value(tenant_value),
            ) else {
                return Ok(None);
            };
            partition
        }
    };
    if current_partition != state.partition {
        return Ok(None);
    }
    Ok(Some(
        crate::search::text::TextDocumentInput::new(state.entity_id.get(), text)
            .with_logical_version(state.logical_version.get()),
    ))
}

/// Constructs the authoritative graph-property key for one typed entity.
fn authoritative_property_key(scope: DataScope, entity: IndexEntity) -> Bytes {
    let kind = match entity.kind {
        IndexElementKind::Node => DataKeyKind::NodeProperty(
            crate::encoding::v1::keys::NodePropertyKey::new(entity.id.get()),
        ),
        IndexElementKind::Edge => DataKeyKind::EdgePropertyById(
            crate::encoding::v1::keys::EdgePropertyByIdKey::new(entity.id.get()),
        ),
    };
    Key::Data { scope, kind }.to_bytes()
}

/// Returns the typed blocker for one invalid authoritative graph row.
fn invalid_source(kind: IndexElementKind, id: IndexEntityId) -> IndexOperationStepResult {
    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData {
        entity_kind: kind,
        entity_id: id,
    })
}

/// Stages one bounded authoritative graph scan as partition-qualified state.
///
/// Writes are accumulated in memory and staged only after every admitted row
/// validates. A blocking source row therefore cannot commit earlier rows while
/// leaving the durable cursor behind them. The enclosing outbox transaction
/// commits these writes and the returned checkpoint atomically.
async fn scan_source(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedTextIndexDefinition,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let source_prefix = source_prefix(scope, definition.element_kind());
    let start = cursor_suffix(&source_prefix, progress.cursor.as_ref())?;
    let upper = cursor_suffix(&source_prefix, Some(&progress.inclusive_upper_bound))?
        .ok_or_else(|| corruption("text source upper bound is absent"))?;
    if source_entity(
        scope,
        definition.element_kind(),
        progress.inclusive_upper_bound.as_bytes(),
    )?
    .is_none()
    {
        return Err(corruption(
            "text source upper bound is not an exact property-by-ID key",
        ));
    }
    match start.as_ref().map(|start| start.cmp(&upper)) {
        Some(std::cmp::Ordering::Greater) => {
            return Err(corruption(
                "text source cursor exceeds its inclusive upper bound",
            ));
        }
        Some(std::cmp::Ordering::Equal) => {
            return Ok(progressed_build(TextBuildStage::ScanPartitions(
                initial_partition_scan(operation, scope, progress.counters)?,
            )));
        }
        Some(std::cmp::Ordering::Less) | None => {}
    }

    let start = start.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = transaction
        .scan_prefix(&source_prefix, (start, Bound::Included(upper)))
        .await?;
    let mut batch_entities = 0_usize;
    let mut batch_input_bytes = 0_u64;
    let mut batch_output_operations = 0_u64;
    let mut batch_output_bytes = 0_u64;
    let mut cursor = progress.cursor.clone();
    let mut writes = Vec::new();
    let mut exhausted = true;

    'scan_rows: while batch_entities < limits.max_entities().get() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        if batch_input_bytes.saturating_add(input_bytes) > limits.max_input_bytes().get() {
            if batch_entities == 0 {
                let entity_id = source_entity(scope, definition.element_kind(), &row.key)?
                    .unwrap_or(IndexEntityId::initial());
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id,
                        observed: input_bytes,
                        limit: limits.max_input_bytes().get(),
                    },
                ));
            }
            exhausted = false;
            break;
        }

        let complete_cursor = IndexCursor::try_new(row.key.clone()).map_err(operation_error)?;
        let entity_id = source_entity(scope, definition.element_kind(), &row.key)?;
        let mut staged = None;
        'stage_entity: {
            let Some(entity_id) = entity_id else {
                break 'stage_entity;
            };
            let properties = match property::decode_properties(&row.value) {
                Ok(properties) => properties,
                Err(_) => {
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::InvalidSourceData {
                            entity_kind: definition.element_kind(),
                            entity_id,
                        },
                    ));
                }
            };
            let label_matches = properties.iter().any(|property| {
                property.name == "$label"
                    && property.value.as_str() == Some(definition.label().as_str())
            });
            if !label_matches {
                break 'stage_entity;
            }
            let Some(indexed_property) = properties
                .iter()
                .find(|property| property.name == definition.property().as_str())
            else {
                break 'stage_entity;
            };
            match crate::search::text::normalize_indexed_text_value(&indexed_property.value) {
                Ok(Some(_)) => {}
                Ok(None) => break 'stage_entity,
                Err(_) => {
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::InvalidSourceData {
                            entity_kind: definition.element_kind(),
                            entity_id,
                        },
                    ));
                }
            }
            let partition = match definition.tenant_property() {
                None => TextPartition::Unpartitioned,
                Some(tenant_property) => {
                    let Some(tenant_value) = properties
                        .iter()
                        .find(|property| property.name == tenant_property.as_str())
                        .and_then(|property| {
                            crate::search::text::normalize_tenant_value(&property.value)
                        })
                    else {
                        return Ok(IndexOperationStepResult::Blocked(
                            IndexOperationBlocker::InvalidSourceData {
                                entity_kind: definition.element_kind(),
                                entity_id,
                            },
                        ));
                    };
                    match TextPartition::try_tenant_value(property::encode_index_partition_value(
                        tenant_value,
                    )) {
                        Ok(partition) => partition,
                        Err(_) => {
                            return Ok(IndexOperationStepResult::Blocked(
                                IndexOperationBlocker::InvalidSourceData {
                                    entity_kind: definition.element_kind(),
                                    entity_id,
                                },
                            ));
                        }
                    }
                }
            };
            let entity = IndexEntity {
                kind: definition.element_kind(),
                id: entity_id,
            };
            let key = scoped_index_key(
                scope,
                IndexV2Key::TextEntityState(TextEntityStateKey {
                    root: TextManifestRootKey {
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        partition: partition.fingerprint(),
                    },
                    entity,
                }),
            );
            let applied_key = scoped_index_key(
                scope,
                IndexV2Key::AppliedState(IndexEntityStateKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    entity,
                }),
            );
            if transaction.get(&key).await?.is_some()
                || transaction.get(&applied_key).await?.is_some()
            {
                return Err(corruption(
                    "text source checkpoint has pre-existing entity or applied state",
                ));
            }
            let value =
                encode_work_value(&IndexV2WorkValue::TextEntityState(TextEntityStateValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: partition.clone(),
                    entity_kind: definition.element_kind(),
                    entity_id,
                    logical_version: TextLogicalVersion::initial(),
                    live: true,
                }));
            let applied_value = encode_work_value(&IndexV2WorkValue::AppliedEntityState(
                AppliedEntityStateValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    entity_kind: definition.element_kind(),
                    entity_id,
                    state: AppliedFamilyState::Text(Some((
                        partition,
                        TextLogicalVersion::initial(),
                    ))),
                },
            ));
            let output_bytes = u64::try_from(
                key.len()
                    .saturating_add(value.len())
                    .saturating_add(applied_key.len())
                    .saturating_add(applied_value.len()),
            )
            .unwrap_or(u64::MAX);
            let output_operations = batch_output_operations.saturating_add(2);
            if output_operations > limits.max_output_operations().get()
                || batch_output_bytes.saturating_add(output_bytes) > limits.max_output_bytes().get()
            {
                if batch_entities == 0 {
                    let (observed, limit) =
                        if output_operations > limits.max_output_operations().get() {
                            (output_operations, limits.max_output_operations().get())
                        } else {
                            (output_bytes, limits.max_output_bytes().get())
                        };
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::OversizedEntity {
                            entity_kind: definition.element_kind(),
                            entity_id,
                            observed,
                            limit,
                        },
                    ));
                }
                exhausted = false;
                break 'scan_rows;
            }
            staged = Some((key, value, applied_key, applied_value, output_bytes));
        }

        batch_entities = batch_entities
            .checked_add(1)
            .ok_or_else(|| corruption("text batch entity count overflowed"))?;
        batch_input_bytes = checked_add(batch_input_bytes, input_bytes, "batch input bytes")?;
        let Some((key, value, applied_key, applied_value, output_bytes)) = staged else {
            cursor = Some(complete_cursor);
            continue;
        };
        batch_output_operations =
            checked_add(batch_output_operations, 2, "batch output operations")?;
        batch_output_bytes = checked_add(batch_output_bytes, output_bytes, "batch output bytes")?;
        writes.push((key, value));
        writes.push((applied_key, applied_value));
        cursor = Some(complete_cursor);
    }
    if batch_entities == limits.max_entities().get() {
        exhausted = false;
    }

    let counters = OperationCounters {
        entities: checked_add(
            progress.counters.entities,
            batch_entities as u64,
            "cumulative entities",
        )?,
        input_bytes: checked_add(
            progress.counters.input_bytes,
            batch_input_bytes,
            "cumulative input bytes",
        )?,
        output_operations: checked_add(
            progress.counters.output_operations,
            batch_output_operations,
            "cumulative output operations",
        )?,
        output_bytes: checked_add(
            progress.counters.output_bytes,
            batch_output_bytes,
            "cumulative output bytes",
        )?,
    };
    let next = if exhausted {
        TextBuildStage::ScanPartitions(initial_partition_scan(operation, scope, counters)?)
    } else {
        TextBuildStage::ScanSource(SourceScanProgress {
            inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
            cursor,
            counters,
        })
    };
    for (key, value) in writes {
        transaction.put(key, value)?;
    }
    Ok(progressed_build(next))
}

/// Captures the exact maximal key for the partition-ordered staging keyspace.
fn initial_partition_scan(
    operation: &IndexOperationRecord,
    scope: DataScope,
    counters: OperationCounters,
) -> Result<SourceScanProgress> {
    let upper = scoped_index_key(
        scope,
        IndexV2Key::TextEntityState(TextEntityStateKey {
            root: TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: PartitionFingerprint::new([u8::MAX; 32]),
            },
            entity: IndexEntity {
                kind: IndexElementKind::Edge,
                id: IndexEntityId::new(u64::MAX),
            },
        }),
    );
    Ok(SourceScanProgress {
        inclusive_upper_bound: IndexCursor::try_new(upper).map_err(operation_error)?,
        cursor: None,
        counters,
    })
}

/// Loads and cross-checks the canonical text record for one claimed operation.
async fn load_operation_index(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Result<IndexRecordV2> {
    let key = scoped_index_key(
        scope,
        IndexV2Key::index_record(operation.identity().clone()),
    );
    let Some(value) = transaction.get(key).await? else {
        return Err(corruption("text operation has no canonical index"));
    };
    let record = decode_index_record(&value)?;
    if record.index_id() != operation.index_id()
        || record.identity() != operation.identity()
        || record.revision() != operation.index_record_revision()
        || record.state().generation() != operation.generation()
    {
        return Err(corruption("text operation/canonical record mismatch"));
    }
    Ok(record)
}

/// Returns whether one exact generation-owned V2 prefix contains any row.
async fn generation_has_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    kind: IndexV2RecordKind,
    operation: &IndexOperationRecord,
) -> Result<bool> {
    let prefix = Key::data_prefix(
        scope,
        IndexV2Key::generation_prefix(kind, operation.index_id(), operation.generation()),
    );
    let mut rows = transaction.scan_prefix(prefix, ..).await?;
    Ok(rows.next().await?.is_some())
}

/// Returns the physical source prefix for the definition's entity kind.
fn source_prefix(scope: DataScope, kind: IndexElementKind) -> Bytes {
    let prefix = match kind {
        IndexElementKind::Node => KeyPrefix::NodeProperty,
        IndexElementKind::Edge => KeyPrefix::EdgePropertyById,
    };
    Key::data_prefix(scope, Bytes::copy_from_slice(prefix.as_slice()))
}

/// Parses one source row and rejects a keyspace/entity-kind mismatch.
fn source_entity(
    scope: DataScope,
    expected: IndexElementKind,
    key: &[u8],
) -> Result<Option<IndexEntityId>> {
    let parsed = Key::parse_from_slice(scope, key)?;
    Ok(match (expected, parsed) {
        (
            IndexElementKind::Node,
            Key::Data {
                kind: DataKeyKind::NodeProperty(key),
                ..
            },
        ) => Some(IndexEntityId::new(key.node_id())),
        (
            IndexElementKind::Edge,
            Key::Data {
                kind: DataKeyKind::EdgePropertyById(key),
                ..
            },
        ) => Some(IndexEntityId::new(key.edge_id())),
        (IndexElementKind::Edge, Key::Data { .. }) => None,
        (IndexElementKind::Node, Key::Data { .. }) | (_, Key::Global { .. }) => {
            return Err(corruption("text source prefix yielded another key kind"));
        }
    })
}

/// Removes an exact physical prefix from a complete persisted cursor.
fn cursor_suffix(prefix: &Bytes, cursor: Option<&IndexCursor>) -> Result<Option<Bytes>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
        return Err(corruption("text cursor is outside its exact scan prefix"));
    };
    Ok(Some(Bytes::copy_from_slice(suffix)))
}

/// Encodes one scoped V2 key through the canonical `encoding/v1` boundary.
fn scoped_index_key(scope: DataScope, key: IndexV2Key) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(key),
    }
    .to_bytes()
}

/// Wraps a text build stage in the only legal constructing progress shape.
fn progressed_build(stage: TextBuildStage) -> IndexOperationStepResult {
    IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(stage),
    ))
}

/// Checked counter addition with a family-specific corruption diagnostic.
fn checked_add(left: u64, right: u64, name: &'static str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| corruption(format!("text {name} overflowed")))
}

fn corruption(message: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.into())
}

fn operation_error(error: crate::index_v2::IndexOperationModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

fn work_error(error: crate::index_v2::work::IndexWorkModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

fn coordinator_error(
    error: crate::index_v2::blob_publication::BlobPublicationError,
) -> HelixDbError {
    HelixDbError::InvariantViolation(format!(
        "text partition publication coordination failed: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStoreExt;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::{SearchIndexBackfillLimits, TextAnalyzerKind};
    use crate::encoding::property::property_value::PropertyValue;
    use crate::encoding::property::Property;
    use crate::encoding::v1::keys::index_v2;
    use crate::encoding::v1::keys::index_v2::IndexV2RecordKind;
    use crate::encoding::v1::keys::{EdgePropertyByIdKey, EdgePropertyPairKey, NodePropertyKey};
    use crate::encoding::v1::property::encode_properties;
    use crate::encoding::v1::values::index_v2::{
        decode_index_record, decode_operation_record, decode_work_value,
    };
    use crate::index_v2::blob_publication::{
        BeginBlobDelete, BlobDeleteFenceKey, BlobOperationDuration, BlobPublicationPermit,
        BlobPublicationStatus, BlobPublicationTiming,
    };
    use crate::index_v2::lifecycle::{
        create_index_operation, drop_index_operation, InitialBuildProgress,
    };
    use crate::index_v2::outbox::{
        claim_operation, execute_claimed_step, observe_operation_pointer, ClaimPermission,
        ClaimedOperation, CommittedOperationStep, OperationPointerObservation,
    };
    use crate::index_v2::repository::bootstrap_writer;
    use crate::index_v2::text::reconciliation::CoordinatorTextUploadDriver;
    use crate::index_v2::text::upload_queue::{
        self, TextUploadStepResult, UploadPointerObservation,
    };
    use crate::index_v2::{
        BlobGcRunId, BlobGcRunRevision, BlobPublicationPermitId, BlobRef, ClaimSequence,
        IndexDdlReceipt, IndexGenerationId, IndexId, IndexOperationExecutionState,
        IndexOperationId, IndexOperationKind, IndexOperationModelError, IndexOperationRevision,
        IndexRevision, IndexStateV2, OperationClaim, TextCleanupProgress, TextIntentRevision,
        TextManifestRevision, ValidatedTextIndexDefinition, WriterEpoch,
    };

    const NOW_MILLIS: u64 = 1;

    async fn test_db(name: &str) -> Db {
        let db = Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .expect("text driver test database opens");
        bootstrap_writer(&db)
            .await
            .expect("text driver test database bootstraps V2 metadata");
        db
    }

    /// Opens one named database against a retained object store.
    async fn reopen_test_db(name: &str, store: Arc<dyn slatedb::object_store::ObjectStore>) -> Db {
        Db::builder(name, store)
            .build()
            .await
            .expect("text driver test database reopens")
    }

    /// Installs a fresh process-local lifecycle driver for one reopened handle.
    fn reopened_lifecycle_driver(
        name: &str,
        store: Arc<dyn slatedb::object_store::ObjectStore>,
        reader_leases: Arc<dyn IndexLeaseCoordinator>,
    ) -> TextIndexDriver {
        let coordinator: Arc<dyn BlobPublicationCoordinator> = Arc::new(
            crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator::new(
                Arc::clone(&store),
                name,
                BlobPublicationTiming::default(),
            ),
        );
        TextIndexDriver::with_lifecycle_runtime(
            Arc::new(crate::index_v2::IndexScopeGates::default()),
            coordinator,
            store,
            name,
            SearchIndexBackfillLimits::default().text_compaction(),
            crate::search::text::BlobGcGate::new(),
            Some(reader_leases),
        )
    }

    /// Drives one operation to a terminal result with a new DB/driver each turn.
    async fn drive_to_terminal_reopening(
        name: &str,
        store: Arc<dyn slatedb::object_store::ObjectStore>,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        reader_leases: Arc<dyn IndexLeaseCoordinator>,
    ) -> CommittedOperationStep {
        for _ in 0..96 {
            let db = reopen_test_db(name, Arc::clone(&store)).await;
            let driver =
                reopened_lifecycle_driver(name, Arc::clone(&store), Arc::clone(&reader_leases));
            let step = drive_one_with(
                &db,
                operation_id,
                claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
                &driver,
            )
            .await;
            db.close()
                .await
                .expect("text lifecycle checkpoint database closes");
            if step != CommittedOperationStep::Progressed {
                return step;
            }
        }
        panic!("text lifecycle operation exceeded bounded test checkpoints")
    }

    /// Reads one exact canonical text record after a restart.
    async fn read_index_record(
        db: &Db,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
    ) -> IndexRecordV2 {
        let key = scoped_index_key(scope, IndexV2Key::index_record(definition.identity()));
        let value = db
            .get(key)
            .await
            .expect("canonical text index is readable")
            .expect("canonical text index exists");
        decode_index_record(&value).expect("canonical text index decodes")
    }

    fn definition(
        element_kind: IndexElementKind,
        tenant_property: Option<&str>,
    ) -> ValidatedDynamicIndexDefinition {
        ValidatedDynamicIndexDefinition::Text(
            ValidatedTextIndexDefinition::try_new(
                element_kind,
                "Document",
                "body",
                tenant_property,
                TextAnalyzerKind::Standard,
                false,
            )
            .expect("text definition validates"),
        )
    }

    fn properties(
        label: &str,
        body: Option<PropertyValue>,
        tenant: Option<PropertyValue>,
    ) -> Vec<Property> {
        let mut properties = vec![Property::new(
            "$label",
            PropertyValue::String(label.to_string()),
        )];
        properties.extend(body.map(|body| Property::new("body", body)));
        properties.extend(tenant.map(|tenant| Property::new("account_id", tenant)));
        properties
    }

    fn source_key(scope: DataScope, kind: IndexElementKind, entity_id: u64) -> Bytes {
        match kind {
            IndexElementKind::Node => Key::Data {
                scope,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
            }
            .to_bytes(),
            IndexElementKind::Edge => Key::Data {
                scope,
                kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(entity_id)),
            }
            .to_bytes(),
        }
    }

    fn source_cursor(scope: DataScope, kind: IndexElementKind, entity_id: u64) -> IndexCursor {
        IndexCursor::try_new(source_key(scope, kind, entity_id))
            .expect("typed source key is a valid cursor")
    }

    async fn put_source(
        db: &Db,
        scope: DataScope,
        kind: IndexElementKind,
        entity_id: u64,
        properties: &[Property],
    ) {
        db.put(
            source_key(scope, kind, entity_id),
            encode_properties(properties),
        )
        .await
        .expect("text source row is written");
    }

    fn build_delta_key(
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
        entity: IndexEntity,
    ) -> Bytes {
        scoped_index_key(
            scope,
            IndexV2Key::BuildDelta(IndexEntityStateKey {
                index_id,
                generation,
                entity,
            }),
        )
    }

    async fn put_build_delta(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
        entity: IndexEntity,
    ) {
        db.put(
            build_delta_key(scope, index_id, generation, entity),
            encode_work_value(&IndexV2WorkValue::CoalescedBuildDelta(
                CoalescedBuildDeltaValue {
                    index_id,
                    generation,
                    entity_kind: entity.kind,
                    entity_id: entity.id,
                },
            )),
        )
        .await
        .expect("text build delta is written");
    }

    async fn read_text_applied(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
        entity: IndexEntity,
    ) -> Option<(TextPartition, TextLogicalVersion)> {
        let key = scoped_index_key(
            scope,
            IndexV2Key::AppliedState(IndexEntityStateKey {
                index_id,
                generation,
                entity,
            }),
        );
        let value = db.get(key).await.expect("text applied state is readable")?;
        let IndexV2WorkValue::AppliedEntityState(value) =
            decode_work_value(&value).expect("text applied state decodes")
        else {
            panic!("text applied-state key contains another work value");
        };
        let AppliedFamilyState::Text(state) = value.state else {
            panic!("text applied state contains another family");
        };
        state
    }

    /// Reads and validates one exact partition manifest root for assertions.
    async fn read_manifest_root(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
        partition: &TextPartition,
    ) -> Option<work::TextManifestRootValue> {
        let key = scoped_index_key(
            scope,
            IndexV2Key::TextManifestRoot(TextManifestRootKey {
                index_id,
                generation,
                partition: partition.fingerprint(),
            }),
        );
        let value = db.get(key).await.expect("manifest root is readable")?;
        let IndexV2WorkValue::TextManifestRoot(root) =
            decode_work_value(&value).expect("manifest root decodes")
        else {
            panic!("manifest root key contains another work value");
        };
        Some(root)
    }

    async fn create_build(
        db: &Db,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
        upper_entity_id: u64,
    ) -> (IndexOperationId, IndexId, IndexGenerationId) {
        let element_kind = definition.identity().element_kind();
        let receipt = create_index_operation(
            db,
            scope,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
            InitialBuildProgress::text(source_cursor(scope, element_kind, upper_entity_id)),
        )
        .await
        .expect("text build is enqueued");
        let IndexDdlReceipt::Accepted {
            operation_id,
            index_id,
            generation,
        } = receipt
        else {
            panic!("new text definition must enqueue a build");
        };
        (operation_id, index_id, generation)
    }

    async fn drive_one(
        db: &Db,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
    ) -> CommittedOperationStep {
        drive_one_with(
            db,
            operation_id,
            claim_sequence,
            limits,
            &TextIndexDriver::new(),
        )
        .await
    }

    async fn drive_one_with(
        db: &Db,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
        driver: &TextIndexDriver,
    ) -> CommittedOperationStep {
        let claimed = claim_one(db, operation_id, claim_sequence).await;
        execute_claimed_step(db, &claimed, driver, limits, NOW_MILLIS)
            .await
            .expect("text step commits")
    }

    async fn claim_one(
        db: &Db,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
    ) -> ClaimedOperation {
        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        let observation = observe_operation_pointer(db, operation_id, writer_epoch, NOW_MILLIS)
            .await
            .expect("text operation pointer is readable");
        let OperationPointerObservation::Eligible(eligible) = observation else {
            panic!("queued text operation must be eligible: {observation:?}");
        };
        let sequence = ClaimSequence::new(*claim_sequence).expect("claim sequence is non-zero");
        *claim_sequence = claim_sequence
            .checked_add(1)
            .expect("claim sequence remains bounded");
        claim_operation(
            db,
            &eligible,
            writer_epoch,
            sequence,
            NOW_MILLIS,
            ClaimPermission::Normal,
        )
        .await
        .expect("text claim succeeds")
        .expect("text operation revision is claimable")
    }

    fn publication_driver(
        name: &str,
    ) -> (
        TextIndexDriver,
        Arc<crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator>,
    ) {
        publication_driver_with_timing(name, BlobPublicationTiming::default())
    }

    /// Builds a text/upload driver pair with an explicit coordinator clock policy.
    fn publication_driver_with_timing(
        name: &str,
        timing: BlobPublicationTiming,
    ) -> (
        TextIndexDriver,
        Arc<crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator>,
    ) {
        let (driver, coordinator, _) = publication_runtime_with_timing(name, timing);
        (driver, coordinator)
    }

    /// Builds a complete test runtime while retaining direct object-store access.
    fn publication_runtime_with_timing(
        name: &str,
        timing: BlobPublicationTiming,
    ) -> (
        TextIndexDriver,
        Arc<crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator>,
        Arc<dyn slatedb::object_store::ObjectStore>,
    ) {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let coordinator = Arc::new(
            crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator::new(
                Arc::clone(&store),
                name,
                timing,
            ),
        );
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let driver = TextIndexDriver::with_publication_runtime(
            dependency,
            Arc::clone(&store),
            name,
            SearchIndexBackfillLimits::default().text_compaction(),
            crate::search::text::BlobGcGate::new(),
        );
        (driver, coordinator, store)
    }

    #[tokio::test]
    async fn owner_retirement_requires_and_retains_the_blob_deletion_gate() {
        let scope = DataScope::LegacyUnscoped;
        let run_id = BlobGcRunId::from_bytes([0x63; 16]).expect("run ID is non-nil");
        let operation = IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([0x64; 16]).expect("operation ID is non-nil"),
            IndexId::initial(),
            definition(IndexElementKind::Node, None).identity().clone(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Drop,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextCleanup(
                crate::index_v2::TextCleanupProgress::RetireManifest(crate::index_v2::GcProgress {
                    gc_run_id: Some(run_id),
                    candidate_cursor: Some(
                        IndexCursor::try_new(Bytes::from_static(b"candidate"))
                            .expect("test cursor is non-empty"),
                    ),
                    stage_cursor: None,
                    counters: OperationCounters::default(),
                }),
            ),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("text cleanup operation validates");

        let Err(error) = TextIndexDriver::new()
            .acquire_step_permit(scope, &operation)
            .await
        else {
            panic!("source-only driver cannot retire blob owners");
        };
        assert!(matches!(
            error,
            HelixDbError::IndexLifecycleUnavailable {
                family: crate::error::IndexFamily::Text,
                reason:
                    crate::error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
            }
        ));

        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let coordinator: Arc<dyn BlobPublicationCoordinator> = Arc::new(
            crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator::new(
                Arc::clone(&store),
                "text-owner-retirement-gate",
                BlobPublicationTiming::default(),
            ),
        );
        let gate = crate::search::text::BlobGcGate::new();
        let publication = gate.acquire_publication().await;
        let driver = Arc::new(TextIndexDriver::with_publication_runtime(
            coordinator,
            store,
            "text-owner-retirement-gate",
            SearchIndexBackfillLimits::default().text_compaction(),
            gate,
        ));
        let waiting_driver = Arc::clone(&driver);
        let waiting_operation = operation.clone();
        let mut waiting = tokio::spawn(async move {
            waiting_driver
                .acquire_step_permit(scope, &waiting_operation)
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
                .await
                .is_err(),
            "owner retirement waits for an in-flight same-writer publication"
        );
        drop(publication);
        let permit = waiting
            .await
            .expect("permit task joins")
            .expect("deletion permit is acquired after publication drains");

        let next_publication = {
            let gate = match &driver.publication {
                TextPublicationRuntime::Installed(runtime) => runtime.gc_gate.clone(),
                TextPublicationRuntime::Unavailable => {
                    panic!("test driver retains its publication runtime")
                }
            };
            let mut waiting = tokio::spawn(async move { gate.acquire_publication().await });
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
                    .await
                    .is_err(),
                "returned operation permit retains blob deletion exclusion"
            );
            waiting
        };
        drop(permit);
        next_publication
            .await
            .expect("publication permit task joins after cleanup permit drops");
    }

    #[tokio::test]
    async fn root_observers_do_not_reacquire_the_blob_deletion_gate() {
        let scope = DataScope::LegacyUnscoped;
        let run_id = BlobGcRunId::from_bytes([0x65; 16]).expect("run ID is non-nil");
        let progress = crate::index_v2::GcProgress {
            gc_run_id: Some(run_id),
            candidate_cursor: Some(
                IndexCursor::try_new(Bytes::from_static(b"candidate"))
                    .expect("test cursor is non-empty"),
            ),
            stage_cursor: None,
            counters: OperationCounters::default(),
        };
        let operation = |stage| {
            IndexOperationRecord::try_new(
                IndexOperationId::from_bytes([0x66; 16]).expect("operation ID is non-nil"),
                IndexId::initial(),
                definition(IndexElementKind::Node, None).identity().clone(),
                IndexGenerationId::initial(),
                IndexRevision::initial(),
                IndexOperationRevision::initial(),
                IndexOperationKind::Drop,
                IndexOperationFamily::Text,
                IndexOperationProgress::TextCleanup(stage),
                0,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .expect("text cleanup operation validates")
        };

        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let coordinator: Arc<dyn BlobPublicationCoordinator> = Arc::new(
            crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator::new(
                Arc::clone(&store),
                "text-root-observer-gate",
                BlobPublicationTiming::default(),
            ),
        );
        let gate = crate::search::text::BlobGcGate::new();
        let root_pass = gate.acquire_deletion().await;
        let driver = TextIndexDriver::with_publication_runtime(
            coordinator,
            store,
            "text-root-observer-gate",
            SearchIndexBackfillLimits::default().text_compaction(),
            gate,
        );

        for stage in [
            crate::index_v2::TextCleanupProgress::MarkReachability(progress.clone()),
            crate::index_v2::TextCleanupProgress::DeleteBlobs(progress.clone()),
        ] {
            let permit = tokio::time::timeout(
                std::time::Duration::from_millis(20),
                driver.acquire_step_permit(scope, &operation(stage)),
            )
            .await
            .expect("root observer must not wait on its worker-owned deletion gate")
            .expect("root observer acquires its scope permit");
            drop(permit);
        }
        drop(root_pass);
    }

    /// Minimal published artifact and runtime needed to exercise manifest fencing.
    struct ManifestPreparationFixture {
        db: Db,
        operation: IndexOperationRecord,
        progress: PrefixScanProgress,
        driver: TextIndexDriver,
        coordinator: Arc<crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator>,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        db_path: String,
        blob: BlobRef,
    }

    /// Publishes one exact artifact with its canonical reachability owner.
    async fn manifest_preparation_fixture(name: &str, seed: u8) -> ManifestPreparationFixture {
        let db = test_db(name).await;
        let scope = DataScope::LegacyUnscoped;
        let progress = PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        };
        let operation = IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([seed; 16]).expect("operation ID is non-nil"),
            IndexId::initial(),
            definition(IndexElementKind::Node, None).identity().clone(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::PrepareManifests(progress.clone()),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("manifest preparation operation is valid");
        let payload = Bytes::from(vec![seed; 128]);
        let blob = BlobRef::new(
            Sha256::digest(&payload).into(),
            u64::try_from(payload.len()).expect("test payload length fits u64"),
        );
        let split = work::SplitRef::try_new(blob, 80, 16, 4, blob.size())
            .expect("test split metadata is valid");
        let artifact_owner = TextBuildArtifactKey {
            root: TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: TextPartition::Unpartitioned.fingerprint(),
            },
            ordinal: 0,
        };
        let artifact_key = scoped_index_key(scope, IndexV2Key::TextBuildArtifact(artifact_owner));
        let intent_id = TextUploadIntentId::from_bytes([seed.saturating_add(1); 16])
            .expect("intent ID is non-nil");
        let artifact_value = encode_work_value(&IndexV2WorkValue::TextBuildArtifact(
            work::TextBuildArtifactValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: TextPartition::Unpartitioned,
                artifact_ordinal: 0,
                split,
                source_intent_id: intent_id,
            },
        ));
        let (reference_key, reference_value) =
            super::super::attachment::build_artifact_reachability_row(blob, scope, artifact_owner);
        db.put(artifact_key, artifact_value)
            .await
            .expect("test artifact is written");
        db.put(reference_key, reference_value)
            .await
            .expect("test artifact reachability is written");

        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db_path = format!("{name}-blobs");
        let coordinator = Arc::new(
            crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator::new(
                Arc::clone(&object_store),
                db_path.clone(),
                BlobPublicationTiming::default(),
            ),
        );
        let permit = coordinator
            .reserve(
                blob,
                intent_id,
                WriterEpoch::from_bytes([seed.saturating_add(2); 16])
                    .expect("writer epoch is non-nil"),
            )
            .await
            .expect("test artifact publication is reserved");
        assert!(matches!(
            coordinator.publish(&permit, payload).await,
            Ok(BlobPublicationStatus::Succeeded(_))
        ));
        coordinator
            .release(
                &permit,
                BlobPermitReleaseAuthority::reference_committed(permit.id()),
            )
            .await
            .expect("durable artifact reference releases its permit");
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let driver = TextIndexDriver::with_publication_runtime(
            dependency,
            Arc::clone(&object_store),
            db_path.clone(),
            SearchIndexBackfillLimits::default().text_compaction(),
            crate::search::text::BlobGcGate::new(),
        );
        ManifestPreparationFixture {
            db,
            operation,
            progress,
            driver,
            coordinator,
            object_store,
            db_path,
            blob,
        }
    }

    /// Claims and executes one exact upload reconciliation checkpoint.
    async fn drive_upload_one(
        db: &Db,
        intent_id: TextUploadIntentId,
        writer_epoch: WriterEpoch,
        sequence: u64,
        driver: &CoordinatorTextUploadDriver,
    ) -> TextUploadStepResult {
        let active_mutations =
            crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let observation = upload_queue::observe_upload_pointer(
            db,
            intent_id,
            &active_mutations,
            writer_epoch,
            u64::MAX,
        )
        .await
        .expect("upload pointer is observable");
        let UploadPointerObservation::Eligible(eligible) = observation else {
            panic!("queued build upload must be eligible: {observation:?}");
        };
        let claimed = upload_queue::claim_upload(
            db,
            &eligible,
            &active_mutations,
            writer_epoch,
            ClaimSequence::new(sequence).expect("upload claim sequence is non-zero"),
            u64::MAX,
            ClaimPermission::Normal,
        )
        .await
        .expect("upload claim succeeds")
        .expect("upload revision is claimable");
        upload_queue::execute_claimed_upload_step(db, &claimed, driver, u64::MAX)
            .await
            .expect("upload checkpoint commits")
    }

    /// Completes one initial split and leaves the build at its first catch-up pass.
    async fn advance_single_partition_to_catch_up(
        db: &Db,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
        driver: &TextIndexDriver,
        coordinator: Arc<crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator>,
    ) {
        assert_eq!(
            drive_one_with(db, operation_id, claim_sequence, limits, driver).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one_with(db, operation_id, claim_sequence, limits, driver).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one_with(db, operation_id, claim_sequence, limits, driver).await,
            CommittedOperationStep::Progressed
        );
        let operation = read_operation(db, DataScope::LegacyUnscoped, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitUpload(wait),
        )) = operation.progress()
        else {
            panic!("initial text partition must wait on one upload");
        };
        let wait = wait.clone();
        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let upload_driver = CoordinatorTextUploadDriver::new(dependency);
        for (sequence, expected) in [
            (300, TextUploadStepResult::PublicationSucceeded),
            (301, TextUploadStepResult::AttachUploaded),
            (302, TextUploadStepResult::ReferenceReleased),
        ] {
            assert_eq!(
                drive_upload_one(db, wait.intent_id(), writer_epoch, sequence, &upload_driver)
                    .await,
                expected
            );
        }
        assert_eq!(
            drive_one_with(db, operation_id, claim_sequence, limits, driver).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one_with(db, operation_id, claim_sequence, limits, driver).await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(db, DataScope::LegacyUnscoped, operation_id)
                .await
                .progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(_)
            ))
        ));
    }

    /// Commits one missing empty root without consuming its partition cursor.
    async fn seed_next_partition_manifest_root(
        db: &Db,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
        driver: &TextIndexDriver,
    ) {
        let before = read_operation(db, DataScope::LegacyUnscoped, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(before),
        )) = before.progress()
        else {
            panic!("empty-root seeding requires partition-scan progress");
        };
        assert_eq!(
            drive_one_with(db, operation_id, claim_sequence, limits, driver).await,
            CommittedOperationStep::Progressed
        );
        let after = read_operation(db, DataScope::LegacyUnscoped, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(after),
        )) = after.progress()
        else {
            panic!("empty-root seeding retains partition-scan progress");
        };
        assert_eq!(after.cursor, before.cursor);
        assert_eq!(
            after.counters.output_operations,
            before.counters.output_operations + 1
        );
        assert!(after.counters.input_bytes > before.counters.input_bytes);
        assert!(after.counters.output_bytes > before.counters.output_bytes);
    }

    /// Completes the exact build upload currently anchoring an operation wait.
    async fn complete_current_build_upload(
        db: &Db,
        operation_id: IndexOperationId,
        upload_sequence: &mut u64,
        coordinator: Arc<crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator>,
    ) {
        let operation = read_operation(db, DataScope::LegacyUnscoped, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(stage)) =
            operation.progress()
        else {
            panic!("text build upload helper requires constructing progress");
        };
        let intent_id = match stage {
            TextBuildStage::AwaitUpload(progress) => progress.intent_id(),
            TextBuildStage::AwaitCatchUpUpload(progress) => progress.intent_id(),
            TextBuildStage::AwaitCompactionUpload(progress) => progress.intent_id(),
            TextBuildStage::ScanSource(_)
            | TextBuildStage::ScanPartitions(_)
            | TextBuildStage::CatchUp(_)
            | TextBuildStage::Compact(_)
            | TextBuildStage::PrepareManifests(_)
            | TextBuildStage::ValidateManifests(_)
            | TextBuildStage::Activate(_) => {
                panic!("text build upload helper requires an exact upload wait")
            }
        };
        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let upload_driver = CoordinatorTextUploadDriver::new(dependency);
        for expected in [
            TextUploadStepResult::PublicationSucceeded,
            TextUploadStepResult::AttachUploaded,
            TextUploadStepResult::ReferenceReleased,
        ] {
            assert_eq!(
                drive_upload_one(
                    db,
                    intent_id,
                    writer_epoch,
                    *upload_sequence,
                    &upload_driver,
                )
                .await,
                expected
            );
            *upload_sequence = upload_sequence
                .checked_add(1)
                .expect("test upload sequence remains bounded");
        }
    }

    /// Drives one build, including its upload children, to page validation.
    async fn advance_build_to_validation(
        db: &Db,
        operation_id: IndexOperationId,
        operation_sequence: &mut u64,
        upload_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
        driver: &TextIndexDriver,
        coordinator: Arc<crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator>,
    ) -> IndexOperationRecord {
        for _ in 0..64 {
            let operation = read_operation(db, DataScope::LegacyUnscoped, operation_id).await;
            let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(stage)) =
                operation.progress()
            else {
                panic!("text build must remain constructing before validation")
            };
            match stage {
                TextBuildStage::ValidateManifests(_) => return operation,
                TextBuildStage::AwaitUpload(progress) => {
                    complete_or_reconcile_current_build_upload(
                        db,
                        operation_id,
                        progress.intent_id(),
                        operation_sequence,
                        upload_sequence,
                        limits,
                        driver,
                        Arc::clone(&coordinator),
                    )
                    .await;
                }
                TextBuildStage::AwaitCatchUpUpload(progress) => {
                    complete_or_reconcile_current_build_upload(
                        db,
                        operation_id,
                        progress.intent_id(),
                        operation_sequence,
                        upload_sequence,
                        limits,
                        driver,
                        Arc::clone(&coordinator),
                    )
                    .await;
                }
                TextBuildStage::AwaitCompactionUpload(progress) => {
                    complete_or_reconcile_current_build_upload(
                        db,
                        operation_id,
                        progress.intent_id(),
                        operation_sequence,
                        upload_sequence,
                        limits,
                        driver,
                        Arc::clone(&coordinator),
                    )
                    .await;
                }
                TextBuildStage::ScanSource(_)
                | TextBuildStage::ScanPartitions(_)
                | TextBuildStage::CatchUp(_)
                | TextBuildStage::Compact(_)
                | TextBuildStage::PrepareManifests(_)
                | TextBuildStage::Activate(_) => {
                    assert_eq!(
                        drive_one_with(db, operation_id, operation_sequence, limits, driver).await,
                        CommittedOperationStep::Progressed
                    );
                }
            }
        }
        panic!("text build did not reach validation within its bounded test steps")
    }

    /// Completes a present upload child or reconciles its removed checkpoint.
    #[allow(
        clippy::too_many_arguments,
        reason = "the test helper binds both operation and upload sequence owners"
    )]
    async fn complete_or_reconcile_current_build_upload(
        db: &Db,
        operation_id: IndexOperationId,
        intent_id: TextUploadIntentId,
        operation_sequence: &mut u64,
        upload_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
        driver: &TextIndexDriver,
        coordinator: Arc<crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator>,
    ) {
        if crate::index_v2::repository::load_upload_from_pointer(db, intent_id)
            .await
            .unwrap()
            .is_some()
        {
            complete_current_build_upload(db, operation_id, upload_sequence, coordinator).await;
        } else {
            assert_eq!(
                drive_one_with(db, operation_id, operation_sequence, limits, driver).await,
                CommittedOperationStep::Progressed
            );
        }
    }

    /// Reads the first manifest page owner and blob from one exact generation.
    async fn first_manifest_page_blob(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> (
        crate::encoding::v1::keys::index_v2::TextManifestPageKey,
        BlobRef,
    ) {
        let prefix = Key::data_prefix(
            scope,
            IndexV2Key::generation_prefix(
                IndexV2RecordKind::TextManifestPage,
                index_id,
                generation,
            ),
        );
        let mut rows = db.scan_prefix(prefix, ..).await.unwrap();
        let row = rows.next().await.unwrap().expect("manifest page exists");
        let Key::Data {
            kind: DataKeyKind::IndexV2(IndexV2Key::TextManifestPage(key)),
            ..
        } = Key::parse_from_slice(scope, &row.key).unwrap()
        else {
            panic!("manifest-page prefix yielded another key kind")
        };
        let IndexV2WorkValue::TextManifestPage(page) = decode_work_value(&row.value).unwrap()
        else {
            panic!("manifest-page key contains another work value")
        };
        (key, page.entries()[0].blob())
    }

    /// Produces two same-partition artifacts and stops at the compaction stage.
    async fn advance_two_splits_to_compact(
        db: &Db,
        operation_id: IndexOperationId,
        operation_sequence: &mut u64,
        upload_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
        driver: &TextIndexDriver,
        coordinator: Arc<crate::index_v2::blob_publication::ProcessLocalBlobPublicationCoordinator>,
    ) {
        for _ in 0..4 {
            assert_eq!(
                drive_one_with(db, operation_id, operation_sequence, limits, driver).await,
                CommittedOperationStep::Progressed
            );
        }
        for _ in 0..2 {
            assert_eq!(
                drive_one_with(db, operation_id, operation_sequence, limits, driver).await,
                CommittedOperationStep::Progressed
            );
            complete_current_build_upload(
                db,
                operation_id,
                upload_sequence,
                Arc::clone(&coordinator),
            )
            .await;
            assert_eq!(
                drive_one_with(db, operation_id, operation_sequence, limits, driver).await,
                CommittedOperationStep::Progressed
            );
        }
        assert_eq!(
            drive_one_with(db, operation_id, operation_sequence, limits, driver).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one_with(db, operation_id, operation_sequence, limits, driver).await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(db, DataScope::LegacyUnscoped, operation_id)
                .await
                .progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Compact(_)
            ))
        ));
    }

    /// Completes a source pass with no staged partition and enters catch-up.
    async fn advance_empty_source_to_catch_up(
        db: &Db,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
        driver: &TextIndexDriver,
    ) {
        for _ in 0..3 {
            assert_eq!(
                drive_one_with(db, operation_id, claim_sequence, limits, driver).await,
                CommittedOperationStep::Progressed
            );
        }
        assert!(matches!(
            read_operation(db, DataScope::LegacyUnscoped, operation_id)
                .await
                .progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(_)
            ))
        ));
    }

    /// Commits one prepared build child with its exact next operation revision.
    async fn commit_prepared_build_child(
        db: &Db,
        scope: DataScope,
        claimed: &ClaimedOperation,
        prepared: &PreparedTextOperationStep,
    ) -> IndexOperationRecord {
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("operation/child transaction opens");
        let IndexOperationStepResult::Progressed(next_progress) = prepared
            .stage(&transaction, scope, &claimed.record)
            .await
            .expect("prepared child stages")
        else {
            panic!("prepared build child progresses its operation");
        };
        let next = claimed
            .record
            .progressed(next_progress)
            .expect("claimed operation advances");
        transaction
            .put(
                scoped_index_key(scope, IndexV2Key::operation(next.operation_id())),
                crate::encoding::v1::values::index_v2::encode_operation_record(&next),
            )
            .expect("next operation is staged");
        transaction
            .put(
                Key::Global {
                    kind: GlobalKeyKind::IndexV2(GlobalIndexV2Key::OperationPointer(
                        next.operation_id(),
                    )),
                }
                .to_bytes(),
                crate::encoding::v1::values::index_v2::encode_metadata_value(
                    &crate::index_v2::IndexV2MetadataValue::OperationQueuePointer(
                        crate::index_v2::OperationQueuePointerValue {
                            scope,
                            index_id: next.index_id(),
                            generation: next.generation(),
                            record_revision: next.operation_revision(),
                        },
                    ),
                ),
            )
            .expect("next operation pointer is staged");
        transaction
            .commit()
            .await
            .expect("operation and child commit atomically");
        next
    }

    async fn read_operation(
        db: &Db,
        scope: DataScope,
        operation_id: IndexOperationId,
    ) -> IndexOperationRecord {
        let key = scoped_index_key(scope, IndexV2Key::operation(operation_id));
        let value = db
            .get(key)
            .await
            .expect("text operation row is readable")
            .expect("text operation row exists");
        decode_operation_record(&value).expect("text operation row decodes")
    }

    async fn entity_state_rows(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> Vec<(TextEntityStateKey, TextEntityStateValue)> {
        let prefix = Key::data_prefix(
            scope,
            IndexV2Key::generation_prefix(IndexV2RecordKind::TextEntityState, index_id, generation),
        );
        let mut rows = db
            .scan_prefix(prefix, ..)
            .await
            .expect("text entity-state prefix is readable");
        let mut decoded = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .expect("text entity-state row is readable")
        {
            let Key::Data {
                kind: DataKeyKind::IndexV2(IndexV2Key::TextEntityState(key)),
                ..
            } = Key::parse_from_slice(scope, &row.key).expect("text entity-state key decodes")
            else {
                panic!("text entity-state prefix yielded another key kind");
            };
            let IndexV2WorkValue::TextEntityState(value) =
                decode_work_value(&row.value).expect("text entity-state value decodes")
            else {
                panic!("text entity-state key contained another value kind");
            };
            decoded.push((key, value));
        }
        decoded
    }

    /// Returns the default transaction policy with one entity admitted per scan.
    fn single_entity_batch_limits() -> SearchIndexBatchLimits {
        let defaults = SearchIndexBackfillLimits::default().batch();
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            defaults.max_input_bytes(),
            defaults.max_output_operations(),
            defaults.max_output_bytes(),
            defaults.max_single_vector_output_bytes(),
        )
        .expect("one-entity text batches satisfy the default byte limits")
    }

    /// Counts exact generation rows for one typed V2 work kind.
    async fn generation_row_count(
        db: &Db,
        scope: DataScope,
        kind: IndexV2RecordKind,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> usize {
        let prefix = Key::data_prefix(
            scope,
            IndexV2Key::generation_prefix(kind, index_id, generation),
        );
        let mut rows = db
            .scan_prefix(prefix, ..)
            .await
            .expect("typed generation prefix is readable");
        let mut count = 0_usize;
        while rows
            .next()
            .await
            .expect("typed generation row is readable")
            .is_some()
        {
            count = count.checked_add(1).expect("test row count is bounded");
        }
        count
    }

    /// Extracts the stable reason from a fail-closed text-driver error.
    fn corruption_reason(error: HelixDbError) -> String {
        let HelixDbError::IndexCatalogCorruption(reason) = error else {
            panic!("text-driver corruption guard returns catalog corruption")
        };
        reason
    }

    #[tokio::test]
    async fn compaction_publishes_replacement_before_retiring_exact_inputs() {
        let db = test_db("text-compaction-durable-child").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for entity_id in [1, 2] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!("document {entity_id}"))),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let (driver, coordinator) = publication_driver("text-compaction-durable-child-blobs");
        let limits = single_entity_batch_limits();
        let mut operation_sequence = 1;
        let mut upload_sequence = 700;
        advance_two_splits_to_compact(
            &db,
            operation_id,
            &mut operation_sequence,
            &mut upload_sequence,
            limits,
            &driver,
            Arc::clone(&coordinator),
        )
        .await;
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextBuildArtifact,
                index_id,
                generation,
            )
            .await,
            2
        );

        assert_eq!(
            drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        let waiting = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitCompactionUpload(wait),
        )) = waiting.progress()
        else {
            panic!("useful compaction must persist its exact replacement child");
        };
        assert_eq!(wait.input_artifact_keys().len(), 2);
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextBuildArtifact,
                index_id,
                generation,
            )
            .await,
            2,
            "pre-publication inputs remain authoritative"
        );

        complete_current_build_upload(
            &db,
            operation_id,
            &mut upload_sequence,
            Arc::clone(&coordinator),
        )
        .await;
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextBuildArtifact,
                index_id,
                generation,
            )
            .await,
            3,
            "attachment cannot retire compaction inputs before operation reconciliation"
        );
        assert_eq!(
            drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextBuildArtifact,
                index_id,
                generation,
            )
            .await,
            1
        );
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::BlobGcCandidate,
                index_id,
                generation,
            )
            .await,
            2
        );
        assert_eq!(
            read_manifest_root(
                &db,
                scope,
                index_id,
                generation,
                &TextPartition::Unpartitioned,
            )
            .await,
            Some(work::TextManifestRootValue::empty(
                index_id,
                generation,
                TextPartition::Unpartitioned,
            ))
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Compact(_)
            ))
        ));
        assert_eq!(
            drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::PrepareManifests(_)
            ))
        ));
        assert_eq!(
            drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextBuildArtifact,
                index_id,
                generation,
            )
            .await,
            0
        );
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextManifestRoot,
                index_id,
                generation,
            )
            .await,
            1
        );
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextManifestPage,
                index_id,
                generation,
            )
            .await,
            1
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::PrepareManifests(PrefixScanProgress {
                    cursor: Some(_),
                    ..
                })
            ))
        ));
    }

    #[tokio::test]
    async fn manifest_reference_guard_blocks_delete_and_closed_fence_retries() {
        let fixture = manifest_preparation_fixture("text-manifest-reference-fence", 0x51).await;
        let TextPublicationRuntime::Installed(runtime) = &fixture.driver.publication else {
            panic!("manifest fixture has a complete publication runtime");
        };
        let prepared = prepare_manifest_step(
            &fixture.db,
            DataScope::LegacyUnscoped,
            &fixture.operation,
            &fixture.progress,
            SearchIndexBackfillLimits::default().batch(),
            runtime,
        )
        .await
        .expect("published artifact prepares one fenced manifest page");
        let PreparedTextOperationStep::ManifestPage(prepared) = prepared else {
            panic!("published artifact owns a manifest relocation guard");
        };
        let fence_key = BlobDeleteFenceKey::new(
            fixture.blob,
            BlobGcRunId::from_bytes([0x52; 16]).expect("GC run ID is non-nil"),
        );
        let BeginBlobDelete::Acquired(fence) = fixture
            .coordinator
            .begin_delete(fence_key)
            .await
            .expect("delete fence closes the exact blob")
        else {
            panic!("fresh blob has no competing delete run");
        };
        assert!(!fixture
            .coordinator
            .check_quiescent(&fence)
            .await
            .expect("manifest reference guard is observable"));
        drop(prepared);
        assert!(fixture
            .coordinator
            .check_quiescent(&fence)
            .await
            .expect("discarded preparation releases its reference guard"));

        let retry = prepare_manifest_step(
            &fixture.db,
            DataScope::LegacyUnscoped,
            &fixture.operation,
            &fixture.progress,
            SearchIndexBackfillLimits::default().batch(),
            runtime,
        )
        .await
        .expect("a closed delete fence becomes retryable operation work");
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("transient manifest transaction opens");
        assert!(matches!(
            retry
                .stage(&transaction, DataScope::LegacyUnscoped, &fixture.operation,)
                .await
                .expect("closed fence stages no physical work"),
            IndexOperationStepResult::TransientFailure
        ));
        assert_eq!(
            generation_row_count(
                &fixture.db,
                DataScope::LegacyUnscoped,
                IndexV2RecordKind::TextManifestPage,
                fixture.operation.index_id(),
                fixture.operation.generation(),
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn complete_text_build_validates_every_lane_before_canonical_activation() {
        let db = test_db("text-validation-activation").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("validated activation".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let (driver, coordinator, _) = publication_runtime_with_timing(
            "text-validation-activation-blobs",
            BlobPublicationTiming::default(),
        );
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut operation_sequence = 1;
        let mut upload_sequence = 1_200;
        let validation = advance_build_to_validation(
            &db,
            operation_id,
            &mut operation_sequence,
            &mut upload_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;
        assert!(matches!(
            validation.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ValidateManifests(TextManifestValidationProgress::Pages(_))
            ))
        ));

        let mut saw_roots = false;
        let mut saw_upload_intents = false;
        let mut saw_activate = false;
        for _ in 0..12 {
            let operation = read_operation(&db, scope, operation_id).await;
            if matches!(
                operation.execution_state(),
                IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                    BuildOperationOutcome::Succeeded
                ))
            ) {
                break;
            }
            let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(stage)) =
                operation.progress()
            else {
                panic!("activation test remains in constructing progress")
            };
            match stage {
                TextBuildStage::ValidateManifests(TextManifestValidationProgress::Roots(_)) => {
                    saw_roots = true;
                }
                TextBuildStage::ValidateManifests(
                    TextManifestValidationProgress::UploadIntents(_),
                ) => saw_upload_intents = true,
                TextBuildStage::Activate(_) => saw_activate = true,
                TextBuildStage::ValidateManifests(TextManifestValidationProgress::Pages(_)) => {}
                TextBuildStage::ScanSource(_)
                | TextBuildStage::ScanPartitions(_)
                | TextBuildStage::AwaitUpload(_)
                | TextBuildStage::CatchUp(_)
                | TextBuildStage::AwaitCatchUpUpload(_)
                | TextBuildStage::Compact(_)
                | TextBuildStage::AwaitCompactionUpload(_)
                | TextBuildStage::PrepareManifests(_) => {
                    panic!("validation must not regress without late build work")
                }
            }
            let committed =
                drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver).await;
            assert!(matches!(
                committed,
                CommittedOperationStep::Progressed | CommittedOperationStep::Completed
            ));
        }
        let completed = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            completed.execution_state(),
            IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Succeeded
            ))
        ));
        assert!(saw_roots && saw_upload_intents && saw_activate);

        let canonical_key = scoped_index_key(
            scope,
            IndexV2Key::index_record(definition.identity().clone()),
        );
        let canonical = decode_index_record(
            &db.get(canonical_key)
                .await
                .unwrap()
                .expect("active canonical record exists"),
        )
        .unwrap();
        assert!(matches!(
            canonical.state(),
            IndexStateV2::Active {
                completed_build_operation_id,
                ..
            } if *completed_build_operation_id == operation_id
        ));
        for kind in [
            IndexV2RecordKind::BuildDelta,
            IndexV2RecordKind::TextBuildArtifact,
            IndexV2RecordKind::TextUploadIntent,
            IndexV2RecordKind::ActiveMutationCommitProof,
        ] {
            assert_eq!(
                generation_row_count(&db, scope, kind, index_id, generation).await,
                0
            );
        }
    }

    #[tokio::test]
    async fn page_validation_maps_missing_objects_and_delete_fences_without_advancing() {
        for (name, close_delete_fence, expected) in [
            (
                "text-validation-missing-object",
                false,
                CommittedOperationStep::Blocked,
            ),
            (
                "text-validation-delete-fence",
                true,
                CommittedOperationStep::TransientFailure,
            ),
        ] {
            let db = test_db(name).await;
            let scope = DataScope::LegacyUnscoped;
            let definition = definition(IndexElementKind::Node, None);
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                1,
                &properties(
                    "Document",
                    Some(PropertyValue::String(name.to_string())),
                    None,
                ),
            )
            .await;
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, 1).await;
            let (driver, coordinator, store) = publication_runtime_with_timing(
                &format!("{name}-blobs"),
                BlobPublicationTiming::default(),
            );
            let limits = SearchIndexBackfillLimits::default().batch();
            let mut operation_sequence = 1;
            let mut upload_sequence = 1_300;
            let before = advance_build_to_validation(
                &db,
                operation_id,
                &mut operation_sequence,
                &mut upload_sequence,
                limits,
                &driver,
                Arc::clone(&coordinator),
            )
            .await;
            let (_, blob) = first_manifest_page_blob(&db, scope, index_id, generation).await;
            if close_delete_fence {
                assert!(matches!(
                    coordinator
                        .begin_delete(BlobDeleteFenceKey::new(
                            blob,
                            BlobGcRunId::from_bytes([0x71; 16]).unwrap(),
                        ))
                        .await
                        .unwrap(),
                    BeginBlobDelete::Acquired(_)
                ));
            } else {
                store
                    .delete(&crate::search::text::blob_object_store_path(
                        &format!("{name}-blobs"),
                        *blob.hash(),
                    ))
                    .await
                    .unwrap();
            }
            assert_eq!(
                drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver).await,
                expected
            );
            let after = read_operation(&db, scope, operation_id).await;
            assert_eq!(after.progress(), before.progress());
            if close_delete_fence {
                assert!(matches!(
                    after.execution_state(),
                    IndexOperationExecutionState::Queued { .. }
                ));
            } else {
                assert!(matches!(
                    after.execution_state(),
                    IndexOperationExecutionState::Blocked(
                        IndexOperationBlocker::InvariantViolation
                    )
                ));
            }
        }
    }

    #[tokio::test]
    async fn prepared_page_validation_retries_when_reachability_goes_stale() {
        let db = test_db("text-validation-stale-reachability").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("stale validation".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let (driver, coordinator, _) = publication_runtime_with_timing(
            "text-validation-stale-reachability-blobs",
            BlobPublicationTiming::default(),
        );
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut operation_sequence = 1;
        let mut upload_sequence = 1_400;
        advance_build_to_validation(
            &db,
            operation_id,
            &mut operation_sequence,
            &mut upload_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;
        let claimed = claim_one(&db, operation_id, &mut operation_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ValidateManifests(progress),
        )) = claimed.record.progress()
        else {
            panic!("prepared stale test requires validation progress")
        };
        let TextPublicationRuntime::Installed(runtime) = &driver.publication else {
            panic!("validation test driver has a complete runtime")
        };
        let prepared =
            prepare_validation_step(&db, scope, &claimed.record, progress, limits, runtime)
                .await
                .unwrap();
        let (page_key, blob) = first_manifest_page_blob(&db, scope, index_id, generation).await;
        let (reference_key, _) =
            super::super::attachment::manifest_page_reachability_row(blob, scope, page_key, 0);
        db.delete(reference_key).await.unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            prepared
                .stage(&transaction, scope, &claimed.record)
                .await
                .unwrap(),
            IndexOperationStepResult::TransientFailure
        ));
    }

    #[tokio::test]
    async fn missing_manifest_blob_blocks_without_moving_artifact_ownership() {
        let fixture = manifest_preparation_fixture("text-manifest-missing-blob", 0x61).await;
        let location =
            crate::search::text::blob_object_store_path(&fixture.db_path, *fixture.blob.hash());
        fixture
            .object_store
            .delete(&location)
            .await
            .expect("test removes the exact published object");
        let TextPublicationRuntime::Installed(runtime) = &fixture.driver.publication else {
            panic!("manifest fixture has a complete publication runtime");
        };
        let blocked = prepare_manifest_step(
            &fixture.db,
            DataScope::LegacyUnscoped,
            &fixture.operation,
            &fixture.progress,
            SearchIndexBackfillLimits::default().batch(),
            runtime,
        )
        .await
        .expect("missing object becomes a durable invariant blocker");
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("blocked manifest transaction opens");
        assert!(matches!(
            blocked
                .stage(&transaction, DataScope::LegacyUnscoped, &fixture.operation,)
                .await
                .expect("missing object stages only its blocker"),
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
        assert_eq!(
            generation_row_count(
                &fixture.db,
                DataScope::LegacyUnscoped,
                IndexV2RecordKind::TextBuildArtifact,
                fixture.operation.index_id(),
                fixture.operation.generation(),
            )
            .await,
            1
        );
        assert_eq!(
            generation_row_count(
                &fixture.db,
                DataScope::LegacyUnscoped,
                IndexV2RecordKind::TextManifestPage,
                fixture.operation.index_id(),
                fixture.operation.generation(),
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn manifest_exhaustion_and_first_row_limit_map_to_closed_repository_steps() {
        let exhausted_fixture = manifest_preparation_fixture("text-manifest-exhausted", 0x62).await;
        let exhausted_cursor = IndexCursor::try_new(scoped_index_key(
            DataScope::LegacyUnscoped,
            IndexV2Key::TextBuildArtifact(TextBuildArtifactKey {
                root: TextManifestRootKey {
                    index_id: exhausted_fixture.operation.index_id(),
                    generation: exhausted_fixture.operation.generation(),
                    partition: TextPartition::Unpartitioned.fingerprint(),
                },
                ordinal: 0,
            }),
        ))
        .expect("exact artifact key is a valid manifest cursor");
        let TextPublicationRuntime::Installed(exhausted_runtime) =
            &exhausted_fixture.driver.publication
        else {
            panic!("manifest fixture has a complete publication runtime");
        };
        let exhausted = prepare_manifest_step(
            &exhausted_fixture.db,
            DataScope::LegacyUnscoped,
            &exhausted_fixture.operation,
            &PrefixScanProgress {
                cursor: Some(exhausted_cursor),
                counters: OperationCounters::default(),
            },
            SearchIndexBackfillLimits::default().batch(),
            exhausted_runtime,
        )
        .await
        .expect("an exhausted artifact cursor prepares activation");
        let transaction = exhausted_fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("exhausted manifest transaction opens");
        assert!(matches!(
            exhausted
                .stage(
                    &transaction,
                    DataScope::LegacyUnscoped,
                    &exhausted_fixture.operation,
                )
                .await
                .expect("exhausted manifest step stages activation progress"),
            IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
                TextBuildProgress::Constructing(TextBuildStage::ValidateManifests(_))
            ))
        ));
        drop(transaction);

        let late_owner = TextBuildArtifactKey {
            root: TextManifestRootKey {
                index_id: exhausted_fixture.operation.index_id(),
                generation: exhausted_fixture.operation.generation(),
                partition: TextPartition::Unpartitioned.fingerprint(),
            },
            ordinal: 1,
        };
        let late_split = work::SplitRef::try_new(
            exhausted_fixture.blob,
            80,
            16,
            4,
            exhausted_fixture.blob.size(),
        )
        .expect("late artifact split metadata is valid");
        exhausted_fixture
            .db
            .put(
                scoped_index_key(
                    DataScope::LegacyUnscoped,
                    IndexV2Key::TextBuildArtifact(late_owner),
                ),
                encode_work_value(&IndexV2WorkValue::TextBuildArtifact(
                    work::TextBuildArtifactValue {
                        index_id: exhausted_fixture.operation.index_id(),
                        generation: exhausted_fixture.operation.generation(),
                        partition: TextPartition::Unpartitioned,
                        artifact_ordinal: 1,
                        split: late_split,
                        source_intent_id: TextUploadIntentId::from_bytes([0x68; 16])
                            .expect("late artifact intent ID is non-nil"),
                    },
                )),
            )
            .await
            .expect("late artifact is inserted after exhaustion preparation");
        let (late_reference_key, late_reference_value) =
            super::super::attachment::build_artifact_reachability_row(
                exhausted_fixture.blob,
                DataScope::LegacyUnscoped,
                late_owner,
            );
        exhausted_fixture
            .db
            .put(late_reference_key, late_reference_value)
            .await
            .expect("late artifact reachability is inserted");
        let transaction = exhausted_fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("late manifest transaction opens");
        assert!(matches!(
            exhausted
                .stage(
                    &transaction,
                    DataScope::LegacyUnscoped,
                    &exhausted_fixture.operation,
                )
                .await
                .expect("late artifact makes the prepared exhaustion stale"),
            IndexOperationStepResult::TransientFailure
        ));

        let blocked_fixture =
            manifest_preparation_fixture("text-manifest-first-row-limit", 0x63).await;
        let TextPublicationRuntime::Installed(blocked_runtime) =
            &blocked_fixture.driver.publication
        else {
            panic!("manifest fixture has a complete publication runtime");
        };
        let default_batch = SearchIndexBackfillLimits::default().batch();
        let blocked_limits = SearchIndexBatchLimits::try_new(
            default_batch.max_entities(),
            default_batch.max_input_bytes(),
            NonZeroU64::MIN,
            default_batch.max_output_bytes(),
            default_batch.max_single_vector_output_bytes(),
        )
        .expect("one operation is a valid positive transaction limit");
        let blocked = prepare_manifest_step(
            &blocked_fixture.db,
            DataScope::LegacyUnscoped,
            &blocked_fixture.operation,
            &blocked_fixture.progress,
            blocked_limits,
            blocked_runtime,
        )
        .await
        .expect("an indivisible manifest row prepares a typed blocker");
        let transaction = blocked_fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("blocked manifest transaction opens");
        assert!(matches!(
            blocked
                .stage(
                    &transaction,
                    DataScope::LegacyUnscoped,
                    &blocked_fixture.operation,
                )
                .await
                .expect("manifest limit observations remain current"),
            IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit { .. })
        ));
    }

    #[tokio::test]
    async fn manifest_counters_reject_each_overflow_after_reference_validation() {
        for (name, seed, counters, expected_counter) in [
            (
                "text-manifest-input-counter-overflow",
                0x64,
                OperationCounters {
                    entities: 0,
                    input_bytes: u64::MAX,
                    output_operations: 0,
                    output_bytes: 0,
                },
                "manifest input bytes",
            ),
            (
                "text-manifest-operation-counter-overflow",
                0x65,
                OperationCounters {
                    entities: 0,
                    input_bytes: 0,
                    output_operations: u64::MAX,
                    output_bytes: 0,
                },
                "manifest output operations",
            ),
            (
                "text-manifest-output-counter-overflow",
                0x66,
                OperationCounters {
                    entities: 0,
                    input_bytes: 0,
                    output_operations: 0,
                    output_bytes: u64::MAX,
                },
                "manifest output bytes",
            ),
        ] {
            let fixture = manifest_preparation_fixture(name, seed).await;
            let TextPublicationRuntime::Installed(runtime) = &fixture.driver.publication else {
                panic!("manifest fixture has a complete publication runtime");
            };
            let result = prepare_manifest_step(
                &fixture.db,
                DataScope::LegacyUnscoped,
                &fixture.operation,
                &PrefixScanProgress {
                    cursor: None,
                    counters,
                },
                SearchIndexBackfillLimits::default().batch(),
                runtime,
            )
            .await;
            let Err(HelixDbError::IndexCatalogCorruption(reason)) = result else {
                panic!("overflowing {expected_counter} must fail closed");
            };
            assert!(reason.contains(expected_counter));
        }
    }

    #[tokio::test]
    async fn compaction_retires_all_stale_inputs_without_creating_a_child() {
        let db = test_db("text-compaction-all-stale").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for entity_id in [1, 2] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!("stale document {entity_id}"))),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let (driver, coordinator) = publication_driver("text-compaction-all-stale-blobs");
        let limits = single_entity_batch_limits();
        let mut operation_sequence = 1;
        let mut upload_sequence = 800;
        advance_two_splits_to_compact(
            &db,
            operation_id,
            &mut operation_sequence,
            &mut upload_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;

        for (key, mut state) in entity_state_rows(&db, scope, index_id, generation).await {
            state.live = false;
            db.put(
                scoped_index_key(scope, IndexV2Key::TextEntityState(key)),
                encode_work_value(&IndexV2WorkValue::TextEntityState(state)),
            )
            .await
            .expect("test marks the exact entity state stale");
        }
        assert_eq!(
            drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextBuildArtifact,
                index_id,
                generation,
            )
            .await,
            0
        );
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::BlobGcCandidate,
                index_id,
                generation,
            )
            .await,
            2
        );
        assert_eq!(
            read_manifest_root(
                &db,
                scope,
                index_id,
                generation,
                &TextPartition::Unpartitioned,
            )
            .await,
            Some(work::TextManifestRootValue::empty(
                index_id,
                generation,
                TextPartition::Unpartitioned,
            ))
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Compact(_)
            ))
        ));
        assert_eq!(
            drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::PrepareManifests(_)
            ))
        ));
    }

    #[tokio::test]
    async fn compaction_preparation_conflicts_with_a_newer_entity_state() {
        let db = test_db("text-compaction-state-conflict").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for entity_id in [1, 2] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!(
                        "conflict document {entity_id}"
                    ))),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let (driver, coordinator) = publication_driver("text-compaction-state-conflict-blobs");
        let limits = single_entity_batch_limits();
        let mut operation_sequence = 1;
        let mut upload_sequence = 900;
        advance_two_splits_to_compact(
            &db,
            operation_id,
            &mut operation_sequence,
            &mut upload_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;

        let claimed = claim_one(&db, operation_id, &mut operation_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::Compact(progress),
        )) = claimed.record.progress()
        else {
            panic!("prepared compaction requires Compact progress");
        };
        let TextPublicationRuntime::Installed(runtime) = &driver.publication else {
            panic!("test compaction driver has a complete publication runtime");
        };
        let prepared =
            prepare_compaction_step(&db, scope, &claimed.record, progress, limits, runtime)
                .await
                .expect("compaction preparation succeeds");
        let PreparedTextOperationStep::CompactionUpload(_) = &prepared else {
            panic!("two live splits prepare one replacement upload");
        };

        let Some((state_key, mut state)) = entity_state_rows(&db, scope, index_id, generation)
            .await
            .into_iter()
            .next()
        else {
            panic!("compaction source has generation-qualified entity state");
        };
        state.logical_version = state
            .logical_version
            .checked_next()
            .expect("initial logical version can advance");
        db.put(
            scoped_index_key(scope, IndexV2Key::TextEntityState(state_key)),
            encode_work_value(&IndexV2WorkValue::TextEntityState(state)),
        )
        .await
        .expect("test commits a newer entity state");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("stale compaction transaction opens");
        assert!(matches!(
            prepared
                .stage(&transaction, scope, &claimed.record)
                .await
                .expect("stale compaction becomes a retryable result"),
            IndexOperationStepResult::TransientFailure
        ));
        drop(transaction);
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextBuildArtifact,
                index_id,
                generation,
            )
            .await,
            2
        );
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::BlobGcCandidate,
                index_id,
                generation,
            )
            .await,
            0
        );
        prepared
            .discard()
            .await
            .expect("stale compaction reservation releases");
    }

    #[tokio::test]
    async fn compaction_terminal_nonpublication_preserves_inputs_and_retries_prefix() {
        let db = test_db("text-compaction-terminal-nonpublication").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for entity_id in [1, 2] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!("retry document {entity_id}"))),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let (driver, coordinator) =
            publication_driver("text-compaction-terminal-nonpublication-blobs");
        let limits = single_entity_batch_limits();
        let mut operation_sequence = 1;
        let mut upload_sequence = 1_000;
        advance_two_splits_to_compact(
            &db,
            operation_id,
            &mut operation_sequence,
            &mut upload_sequence,
            limits,
            &driver,
            Arc::clone(&coordinator),
        )
        .await;

        let claimed = claim_one(&db, operation_id, &mut operation_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::Compact(progress),
        )) = claimed.record.progress()
        else {
            panic!("terminal nonpublication test requires Compact progress");
        };
        let TextPublicationRuntime::Installed(runtime) = &driver.publication else {
            panic!("test compaction driver has a complete publication runtime");
        };
        let prepared =
            prepare_compaction_step(&db, scope, &claimed.record, progress, limits, runtime)
                .await
                .expect("compaction preparation succeeds");
        let next = commit_prepared_build_child(&db, scope, &claimed, &prepared).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitCompactionUpload(wait),
        )) = next.progress()
        else {
            panic!("committed replacement child enters AwaitCompactionUpload");
        };
        let wait = wait.clone();
        let PreparedTextOperationStep::CompactionUpload(prepared_upload) = &prepared else {
            panic!("terminal nonpublication test prepared one compaction upload")
        };
        coordinator.expire_unused_permit(prepared_upload.intent.permit());
        drop(prepared);

        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        let upload_dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let upload_driver = CoordinatorTextUploadDriver::new(upload_dependency);
        assert_eq!(
            drive_upload_one(
                &db,
                wait.intent_id(),
                writer_epoch,
                upload_sequence,
                &upload_driver,
            )
            .await,
            TextUploadStepResult::NonPublicationProven
        );
        upload_sequence = upload_sequence
            .checked_add(1)
            .expect("test upload sequence remains bounded");
        assert_eq!(
            drive_upload_one(
                &db,
                wait.intent_id(),
                writer_epoch,
                upload_sequence,
                &upload_driver,
            )
            .await,
            TextUploadStepResult::NonPublicationReleased
        );
        assert_eq!(
            drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        let retried = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::Compact(progress),
        )) = retried.progress()
        else {
            panic!("missing replacement retries the retained compaction prefix");
        };
        assert_eq!(progress, wait.compact());
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextBuildArtifact,
                index_id,
                generation,
            )
            .await,
            2
        );
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::BlobGcCandidate,
                index_id,
                generation,
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn compaction_output_limit_blocks_without_retiring_inputs() {
        let db = test_db("text-compaction-output-limit").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for entity_id in [1, 2] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!(
                        "limited document {entity_id}"
                    ))),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let (mut driver, coordinator) = publication_driver("text-compaction-output-limit-blobs");
        let limits = single_entity_batch_limits();
        let mut operation_sequence = 1;
        let mut upload_sequence = 1_100;
        advance_two_splits_to_compact(
            &db,
            operation_id,
            &mut operation_sequence,
            &mut upload_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;
        let TextPublicationRuntime::Installed(runtime) = &mut driver.publication else {
            panic!("compaction limit test has a complete publication runtime")
        };
        let current = runtime.compaction_limits;
        runtime.compaction_limits = TextBackfillCompactionLimits::new(
            current.max_fan_in(),
            current.max_input_bytes(),
            current.max_temporary_disk_bytes(),
            NonZeroU64::MIN,
            current.max_manifest_bytes(),
        );

        assert_eq!(
            drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver).await,
            CommittedOperationStep::Blocked
        );
        let operation = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::ManifestLimit {
                observed,
                limit: 1,
                ..
            }) if *observed > 1
        ));
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextBuildArtifact,
                index_id,
                generation,
            )
            .await,
            2
        );
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::BlobGcCandidate,
                index_id,
                generation,
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn compaction_artifact_ordinal_exhaustion_blocks_before_reservation() {
        let db = test_db("text-compaction-artifact-ordinal-limit").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for entity_id in [1, 2] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!(
                        "ordinal document {entity_id}"
                    ))),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, _, _) = create_build(&db, scope, &definition, 2).await;
        let (driver, coordinator) = publication_driver("text-compaction-artifact-ordinal-blobs");
        let limits = single_entity_batch_limits();
        let mut operation_sequence = 1;
        let mut upload_sequence = 1_200;
        advance_two_splits_to_compact(
            &db,
            operation_id,
            &mut operation_sequence,
            &mut upload_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;
        let claimed = claim_one(&db, operation_id, &mut operation_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::Compact(progress),
        )) = claimed.record.progress()
        else {
            panic!("artifact ordinal test requires Compact progress")
        };
        let overflow_progress = PrefixScanProgress {
            cursor: progress.cursor.clone(),
            counters: OperationCounters {
                output_operations: u64::from(u32::MAX) + 1,
                ..progress.counters
            },
        };
        let overflow_operation = IndexOperationRecord::try_new(
            claimed.record.operation_id(),
            claimed.record.index_id(),
            claimed.record.identity().clone(),
            claimed.record.generation(),
            claimed.record.index_record_revision(),
            claimed.record.operation_revision(),
            claimed.record.kind(),
            claimed.record.family(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Compact(overflow_progress.clone()),
            )),
            claimed.record.attempt(),
            claimed.record.execution_state().clone(),
        )
        .expect("large compaction counters remain a valid persisted checkpoint");
        let TextPublicationRuntime::Installed(runtime) = &driver.publication else {
            panic!("artifact ordinal test has a complete publication runtime")
        };
        let prepared = prepare_compaction_step(
            &db,
            scope,
            &overflow_operation,
            &overflow_progress,
            limits,
            runtime,
        )
        .await
        .expect("ordinal exhaustion prepares a durable blocker");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("ordinal blocker transaction opens");
        assert!(matches!(
            prepared
                .stage(&transaction, scope, &overflow_operation)
                .await
                .expect("ordinal blocker stages"),
            IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                observed,
                limit,
                ..
            }) if observed == u64::from(u32::MAX) + 1 && limit == u64::from(u32::MAX)
        ));
    }

    #[tokio::test]
    async fn compaction_reconciliation_rejects_durable_child_and_corrupt_output() {
        let db = test_db("text-compaction-reconciliation-corruption").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for entity_id in [1, 2] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!(
                        "reconcile document {entity_id}"
                    ))),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, _, _) = create_build(&db, scope, &definition, 2).await;
        let (driver, coordinator) = publication_driver("text-compaction-reconcile-blobs");
        let limits = single_entity_batch_limits();
        let mut operation_sequence = 1;
        let mut upload_sequence = 1_300;
        advance_two_splits_to_compact(
            &db,
            operation_id,
            &mut operation_sequence,
            &mut upload_sequence,
            limits,
            &driver,
            Arc::clone(&coordinator),
        )
        .await;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut operation_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        let waiting = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitCompactionUpload(wait),
        )) = waiting.progress()
        else {
            panic!("compaction reconciliation test requires its exact wait checkpoint")
        };
        let wait = wait.clone();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("durable-child reconciliation transaction opens");
        assert_eq!(
            corruption_reason(
                reconcile_await_compaction_upload(&transaction, scope, &waiting, &wait)
                    .await
                    .expect_err("a durable child prevents operation reconciliation"),
            ),
            "text AwaitCompactionUpload operation was claimed while its child remained durable"
        );
        drop(transaction);

        complete_current_build_upload(&db, operation_id, &mut upload_sequence, coordinator).await;
        let artifact_bytes = db
            .get(wait.artifact_key().as_bytes())
            .await
            .expect("replacement artifact is readable")
            .expect("replacement artifact exists");
        let IndexV2WorkValue::TextBuildArtifact(mut artifact) =
            decode_work_value(&artifact_bytes).expect("replacement artifact decodes")
        else {
            panic!("replacement key contains its declared artifact value")
        };
        db.put(
            wait.artifact_key().as_bytes(),
            encode_work_value(&IndexV2WorkValue::BlobGcCandidate(
                work::BlobGcCandidateValue {
                    owner: work::BlobGcCandidateOwner::GenerationCleanup(waiting.operation_id()),
                    index_id: waiting.index_id(),
                    generation: waiting.generation(),
                    blob: artifact.split.blob(),
                },
            )),
        )
        .await
        .expect("wrong replacement value kind is installed");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("wrong-value reconciliation transaction opens");
        assert_eq!(
            corruption_reason(
                reconcile_await_compaction_upload(&transaction, scope, &waiting, &wait)
                    .await
                    .expect_err("another replacement value kind fails closed"),
            ),
            "text AwaitCompactionUpload output key contains another work value"
        );
        drop(transaction);

        artifact.source_intent_id = TextUploadIntentId::from_bytes([0xDD; 16])
            .expect("mismatched source intent is non-nil");
        db.put(
            wait.artifact_key().as_bytes(),
            encode_work_value(&IndexV2WorkValue::TextBuildArtifact(artifact)),
        )
        .await
        .expect("wrong replacement owner is installed");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("wrong-owner reconciliation transaction opens");
        assert_eq!(
            corruption_reason(
                reconcile_await_compaction_upload(&transaction, scope, &waiting, &wait)
                    .await
                    .expect_err("replacement ownership disagreement fails closed"),
            ),
            "text AwaitCompactionUpload replacement disagrees with its exact owner"
        );
    }

    #[tokio::test]
    async fn stale_compaction_retirement_neither_writes_nor_needs_commit_resolution() {
        let db = test_db("text-compaction-retirement-stale").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for entity_id in [1, 2] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!(
                        "retire document {entity_id}"
                    ))),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let (driver, coordinator) = publication_driver("text-compaction-retirement-stale-blobs");
        let limits = single_entity_batch_limits();
        let mut operation_sequence = 1;
        let mut upload_sequence = 1_400;
        advance_two_splits_to_compact(
            &db,
            operation_id,
            &mut operation_sequence,
            &mut upload_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;
        for (key, mut state) in entity_state_rows(&db, scope, index_id, generation).await {
            state.live = false;
            db.put(
                scoped_index_key(scope, IndexV2Key::TextEntityState(key)),
                encode_work_value(&IndexV2WorkValue::TextEntityState(state)),
            )
            .await
            .expect("test marks the exact entity state stale");
        }
        let claimed = claim_one(&db, operation_id, &mut operation_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::Compact(progress),
        )) = claimed.record.progress()
        else {
            panic!("stale retirement test requires Compact progress")
        };
        let TextPublicationRuntime::Installed(runtime) = &driver.publication else {
            panic!("stale retirement test has a complete publication runtime")
        };
        let prepared =
            prepare_compaction_step(&db, scope, &claimed.record, progress, limits, runtime)
                .await
                .expect("all-stale compaction prepares exact retirement");
        assert!(matches!(
            prepared
                .resolve_commit_error(&db, scope)
                .await
                .expect("repository-only retirement needs no child resolution"),
            PreparedStepCommitResolution::Ordinary
        ));
        let different_operation = claimed
            .record
            .progressed(claimed.record.progress().clone())
            .expect("same-progress operation advances to another revision");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("wrong-operation retirement transaction opens");
        assert_eq!(
            corruption_reason(
                prepared
                    .stage(&transaction, scope, &different_operation)
                    .await
                    .expect_err("another operation revision fails closed"),
            ),
            "prepared text compaction retirement no longer matches its claimed operation"
        );
        drop(transaction);

        let Some((state_key, mut state)) = entity_state_rows(&db, scope, index_id, generation)
            .await
            .into_iter()
            .next()
        else {
            panic!("stale retirement retains exact entity observations")
        };
        state.logical_version = state
            .logical_version
            .checked_next()
            .expect("initial logical version can advance");
        db.put(
            scoped_index_key(scope, IndexV2Key::TextEntityState(state_key)),
            encode_work_value(&IndexV2WorkValue::TextEntityState(state)),
        )
        .await
        .expect("newer entity state is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("state-conflict retirement transaction opens");
        assert!(matches!(
            prepared
                .stage(&transaction, scope, &claimed.record)
                .await
                .expect("stale retirement becomes a transient retry"),
            IndexOperationStepResult::TransientFailure
        ));
        drop(transaction);
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextBuildArtifact,
                index_id,
                generation,
            )
            .await,
            2
        );
        prepared
            .discard()
            .await
            .expect("repository-only retirement owns no permit");
    }

    #[test]
    fn compaction_error_maps_database_and_every_capacity_variant() {
        let database = compaction_error(
            crate::search::text::compaction::TextBuildCompactionError::Database(
                HelixDbError::Config("compaction database sentinel".to_string()),
            ),
        );
        assert!(matches!(
            database,
            HelixDbError::Config(reason) if reason == "compaction database sentinel"
        ));
        let positive = NonZeroU64::MIN;
        for error in [
            crate::search::text::compaction::TextBuildCompactionError::TooFewInputSplits,
            crate::search::text::compaction::TextBuildCompactionError::FanInExceeded {
                required: positive,
                limit: positive,
            },
            crate::search::text::compaction::TextBuildCompactionError::InputSplitBytesEmpty,
            crate::search::text::compaction::TextBuildCompactionError::InputBytesExceeded {
                required: positive,
                limit: positive,
            },
            crate::search::text::compaction::TextBuildCompactionError::TemporaryDiskExceeded {
                required: positive,
                limit: positive,
            },
            crate::search::text::compaction::TextBuildCompactionError::OutputBlobEmpty,
            crate::search::text::compaction::TextBuildCompactionError::OutputBlobExceeded {
                required: positive,
                limit: positive,
            },
            crate::search::text::compaction::TextBuildCompactionError::DuplicateDocumentVersion {
                entity_id: 1,
                logical_version: 1,
            },
            crate::search::text::compaction::TextBuildCompactionError::MeasurementOverflow,
        ] {
            let reason = corruption_reason(compaction_error(error));
            assert!(reason.starts_with("invalid text compaction input or capacity:"));
        }
    }

    #[tokio::test]
    async fn catch_up_live_update_commits_state_delta_and_upload_child_atomically() {
        let db = test_db("text-catch-up-live-update").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("before catch-up".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (driver, coordinator) = publication_driver("text-catch-up-live-update-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &driver,
            coordinator.clone(),
        )
        .await;

        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("after catch-up".to_string())),
                None,
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;

        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        let waiting = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitCatchUpUpload(wait),
        )) = waiting.progress()
        else {
            panic!("live catch-up must wait on its exact upload child");
        };
        let wait = wait.clone();
        assert!(db
            .get(build_delta_key(scope, index_id, generation, entity))
            .await
            .expect("consumed catch-up delta is readable")
            .is_none());
        let next_version = TextLogicalVersion::initial()
            .checked_next()
            .expect("initial text version can advance");
        assert_eq!(
            read_text_applied(&db, scope, index_id, generation, entity).await,
            Some((TextPartition::Unpartitioned, next_version))
        );
        let states = entity_state_rows(&db, scope, index_id, generation).await;
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].1.logical_version, next_version);
        assert!(states[0].1.live);
        assert!(
            crate::index_v2::repository::load_upload_from_pointer(&db, wait.intent_id())
                .await
                .expect("catch-up upload pointer is readable")
                .is_some()
        );

        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let upload_driver = CoordinatorTextUploadDriver::new(dependency);
        for (sequence, expected) in [
            (400, TextUploadStepResult::PublicationSucceeded),
            (401, TextUploadStepResult::AttachUploaded),
            (402, TextUploadStepResult::ReferenceReleased),
        ] {
            assert_eq!(
                drive_upload_one(
                    &db,
                    wait.intent_id(),
                    writer_epoch,
                    sequence,
                    &upload_driver
                )
                .await,
                expected
            );
        }
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(progress)
            )) if progress == &wait.completed_catch_up()
        ));
        assert!(db
            .get(wait.artifact_key().as_bytes())
            .await
            .expect("catch-up artifact is readable")
            .is_some());

        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Compact(_)
            ))
        ));
    }

    #[tokio::test]
    async fn catch_up_deletion_applies_dead_state_without_an_upload_child() {
        let db = test_db("text-catch-up-deletion").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("deleted during build".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (driver, coordinator) = publication_driver("text-catch-up-deletion-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;

        db.delete(source_key(scope, entity.kind, entity.id.get()))
            .await
            .expect("authoritative entity is deleted");
        put_build_delta(&db, scope, index_id, generation, entity).await;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );

        assert!(db
            .get(build_delta_key(scope, index_id, generation, entity))
            .await
            .expect("consumed deletion delta is readable")
            .is_none());
        let next_version = TextLogicalVersion::initial()
            .checked_next()
            .expect("initial text version can advance");
        assert_eq!(
            read_text_applied(&db, scope, index_id, generation, entity).await,
            Some((TextPartition::Unpartitioned, next_version))
        );
        let states = entity_state_rows(&db, scope, index_id, generation).await;
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].1.logical_version, next_version);
        assert!(!states[0].1.live);
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(_)
            ))
        ));
    }

    #[tokio::test]
    async fn catch_up_stale_preparation_commits_no_state_or_upload_child() {
        let db = test_db("text-catch-up-stale-preparation").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("initial text".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (driver, coordinator) = publication_driver("text-catch-up-stale-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &driver,
            coordinator.clone(),
        )
        .await;
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("prepared text".to_string())),
                None,
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;

        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::CatchUp(progress),
        )) = claimed.record.progress()
        else {
            panic!("prepared operation must be in catch-up");
        };
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let prepared =
            prepare_catch_up_step(&db, scope, &claimed.record, progress, limits, dependency)
                .await
                .expect("catch-up preparation succeeds")
                .expect("live catch-up prepares one child");
        let PreparedTextOperationStep::CatchUpUpload(upload) = &prepared else {
            panic!("live catch-up preparation must own a catch-up child");
        };
        let intent_id = upload.intent.value().intent_id;

        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("newer text".to_string())),
                None,
            ),
        )
        .await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("stale catch-up transaction opens");
        assert!(matches!(
            prepared
                .stage(&transaction, scope, &claimed.record)
                .await
                .expect("stale read is a retryable result"),
            IndexOperationStepResult::TransientFailure
        ));
        drop(transaction);

        assert!(db
            .get(build_delta_key(scope, index_id, generation, entity))
            .await
            .expect("stale catch-up delta is readable")
            .is_some());
        assert_eq!(
            read_text_applied(&db, scope, index_id, generation, entity).await,
            Some((TextPartition::Unpartitioned, TextLogicalVersion::initial()))
        );
        assert!(
            crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
                .await
                .expect("stale upload pointer is readable")
                .is_none()
        );
        prepared
            .discard()
            .await
            .expect("uncommitted catch-up reservation releases");
    }

    #[tokio::test]
    async fn catch_up_concurrent_mutation_conflicts_after_child_staging() {
        let db = test_db("text-catch-up-concurrent-mutation").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("initial text".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (driver, coordinator) = publication_driver("text-catch-up-concurrent-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &driver,
            coordinator.clone(),
        )
        .await;
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("prepared text".to_string())),
                None,
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;

        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::CatchUp(progress),
        )) = claimed.record.progress()
        else {
            panic!("prepared operation must be in catch-up");
        };
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let prepared =
            prepare_catch_up_step(&db, scope, &claimed.record, progress, limits, dependency)
                .await
                .expect("catch-up preparation succeeds")
                .expect("live catch-up prepares one child");
        let PreparedTextOperationStep::CatchUpUpload(upload) = &prepared else {
            panic!("live catch-up preparation must own a catch-up child");
        };
        let intent_id = upload.intent.value().intent_id;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("concurrent catch-up transaction opens");
        assert!(matches!(
            prepared
                .stage(&transaction, scope, &claimed.record)
                .await
                .expect("prepared child stages against exact reads"),
            IndexOperationStepResult::Progressed(_)
        ));

        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("concurrent text".to_string())),
                None,
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;
        assert!(transaction.commit().await.is_err());
        assert_eq!(
            prepared
                .resolve_commit_error(&db, scope)
                .await
                .expect("failed catch-up commit resolves by exact point reads"),
            PreparedStepCommitResolution::NotCommitted
        );
        assert!(
            crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
                .await
                .expect("conflicted upload pointer is readable")
                .is_none()
        );
        assert!(db
            .get(build_delta_key(scope, index_id, generation, entity))
            .await
            .expect("concurrent mutation delta is readable")
            .is_some());
        assert_eq!(
            read_text_applied(&db, scope, index_id, generation, entity).await,
            Some((TextPartition::Unpartitioned, TextLogicalVersion::initial()))
        );
        prepared
            .discard()
            .await
            .expect("conflicted catch-up reservation releases");
    }

    #[tokio::test]
    async fn catch_up_terminal_nonpublication_restores_the_exact_delta() {
        let db = test_db("text-catch-up-terminal-nonpublication").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("initial child".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (build_driver, build_coordinator) =
            publication_driver("text-catch-up-nonpublication-initial-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &build_driver,
            build_coordinator,
        )
        .await;
        let default_timing = BlobPublicationTiming::default();
        let timing = BlobPublicationTiming::new(
            BlobOperationDuration::from_millis(NonZeroU64::MIN),
            default_timing.publish_timeout(),
            default_timing.safety_margin(),
        );
        let (driver, coordinator) =
            publication_driver_with_timing("text-catch-up-nonpublication-blobs", timing);
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("unpublished catch-up".to_string())),
                None,
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;
        let delta_key = build_delta_key(scope, index_id, generation, entity);
        let delta_value = db
            .get(&delta_key)
            .await
            .expect("catch-up delta is readable")
            .expect("catch-up delta exists");

        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::CatchUp(progress),
        )) = claimed.record.progress()
        else {
            panic!("prepared operation must be in catch-up");
        };
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let prepared =
            prepare_catch_up_step(&db, scope, &claimed.record, progress, limits, dependency)
                .await
                .expect("catch-up preparation succeeds")
                .expect("live catch-up prepares one child");
        let next = commit_prepared_build_child(&db, scope, &claimed, &prepared).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitCatchUpUpload(wait),
        )) = next.progress()
        else {
            panic!("committed catch-up child must enter AwaitCatchUpUpload");
        };
        let wait = wait.clone();
        drop(prepared);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        let upload_dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let upload_driver = CoordinatorTextUploadDriver::new(upload_dependency);
        assert_eq!(
            drive_upload_one(&db, wait.intent_id(), writer_epoch, 500, &upload_driver).await,
            TextUploadStepResult::NonPublicationProven
        );
        assert_eq!(
            drive_upload_one(&db, wait.intent_id(), writer_epoch, 501, &upload_driver).await,
            TextUploadStepResult::NonPublicationReleased
        );
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );

        assert_eq!(
            db.get(&delta_key)
                .await
                .expect("restored catch-up delta is readable"),
            Some(delta_value)
        );
        assert!(db
            .get(wait.artifact_key().as_bytes())
            .await
            .expect("absent catch-up artifact is readable")
            .is_none());
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(progress)
            )) if progress == &wait.completed_catch_up()
        ));
    }

    #[tokio::test]
    async fn catch_up_handles_unseen_live_and_still_absent_entities_in_key_order() {
        let db = test_db("text-catch-up-unseen-entities").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for entity_id in 1..=2 {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Other",
                    Some(PropertyValue::String("not initially indexed".to_string())),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let (driver, coordinator) = publication_driver("text-catch-up-unseen-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_empty_source_to_catch_up(&db, operation_id, &mut claim_sequence, limits, &driver)
            .await;
        assert_eq!(
            read_manifest_root(
                &db,
                scope,
                index_id,
                generation,
                &TextPartition::Unpartitioned,
            )
            .await,
            Some(work::TextManifestRootValue::empty(
                index_id,
                generation,
                TextPartition::Unpartitioned,
            ))
        );

        let live_entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        let absent_entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(2),
        };
        put_source(
            &db,
            scope,
            live_entity.kind,
            live_entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("newly indexed".to_string())),
                None,
            ),
        )
        .await;
        db.delete(source_key(
            scope,
            absent_entity.kind,
            absent_entity.id.get(),
        ))
        .await
        .expect("still-absent entity is removed from authoritative state");
        put_build_delta(&db, scope, index_id, generation, live_entity).await;
        put_build_delta(&db, scope, index_id, generation, absent_entity).await;

        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        let waiting = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitCatchUpUpload(wait),
        )) = waiting.progress()
        else {
            panic!("lowest unseen live entity must prepare the first child");
        };
        let wait = wait.clone();
        assert_eq!(
            read_text_applied(&db, scope, index_id, generation, live_entity).await,
            Some((TextPartition::Unpartitioned, TextLogicalVersion::initial()))
        );
        assert!(db
            .get(build_delta_key(scope, index_id, generation, absent_entity))
            .await
            .expect("later absent delta is readable")
            .is_some());

        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let upload_driver = CoordinatorTextUploadDriver::new(dependency);
        for (sequence, expected) in [
            (600, TextUploadStepResult::PublicationSucceeded),
            (601, TextUploadStepResult::AttachUploaded),
            (602, TextUploadStepResult::ReferenceReleased),
        ] {
            assert_eq!(
                drive_upload_one(
                    &db,
                    wait.intent_id(),
                    writer_epoch,
                    sequence,
                    &upload_driver
                )
                .await,
                expected
            );
        }
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        assert!(db
            .get(build_delta_key(scope, index_id, generation, absent_entity))
            .await
            .expect("consumed absent delta is readable")
            .is_none());
        assert_eq!(
            read_text_applied(&db, scope, index_id, generation, absent_entity).await,
            None
        );
        let states = entity_state_rows(&db, scope, index_id, generation).await;
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].0.entity, live_entity);
    }

    #[tokio::test]
    async fn catch_up_partition_move_tombstones_old_partition_then_blocks_missing_tenant() {
        let db = test_db("text-catch-up-partition-move").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, Some("account_id"));
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("tenant one".to_string())),
                Some(PropertyValue::I64(1)),
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (driver, coordinator) = publication_driver("text-catch-up-partition-move-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &driver,
            coordinator.clone(),
        )
        .await;
        let Some((old_partition, _)) =
            read_text_applied(&db, scope, index_id, generation, entity).await
        else {
            panic!("initial tenant partition has applied state");
        };

        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("tenant two".to_string())),
                Some(PropertyValue::I64(2)),
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        let waiting = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitCatchUpUpload(wait),
        )) = waiting.progress()
        else {
            panic!("partition move must publish the new partition document");
        };
        let wait = wait.clone();
        let Some((new_partition, applied_version)) =
            read_text_applied(&db, scope, index_id, generation, entity).await
        else {
            panic!("moved tenant partition has applied state");
        };
        assert_ne!(new_partition, old_partition);
        assert_eq!(applied_version, TextLogicalVersion::initial());
        assert_eq!(
            read_manifest_root(&db, scope, index_id, generation, &new_partition).await,
            Some(work::TextManifestRootValue::empty(
                index_id,
                generation,
                new_partition.clone(),
            ))
        );
        let states = entity_state_rows(&db, scope, index_id, generation).await;
        assert_eq!(states.len(), 2);
        assert!(states.iter().any(|(_, state)| {
            state.partition == old_partition
                && state.logical_version
                    == TextLogicalVersion::initial()
                        .checked_next()
                        .expect("initial text version can advance")
                && !state.live
        }));
        assert!(states.iter().any(|(_, state)| {
            state.partition == new_partition
                && state.logical_version == TextLogicalVersion::initial()
                && state.live
        }));

        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let upload_driver = CoordinatorTextUploadDriver::new(dependency);
        for (sequence, expected) in [
            (700, TextUploadStepResult::PublicationSucceeded),
            (701, TextUploadStepResult::AttachUploaded),
            (702, TextUploadStepResult::ReferenceReleased),
        ] {
            assert_eq!(
                drive_upload_one(
                    &db,
                    wait.intent_id(),
                    writer_epoch,
                    sequence,
                    &upload_driver
                )
                .await,
                expected
            );
        }
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );

        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("tenant one again".to_string())),
                Some(PropertyValue::I64(1)),
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        let waiting = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitCatchUpUpload(return_wait),
        )) = waiting.progress()
        else {
            panic!("returning to an old partition must publish a new version");
        };
        let return_wait = return_wait.clone();
        let returned_version = TextLogicalVersion::initial()
            .checked_next()
            .expect("initial text version can advance")
            .checked_next()
            .expect("tombstone version can advance");
        assert_eq!(
            read_text_applied(&db, scope, index_id, generation, entity).await,
            Some((old_partition.clone(), returned_version))
        );
        let states = entity_state_rows(&db, scope, index_id, generation).await;
        assert_eq!(states.len(), 2);
        assert!(states.iter().any(|(_, state)| {
            state.partition == old_partition
                && state.logical_version == returned_version
                && state.live
        }));
        assert!(states.iter().any(|(_, state)| {
            state.partition == new_partition
                && state.logical_version
                    == TextLogicalVersion::initial()
                        .checked_next()
                        .expect("initial text version can advance")
                && !state.live
        }));
        for (sequence, expected) in [
            (800, TextUploadStepResult::PublicationSucceeded),
            (801, TextUploadStepResult::AttachUploaded),
            (802, TextUploadStepResult::ReferenceReleased),
        ] {
            assert_eq!(
                drive_upload_one(
                    &db,
                    return_wait.intent_id(),
                    writer_epoch,
                    sequence,
                    &upload_driver,
                )
                .await,
                expected
            );
        }
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );

        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("tenant missing".to_string())),
                None,
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Blocked
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id)
                .await
                .execution_state(),
            IndexOperationExecutionState::Blocked(
                IndexOperationBlocker::InvalidSourceData {
                    entity_kind: IndexElementKind::Node,
                    entity_id,
                }
            ) if *entity_id == entity.id
        ));
        assert!(db
            .get(build_delta_key(scope, index_id, generation, entity))
            .await
            .expect("blocked catch-up delta is readable")
            .is_some());
    }

    #[tokio::test]
    async fn catch_up_new_partition_root_is_inside_the_atomic_output_limit() {
        let db = test_db("text-catch-up-new-root-limit").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, Some("account_id"));
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("tenant one".to_string())),
                Some(PropertyValue::I64(1)),
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (driver, coordinator) = publication_driver("text-catch-up-new-root-limit-blobs");
        let default_limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            default_limits,
            &driver,
            coordinator,
        )
        .await;

        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("tenant two".to_string())),
                Some(PropertyValue::I64(2)),
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;
        let operation = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::CatchUp(progress),
        )) = operation.progress()
        else {
            panic!("new-partition limit test requires catch-up progress");
        };
        let ValidatedDynamicIndexDefinition::Text(text_definition) = &definition else {
            panic!("test definition is text");
        };
        let snapshot = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("catch-up planning snapshot opens");
        let TextCatchUpPlanRead::Planned(plan) =
            plan_next_catch_up(&snapshot, scope, &operation, text_definition, progress)
                .await
                .expect("new-partition catch-up plan validates")
        else {
            panic!("new tenant produces one catch-up plan");
        };
        drop(snapshot);
        let Some((new_partition, _)) = plan.document.as_ref() else {
            panic!("new tenant produces one document");
        };
        let root_key = scoped_index_key(
            scope,
            IndexV2Key::TextManifestRoot(TextManifestRootKey {
                index_id,
                generation,
                partition: new_partition.fingerprint(),
            }),
        );
        assert!(plan
            .expected_reads
            .iter()
            .any(|read| { read.key == root_key && read.value.is_none() }));
        assert!(plan.writes.iter().any(|write| {
            matches!(write, PreparedTextWrite::Put { key, .. } if key == &root_key)
        }));
        let blocked_operations = plan
            .output_operations
            .checked_sub(1)
            .and_then(NonZeroU64::new)
            .expect("new-root plan has at least two writes");
        let limits = SearchIndexBatchLimits::try_new(
            default_limits.max_entities(),
            default_limits.max_input_bytes(),
            blocked_operations,
            default_limits.max_output_bytes(),
            default_limits.max_single_vector_output_bytes(),
        )
        .expect("one-fewer-operation limit remains structurally valid");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("blocked catch-up transaction opens");
        assert!(matches!(
            catch_up(
                &transaction,
                scope,
                &operation,
                text_definition,
                progress,
                limits,
            )
            .await
            .expect("catch-up limit is modeled"),
            IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                observed,
                limit,
                ..
            }) if observed == plan.output_operations && limit == blocked_operations.get()
        ));
        transaction
            .commit()
            .await
            .expect("blocked catch-up commits no writes");
        assert!(db
            .get(root_key)
            .await
            .expect("new root point read succeeds")
            .is_none());
        assert!(db
            .get(build_delta_key(scope, index_id, generation, entity))
            .await
            .expect("blocked delta remains readable")
            .is_some());
    }

    #[tokio::test]
    async fn catch_up_live_entity_blocks_at_each_atomic_transaction_limit() {
        const LARGE_LIMIT: u64 = 1024 * 1024;
        for (case, input_limit, operation_limit, output_limit) in [
            ("input", 1, LARGE_LIMIT, LARGE_LIMIT),
            ("operations", LARGE_LIMIT, 1, LARGE_LIMIT),
            ("output", LARGE_LIMIT, LARGE_LIMIT, 1),
        ] {
            let db = test_db(&format!("text-catch-up-{case}-limit")).await;
            let scope = DataScope::LegacyUnscoped;
            let definition = definition(IndexElementKind::Node, None);
            let entity = IndexEntity {
                kind: IndexElementKind::Node,
                id: IndexEntityId::new(1),
            };
            put_source(
                &db,
                scope,
                entity.kind,
                entity.id.get(),
                &properties(
                    "Document",
                    Some(PropertyValue::String("initial text".to_string())),
                    None,
                ),
            )
            .await;
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, entity.id.get()).await;
            let (driver, coordinator) =
                publication_driver(&format!("text-catch-up-{case}-limit-blobs"));
            let default_limits = SearchIndexBackfillLimits::default().batch();
            let mut claim_sequence = 1;
            advance_single_partition_to_catch_up(
                &db,
                operation_id,
                &mut claim_sequence,
                default_limits,
                &driver,
                coordinator,
            )
            .await;
            put_source(
                &db,
                scope,
                entity.kind,
                entity.id.get(),
                &properties(
                    "Document",
                    Some(PropertyValue::String("limited catch-up text".to_string())),
                    None,
                ),
            )
            .await;
            put_build_delta(&db, scope, index_id, generation, entity).await;
            let limits = SearchIndexBatchLimits::try_new(
                NonZeroUsize::new(8).expect("entity limit is positive"),
                NonZeroU64::new(input_limit).expect("input limit is positive"),
                NonZeroU64::new(operation_limit).expect("operation limit is positive"),
                NonZeroU64::new(output_limit).expect("output limit is positive"),
                NonZeroU64::MIN,
            )
            .expect("catch-up limits are structurally valid");

            assert_eq!(
                drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
                CommittedOperationStep::Blocked,
                "{case} limit must block the indivisible catch-up entity"
            );
            assert!(matches!(
                read_operation(&db, scope, operation_id)
                    .await
                    .execution_state(),
                IndexOperationExecutionState::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: IndexElementKind::Node,
                        entity_id,
                        observed,
                        limit: 1,
                    }
                ) if *entity_id == entity.id && *observed > 1
            ));
            assert!(db
                .get(build_delta_key(scope, index_id, generation, entity))
                .await
                .expect("limited catch-up delta is readable")
                .is_some());
            assert_eq!(
                read_text_applied(&db, scope, index_id, generation, entity).await,
                Some((TextPartition::Unpartitioned, TextLogicalVersion::initial()))
            );
        }
    }

    #[tokio::test]
    async fn catch_up_live_entity_retries_without_a_publication_runtime() {
        let db = test_db("text-catch-up-no-publication-runtime").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("initial text".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (build_driver, coordinator) = publication_driver("text-catch-up-no-runtime-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &build_driver,
            coordinator,
        )
        .await;
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("cannot publish yet".to_string())),
                None,
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;

        assert_eq!(
            drive_one_with(
                &db,
                operation_id,
                &mut claim_sequence,
                limits,
                &TextIndexDriver::new(),
            )
            .await,
            CommittedOperationStep::TransientFailure
        );
        assert!(db
            .get(build_delta_key(scope, index_id, generation, entity))
            .await
            .expect("retryable catch-up delta is readable")
            .is_some());
        assert_eq!(
            read_text_applied(&db, scope, index_id, generation, entity).await,
            Some((TextPartition::Unpartitioned, TextLogicalVersion::initial()))
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(_)
            ))
        ));
    }

    #[tokio::test]
    async fn catch_up_blocks_before_wrapping_an_exhausted_logical_version() {
        let db = test_db("text-catch-up-version-exhaustion").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("versioned text".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (driver, coordinator) = publication_driver("text-catch-up-version-exhaustion-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;

        let applied_key = scoped_index_key(
            scope,
            IndexV2Key::AppliedState(IndexEntityStateKey {
                index_id,
                generation,
                entity,
            }),
        );
        let maximum =
            TextLogicalVersion::new(u64::MAX).expect("maximum logical version is non-zero");
        db.put(
            applied_key,
            encode_work_value(&IndexV2WorkValue::AppliedEntityState(
                AppliedEntityStateValue {
                    index_id,
                    generation,
                    entity_kind: entity.kind,
                    entity_id: entity.id,
                    state: AppliedFamilyState::Text(Some((TextPartition::Unpartitioned, maximum))),
                },
            )),
        )
        .await
        .expect("exhausted applied state is written");
        db.put(
            scoped_index_key(
                scope,
                IndexV2Key::TextEntityState(TextEntityStateKey {
                    root: TextManifestRootKey {
                        index_id,
                        generation,
                        partition: TextPartition::Unpartitioned.fingerprint(),
                    },
                    entity,
                }),
            ),
            encode_work_value(&IndexV2WorkValue::TextEntityState(TextEntityStateValue {
                index_id,
                generation,
                partition: TextPartition::Unpartitioned,
                entity_kind: entity.kind,
                entity_id: entity.id,
                logical_version: maximum,
                live: true,
            })),
        )
        .await
        .expect("exhausted partition state is written");
        db.delete(source_key(scope, entity.kind, entity.id.get()))
            .await
            .expect("authoritative entity is deleted");
        put_build_delta(&db, scope, index_id, generation, entity).await;

        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Blocked
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id)
                .await
                .execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
        assert!(db
            .get(build_delta_key(scope, index_id, generation, entity))
            .await
            .expect("exhausted-version delta is readable")
            .is_some());
    }

    #[tokio::test]
    async fn catch_up_rejects_malformed_delta_and_applied_state_ownership() {
        for case in [
            "delta-kind",
            "delta-owner",
            "applied-kind",
            "applied-owner",
            "applied-family",
            "state-kind",
            "state-owner",
            "state-missing",
        ] {
            let db = test_db(&format!("text-catch-up-corruption-{case}")).await;
            let scope = DataScope::LegacyUnscoped;
            let definition = definition(IndexElementKind::Node, None);
            let entity = IndexEntity {
                kind: IndexElementKind::Node,
                id: IndexEntityId::new(1),
            };
            put_source(
                &db,
                scope,
                entity.kind,
                entity.id.get(),
                &properties(
                    "Document",
                    Some(PropertyValue::String("initial text".to_string())),
                    None,
                ),
            )
            .await;
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, entity.id.get()).await;
            let (driver, coordinator) =
                publication_driver(&format!("text-catch-up-corruption-{case}-blobs"));
            let limits = SearchIndexBackfillLimits::default().batch();
            let mut claim_sequence = 1;
            advance_single_partition_to_catch_up(
                &db,
                operation_id,
                &mut claim_sequence,
                limits,
                &driver,
                coordinator,
            )
            .await;
            put_source(
                &db,
                scope,
                entity.kind,
                entity.id.get(),
                &properties(
                    "Document",
                    Some(PropertyValue::String("updated text".to_string())),
                    None,
                ),
            )
            .await;
            put_build_delta(&db, scope, index_id, generation, entity).await;
            let delta_key = build_delta_key(scope, index_id, generation, entity);
            let applied_key = scoped_index_key(
                scope,
                IndexV2Key::AppliedState(IndexEntityStateKey {
                    index_id,
                    generation,
                    entity,
                }),
            );
            let delta_value = db
                .get(&delta_key)
                .await
                .expect("delta row is readable")
                .expect("delta row exists");
            let applied_value = db
                .get(&applied_key)
                .await
                .expect("applied row is readable")
                .expect("applied row exists");
            let state_key = scoped_index_key(
                scope,
                IndexV2Key::TextEntityState(TextEntityStateKey {
                    root: TextManifestRootKey {
                        index_id,
                        generation,
                        partition: TextPartition::Unpartitioned.fingerprint(),
                    },
                    entity,
                }),
            );
            match case {
                "delta-kind" => db
                    .put(&delta_key, applied_value.clone())
                    .await
                    .expect("wrong delta value kind is written"),
                "delta-owner" => db
                    .put(
                        &delta_key,
                        encode_work_value(&IndexV2WorkValue::CoalescedBuildDelta(
                            CoalescedBuildDeltaValue {
                                index_id,
                                generation: generation
                                    .checked_next()
                                    .expect("test generation can advance"),
                                entity_kind: entity.kind,
                                entity_id: entity.id,
                            },
                        )),
                    )
                    .await
                    .expect("wrong delta owner is written"),
                "applied-kind" => db
                    .put(&applied_key, delta_value)
                    .await
                    .expect("wrong applied value kind is written"),
                "applied-owner" => db
                    .put(
                        &applied_key,
                        encode_work_value(&IndexV2WorkValue::AppliedEntityState(
                            AppliedEntityStateValue {
                                index_id: index_id
                                    .checked_next()
                                    .expect("test index ID can advance"),
                                generation,
                                entity_kind: entity.kind,
                                entity_id: entity.id,
                                state: AppliedFamilyState::Text(Some((
                                    TextPartition::Unpartitioned,
                                    TextLogicalVersion::initial(),
                                ))),
                            },
                        )),
                    )
                    .await
                    .expect("wrong applied owner is written"),
                "applied-family" => db
                    .put(
                        &applied_key,
                        encode_work_value(&IndexV2WorkValue::AppliedEntityState(
                            AppliedEntityStateValue {
                                index_id,
                                generation,
                                entity_kind: entity.kind,
                                entity_id: entity.id,
                                state: AppliedFamilyState::Vector(None),
                            },
                        )),
                    )
                    .await
                    .expect("wrong applied family is written"),
                "state-kind" => db
                    .put(&state_key, applied_value)
                    .await
                    .expect("wrong state value kind is written"),
                "state-owner" => db
                    .put(
                        &state_key,
                        encode_work_value(&IndexV2WorkValue::TextEntityState(
                            TextEntityStateValue {
                                index_id: index_id
                                    .checked_next()
                                    .expect("test index ID can advance"),
                                generation,
                                partition: TextPartition::Unpartitioned,
                                entity_kind: entity.kind,
                                entity_id: entity.id,
                                logical_version: TextLogicalVersion::initial(),
                                live: true,
                            },
                        )),
                    )
                    .await
                    .expect("wrong state owner is written"),
                "state-missing" => db
                    .delete(&state_key)
                    .await
                    .expect("required state row is deleted"),
                _ => panic!("test case list is exhaustive"),
            };

            let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
            assert!(matches!(
                execute_claimed_step(&db, &claimed, &driver, limits, NOW_MILLIS).await,
                Err(HelixDbError::IndexCatalogCorruption(_))
            ));
        }
    }

    #[tokio::test]
    async fn catch_up_validates_each_authoritative_text_source_shape() {
        for (case, should_block) in [
            ("label-changed", false),
            ("property-removed", false),
            ("property-invalid", true),
            ("encoding-malformed", true),
        ] {
            let db = test_db(&format!("text-catch-up-source-{case}")).await;
            let scope = DataScope::LegacyUnscoped;
            let definition = definition(IndexElementKind::Node, None);
            let entity = IndexEntity {
                kind: IndexElementKind::Node,
                id: IndexEntityId::new(1),
            };
            put_source(
                &db,
                scope,
                entity.kind,
                entity.id.get(),
                &properties(
                    "Document",
                    Some(PropertyValue::String("initial text".to_string())),
                    None,
                ),
            )
            .await;
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, entity.id.get()).await;
            let (driver, coordinator) =
                publication_driver(&format!("text-catch-up-source-{case}-blobs"));
            let limits = SearchIndexBackfillLimits::default().batch();
            let mut claim_sequence = 1;
            advance_single_partition_to_catch_up(
                &db,
                operation_id,
                &mut claim_sequence,
                limits,
                &driver,
                coordinator,
            )
            .await;

            match case {
                "label-changed" => {
                    put_source(
                        &db,
                        scope,
                        entity.kind,
                        entity.id.get(),
                        &properties(
                            "Other",
                            Some(PropertyValue::String("ignored text".to_string())),
                            None,
                        ),
                    )
                    .await;
                }
                "property-removed" => {
                    put_source(
                        &db,
                        scope,
                        entity.kind,
                        entity.id.get(),
                        &properties("Document", None, None),
                    )
                    .await;
                }
                "property-invalid" => {
                    put_source(
                        &db,
                        scope,
                        entity.kind,
                        entity.id.get(),
                        &properties("Document", Some(PropertyValue::I64(7)), None),
                    )
                    .await;
                }
                "encoding-malformed" => {
                    db.put(
                        source_key(scope, entity.kind, entity.id.get()),
                        Bytes::from_static(b"malformed-properties"),
                    )
                    .await
                    .expect("malformed authoritative properties are written");
                }
                _ => panic!("test case list is exhaustive"),
            }
            put_build_delta(&db, scope, index_id, generation, entity).await;

            let committed =
                drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await;
            assert_eq!(
                committed,
                if should_block {
                    CommittedOperationStep::Blocked
                } else {
                    CommittedOperationStep::Progressed
                },
                "unexpected authoritative-source result for {case}"
            );
            if should_block {
                assert!(matches!(
                    read_operation(&db, scope, operation_id)
                        .await
                        .execution_state(),
                    IndexOperationExecutionState::Blocked(
                        IndexOperationBlocker::InvalidSourceData { entity_id, .. }
                    ) if *entity_id == entity.id
                ));
                assert!(db
                    .get(build_delta_key(scope, index_id, generation, entity))
                    .await
                    .expect("blocked source delta is readable")
                    .is_some());
            } else {
                let states = entity_state_rows(&db, scope, index_id, generation).await;
                assert_eq!(states.len(), 1);
                assert!(!states[0].1.live);
                assert!(db
                    .get(build_delta_key(scope, index_id, generation, entity))
                    .await
                    .expect("consumed source delta is readable")
                    .is_none());
            }
        }
    }

    #[tokio::test]
    async fn catch_up_rejects_cursor_and_counter_exhaustion_before_commit() {
        let db = test_db("text-catch-up-progress-exhaustion").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("initial text".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (driver, coordinator) = publication_driver("text-catch-up-progress-exhaustion-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &driver,
            coordinator.clone(),
        )
        .await;
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("updated text".to_string())),
                None,
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;
        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::CatchUp(base_progress),
        )) = claimed.record.progress()
        else {
            panic!("claimed operation must be in catch-up");
        };
        let ValidatedDynamicIndexDefinition::Text(text_definition) = &definition else {
            panic!("test definition is text");
        };

        let cursor_progress = PrefixScanProgress {
            cursor: Some(source_cursor(scope, entity.kind, entity.id.get())),
            counters: base_progress.counters,
        };
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("invalid-cursor transaction opens");
        assert!(matches!(
            plan_next_catch_up(
                &transaction,
                scope,
                &claimed.record,
                text_definition,
                &cursor_progress,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);

        for counters in [
            OperationCounters {
                entities: u64::MAX,
                ..base_progress.counters
            },
            OperationCounters {
                input_bytes: u64::MAX,
                ..base_progress.counters
            },
            OperationCounters {
                output_operations: u64::MAX,
                ..base_progress.counters
            },
            OperationCounters {
                output_bytes: u64::MAX,
                ..base_progress.counters
            },
        ] {
            let progress = PrefixScanProgress {
                cursor: None,
                counters,
            };
            let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
            assert!(matches!(
                prepare_catch_up_step(&db, scope, &claimed.record, &progress, limits, dependency,)
                    .await,
                Err(HelixDbError::IndexCatalogCorruption(_))
            ));
        }

        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties("Other", None, None),
        )
        .await;
        for counters in [
            OperationCounters {
                entities: u64::MAX,
                ..base_progress.counters
            },
            OperationCounters {
                input_bytes: u64::MAX,
                ..base_progress.counters
            },
            OperationCounters {
                output_operations: u64::MAX,
                ..base_progress.counters
            },
            OperationCounters {
                output_bytes: u64::MAX,
                ..base_progress.counters
            },
        ] {
            let progress = PrefixScanProgress {
                cursor: None,
                counters,
            };
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("counter-exhaustion transaction opens");
            assert!(matches!(
                catch_up(
                    &transaction,
                    scope,
                    &claimed.record,
                    text_definition,
                    &progress,
                    limits,
                )
                .await,
                Err(HelixDbError::IndexCatalogCorruption(_))
            ));
            drop(transaction);
        }
        assert!(db
            .get(build_delta_key(scope, index_id, generation, entity))
            .await
            .expect("exhaustion delta is readable")
            .is_some());
    }

    #[tokio::test]
    async fn catch_up_blocks_every_exhausted_live_version_transition() {
        let db = test_db("text-catch-up-live-version-exhaustion").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("versioned text".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, entity.id.get()).await;
        let (driver, coordinator) = publication_driver("text-catch-up-live-version-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;
        let operation = read_operation(&db, scope, operation_id).await;
        let maximum =
            TextLogicalVersion::new(u64::MAX).expect("maximum logical version is non-zero");
        let exhausted_unpartitioned = ObservedTextEntityState::Present {
            partition: TextPartition::Unpartitioned,
            key: Bytes::from_static(b"unpartitioned-state"),
            value: Bytes::from_static(b"unpartitioned-value"),
            logical_version: maximum,
            live: true,
        };
        let same_partition = build_text_catch_up_plan(
            &operation,
            entity,
            Bytes::from_static(b"delta"),
            Bytes::from_static(b"delta-value"),
            Bytes::from_static(b"applied"),
            None,
            Bytes::from_static(b"graph"),
            None,
            Some((TextPartition::Unpartitioned, maximum)),
            Some((TextPartition::Unpartitioned, "same".to_string())),
            Some(exhausted_unpartitioned.clone()),
            Some(exhausted_unpartitioned),
            PreparedCatchUpManifestRoots::None,
        )
        .expect("same-partition version exhaustion is modeled");
        assert!(matches!(
            same_partition,
            TextCatchUpPlanRead::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
        let destination = TextPartition::try_tenant_value(Bytes::from_static(b"tenant"))
            .expect("tenant partition is bounded");
        let moved_partition = build_text_catch_up_plan(
            &operation,
            entity,
            Bytes::from_static(b"delta"),
            Bytes::from_static(b"delta-value"),
            Bytes::from_static(b"applied"),
            None,
            Bytes::from_static(b"graph"),
            None,
            Some((TextPartition::Unpartitioned, TextLogicalVersion::initial())),
            Some((destination.clone(), "moved".to_string())),
            Some(ObservedTextEntityState::Present {
                partition: TextPartition::Unpartitioned,
                key: Bytes::from_static(b"previous-state"),
                value: Bytes::from_static(b"previous-value"),
                logical_version: TextLogicalVersion::initial(),
                live: true,
            }),
            Some(ObservedTextEntityState::Present {
                partition: destination,
                key: Bytes::from_static(b"destination-state"),
                value: Bytes::from_static(b"destination-value"),
                logical_version: maximum,
                live: false,
            }),
            PreparedCatchUpManifestRoots::None,
        )
        .expect("moved-partition version exhaustion is modeled");
        assert!(matches!(
            moved_partition,
            TextCatchUpPlanRead::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
        let occupied_destination = TextPartition::try_tenant_value(Bytes::from_static(b"live"))
            .expect("tenant partition is bounded");
        assert!(matches!(
            build_text_catch_up_plan(
                &operation,
                entity,
                Bytes::from_static(b"delta"),
                Bytes::from_static(b"delta-value"),
                Bytes::from_static(b"applied"),
                None,
                Bytes::from_static(b"graph"),
                None,
                Some((TextPartition::Unpartitioned, TextLogicalVersion::initial(),)),
                Some((occupied_destination.clone(), "occupied".to_string())),
                Some(ObservedTextEntityState::Present {
                    partition: TextPartition::Unpartitioned,
                    key: Bytes::from_static(b"previous-state"),
                    value: Bytes::from_static(b"previous-value"),
                    logical_version: TextLogicalVersion::initial(),
                    live: true,
                }),
                Some(ObservedTextEntityState::Present {
                    partition: occupied_destination,
                    key: Bytes::from_static(b"occupied-state"),
                    value: Bytes::from_static(b"occupied-value"),
                    logical_version: TextLogicalVersion::initial(),
                    live: true,
                }),
                PreparedCatchUpManifestRoots::None,
            ),
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert!(matches!(
            build_text_catch_up_plan(
                &operation,
                entity,
                Bytes::from_static(b"delta"),
                Bytes::from_static(b"delta-value"),
                Bytes::from_static(b"applied"),
                None,
                Bytes::from_static(b"graph"),
                None,
                None,
                None,
                Some(ObservedTextEntityState::Absent {
                    partition: TextPartition::Unpartitioned,
                    key: Bytes::from_static(b"unexpected-state"),
                }),
                None,
                PreparedCatchUpManifestRoots::None,
            ),
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert!(matches!(
            build_text_catch_up_plan(
                &operation,
                entity,
                Bytes::from_static(b"delta"),
                Bytes::from_static(b"delta-value"),
                Bytes::from_static(b"applied"),
                None,
                Bytes::from_static(b"graph"),
                None,
                Some((TextPartition::Unpartitioned, TextLogicalVersion::initial(),)),
                None,
                Some(ObservedTextEntityState::Absent {
                    partition: TextPartition::Unpartitioned,
                    key: Bytes::from_static(b"missing-previous"),
                }),
                None,
                PreparedCatchUpManifestRoots::None,
            ),
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert!(matches!(
            build_text_catch_up_plan(
                &operation,
                entity,
                Bytes::from_static(b"delta"),
                Bytes::from_static(b"delta-value"),
                Bytes::from_static(b"applied"),
                None,
                Bytes::from_static(b"graph"),
                None,
                None,
                Some((TextPartition::Unpartitioned, "new".to_string())),
                None,
                Some(ObservedTextEntityState::Present {
                    partition: TextPartition::Unpartitioned,
                    key: Bytes::from_static(b"orphan-state"),
                    value: Bytes::from_static(b"orphan-value"),
                    logical_version: TextLogicalVersion::initial(),
                    live: false,
                }),
                PreparedCatchUpManifestRoots::None,
            ),
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        let missing_destination = TextPartition::try_tenant_value(Bytes::from_static(b"missing"))
            .expect("tenant partition is bounded");
        assert!(matches!(
            build_text_catch_up_plan(
                &operation,
                entity,
                Bytes::from_static(b"delta"),
                Bytes::from_static(b"delta-value"),
                Bytes::from_static(b"applied"),
                None,
                Bytes::from_static(b"graph"),
                None,
                Some((TextPartition::Unpartitioned, TextLogicalVersion::initial(),)),
                Some((missing_destination.clone(), "moved".to_string())),
                Some(ObservedTextEntityState::Absent {
                    partition: TextPartition::Unpartitioned,
                    key: Bytes::from_static(b"missing-move-source"),
                }),
                Some(ObservedTextEntityState::Absent {
                    partition: missing_destination,
                    key: Bytes::from_static(b"move-destination"),
                }),
                PreparedCatchUpManifestRoots::None,
            ),
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        let exhausted_move_destination =
            TextPartition::try_tenant_value(Bytes::from_static(b"exhausted-move"))
                .expect("tenant partition is bounded");
        let exhausted_move = build_text_catch_up_plan(
            &operation,
            entity,
            Bytes::from_static(b"delta"),
            Bytes::from_static(b"delta-value"),
            Bytes::from_static(b"applied"),
            None,
            Bytes::from_static(b"graph"),
            None,
            Some((TextPartition::Unpartitioned, maximum)),
            Some((exhausted_move_destination.clone(), "moved".to_string())),
            Some(ObservedTextEntityState::Present {
                partition: TextPartition::Unpartitioned,
                key: Bytes::from_static(b"exhausted-move-source"),
                value: Bytes::from_static(b"exhausted-move-source-value"),
                logical_version: maximum,
                live: true,
            }),
            Some(ObservedTextEntityState::Absent {
                partition: exhausted_move_destination,
                key: Bytes::from_static(b"exhausted-move-destination"),
            }),
            PreparedCatchUpManifestRoots::None,
        )
        .expect("move-source exhaustion is modeled");
        assert!(matches!(
            exhausted_move,
            TextCatchUpPlanRead::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
    }

    #[tokio::test]
    async fn catch_up_blocks_an_oversized_canonical_tenant_partition() {
        const OVERSIZED_TENANT_BYTES: usize = 16 * 1024 * 1024 + 1;

        let db = test_db("text-catch-up-oversized-tenant").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, Some("account_id"));
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("initial tenant".to_string())),
                Some(PropertyValue::I64(1)),
            ),
        )
        .await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, entity.id.get()).await;
        let (driver, coordinator) = publication_driver("text-catch-up-oversized-tenant-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        advance_single_partition_to_catch_up(
            &db,
            operation_id,
            &mut claim_sequence,
            limits,
            &driver,
            coordinator,
        )
        .await;
        put_source(
            &db,
            scope,
            entity.kind,
            entity.id.get(),
            &properties(
                "Document",
                Some(PropertyValue::String("oversized tenant".to_string())),
                Some(PropertyValue::Bytes(vec![0; OVERSIZED_TENANT_BYTES])),
            ),
        )
        .await;
        put_build_delta(&db, scope, index_id, generation, entity).await;

        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Blocked
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id)
                .await
                .execution_state(),
            IndexOperationExecutionState::Blocked(
                IndexOperationBlocker::InvalidSourceData { entity_id, .. }
            ) if *entity_id == entity.id
        ));
        assert!(db
            .get(build_delta_key(scope, index_id, generation, entity))
            .await
            .expect("oversized-tenant delta is readable")
            .is_some());
    }

    #[tokio::test]
    async fn source_scan_stages_only_matching_text_entities_then_scans_partitions() {
        let db = test_db("text-source-filtering").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("first".to_string())),
                None,
            ),
        )
        .await;
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            2,
            &properties(
                "Other",
                Some(PropertyValue::String("ignored".to_string())),
                None,
            ),
        )
        .await;
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            3,
            &properties("Document", None, None),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 3).await;

        assert_eq!(
            drive_one(
                &db,
                operation_id,
                &mut 1,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );

        let operation = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = operation.progress()
        else {
            panic!("completed source pass must enter partition scan");
        };
        assert_eq!(progress.counters.entities, 3);
        assert_eq!(progress.counters.output_operations, 2);
        assert!(progress.counters.input_bytes > 0);
        assert!(progress.counters.output_bytes > 0);
        assert!(progress.cursor.is_none());
        let Key::Data {
            kind: DataKeyKind::IndexV2(IndexV2Key::TextEntityState(upper)),
            ..
        } = Key::parse_from_slice(scope, progress.inclusive_upper_bound.as_bytes())
            .expect("partition upper bound decodes")
        else {
            panic!("partition upper bound must be a text entity-state key");
        };
        assert_eq!(
            upper.root.partition,
            PartitionFingerprint::new([u8::MAX; 32])
        );
        assert_eq!(upper.entity.kind, IndexElementKind::Edge);
        assert_eq!(upper.entity.id, IndexEntityId::new(u64::MAX));

        let rows = entity_state_rows(&db, scope, index_id, generation).await;
        assert_eq!(rows.len(), 1);
        let (key, value) = &rows[0];
        assert_eq!(key.entity.id, IndexEntityId::new(1));
        assert_eq!(
            key.root.partition,
            TextPartition::Unpartitioned.fingerprint()
        );
        assert_eq!(value.index_id, index_id);
        assert_eq!(value.generation, generation);
        assert_eq!(value.partition, TextPartition::Unpartitioned);
        assert_eq!(value.entity_kind, IndexElementKind::Node);
        assert_eq!(value.entity_id, IndexEntityId::new(1));
        assert_eq!(value.logical_version, TextLogicalVersion::initial());
        assert!(value.live);
    }

    #[tokio::test]
    async fn partition_keys_group_interleaved_tenants_before_split_construction() {
        let db = test_db("text-source-partition-order").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, Some("account_id"));
        for (entity_id, tenant) in [(1, 7), (2, 9), (3, 7)] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!("body-{entity_id}"))),
                    Some(PropertyValue::I64(tenant)),
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 3).await;

        assert_eq!(
            drive_one(
                &db,
                operation_id,
                &mut 1,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );

        let rows = entity_state_rows(&db, scope, index_id, generation).await;
        assert_eq!(rows.len(), 3);
        let position = |entity_id| {
            rows.iter()
                .position(|(_, value)| value.entity_id == IndexEntityId::new(entity_id))
                .expect("staged entity is present")
        };
        let first_seven = position(1);
        let second_seven = position(3);
        assert_eq!(first_seven.abs_diff(second_seven), 1);
        assert_eq!(
            rows[first_seven].1.partition,
            rows[second_seven].1.partition
        );
        assert_ne!(rows[first_seven].1.partition, rows[position(2)].1.partition);
        for (key, value) in rows {
            assert_eq!(key.root.partition, value.partition.fingerprint());
        }
    }

    #[tokio::test]
    async fn invalid_source_blocks_without_committing_an_earlier_staged_row() {
        let db = test_db("text-source-invalid-atomic").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("valid".to_string())),
                None,
            ),
        )
        .await;
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            2,
            &properties("Document", Some(PropertyValue::I64(42)), None),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;

        assert_eq!(
            drive_one(
                &db,
                operation_id,
                &mut 1,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Blocked
        );

        assert!(entity_state_rows(&db, scope, index_id, generation)
            .await
            .is_empty());
        let operation = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvalidSourceData {
                entity_kind: IndexElementKind::Node,
                entity_id,
            }) if *entity_id == IndexEntityId::new(2)
        ));
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanSource(progress),
        )) = operation.progress()
        else {
            panic!("blocked source scan must retain its original checkpoint");
        };
        assert!(progress.cursor.is_none());
        assert_eq!(progress.counters, OperationCounters::default());
    }

    #[tokio::test]
    async fn missing_tenant_partition_blocks_the_source_pass() {
        let db = test_db("text-source-missing-tenant").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Edge, Some("account_id"));
        put_source(
            &db,
            scope,
            IndexElementKind::Edge,
            4,
            &properties(
                "Document",
                Some(PropertyValue::String("edge".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 4).await;

        assert_eq!(
            drive_one(
                &db,
                operation_id,
                &mut 1,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Blocked
        );
        assert!(entity_state_rows(&db, scope, index_id, generation)
            .await
            .is_empty());
        let operation = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvalidSourceData {
                entity_kind: IndexElementKind::Edge,
                entity_id,
            }) if *entity_id == IndexEntityId::new(4)
        ));
    }

    #[tokio::test]
    async fn malformed_property_encoding_blocks_without_entity_state() {
        let db = test_db("text-source-malformed-properties").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        db.put(
            source_key(scope, IndexElementKind::Node, 5),
            Bytes::from_static(&[1, 2, 3]),
        )
        .await
        .expect("malformed source bytes are written for validation");
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 5).await;

        assert_eq!(
            drive_one(
                &db,
                operation_id,
                &mut 1,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Blocked
        );
        assert!(entity_state_rows(&db, scope, index_id, generation)
            .await
            .is_empty());
        let operation = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvalidSourceData {
                entity_kind: IndexElementKind::Node,
                entity_id,
            }) if *entity_id == IndexEntityId::new(5)
        ));
    }

    #[tokio::test]
    async fn indivisible_input_and_output_rows_block_at_their_exact_limits() {
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let source = properties(
            "Document",
            Some(PropertyValue::String("bounded".to_string())),
            None,
        );

        let input_db = test_db("text-source-input-limit").await;
        put_source(&input_db, scope, IndexElementKind::Node, 6, &source).await;
        let (input_operation, input_index, input_generation) =
            create_build(&input_db, scope, &definition, 6).await;
        let input_limits = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::MIN,
            NonZeroU64::new(16).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .expect("input-limited policy validates");
        assert_eq!(
            drive_one(&input_db, input_operation, &mut 1, input_limits).await,
            CommittedOperationStep::Blocked
        );
        assert!(
            entity_state_rows(&input_db, scope, input_index, input_generation)
                .await
                .is_empty()
        );
        let input_operation = read_operation(&input_db, scope, input_operation).await;
        assert!(matches!(
            input_operation.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::OversizedEntity {
                observed,
                limit: 1,
                ..
            }) if *observed > 1
        ));

        let output_db = test_db("text-source-output-limit").await;
        put_source(&output_db, scope, IndexElementKind::Node, 7, &source).await;
        let (output_operation, output_index, output_generation) =
            create_build(&output_db, scope, &definition, 7).await;
        let output_limits = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::MIN,
            NonZeroU64::MIN,
            NonZeroU64::MIN,
        )
        .expect("output-limited policy validates");
        assert_eq!(
            drive_one(&output_db, output_operation, &mut 1, output_limits).await,
            CommittedOperationStep::Blocked
        );
        assert!(
            entity_state_rows(&output_db, scope, output_index, output_generation)
                .await
                .is_empty()
        );
        let output_operation = read_operation(&output_db, scope, output_operation).await;
        assert!(matches!(
            output_operation.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::OversizedEntity {
                observed,
                limit: 1,
                ..
            }) if *observed > 1
        ));
    }

    #[test]
    fn typed_source_cursor_and_counter_helpers_fail_closed() {
        let scope = DataScope::LegacyUnscoped;
        let edge_pair = Key::Data {
            scope,
            kind: DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(1, 2)),
        }
        .to_bytes();
        assert_eq!(
            source_entity(scope, IndexElementKind::Edge, &edge_pair).unwrap(),
            None
        );
        let edge_by_id = source_key(scope, IndexElementKind::Edge, 3);
        assert!(source_entity(scope, IndexElementKind::Node, &edge_by_id).is_err());

        let foreign_cursor =
            IndexCursor::try_new(Bytes::from_static(b"foreign-key")).expect("cursor is bounded");
        assert!(cursor_suffix(
            &Bytes::from_static(b"expected-prefix"),
            Some(&foreign_cursor)
        )
        .is_err());
        assert!(checked_add(u64::MAX, 1, "test counter").is_err());
        assert!(matches!(
            operation_error(IndexOperationModelError::ZeroClaimSequence),
            HelixDbError::InvariantViolation(_)
        ));
    }

    #[tokio::test]
    async fn invalid_source_checkpoint_keyspaces_fail_before_scanning() {
        let db = test_db("text-source-invalid-checkpoints").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Edge, None);
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let operation = read_operation(&db, scope, operation_id).await;
        let ValidatedDynamicIndexDefinition::Text(definition) = &definition else {
            panic!("test definition is text");
        };
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("text checkpoint transaction opens");
        let edge_pair_upper = IndexCursor::try_new(
            Key::Data {
                scope,
                kind: DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(1, 2)),
            }
            .to_bytes(),
        )
        .expect("edge-pair key is a bounded cursor");
        assert!(scan_source(
            &transaction,
            scope,
            &operation,
            definition,
            &SourceScanProgress {
                inclusive_upper_bound: edge_pair_upper,
                cursor: None,
                counters: OperationCounters::default(),
            },
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .is_err());
        assert!(scan_source(
            &transaction,
            scope,
            &operation,
            definition,
            &SourceScanProgress {
                inclusive_upper_bound: source_cursor(scope, IndexElementKind::Edge, 1),
                cursor: Some(source_cursor(scope, IndexElementKind::Edge, 2)),
                counters: OperationCounters::default(),
            },
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn shared_edge_prefix_rows_advance_without_becoming_text_entities() {
        let db = test_db("text-source-shared-edge-prefix").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Edge, None);
        db.put(
            Key::Data {
                scope,
                kind: DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(0, 9)),
            }
            .to_bytes(),
            Bytes::from_static(b"not-an-edge-property-row"),
        )
        .await
        .expect("shared-prefix edge-pair row is written");
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 10).await;

        assert_eq!(
            drive_one(
                &db,
                operation_id,
                &mut 1,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(entity_state_rows(&db, scope, index_id, generation)
            .await
            .is_empty());
        let operation = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = operation.progress()
        else {
            panic!("exhausted shared-prefix scan enters partition scan");
        };
        assert_eq!(progress.counters.entities, 1);
        assert_eq!(progress.counters.output_operations, 0);
    }

    #[tokio::test]
    async fn later_input_and_output_limits_commit_only_the_admitted_prefix() {
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let source = properties(
            "Document",
            Some(PropertyValue::String("same-size".to_string())),
            None,
        );
        let one_input = source_key(scope, IndexElementKind::Node, 1)
            .len()
            .saturating_add(encode_properties(&source).len()) as u64;

        let input_db = test_db("text-source-later-input-limit").await;
        for entity_id in [1, 2] {
            put_source(&input_db, scope, IndexElementKind::Node, entity_id, &source).await;
        }
        let (input_operation, input_index, input_generation) =
            create_build(&input_db, scope, &definition, 2).await;
        let input_limits = SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::new(one_input).unwrap(),
            NonZeroU64::new(8).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .expect("later-input policy validates");
        assert_eq!(
            drive_one(&input_db, input_operation, &mut 1, input_limits).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            entity_state_rows(&input_db, scope, input_index, input_generation)
                .await
                .len(),
            1
        );
        let input_operation = read_operation(&input_db, scope, input_operation).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanSource(input_progress),
        )) = input_operation.progress()
        else {
            panic!("input-limited step remains in source scan");
        };
        assert_eq!(
            input_progress.cursor.as_ref().unwrap().as_bytes(),
            &source_key(scope, IndexElementKind::Node, 1)
        );

        let output_db = test_db("text-source-later-output-limit").await;
        for entity_id in [1, 2] {
            put_source(
                &output_db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &source,
            )
            .await;
        }
        let (output_operation, output_index, output_generation) =
            create_build(&output_db, scope, &definition, 2).await;
        let output_limits = SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(2).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .expect("later-output policy validates");
        assert_eq!(
            drive_one(&output_db, output_operation, &mut 1, output_limits).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            entity_state_rows(&output_db, scope, output_index, output_generation)
                .await
                .len(),
            1
        );
        let output_operation = read_operation(&output_db, scope, output_operation).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanSource(output_progress),
        )) = output_operation.progress()
        else {
            panic!("output-limited step remains in source scan");
        };
        assert_eq!(
            output_progress.cursor.as_ref().unwrap().as_bytes(),
            &source_key(scope, IndexElementKind::Node, 1)
        );
    }

    #[tokio::test]
    async fn preexisting_entity_state_fails_without_advancing_the_checkpoint() {
        let db = test_db("text-source-preexisting-state").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            8,
            &properties(
                "Document",
                Some(PropertyValue::String("existing".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 8).await;
        let state_key = scoped_index_key(
            scope,
            IndexV2Key::TextEntityState(TextEntityStateKey {
                root: TextManifestRootKey {
                    index_id,
                    generation,
                    partition: TextPartition::Unpartitioned.fingerprint(),
                },
                entity: IndexEntity {
                    kind: IndexElementKind::Node,
                    id: IndexEntityId::new(8),
                },
            }),
        );
        db.put(
            state_key,
            encode_work_value(&IndexV2WorkValue::TextEntityState(TextEntityStateValue {
                index_id,
                generation,
                partition: TextPartition::Unpartitioned,
                entity_kind: IndexElementKind::Node,
                entity_id: IndexEntityId::new(8),
                logical_version: TextLogicalVersion::initial(),
                live: true,
            })),
        )
        .await
        .expect("pre-existing typed text state is written");

        assert_eq!(
            drive_one(
                &db,
                operation_id,
                &mut 1,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::TransientFailure
        );
        let operation = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanSource(progress),
        )) = operation.progress()
        else {
            panic!("pre-existing state must retain source progress");
        };
        assert!(progress.cursor.is_none());
        assert_eq!(progress.counters, OperationCounters::default());
    }

    #[tokio::test]
    async fn canonical_record_absence_and_revision_mismatch_fail_closed() {
        let db = test_db("text-source-canonical-mismatch").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let operation = read_operation(&db, scope, operation_id).await;

        let missing = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("missing-canonical transaction opens");
        missing
            .delete(scoped_index_key(
                scope,
                IndexV2Key::index_record(operation.identity().clone()),
            ))
            .expect("canonical delete is staged");
        assert!(load_operation_index(&missing, scope, &operation)
            .await
            .is_err());

        let mismatched_operation = IndexOperationRecord::try_new(
            operation.operation_id(),
            operation.index_id(),
            operation.identity().clone(),
            operation.generation(),
            operation
                .index_record_revision()
                .checked_next()
                .expect("test canonical revision advances"),
            operation.operation_revision(),
            operation.kind(),
            operation.family(),
            operation.progress().clone(),
            operation.attempt(),
            operation.execution_state().clone(),
        )
        .expect("revision-mismatched operation remains structurally valid");
        let mismatched = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("mismatched-canonical transaction opens");
        assert!(
            load_operation_index(&mismatched, scope, &mismatched_operation)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cumulative_counter_overflow_rejects_the_entire_source_batch() {
        let db = test_db("text-source-counter-overflow").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            9,
            &properties(
                "Document",
                Some(PropertyValue::String("overflow".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 9).await;
        let operation = read_operation(&db, scope, operation_id).await;
        let ValidatedDynamicIndexDefinition::Text(definition) = &definition else {
            panic!("test definition is text");
        };
        let upper = source_cursor(scope, IndexElementKind::Node, 9);
        let counters = [
            OperationCounters {
                entities: u64::MAX,
                ..OperationCounters::default()
            },
            OperationCounters {
                input_bytes: u64::MAX,
                ..OperationCounters::default()
            },
            OperationCounters {
                output_operations: u64::MAX,
                ..OperationCounters::default()
            },
            OperationCounters {
                output_bytes: u64::MAX,
                ..OperationCounters::default()
            },
        ];
        for counters in counters {
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("counter-overflow transaction opens");
            assert!(scan_source(
                &transaction,
                scope,
                &operation,
                definition,
                &SourceScanProgress {
                    inclusive_upper_bound: upper.clone(),
                    cursor: None,
                    counters,
                },
                SearchIndexBackfillLimits::default().batch(),
            )
            .await
            .is_err());
        }
        assert!(entity_state_rows(&db, scope, index_id, generation)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn oversized_canonical_tenant_partition_blocks_before_staging() {
        const OVERSIZED_TENANT_BYTES: usize = 16 * 1024 * 1024 + 1;

        let db = test_db("text-source-oversized-tenant").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, Some("account_id"));
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            10,
            &properties(
                "Document",
                Some(PropertyValue::String("tenant".to_string())),
                Some(PropertyValue::Bytes(vec![0; OVERSIZED_TENANT_BYTES])),
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 10).await;
        let limits = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            NonZeroU64::new(8).unwrap(),
            NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        )
        .expect("large-source validation policy validates");

        assert_eq!(
            drive_one(&db, operation_id, &mut 1, limits).await,
            CommittedOperationStep::Blocked
        );
        assert!(entity_state_rows(&db, scope, index_id, generation)
            .await
            .is_empty());
        let operation = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvalidSourceData {
                entity_id,
                ..
            }) if *entity_id == IndexEntityId::new(10)
        ));
    }

    #[tokio::test]
    async fn bounded_source_steps_resume_strictly_after_the_committed_cursor() {
        let db = test_db("text-source-resume").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for entity_id in [1, 2] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!("body-{entity_id}"))),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let limits = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(16).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .expect("one-entity limits validate");
        let mut claim_sequence = 1;

        assert_eq!(
            drive_one(&db, operation_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
        let first = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanSource(first),
        )) = first.progress()
        else {
            panic!("first bounded step must remain in source scan");
        };
        assert_eq!(
            first.cursor.as_ref().unwrap().as_bytes(),
            &source_key(scope, IndexElementKind::Node, 1)
        );
        assert_eq!(first.counters.entities, 1);
        assert_eq!(first.counters.output_operations, 2);

        assert_eq!(
            drive_one(&db, operation_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
        let second = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanSource(second),
        )) = second.progress()
        else {
            panic!("full second batch conservatively retains source scan");
        };
        assert_eq!(
            second.cursor.as_ref().unwrap().as_bytes(),
            &source_key(scope, IndexElementKind::Node, 2)
        );
        assert_eq!(second.counters.entities, 2);
        assert_eq!(second.counters.output_operations, 4);

        assert_eq!(
            drive_one(&db, operation_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
        let third = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            third.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ScanPartitions(_)
            ))
        ));
        let rows = entity_state_rows(&db, scope, index_id, generation).await;
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn empty_tenant_partitioned_source_creates_no_fabricated_root() {
        let db = test_db("text-empty-tenant-source").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, Some("account_id"));
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let (driver, _) = publication_driver("text-empty-tenant-source-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;

        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(_)
            ))
        ));
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextManifestRoot,
                index_id,
                generation,
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn partition_step_commits_exact_child_then_publishes_multi_document_split() {
        let db = test_db("text-partition-atomic-child").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        for (entity_id, text) in [(1, "first document"), (2, "second document")] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(text.to_string())),
                    None,
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let (driver, coordinator) = publication_driver("text-partition-atomic-child-blobs");
        let mut claim_sequence = 1;
        let limits = SearchIndexBackfillLimits::default().batch();

        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ScanPartitions(_)
            ))
        ));
        seed_next_partition_manifest_root(&db, operation_id, &mut claim_sequence, limits, &driver)
            .await;
        assert_eq!(
            read_manifest_root(
                &db,
                scope,
                index_id,
                generation,
                &TextPartition::Unpartitioned,
            )
            .await,
            Some(work::TextManifestRootValue::empty(
                index_id,
                generation,
                TextPartition::Unpartitioned,
            ))
        );

        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        let operation = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitUpload(wait),
        )) = operation.progress()
        else {
            panic!("partition split must wait on its exact upload child");
        };
        assert_eq!(wait.completed_counters().entities, 4);
        assert_eq!(wait.completed_counters().output_operations, 6);
        assert!(db
            .get(wait.artifact_key().as_bytes())
            .await
            .expect("artifact point read succeeds")
            .is_none());

        let intent = crate::index_v2::repository::load_upload_from_pointer(&db, wait.intent_id())
            .await
            .expect("prepared child triple is readable")
            .expect("prepared child is durable");
        assert_eq!(intent.index_id, index_id);
        assert_eq!(intent.generation, generation);
        assert!(matches!(
            intent.owner,
            work::TextUploadOwner::Build {
                operation_id: owner_operation_id,
                expected_operation_revision,
            } if owner_operation_id == operation_id
                && expected_operation_revision == operation.operation_revision()
        ));
        let work::TextUploadAttachment::BuildArtifact {
            artifact_ordinal,
            split,
        } = intent.attachment
        else {
            panic!("partition child must declare one build artifact");
        };
        assert_eq!(artifact_ordinal, 5);
        assert_eq!(intent.blob, split.blob());

        let intent_owner =
            IndexV2Key::TextUploadIntent(crate::encoding::v1::keys::index_v2::TextIntentOwnedKey {
                index_id,
                generation,
                intent_id: intent.intent_id,
            });
        let reference_key = Key::Global {
            kind: crate::encoding::v1::keys::GlobalKeyKind::IndexV2(
                crate::encoding::v1::keys::index_v2::GlobalIndexV2Key::BlobReachabilityReference(
                    crate::encoding::v1::keys::index_v2::BlobReferenceGlobalKey::try_new(
                        crate::encoding::v1::keys::index_v2::BlobHash::new(*intent.blob.hash()),
                        crate::encoding::v1::keys::index_v2::BlobReferenceOwnerKind::UploadIntent,
                        scope,
                        intent_owner.to_bytes(),
                        0,
                    )
                    .expect("typed intent owner forms a reachability key"),
                ),
            ),
        }
        .to_bytes();
        assert!(db
            .get(reference_key)
            .await
            .expect("intent reachability point read succeeds")
            .is_some());

        let status = coordinator
            .publication_status(&BlobPublicationPermit::from_id(
                intent.publication_permit_id,
            ))
            .await
            .expect("durable child permit remains observable");
        assert!(matches!(
            status,
            crate::index_v2::blob_publication::BlobPublicationStatus::Succeeded(metadata)
                if metadata.blob() == intent.blob
        ));
    }

    #[tokio::test]
    async fn partition_scan_keeps_interleaved_tenant_entities_in_one_contiguous_run() {
        let db = test_db("text-partition-contiguous-tenants").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, Some("account_id"));
        for (entity_id, tenant) in [(1, "a"), (2, "b"), (3, "a"), (4, "b")] {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &properties(
                    "Document",
                    Some(PropertyValue::String(format!("document-{entity_id}"))),
                    Some(PropertyValue::String(tenant.to_string())),
                ),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 4).await;
        let (driver, _) = publication_driver("text-partition-contiguous-tenants-blobs");
        let mut claim_sequence = 1;
        let limits = SearchIndexBackfillLimits::default().batch();
        assert_eq!(
            drive_one(&db, operation_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
        let staged = entity_state_rows(&db, scope, index_id, generation).await;
        let first_fingerprint = staged[0].0.root.partition;
        let expected_entities = staged
            .iter()
            .take_while(|(key, _)| key.root.partition == first_fingerprint)
            .map(|(key, _)| key.entity.id.get())
            .collect::<Vec<_>>();
        assert_eq!(expected_entities.len(), 2);
        seed_next_partition_manifest_root(&db, operation_id, &mut claim_sequence, limits, &driver)
            .await;

        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = claimed.record.progress()
        else {
            panic!("source staging must enter partition scan");
        };
        let ValidatedDynamicIndexDefinition::Text(definition) = &definition else {
            panic!("test definition is text");
        };
        let snapshot = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("partition read snapshot opens");
        let selection = scan_partition_documents(
            &snapshot,
            scope,
            &claimed.record,
            definition,
            progress,
            limits,
        )
        .await
        .expect("partition run validates");
        let PartitionScanSelection::Upload(documents) = selection else {
            panic!("matching current rows produce documents");
        };
        assert_eq!(
            documents
                .documents
                .iter()
                .map(|document| document.entity_id)
                .collect::<Vec<_>>(),
            expected_entities
        );
        assert_eq!(documents.partition.fingerprint(), first_fingerprint);
    }

    #[tokio::test]
    async fn partition_scan_with_no_current_documents_advances_without_an_upload() {
        let db = test_db("text-partition-empty-current-run").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("deleted before split".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let (driver, _) = publication_driver("text-partition-empty-current-run-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        seed_next_partition_manifest_root(&db, operation_id, &mut claim_sequence, limits, &driver)
            .await;
        db.delete(source_key(scope, IndexElementKind::Node, 1))
            .await
            .expect("authoritative graph row is deleted");

        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        let operation = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            operation.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(_)
            ))
        ));
        assert_eq!(
            read_manifest_root(
                &db,
                scope,
                index_id,
                generation,
                &TextPartition::Unpartitioned,
            )
            .await,
            Some(work::TextManifestRootValue::empty(
                index_id,
                generation,
                TextPartition::Unpartitioned,
            ))
        );
        let prefix = crate::encoding::v1::keys::index_v2::GlobalIndexV2Key::logical_prefix(
            crate::encoding::v1::keys::index_v2::GlobalIndexV2Kind::UploadPointer,
        );
        let mut uploads = db
            .scan_prefix(prefix, ..)
            .await
            .expect("upload pointer prefix is readable");
        assert!(uploads
            .next()
            .await
            .expect("upload pointer scan succeeds")
            .is_none());
    }

    #[tokio::test]
    async fn oversized_partition_split_blocks_before_reserving_a_child() {
        let db = test_db("text-partition-split-limit").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("split exceeds one byte".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let (driver, _) = publication_driver("text-partition-split-limit-blobs");
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(
                &db,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        seed_next_partition_manifest_root(
            &db,
            operation_id,
            &mut claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
            &driver,
        )
        .await;
        let limits = SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(8).unwrap(),
            NonZeroU64::MIN,
            NonZeroU64::MIN,
        )
        .expect("one-byte split limit is structurally valid");
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver,).await,
            CommittedOperationStep::Blocked
        );
        let operation = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::ManifestLimit {
                observed,
                limit: 1,
                ..
            }) if *observed > 1
        ));
    }

    #[tokio::test]
    async fn uncommitted_partition_preparation_is_point_read_then_released() {
        let db = test_db("text-partition-uncommitted-release").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("uncommitted".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let (driver, coordinator) = publication_driver("text-partition-uncommitted-release-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        seed_next_partition_manifest_root(&db, operation_id, &mut claim_sequence, limits, &driver)
            .await;
        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = claimed.record.progress()
        else {
            panic!("source staging must enter partition scan");
        };
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let prepared =
            prepare_partition_step(&db, scope, &claimed.record, progress, limits, dependency)
                .await
                .expect("partition preparation succeeds");
        let PreparedTextOperationStep::PartitionUpload(upload) = &prepared else {
            panic!("current document must prepare one upload");
        };
        let permit = upload.intent.permit();
        assert_eq!(
            prepared
                .resolve_commit_error(&db, scope)
                .await
                .expect("exact point reads resolve noncommit"),
            PreparedStepCommitResolution::NotCommitted
        );
        prepared
            .discard()
            .await
            .expect("uncommitted reservation releases");
        assert!(matches!(
            coordinator.publication_status(&permit).await,
            Err(crate::index_v2::blob_publication::BlobPublicationError::UnknownPermit)
        ));
    }

    #[tokio::test]
    async fn durable_partition_preparation_resolves_commit_before_publication() {
        let db = test_db("text-partition-ambiguous-resolution").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("durable resolution".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let (driver, coordinator) = publication_driver("text-partition-ambiguous-resolution-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver,).await,
            CommittedOperationStep::Progressed
        );
        seed_next_partition_manifest_root(&db, operation_id, &mut claim_sequence, limits, &driver)
            .await;
        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = claimed.record.progress()
        else {
            panic!("source staging must enter partition scan");
        };
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let prepared =
            prepare_partition_step(&db, scope, &claimed.record, progress, limits, dependency)
                .await
                .expect("partition preparation succeeds");
        let _next = commit_prepared_build_child(&db, scope, &claimed, &prepared).await;

        assert_eq!(
            prepared
                .resolve_commit_error(&db, scope)
                .await
                .expect("exact point reads resolve durability"),
            PreparedStepCommitResolution::Committed
        );
        let PreparedTextOperationStep::PartitionUpload(upload) = &prepared else {
            panic!("current document must prepare one upload");
        };
        let permit = upload.intent.permit();
        assert_eq!(
            coordinator
                .publication_status(&permit)
                .await
                .expect("permit remains reserved before post-commit hook"),
            crate::index_v2::blob_publication::BlobPublicationStatus::Reserved
        );
        prepared.after_commit().await;
        assert!(matches!(
            coordinator.publication_status(&permit).await,
            Ok(crate::index_v2::blob_publication::BlobPublicationStatus::Succeeded(_))
        ));
    }

    #[tokio::test]
    async fn partition_step_without_publication_runtime_retries_without_a_child() {
        let db = test_db("text-partition-runtime-unavailable").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("retry without runtime".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one(&db, operation_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one(&db, operation_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::TransientFailure
        );

        let operation = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            operation.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ScanPartitions(_)
            ))
        ));
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: Some(_)
            }
        ));
        let prefix = crate::encoding::v1::keys::index_v2::GlobalIndexV2Key::logical_prefix(
            crate::encoding::v1::keys::index_v2::GlobalIndexV2Kind::UploadPointer,
        );
        let mut uploads = db
            .scan_prefix(prefix, ..)
            .await
            .expect("upload pointer prefix is readable");
        assert!(uploads
            .next()
            .await
            .expect("upload pointer scan succeeds")
            .is_none());
    }

    #[tokio::test]
    async fn prepared_partition_child_rejects_stale_occupied_and_partial_checkpoints() {
        let db = test_db("text-partition-prepared-exactness").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("exact prepared child".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let (driver, coordinator) = publication_driver("text-partition-prepared-exactness-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        seed_next_partition_manifest_root(&db, operation_id, &mut claim_sequence, limits, &driver)
            .await;
        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = claimed.record.progress()
        else {
            panic!("source staging must enter partition scan");
        };
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let prepared =
            prepare_partition_step(&db, scope, &claimed.record, progress, limits, dependency)
                .await
                .expect("partition preparation succeeds");
        let PreparedTextOperationStep::PartitionUpload(upload) = &prepared else {
            panic!("current document must prepare one upload");
        };
        let artifact_key = upload.artifact_key.clone();
        let permit = upload.intent.permit();
        let intent = upload.intent.value();
        let work::TextUploadAttachment::BuildArtifact {
            artifact_ordinal,
            split,
        } = intent.attachment
        else {
            panic!("partition upload must target one build artifact");
        };
        let artifact_value = encode_work_value(&IndexV2WorkValue::TextBuildArtifact(
            work::TextBuildArtifactValue {
                index_id: intent.index_id,
                generation: intent.generation,
                partition: intent.partition.clone(),
                artifact_ordinal,
                split,
                source_intent_id: intent.intent_id,
            },
        ));

        let root_key = scoped_index_key(
            scope,
            IndexV2Key::TextManifestRoot(TextManifestRootKey {
                index_id: intent.index_id,
                generation: intent.generation,
                partition: intent.partition.fingerprint(),
            }),
        );
        let root_value = db
            .get(&root_key)
            .await
            .expect("prepared root is readable")
            .expect("prepared root exists");
        db.delete(&root_key)
            .await
            .expect("test removes the observed root");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("stale-root transaction opens");
        assert!(matches!(
            prepared
                .stage(&transaction, scope, &claimed.record)
                .await
                .expect("stale root maps to a retry"),
            IndexOperationStepResult::TransientFailure
        ));
        drop(transaction);
        assert!(
            crate::index_v2::repository::load_upload_from_pointer(&db, intent.intent_id)
                .await
                .expect("absent child pointer is readable")
                .is_none()
        );
        db.put(root_key, root_value)
            .await
            .expect("exact empty root is restored");

        let stale = claimed
            .record
            .transient_failure(NOW_MILLIS + 1)
            .expect("claimed operation can form a stale queued revision");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("stale-operation transaction opens");
        assert!(matches!(
            prepared.stage(&transaction, scope, &stale).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);

        db.put(artifact_key.clone(), artifact_value)
            .await
            .expect("typed occupied artifact is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("occupied-artifact transaction opens");
        assert!(matches!(
            prepared.stage(&transaction, scope, &claimed.record).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);
        db.delete(artifact_key)
            .await
            .expect("occupied artifact is removed");

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("child-only transaction opens");
        assert!(matches!(
            prepared
                .stage(&transaction, scope, &claimed.record)
                .await
                .expect("exact prepared child stages"),
            IndexOperationStepResult::Progressed(_)
        ));
        transaction
            .commit()
            .await
            .expect("child-only checkpoint commits for disagreement test");

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("already-durable transaction opens");
        assert!(matches!(
            prepared.stage(&transaction, scope, &claimed.record).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);
        assert!(matches!(
            prepared.resolve_commit_error(&db, scope).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert_eq!(
            coordinator
                .publication_status(&permit)
                .await
                .expect("disagreeing checkpoint retains its reservation"),
            crate::index_v2::blob_publication::BlobPublicationStatus::Reserved
        );
    }

    #[tokio::test]
    async fn prepared_empty_root_seed_retries_when_absence_goes_stale() {
        let db = test_db("text-empty-root-stale-absence").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("stale empty root".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let (driver, coordinator) = publication_driver("text-empty-root-stale-absence-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = claimed.record.progress()
        else {
            panic!("source staging must enter partition scan");
        };
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let prepared =
            prepare_partition_step(&db, scope, &claimed.record, progress, limits, dependency)
                .await
                .expect("missing root prepares a repository seed");
        let PreparedTextOperationStep::Repository(_) = &prepared else {
            panic!("missing root must be seeded before partition upload");
        };
        let root_key = scoped_index_key(
            scope,
            IndexV2Key::TextManifestRoot(TextManifestRootKey {
                index_id,
                generation,
                partition: TextPartition::Unpartitioned.fingerprint(),
            }),
        );
        db.put(
            root_key,
            encode_work_value(&IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::try_new(
                    index_id,
                    generation,
                    TextPartition::Unpartitioned,
                    TextManifestRevision::initial(),
                    1,
                    1,
                )
                .expect("concurrent populated root is structurally valid"),
            )),
        )
        .await
        .expect("concurrent root is written after preparation");

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("stale seed transaction opens");
        assert!(matches!(
            prepared
                .stage(&transaction, scope, &claimed.record)
                .await
                .expect("stale absence maps to retry"),
            IndexOperationStepResult::TransientFailure
        ));
        drop(transaction);
        let root = read_manifest_root(
            &db,
            scope,
            index_id,
            generation,
            &TextPartition::Unpartitioned,
        )
        .await
        .expect("concurrent root remains present");
        assert_eq!(root.page_count(), 1);
        assert_eq!(root.split_count(), 1);
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextUploadIntent,
                index_id,
                generation,
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn empty_root_output_limit_blocks_before_root_or_child_creation() {
        let db = test_db("text-empty-root-output-limit").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("root output limit".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let (driver, _) = publication_driver("text-empty-root-output-limit-blobs");
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(
                &db,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let limits = SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(8).unwrap(),
            NonZeroU64::MIN,
            NonZeroU64::MIN,
        )
        .expect("one-byte output limit is structurally valid");
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Blocked
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id)
                .await
                .execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::ManifestLimit {
                observed,
                limit: 1,
                ..
            }) if *observed > 1
        ));
        assert!(read_manifest_root(
            &db,
            scope,
            index_id,
            generation,
            &TextPartition::Unpartitioned,
        )
        .await
        .is_none());
    }

    #[tokio::test]
    async fn nonempty_or_wrong_kind_root_before_partition_build_is_corruption() {
        let db = test_db("text-populated-root-before-partition-build").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("premature root".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let (driver, coordinator) =
            publication_driver("text-populated-root-before-partition-build-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        let root_key = scoped_index_key(
            scope,
            IndexV2Key::TextManifestRoot(TextManifestRootKey {
                index_id,
                generation,
                partition: TextPartition::Unpartitioned.fingerprint(),
            }),
        );
        db.put(
            root_key.clone(),
            encode_work_value(&IndexV2WorkValue::AppliedEntityState(
                AppliedEntityStateValue {
                    index_id,
                    generation,
                    entity_kind: IndexElementKind::Node,
                    entity_id: IndexEntityId::new(1),
                    state: AppliedFamilyState::Text(None),
                },
            )),
        )
        .await
        .expect("wrong root value kind is written");
        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = claimed.record.progress()
        else {
            panic!("source staging must enter partition scan");
        };
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        assert!(matches!(
            prepare_partition_step(&db, scope, &claimed.record, progress, limits, dependency,)
                .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));

        db.put(
            root_key,
            encode_work_value(&IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::try_new(
                    index_id,
                    generation,
                    TextPartition::Unpartitioned,
                    TextManifestRevision::initial(),
                    1,
                    1,
                )
                .expect("populated root is structurally valid"),
            )),
        )
        .await
        .expect("premature populated root is written");
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        assert!(matches!(
            prepare_partition_step(&db, scope, &claimed.record, progress, limits, dependency,)
                .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert_eq!(
            generation_row_count(
                &db,
                scope,
                IndexV2RecordKind::TextUploadIntent,
                index_id,
                generation,
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn empty_root_input_limit_blocks_before_root_or_child_creation() {
        let db = test_db("text-partition-input-limit").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String(
                    "partition input exceeds one byte".to_string(),
                )),
                None,
            ),
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let (driver, _) = publication_driver("text-partition-input-limit-blobs");
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(
                &db,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let limits = SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::MIN,
            NonZeroU64::new(8).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::MIN,
        )
        .expect("one-byte input limit is structurally valid");
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Blocked
        );
        let operation = read_operation(&db, scope, operation_id).await;
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::ManifestLimit {
                observed,
                limit: 1,
                ..
            }) if *observed > 1
        ));
        let root_key = scoped_index_key(
            scope,
            IndexV2Key::TextManifestRoot(TextManifestRootKey {
                index_id,
                generation,
                partition: TextPartition::Unpartitioned.fingerprint(),
            }),
        );
        assert!(db
            .get(root_key)
            .await
            .expect("empty root point read succeeds")
            .is_none());
    }

    #[tokio::test]
    async fn partition_artifact_ordinal_overflow_blocks_without_a_reservation() {
        let db = test_db("text-partition-artifact-ordinal-limit").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("ordinal overflow".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let (driver, coordinator) = publication_driver("text-partition-artifact-ordinal-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        seed_next_partition_manifest_root(&db, operation_id, &mut claim_sequence, limits, &driver)
            .await;
        let claimed = claim_one(&db, operation_id, &mut claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = claimed.record.progress()
        else {
            panic!("source staging must enter partition scan");
        };
        let overflow_progress = SourceScanProgress {
            inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
            cursor: progress.cursor.clone(),
            counters: OperationCounters {
                entities: progress.counters.entities,
                input_bytes: progress.counters.input_bytes,
                output_operations: u64::from(u32::MAX) + 1,
                output_bytes: progress.counters.output_bytes,
            },
        };
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let prepared = prepare_partition_step(
            &db,
            scope,
            &claimed.record,
            &overflow_progress,
            limits,
            dependency,
        )
        .await
        .expect("overflow is represented as a durable blocker");
        let PreparedTextOperationStep::Repository(prepared) = prepared else {
            panic!("overflow must not reserve a partition upload");
        };
        assert!(matches!(
            prepared.result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                observed,
                limit,
                ..
            }) if observed == u64::from(u32::MAX) + 1 && limit == u64::from(u32::MAX)
        ));
    }

    #[tokio::test]
    async fn await_upload_preserves_revision_then_advances_from_its_exact_artifact() {
        let db = test_db("text-await-upload-artifact-advance").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("advance exact artifact".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let (driver, coordinator) = publication_driver("text-await-upload-artifact-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut operation_claim_sequence = 1;
        assert_eq!(
            drive_one_with(
                &db,
                operation_id,
                &mut operation_claim_sequence,
                limits,
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        seed_next_partition_manifest_root(
            &db,
            operation_id,
            &mut operation_claim_sequence,
            limits,
            &driver,
        )
        .await;
        assert_eq!(
            drive_one_with(
                &db,
                operation_id,
                &mut operation_claim_sequence,
                limits,
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let waiting_operation = read_operation(&db, scope, operation_id).await;
        let waiting_revision = waiting_operation.operation_revision();
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitUpload(wait),
        )) = waiting_operation.progress()
        else {
            panic!("partition split must wait for its child upload");
        };
        let wait = wait.clone();
        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        assert!(matches!(
            observe_operation_pointer(&db, operation_id, writer_epoch, u64::MAX)
                .await
                .expect("operation pointer is observable"),
            OperationPointerObservation::WaitingOnChild
        ));
        assert_eq!(
            read_operation(&db, scope, operation_id)
                .await
                .operation_revision(),
            waiting_revision
        );

        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator;
        let upload_driver = CoordinatorTextUploadDriver::new(dependency);
        assert_eq!(
            drive_upload_one(&db, wait.intent_id(), writer_epoch, 100, &upload_driver).await,
            TextUploadStepResult::PublicationSucceeded
        );
        assert!(matches!(
            observe_operation_pointer(&db, operation_id, writer_epoch, u64::MAX)
                .await
                .expect("uploaded child still owns the operation revision"),
            OperationPointerObservation::WaitingOnChild
        ));
        assert_eq!(
            drive_upload_one(&db, wait.intent_id(), writer_epoch, 101, &upload_driver).await,
            TextUploadStepResult::AttachUploaded
        );
        assert!(matches!(
            observe_operation_pointer(&db, operation_id, writer_epoch, u64::MAX)
                .await
                .expect("reference-committed child still owns the operation revision"),
            OperationPointerObservation::WaitingOnChild
        ));
        assert_eq!(
            drive_upload_one(&db, wait.intent_id(), writer_epoch, 102, &upload_driver).await,
            TextUploadStepResult::ReferenceReleased
        );
        assert!(
            crate::index_v2::repository::load_upload_from_pointer(&db, wait.intent_id())
                .await
                .expect("removed child pointer is readable")
                .is_none()
        );
        assert_eq!(
            read_operation(&db, scope, operation_id)
                .await
                .operation_revision(),
            waiting_revision
        );

        assert_eq!(
            drive_one_with(
                &db,
                operation_id,
                &mut operation_claim_sequence,
                limits,
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let advanced = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = advanced.progress()
        else {
            panic!("exact artifact must resume the partition scan");
        };
        assert_eq!(progress.cursor.as_ref(), Some(wait.completed_cursor()));
        assert_eq!(progress.counters, wait.completed_counters());
        assert!(db
            .get(wait.artifact_key().as_bytes())
            .await
            .expect("artifact point read succeeds")
            .is_some());

        assert_eq!(
            drive_one_with(
                &db,
                operation_id,
                &mut operation_claim_sequence,
                limits,
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(_)
            ))
        ));
    }

    #[tokio::test]
    async fn await_upload_terminal_nonpublication_retries_from_its_retained_source() {
        let db = test_db("text-await-upload-nonpublication-retry").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("retry retained source".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let default_timing = BlobPublicationTiming::default();
        let timing = BlobPublicationTiming::new(
            BlobOperationDuration::from_millis(NonZeroU64::MIN),
            default_timing.publish_timeout(),
            default_timing.safety_margin(),
        );
        let (driver, coordinator) =
            publication_driver_with_timing("text-await-upload-nonpublication-blobs", timing);
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut operation_claim_sequence = 1;
        assert_eq!(
            drive_one_with(
                &db,
                operation_id,
                &mut operation_claim_sequence,
                limits,
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        seed_next_partition_manifest_root(
            &db,
            operation_id,
            &mut operation_claim_sequence,
            limits,
            &driver,
        )
        .await;
        let claimed = claim_one(&db, operation_id, &mut operation_claim_sequence).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = claimed.record.progress()
        else {
            panic!("source staging must enter partition scan");
        };
        let dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let prepared =
            prepare_partition_step(&db, scope, &claimed.record, progress, limits, dependency)
                .await
                .expect("partition preparation succeeds");
        let next = commit_prepared_build_child(&db, scope, &claimed, &prepared).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitUpload(wait),
        )) = next.progress()
        else {
            panic!("committed partition child must enter AwaitUpload");
        };
        let wait = wait.clone();
        let PreparedTextOperationStep::PartitionUpload(upload) = &prepared else {
            panic!("current document must prepare one upload");
        };
        let permit = upload.intent.permit();
        drop(prepared);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        let upload_dependency: Arc<dyn BlobPublicationCoordinator> = coordinator.clone();
        let upload_driver = CoordinatorTextUploadDriver::new(upload_dependency);
        assert_eq!(
            drive_upload_one(&db, wait.intent_id(), writer_epoch, 200, &upload_driver).await,
            TextUploadStepResult::NonPublicationProven
        );
        assert!(matches!(
            observe_operation_pointer(&db, operation_id, writer_epoch, u64::MAX)
                .await
                .expect("nonpublication proof retains child ownership"),
            OperationPointerObservation::WaitingOnChild
        ));
        assert_eq!(
            drive_upload_one(&db, wait.intent_id(), writer_epoch, 201, &upload_driver).await,
            TextUploadStepResult::NonPublicationReleased
        );
        assert!(matches!(
            coordinator.publication_status(&permit).await,
            Err(crate::index_v2::blob_publication::BlobPublicationError::UnknownPermit)
        ));
        assert!(db
            .get(wait.artifact_key().as_bytes())
            .await
            .expect("artifact point read succeeds")
            .is_none());

        assert_eq!(
            drive_one_with(
                &db,
                operation_id,
                &mut operation_claim_sequence,
                limits,
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let retried = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(progress),
        )) = retried.progress()
        else {
            panic!("missing artifact must retry the retained partition source");
        };
        assert_eq!(progress, wait.source());
    }

    #[tokio::test]
    async fn await_upload_partial_child_anchors_fail_before_claim_revision_churn() {
        let db = test_db("text-await-upload-partial-child").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("partial child anchors".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let (driver, _) = publication_driver("text-await-upload-partial-child-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        seed_next_partition_manifest_root(&db, operation_id, &mut claim_sequence, limits, &driver)
            .await;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        let operation = read_operation(&db, scope, operation_id).await;
        let revision = operation.operation_revision();
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitUpload(wait),
        )) = operation.progress()
        else {
            panic!("partition split must wait for its child upload");
        };
        let pointer_key = Key::Global {
            kind: GlobalKeyKind::IndexV2(GlobalIndexV2Key::UploadPointer(wait.intent_id())),
        }
        .to_bytes();
        let pointer_value = db
            .get(&pointer_key)
            .await
            .expect("upload pointer is readable")
            .expect("upload pointer exists");
        db.delete(&pointer_key)
            .await
            .expect("test removes only the pointer anchor");

        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        assert!(matches!(
            observe_operation_pointer(&db, operation_id, writer_epoch, u64::MAX).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert_eq!(
            read_operation(&db, scope, operation_id)
                .await
                .operation_revision(),
            revision
        );

        db.put(pointer_key, pointer_value)
            .await
            .expect("typed pointer anchor is restored");
        let intent_key = scoped_index_key(
            scope,
            IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                intent_id: wait.intent_id(),
            }),
        );
        db.delete(intent_key)
            .await
            .expect("test removes only the intent anchor");
        assert!(matches!(
            observe_operation_pointer(&db, operation_id, writer_epoch, u64::MAX).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert_eq!(
            read_operation(&db, scope, operation_id)
                .await
                .operation_revision(),
            revision
        );
    }

    #[tokio::test]
    async fn await_upload_rejects_claimed_children_and_disagreeing_artifacts() {
        let db = test_db("text-await-upload-corruption-guards").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &properties(
                "Document",
                Some(PropertyValue::String("corruption guards".to_string())),
                None,
            ),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let (driver, _) = publication_driver("text-await-upload-corruption-guard-blobs");
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        seed_next_partition_manifest_root(&db, operation_id, &mut claim_sequence, limits, &driver)
            .await;
        assert_eq!(
            drive_one_with(&db, operation_id, &mut claim_sequence, limits, &driver).await,
            CommittedOperationStep::Progressed
        );
        let operation = read_operation(&db, scope, operation_id).await;
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::AwaitUpload(wait),
        )) = operation.progress()
        else {
            panic!("partition split must wait for its child upload");
        };
        let wait = wait.clone();
        let compaction_artifact_key = |ordinal| {
            IndexCursor::try_new(scoped_index_key(
                scope,
                IndexV2Key::TextBuildArtifact(TextBuildArtifactKey {
                    root: TextManifestRootKey {
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        partition: TextPartition::Unpartitioned.fingerprint(),
                    },
                    ordinal,
                }),
            ))
            .expect("typed compaction artifact key is a valid cursor")
        };
        let compaction_operation = IndexOperationRecord::try_new(
            operation.operation_id(),
            operation.index_id(),
            operation.identity().clone(),
            operation.generation(),
            operation.index_record_revision(),
            operation.operation_revision(),
            operation.kind(),
            operation.family(),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::AwaitCompactionUpload(
                    crate::index_v2::TextCompactionUploadProgress::try_new(
                        PrefixScanProgress {
                            cursor: None,
                            counters: OperationCounters::default(),
                        },
                        vec![compaction_artifact_key(10), compaction_artifact_key(11)],
                        OperationCounters {
                            entities: 1,
                            input_bytes: 1,
                            output_operations: 1,
                            output_bytes: 1,
                        },
                        compaction_artifact_key(12),
                        TextUploadIntentId::from_bytes([0xBC; 16])
                            .expect("compaction intent ID is non-nil"),
                    )
                    .expect("compaction checkpoint is valid"),
                ),
            )),
            operation.attempt(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("queued compaction checkpoint is valid");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("compaction child-gate transaction opens");
        assert!(
            !await_upload_child_is_pending(&transaction, scope, &compaction_operation)
                .await
                .expect("missing compaction child anchors are not pending")
        );
        drop(transaction);

        let intent = crate::index_v2::repository::load_upload_from_pointer(&db, wait.intent_id())
            .await
            .expect("child pointer is readable")
            .expect("child intent exists");
        let work::TextUploadAttachment::BuildArtifact {
            artifact_ordinal,
            split,
        } = intent.attachment
        else {
            panic!("partition child targets one build artifact");
        };
        let artifact_value = work::TextBuildArtifactValue {
            index_id: intent.index_id,
            generation: intent.generation,
            partition: intent.partition.clone(),
            artifact_ordinal,
            split,
            source_intent_id: intent.intent_id,
        };
        let writer_epoch = WriterEpoch::from_bytes([0x7A; 16]).expect("writer epoch is non-nil");
        let claimed_operation = operation
            .claim(OperationClaim {
                writer_epoch,
                sequence: ClaimSequence::new(250).expect("claim sequence is non-zero"),
            })
            .expect("queued AwaitUpload can form a claimed revision");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("child-gate transaction opens");
        assert!(matches!(
            await_upload_child_is_pending(&transaction, scope, &claimed_operation).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert!(matches!(
            reconcile_await_upload(&transaction, scope, &operation, &wait).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);

        let intent_key = scoped_index_key(
            scope,
            IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                intent_id: wait.intent_id(),
            }),
        );
        let pointer_key = Key::Global {
            kind: GlobalKeyKind::IndexV2(GlobalIndexV2Key::UploadPointer(wait.intent_id())),
        }
        .to_bytes();
        let pointer_value = db
            .get(&pointer_key)
            .await
            .expect("child pointer is readable")
            .expect("child pointer exists");

        db.put(
            &intent_key,
            encode_work_value(&IndexV2WorkValue::TextBuildArtifact(artifact_value.clone())),
        )
        .await
        .expect("typed wrong intent value is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("wrong-intent-value transaction opens");
        assert!(matches!(
            await_upload_child_is_pending(&transaction, scope, &operation).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);
        db.put(
            &intent_key,
            encode_work_value(&IndexV2WorkValue::TextUploadIntent(Box::new(
                intent.clone(),
            ))),
        )
        .await
        .expect("exact child intent is restored");

        let mut manifest_child = intent.clone();
        manifest_child.attachment = work::TextUploadAttachment::ManifestSplit(split);
        db.put(
            &intent_key,
            encode_work_value(&IndexV2WorkValue::TextUploadIntent(Box::new(
                manifest_child.clone(),
            ))),
        )
        .await
        .expect("build-owned manifest child is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("manifest-child transaction opens");
        assert!(matches!(
            await_upload_child_is_pending(&transaction, scope, &operation).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);

        manifest_child.owner = work::TextUploadOwner::ActiveMutation {
            writer_epoch,
            mutation_id: crate::index_v2::MutationId::from_bytes([0xAB; 16])
                .expect("active mutation ID is non-nil"),
            active_record_revision: crate::index_v2::IndexRevision::initial(),
        };
        db.put(
            &intent_key,
            encode_work_value(&IndexV2WorkValue::TextUploadIntent(Box::new(
                manifest_child,
            ))),
        )
        .await
        .expect("active-owned manifest child is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("active-child transaction opens");
        assert!(matches!(
            await_upload_child_is_pending(&transaction, scope, &operation).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);
        db.put(
            &intent_key,
            encode_work_value(&IndexV2WorkValue::TextUploadIntent(Box::new(
                intent.clone(),
            ))),
        )
        .await
        .expect("exact child intent is restored");

        db.put(
            &pointer_key,
            crate::encoding::v1::values::index_v2::encode_metadata_value(
                &IndexV2MetadataValue::OperationQueuePointer(
                    crate::index_v2::OperationQueuePointerValue {
                        scope,
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        record_revision: operation.operation_revision(),
                    },
                ),
            ),
        )
        .await
        .expect("typed wrong pointer value is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("wrong-pointer-value transaction opens");
        assert!(matches!(
            await_upload_child_is_pending(&transaction, scope, &operation).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);

        let IndexV2MetadataValue::UploadQueuePointer(mut mismatched_pointer) =
            decode_metadata_value(&pointer_value).expect("exact pointer value decodes")
        else {
            panic!("exact child pointer uses the upload lane");
        };
        mismatched_pointer.record_revision = mismatched_pointer
            .record_revision
            .checked_next()
            .expect("test pointer revision remains bounded");
        db.put(
            &pointer_key,
            crate::encoding::v1::values::index_v2::encode_metadata_value(
                &IndexV2MetadataValue::UploadQueuePointer(mismatched_pointer),
            ),
        )
        .await
        .expect("typed mismatched pointer is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("mismatched-pointer transaction opens");
        assert!(matches!(
            await_upload_child_is_pending(&transaction, scope, &operation).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);
        db.put(&pointer_key, pointer_value)
            .await
            .expect("exact child pointer is restored");

        db.delete(intent_key)
            .await
            .expect("test removes the exact intent anchor");
        db.delete(pointer_key)
            .await
            .expect("test removes the exact pointer anchor");

        db.put(
            wait.artifact_key().as_bytes(),
            encode_work_value(&IndexV2WorkValue::TextUploadIntent(Box::new(
                intent.clone(),
            ))),
        )
        .await
        .expect("typed disagreeing work value is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("wrong-value transaction opens");
        assert!(matches!(
            reconcile_await_upload(&transaction, scope, &operation, &wait).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);

        db.put(
            wait.artifact_key().as_bytes(),
            encode_work_value(&IndexV2WorkValue::TextBuildArtifact(
                work::TextBuildArtifactValue {
                    source_intent_id: TextUploadIntentId::from_bytes([0xEE; 16])
                        .expect("different source intent ID is non-nil"),
                    ..artifact_value
                },
            )),
        )
        .await
        .expect("typed disagreeing artifact is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("wrong-owner transaction opens");
        assert!(matches!(
            reconcile_await_upload(&transaction, scope, &operation, &wait).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }

    /// Proves intent normalization and its outbox cursor survive every worker restart.
    #[tokio::test]
    async fn retire_upload_intents_resumes_each_row_across_database_and_driver_restarts() {
        let name = "text-driver-retire-upload-intents-reopen";
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let db = reopen_test_db(name, Arc::clone(&store)).await;
        bootstrap_writer(&db)
            .await
            .expect("text lifecycle database bootstraps V2 metadata");
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        db.close().await.expect("queued text build database closes");

        let mut claim_sequence = 1;
        let reader_leases: Arc<dyn IndexLeaseCoordinator> = Arc::new(
            crate::index_v2::reader_lease::ProcessLocalIndexLeaseCoordinator::new(
                crate::index_v2::reader_lease::ReaderLeaseTiming::default(),
            ),
        );
        assert_eq!(
            drive_to_terminal_reopening(
                name,
                Arc::clone(&store),
                build_id,
                &mut claim_sequence,
                Arc::clone(&reader_leases),
            )
            .await,
            CommittedOperationStep::Completed
        );

        let db = reopen_test_db(name, Arc::clone(&store)).await;
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            index_id: dropped_index_id,
            generation: dropped_generation,
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("active text index drop is enqueued")
        else {
            panic!("active text index drop creates a new operation");
        };
        assert_eq!(dropped_index_id, index_id);
        assert_eq!(dropped_generation, generation);
        let operation = read_operation(&db, scope, drop_id).await;
        let run_id = BlobGcRunId::from_bytes([0xD0; 16]).expect("run ID is non-nil");
        let blobs = [BlobRef::new([0xD1; 32], 64), BlobRef::new([0xD2; 32], 64)];
        let mut intent_rows = Vec::new();
        let mut candidate_keys = Vec::new();
        for (position, blob) in blobs.into_iter().enumerate() {
            let seed = u8::try_from(position)
                .expect("test position fits u8")
                .checked_add(0xD3)
                .expect("test intent seed remains bounded");
            let intent_id =
                TextUploadIntentId::from_bytes([seed; 16]).expect("intent ID is non-nil");
            let split =
                work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).expect("test split validates");
            let intent = work::TextUploadIntentValue::try_new(
                intent_id,
                TextIntentRevision::initial(),
                index_id,
                definition.identity(),
                generation,
                work::TextPartition::Unpartitioned,
                blob,
                BlobPublicationPermitId::from_bytes([seed.checked_add(2).unwrap(); 16])
                    .expect("permit ID is non-nil"),
                work::TextUploadOwner::Build {
                    operation_id: build_id,
                    expected_operation_revision: IndexOperationRevision::initial(),
                },
                work::TextUploadAttachment::BuildArtifact {
                    artifact_ordinal: u32::try_from(position)
                        .expect("test artifact ordinal fits u32"),
                    split,
                },
                work::TextUploadPhase::Uploaded,
                0,
                work::TextUploadWorkState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .expect("uploaded cleanup intent validates");
            let rows = super::super::upload::upload_anchor_rows(scope, &intent)
                .expect("uploaded cleanup rows encode");
            db.put(&rows.intent_key, &rows.intent_value)
                .await
                .expect("uploaded intent is written");
            db.put(&rows.pointer_key, &rows.pointer_value)
                .await
                .expect("upload pointer is written");
            db.put(&rows.reachability_key, &rows.reachability_value)
                .await
                .expect("upload reachability is written");
            intent_rows.push((intent_id, rows.intent_key));

            let candidate_key = scoped_index_key(
                scope,
                IndexV2Key::BlobGcCandidate(index_v2::BlobGcCandidateKey {
                    index_id,
                    generation,
                    owner: index_v2::BlobGcCandidateKeyOwner::GenerationCleanup,
                    blob_hash: index_v2::BlobHash::new(*blob.hash()),
                }),
            );
            db.put(
                &candidate_key,
                encode_work_value(&IndexV2WorkValue::BlobGcCandidate(
                    work::BlobGcCandidateValue {
                        owner: work::BlobGcCandidateOwner::GenerationCleanup(drop_id),
                        index_id,
                        generation,
                        blob,
                    },
                )),
            )
            .await
            .expect("generation candidate is written");
            candidate_keys.push(candidate_key);
            db.put(
                GlobalIndexV2Key::BlobGcCandidateMember {
                    run_id,
                    blob_hash: index_v2::BlobHash::new(*blob.hash()),
                }
                .to_bytes(),
                encode_work_value(&IndexV2WorkValue::BlobGcEntry(
                    work::BlobGcEntryValue::CandidateMember(work::BlobGcCandidateMemberValue {
                        run_id,
                        blob,
                        state: work::BlobGcMemberState::PendingDisposition { owner_cursor: None },
                    }),
                )),
            )
            .await
            .expect("immutable GC member is written");
        }
        candidate_keys.sort();
        intent_rows.sort_by(|left, right| left.1.cmp(&right.1));

        let root = work::BlobGcRunRootValue::try_new(
            run_id,
            work::BlobGcRunOwner::GenerationCleanup {
                scope,
                operation_id: drop_id,
                index_id,
                generation,
            },
            BlobGcRunRevision::initial(),
            0,
            None,
            work::BlobGcPhase::FencesClosed,
            2,
        )
        .expect("FencesClosed generation root validates");
        db.put(
            GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes(),
            encode_work_value(&IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::RunRoot(root),
            )),
        )
        .await
        .expect("generation root is written");
        let progress = crate::index_v2::GcProgress {
            gc_run_id: Some(run_id),
            candidate_cursor: Some(
                IndexCursor::try_new(
                    candidate_keys
                        .last()
                        .expect("two candidates have a final cursor")
                        .clone(),
                )
                .expect("candidate cursor validates"),
            ),
            stage_cursor: None,
            counters: OperationCounters::default(),
        };
        let seeded = IndexOperationRecord::try_new(
            operation.operation_id(),
            operation.index_id(),
            operation.identity().clone(),
            operation.generation(),
            operation.index_record_revision(),
            operation.operation_revision(),
            operation.kind(),
            operation.family(),
            IndexOperationProgress::TextCleanup(TextCleanupProgress::RetireUploadIntents(progress)),
            operation.attempt(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("seeded cleanup operation validates");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("cleanup checkpoint transaction opens");
        transaction
            .put(
                scoped_index_key(scope, IndexV2Key::operation(drop_id)),
                crate::encoding::v1::values::index_v2::encode_operation_record(&seeded),
            )
            .expect("seeded operation is staged");
        transaction
            .put(
                GlobalIndexV2Key::OperationPointer(drop_id).to_bytes(),
                crate::encoding::v1::values::index_v2::encode_metadata_value(
                    &IndexV2MetadataValue::OperationQueuePointer(
                        crate::index_v2::OperationQueuePointerValue {
                            scope,
                            index_id,
                            generation,
                            record_revision: seeded.operation_revision(),
                        },
                    ),
                ),
            )
            .expect("seeded pointer is staged");
        transaction
            .commit()
            .await
            .expect("seeded cleanup checkpoint commits");
        db.close().await.expect("seeded cleanup database closes");

        let expected_cursors = [
            None,
            Some(intent_rows[0].1.clone()),
            Some(intent_rows[0].1.clone()),
            Some(intent_rows[1].1.clone()),
        ];
        for (turn, expected_cursor) in expected_cursors.into_iter().enumerate() {
            let db = reopen_test_db(name, Arc::clone(&store)).await;
            let driver =
                reopened_lifecycle_driver(name, Arc::clone(&store), Arc::clone(&reader_leases));
            assert_eq!(
                drive_one_with(
                    &db,
                    drop_id,
                    &mut claim_sequence,
                    SearchIndexBackfillLimits::default().batch(),
                    &driver,
                )
                .await,
                CommittedOperationStep::Progressed
            );
            let persisted = read_operation(&db, scope, drop_id).await;
            let IndexOperationProgress::TextCleanup(TextCleanupProgress::RetireUploadIntents(
                persisted,
            )) = persisted.progress()
            else {
                panic!("normalization turn remains in RetireUploadIntents");
            };
            assert_eq!(
                persisted.stage_cursor.as_ref().map(IndexCursor::as_bytes),
                expected_cursor.as_ref(),
                "turn {turn} persists the exact restart cursor"
            );
            db.close()
                .await
                .expect("normalization checkpoint database closes");
        }

        let db = reopen_test_db(name, Arc::clone(&store)).await;
        for (intent_id, intent_key) in &intent_rows {
            let value = db
                .get(intent_key)
                .await
                .expect("assigned intent is readable")
                .expect("assigned intent remains durable");
            assert!(matches!(
                decode_work_value(&value).expect("assigned intent decodes"),
                IndexV2WorkValue::TextUploadIntent(intent)
                    if intent.intent_id == *intent_id
                        && matches!(
                            intent.phase,
                            work::TextUploadPhase::Reclaimable(
                                work::ReclaimAssignment::Assigned(assigned)
                            ) if assigned == run_id
                        )
            ));
        }
        let driver = reopened_lifecycle_driver(name, Arc::clone(&store), reader_leases);
        assert_eq!(
            drive_one_with(
                &db,
                drop_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let persisted = read_operation(&db, scope, drop_id).await;
        assert!(matches!(
            persisted.progress(),
            IndexOperationProgress::TextCleanup(TextCleanupProgress::MarkReachability(next))
                if next.gc_run_id == Some(run_id) && next.stage_cursor.is_none()
        ));
        let root_value = db
            .get(GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes())
            .await
            .expect("first-pass root is readable")
            .expect("first-pass root remains durable");
        assert!(matches!(
            decode_work_value(&root_value).expect("first-pass root decodes"),
            IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::RunRoot(root))
                if matches!(
                    root.phase,
                    work::BlobGcPhase::FirstPass {
                        first_attempt,
                        reference_cursor: None,
                        ..
                    } if first_attempt.get() == 1
                )
        ));
        db.close()
            .await
            .expect("first-pass checkpoint database closes");
    }

    #[tokio::test]
    async fn empty_build_and_drop_resume_across_every_database_reopen() {
        let name = "text-driver-empty-build-drop-reopen";
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let db = reopen_test_db(name, Arc::clone(&store)).await;
        bootstrap_writer(&db)
            .await
            .expect("text lifecycle database bootstraps V2 metadata");
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        db.close().await.expect("queued text build database closes");

        let mut claim_sequence = 1;
        let reader_leases: Arc<dyn IndexLeaseCoordinator> = Arc::new(
            crate::index_v2::reader_lease::ProcessLocalIndexLeaseCoordinator::new(
                crate::index_v2::reader_lease::ReaderLeaseTiming::default(),
            ),
        );
        assert_eq!(
            drive_to_terminal_reopening(
                name,
                Arc::clone(&store),
                build_id,
                &mut claim_sequence,
                Arc::clone(&reader_leases),
            )
            .await,
            CommittedOperationStep::Completed
        );

        let db = reopen_test_db(name, Arc::clone(&store)).await;
        assert!(matches!(
            read_index_record(&db, scope, &definition).await.state(),
            IndexStateV2::Active { .. }
        ));
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            index_id: dropped_index_id,
            generation: dropped_generation,
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("active text index drop is enqueued")
        else {
            panic!("active text index drop creates a new operation");
        };
        assert_eq!(dropped_index_id, index_id);
        assert_eq!(dropped_generation, generation);
        db.close().await.expect("queued text drop database closes");

        assert_eq!(
            drive_to_terminal_reopening(
                name,
                Arc::clone(&store),
                drop_id,
                &mut claim_sequence,
                reader_leases,
            )
            .await,
            CommittedOperationStep::Completed
        );
        let db = reopen_test_db(name, store).await;
        assert!(matches!(
            read_index_record(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        db.close()
            .await
            .expect("completed text lifecycle database closes");
    }

    #[tokio::test]
    async fn abort_removes_hidden_text_rows_across_every_database_reopen() {
        let name = "text-driver-abort-hidden-rows-reopen";
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(IndexElementKind::Node, None);
        let db = reopen_test_db(name, Arc::clone(&store)).await;
        bootstrap_writer(&db)
            .await
            .expect("text abort database bootstraps V2 metadata");
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            0,
            &properties(
                "Document",
                Some(PropertyValue::String("hidden row".to_string())),
                None,
            ),
        )
        .await;
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let reader_leases: Arc<dyn IndexLeaseCoordinator> = Arc::new(
            crate::index_v2::reader_lease::ProcessLocalIndexLeaseCoordinator::new(
                crate::index_v2::reader_lease::ReaderLeaseTiming::default(),
            ),
        );
        let driver =
            reopened_lifecycle_driver(name, Arc::clone(&store), Arc::clone(&reader_leases));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one_with(
                &db,
                build_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
                &driver,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let state_prefix = Key::data_prefix(
            scope,
            IndexV2Key::generation_prefix(IndexV2RecordKind::TextEntityState, index_id, generation),
        );
        let mut state_rows = db
            .scan_prefix(&state_prefix, ..)
            .await
            .expect("hidden text state is readable");
        assert!(state_rows.next().await.unwrap().is_some());

        assert!(matches!(
            drop_index_operation(&db, scope, &definition)
                .await
                .expect("building text index converts to abort cleanup"),
            IndexDdlReceipt::ExistingOperation { operation_id } if operation_id == build_id
        ));
        db.close().await.expect("aborting text database closes");

        assert_eq!(
            drive_to_terminal_reopening(
                name,
                Arc::clone(&store),
                build_id,
                &mut claim_sequence,
                reader_leases,
            )
            .await,
            CommittedOperationStep::Completed
        );
        let db = reopen_test_db(name, store).await;
        assert!(matches!(
            read_index_record(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        let mut state_rows = db
            .scan_prefix(state_prefix, ..)
            .await
            .expect("cleaned text state prefix is readable");
        assert!(state_rows.next().await.unwrap().is_none());
        db.close()
            .await
            .expect("completed text abort database closes");
    }
}
