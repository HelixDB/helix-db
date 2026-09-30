//! Bounded V2 text-index build driver.
//!
//! Text construction starts with a durable two-pass source boundary. The
//! `ScanSource` pass reads authoritative graph property rows and stages
//! typed, generation-qualified
//! [`TextEntityStateValue`](crate::index_lifecycle::work::TextEntityStateValue)
//! records with their statistics. Those
//! keys sort by partition fingerprint before entity identity, so the later
//! `ScanPartitions` pass can build bounded multi-document splits even when
//! tenant values are arbitrarily interleaved in graph-ID order.
//!
//! Graph writes during a build only queue operations, which publish after
//! activation. `ScanPartitions` rereads each entity's graph row, so it aligns
//! the entity's state and statistics with the document it actually builds
//! (see `PreparedPartitionReconciliation`); an entity that was deleted,
//! moved, or un-indexed in between is retired rather than left live.
//!
//! The driver owns no database handle. Source staging borrows the repository
//! transaction supplied by the outbox dispatcher. Partition construction uses
//! a short-lived read snapshot, drops it before CPU-heavy split construction,
//! and retains only immutable bytes plus the exact database observations needed
//! to attach the uploaded split transactionally.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, StreamExt};
use slatedb::object_store::{ObjectStore, ObjectStoreExt};
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::config::{
    ActiveTextMutationLimits, IndexLifecycleScanTuning, SearchIndexBatchLimits,
    TextBackfillCompactionLimits,
};
use crate::encoding::property;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, KeyPrefix};
use crate::encoding::v2::keys::{
    IndexEntity, IndexEntityStateKey, PartitionFingerprint, RecordKind, ScopedKey,
    TextBuildArtifactKey, TextEntityStateKey, TextManifestRootKey,
};
use crate::encoding::v2::values::{
    decode_index_record, decode_manifest_root, decode_text_entity_state, encode_applied_state,
    encode_build_artifact, encode_manifest_root, encode_text_entity_state,
};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::outbox::{
    IndexOperationDriver, IndexOperationStepExecution, IndexOperationStepPermit,
    IndexOperationStepResult, PreparedIndexOperationStep, StepResourceUsage,
};
use crate::index_lifecycle::work::{
    self, AppliedEntityStateValue, AppliedFamilyState, TextEntityStateValue, TextPartition,
};
use crate::index_lifecycle::{
    BuildOperationOutcome, IndexCursor, IndexElementKind, IndexEntityId, IndexOperationBlocker,
    IndexOperationExecutionState, IndexOperationFamily, IndexOperationOutcome,
    IndexOperationProgress, IndexOperationRecord, IndexRecordV2, OperationCounters,
    PrefixScanProgress, SourceScanProgress, TextBuildProgress, TextBuildStage, TextLogicalVersion,
    TextManifestValidationProgress, ValidatedDynamicIndexDefinition, ValidatedTextIndexDefinition,
};

/// Inseparable storage services required by split-producing lifecycle work.
struct TextStorageRuntime {
    object_store: Arc<dyn ObjectStore>,
    db_path: String,
    compaction_limits: TextBackfillCompactionLimits,
}

/// Fixed upper bound for immutable manifest metadata requests.
const MANIFEST_VALIDATION_HEAD_CONCURRENCY: usize = 8;

/// Complete classification of one immutable manifest blob metadata proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManifestBlobMetadataProof {
    Valid,
    InvariantViolation,
    TransientFailure,
}

/// Family driver for durable text build checkpoints.
///
/// The outbox repository owns transaction creation and commits. This driver
/// stages only family-specific rows and returns the next closed progress ADT.
pub(crate) struct TextIndexDriver {
    scope_gates: Arc<crate::index_lifecycle::IndexScopeGates>,
    storage: Option<TextStorageRuntime>,
    /// Limits of the queued publication that later replaces each built
    /// document; source scans admit every document against them.
    document_limits: ActiveTextMutationLimits,
    scan_tuning: IndexLifecycleScanTuning,
}

impl core::fmt::Debug for TextIndexDriver {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TextIndexDriver")
            .field("storage_installed", &self.storage.is_some())
            .finish()
    }
}

impl Default for TextIndexDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl TextIndexDriver {
    /// Constructs a source-only driver for repository-only unit tests.
    ///
    /// It scans sources against the default policy's document limits, and
    /// every split-producing stage fails transiently without storage.
    pub(crate) fn new() -> Self {
        Self {
            scope_gates: Arc::new(crate::index_lifecycle::IndexScopeGates::default()),
            storage: None,
            document_limits: crate::config::SearchIndexBackfillLimits::default()
                .active_text_mutation(),
            scan_tuning: IndexLifecycleScanTuning::default(),
        }
    }

    /// Constructs a complete text driver sharing the production mutation gate.
    ///
    /// `limits` is the policy the queue publisher also runs under, so builds
    /// admit documents against the same per-document allowances as writes.
    pub(crate) fn with_storage(
        scope_gates: Arc<crate::index_lifecycle::IndexScopeGates>,
        object_store: Arc<dyn ObjectStore>,
        db_path: impl Into<String>,
        limits: crate::config::SearchIndexBackfillLimits,
    ) -> Self {
        Self {
            scope_gates,
            storage: Some(TextStorageRuntime {
                object_store,
                db_path: db_path.into(),
                compaction_limits: limits.text_compaction(),
            }),
            document_limits: limits.active_text_mutation(),
            scan_tuning: IndexLifecycleScanTuning::default(),
        }
    }

    /// Applies runtime source-scan prefetching without admitting blocks to cache.
    pub(crate) const fn with_scan_tuning(mut self, scan_tuning: IndexLifecycleScanTuning) -> Self {
        self.scan_tuning = scan_tuning;
        self
    }
}

#[async_trait]
impl crate::index_lifecycle::worker::ActiveTextCompactionDriver for TextIndexDriver {
    async fn compact_active_text_once(&self, db: &Db) -> Result<bool> {
        let Some(runtime) = &self.storage else {
            return Ok(false);
        };
        super::active_compaction::compact_once(
            db,
            &self.scope_gates,
            &runtime.object_store,
            &runtime.db_path,
            runtime.compaction_limits,
        )
        .await
    }
}

/// Closed text preparation consumed by exactly one repository dispatch.
pub(crate) enum PreparedTextOperationStep {
    /// A pre-read selected a repository-only transition or blocker.
    Repository(Box<PreparedTextRepositoryStep>),
    /// One directly uploaded partition split and its transactional attachment.
    PartitionUpload(Box<PreparedTextBuildUpload>),
    /// One directly uploaded compaction replacement and its atomic retirement.
    CompactionUpload(Box<PreparedTextBuildUpload>),
    /// One all-stale compaction whose exact inputs can retire without a child.
    CompactionRetirement(Box<PreparedTextCompactionRetirement>),
    /// One range-validated manifest exhaustion or blocker transition.
    ManifestRepository(Box<PreparedTextManifestRepositoryStep>),
    /// One artifact-to-manifest-page relocation.
    ManifestPage(Box<PreparedTextManifestPage>),
    /// One range-fenced pre-activation validation checkpoint.
    Validation(Box<PreparedTextValidationStep>),
}

/// Closed validation preparation with exactly the authority its lane requires.
pub(crate) enum PreparedTextValidationStep {
    /// Root, exhaustion, or invariant-blocker validation.
    Database {
        source_operation: IndexOperationRecord,
        prepared: super::validation::PreparedDatabaseValidation,
    },
    /// Page validation after object metadata was checked.
    Page {
        source_operation: IndexOperationRecord,
        prepared: super::validation::PreparedPageValidation,
    },
}

/// Repository-only text result prepared without an external reservation.
pub(crate) struct PreparedTextRepositoryStep {
    source_operation: IndexOperationRecord,
    expected_reads: Vec<PreparedTextExpectedRead>,
    writes: Vec<PreparedTextWrite>,
    result: IndexOperationStepResult,
}

/// Exact directly uploaded build output retained across its atomic attachment.
pub(crate) struct PreparedTextBuildUpload {
    source_operation: IndexOperationRecord,
    progress: IndexOperationProgress,
    artifact_key: Bytes,
    artifact_value: Bytes,
    expected_reads: Vec<PreparedTextExpectedRead>,
    lifecycle_writes: Vec<PreparedTextWrite>,
    retired_artifact_keys: Vec<IndexCursor>,
    uploaded_bytes: u64,
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

/// Exact manifest page retained across repository dispatch.
pub(crate) struct PreparedTextManifestPage {
    source_operation: IndexOperationRecord,
    prepared: super::manifest::PreparedManifestPage,
    progress: IndexOperationProgress,
}

/// Exact row observation that prevents a prepared catch-up split from going stale.
#[derive(Clone)]
struct PreparedTextExpectedRead {
    key: Bytes,
    value: Option<Bytes>,
}

/// Typed operation-owned write staged only with the matching uploaded split.
#[derive(Clone)]
struct PreparedTextWrite {
    key: Bytes,
    /// `None` deletes the row.
    value: Option<Bytes>,
}

impl PreparedTextWrite {
    /// Stages this put or delete.
    fn stage(&self, transaction: &DbTransaction) -> Result<()> {
        match &self.value {
            Some(value) => transaction.put(&self.key, value)?,
            None => transaction.delete(&self.key)?,
        }
        Ok(())
    }
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
            self.write.map(|(key, value)| PreparedTextWrite {
                key,
                value: Some(value),
            }),
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
        ScopedKey::TextManifestRoot(TextManifestRootKey {
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
            write: Some((key, encode_manifest_root(&empty))),
        });
    };
    let root = decode_manifest_root(observed_value)?;
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
    /// Returns disposable measurements retained by this closed preparation.
    pub(crate) fn resource_usage(&self) -> StepResourceUsage {
        match self {
            Self::PartitionUpload(prepared) => StepResourceUsage {
                text_artifact_bytes: prepared.uploaded_bytes,
                text_upload_bytes: prepared.uploaded_bytes,
                ..StepResourceUsage::default()
            },
            Self::CompactionUpload(prepared) => {
                let fan_in =
                    u64::try_from(prepared.retired_artifact_keys.len()).unwrap_or(u64::MAX);
                StepResourceUsage {
                    text_artifact_bytes: prepared.uploaded_bytes,
                    text_upload_bytes: prepared.uploaded_bytes,
                    compaction_fan_in: fan_in,
                    compaction_input_bytes: 0,
                    temporary_bytes: prepared.uploaded_bytes,
                    ..StepResourceUsage::default()
                }
            }
            Self::CompactionRetirement(prepared) => {
                let next = prepared
                    .source_operation
                    .progressed(prepared.progress.clone())
                    .ok();
                let input_bytes = next.as_ref().map_or(0, |next| {
                    operation_input_delta(&prepared.source_operation, next)
                });
                StepResourceUsage {
                    compaction_fan_in: u64::try_from(prepared.input_artifact_keys.len())
                        .unwrap_or(u64::MAX),
                    compaction_input_bytes: input_bytes,
                    temporary_bytes: input_bytes,
                    ..StepResourceUsage::default()
                }
            }
            Self::ManifestPage(prepared) => StepResourceUsage {
                manifest_page_bytes: prepared.prepared.manifest_page_bytes(),
                manifest_root_bytes: prepared.prepared.manifest_root_bytes(),
                ..StepResourceUsage::default()
            },
            Self::Repository(_) | Self::ManifestRepository(_) | Self::Validation(_) => {
                StepResourceUsage::default()
            }
        }
    }

    /// Stages only the transition already authorized by this preparation.
    pub(crate) async fn stage(
        &self,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Result<IndexOperationStepResult> {
        let late_boundary_counters = match operation.progress() {
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Compact(progress) | TextBuildStage::PrepareManifests(progress),
            )) => Some(progress.counters),
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ValidateManifests(progress),
            )) => Some(progress.counters()),
            IndexOperationProgress::TextBuild(
                TextBuildProgress::Constructing(
                    TextBuildStage::ScanSource(_)
                    | TextBuildStage::ScanPartitions(_)
                    | TextBuildStage::CatchUp(_)
                    | TextBuildStage::Activate(_),
                )
                | TextBuildProgress::Aborting(_),
            )
            | IndexOperationProgress::SecondaryBuild(_)
            | IndexOperationProgress::VectorBuild(_)
            | IndexOperationProgress::SecondaryCleanup(_)
            | IndexOperationProgress::VectorCleanup(_)
            | IndexOperationProgress::TextCleanup(_) => None,
        };
        if late_boundary_counters.is_some()
            && has_pre_queue_deltas(transaction, scope, operation).await?
        {
            return Ok(IndexOperationStepResult::Blocked(
                IndexOperationBlocker::InvariantViolation,
            ));
        }
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
                    write.stage(transaction)?;
                }
                Ok(prepared.result.clone())
            }
            Self::PartitionUpload(prepared) | Self::CompactionUpload(prepared) => {
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
                for write in &prepared.lifecycle_writes {
                    write.stage(transaction)?;
                }
                transaction.put(&prepared.artifact_key, &prepared.artifact_value)?;
                if !prepared.retired_artifact_keys.is_empty() {
                    super::compaction::stage_input_retirement(
                        transaction,
                        scope,
                        operation,
                        &prepared.retired_artifact_keys,
                    )
                    .await?;
                }
                Ok(IndexOperationStepResult::Progressed(
                    prepared.progress.clone(),
                ))
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

    /// Direct uploads are immutable; discarded preparation may leave an orphan.
    pub(crate) async fn discard(self) -> Result<()> {
        Ok(())
    }

    /// Direct object I/O completed before the transaction was staged.
    pub(crate) async fn after_commit(self) {}
}

fn operation_input_delta(before: &IndexOperationRecord, after: &IndexOperationRecord) -> u64 {
    let before = crate::index_lifecycle::IndexOperationStatus::from_record(before)
        .common()
        .progress
        .input_bytes;
    let after = crate::index_lifecycle::IndexOperationStatus::from_record(after)
        .common()
        .progress
        .input_bytes;
    after.saturating_sub(before)
}

#[async_trait]
impl IndexOperationDriver for TextIndexDriver {
    fn family(&self) -> IndexOperationFamily {
        IndexOperationFamily::Text
    }

    async fn acquire_generation_ownership(
        &self,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Box<dyn IndexOperationStepPermit> {
        Box::new(
            self.scope_gates
                .publication_permit(crate::index_lifecycle::queue::QueueTarget::new(
                    scope,
                    operation.index_id(),
                    operation.generation(),
                ))
                .await,
        )
    }

    async fn acquire_step_permit(
        &self,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Result<Box<dyn IndexOperationStepPermit>> {
        let needs_exclusive = matches!(
            operation.progress(),
            IndexOperationProgress::TextBuild(
                TextBuildProgress::Constructing(TextBuildStage::Activate(_))
                    | TextBuildProgress::Aborting(_)
            ) | IndexOperationProgress::TextCleanup(_)
        );
        if needs_exclusive {
            return Ok(Box::new(self.scope_gates.lifecycle_permit(scope).await));
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
                let Some(runtime) = &self.storage else {
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
                let step = prepare_partition_step_with_scan_tuning(
                    db,
                    scope,
                    operation,
                    progress,
                    limits,
                    self.document_limits,
                    self.scan_tuning,
                    runtime,
                )
                .await?;
                Ok(PreparedIndexOperationStep::text(step))
            }
            TextBuildStage::Compact(progress) => {
                let Some(runtime) = &self.storage else {
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
                let Some(runtime) = &self.storage else {
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
                let Some(runtime) = &self.storage else {
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
            | TextBuildStage::CatchUp(_)
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
    ) -> Result<IndexOperationStepExecution> {
        let record = load_operation_index(transaction, scope, operation).await?;
        let ValidatedDynamicIndexDefinition::Text(_) = record.definition() else {
            return Err(corruption("text operation loaded another family"));
        };
        let result = match operation.progress() {
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ScanSource(progress),
            )) => {
                scan_source(
                    transaction,
                    scope,
                    operation,
                    &record,
                    progress,
                    limits,
                    self.document_limits,
                    self.scan_tuning,
                )
                .await
            }
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(progress),
            )) => {
                // Only builds started before operations were queued persist
                // this stage; queued builds go from ScanPartitions to Compact.
                if has_pre_queue_deltas(transaction, scope, operation).await? {
                    Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::InvariantViolation,
                    ))
                } else {
                    Ok(progressed_build(TextBuildStage::Compact(
                        PrefixScanProgress {
                            cursor: None,
                            counters: progress.counters,
                        },
                    )))
                }
            }
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
        }?;
        Ok(IndexOperationStepExecution::new(result))
    }
}

/// One admitted partition run materialized from a short-lived read snapshot.
struct PartitionDocuments {
    empty_root: PreparedEmptyManifestRoot,
    reconciliation: PreparedPartitionReconciliation,
    partition: TextPartition,
    documents: Vec<crate::search::text::TextDocumentInput>,
    completed_cursor: IndexCursor,
    completed_counters: OperationCounters,
}

/// Closed partition-scan decision with all writes needed by its transition.
enum PartitionScanSelection {
    /// A durable blocker; nothing is staged.
    Blocked(IndexOperationBlocker),
    /// Progress without an upload: root creation, a run without documents, or exhaustion.
    Repository {
        empty_root: Option<PreparedEmptyManifestRoot>,
        reconciliation: PreparedPartitionReconciliation,
        result: IndexOperationStepResult,
    },
    Upload(PartitionDocuments),
}

/// Rows that align one run's scanned entities with the documents it builds.
///
/// `ScanSource` accounted every entity from an earlier graph read, and graph
/// writes during a build only queue operations. After activation, queued
/// publication diffs each entity against its statistics marker, so the
/// marker must describe exactly the document the build holds. Partition
/// construction therefore replaces the marker with the built document's
/// contribution when the text changed, and retires the entity (dead state, no
/// marker) when it was deleted, moved to another partition, or stopped being
/// indexed, as Active retirement does. Text applied state keeps the value
/// `ScanSource` wrote: only that scan's pre-existing-state check reads it. The
/// retained observations fence every row through the step's commit.
#[derive(Default)]
struct PreparedPartitionReconciliation {
    expected_reads: Vec<PreparedTextExpectedRead>,
    writes: Vec<PreparedTextWrite>,
}

/// Complete split input whose reads/writes remain bound to one operation claim.
struct PreparedTextSplitInput {
    partition: TextPartition,
    documents: Vec<crate::search::text::TextDocumentInput>,
    completed_counters: OperationCounters,
    /// Partition scan checkpoint this split resumes after.
    progress: SourceScanProgress,
    completed_cursor: IndexCursor,
    expected_reads: Vec<PreparedTextExpectedRead>,
    lifecycle_writes: Vec<PreparedTextWrite>,
}

/// Prepares one partition-ordered step without retaining a database snapshot.
///
/// `limits` bounds the build transaction; `document_limits` are the queue
/// publisher's, which bound each entity's reconciliation reads.
#[allow(
    clippy::too_many_arguments,
    reason = "a partition step binds one operation, cursor, both budgets, tuning, and storage runtime"
)]
async fn prepare_partition_step_with_scan_tuning(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
    document_limits: ActiveTextMutationLimits,
    scan_tuning: IndexLifecycleScanTuning,
    runtime: &TextStorageRuntime,
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
    let prepared = scan_partition_documents(
        &snapshot,
        scope,
        operation,
        definition,
        progress,
        limits,
        document_limits,
        scan_tuning,
    )
    .await?;
    drop(snapshot);

    let documents = match prepared {
        PartitionScanSelection::Blocked(blocker) => {
            return Ok(PreparedTextOperationStep::Repository(Box::new(
                PreparedTextRepositoryStep {
                    source_operation: operation.clone(),
                    expected_reads: Vec::new(),
                    writes: Vec::new(),
                    result: IndexOperationStepResult::Blocked(blocker),
                },
            )));
        }
        PartitionScanSelection::Repository {
            empty_root,
            reconciliation,
            result,
        } => {
            let (root_read, root_write) =
                match empty_root.map(PreparedEmptyManifestRoot::into_parts) {
                    Some((read, write)) => (Some(read), write),
                    None => (None, None),
                };
            return Ok(PreparedTextOperationStep::Repository(Box::new(
                PreparedTextRepositoryStep {
                    source_operation: operation.clone(),
                    expected_reads: root_read
                        .into_iter()
                        .chain(reconciliation.expected_reads)
                        .collect(),
                    writes: root_write
                        .into_iter()
                        .chain(reconciliation.writes)
                        .collect(),
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
        runtime,
        PreparedTextSplitInput {
            partition: documents.partition,
            documents: documents.documents,
            completed_counters: documents.completed_counters,
            progress: progress.clone(),
            completed_cursor: documents.completed_cursor,
            expected_reads: std::iter::once(root_read)
                .chain(documents.reconciliation.expected_reads)
                .collect(),
            lifecycle_writes: root_write
                .into_iter()
                .chain(documents.reconciliation.writes)
                .collect(),
        },
    )
    .await
}

/// Constructs and uploads one exact split without retaining a database view.
async fn prepare_build_upload(
    operation: &IndexOperationRecord,
    scope: DataScope,
    definition: &ValidatedTextIndexDefinition,
    limits: SearchIndexBatchLimits,
    runtime: &TextStorageRuntime,
    input: PreparedTextSplitInput,
) -> Result<PreparedTextOperationStep> {
    let IndexOperationExecutionState::Claimed(_) = operation.execution_state() else {
        return Err(corruption(
            "text split preparation requires an exact claimed operation",
        ));
    };
    let runtime_definition = definition.to_runtime();
    let documents = input.documents;
    let unpublished = tokio::task::spawn_blocking(move || {
        crate::search::text::build_documents_as_split(&runtime_definition, &documents)
    })
    .await
    .map_err(|error| corruption(format!("text split construction task failed: {error}")))??
    .ok_or_else(|| corruption("non-empty text build batch produced no split"))?;
    let (payload, runtime_split, pruning) = unpublished.into_parts();
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
        pruning,
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
    // The partition scan admitted this row within the batch output limits.
    let (artifact_key, artifact_value) =
        build_artifact_row(scope, operation, &input.partition, artifact_ordinal, split);
    let artifact_bytes =
        u64::try_from(artifact_key.len().saturating_add(artifact_value.len())).unwrap_or(u64::MAX);
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
    let next_stage = TextBuildStage::ScanPartitions(SourceScanProgress {
        inclusive_upper_bound: input.progress.inclusive_upper_bound.clone(),
        cursor: Some(input.completed_cursor.clone()),
        counters: completed_counters,
    });
    let next_progress =
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(next_stage));
    let uploaded =
        crate::search::text::upload_blob(&runtime.object_store, &runtime.db_path, &payload).await?;
    if uploaded.sha256 != *split.blob().hash() || uploaded.size_bytes != split.blob().size() {
        return Err(corruption(
            "text split upload returned metadata for different content",
        ));
    }
    let prepared = Box::new(PreparedTextBuildUpload {
        source_operation: operation.clone(),
        progress: next_progress,
        artifact_key,
        artifact_value,
        expected_reads: input.expected_reads,
        lifecycle_writes: input.lifecycle_writes,
        retired_artifact_keys: Vec::new(),
        uploaded_bytes: payload_bytes,
    });
    Ok(PreparedTextOperationStep::PartitionUpload(prepared))
}

/// Encodes the build-artifact row that attaches `split` to `partition`.
fn build_artifact_row(
    scope: DataScope,
    operation: &IndexOperationRecord,
    partition: &TextPartition,
    ordinal: u32,
    split: work::SplitRef,
) -> (Bytes, Bytes) {
    let key = scoped_index_key(
        scope,
        ScopedKey::TextBuildArtifact(TextBuildArtifactKey {
            root: TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            },
            ordinal,
        }),
    );
    let value = encode_build_artifact(&work::TextBuildArtifactValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: partition.clone(),
        artifact_ordinal: ordinal,
        split,
    });
    (key, value)
}

/// Encoded bytes of the artifact row an upload step writes for `partition`.
///
/// Every field but the partition is fixed-width, including the Bloom pruning
/// every build split carries, so a placeholder split measures the real row
/// exactly before the split is built.
fn build_artifact_row_bytes(
    scope: DataScope,
    operation: &IndexOperationRecord,
    partition: &TextPartition,
) -> Result<u64> {
    let split = work::SplitRef::try_new(
        work::BlobRef::new([0; 32], 1),
        0,
        0,
        0,
        1,
        work::SplitPruning::from_terms(std::iter::empty::<&[u8]>()),
    )
    .map_err(work_error)?;
    let (key, value) = build_artifact_row(scope, operation, partition, 0, split);
    Ok(u64::try_from(key.len().saturating_add(value.len())).unwrap_or(u64::MAX))
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
    runtime: &TextStorageRuntime,
) -> Result<PreparedTextOperationStep> {
    let IndexOperationExecutionState::Claimed(_) = operation.execution_state() else {
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
        selected.pruning,
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

    let (payload, runtime_split, pruning) = unpublished.into_parts();
    let split = work::SplitRef::try_new(
        work::BlobRef::new(runtime_split.blob.sha256, runtime_split.blob.size_bytes),
        runtime_split.footer_offset,
        runtime_split.footer_len,
        runtime_split.hotcache_len,
        runtime_split.total_size_bytes,
        pruning,
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
    let (artifact_key, artifact_value) = build_artifact_row(
        scope,
        operation,
        &selected.partition,
        artifact_ordinal,
        split,
    );
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
    let next_stage = TextBuildStage::Compact(PrefixScanProgress {
        cursor: progress.cursor.clone(),
        counters: completed_counters,
    });
    let next_progress =
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(next_stage));
    let uploaded =
        crate::search::text::upload_blob(&runtime.object_store, &runtime.db_path, &payload).await?;
    if uploaded.sha256 != *split.blob().hash() || uploaded.size_bytes != split.blob().size() {
        return Err(corruption(
            "text compaction upload returned metadata for different content",
        ));
    }
    expected_reads.shrink_to_fit();
    Ok(PreparedTextOperationStep::CompactionUpload(Box::new(
        PreparedTextBuildUpload {
            source_operation: operation.clone(),
            progress: next_progress,
            artifact_key,
            artifact_value,
            expected_reads,
            lifecycle_writes: Vec::new(),
            retired_artifact_keys: selected.artifact_keys,
            uploaded_bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
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
/// Database selection completes before the page is transactionally attached.
async fn prepare_manifest_step(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    batch_limits: SearchIndexBatchLimits,
    runtime: &TextStorageRuntime,
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
        },
    )))
}

/// Prepares one bounded page/root validation checkpoint.
///
/// Database selection finishes under a short snapshot. Page work then checks
/// exact object metadata before returning a closed serializable checkpoint.
async fn prepare_validation_step(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &TextManifestValidationProgress,
    limits: SearchIndexBatchLimits,
    runtime: &TextStorageRuntime,
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

    let mut proofs = stream::iter(prepared.blobs().iter().copied())
        .map(|blob| async move {
            let location =
                crate::search::text::blob_object_store_path(&runtime.db_path, *blob.hash());
            match runtime.object_store.head(&location).await {
                Ok(metadata) if metadata.size == blob.size() => ManifestBlobMetadataProof::Valid,
                Ok(_) | Err(slatedb::object_store::Error::NotFound { .. }) => {
                    ManifestBlobMetadataProof::InvariantViolation
                }
                Err(_) => ManifestBlobMetadataProof::TransientFailure,
            }
        })
        .buffer_unordered(MANIFEST_VALIDATION_HEAD_CONCURRENCY);
    let mut aggregate = ManifestBlobMetadataProof::Valid;
    while let Some(proof) = proofs.next().await {
        aggregate = match (aggregate, proof) {
            (ManifestBlobMetadataProof::InvariantViolation, _)
            | (_, ManifestBlobMetadataProof::InvariantViolation) => {
                ManifestBlobMetadataProof::InvariantViolation
            }
            (ManifestBlobMetadataProof::TransientFailure, _)
            | (_, ManifestBlobMetadataProof::TransientFailure) => {
                ManifestBlobMetadataProof::TransientFailure
            }
            (ManifestBlobMetadataProof::Valid, ManifestBlobMetadataProof::Valid) => {
                ManifestBlobMetadataProof::Valid
            }
        };
    }
    drop(proofs);
    let external_result = match aggregate {
        ManifestBlobMetadataProof::Valid => None,
        ManifestBlobMetadataProof::InvariantViolation => Some(IndexOperationStepResult::Blocked(
            IndexOperationBlocker::InvariantViolation,
        )),
        ManifestBlobMetadataProof::TransientFailure => {
            Some(IndexOperationStepResult::TransientFailure)
        }
    };
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
        },
    )))
}

/// Detects build deltas written before text operations were queued.
///
/// Queued builds never write `BuildDelta` rows: writes during a build enqueue
/// complete operations that the publisher applies after activation. Rows left
/// by an in-flight pre-queue build are not replayed, so that build blocks
/// until it is aborted and the index is created again.
pub(super) async fn has_pre_queue_deltas(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Result<bool> {
    let found = generation_has_rows(transaction, scope, RecordKind::BuildDelta, operation).await?;
    if found {
        tracing::error!(
            operation_id = %operation.operation_id().as_uuid(),
            "text build holds pre-queue build deltas; abort it and create the index again"
        );
    }
    Ok(found)
}

/// Rechecks late work in the same transaction that canonically activates text.
async fn activate(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    counters: OperationCounters,
) -> Result<IndexOperationStepResult> {
    if has_pre_queue_deltas(transaction, scope, operation).await? {
        return Ok(IndexOperationStepResult::Blocked(
            IndexOperationBlocker::InvariantViolation,
        ));
    }
    if generation_has_rows(transaction, scope, RecordKind::TextBuildArtifact, operation).await? {
        return Ok(progressed_build(TextBuildStage::PrepareManifests(
            PrefixScanProgress {
                cursor: None,
                counters,
            },
        )));
    }
    Ok(IndexOperationStepResult::Completed(
        IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
    ))
}

/// Reads one bounded contiguous kind-`0x0C` partition run and its graph rows.
///
/// Every admitted partition carries its exact empty-root observation. Upload
/// selections require that root, while repository-only progress may create the
/// canonical unpartitioned root even when the authoritative source is empty.
/// Each live entity is reconciled with its current graph row; see
/// [`PreparedPartitionReconciliation`].
///
/// The batch input limit bounds source rows: the root, entity states, and
/// graph rows. Reconciliation replaces an entity's accounted document with its
/// current one, as a queued publication does, so its reads (markers, corpus,
/// and term rows) are bounded by the publication input
/// allowance of `document_limits`. Output counts every reconciliation write
/// plus the artifact row an upload commits, against `batch`.
///
/// A step's first entity therefore blocks only when its source rows alone
/// exceed the batch input, as they would in a fresh build of the same row.
/// Its reconciliation always fits: `ScanSource` admits its accounted document
/// and the queue producer its current one, each within half of every
/// publication allowance after a manifest page is reserved (see
/// [`super::active_batch::TextDocumentFootprint`]). The page, root, and
/// pointer rows a footprint reserves but reconciliation never writes cover the
/// retired entity-state and artifact rows. The other first-entity blockers are reachable
/// only when limits are lowered after admission.
#[allow(
    clippy::too_many_arguments,
    reason = "a partition scan binds one snapshot, operation, definition, cursor, both budgets, and tuning"
)]
async fn scan_partition_documents(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedTextIndexDefinition,
    progress: &SourceScanProgress,
    batch: SearchIndexBatchLimits,
    document_limits: ActiveTextMutationLimits,
    scan_tuning: IndexLifecycleScanTuning,
) -> Result<PartitionScanSelection> {
    let expected_upper =
        initial_partition_scan(operation, scope, progress.counters)?.inclusive_upper_bound;
    if progress.inclusive_upper_bound != expected_upper {
        return Err(corruption(
            "text partition scan does not retain its exact maximal generation key",
        ));
    }
    let prefix = IndexKey::data_prefix(
        scope,
        ScopedKey::generation_prefix(
            RecordKind::TextEntityState,
            operation.index_id(),
            operation.generation(),
        ),
    );
    let start = cursor_suffix(&prefix, progress.cursor.as_ref())?;
    let upper = cursor_suffix(&prefix, Some(&progress.inclusive_upper_bound))?
        .ok_or_else(|| corruption("text partition upper bound is absent"))?;
    let start = start.map_or(Bound::Unbounded, Bound::Excluded);
    let scan_options = scan_tuning.scan_options();
    let mut rows = transaction
        .scan_prefix_with_options(&prefix, (start, Bound::Included(upper)), &scan_options)
        .await?;
    let mut partition = None::<TextPartition>;
    let mut documents = Vec::new();
    let mut completed_cursor = progress.cursor.clone();
    let mut batch_entities = 0_usize;
    // Source rows: the manifest root, entity states, and graph rows.
    let mut batch_input_bytes = 0_u64;
    let mut batch_reconciliation_input_bytes = 0_u64;
    let mut batch_output_operations = 0_u64;
    let mut batch_output_bytes = 0_u64;
    // The artifact row an upload commits, reserved with the first document.
    let mut artifact_reservation = None::<u64>;
    let mut statistics = super::statistics::PreparedTextStatisticsBatch::default();
    let mut retirements = Vec::new();
    let mut empty_root = None::<PreparedEmptyManifestRoot>;
    let mut exhausted = true;

    while batch_entities < batch.max_entities().get() {
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
                if root_input_bytes > batch.max_input_bytes().get() {
                    return Ok(PartitionScanSelection::Blocked(
                        IndexOperationBlocker::ManifestLimit {
                            partition: row_partition,
                            observed: root_input_bytes,
                            limit: batch.max_input_bytes().get(),
                        },
                    ));
                }
                let root_output_operations = root.output_operations();
                let root_output_bytes = root.output_bytes();
                if root_output_bytes > batch.max_output_bytes().get() {
                    return Ok(PartitionScanSelection::Blocked(
                        IndexOperationBlocker::ManifestLimit {
                            partition: row_partition,
                            observed: root_output_bytes,
                            limit: batch.max_output_bytes().get(),
                        },
                    ));
                }
                if root.requires_creation() {
                    let seed_input_bytes = root_input_bytes.saturating_add(
                        u64::try_from(row.key.len().saturating_add(row.value.len()))
                            .unwrap_or(u64::MAX),
                    );
                    if seed_input_bytes > batch.max_input_bytes().get() {
                        return Ok(PartitionScanSelection::Blocked(
                            IndexOperationBlocker::ManifestLimit {
                                partition: row_partition,
                                observed: seed_input_bytes,
                                limit: batch.max_input_bytes().get(),
                            },
                        ));
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
                        reconciliation: PreparedPartitionReconciliation::default(),
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
        // A live state's statistics marker is read concurrently with its graph row.
        let (graph_value, marker) = futures::try_join!(
            async { Ok::<_, HelixDbError>(transaction.get(&graph_key).await?) },
            async {
                if !state.live {
                    return Ok(None);
                }
                super::statistics::read_marker(
                    transaction,
                    Some(&statistics),
                    scope,
                    operation.index_id(),
                    operation.generation(),
                    key.entity,
                )
                .await
            },
        )?;
        let source_bytes = u64::try_from(
            row.key
                .len()
                .saturating_add(row.value.len())
                .saturating_add(graph_key.len())
                .saturating_add(graph_value.as_ref().map_or(0, Bytes::len)),
        )
        .unwrap_or(u64::MAX);
        let admitted_input_bytes = batch_input_bytes.saturating_add(source_bytes);
        if admitted_input_bytes > batch.max_input_bytes().get() {
            if batch_entities == 0 {
                let blocker = if source_bytes > batch.max_input_bytes().get() {
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: key.entity.kind,
                        entity_id: key.entity.id,
                        observed: source_bytes,
                        limit: batch.max_input_bytes().get(),
                    }
                } else {
                    IndexOperationBlocker::ManifestLimit {
                        partition: row_partition,
                        observed: admitted_input_bytes,
                        limit: batch.max_input_bytes().get(),
                    }
                };
                return Ok(PartitionScanSelection::Blocked(blocker));
            }
            exhausted = false;
            break;
        }

        let invalid_source = IndexOperationBlocker::InvalidSourceData {
            entity_kind: key.entity.kind,
            entity_id: key.entity.id,
        };
        let document = match graph_value {
            Some(value) if state.live => {
                let Ok(properties) = property::decode_properties(&value) else {
                    return Ok(PartitionScanSelection::Blocked(invalid_source));
                };
                let Ok(document) = text_document(definition, &properties, &state) else {
                    return Ok(PartitionScanSelection::Blocked(invalid_source));
                };
                document
            }
            Some(_) | None => None,
        };
        // Dead states come only from builds that predate queued operations.
        let reconciliation = if state.live {
            Some(
                reconcile_scanned_entity(
                    transaction,
                    &statistics,
                    scope,
                    definition,
                    (&row.key, &row.value),
                    &state,
                    marker,
                    document.as_ref(),
                )
                .await?,
            )
        } else {
            None
        };
        let (entity_input_bytes, entity_output_operations, entity_output_bytes) =
            reconciliation.as_ref().map_or((0, 0, 0), |reconciliation| {
                (
                    reconciliation.input_bytes,
                    reconciliation.output_operations,
                    reconciliation.output_bytes,
                )
            });
        let artifact = match (artifact_reservation, &document) {
            (Some(bytes), _) => Some(bytes),
            (None, Some(_)) => Some(build_artifact_row_bytes(scope, operation, &row_partition)?),
            (None, None) => None,
        };
        let (artifact_operations, artifact_bytes) = artifact.map_or((0, 0), |bytes| (1, bytes));
        let reconciliation_input_bytes =
            batch_reconciliation_input_bytes.saturating_add(entity_input_bytes);
        let output_operations = batch_output_operations.saturating_add(entity_output_operations);
        let output_bytes = batch_output_bytes.saturating_add(entity_output_bytes);
        let over_limit = [
            (
                reconciliation_input_bytes,
                entity_input_bytes,
                document_limits.max_input_bytes().get(),
            ),
            (
                output_operations.saturating_add(artifact_operations),
                entity_output_operations,
                batch.max_output_operations().get(),
            ),
            (
                output_bytes.saturating_add(artifact_bytes),
                entity_output_bytes,
                batch.max_output_bytes().get(),
            ),
        ]
        .into_iter()
        .find(|(admitted, _, limit)| admitted > limit);
        match over_limit {
            Some((admitted, observed, limit)) if batch_entities == 0 => {
                let blocker = if observed > limit {
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: key.entity.kind,
                        entity_id: key.entity.id,
                        observed,
                        limit,
                    }
                } else {
                    IndexOperationBlocker::ManifestLimit {
                        partition: row_partition,
                        observed: admitted,
                        limit,
                    }
                };
                return Ok(PartitionScanSelection::Blocked(blocker));
            }
            Some(_) => {
                exhausted = false;
                break;
            }
            None => {}
        }

        documents.extend(document);
        match reconciliation.map(|reconciliation| reconciliation.change) {
            None | Some(EntityReconciliation::Unchanged) => {}
            Some(EntityReconciliation::Retexted(transition)) => statistics.push(transition)?,
            Some(EntityReconciliation::Retired(transition, retirement)) => {
                statistics.push(transition)?;
                retirements.push(retirement);
            }
        }
        artifact_reservation = artifact;
        batch_entities = batch_entities
            .checked_add(1)
            .ok_or_else(|| corruption("text partition batch entity count overflowed"))?;
        batch_input_bytes = admitted_input_bytes;
        batch_reconciliation_input_bytes = reconciliation_input_bytes;
        batch_output_operations = output_operations;
        batch_output_bytes = output_bytes;
        completed_cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
    }
    if batch_entities == batch.max_entities().get() {
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
        if root_input_bytes > batch.max_input_bytes().get() {
            return Ok(PartitionScanSelection::Blocked(
                IndexOperationBlocker::ManifestLimit {
                    partition: TextPartition::Unpartitioned,
                    observed: root_input_bytes,
                    limit: batch.max_input_bytes().get(),
                },
            ));
        }
        let root_output_operations = root.output_operations();
        let root_output_bytes = root.output_bytes();
        if root_output_bytes > batch.max_output_bytes().get() {
            return Ok(PartitionScanSelection::Blocked(
                IndexOperationBlocker::ManifestLimit {
                    partition: TextPartition::Unpartitioned,
                    observed: root_output_bytes,
                    limit: batch.max_output_bytes().get(),
                },
            ));
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
                reconciliation: PreparedPartitionReconciliation::default(),
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
            checked_add(
                batch_input_bytes,
                batch_reconciliation_input_bytes,
                "partition batch input bytes",
            )?,
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
    let (statistics_reads, statistics_writes): (Vec<_>, Vec<_>) = statistics
        .into_rows()
        .map(|row| {
            let write = (row.replacement != row.observed).then(|| PreparedTextWrite {
                key: row.key.clone(),
                value: row.replacement,
            });
            (
                PreparedTextExpectedRead {
                    key: row.key,
                    value: row.observed,
                },
                write,
            )
        })
        .unzip();
    let (retirement_reads, retirement_writes): (Vec<_>, Vec<_>) = retirements.into_iter().unzip();
    let reconciliation = PreparedPartitionReconciliation {
        expected_reads: statistics_reads
            .into_iter()
            .chain(retirement_reads)
            .collect(),
        writes: statistics_writes
            .into_iter()
            .flatten()
            .chain(retirement_writes)
            .collect(),
    };
    let Some(completed_cursor) = completed_cursor else {
        return Ok(PartitionScanSelection::Repository {
            empty_root,
            reconciliation,
            result: progressed_build(TextBuildStage::Compact(PrefixScanProgress {
                cursor: None,
                counters: completed_counters,
            })),
        });
    };
    if documents.is_empty() {
        let next = if exhausted {
            TextBuildStage::Compact(PrefixScanProgress {
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
            reconciliation,
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
        reconciliation,
        partition,
        documents,
        completed_cursor,
        completed_counters,
    }))
}

/// How one live scanned entity's accounting changes to match its document.
enum EntityReconciliation {
    /// The accounted contribution already describes the document.
    Unchanged,
    /// The text changed; the transition replaces the accounted contribution.
    Retexted(super::statistics::PreparedTextStatisticsTransition),
    /// No document is built. The transition removes the contribution, and the
    /// row marks the entity state dead.
    Retired(
        super::statistics::PreparedTextStatisticsTransition,
        (PreparedTextExpectedRead, PreparedTextWrite),
    ),
}

/// One live scanned entity's reconciliation and the rows it reads and writes.
struct ScannedEntityReconciliation {
    change: EntityReconciliation,
    /// Reads beyond the entity-state and graph rows.
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
}

/// Aligns one live scanned entity's accounting with `document`.
///
/// `marker` is the entity's observed statistics marker. `ScanSource` stages
/// every live state with its partition's present contribution, and nothing
/// else writes a Building generation's markers, so any other marker is
/// corruption.
#[allow(
    clippy::too_many_arguments,
    reason = "reconciliation binds one exact snapshot, batch, entity row, marker, and document"
)]
async fn reconcile_scanned_entity(
    transaction: &DbTransaction,
    statistics: &super::statistics::PreparedTextStatisticsBatch,
    scope: DataScope,
    definition: &ValidatedTextIndexDefinition,
    (state_key, state_value): (&Bytes, &Bytes),
    state: &TextEntityStateValue,
    marker: Option<(Bytes, work::TextStatisticsEntityValue)>,
    document: Option<&crate::search::text::TextDocumentInput>,
) -> Result<ScannedEntityReconciliation> {
    let entity = IndexEntity {
        kind: state.entity_kind,
        id: state.entity_id,
    };
    let Some((marker_value, marker)) = marker else {
        return Err(corruption(
            "live text build entity has no statistics marker",
        ));
    };
    if !matches!(
        &marker.contribution,
        work::TextStatisticsContribution::Present { partition, .. } if partition == &state.partition
    ) {
        return Err(corruption(
            "live text build entity disagrees with its statistics marker",
        ));
    }
    // A present marker never equals the absent contribution, so an unchanged
    // entity always builds its document.
    let current = document
        .map(|document| {
            super::statistics::present_contribution(
                definition.analyzer(),
                state.partition.clone(),
                &document.text,
            )
        })
        .transpose()?
        .unwrap_or(work::TextStatisticsContribution::Absent);
    let change = if marker.contribution == current {
        EntityReconciliation::Unchanged
    } else {
        let transition = super::statistics::prepare_mutation_in_batch(
            transaction,
            statistics,
            super::statistics::TextStatisticsMutation::new(
                scope,
                state.index_id,
                state.generation,
                entity,
                marker.contribution,
                current,
            ),
        )
        .await?;
        match document {
            Some(_) => EntityReconciliation::Retexted(transition),
            None => EntityReconciliation::Retired(
                transition,
                (
                    PreparedTextExpectedRead {
                        key: state_key.clone(),
                        value: Some(state_value.clone()),
                    },
                    PreparedTextWrite {
                        key: state_key.clone(),
                        value: Some(encode_text_entity_state(&TextEntityStateValue {
                            live: false,
                            ..state.clone()
                        })),
                    },
                ),
            ),
        }
    };
    let row_bytes = |key: &Bytes, value: Option<&Bytes>| {
        u64::try_from(key.len().saturating_add(value.map_or(0, Bytes::len))).unwrap_or(u64::MAX)
    };
    let observed = |rows: &[super::statistics::PreparedStatisticsRow]| {
        rows.iter().fold(0_u64, |bytes, row| {
            bytes.saturating_add(row_bytes(&row.key, row.observed.as_ref()))
        })
    };
    // A transition's rows include the marker's observation, and the caller
    // already measured the live entity-state row.
    let (input_bytes, statistics_rows, retirement) = match &change {
        EntityReconciliation::Unchanged => {
            let marker_key = scoped_index_key(
                scope,
                ScopedKey::TextStatisticsEntity(
                    crate::encoding::v2::keys::TextStatisticsEntityKey {
                        index_id: state.index_id,
                        generation: state.generation,
                        entity,
                    },
                ),
            );
            (row_bytes(&marker_key, Some(&marker_value)), &[][..], None)
        }
        EntityReconciliation::Retexted(transition) => {
            (observed(transition.rows()), transition.rows(), None)
        }
        EntityReconciliation::Retired(transition, (_, retirement)) => (
            observed(transition.rows()),
            transition.rows(),
            Some(retirement),
        ),
    };
    let (output_operations, output_bytes) = statistics_rows
        .iter()
        .filter(|row| row.replacement != row.observed)
        .map(|row| row_bytes(&row.key, row.replacement.as_ref()))
        .chain(retirement.map(|write| row_bytes(&write.key, write.value.as_ref())))
        .fold((0_u64, 0_u64), |(operations, bytes), written| {
            (operations.saturating_add(1), bytes.saturating_add(written))
        });
    Ok(ScannedEntityReconciliation {
        change,
        input_bytes,
        output_operations,
        output_bytes,
    })
}

/// Decodes and cross-checks one generation-qualified text entity-state row.
fn decode_entity_state(
    scope: DataScope,
    key: &[u8],
    value: &[u8],
    operation: &IndexOperationRecord,
) -> Result<(TextEntityStateKey, TextEntityStateValue)> {
    let IndexKey::Data {
        kind: ScopedKey::TextEntityState(key),
        ..
    } = IndexKey::parse_from_slice(scope, key)?
    else {
        return Err(corruption(
            "text entity-state prefix yielded another key kind",
        ));
    };
    let state = decode_text_entity_state(value)?;
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
) -> std::result::Result<
    Option<crate::search::text::TextDocumentInput>,
    super::projection::TextSourceProjectionError,
> {
    let (current_partition, text) = match super::projection::project(definition, properties)? {
        super::projection::TextSourceProjection::NotIndexed => return Ok(None),
        super::projection::TextSourceProjection::Indexed { partition, text } => (partition, text),
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
            crate::encoding::v2::keys::NodePropertyKey::new(entity.id.get()),
        ),
        IndexElementKind::Edge => DataKeyKind::EdgePropertyById(
            crate::encoding::v2::keys::EdgePropertyByIdKey::new(entity.id.get()),
        ),
    };
    DataKey::Data { scope, kind }.to_bytes()
}

/// Stages one bounded authoritative graph scan as partition-qualified state.
///
/// Writes are accumulated in memory and staged only after every admitted row
/// validates. A blocking source row therefore cannot commit earlier rows while
/// leaving the durable cursor behind them. The enclosing outbox transaction
/// commits these writes and the returned checkpoint atomically.
///
/// `limits` bounds this build transaction. `document_limits` are the queue
/// publisher's: each indexed document must fit the share of a publication
/// [`super::active_batch::admit_document`] grants, exactly as writes must, or
/// it blocks the build as an oversized entity until its source row is
/// repaired. The two are independent parameters, so one entity can fit its
/// publication share yet still exceed this transaction.
#[allow(
    clippy::too_many_arguments,
    reason = "a source scan binds one snapshot, operation, record, cursor, both budgets, and tuning"
)]
async fn scan_source(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
    document_limits: ActiveTextMutationLimits,
    scan_tuning: IndexLifecycleScanTuning,
) -> Result<IndexOperationStepResult> {
    let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
        return Err(corruption("text source scan loaded another family"));
    };
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
    let scan_options = scan_tuning.scan_options();
    let mut rows = transaction
        .scan_prefix_with_options(
            &source_prefix,
            (start, Bound::Included(upper)),
            &scan_options,
        )
        .await?;
    let mut batch_entities = 0_usize;
    let mut batch_input_bytes = 0_u64;
    let mut batch_output_operations = 0_u64;
    let mut batch_output_bytes = 0_u64;
    let mut cursor = progress.cursor.clone();
    let mut writes = Vec::new();
    let mut statistics_batch = super::statistics::PreparedTextStatisticsBatch::default();
    let mut exhausted = true;

    'scan_rows: while batch_entities < limits.max_entities().get() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let graph_input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        if batch_input_bytes.saturating_add(graph_input_bytes) > limits.max_input_bytes().get() {
            if batch_entities == 0 {
                let entity_id = source_entity(scope, definition.element_kind(), &row.key)?
                    .unwrap_or(IndexEntityId::initial());
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id,
                        observed: graph_input_bytes,
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
            let (partition, text) = match super::projection::project(definition, &properties) {
                Ok(super::projection::TextSourceProjection::NotIndexed) => break 'stage_entity,
                Ok(super::projection::TextSourceProjection::Indexed { partition, text }) => {
                    (partition, text)
                }
                Err(_) => {
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::InvalidSourceData {
                            entity_kind: definition.element_kind(),
                            entity_id,
                        },
                    ));
                }
            };
            let entity = IndexEntity {
                kind: definition.element_kind(),
                id: entity_id,
            };
            // Queued publication later replaces this document with any
            // admitted document of the entity in one epoch, so it must fit
            // the same per-document share queue producers enforce.
            let analyzed = match super::active_batch::admit_document(
                scope,
                record,
                definition,
                entity,
                &partition,
                &text,
                document_limits,
            ) {
                Ok(analyzed) => analyzed,
                Err(HelixDbError::ActiveTextMutationLimitExceeded {
                    resource,
                    observed,
                    limit,
                }) => {
                    // The durable blocker has no resource field; name it here.
                    tracing::warn!(
                        operation_id = %operation.operation_id().as_uuid(),
                        entity_id = entity_id.get(),
                        %resource,
                        observed,
                        limit,
                        "text build blocked on a document over its per-document allowance"
                    );
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::OversizedEntity {
                            entity_kind: definition.element_kind(),
                            entity_id,
                            observed,
                            limit,
                        },
                    ));
                }
                Err(error) => return Err(error),
            };
            let contribution = super::statistics::present_contribution_from_analysis(
                definition.analyzer(),
                partition.clone(),
                &analyzed,
            )?;
            let Some(statistics) = super::statistics::prepare_source_scan_in_batch(
                transaction,
                &statistics_batch,
                scope,
                operation.index_id(),
                operation.generation(),
                entity,
                contribution,
            )
            .await?
            else {
                break 'stage_entity;
            };
            let statistics_input_bytes = statistics.rows().iter().fold(0_u64, |bytes, row| {
                bytes
                    .saturating_add(u64::try_from(row.key.len()).unwrap_or(u64::MAX))
                    .saturating_add(
                        row.observed
                            .as_ref()
                            .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                    )
            });
            let entity_input_bytes = graph_input_bytes.saturating_add(statistics_input_bytes);
            if batch_input_bytes.saturating_add(entity_input_bytes) > limits.max_input_bytes().get()
            {
                if batch_entities == 0 {
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::OversizedEntity {
                            entity_kind: definition.element_kind(),
                            entity_id,
                            observed: entity_input_bytes,
                            limit: limits.max_input_bytes().get(),
                        },
                    ));
                }
                exhausted = false;
                break 'scan_rows;
            }
            let key = scoped_index_key(
                scope,
                ScopedKey::TextEntityState(TextEntityStateKey {
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
                ScopedKey::AppliedState(IndexEntityStateKey {
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
            let value = encode_text_entity_state(&TextEntityStateValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.clone(),
                entity_kind: definition.element_kind(),
                entity_id,
                logical_version: TextLogicalVersion::initial(),
                live: true,
            });
            let applied_value = encode_applied_state(&AppliedEntityStateValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                entity_kind: definition.element_kind(),
                entity_id,
                state: AppliedFamilyState::Text(Some((partition, TextLogicalVersion::initial()))),
            });
            let state_output_bytes = u64::try_from(
                key.len()
                    .saturating_add(value.len())
                    .saturating_add(applied_key.len())
                    .saturating_add(applied_value.len()),
            )
            .unwrap_or(u64::MAX);
            let statistics_output_operations = statistics
                .rows()
                .iter()
                .filter(|row| row.replacement != row.observed)
                .count();
            let statistics_output_bytes = statistics.rows().iter().fold(0_u64, |bytes, row| {
                if row.replacement == row.observed {
                    return bytes;
                }
                bytes
                    .saturating_add(u64::try_from(row.key.len()).unwrap_or(u64::MAX))
                    .saturating_add(
                        row.replacement
                            .as_ref()
                            .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                    )
            });
            let entity_output_operations = 2_u64
                .saturating_add(u64::try_from(statistics_output_operations).unwrap_or(u64::MAX));
            let entity_output_bytes = state_output_bytes.saturating_add(statistics_output_bytes);
            let output_operations =
                batch_output_operations.saturating_add(entity_output_operations);
            if output_operations > limits.max_output_operations().get()
                || batch_output_bytes.saturating_add(entity_output_bytes)
                    > limits.max_output_bytes().get()
            {
                if batch_entities == 0 {
                    let (observed, limit) =
                        if output_operations > limits.max_output_operations().get() {
                            (output_operations, limits.max_output_operations().get())
                        } else {
                            (entity_output_bytes, limits.max_output_bytes().get())
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
            staged = Some((
                key,
                value,
                applied_key,
                applied_value,
                statistics,
                entity_input_bytes,
                entity_output_operations,
                entity_output_bytes,
            ));
        }

        batch_entities = batch_entities
            .checked_add(1)
            .ok_or_else(|| corruption("text batch entity count overflowed"))?;
        let Some((
            key,
            value,
            applied_key,
            applied_value,
            statistics,
            entity_input_bytes,
            entity_output_operations,
            entity_output_bytes,
        )) = staged
        else {
            batch_input_bytes =
                checked_add(batch_input_bytes, graph_input_bytes, "batch input bytes")?;
            cursor = Some(complete_cursor);
            continue;
        };
        batch_input_bytes =
            checked_add(batch_input_bytes, entity_input_bytes, "batch input bytes")?;
        batch_output_operations = checked_add(
            batch_output_operations,
            entity_output_operations,
            "batch output operations",
        )?;
        batch_output_bytes = checked_add(
            batch_output_bytes,
            entity_output_bytes,
            "batch output bytes",
        )?;
        writes.push((key, value));
        writes.push((applied_key, applied_value));
        statistics_batch.push(statistics)?;
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
    statistics_batch.validate(transaction).await?;
    statistics_batch.stage_validated(transaction)?;
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
        ScopedKey::TextEntityState(TextEntityStateKey {
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
    let key = scoped_index_key(scope, ScopedKey::index_record(operation.identity().clone()));
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
    kind: RecordKind,
    operation: &IndexOperationRecord,
) -> Result<bool> {
    let prefix = IndexKey::data_prefix(
        scope,
        ScopedKey::generation_prefix(kind, operation.index_id(), operation.generation()),
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
    DataKey::data_prefix(scope, Bytes::copy_from_slice(prefix.as_slice()))
}

/// Parses one source row and rejects a keyspace/entity-kind mismatch.
fn source_entity(
    scope: DataScope,
    expected: IndexElementKind,
    key: &[u8],
) -> Result<Option<IndexEntityId>> {
    let parsed = DataKey::parse_from_slice(scope, key)?;
    Ok(match (expected, parsed) {
        (
            IndexElementKind::Node,
            DataKey::Data {
                kind: DataKeyKind::NodeProperty(key),
                ..
            },
        ) => Some(IndexEntityId::new(key.node_id())),
        (
            IndexElementKind::Edge,
            DataKey::Data {
                kind: DataKeyKind::EdgePropertyById(key),
                ..
            },
        ) => Some(IndexEntityId::new(key.edge_id())),
        (IndexElementKind::Edge, DataKey::Data { .. }) => None,
        (IndexElementKind::Node, DataKey::Data { .. }) | (_, DataKey::Global { .. }) => {
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

/// Encodes one scoped V2 key through the canonical `encoding/v2` boundary.
fn scoped_index_key(scope: DataScope, key: ScopedKey) -> Bytes {
    IndexKey::Data { scope, kind: key }.to_bytes()
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

fn operation_error(error: crate::index_lifecycle::IndexOperationModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

fn work_error(error: crate::index_lifecycle::work::IndexWorkModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

#[cfg(test)]
mod tests {

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::index_lifecycle::worker::ActiveTextCompactionDriver;
    use crate::index_lifecycle::{
        IndexGenerationId, IndexId, IndexOperationId, IndexOperationKind, IndexOperationRevision,
        IndexRevision,
    };

    fn operation() -> IndexOperationRecord {
        let runtime = crate::config::TextIndexDefinition::new_node("Document", "body")
            .expect("text test definition validates");
        let definition = ValidatedTextIndexDefinition::try_from_runtime(&runtime)
            .expect("text test definition has a V2 representation");
        IndexOperationRecord::try_new(
            IndexOperationId::new_v4(),
            IndexId::initial(),
            definition.identity(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                }),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("text test operation is internally consistent")
    }

    #[tokio::test]
    async fn driver_and_manifest_root_boundaries_preserve_exact_observations() {
        let source_only =
            TextIndexDriver::default().with_scan_tuning(IndexLifecycleScanTuning::default());
        assert!(format!("{source_only:?}").contains("storage_installed: false"));

        let db = Db::open("text-driver-root-contracts", Arc::new(InMemory::new()))
            .await
            .expect("text driver test database opens");
        assert!(!source_only.compact_active_text_once(&db).await.unwrap());
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("text driver test transaction begins");
        let operation = operation();
        let scope = DataScope::LegacyUnscoped;

        let missing = prepare_empty_manifest_root(
            &transaction,
            scope,
            &operation,
            TextPartition::Unpartitioned,
        )
        .await
        .unwrap();
        assert!(missing.requires_creation());
        assert_eq!(missing.output_operations(), 1);
        assert!(missing.input_bytes() > 0);
        assert!(missing.output_bytes() > 0);
        let (
            _,
            Some(PreparedTextWrite {
                key,
                value: Some(value),
            }),
        ) = missing.into_parts()
        else {
            panic!("missing root produces one typed put")
        };
        transaction.put(key, value).unwrap();

        let existing = prepare_empty_manifest_root(
            &transaction,
            scope,
            &operation,
            TextPartition::Unpartitioned,
        )
        .await
        .unwrap();
        assert!(!existing.requires_creation());
        assert_eq!(existing.output_operations(), 0);
        assert_eq!(existing.output_bytes(), 0);
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/index_lifecycle_text_driver_prepared.rs"]
mod prepared_contracts;
