//! Bounded V2 text-index build driver.
//!
//! Text construction starts with a durable two-pass source boundary. The
//! `ScanSource` pass reads authoritative graph property rows and stages only
//! typed, generation-qualified
//! [`TextEntityStateValue`](crate::index_lifecycle::work::TextEntityStateValue)
//! records. Those
//! keys sort by partition fingerprint before entity identity, so the later
//! `ScanPartitions` pass can build bounded multi-document splits even when
//! tenant values are arbitrarily interleaved in graph-ID order.
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
use slatedb::object_store::{ObjectStore, ObjectStoreExt};
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::config::{
    IndexLifecycleScanTuning, SearchIndexBatchLimits, TextBackfillCompactionLimits,
};
use crate::encoding::property;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, Key, KeyPrefix};
use crate::encoding::v2::keys::Key as IndexKey;
use crate::encoding::v2::keys::{
    IndexEntity, IndexEntityStateKey, PartitionFingerprint, RecordKind, ScopedKey,
    TextBuildArtifactKey, TextEntityStateKey, TextManifestRootKey,
};
use crate::encoding::v2::values::{
    decode_applied_state, decode_build_delta, decode_index_record, decode_manifest_root,
    decode_text_entity_state, encode_applied_state, encode_build_artifact, encode_manifest_root,
    encode_text_entity_state,
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
    TextManifestRevision, TextManifestValidationProgress, ValidatedDynamicIndexDefinition,
    ValidatedTextIndexDefinition,
};

/// Inseparable storage services required by split-producing lifecycle work.
struct TextStorageRuntime {
    object_store: Arc<dyn ObjectStore>,
    db_path: String,
    compaction_limits: TextBackfillCompactionLimits,
}

/// Family driver for durable text build checkpoints.
///
/// The outbox repository owns transaction creation and commits. This driver
/// stages only family-specific rows and returns the next closed progress ADT.
pub(crate) struct TextIndexDriver {
    scope_gates: Arc<crate::index_lifecycle::IndexScopeGates>,
    storage: Option<TextStorageRuntime>,
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
    pub(crate) fn new() -> Self {
        Self {
            scope_gates: Arc::new(crate::index_lifecycle::IndexScopeGates::default()),
            storage: None,
            scan_tuning: IndexLifecycleScanTuning::default(),
        }
    }

    /// Constructs a complete text driver sharing the production mutation gate.
    pub(crate) fn with_storage(
        scope_gates: Arc<crate::index_lifecycle::IndexScopeGates>,
        object_store: Arc<dyn ObjectStore>,
        db_path: impl Into<String>,
        compaction_limits: TextBackfillCompactionLimits,
    ) -> Self {
        Self {
            scope_gates,
            storage: Some(TextStorageRuntime {
                object_store,
                db_path: db_path.into(),
                compaction_limits,
            }),
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
    /// One directly uploaded catch-up split and its transactional attachment.
    CatchUpUpload(Box<PreparedTextBuildUpload>),
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

/// Exact observed root used while applying one late authoritative delta.
///
/// Unlike [`PreparedEmptyManifestRoot`], this representation admits an
/// already-populated BUILD root. The retained root is also the sole allocator
/// for entity logical versions after manifest construction has begun.
#[derive(Clone)]
struct PreparedCatchUpManifestRoot {
    observation: PreparedTextExpectedRead,
    root: work::TextManifestRootValue,
    write: Option<(Bytes, Bytes)>,
}

impl PreparedCatchUpManifestRoot {
    /// Returns bytes read while proving the exact root state.
    fn input_bytes(&self) -> u64 {
        u64::try_from(
            self.observation
                .key
                .len()
                .saturating_add(self.observation.value.as_ref().map_or(0, Bytes::len)),
        )
        .unwrap_or(u64::MAX)
    }

    /// Returns the logical version reserved by the next root revision.
    fn next_logical_version(&self) -> Option<TextLogicalVersion> {
        let revision = self.root.revision().checked_next().ok()?;
        TextLogicalVersion::new(revision.get()).ok()
    }

    /// Advances a root for one entity transition and returns that revision.
    fn advance_for_entity_transition(&mut self) -> Result<Option<TextLogicalVersion>> {
        let Some(logical_version) = self.next_logical_version() else {
            return Ok(None);
        };
        self.root = work::TextManifestRootValue::try_new(
            self.root.index_id(),
            self.root.generation(),
            self.root.partition().clone(),
            TextManifestRevision::new(logical_version.get())
                .map_err(|_| corruption("text catch-up logical version is not a revision"))?,
            self.root.page_count(),
            self.root.split_count(),
        )
        .map_err(|error| {
            corruption(format!("text catch-up root transition is invalid: {error}"))
        })?;
        self.write = Some((
            self.observation.key.clone(),
            encode_manifest_root(&self.root.clone()),
        ));
        Ok(Some(logical_version))
    }

    /// Separates the retained read from the optional atomic root write.
    fn into_parts(self) -> (PreparedTextExpectedRead, Option<PreparedTextWrite>) {
        (
            self.observation,
            self.write
                .map(|(key, value)| PreparedTextWrite::Put { key, value }),
        )
    }
}

/// Point-observes one root for catch-up, creating only an absent canonical root.
async fn prepare_catch_up_manifest_root(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    partition: TextPartition,
) -> Result<PreparedCatchUpManifestRoot> {
    let key = scoped_index_key(
        scope,
        ScopedKey::TextManifestRoot(TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: partition.fingerprint(),
        }),
    );
    let value = transaction.get(&key).await?;
    let Some(observed_value) = value.as_ref() else {
        let root = work::TextManifestRootValue::empty(
            operation.index_id(),
            operation.generation(),
            partition,
        );
        return Ok(PreparedCatchUpManifestRoot {
            observation: PreparedTextExpectedRead {
                key: key.clone(),
                value: None,
            },
            write: Some((key, encode_manifest_root(&root.clone()))),
            root,
        });
    };
    let root = decode_manifest_root(observed_value)?;
    let minimum_revision = u64::from(root.page_count()).saturating_add(1);
    let revision_is_valid = if root.page_count() == 0 {
        root.split_count() == 0
    } else {
        root.revision().get() >= minimum_revision && root.split_count() != 0
    };
    if root.index_id() != operation.index_id()
        || root.generation() != operation.generation()
        || root.partition() != &partition
        || !revision_is_valid
    {
        return Err(corruption(
            "text catch-up manifest root ownership or revision is invalid",
        ));
    }
    Ok(PreparedCatchUpManifestRoot {
        observation: PreparedTextExpectedRead { key, value },
        root,
        write: None,
    })
}

impl PreparedTextOperationStep {
    /// Returns disposable measurements retained by this closed preparation.
    pub(crate) fn resource_usage(&self) -> StepResourceUsage {
        match self {
            Self::PartitionUpload(prepared) | Self::CatchUpUpload(prepared) => StepResourceUsage {
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
                for write in &prepared.lifecycle_writes {
                    match write {
                        PreparedTextWrite::Put { key, value } => {
                            transaction.put(key, value)?;
                        }
                        PreparedTextWrite::Delete { key } => transaction.delete(key)?,
                    }
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
                    self.scan_tuning,
                    runtime,
                )
                .await?;
                Ok(PreparedIndexOperationStep::text(step))
            }
            TextBuildStage::CatchUp(progress) => {
                let Some(runtime) = &self.storage else {
                    return Ok(PreparedIndexOperationStep::driver_owned(
                        IndexOperationFamily::Text,
                        Box::new(()),
                    ));
                };
                let Some(step) =
                    prepare_catch_up_step(db, scope, operation, progress, limits, runtime).await?
                else {
                    return Ok(PreparedIndexOperationStep::driver_owned(
                        IndexOperationFamily::Text,
                        Box::new(()),
                    ));
                };
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
            TextBuildStage::ScanSource(_) | TextBuildStage::Activate(_) => Ok(
                PreparedIndexOperationStep::driver_owned(IndexOperationFamily::Text, Box::new(())),
            ),
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
        let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
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
                    definition,
                    progress,
                    limits,
                    self.scan_tuning,
                )
                .await
            }
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
        }?;
        Ok(IndexOperationStepExecution::new(result))
    }
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
    CatchUp,
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
async fn prepare_partition_step_with_scan_tuning(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
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
        scan_tuning,
    )
    .await?;
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
        runtime,
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
    let artifact_owner = TextBuildArtifactKey {
        root: TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: input.partition.fingerprint(),
        },
        ordinal: artifact_ordinal,
    };
    let artifact_key = scoped_index_key(scope, ScopedKey::TextBuildArtifact(artifact_owner));
    let artifact_value = encode_build_artifact(&work::TextBuildArtifactValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: input.partition.clone(),
        artifact_ordinal,
        split,
    });
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
    let is_catch_up = matches!(&input.source, PreparedTextUploadSource::CatchUp);
    let next_stage = match &input.source {
        PreparedTextUploadSource::Partition {
            progress,
            completed_cursor,
        } => TextBuildStage::ScanPartitions(SourceScanProgress {
            inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
            cursor: Some(completed_cursor.clone()),
            counters: completed_counters,
        }),
        PreparedTextUploadSource::CatchUp => TextBuildStage::CatchUp(PrefixScanProgress {
            cursor: None,
            counters: completed_counters,
        }),
    };
    let next_progress =
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(next_stage));
    let uploaded =
        crate::search::text::upload_blob(&runtime.object_store, &runtime.db_path, &payload)
            .await
            .map_err(|error| {
                HelixDbError::InvariantViolation(format!("text split upload failed: {error}"))
            })?;
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
    let artifact_owner = TextBuildArtifactKey {
        root: TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: selected.partition.fingerprint(),
        },
        ordinal: artifact_ordinal,
    };
    let artifact_key = scoped_index_key(scope, ScopedKey::TextBuildArtifact(artifact_owner));
    let artifact_value = encode_build_artifact(&work::TextBuildArtifactValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: selected.partition.clone(),
        artifact_ordinal,
        split,
    });
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
        crate::search::text::upload_blob(&runtime.object_store, &runtime.db_path, &payload)
            .await
            .map_err(|error| {
                HelixDbError::InvariantViolation(format!("text compaction upload failed: {error}"))
            })?;
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

    let mut external_result = None;
    for blob in prepared.blobs().iter().copied() {
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
    if generation_has_rows(transaction, scope, RecordKind::BuildDelta, operation).await? {
        return Ok(progressed_build(TextBuildStage::CatchUp(
            PrefixScanProgress {
                cursor: None,
                counters,
            },
        )));
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

/// One point-read catch-up decision with no partially representable outcome.
enum TextCatchUpPlanRead {
    Exhausted,
    Blocked(IndexOperationBlocker),
    Planned(TextCatchUpEntityPlan),
}

/// Exact roots required by one authoritative catch-up transition.
enum PreparedCatchUpManifestRoots {
    None,
    One(PreparedCatchUpManifestRoot),
    Move {
        previous: PreparedCatchUpManifestRoot,
        current: PreparedCatchUpManifestRoot,
    },
}

impl PreparedCatchUpManifestRoots {
    /// Returns the zero, one, or two distinct roots as borrowed slots.
    fn as_refs(&self) -> [Option<&PreparedCatchUpManifestRoot>; 2] {
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

    /// Returns the only partition root for an insert, update, or retirement.
    fn one_mut(&mut self) -> Result<&mut PreparedCatchUpManifestRoot> {
        let Self::One(root) = self else {
            return Err(corruption(
                "text catch-up transition requires exactly one manifest root",
            ));
        };
        Ok(root)
    }

    /// Returns the independently revisioned roots for a tenant move.
    fn move_mut(
        &mut self,
    ) -> Result<(
        &mut PreparedCatchUpManifestRoot,
        &mut PreparedCatchUpManifestRoot,
    )> {
        let Self::Move { previous, current } = self else {
            return Err(corruption(
                "text catch-up tenant move requires two manifest roots",
            ));
        };
        Ok((previous, current))
    }

    /// Finds the exact root that owns one observed entity state.
    fn root_for_partition(
        &self,
        partition: &TextPartition,
    ) -> Option<&PreparedCatchUpManifestRoot> {
        self.as_refs()
            .into_iter()
            .flatten()
            .find(|root| root.root.partition() == partition)
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
        ScopedKey::TextEntityState(TextEntityStateKey {
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
    let state = decode_text_entity_state(&value)?;
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
    runtime: &TextStorageRuntime,
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
        runtime,
        PreparedTextSplitInput {
            partition,
            documents: vec![document],
            completed_counters,
            source: PreparedTextUploadSource::CatchUp,
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
    let prefix = IndexKey::data_prefix(
        scope,
        ScopedKey::generation_prefix(
            RecordKind::BuildDelta,
            operation.index_id(),
            operation.generation(),
        ),
    );
    let mut rows = transaction.scan_prefix(&prefix, ..).await?;
    let Some(row) = rows.next().await? else {
        return Ok(TextCatchUpPlanRead::Exhausted);
    };
    let IndexKey::Data {
        kind: ScopedKey::BuildDelta(delta_key),
        ..
    } = IndexKey::parse_from_slice(scope, &row.key)?
    else {
        return Err(corruption(
            "text build-delta prefix yielded another key kind",
        ));
    };
    let delta = decode_build_delta(&row.value)?;
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
        ScopedKey::AppliedState(IndexEntityStateKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        }),
    );
    let applied_value = transaction.get(&applied_key).await?;
    let previous = match applied_value.as_ref() {
        Some(value) => {
            let applied = decode_applied_state(value)?;
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
        match super::projection::project(definition, &properties) {
            Ok(super::projection::TextSourceProjection::NotIndexed) => break 'current None,
            Ok(super::projection::TextSourceProjection::Indexed { partition, text }) => {
                break 'current Some((partition, text));
            }
            Err(_) => {
                return Ok(TextCatchUpPlanRead::Blocked(
                    IndexOperationBlocker::InvalidSourceData {
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                    },
                ));
            }
        }
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
                previous: prepare_catch_up_manifest_root(
                    transaction,
                    scope,
                    operation,
                    previous_partition.clone(),
                )
                .await?,
                current: prepare_catch_up_manifest_root(
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
                prepare_catch_up_manifest_root(transaction, scope, operation, partition.clone())
                    .await?,
            )
        }
    };
    let current_contribution = match current.as_ref() {
        Some((partition, text)) => {
            super::statistics::present_contribution(definition.analyzer(), partition.clone(), text)?
        }
        None => work::TextStatisticsContribution::Absent,
    };
    let statistics = super::statistics::prepare_build_mutation(
        transaction,
        scope,
        operation.index_id(),
        operation.generation(),
        entity,
        current_contribution,
    )
    .await?;
    let plan = build_text_catch_up_plan(
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
    )?;
    let TextCatchUpPlanRead::Planned(mut plan) = plan else {
        return Ok(plan);
    };
    let (statistics_input, statistics_operations, statistics_output) = statistics.measurements();
    for row in statistics.rows() {
        plan.expected_reads.push(PreparedTextExpectedRead {
            key: row.key.clone(),
            value: row.observed.clone(),
        });
        if row.replacement == row.observed {
            continue;
        }
        match &row.replacement {
            Some(value) => plan.writes.push(PreparedTextWrite::Put {
                key: row.key.clone(),
                value: value.clone(),
            }),
            None => plan.writes.push(PreparedTextWrite::Delete {
                key: row.key.clone(),
            }),
        }
    }
    plan.input_bytes = checked_add(
        plan.input_bytes,
        statistics_input,
        "catch-up statistics input bytes",
    )?;
    plan.output_operations = checked_add(
        plan.output_operations,
        statistics_operations,
        "catch-up statistics output operations",
    )?;
    plan.output_bytes = checked_add(
        plan.output_bytes,
        statistics_output,
        "catch-up statistics output bytes",
    )?;
    Ok(TextCatchUpPlanRead::Planned(plan))
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
    mut manifest_roots: PreparedCatchUpManifestRoots,
) -> Result<TextCatchUpPlanRead> {
    let state_row = |key, partition: &TextPartition, logical_version, live| {
        let value = encode_text_entity_state(&TextEntityStateValue {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: partition.clone(),
            entity_kind: entity.kind,
            entity_id: entity.id,
            logical_version,
            live,
        });
        PreparedTextWrite::Put { key, value }
    };
    let applied_row = |partition: TextPartition, logical_version| PreparedTextWrite::Put {
        key: applied_key.clone(),
        value: encode_applied_state(&AppliedEntityStateValue {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity_kind: entity.kind,
            entity_id: entity.id,
            state: AppliedFamilyState::Text(Some((partition, logical_version))),
        }),
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
        let (partition, logical_version) = match observed {
            ObservedTextEntityState::Absent { partition, .. } => (partition, None),
            ObservedTextEntityState::Present {
                partition,
                logical_version,
                ..
            } => (partition, Some(*logical_version)),
        };
        let Some(root) = manifest_roots.root_for_partition(partition) else {
            return Err(corruption(
                "text catch-up entity state has no matching manifest root",
            ));
        };
        if logical_version.is_some_and(|version| version.get() > root.root.revision().get()) {
            return Err(corruption(
                "text catch-up entity logical version exceeds its manifest revision",
            ));
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
            let Some(next_version) = manifest_roots.one_mut()?.advance_for_entity_transition()?
            else {
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
            let Some(version) = manifest_roots.one_mut()?.advance_for_entity_transition()? else {
                return Ok(TextCatchUpPlanRead::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
            };
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
            let Some(next_version) = manifest_roots.one_mut()?.advance_for_entity_transition()?
            else {
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
            let (previous_root, current_root) = manifest_roots.move_mut()?;
            let Some(dead_version) = previous_root.advance_for_entity_transition()? else {
                return Ok(TextCatchUpPlanRead::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
            };
            let Some(live_version) = current_root.advance_for_entity_transition()? else {
                return Ok(TextCatchUpPlanRead::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
            };
            let current_key = match current_state.as_ref() {
                Some(ObservedTextEntityState::Absent {
                    partition: observed_partition,
                    key,
                }) if observed_partition == &partition => key.clone(),
                Some(ObservedTextEntityState::Present {
                    partition: observed_partition,
                    key,
                    live: false,
                    ..
                }) if observed_partition == &partition => key.clone(),
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
    let root_input_bytes = manifest_roots.input_bytes();
    let (root_reads, root_writes) = manifest_roots.into_parts();
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
                match text_document(definition, &properties, &state) {
                    Ok(document) => document,
                    Err(_) => {
                        return Ok(PartitionScanSelection::Repository {
                            empty_root: None,
                            result: invalid_source(key.entity.kind, key.entity.id),
                        });
                    }
                }
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
    scan_tuning: IndexLifecycleScanTuning,
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
            let contribution = super::statistics::present_contribution(
                definition.analyzer(),
                partition.clone(),
                &text,
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
    use std::num::{NonZeroU64, NonZeroUsize};

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

    fn definition() -> ValidatedTextIndexDefinition {
        let runtime = crate::config::TextIndexDefinition::new_node("Document", "body")
            .expect("text test definition validates");
        ValidatedTextIndexDefinition::try_from_runtime(&runtime)
            .expect("text test definition has a V2 representation")
    }

    fn limits(
        max_input_bytes: u64,
        max_output_operations: u64,
        max_output_bytes: u64,
    ) -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::new(max_input_bytes).unwrap(),
            NonZeroU64::new(max_output_operations).unwrap(),
            NonZeroU64::new(max_output_bytes).unwrap(),
            NonZeroU64::new(max_output_bytes).unwrap(),
        )
        .expect("test limits are internally consistent")
    }

    fn entity() -> IndexEntity {
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(7),
        }
    }

    fn catch_up_root(
        partition: TextPartition,
        revision: TextManifestRevision,
    ) -> PreparedCatchUpManifestRoot {
        let key = partition.canonical_bytes();
        PreparedCatchUpManifestRoot {
            observation: PreparedTextExpectedRead { key, value: None },
            root: work::TextManifestRootValue::try_new(
                IndexId::initial(),
                IndexGenerationId::initial(),
                partition,
                revision,
                0,
                0,
            )
            .expect("zero-page test root is valid"),
            write: None,
        }
    }

    fn absent_state(partition: TextPartition) -> ObservedTextEntityState {
        ObservedTextEntityState::Absent {
            key: partition.canonical_bytes(),
            partition,
        }
    }

    fn present_state(
        partition: TextPartition,
        logical_version: TextLogicalVersion,
        live: bool,
    ) -> ObservedTextEntityState {
        ObservedTextEntityState::Present {
            key: partition.canonical_bytes(),
            value: Bytes::from_static(b"observed-state"),
            partition,
            logical_version,
            live,
        }
    }

    fn build_plan(
        previous: Option<(TextPartition, TextLogicalVersion)>,
        current: Option<(TextPartition, String)>,
        previous_state: Option<ObservedTextEntityState>,
        current_state: Option<ObservedTextEntityState>,
        roots: PreparedCatchUpManifestRoots,
    ) -> Result<TextCatchUpPlanRead> {
        build_text_catch_up_plan(
            &operation(),
            entity(),
            Bytes::from_static(b"delta-key"),
            Bytes::from_static(b"delta-value"),
            Bytes::from_static(b"applied-key"),
            Some(Bytes::from_static(b"applied-value")),
            Bytes::from_static(b"graph-key"),
            Some(Bytes::from_static(b"graph-value")),
            previous,
            current,
            previous_state,
            current_state,
            roots,
        )
    }

    fn planned(result: Result<TextCatchUpPlanRead>) -> TextCatchUpEntityPlan {
        let TextCatchUpPlanRead::Planned(plan) = result.expect("catch-up plan succeeds") else {
            panic!("catch-up fixture must produce physical work")
        };
        plan
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
        let (_, Some(PreparedTextWrite::Put { key, value })) = missing.into_parts() else {
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

        let mut catch_up = prepare_catch_up_manifest_root(
            &transaction,
            scope,
            &operation,
            TextPartition::Unpartitioned,
        )
        .await
        .unwrap();
        assert_eq!(catch_up.next_logical_version().unwrap().get(), 2);
        assert_eq!(
            catch_up
                .advance_for_entity_transition()
                .unwrap()
                .unwrap()
                .get(),
            2
        );
        let (observation, write) = catch_up.into_parts();
        assert!(observation.value.is_some());
        assert!(matches!(write, Some(PreparedTextWrite::Put { .. })));

        let state_key = scoped_index_key(
            scope,
            ScopedKey::TextEntityState(TextEntityStateKey {
                root: TextManifestRootKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: TextPartition::Unpartitioned.fingerprint(),
                },
                entity: entity(),
            }),
        );
        assert!(matches!(
            read_catch_up_entity_state(
                &transaction,
                scope,
                &operation,
                entity(),
                TextPartition::Unpartitioned,
            )
            .await
            .unwrap(),
            ObservedTextEntityState::Absent { .. }
        ));
        transaction
            .put(
                state_key.clone(),
                encode_text_entity_state(&TextEntityStateValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: TextPartition::Unpartitioned,
                    entity_kind: entity().kind,
                    entity_id: entity().id,
                    logical_version: TextLogicalVersion::initial(),
                    live: true,
                }),
            )
            .unwrap();
        assert!(matches!(
            read_catch_up_entity_state(
                &transaction,
                scope,
                &operation,
                entity(),
                TextPartition::Unpartitioned,
            )
            .await
            .unwrap(),
            ObservedTextEntityState::Present { live: true, .. }
        ));
        transaction
            .put(
                state_key,
                encode_text_entity_state(&TextEntityStateValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: TextPartition::Unpartitioned,
                    entity_kind: entity().kind,
                    entity_id: IndexEntityId::new(8),
                    logical_version: TextLogicalVersion::initial(),
                    live: true,
                }),
            )
            .unwrap();
        assert!(read_catch_up_entity_state(
            &transaction,
            scope,
            &operation,
            entity(),
            TextPartition::Unpartitioned,
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn catch_up_reads_one_exact_delta_and_obeys_encoded_resource_boundaries() {
        let db = Db::open("text-driver-catch-up-contracts", Arc::new(InMemory::new()))
            .await
            .expect("text catch-up test database opens");
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let operation = operation();
        let definition = definition();
        let scope = DataScope::LegacyUnscoped;
        let progress = PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        };
        let delta_key = scoped_index_key(
            scope,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                entity: entity(),
            }),
        );
        transaction
            .put(
                delta_key.clone(),
                crate::encoding::v2::values::encode_build_delta(&work::CoalescedBuildDeltaValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    entity_kind: entity().kind,
                    entity_id: entity().id,
                }),
            )
            .unwrap();

        let no_document =
            plan_next_catch_up(&transaction, scope, &operation, &definition, &progress)
                .await
                .unwrap();
        let TextCatchUpPlanRead::Planned(no_document) = no_document else {
            panic!("missing graph row produces one repository-only catch-up plan")
        };
        assert!(no_document.document.is_none());

        for constrained in [limits(1, u64::MAX, u64::MAX), limits(u64::MAX, 1, u64::MAX)] {
            assert!(matches!(
                catch_up(
                    &transaction,
                    scope,
                    &operation,
                    &definition,
                    &progress,
                    constrained,
                )
                .await
                .unwrap(),
                IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity { .. })
            ));
        }
        assert!(matches!(
            catch_up(
                &transaction,
                scope,
                &operation,
                &definition,
                &progress,
                limits(u64::MAX, u64::MAX, 1),
            )
            .await
            .unwrap(),
            IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity { .. })
        ));
        assert!(matches!(
            catch_up(
                &transaction,
                scope,
                &operation,
                &definition,
                &progress,
                limits(u64::MAX, u64::MAX, u64::MAX),
            )
            .await
            .unwrap(),
            IndexOperationStepResult::Progressed(_)
        ));
        assert!(transaction.get(&delta_key).await.unwrap().is_none());
        assert!(matches!(
            plan_next_catch_up(&transaction, scope, &operation, &definition, &progress,)
                .await
                .unwrap(),
            TextCatchUpPlanRead::Exhausted
        ));
        assert!(plan_next_catch_up(
            &transaction,
            scope,
            &operation,
            &definition,
            &PrefixScanProgress {
                cursor: Some(IndexCursor::try_new(Bytes::from_static(b"cursor")).unwrap()),
                counters: OperationCounters::default(),
            },
        )
        .await
        .is_err());

        transaction
            .put(
                delta_key.clone(),
                crate::encoding::v2::values::encode_build_delta(&work::CoalescedBuildDeltaValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    entity_kind: entity().kind,
                    entity_id: entity().id,
                }),
            )
            .unwrap();
        transaction
            .put(
                authoritative_property_key(scope, entity()),
                property::encode_properties(&[
                    property::Property::string("$label", "Document"),
                    property::Property::string("body", "the searchable document"),
                ]),
            )
            .unwrap();
        assert!(matches!(
            catch_up(
                &transaction,
                scope,
                &operation,
                &definition,
                &progress,
                limits(u64::MAX, u64::MAX, u64::MAX),
            )
            .await
            .unwrap(),
            IndexOperationStepResult::TransientFailure
        ));
        transaction
            .put(
                authoritative_property_key(scope, entity()),
                Bytes::from_static(b"malformed-properties"),
            )
            .unwrap();
        assert!(matches!(
            plan_next_catch_up(&transaction, scope, &operation, &definition, &progress,)
                .await
                .unwrap(),
            TextCatchUpPlanRead::Blocked(IndexOperationBlocker::InvalidSourceData { .. })
        ));
    }

    #[test]
    fn catch_up_plan_encodes_insert_update_retire_move_and_noop_algorithms() {
        let first = TextPartition::Unpartitioned;
        let second = TextPartition::try_tenant_value(Bytes::from_static(b"tenant-b")).unwrap();
        let version = TextLogicalVersion::initial();
        let revision = TextManifestRevision::initial();

        let noop = planned(build_plan(
            None,
            None,
            None,
            None,
            PreparedCatchUpManifestRoots::None,
        ));
        assert!(noop.document.is_none());
        assert_eq!(noop.writes.len(), 1);
        assert_eq!(noop.expected_reads.len(), 3);

        let insert = planned(build_plan(
            None,
            Some((first.clone(), "inserted".to_string())),
            None,
            Some(absent_state(first.clone())),
            PreparedCatchUpManifestRoots::One(catch_up_root(first.clone(), revision)),
        ));
        assert!(insert.document.is_some());
        assert_eq!(insert.writes.len(), 4);

        let update = planned(build_plan(
            Some((first.clone(), version)),
            Some((first.clone(), "updated".to_string())),
            Some(present_state(first.clone(), version, true)),
            Some(present_state(first.clone(), version, true)),
            PreparedCatchUpManifestRoots::One(catch_up_root(first.clone(), revision)),
        ));
        assert!(update.document.is_some());
        assert_eq!(update.writes.len(), 4);
        assert!(update.input_bytes > 0);
        assert_eq!(update.output_operations, update.writes.len() as u64);
        assert!(update.output_bytes > 0);

        let retirement = planned(build_plan(
            Some((first.clone(), version)),
            None,
            Some(present_state(first.clone(), version, true)),
            None,
            PreparedCatchUpManifestRoots::One(catch_up_root(first.clone(), revision)),
        ));
        assert!(retirement.document.is_none());
        assert_eq!(retirement.writes.len(), 4);

        for destination_state in [
            absent_state(second.clone()),
            present_state(second.clone(), version, false),
        ] {
            let moved = planned(build_plan(
                Some((first.clone(), version)),
                Some((second.clone(), "moved".to_string())),
                Some(present_state(first.clone(), version, true)),
                Some(destination_state),
                PreparedCatchUpManifestRoots::Move {
                    previous: catch_up_root(first.clone(), revision),
                    current: catch_up_root(second.clone(), revision),
                },
            ));
            assert!(moved.document.is_some());
            assert_eq!(moved.writes.len(), 6);
        }
    }

    #[test]
    fn catch_up_plan_rejects_every_cross_state_and_revision_mismatch() {
        let first = TextPartition::Unpartitioned;
        let second = TextPartition::try_tenant_value(Bytes::from_static(b"tenant-b")).unwrap();
        let version = TextLogicalVersion::initial();
        let revision = TextManifestRevision::initial();

        assert!(build_plan(
            None,
            None,
            Some(absent_state(first.clone())),
            None,
            PreparedCatchUpManifestRoots::None,
        )
        .is_err());
        assert!(build_plan(
            None,
            Some((first.clone(), "insert".to_string())),
            None,
            Some(absent_state(first.clone())),
            PreparedCatchUpManifestRoots::None,
        )
        .is_err());
        assert!(build_plan(
            Some((first.clone(), version)),
            None,
            None,
            None,
            PreparedCatchUpManifestRoots::One(catch_up_root(first.clone(), revision)),
        )
        .is_err());
        assert!(build_plan(
            Some((first.clone(), version)),
            Some((first.clone(), "update".to_string())),
            Some(present_state(
                first.clone(),
                TextLogicalVersion::new(2).unwrap(),
                true,
            )),
            None,
            PreparedCatchUpManifestRoots::One(catch_up_root(first.clone(), revision)),
        )
        .is_err());
        assert!(build_plan(
            Some((first.clone(), version)),
            Some((second.clone(), "move".to_string())),
            Some(present_state(first.clone(), version, true)),
            Some(present_state(second.clone(), version, true)),
            PreparedCatchUpManifestRoots::Move {
                previous: catch_up_root(first.clone(), revision),
                current: catch_up_root(second.clone(), revision),
            },
        )
        .is_err());

        let blocked = build_plan(
            None,
            Some((first.clone(), "insert".to_string())),
            None,
            Some(absent_state(first.clone())),
            PreparedCatchUpManifestRoots::One(catch_up_root(
                first,
                TextManifestRevision::new(u64::MAX).unwrap(),
            )),
        )
        .unwrap();
        assert!(matches!(
            blocked,
            TextCatchUpPlanRead::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/index_lifecycle_text_driver_prepared.rs"]
mod prepared_contracts;
