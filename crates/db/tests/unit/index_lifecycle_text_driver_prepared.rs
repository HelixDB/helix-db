use std::collections::HashSet;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::BoxStream;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use slatedb::{Db, IsolationLevel};
use tokio::sync::Notify;

use super::*;
use crate::index_lifecycle::TextManifestRevision;
use crate::index_lifecycle::{
    ClaimSequence, IndexGenerationId, IndexId, IndexOperationId, IndexOperationKind,
    IndexOperationRevision, IndexRevision, OperationClaim, WriterEpoch,
};

fn operation() -> IndexOperationRecord {
    let runtime = crate::config::TextIndexDefinition::new_node("Document", "body")
        .expect("prepared-step definition validates");
    let definition = ValidatedTextIndexDefinition::try_from_runtime(&runtime)
        .expect("prepared-step definition has a V2 representation");
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
    .expect("prepared-step operation is internally consistent")
}

fn progressed(operation: &IndexOperationRecord) -> IndexOperationProgress {
    operation.progress().clone()
}

fn claimed_operation() -> IndexOperationRecord {
    operation()
        .claim(OperationClaim {
            writer_epoch: WriterEpoch::from_bytes([5; 16]).unwrap(),
            sequence: ClaimSequence::new(1).unwrap(),
        })
        .unwrap()
}

fn definition() -> ValidatedTextIndexDefinition {
    ValidatedTextIndexDefinition::try_from_runtime(
        &crate::config::TextIndexDefinition::new_node("Document", "body").unwrap(),
    )
    .unwrap()
}

/// The build record whose generation `operation()` scans.
fn record() -> IndexRecordV2 {
    let operation = operation();
    IndexRecordV2::building(
        operation.index_id(),
        ValidatedDynamicIndexDefinition::Text(definition()),
        operation.index_record_revision(),
        crate::index_lifecycle::PhysicalGeneration::Text {
            generation: operation.generation(),
        },
        operation.operation_id(),
    )
    .unwrap()
}

/// The default policy's per-document publication limits.
fn document_limits() -> crate::config::ActiveTextMutationLimits {
    crate::config::SearchIndexBackfillLimits::default().active_text_mutation()
}

fn limits(max_output_bytes: u64) -> SearchIndexBatchLimits {
    batch_limits(8, u64::MAX, u64::MAX, max_output_bytes)
}

fn batch_limits(
    max_entities: usize,
    max_input_bytes: u64,
    max_output_operations: u64,
    max_output_bytes: u64,
) -> SearchIndexBatchLimits {
    SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(max_entities).unwrap(),
        NonZeroU64::new(max_input_bytes).unwrap(),
        NonZeroU64::new(max_output_operations).unwrap(),
        NonZeroU64::new(max_output_bytes).unwrap(),
        NonZeroU64::new(max_output_bytes).unwrap(),
    )
    .unwrap()
}

#[derive(Debug, Default)]
struct HeadState {
    active: usize,
    armed: bool,
    entered: usize,
    peak: usize,
    released: bool,
    transient: HashSet<Path>,
    fail_put: bool,
}

#[derive(Debug, Default)]
struct ControlledHeadStore {
    inner: InMemory,
    state: Mutex<HeadState>,
    changed: Notify,
}

impl ControlledHeadStore {
    fn arm(&self) {
        let mut state = self.state.lock().expect("head state is healthy");
        state.active = 0;
        state.armed = true;
        state.entered = 0;
        state.peak = 0;
        state.released = false;
    }

    fn fail_head(&self, path: Path) {
        self.state
            .lock()
            .expect("head state is healthy")
            .transient
            .insert(path);
    }

    async fn wait_until_entered(&self, expected: usize) {
        loop {
            let notified = self.changed.notified();
            if self.state.lock().expect("head state is healthy").entered >= expected {
                return;
            }
            notified.await;
        }
    }

    fn release(&self) {
        self.state.lock().expect("head state is healthy").released = true;
        self.changed.notify_waiters();
    }

    fn peak(&self) -> usize {
        self.state.lock().expect("head state is healthy").peak
    }
}

struct ActiveHeadGuard<'a> {
    state: &'a Mutex<HeadState>,
}

impl Drop for ActiveHeadGuard<'_> {
    fn drop(&mut self) {
        self.state.lock().expect("head state is healthy").active -= 1;
    }
}

impl fmt::Display for ControlledHeadStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("controlled-head-memory")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ControlledHeadStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if self.state.lock().unwrap().fail_put {
            return Err(slatedb::object_store::Error::Generic {
                store: "fixture",
                source: std::io::Error::other("temporary upload failure").into(),
            });
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let _guard = if options.head {
            let armed = {
                let mut state = self.state.lock().expect("head state is healthy");
                state.active += 1;
                state.entered += 1;
                state.peak = state.peak.max(state.active);
                state.armed
            };
            self.changed.notify_waiters();
            if armed {
                loop {
                    let notified = self.changed.notified();
                    if self.state.lock().expect("head state is healthy").released {
                        break;
                    }
                    notified.await;
                }
            }
            Some(ActiveHeadGuard { state: &self.state })
        } else {
            None
        };
        if options.head
            && self
                .state
                .lock()
                .expect("head state is healthy")
                .transient
                .contains(location)
        {
            return Err(slatedb::object_store::Error::Generic {
                store: "controlled-head-memory",
                source: Box::new(std::io::Error::other("injected HEAD failure")),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug, Clone, Copy)]
enum HeadFixture {
    Valid,
    Missing,
    WrongSize,
    Transient,
}

async fn validation_head_fixture(
    database: &str,
    fixtures: &[HeadFixture],
) -> (
    Db,
    IndexOperationRecord,
    ValidatedTextIndexDefinition,
    TextManifestValidationProgress,
    TextStorageRuntime,
    Arc<ControlledHeadStore>,
) {
    let scope = DataScope::LegacyUnscoped;
    let operation = operation();
    let definition = definition();
    let dynamic = ValidatedDynamicIndexDefinition::Text(definition.clone());
    let record = IndexRecordV2::building(
        operation.index_id(),
        dynamic,
        operation.index_record_revision(),
        crate::index_lifecycle::PhysicalGeneration::Text {
            generation: operation.generation(),
        },
        operation.operation_id(),
    )
    .unwrap();
    let db = Db::open(database, Arc::new(InMemory::new())).await.unwrap();
    db.put(
        scoped_index_key(scope, ScopedKey::index_record(record.identity().clone())),
        crate::encoding::v2::values::encode_index_record(&record),
    )
    .await
    .unwrap();

    let partition = TextPartition::Unpartitioned;
    let root = TextManifestRootKey {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: partition.fingerprint(),
    };
    let root_value = work::TextManifestRootValue::try_new(
        operation.index_id(),
        operation.generation(),
        partition.clone(),
        TextManifestRevision::new(2).unwrap(),
        1,
        u64::try_from(fixtures.len()).unwrap(),
    )
    .unwrap();
    db.put(
        scoped_index_key(scope, ScopedKey::TextManifestRoot(root)),
        crate::encoding::v2::values::encode_manifest_root(&root_value),
    )
    .await
    .unwrap();

    let store = Arc::new(ControlledHeadStore::default());
    let mut splits = Vec::new();
    for (ordinal, fixture) in fixtures.iter().copied().enumerate() {
        let seed = u8::try_from(ordinal + 1).unwrap();
        let split = crate::index_lifecycle::text::test_support::split(seed, 128);
        let path = crate::search::text::blob_object_store_path(database, *split.blob().hash());
        match fixture {
            HeadFixture::Valid | HeadFixture::Transient => {
                store
                    .put(&path, PutPayload::from(vec![0; 128]))
                    .await
                    .unwrap();
            }
            HeadFixture::Missing => {}
            HeadFixture::WrongSize => {
                store
                    .put(&path, PutPayload::from(vec![0; 127]))
                    .await
                    .unwrap();
            }
        }
        if matches!(fixture, HeadFixture::Transient) {
            store.fail_head(path);
        }
        splits.push(split);
    }
    let page = work::TextManifestPageValue::try_new(
        operation.index_id(),
        operation.generation(),
        partition,
        0,
        splits,
    )
    .unwrap();
    db.put(
        scoped_index_key(
            scope,
            ScopedKey::TextManifestPage(crate::encoding::v2::keys::TextManifestPageKey {
                root,
                page: 0,
            }),
        ),
        crate::encoding::v2::values::encode_manifest_page(&page),
    )
    .await
    .unwrap();
    let runtime = TextStorageRuntime {
        object_store: store.clone(),
        db_path: database.to_string(),
        compaction_limits: crate::config::SearchIndexBackfillLimits::default().text_compaction(),
    };
    (
        db,
        operation,
        definition,
        TextManifestValidationProgress::initial(OperationCounters::default()),
        runtime,
        store,
    )
}

async fn stage_prepared_validation(
    db: &Db,
    step: PreparedTextOperationStep,
) -> IndexOperationStepResult {
    let PreparedTextOperationStep::Validation(prepared) = step else {
        panic!("manifest validation prepares an exact validation step")
    };
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    match *prepared {
        PreparedTextValidationStep::Database { prepared, .. } => {
            prepared.stage(&transaction).await.unwrap()
        }
        PreparedTextValidationStep::Page { prepared, .. } => {
            prepared.stage(&transaction).await.unwrap()
        }
    }
}

#[tokio::test]
async fn manifest_blob_heads_run_with_exact_bounded_concurrency() {
    let (db, operation, _, progress, runtime, store) =
        validation_head_fixture("text-validation-concurrent-heads", &[HeadFixture::Valid; 9]).await;
    store.arm();
    let preparation = prepare_validation_step(
        &db,
        DataScope::LegacyUnscoped,
        &operation,
        &progress,
        limits(u64::MAX),
        &runtime,
    );
    tokio::pin!(preparation);
    let reached_window = tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(1), store.wait_until_entered(8)) => {
            result.is_ok()
        }
        _ = &mut preparation => panic!("validation completed before HEAD window filled"),
    };
    store.release();
    let step = preparation.await.unwrap();
    assert!(reached_window, "eight HEAD requests must overlap");
    assert_eq!(store.peak(), 8);
    assert!(matches!(
        stage_prepared_validation(&db, step).await,
        IndexOperationStepResult::Progressed(_)
    ));
}

#[tokio::test]
async fn concurrent_head_results_preserve_complete_error_classification() {
    #[derive(Debug, Clone, Copy)]
    enum Expected {
        Progressed,
        Blocked,
        Transient,
    }

    for (ordinal, (fixtures, expected)) in [
        (vec![HeadFixture::Valid], Expected::Progressed),
        (vec![HeadFixture::Missing], Expected::Blocked),
        (vec![HeadFixture::WrongSize], Expected::Blocked),
        (vec![HeadFixture::Transient], Expected::Transient),
        (
            vec![HeadFixture::Transient, HeadFixture::WrongSize],
            Expected::Blocked,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let database = format!("text-validation-head-classification-{ordinal}");
        let (db, operation, _, progress, runtime, _) =
            validation_head_fixture(&database, &fixtures).await;
        let step = prepare_validation_step(
            &db,
            DataScope::LegacyUnscoped,
            &operation,
            &progress,
            limits(u64::MAX),
            &runtime,
        )
        .await
        .unwrap();
        let actual = stage_prepared_validation(&db, step).await;
        assert!(
            matches!(
                (actual, expected),
                (
                    IndexOperationStepResult::Progressed(_),
                    Expected::Progressed
                ) | (
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation),
                    Expected::Blocked
                ) | (
                    IndexOperationStepResult::TransientFailure,
                    Expected::Transient
                )
            ),
            "HEAD fixture {fixtures:?} returned the wrong result"
        );
        db.close().await.unwrap();
    }
}

fn split_input(counters: OperationCounters) -> PreparedTextSplitInput {
    PreparedTextSplitInput {
        partition: TextPartition::Unpartitioned,
        documents: vec![crate::search::text::TextDocumentInput::new(
            7,
            "one searchable document",
        )],
        completed_counters: counters,
        progress: SourceScanProgress {
            inclusive_upper_bound: IndexCursor::try_new(Bytes::from_static(b"upper")).unwrap(),
            cursor: None,
            counters: OperationCounters::default(),
        },
        completed_cursor: IndexCursor::try_new(Bytes::from_static(b"completed")).unwrap(),
        expected_reads: Vec::new(),
        lifecycle_writes: Vec::new(),
    }
}

fn expected(key: &'static [u8], value: Option<&'static [u8]>) -> PreparedTextExpectedRead {
    PreparedTextExpectedRead {
        key: Bytes::from_static(key),
        value: value.map(Bytes::from_static),
    }
}

fn upload(
    operation: &IndexOperationRecord,
    artifact_key: &'static [u8],
) -> PreparedTextBuildUpload {
    PreparedTextBuildUpload {
        source_operation: operation.clone(),
        progress: progressed(operation),
        artifact_key: Bytes::from_static(artifact_key),
        artifact_value: Bytes::from_static(b"artifact-value"),
        expected_reads: vec![expected(b"upload-observation", None)],
        lifecycle_writes: vec![
            PreparedTextWrite {
                key: Bytes::from_static(b"upload-put"),
                value: Some(Bytes::from_static(b"put-value")),
            },
            PreparedTextWrite {
                key: Bytes::from_static(b"upload-delete"),
                value: None,
            },
        ],
        retired_artifact_keys: Vec::new(),
        uploaded_bytes: 14,
    }
}

#[tokio::test]
async fn repository_and_upload_preparations_obey_exact_observations_and_variants() {
    let db = Db::open(
        "text-driver-prepared-operation-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .expect("prepared-step database opens");
    let other_operation = operation();
    let operation = operation();
    let scope = DataScope::LegacyUnscoped;

    let repository = PreparedTextOperationStep::Repository(Box::new(PreparedTextRepositoryStep {
        source_operation: operation.clone(),
        expected_reads: vec![expected(b"repository-observation", None)],
        writes: vec![PreparedTextWrite {
            key: Bytes::from_static(b"repository-put"),
            value: Some(Bytes::from_static(b"put-value")),
        }],
        result: IndexOperationStepResult::Progressed(progressed(&operation)),
    }));
    assert_eq!(repository.resource_usage(), StepResourceUsage::default());
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(repository
        .stage(&transaction, scope, &other_operation)
        .await
        .is_err());
    drop(transaction);

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(b"repository-observation", b"now-stale")
        .unwrap();
    assert!(matches!(
        repository
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
    drop(transaction);

    let repository_current =
        PreparedTextOperationStep::Repository(Box::new(PreparedTextRepositoryStep {
            source_operation: operation.clone(),
            expected_reads: vec![expected(b"repository-current", Some(b"current"))],
            writes: vec![
                PreparedTextWrite {
                    key: Bytes::from_static(b"repository-current-put"),
                    value: Some(Bytes::from_static(b"put-value")),
                },
                PreparedTextWrite {
                    key: Bytes::from_static(b"repository-current-delete"),
                    value: None,
                },
            ],
            result: IndexOperationStepResult::Progressed(progressed(&operation)),
        }));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction.put(b"repository-current", b"current").unwrap();
    transaction
        .put(b"repository-current-delete", b"stale-row")
        .unwrap();
    assert!(matches!(
        repository_current
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::Progressed(_)
    ));
    assert_eq!(
        transaction.get(b"repository-current-put").await.unwrap(),
        Some(Bytes::from_static(b"put-value"))
    );
    assert_eq!(
        transaction.get(b"repository-current-delete").await.unwrap(),
        None
    );
    drop(transaction);

    let partition = PreparedTextOperationStep::PartitionUpload(Box::new(upload(
        &operation,
        b"partition-artifact",
    )));
    assert_eq!(
        partition.resource_usage(),
        StepResourceUsage {
            text_artifact_bytes: 14,
            text_upload_bytes: 14,
            ..StepResourceUsage::default()
        }
    );
    let compaction = PreparedTextOperationStep::CompactionUpload(Box::new(upload(
        &operation,
        b"compaction-artifact",
    )));
    assert_eq!(
        compaction.resource_usage(),
        StepResourceUsage {
            text_artifact_bytes: 14,
            text_upload_bytes: 14,
            compaction_fan_in: 0,
            compaction_input_bytes: 0,
            temporary_bytes: 14,
            ..StepResourceUsage::default()
        }
    );

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(partition
        .stage(&transaction, scope, &other_operation)
        .await
        .is_err());
    drop(transaction);

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(b"partition-artifact", b"already-occupied")
        .unwrap();
    assert!(partition
        .stage(&transaction, scope, &operation)
        .await
        .is_err());
    drop(transaction);

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(b"upload-observation", b"now-stale")
        .unwrap();
    assert!(matches!(
        partition
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
    drop(transaction);

    let current_upload =
        PreparedTextOperationStep::PartitionUpload(Box::new(PreparedTextBuildUpload {
            expected_reads: vec![expected(b"upload-current", Some(b"current"))],
            ..upload(&operation, b"current-artifact")
        }));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction.put(b"upload-current", b"current").unwrap();
    transaction.put(b"upload-delete", b"stale-row").unwrap();
    assert!(matches!(
        current_upload
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::Progressed(_)
    ));
    assert_eq!(
        transaction.get(b"current-artifact").await.unwrap(),
        Some(Bytes::from_static(b"artifact-value"))
    );
    assert_eq!(
        transaction.get(b"upload-put").await.unwrap(),
        Some(Bytes::from_static(b"put-value"))
    );
    assert_eq!(transaction.get(b"upload-delete").await.unwrap(), None);
    drop(transaction);

    partition.discard().await.unwrap();
    compaction.after_commit().await;
    db.close().await.expect("prepared-step database closes");
}

#[tokio::test]
async fn compaction_retirement_and_manifest_roots_fail_closed_at_every_boundary() {
    let db = Db::open(
        "text-driver-prepared-root-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .expect("prepared-root database opens");
    let other_operation = operation();
    let operation = operation();
    let scope = DataScope::LegacyUnscoped;

    let retirement = PreparedTextOperationStep::CompactionRetirement(Box::new(
        PreparedTextCompactionRetirement {
            source_operation: operation.clone(),
            expected_reads: vec![expected(b"retirement-observation", None)],
            input_artifact_keys: Vec::new(),
            progress: progressed(&operation),
        },
    ));
    assert_eq!(
        retirement.resource_usage(),
        StepResourceUsage {
            compaction_fan_in: 0,
            compaction_input_bytes: 0,
            temporary_bytes: 0,
            ..StepResourceUsage::default()
        }
    );
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(retirement
        .stage(&transaction, scope, &other_operation)
        .await
        .is_err());
    drop(transaction);

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(b"retirement-observation", b"now-stale")
        .unwrap();
    assert!(matches!(
        retirement
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
    drop(transaction);

    let current_retirement = PreparedTextOperationStep::CompactionRetirement(Box::new(
        PreparedTextCompactionRetirement {
            source_operation: operation.clone(),
            expected_reads: Vec::new(),
            input_artifact_keys: Vec::new(),
            progress: progressed(&operation),
        },
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(current_retirement
        .stage(&transaction, scope, &operation)
        .await
        .is_err());
    drop(transaction);

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let non_empty = work::TextManifestRootValue::try_new(
        operation.index_id(),
        operation.generation(),
        TextPartition::Unpartitioned,
        TextManifestRevision::new(2).unwrap(),
        1,
        1,
    )
    .unwrap();
    let root_key = scoped_index_key(
        scope,
        ScopedKey::TextManifestRoot(TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: TextPartition::Unpartitioned.fingerprint(),
        }),
    );
    transaction
        .put(&root_key, encode_manifest_root(&non_empty))
        .unwrap();
    assert!(prepare_empty_manifest_root(
        &transaction,
        scope,
        &operation,
        TextPartition::Unpartitioned,
    )
    .await
    .is_err());
    drop(transaction);

    assert!(matches!(
        operation_error(crate::index_lifecycle::IndexOperationModelError::ZeroClaimSequence),
        HelixDbError::InvariantViolation(_)
    ));
    assert!(matches!(
        work_error(crate::index_lifecycle::work::IndexWorkModelError::EmptyTenantPartition),
        HelixDbError::InvariantViolation(_)
    ));
    db.close().await.expect("prepared-root database closes");
}

#[tokio::test]
async fn build_upload_classifies_limits_and_encodes_partition_resumes() {
    let unclaimed_operation = operation();
    let operation = claimed_operation();
    let definition = definition();
    let object_store = Arc::new(ControlledHeadStore::default());
    let runtime = TextStorageRuntime {
        object_store: object_store.clone(),
        db_path: "text-driver-build-upload-contracts".to_string(),
        compaction_limits: crate::config::SearchIndexBackfillLimits::default().text_compaction(),
    };

    let blocked_payload = prepare_build_upload(
        &operation,
        DataScope::LegacyUnscoped,
        &definition,
        limits(1),
        &runtime,
        split_input(OperationCounters::default()),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::Repository(blocked_payload) = blocked_payload else {
        panic!("one-byte output limit blocks the physical split payload")
    };
    assert!(matches!(
        blocked_payload.result,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit { limit: 1, .. })
    ));

    let blocked_ordinal = prepare_build_upload(
        &operation,
        DataScope::LegacyUnscoped,
        &definition,
        limits(u64::MAX),
        &runtime,
        split_input(OperationCounters {
            output_operations: u64::from(u32::MAX) + 1,
            ..OperationCounters::default()
        }),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::Repository(blocked_ordinal) = blocked_ordinal else {
        panic!("an exhausted artifact ordinal blocks before publication")
    };
    assert!(matches!(
        blocked_ordinal.result,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
            limit,
            ..
        }) if limit == u64::from(u32::MAX)
    ));

    let partition = prepare_build_upload(
        &operation,
        DataScope::LegacyUnscoped,
        &definition,
        limits(u64::MAX),
        &runtime,
        split_input(OperationCounters::default()),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::PartitionUpload(partition) = partition else {
        panic!("a bounded partition batch chooses one exact upload")
    };
    assert!(partition.uploaded_bytes > 1);
    assert_eq!(
        u64::try_from(partition.artifact_key.len() + partition.artifact_value.len()).unwrap(),
        build_artifact_row_bytes(
            DataScope::LegacyUnscoped,
            &operation,
            &TextPartition::Unpartitioned
        )
        .unwrap(),
        "the partition scan reserves exactly the artifact row an upload writes"
    );
    assert!(matches!(
        partition.progress,
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(SourceScanProgress {
                cursor: Some(_),
                ..
            })
        ))
    ));

    object_store.state.lock().unwrap().fail_put = true;
    assert!(matches!(
        prepare_build_upload(
            &operation,
            DataScope::LegacyUnscoped,
            &definition,
            limits(u64::MAX),
            &runtime,
            split_input(OperationCounters::default()),
        )
        .await,
        Err(HelixDbError::ObjectStore(_))
    ));

    assert!(prepare_build_upload(
        &unclaimed_operation,
        DataScope::LegacyUnscoped,
        &definition,
        limits(u64::MAX),
        &runtime,
        split_input(OperationCounters::default()),
    )
    .await
    .is_err());
}

#[test]
fn text_driver_key_projection_and_counter_helpers_fail_closed() {
    let operation = operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(7),
    };
    let state = TextEntityStateValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: TextPartition::Unpartitioned,
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version: TextLogicalVersion::initial(),
        live: true,
    };
    let state_key = scoped_index_key(
        scope,
        ScopedKey::TextEntityState(TextEntityStateKey {
            root: TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: TextPartition::Unpartitioned.fingerprint(),
            },
            entity,
        }),
    );
    assert_eq!(
        decode_entity_state(
            scope,
            &state_key,
            &encode_text_entity_state(&state),
            &operation
        )
        .unwrap()
        .1,
        state
    );
    let wrong_state = TextEntityStateValue {
        entity_id: IndexEntityId::new(8),
        ..state.clone()
    };
    assert!(decode_entity_state(
        scope,
        &state_key,
        &encode_text_entity_state(&wrong_state),
        &operation,
    )
    .is_err());
    let wrong_key = scoped_index_key(
        scope,
        ScopedKey::AppliedState(IndexEntityStateKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        }),
    );
    assert!(decode_entity_state(
        scope,
        &wrong_key,
        &encode_text_entity_state(&state),
        &operation,
    )
    .is_err());
    assert!(decode_entity_state(scope, &state_key, b"malformed", &operation).is_err());

    let indexed = vec![
        property::Property::string("$label", "Document"),
        property::Property::string("body", "searchable"),
    ];
    assert_eq!(
        text_document(&definition, &indexed, &state)
            .unwrap()
            .unwrap(),
        crate::search::text::TextDocumentInput::new(7, "searchable")
    );
    let moved_state = TextEntityStateValue {
        partition: TextPartition::try_tenant_value(Bytes::from_static(b"other")).unwrap(),
        ..state.clone()
    };
    assert_eq!(
        text_document(&definition, &indexed, &moved_state).unwrap(),
        None
    );
    assert_eq!(
        text_document(
            &definition,
            &[property::Property::string("$label", "Other")],
            &state,
        )
        .unwrap(),
        None
    );
    assert!(text_document(
        &definition,
        &[
            property::Property::string("$label", "Document"),
            property::Property::new(
                "body",
                crate::encoding::v2::values::property::property_value::PropertyValue::I64(1),
            ),
        ],
        &state,
    )
    .is_err());

    let node_key = authoritative_property_key(scope, entity);
    assert_eq!(
        source_entity(scope, IndexElementKind::Node, &node_key).unwrap(),
        Some(entity.id)
    );
    let edge = IndexEntity {
        kind: IndexElementKind::Edge,
        id: IndexEntityId::new(9),
    };
    let edge_key = authoritative_property_key(scope, edge);
    assert_eq!(
        source_entity(scope, IndexElementKind::Edge, &edge_key).unwrap(),
        Some(edge.id)
    );
    assert_eq!(
        source_entity(scope, IndexElementKind::Edge, &node_key).unwrap(),
        None
    );
    assert!(source_entity(scope, IndexElementKind::Node, &edge_key).is_err());
    assert_ne!(
        source_prefix(scope, IndexElementKind::Node),
        source_prefix(scope, IndexElementKind::Edge)
    );

    let prefix = Bytes::from_static(b"prefix/");
    assert_eq!(cursor_suffix(&prefix, None).unwrap(), None);
    let complete = IndexCursor::try_new(Bytes::from_static(b"prefix/suffix")).unwrap();
    assert_eq!(
        cursor_suffix(&prefix, Some(&complete)).unwrap(),
        Some(Bytes::from_static(b"suffix"))
    );
    let foreign = IndexCursor::try_new(Bytes::from_static(b"foreign/suffix")).unwrap();
    assert!(cursor_suffix(&prefix, Some(&foreign)).is_err());
    assert_eq!(checked_add(2, 3, "fixture").unwrap(), 5);
    assert!(checked_add(u64::MAX, 1, "fixture").is_err());
    assert!(initial_partition_scan(&operation, scope, OperationCounters::default()).is_ok());
}

async fn scan_source_case(
    database: &'static str,
    value: Bytes,
    limits: SearchIndexBatchLimits,
    preexisting_state: bool,
) -> Result<IndexOperationStepResult> {
    let db = Db::open(database, Arc::new(InMemory::new())).await.unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = operation();
    let scope = DataScope::LegacyUnscoped;
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(7),
    };
    let source_key = authoritative_property_key(scope, entity);
    transaction.put(&source_key, value).unwrap();
    if preexisting_state {
        transaction
            .put(
                scoped_index_key(
                    scope,
                    ScopedKey::TextEntityState(TextEntityStateKey {
                        root: TextManifestRootKey {
                            index_id: operation.index_id(),
                            generation: operation.generation(),
                            partition: TextPartition::Unpartitioned.fingerprint(),
                        },
                        entity,
                    }),
                ),
                b"occupied",
            )
            .unwrap();
    }
    let result = scan_source(
        &transaction,
        &transaction,
        scope,
        &operation,
        &record(),
        &SourceScanProgress {
            inclusive_upper_bound: IndexCursor::try_new(source_key).unwrap(),
            cursor: None,
            counters: OperationCounters::default(),
        },
        limits,
        document_limits(),
        IndexLifecycleScanTuning::default(),
    )
    .await;
    drop(transaction);
    db.close().await.unwrap();
    result
}

#[tokio::test]
async fn source_scan_attributes_every_input_output_and_corruption_boundary() {
    let malformed = scan_source_case(
        "text-driver-source-malformed",
        Bytes::from_static(b"malformed"),
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        false,
    )
    .await
    .unwrap();
    assert!(matches!(
        malformed,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData { .. })
    ));

    let oversized_input = scan_source_case(
        "text-driver-source-input-limit",
        Bytes::from(vec![0; 128]),
        batch_limits(8, 1, u64::MAX, u64::MAX),
        false,
    )
    .await
    .unwrap();
    assert!(matches!(
        oversized_input,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity { limit: 1, .. })
    ));

    let ignored = scan_source_case(
        "text-driver-source-not-indexed",
        property::encode_properties(&[property::Property::string("$label", "Other")]),
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        false,
    )
    .await
    .unwrap();
    assert!(matches!(
        ignored,
        IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::ScanPartitions(_))
        ))
    ));

    let indexed = property::encode_properties(&[
        property::Property::string("$label", "Document"),
        property::Property::string("body", "searchable"),
    ]);
    // The build transaction's own limits are independent of the publication
    // allowance every document is admitted to. One term stages its entity
    // and applied states, term row, marker, and corpus row.
    let output_operations = scan_source_case(
        "text-driver-source-output-operations",
        indexed.clone(),
        batch_limits(8, u64::MAX, 1, u64::MAX),
        false,
    )
    .await
    .unwrap();
    assert!(
        matches!(
            output_operations,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                observed: 5,
                limit: 1,
                ..
            })
        ),
        "{output_operations:?}"
    );
    let output_bytes = scan_source_case(
        "text-driver-source-output-bytes",
        indexed.clone(),
        batch_limits(8, u64::MAX, u64::MAX, 1),
        false,
    )
    .await
    .unwrap();
    assert!(
        matches!(
            output_bytes,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                observed,
                limit: 1,
                ..
            }) if observed > 1
        ),
        "{output_bytes:?}"
    );
    assert!(scan_source_case(
        "text-driver-source-preexisting",
        indexed,
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        true,
    )
    .await
    .is_err());

    let db = Db::open("text-driver-source-cursors", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = operation();
    let scope = DataScope::LegacyUnscoped;
    let upper = authoritative_property_key(
        scope,
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(7),
        },
    );
    let equal = IndexCursor::try_new(upper.clone()).unwrap();
    assert!(matches!(
        scan_source(
            &transaction,
            &transaction,
            scope,
            &operation,
            &record(),
            &SourceScanProgress {
                inclusive_upper_bound: equal.clone(),
                cursor: Some(equal),
                counters: OperationCounters::default(),
            },
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            document_limits(),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        IndexOperationStepResult::Progressed(_)
    ));
    let greater = authoritative_property_key(
        scope,
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(8),
        },
    );
    assert!(scan_source(
        &transaction,
        &transaction,
        scope,
        &operation,
        &record(),
        &SourceScanProgress {
            inclusive_upper_bound: IndexCursor::try_new(upper).unwrap(),
            cursor: Some(IndexCursor::try_new(greater).unwrap()),
            counters: OperationCounters::default(),
        },
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        document_limits(),
        IndexLifecycleScanTuning::default(),
    )
    .await
    .is_err());
    drop(transaction);
    db.close().await.unwrap();
}

/// A build must not index a document whose later publication could not
/// analyze it: 300,000 one-byte tokens fit every row budget but exceed the
/// publisher's analysis budget.
#[tokio::test]
async fn source_scan_blocks_documents_over_the_publication_analysis_budget() {
    let limits = crate::config::SearchIndexBackfillLimits::default();
    let analysis_limit = limits.text_compaction().max_input_bytes().get();
    let result = scan_source_case(
        "text-driver-source-analysis-budget",
        property::encode_properties(&[
            property::Property::string("$label", "Document"),
            property::Property::string("body", "a ".repeat(300_000)),
        ]),
        limits.batch(),
        false,
    )
    .await
    .unwrap();
    assert!(
        matches!(
            result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                observed,
                limit,
                ..
            }) if limit == analysis_limit && observed > limit
        ),
        "{result:?}"
    );
}

/// Writes to a graph row the step already read (1) and to one ahead of its
/// batch (6) commit between the step's reads and its commit. Graph rows are
/// read from a snapshot, so the step still commits; a serializable scan of
/// the range fails that commit with a conflict. Each entity's statistics
/// marker records the text its own step read.
#[tokio::test]
async fn source_scan_step_commits_through_writes_to_its_source_range() {
    use crate::index_lifecycle::lifecycle::{create_index_operation, InitialBuildProgress};
    use crate::index_lifecycle::outbox::{
        claim_operation, execute_claimed_step, observe_operation_pointer, read_operation,
        ClaimPermission, CommittedOperationStep, OperationPointerObservation,
        WriteDuringStepDriver,
    };

    let db = Db::open(
        "text-driver-source-concurrent-writes",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    crate::migrations::startup::bootstrap_writer(&db)
        .await
        .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let key = |id| authoritative_property_key(scope, reconciliation_entity(id));
    let row = |body: &str| {
        property::encode_properties(&[
            property::Property::string("$label", "Document"),
            property::Property::string("body", body),
        ])
    };
    for id in 0..8 {
        db.put(key(id), row(&format!("alpha {id}"))).await.unwrap();
    }
    let crate::index_lifecycle::IndexDdlReceipt::Accepted {
        operation_id,
        index_id,
        generation,
    } = create_index_operation(
        &db,
        scope,
        ValidatedDynamicIndexDefinition::Text(definition()),
        helix_planner::ir::IndexCreateMode::ErrorIfExists,
        InitialBuildProgress::text(IndexCursor::try_new(key(7)).unwrap()),
    )
    .await
    .unwrap()
    else {
        panic!("a new text definition enqueues a build");
    };
    let inner = TextIndexDriver::new();
    let racing = WriteDuringStepDriver {
        inner: &inner,
        writes: vec![(key(1), row("omega")), (key(6), row("omega"))],
    };
    let writer_epoch = WriterEpoch::from_bytes([0x7C; 16]).unwrap();
    let steps: [(&dyn IndexOperationDriver, u64); 2] = [(&racing, 3), (&inner, 7)];
    for (sequence, (driver, cursor)) in (1..).zip(steps) {
        let OperationPointerObservation::Eligible(eligible) =
            observe_operation_pointer(&db, operation_id, writer_epoch, 1)
                .await
                .unwrap()
        else {
            panic!("the queued text build is eligible");
        };
        let claimed = claim_operation(
            &db,
            &eligible,
            writer_epoch,
            ClaimSequence::new(sequence).unwrap(),
            1,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            execute_claimed_step(
                &db,
                &claimed,
                driver,
                batch_limits(4, u64::MAX, u64::MAX, u64::MAX),
                1
            )
            .await
            .unwrap(),
            CommittedOperationStep::Progressed
        );
        let operation = read_operation(&db, scope, operation_id)
            .await
            .unwrap()
            .unwrap();
        let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanSource(progress),
        )) = operation.progress()
        else {
            panic!("the build is still scanning its source");
        };
        assert_eq!(
            progress.cursor,
            Some(IndexCursor::try_new(key(cursor)).unwrap())
        );
        assert_eq!(progress.counters.entities, cursor + 1);
    }

    let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
    for (id, text) in [(1, "alpha 1"), (6, "omega")] {
        let (_, marker) = crate::index_lifecycle::text::statistics::read_marker(
            &snapshot,
            None,
            scope,
            index_id,
            generation,
            reconciliation_entity(id),
        )
        .await
        .unwrap()
        .expect("scanned entity has a statistics marker");
        assert_eq!(
            marker.contribution,
            crate::index_lifecycle::text::statistics::present_contribution(
                definition().analyzer(),
                TextPartition::Unpartitioned,
                text,
            )
            .unwrap(),
            "entity {id}"
        );
    }
    drop(snapshot);
    db.close().await.unwrap();
}

/// A write repairs the invalid graph row 0 after the ScanSource step reads it
/// and before the step commits. The step reads its blocker's row through the
/// serializable transaction, so the commit fails instead of blocking the
/// build durably, and the retried step stages the repaired row.
#[tokio::test]
async fn source_scan_step_does_not_commit_a_blocker_repaired_in_its_window() {
    use crate::index_lifecycle::lifecycle::{create_index_operation, InitialBuildProgress};
    use crate::index_lifecycle::outbox::{
        claim_operation, execute_claimed_step, observe_operation_pointer, read_operation,
        ClaimPermission, CommittedOperationStep, OperationPointerObservation,
        SameEpochRecoveryProof, WriteDuringStepDriver,
    };

    let db = Db::open(
        "text-driver-source-repaired-blocker",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    crate::migrations::startup::bootstrap_writer(&db)
        .await
        .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let key = |id| authoritative_property_key(scope, reconciliation_entity(id));
    let row = |body: &str| {
        property::encode_properties(&[
            property::Property::string("$label", "Document"),
            property::Property::string("body", body),
        ])
    };
    db.put(key(0), b"malformed").await.unwrap();
    for id in 1..4 {
        db.put(key(id), row(&format!("alpha {id}"))).await.unwrap();
    }
    let crate::index_lifecycle::IndexDdlReceipt::Accepted {
        operation_id,
        index_id,
        generation,
    } = create_index_operation(
        &db,
        scope,
        ValidatedDynamicIndexDefinition::Text(definition()),
        helix_planner::ir::IndexCreateMode::ErrorIfExists,
        InitialBuildProgress::text(IndexCursor::try_new(key(3)).unwrap()),
    )
    .await
    .unwrap()
    else {
        panic!("a new text definition enqueues a build");
    };
    let inner = TextIndexDriver::new();
    let racing = WriteDuringStepDriver {
        inner: &inner,
        writes: vec![(key(0), row("repaired"))],
    };
    let writer_epoch = WriterEpoch::from_bytes([0x7D; 16]).unwrap();
    let limits = batch_limits(4, u64::MAX, u64::MAX, u64::MAX);
    let OperationPointerObservation::Eligible(eligible) =
        observe_operation_pointer(&db, operation_id, writer_epoch, 1)
            .await
            .unwrap()
    else {
        panic!("the queued text build is eligible");
    };
    let claimed = claim_operation(
        &db,
        &eligible,
        writer_epoch,
        ClaimSequence::new(1).unwrap(),
        1,
        ClaimPermission::Normal,
    )
    .await
    .unwrap()
    .unwrap();
    let error = execute_claimed_step(&db, &claimed, &racing, limits, 1)
        .await
        .expect_err("a blocker whose row was repaired does not commit");
    assert!(error.is_transaction_conflict(), "{error}");

    // The supervisor rejoins its task and retries the same step.
    let OperationPointerObservation::ClaimedByCurrentWriter(eligible) =
        observe_operation_pointer(&db, operation_id, writer_epoch, 1)
            .await
            .unwrap()
    else {
        panic!("the failed step leaves its claim with this writer");
    };
    let claimed = claim_operation(
        &db,
        &eligible,
        writer_epoch,
        ClaimSequence::new(2).unwrap(),
        1,
        ClaimPermission::SameEpochRecovery(SameEpochRecoveryProof::after_join(writer_epoch)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        execute_claimed_step(&db, &claimed, &inner, limits, 1)
            .await
            .unwrap(),
        CommittedOperationStep::Progressed
    );
    let operation = read_operation(&db, scope, operation_id)
        .await
        .unwrap()
        .unwrap();
    let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
        TextBuildStage::ScanSource(progress),
    )) = operation.progress()
    else {
        panic!("the build is still scanning its source");
    };
    assert_eq!(progress.cursor, Some(IndexCursor::try_new(key(3)).unwrap()));
    assert_eq!(progress.counters.entities, 4);
    let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let (_, marker) = crate::index_lifecycle::text::statistics::read_marker(
        &snapshot,
        None,
        scope,
        index_id,
        generation,
        reconciliation_entity(0),
    )
    .await
    .unwrap()
    .expect("the repaired entity has a statistics marker");
    assert_eq!(
        marker.contribution,
        crate::index_lifecycle::text::statistics::present_contribution(
            definition().analyzer(),
            TextPartition::Unpartitioned,
            "repaired",
        )
        .unwrap()
    );
    drop(snapshot);
    db.close().await.unwrap();
}

/// A graph row turns invalid after ScanSource, so ScanPartitions prepares an
/// InvalidSourceData blocker. The blocker carries that row as observed: staged
/// against the unchanged row, a repair before commit fails the commit, and
/// once the repair is visible the prepared blocker stages as a retry.
#[tokio::test]
async fn partition_blocker_retries_once_its_graph_row_is_repaired() {
    let scope = DataScope::LegacyUnscoped;
    let operation = claimed_operation();
    let db = Db::open(
        "text-driver-partition-repaired-blocker",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let graph_key = authoritative_property_key(scope, reconciliation_entity(7));
    let row = |body: &str| {
        property::encode_properties(&[
            property::Property::string("$label", "Document"),
            property::Property::string("body", body),
        ])
    };
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            scoped_index_key(scope, ScopedKey::index_record(record().identity().clone())),
            crate::encoding::v2::values::encode_index_record(&record()),
        )
        .unwrap();
    transaction.put(&graph_key, row("alpha")).unwrap();
    assert!(matches!(
        scan_source(
            &transaction,
            &transaction,
            scope,
            &operation,
            &record(),
            &SourceScanProgress {
                inclusive_upper_bound: IndexCursor::try_new(graph_key.clone()).unwrap(),
                cursor: None,
                counters: OperationCounters::default(),
            },
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            document_limits(),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::ScanPartitions(_))
        ))
    ));
    let (_, root) = prepare_empty_manifest_root(
        &transaction,
        scope,
        &operation,
        TextPartition::Unpartitioned,
    )
    .await
    .unwrap()
    .into_parts();
    root.into_iter()
        .try_for_each(|root| root.stage(&transaction))
        .unwrap();
    transaction.commit().await.unwrap();
    db.put(&graph_key, b"malformed").await.unwrap();

    let runtime = TextStorageRuntime {
        object_store: Arc::new(InMemory::new()),
        db_path: "text-driver-partition-repaired-blocker".to_string(),
        compaction_limits: crate::config::SearchIndexBackfillLimits::default().text_compaction(),
    };
    let prepared = prepare_partition_step_with_scan_tuning(
        &db,
        scope,
        &operation,
        &initial_partition_scan(&operation, scope, OperationCounters::default()).unwrap(),
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        document_limits(),
        IndexLifecycleScanTuning::default(),
        &runtime,
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::Repository(repository) = &prepared else {
        panic!("a partition blocker is a repository step")
    };
    assert!(matches!(
        repository.result,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData { .. })
    ));

    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(matches!(
        prepared
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData { .. })
    ));
    db.put(&graph_key, row("repaired")).await.unwrap();
    let error = HelixDbError::from(transaction.commit().await.unwrap_err());
    assert!(error.is_transaction_conflict(), "{error}");

    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(matches!(
        prepared
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
    drop(transaction);
    db.close().await.unwrap();
}

#[tokio::test]
async fn partition_scan_separates_root_creation_document_upload_and_empty_exhaustion() {
    let empty_operation = operation();
    let db = Db::open(
        "text-driver-partition-scan-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(7),
    };
    let partition = TextPartition::Unpartitioned;
    let state_key = scoped_index_key(
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
    let state = TextEntityStateValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: partition.clone(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version: TextLogicalVersion::initial(),
        live: true,
    };
    transaction
        .put(&state_key, encode_text_entity_state(&state))
        .unwrap();
    transaction
        .put(
            authoritative_property_key(scope, entity),
            property::encode_properties(&[
                property::Property::string("$label", "Document"),
                property::Property::string("body", "searchable"),
            ]),
        )
        .unwrap();
    let progress = initial_partition_scan(&operation, scope, OperationCounters::default()).unwrap();

    let missing_root = scan_partition_documents(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        document_limits(),
        IndexLifecycleScanTuning::default(),
    )
    .await
    .unwrap();
    let PartitionScanSelection::Repository {
        empty_root: Some(root),
        result: IndexOperationStepResult::Progressed(_),
        ..
    } = missing_root
    else {
        panic!("a missing canonical root is created before any upload")
    };
    let root_input_bytes = root.input_bytes();
    let root_output_bytes = root.output_bytes();
    assert!(root.requires_creation());

    for (limits, expected_limit) in [
        (batch_limits(8, 1, u64::MAX, u64::MAX), 1),
        (batch_limits(8, u64::MAX, u64::MAX, 1), 1),
    ] {
        let PartitionScanSelection::Blocked(
            IndexOperationBlocker::ManifestLimit { limit, .. },
            None,
        ) = scan_partition_documents(
            &transaction,
            scope,
            &operation,
            &definition,
            &progress,
            limits,
            document_limits(),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap()
        else {
            panic!("empty-root resource boundaries are durable manifest blockers")
        };
        assert_eq!(limit, expected_limit);
    }
    assert!(root_input_bytes > 1);
    assert!(root_output_bytes > 1);
    let seed_limit = root_input_bytes.saturating_add(1);
    let PartitionScanSelection::Blocked(IndexOperationBlocker::ManifestLimit { .. }, None) =
        scan_partition_documents(
            &transaction,
            scope,
            &operation,
            &definition,
            &progress,
            batch_limits(8, seed_limit, u64::MAX, u64::MAX),
            document_limits(),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap()
    else {
        panic!("root plus first state row is bounded as one seed observation")
    };

    let root_key = scoped_index_key(
        scope,
        ScopedKey::TextManifestRoot(TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: partition.fingerprint(),
        }),
    );
    transaction
        .put(
            root_key,
            encode_manifest_root(&work::TextManifestRootValue::empty(
                operation.index_id(),
                operation.generation(),
                partition.clone(),
            )),
        )
        .unwrap();
    // ScanSource stages every live state with its partition's contribution.
    let marker_key = scoped_index_key(
        scope,
        ScopedKey::TextStatisticsEntity(crate::encoding::v2::keys::TextStatisticsEntityKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        }),
    );
    let marker = |contribution| {
        crate::encoding::v2::values::encode_statistics_entity(&work::TextStatisticsEntityValue {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity_kind: entity.kind,
            entity_id: entity.id,
            contribution,
        })
    };
    for unaccounted in [None, Some(work::TextStatisticsContribution::Absent)] {
        match unaccounted {
            Some(contribution) => transaction.put(&marker_key, marker(contribution)).unwrap(),
            None => transaction.delete(&marker_key).unwrap(),
        }
        assert!(
            scan_partition_documents(
                &transaction,
                scope,
                &operation,
                &definition,
                &progress,
                batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
                document_limits(),
                IndexLifecycleScanTuning::default(),
            )
            .await
            .is_err(),
            "a live state without its present contribution is corruption"
        );
    }
    transaction
        .put(
            &marker_key,
            marker(
                super::super::statistics::present_contribution(
                    definition.analyzer(),
                    partition.clone(),
                    "searchable",
                )
                .unwrap(),
            ),
        )
        .unwrap();
    let PartitionScanSelection::Upload(upload) = scan_partition_documents(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        document_limits(),
        IndexLifecycleScanTuning::default(),
    )
    .await
    .unwrap() else {
        panic!("one live current document is an explicit upload selection")
    };
    assert_eq!(upload.partition, partition);
    assert_eq!(upload.documents.len(), 1);
    assert!(
        upload.reconciliation.expected_reads.is_empty() && upload.reconciliation.writes.is_empty(),
        "an unchanged document needs no reconciliation"
    );
    assert_eq!(upload.completed_cursor.as_bytes(), &state_key);

    transaction
        .put(
            &state_key,
            encode_text_entity_state(&TextEntityStateValue {
                live: false,
                ..state.clone()
            }),
        )
        .unwrap();
    assert!(matches!(
        scan_partition_documents(
            &transaction,
            scope,
            &operation,
            &definition,
            &progress,
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            document_limits(),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        PartitionScanSelection::Repository {
            result: IndexOperationStepResult::Progressed(_),
            ..
        }
    ));
    transaction
        .put(&state_key, encode_text_entity_state(&state))
        .unwrap();
    transaction
        .put(authoritative_property_key(scope, entity), b"malformed")
        .unwrap();
    assert!(matches!(
        scan_partition_documents(
            &transaction,
            scope,
            &operation,
            &definition,
            &progress,
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            document_limits(),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        PartitionScanSelection::Blocked(
            IndexOperationBlocker::InvalidSourceData { .. },
            Some(PreparedTextExpectedRead { key, value }),
        ) if key == authoritative_property_key(scope, entity)
            && value.as_deref() == Some(b"malformed".as_slice())
    ));

    let mut wrong_progress = progress.clone();
    wrong_progress.inclusive_upper_bound =
        IndexCursor::try_new(Bytes::from_static(b"wrong")).unwrap();
    assert!(scan_partition_documents(
        &transaction,
        scope,
        &operation,
        &definition,
        &wrong_progress,
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        document_limits(),
        IndexLifecycleScanTuning::default(),
    )
    .await
    .is_err());
    drop(transaction);
    db.close().await.unwrap();

    let empty_db = Db::open(
        "text-driver-partition-empty-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = empty_db.begin(IsolationLevel::Snapshot).await.unwrap();
    let empty_progress =
        initial_partition_scan(&empty_operation, scope, OperationCounters::default()).unwrap();
    assert!(matches!(
        scan_partition_documents(
            &transaction,
            scope,
            &empty_operation,
            &definition,
            &empty_progress,
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            document_limits(),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        PartitionScanSelection::Repository {
            empty_root: Some(_),
            ..
        }
    ));
    let tenant_runtime = crate::config::TextIndexDefinition::new_node("Document", "body")
        .unwrap()
        .with_tenant_property("tenant")
        .unwrap();
    let tenant_definition =
        ValidatedTextIndexDefinition::try_from_runtime(&tenant_runtime).unwrap();
    assert!(matches!(
        scan_partition_documents(
            &transaction,
            scope,
            &empty_operation,
            &tenant_definition,
            &empty_progress,
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            document_limits(),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        PartitionScanSelection::Repository {
            empty_root: None,
            result: IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
                TextBuildProgress::Constructing(TextBuildStage::Compact(_))
            )),
            ..
        }
    ));
    drop(transaction);
    empty_db.close().await.unwrap();
}

/// Graph rows of the reconciliation fixture: `(entity, tenant, body)`.
type ReconciliationRows = [(u64, &'static str, Option<&'static str>); 6];

const SCANNED_ROWS: ReconciliationRows = [
    (7, "a", Some("stable alpha")),
    (8, "a", Some("deleted beta")),
    (9, "a", Some("before gamma")),
    (10, "a", Some("unindexed epsilon")),
    (11, "a", Some("moved zeta")),
    (12, "a", Some("same order")),
];

/// Deleted, re-texted, un-indexed, moved, and reordered after `ScanSource`.
const CURRENT_ROWS: ReconciliationRows = [
    (7, "a", Some("stable alpha")),
    (8, "a", None),
    (9, "a", Some("after gamma delta")),
    (10, "a", Some("")),
    (11, "b", Some("moved zeta")),
    (12, "a", Some("order same")),
];

fn tenant_definition() -> ValidatedTextIndexDefinition {
    ValidatedTextIndexDefinition::try_from_runtime(
        &crate::config::TextIndexDefinition::new_node("Document", "body")
            .unwrap()
            .with_tenant_property("tenant")
            .unwrap(),
    )
    .unwrap()
}

/// The build record whose generation `operation` scans, for the tenant index.
fn tenant_record(operation: &IndexOperationRecord) -> IndexRecordV2 {
    IndexRecordV2::building(
        operation.index_id(),
        ValidatedDynamicIndexDefinition::Text(tenant_definition()),
        operation.index_record_revision(),
        crate::index_lifecycle::PhysicalGeneration::Text {
            generation: operation.generation(),
        },
        operation.operation_id(),
    )
    .unwrap()
}

fn reconciliation_entity(id: u64) -> IndexEntity {
    IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(id),
    }
}

fn tenant_partition(tenant: &str) -> TextPartition {
    TextPartition::try_tenant_value(
        crate::encoding::v2::values::property::encode_index_partition_value(
            &crate::encoding::v2::values::property::property_value::PropertyValue::String(
                tenant.to_string(),
            ),
        ),
    )
    .unwrap()
}

/// Entity-state key of `id` in `tenant`'s partition of `operation`'s generation.
fn tenant_state_key(
    scope: DataScope,
    operation: &IndexOperationRecord,
    tenant: &str,
    id: u64,
) -> Bytes {
    scoped_index_key(
        scope,
        ScopedKey::TextEntityState(TextEntityStateKey {
            root: TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: tenant_partition(tenant).fingerprint(),
            },
            entity: reconciliation_entity(id),
        }),
    )
}

/// Writes graph rows; `Some("")` keeps the entity but drops the indexed text.
fn put_reconciliation_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    rows: &[(u64, &'static str, Option<&'static str>)],
) {
    for (id, tenant, body) in rows {
        let key = authoritative_property_key(scope, reconciliation_entity(*id));
        let Some(body) = body else {
            transaction.delete(&key).unwrap();
            continue;
        };
        let mut properties = vec![
            property::Property::string("$label", "Document"),
            property::Property::string("tenant", *tenant),
        ];
        if !body.is_empty() {
            properties.push(property::Property::string("body", *body));
        }
        transaction
            .put(&key, property::encode_properties(&properties))
            .unwrap();
    }
}

/// Stages one source scan over `rows` and the empty roots it needs.
async fn scanned_generation(
    database: &'static str,
    scope: DataScope,
    operation: &IndexOperationRecord,
    rows: &[(u64, &'static str, Option<&'static str>)],
) -> Db {
    let db = Db::open(database, Arc::new(InMemory::new())).await.unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    put_reconciliation_rows(&transaction, scope, rows);
    let upper = rows.iter().map(|(id, _, _)| *id).max().unwrap();
    assert!(matches!(
        scan_source(
            &transaction,
            &transaction,
            scope,
            operation,
            &tenant_record(operation),
            &SourceScanProgress {
                inclusive_upper_bound: IndexCursor::try_new(authoritative_property_key(
                    scope,
                    reconciliation_entity(upper),
                ))
                .unwrap(),
                cursor: None,
                counters: OperationCounters::default(),
            },
            batch_limits(64, u64::MAX, u64::MAX, u64::MAX),
            document_limits(),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::ScanPartitions(_))
        ))
    ));
    for tenant in ["a", "b"] {
        let (_, root) =
            prepare_empty_manifest_root(&transaction, scope, operation, tenant_partition(tenant))
                .await
                .unwrap()
                .into_parts();
        root.into_iter()
            .try_for_each(|root| root.stage(&transaction))
            .unwrap();
    }
    transaction.commit().await.unwrap();
    db
}

/// Every statistics row of one generation except the per-entity markers.
async fn corpus_and_term_rows(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Vec<(Bytes, Bytes)> {
    let mut rows = Vec::new();
    for kind in [
        RecordKind::TextCorpusStatistics,
        RecordKind::TextTermStatistics,
    ] {
        let prefix = IndexKey::data_prefix(
            scope,
            ScopedKey::generation_prefix(kind, operation.index_id(), operation.generation()),
        );
        let mut scan = db.scan_prefix(&prefix, ..).await.unwrap();
        while let Some(row) = scan.next().await.unwrap() {
            rows.push((row.key, row.value));
        }
    }
    rows
}

#[tokio::test]
async fn partition_scan_reconciles_entities_changed_after_the_source_scan() {
    let scope = DataScope::LegacyUnscoped;
    let operation = operation();
    let definition = tenant_definition();
    let db = scanned_generation(
        "text-driver-partition-reconciliation",
        scope,
        &operation,
        &SCANNED_ROWS,
    )
    .await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    put_reconciliation_rows(&transaction, scope, &CURRENT_ROWS);
    transaction.commit().await.unwrap();
    let progress = initial_partition_scan(&operation, scope, OperationCounters::default()).unwrap();
    let state_key = |id| tenant_state_key(scope, &operation, "a", id);

    let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let scan = |progress: SourceScanProgress, batch, document| {
        let snapshot = &snapshot;
        let operation = &operation;
        let definition = &definition;
        async move {
            scan_partition_documents(
                snapshot,
                scope,
                operation,
                definition,
                &progress,
                batch,
                document,
                IndexLifecycleScanTuning::default(),
            )
            .await
            .unwrap()
        }
    };

    // The unchanged entity fits; the deleted one's retirement does not.
    let PartitionScanSelection::Upload(first) = scan(
        progress.clone(),
        batch_limits(8, u64::MAX, 2, u64::MAX),
        document_limits(),
    )
    .await
    else {
        panic!("the unchanged entity uploads before the budget ends")
    };
    assert_eq!(first.documents.len(), 1);
    assert_eq!(first.completed_cursor.as_bytes(), &state_key(7));
    assert!(first.reconciliation.writes.is_empty());
    let after_stable = SourceScanProgress {
        cursor: Some(first.completed_cursor.clone()),
        ..progress.clone()
    };
    let PartitionScanSelection::Blocked(
        IndexOperationBlocker::OversizedEntity {
            entity_id,
            observed,
            limit: 2,
            ..
        },
        Some(graph_read),
    ) = scan(
        after_stable.clone(),
        batch_limits(8, u64::MAX, 2, u64::MAX),
        document_limits(),
    )
    .await
    else {
        panic!("a first entity whose retirement exceeds the budget blocks")
    };
    assert_eq!(entity_id, IndexEntityId::new(8));
    assert!(observed > 2);
    // The blocker carries the deleted entity's absent graph row.
    assert_eq!(
        graph_read.key,
        authoritative_property_key(scope, reconciliation_entity(8))
    );
    assert_eq!(graph_read.value, None);
    let (retired_state, graph) = (
        snapshot.get(&state_key(8)).await.unwrap().unwrap(),
        authoritative_property_key(scope, reconciliation_entity(8)),
    );
    let root_input =
        prepare_empty_manifest_root(&snapshot, scope, &operation, first.partition.clone())
            .await
            .unwrap()
            .input_bytes();
    let PartitionScanSelection::Repository { result, .. } = scan(
        after_stable.clone(),
        batch_limits(1, u64::MAX, u64::MAX, u64::MAX),
        document_limits(),
    )
    .await
    else {
        panic!("a lone deleted entity advances without an upload")
    };
    let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(TextBuildStage::ScanPartitions(retired)),
    )) = result
    else {
        panic!("a lone deleted entity keeps scanning its partition")
    };
    // Source rows are bounded by the batch input, and the retirement's reads
    // by the publication input allowance.
    let source =
        root_input + u64::try_from(state_key(8).len() + retired_state.len() + graph.len()).unwrap();
    let retirement_input = retired.counters.input_bytes - source;
    assert!(retirement_input > 0);
    let page = crate::config::SearchIndexBackfillLimits::default()
        .text_compaction()
        .max_manifest_bytes()
        .get();
    let exact = |max_source| batch_limits(8, max_source, u64::MAX, u64::MAX);
    assert!(matches!(
        scan(
            after_stable.clone(),
            exact(source),
            mutation_limits(
                crate::config::SearchIndexBackfillLimits::default().batch(),
                retirement_input,
                page
            ),
        )
        .await,
        PartitionScanSelection::Repository {
            result: IndexOperationStepResult::Progressed(_),
            ..
        }
    ));
    assert!(matches!(
        scan(
            after_stable.clone(),
            exact(source - 1),
            mutation_limits(crate::config::SearchIndexBackfillLimits::default().batch(), retirement_input, page),
        )
        .await,
        PartitionScanSelection::Blocked(IndexOperationBlocker::ManifestLimit { observed, .. }, Some(_))
            if observed == source
    ));
    assert!(matches!(
        scan(
            after_stable,
            exact(source),
            mutation_limits(crate::config::SearchIndexBackfillLimits::default().batch(), retirement_input - 1, page),
        )
        .await,
        PartitionScanSelection::Blocked(IndexOperationBlocker::OversizedEntity { observed, .. }, Some(_))
            if observed == retirement_input
    ));

    // One unbounded run reconciles every change.
    let PartitionScanSelection::Upload(upload) = scan(
        progress,
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        document_limits(),
    )
    .await
    else {
        panic!("the reconciled partition uploads its current documents")
    };
    drop(snapshot);
    assert_eq!(
        upload
            .documents
            .iter()
            .map(|document| (document.entity_id, document.text.as_str()))
            .collect::<Vec<_>>(),
        [
            (7, "stable alpha"),
            (9, "after gamma delta"),
            (12, "order same")
        ]
    );
    assert!(upload.completed_counters.output_operations > 0);
    // Nothing reads text applied state after `ScanSource`, so reconciliation
    // neither fences nor rewrites it.
    assert!(upload
        .reconciliation
        .expected_reads
        .iter()
        .map(|read| &read.key)
        .chain(upload.reconciliation.writes.iter().map(|write| &write.key))
        .all(|key| !matches!(
            IndexKey::parse_from_slice(scope, key),
            Ok(IndexKey::Data {
                kind: ScopedKey::AppliedState(_),
                ..
            })
        )));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    for read in &upload.reconciliation.expected_reads {
        assert_eq!(transaction.get(&read.key).await.unwrap(), read.value);
    }
    for write in &upload.reconciliation.writes {
        write.stage(&transaction).unwrap();
    }
    transaction.commit().await.unwrap();

    for (id, live) in [
        (7, true),
        (8, false),
        (9, true),
        (10, false),
        (11, false),
        (12, true),
    ] {
        let state =
            decode_text_entity_state(&db.get(state_key(id)).await.unwrap().unwrap()).unwrap();
        assert_eq!(
            (state.live, state.logical_version),
            (live, TextLogicalVersion::initial())
        );
        let applied = crate::encoding::v2::values::decode_applied_state(
            &db.get(scoped_index_key(
                scope,
                ScopedKey::AppliedState(IndexEntityStateKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    entity: reconciliation_entity(id),
                }),
            ))
            .await
            .unwrap()
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            applied.state,
            AppliedFamilyState::Text(Some((tenant_partition("a"), TextLogicalVersion::initial()))),
            "entity {id} keeps its source-scan applied state"
        );
        let contribution = super::super::statistics::load_entity_contribution(
            &db,
            scope,
            operation.index_id(),
            operation.generation(),
            reconciliation_entity(id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            matches!(
                contribution,
                work::TextStatisticsContribution::Present { .. }
            ),
            live,
            "entity {id} marker follows its document"
        );
    }

    // The corpus now equals a fresh scan of the documents the build holds.
    let reference = scanned_generation(
        "text-driver-partition-reconciliation-reference",
        scope,
        &operation,
        &[
            (7, "a", Some("stable alpha")),
            (9, "a", Some("after gamma delta")),
            (12, "a", Some("order same")),
        ],
    )
    .await;
    assert_eq!(
        corpus_and_term_rows(&db, scope, &operation).await,
        corpus_and_term_rows(&reference, scope, &operation).await
    );
    reference.close().await.unwrap();
    db.close().await.unwrap();
}

/// Per-document publication limits: `batch`, a publication input allowance
/// of `max_input` bytes, and a `max_page`-byte manifest page.
fn mutation_limits(
    batch: SearchIndexBatchLimits,
    max_input: u64,
    max_page: u64,
) -> crate::config::ActiveTextMutationLimits {
    crate::config::ActiveTextMutationLimits::unchecked_for_tests(
        batch,
        NonZeroU64::new(max_input).unwrap(),
        crate::config::SearchIndexBackfillLimits::default()
            .text_compaction()
            .max_output_blob_bytes(),
        NonZeroU64::new(max_page).unwrap(),
    )
}

/// The admission footprint of `text` as entity `id`'s document in tenant `a`.
fn document_footprint(
    scope: DataScope,
    operation: &IndexOperationRecord,
    id: u64,
    text: &str,
) -> super::super::active_batch::TextDocumentFootprint {
    let (_, totals) = crate::search::text::analyze_text_within_budget(
        tenant_definition().analyzer(),
        text,
        &mut crate::search::text::TextAnalysisMemoryBudget::new(
            document_limits().max_input_bytes(),
        ),
    )
    .unwrap();
    super::super::active_batch::TextDocumentFootprint::measure(
        scope,
        &tenant_record(operation),
        reconciliation_entity(id),
        &tenant_partition("a"),
        totals,
    )
    .unwrap()
}

/// A footprint's `[page, operations, output, input]` measures, read back from
/// the rejections of limits that leave only that resource no allowance.
fn admission_measures(footprint: super::super::active_batch::TextDocumentFootprint) -> [u64; 4] {
    use crate::error::ActiveTextMutationResource as Resource;
    let observed = |expected: Resource, limits| match footprint.admit(limits) {
        Err(HelixDbError::ActiveTextMutationLimitExceeded {
            resource, observed, ..
        }) if resource == expected => observed,
        other => panic!("{expected:?} must be the only exhausted allowance: {other:?}"),
    };
    let unbounded = || batch_limits(1, u64::MAX, u64::MAX, u64::MAX);
    let page = observed(
        Resource::ManifestPageBytes,
        mutation_limits(unbounded(), u64::MAX, 1),
    );
    [
        page,
        observed(
            Resource::OutputOperations,
            mutation_limits(batch_limits(1, u64::MAX, 1, u64::MAX), u64::MAX, page),
        ),
        observed(
            Resource::OutputBytes,
            mutation_limits(
                batch_limits(1, u64::MAX, u64::MAX, page + 1),
                u64::MAX,
                page,
            ),
        ),
        {
            // Analysis is charged the whole input budget and checked first, so
            // the input share can only be exhausted alone above that charge.
            let analysis = observed(
                Resource::AnalysisBytes,
                mutation_limits(unbounded(), 1, page),
            );
            observed(
                Resource::InputBytes,
                mutation_limits(unbounded(), analysis.max(2 * page + 1), page),
            )
        },
    ]
}

/// Accounted text of the lone-reconciliation fixture.
const LONE_BEFORE: &str = "qqaa qqbb qqcc qqdd";
/// Current text of entity 7; entity 8 is deleted.
const LONE_AFTER: &str = "zzaa zzbb zzcc zzdd zzee";

/// A scanned generation whose entity 7 was re-texted and entity 8 deleted.
async fn lone_reconciliation_generation(
    database: &'static str,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Db {
    let db = scanned_generation(
        database,
        scope,
        operation,
        &[(7, "a", Some(LONE_BEFORE)), (8, "a", Some(LONE_BEFORE))],
    )
    .await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    put_reconciliation_rows(
        &transaction,
        scope,
        &[(7, "a", Some(LONE_AFTER)), (8, "a", None)],
    );
    transaction.commit().await.unwrap();
    db
}

/// A lone changed entity fits its step whenever both of its documents were
/// admitted, so a text change during a build never blocks it.
///
/// Every allowance is exactly the one that admits the larger document (the
/// input allowance is its input share or its lone split, whichever is larger),
/// and the source input is exactly the manifest root, entity state, and graph
/// row that a fresh build of the current row reads.
#[tokio::test]
async fn partition_scan_admits_a_lone_changed_entity_by_document_admission() {
    let scope = DataScope::LegacyUnscoped;
    let operation = operation();
    let definition = tenant_definition();
    let db =
        lone_reconciliation_generation("text-driver-partition-lone-admission", scope, &operation)
            .await;
    let footprints =
        [LONE_BEFORE, LONE_AFTER].map(|text| document_footprint(scope, &operation, 7, text));
    let [before, after] = footprints.map(admission_measures);
    let page = before[0];
    let largest = |resource: usize| before[resource].max(after[resource]);

    let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let row_bytes = |key: &Bytes, value: Option<Bytes>| {
        u64::try_from(key.len() + value.map_or(0, |value| value.len())).unwrap()
    };
    let state = tenant_state_key(scope, &operation, "a", 7);
    let graph = authoritative_property_key(scope, reconciliation_entity(7));
    let source = prepare_empty_manifest_root(&snapshot, scope, &operation, tenant_partition("a"))
        .await
        .unwrap()
        .input_bytes()
        + row_bytes(&state, snapshot.get(&state).await.unwrap())
        + row_bytes(&graph, snapshot.get(&graph).await.unwrap());
    let batch = batch_limits(8, source, 2 * largest(1), 2 * largest(2) + page);
    let split = footprints
        .map(|footprint| {
            crate::search::text::single_document_split_bytes(footprint.analysis_bytes())
        })
        .into_iter()
        .max()
        .unwrap();
    let limits = mutation_limits(batch, (2 * largest(3) + 2 * page).max(split), page);
    for footprint in footprints {
        footprint.admit(limits).unwrap();
    }

    let progress = initial_partition_scan(&operation, scope, OperationCounters::default()).unwrap();
    let scan = |progress: SourceScanProgress| {
        let snapshot = &snapshot;
        let operation = &operation;
        let definition = &definition;
        async move {
            scan_partition_documents(
                snapshot,
                scope,
                operation,
                definition,
                &progress,
                batch,
                limits,
                IndexLifecycleScanTuning::default(),
            )
            .await
            .unwrap()
        }
    };
    let PartitionScanSelection::Upload(retexted) = scan(progress.clone()).await else {
        panic!("a lone re-texted entity uploads its current document")
    };
    assert_eq!(
        retexted
            .documents
            .iter()
            .map(|document| (document.entity_id, document.text.as_str()))
            .collect::<Vec<_>>(),
        [(7, LONE_AFTER)]
    );
    assert_eq!(retexted.completed_cursor.as_bytes(), &state);
    assert!(!retexted.reconciliation.writes.is_empty());
    let PartitionScanSelection::Repository {
        result: IndexOperationStepResult::Progressed(_),
        ..
    } = scan(SourceScanProgress {
        cursor: Some(retexted.completed_cursor),
        ..progress
    })
    .await
    else {
        panic!("a lone deleted entity retires")
    };
    drop(snapshot);
    db.close().await.unwrap();
}

/// An upload step admits its artifact row within the batch output limits.
#[tokio::test]
async fn partition_scan_reserves_the_upload_artifact_row() {
    let scope = DataScope::LegacyUnscoped;
    let operation = operation();
    let definition = tenant_definition();
    let db = lone_reconciliation_generation(
        "text-driver-partition-artifact-reservation",
        scope,
        &operation,
    )
    .await;
    let snapshot = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let progress = initial_partition_scan(&operation, scope, OperationCounters::default()).unwrap();
    let scan = |max_output_operations, max_output_bytes| {
        let snapshot = &snapshot;
        let operation = &operation;
        let definition = &definition;
        let progress = &progress;
        async move {
            scan_partition_documents(
                snapshot,
                scope,
                operation,
                definition,
                progress,
                batch_limits(1, u64::MAX, max_output_operations, max_output_bytes),
                document_limits(),
                IndexLifecycleScanTuning::default(),
            )
            .await
            .unwrap()
        }
    };
    let PartitionScanSelection::Upload(upload) = scan(u64::MAX, u64::MAX).await else {
        panic!("the re-texted entity uploads")
    };
    let operations = u64::try_from(upload.reconciliation.writes.len()).unwrap();
    let output = upload
        .reconciliation
        .writes
        .iter()
        .map(|write| write.key.len() + write.value.as_ref().map_or(0, Bytes::len))
        .sum::<usize>();
    let output = u64::try_from(output).unwrap()
        + build_artifact_row_bytes(scope, &operation, &tenant_partition("a")).unwrap();
    for (max_output_operations, max_output_bytes) in
        [(operations + 1, u64::MAX), (u64::MAX, output)]
    {
        assert!(matches!(
            scan(max_output_operations, max_output_bytes).await,
            PartitionScanSelection::Upload(_)
        ));
    }
    for (max_output_operations, max_output_bytes, admitted) in [
        (operations, u64::MAX, operations + 1),
        (u64::MAX, output - 1, output),
    ] {
        assert!(matches!(
            scan(max_output_operations, max_output_bytes).await,
            PartitionScanSelection::Blocked(
                IndexOperationBlocker::ManifestLimit { observed, .. },
                Some(_),
            ) if observed == admitted
        ));
    }
    drop(snapshot);
    db.close().await.unwrap();
}

#[tokio::test]
async fn compaction_preparation_requires_a_claim_and_exhaustion_advances_exactly_once() {
    let scope = DataScope::LegacyUnscoped;
    let definition = definition();
    let operation_id = IndexOperationId::new_v4();
    let progress = PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    };
    let unclaimed = IndexOperationRecord::try_new(
        operation_id,
        IndexId::initial(),
        definition.identity(),
        IndexGenerationId::initial(),
        IndexRevision::initial(),
        IndexOperationRevision::initial(),
        IndexOperationKind::Build,
        IndexOperationFamily::Text,
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::Compact(progress.clone()),
        )),
        0,
        IndexOperationExecutionState::Queued {
            not_before_unix_millis: None,
        },
    )
    .unwrap();
    let claimed = unclaimed
        .clone()
        .claim(OperationClaim {
            writer_epoch: WriterEpoch::from_bytes([9; 16]).unwrap(),
            sequence: ClaimSequence::new(1).unwrap(),
        })
        .unwrap();
    let object_store = Arc::new(ControlledHeadStore::default());
    let runtime = TextStorageRuntime {
        object_store: object_store.clone(),
        db_path: "text-driver-empty-compaction".to_string(),
        compaction_limits: crate::config::SearchIndexBackfillLimits::default().text_compaction(),
    };
    let db = Db::open("text-driver-empty-compaction", Arc::new(InMemory::new()))
        .await
        .unwrap();
    assert!(prepare_compaction_step(
        &db,
        scope,
        &unclaimed,
        &progress,
        limits(u64::MAX),
        &runtime,
    )
    .await
    .is_err());

    let dynamic = ValidatedDynamicIndexDefinition::Text(definition.clone());
    let record = IndexRecordV2::building(
        IndexId::initial(),
        dynamic,
        IndexRevision::initial(),
        crate::index_lifecycle::PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
        operation_id,
    )
    .unwrap();
    db.put(
        scoped_index_key(scope, ScopedKey::index_record(record.identity().clone())),
        crate::encoding::v2::values::encode_index_record(&record),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::Repository(prepared) =
        prepare_compaction_step(&db, scope, &claimed, &progress, limits(u64::MAX), &runtime)
            .await
            .unwrap()
    else {
        panic!("an empty compaction lane advances through a repository step")
    };
    assert!(prepared.expected_reads.is_empty());
    assert!(prepared.writes.is_empty());
    assert!(matches!(
        prepared.result,
        IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::PrepareManifests(PrefixScanProgress {
                cursor: None,
                ..
            }))
        ))
    ));

    let partition = TextPartition::Unpartitioned;
    let artifact_owner = TextBuildArtifactKey {
        root: TextManifestRootKey {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            partition: partition.fingerprint(),
        },
        ordinal: 0,
    };
    let artifact_key = scoped_index_key(scope, ScopedKey::TextBuildArtifact(artifact_owner));
    let split = work::SplitRef::try_new(
        work::BlobRef::new([3; 32], 1),
        0,
        1,
        0,
        1,
        work::SplitPruning::Unavailable,
    )
    .unwrap();
    db.put(
        artifact_key,
        crate::encoding::v2::values::encode_build_artifact(&work::TextBuildArtifactValue {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            partition,
            artifact_ordinal: 0,
            split,
        }),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::Repository(prepared) =
        prepare_compaction_step(&db, scope, &claimed, &progress, limits(u64::MAX), &runtime)
            .await
            .unwrap()
    else {
        panic!("an undersized compaction group advances its cursor")
    };
    assert_eq!(prepared.expected_reads.len(), 1);
    assert!(prepared.writes.is_empty());
    assert!(matches!(
        prepared.result,
        IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::Compact(PrefixScanProgress {
                cursor: Some(_),
                ..
            }))
        ))
    ));

    let runtime_definition = definition.to_runtime();
    let first_runtime_split = crate::search::text::persist_documents_as_split(
        &runtime.object_store,
        &runtime.db_path,
        &runtime_definition,
        &[
            crate::search::text::TextDocumentInput::new(7, "old searchable document")
                .with_logical_version(1),
        ],
    )
    .await
    .unwrap()
    .expect("first compaction input split is non-empty");
    let second_runtime_split = crate::search::text::persist_documents_as_split(
        &runtime.object_store,
        &runtime.db_path,
        &runtime_definition,
        &[
            crate::search::text::TextDocumentInput::new(7, "current searchable document")
                .with_logical_version(2),
        ],
    )
    .await
    .unwrap()
    .expect("second compaction input split is non-empty");
    let first_split = work::SplitRef::try_new(
        work::BlobRef::new(
            first_runtime_split.blob.sha256,
            first_runtime_split.blob.size_bytes,
        ),
        first_runtime_split.footer_offset,
        first_runtime_split.footer_len,
        first_runtime_split.hotcache_len,
        first_runtime_split.total_size_bytes,
        work::SplitPruning::Unavailable,
    )
    .unwrap();
    let second_split = work::SplitRef::try_new(
        work::BlobRef::new(
            second_runtime_split.blob.sha256,
            second_runtime_split.blob.size_bytes,
        ),
        second_runtime_split.footer_offset,
        second_runtime_split.footer_len,
        second_runtime_split.hotcache_len,
        second_runtime_split.total_size_bytes,
        work::SplitPruning::Unavailable,
    )
    .unwrap();
    for (ordinal, split) in [(0, first_split), (1, second_split)] {
        let owner = TextBuildArtifactKey {
            root: TextManifestRootKey {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                partition: TextPartition::Unpartitioned.fingerprint(),
            },
            ordinal,
        };
        db.put(
            scoped_index_key(scope, ScopedKey::TextBuildArtifact(owner)),
            crate::encoding::v2::values::encode_build_artifact(&work::TextBuildArtifactValue {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                partition: TextPartition::Unpartitioned,
                artifact_ordinal: ordinal,
                split,
            }),
        )
        .await
        .unwrap();
    }
    let entity = crate::encoding::v2::keys::IndexEntity {
        kind: crate::index_lifecycle::IndexElementKind::Node,
        id: crate::index_lifecycle::IndexEntityId::new(7),
    };
    db.put(
        scoped_index_key(
            scope,
            ScopedKey::TextEntityState(crate::encoding::v2::keys::TextEntityStateKey {
                root: TextManifestRootKey {
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    partition: TextPartition::Unpartitioned.fingerprint(),
                },
                entity,
            }),
        ),
        crate::encoding::v2::values::encode_text_entity_state(&work::TextEntityStateValue {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            partition: TextPartition::Unpartitioned,
            entity_kind: crate::index_lifecycle::IndexElementKind::Node,
            entity_id: entity.id,
            logical_version: crate::index_lifecycle::TextLogicalVersion::new(2).unwrap(),
            live: true,
        }),
    )
    .await
    .unwrap();
    object_store.state.lock().unwrap().fail_put = true;
    assert!(matches!(
        prepare_compaction_step(&db, scope, &claimed, &progress, limits(u64::MAX), &runtime).await,
        Err(HelixDbError::ObjectStore(_))
    ));
    object_store.state.lock().unwrap().fail_put = false;
    let PreparedTextOperationStep::CompactionUpload(prepared) =
        prepare_compaction_step(&db, scope, &claimed, &progress, limits(u64::MAX), &runtime)
            .await
            .unwrap()
    else {
        panic!("two live-versioned splits produce one exact compaction upload")
    };
    assert_eq!(prepared.retired_artifact_keys.len(), 2);
    assert!(prepared.uploaded_bytes > 0);
    assert!(prepared.expected_reads.len() >= 3);
    db.close().await.unwrap();
}
