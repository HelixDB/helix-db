//! Production contracts for the retained vector build planning cache.
//!
//! This feature-gated child of the vector lifecycle driver exercises the
//! driver-owned [`VectorBuildCache`] against canonical operation and index
//! records written by the production lifecycle entry points. The sessions it
//! retains hold only fixture SimHashes that mark reuse, so no vector row
//! family is written.

use std::num::NonZeroU64;

use slatedb::object_store::memory::InMemory;

use super::*;
use crate::config::VectorIndexDefinition;
use crate::encoding::v2::keys::NodePropertyKey;
use crate::index_lifecycle::lifecycle::{create_index_operation, InitialBuildProgress};
use crate::index_lifecycle::IndexDdlReceipt;
use crate::migrations::startup::bootstrap_writer;

type Euclidean = vector::distance::Euclidean;

/// Creates one vector build and returns its canonical operation and index records.
async fn create_build(
    db: &Db,
    scope: DataScope,
    property: &str,
) -> (IndexOperationRecord, IndexRecordV2) {
    let definition = ValidatedDynamicIndexDefinition::Vector(
        ValidatedVectorIndexDefinition::try_from_runtime(
            &VectorIndexDefinition::new_node(
                "Document",
                property,
                3,
                VectorDistanceMetric::Euclidean,
            )
            .expect("contract vector definition validates"),
        )
        .expect("contract V2 vector definition validates"),
    );
    let upper_bound = IndexCursor::try_new(
        DataKey::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(1)),
        }
        .to_bytes(),
    )
    .expect("source key is a valid cursor");
    let IndexDdlReceipt::Accepted { operation_id, .. } = create_index_operation(
        db,
        scope,
        definition.clone(),
        helix_planner::ir::IndexCreateMode::ErrorIfExists,
        InitialBuildProgress::vector(upper_bound),
    )
    .await
    .expect("contract vector build is enqueued") else {
        panic!("a new vector definition enqueues a build");
    };
    let operation = crate::encoding::v2::values::decode_operation_record(
        &db.get(crate::index_lifecycle::outbox::scoped_operation_key(
            scope,
            operation_id,
        ))
        .await
        .expect("contract operation is readable")
        .expect("contract operation exists"),
    )
    .expect("contract operation decodes");
    let record = decode_index_record(
        &db.get(scoped_index_key(
            scope,
            ScopedKey::index_record(definition.identity()),
        ))
        .await
        .expect("contract index record is readable")
        .expect("contract index record exists"),
    )
    .expect("contract index record decodes");
    (operation, record)
}

/// Checks out `checkpoint`'s session and returns the SimHashes it retains.
async fn checked_out_simhashes<D: Distance>(
    cache: &VectorBuildCache,
    checkpoint: &VectorBuildCheckpoint,
) -> usize {
    cache.checkout::<D>(checkpoint).await.simhash_count()
}

/// Proves the retained cache reuses only the exact committed checkpoint.
///
/// A session is released to the next step only for the operation, index
/// revision, and progress it was committed at, and only for its own metric.
/// Another operation's step or commit leaves it in place, a stale checkpoint
/// or a committed step without state forgets it, and each operation keeps its
/// own session. Only a step progressing to another Scan offers a session.
/// The committed state crosses the outbox boundary with a diagnostic that
/// names it without exposing its rows, and source-scan planning and storage
/// errors return from the step without offering a session.
pub(crate) async fn run() {
    let db = Db::builder(
        "vector-build-cache-production-contracts",
        Arc::new(InMemory::new()),
    )
    .build()
    .await
    .expect("contract database opens");
    bootstrap_writer(&db)
        .await
        .expect("contract database bootstraps");
    let scope = DataScope::LegacyUnscoped;
    let (first, first_record) = create_build(&db, scope, "embedding").await;
    let (second, second_record) = create_build(&db, scope, "other_embedding").await;
    let checkpoint = VectorBuildCheckpoint::new(&first, &first_record, first.progress().clone());
    let other_operation =
        VectorBuildCheckpoint::new(&second, &second_record, second.progress().clone());
    let source_cursor = |entity_id| {
        IndexCursor::try_new(
            DataKey::Data {
                scope,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
            }
            .to_bytes(),
        )
        .expect("source key is a valid cursor")
    };
    let mut advanced = checkpoint.clone();
    advanced.progress = IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
        VectorBuildStage::Scan(SourceScanProgress {
            inclusive_upper_bound: source_cursor(1),
            cursor: Some(source_cursor(1)),
            counters: OperationCounters::default(),
        }),
    ));
    assert_ne!(advanced.progress, checkpoint.progress);

    const BUDGET: u64 = 1 << 20;
    const MARKED: usize = 3;
    let cache = VectorBuildCache::new(NonZeroU64::new(BUDGET).expect("budget is positive"));
    let marked = |checkpoint: &VectorBuildCheckpoint| {
        Some(CommittedStepState::VectorBuild(Box::new(
            RetainedVectorBuild {
                checkpoint: checkpoint.clone(),
                session: Box::new(VectorBuildSession::<Euclidean>::with_test_simhashes(
                    NonZeroU64::new(BUDGET).expect("marker budget is positive"),
                    u64::try_from(MARKED).expect("marker count fits u64"),
                )),
            },
        )))
    };
    let retained_checkpoints = || {
        cache
            .retained
            .try_lock()
            .expect("no commit is trimming the retained sessions")
            .iter()
            .map(|retained| retained.checkpoint.clone())
            .collect::<Vec<_>>()
    };

    let execution = VectorStepResult::ordinary(IndexOperationStepResult::Progressed(
        first.progress().clone(),
    ))
    .retaining(
        &first,
        &first_record,
        VectorBuildSession::<Euclidean>::new(NonZeroU64::new(BUDGET).expect("budget is positive")),
    )
    .into_execution();
    assert!(format!("{execution:?}").contains("CommittedStepState::VectorBuild"));
    let validate = IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
        VectorBuildProgress::Constructing(VectorBuildStage::ValidateDescriptor(
            PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            },
        )),
    ));
    for result in [validate, IndexOperationStepResult::TransientFailure] {
        let execution = VectorStepResult::ordinary(result)
            .retaining(
                &first,
                &first_record,
                VectorBuildSession::<Euclidean>::new(
                    NonZeroU64::new(BUDGET).expect("budget is positive"),
                ),
            )
            .into_execution();
        assert!(
            !format!("{execution:?}").contains("CommittedStepState::VectorBuild"),
            "no later step checks out this session"
        );
    }

    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &checkpoint).await,
        0
    );
    for committed in [
        CommittedOperationStep::Blocked,
        CommittedOperationStep::Completed,
        CommittedOperationStep::TransientFailure,
    ] {
        cache
            .after_commit(first.operation_id(), committed, marked(&checkpoint))
            .await;
        assert!(retained_checkpoints().is_empty(), "{committed:?}");
    }

    cache
        .after_commit(
            first.operation_id(),
            CommittedOperationStep::Progressed,
            marked(&checkpoint),
        )
        .await;
    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &checkpoint).await,
        MARKED
    );
    assert!(retained_checkpoints().is_empty());

    cache
        .after_commit(
            first.operation_id(),
            CommittedOperationStep::Progressed,
            marked(&checkpoint),
        )
        .await;
    assert_eq!(
        checked_out_simhashes::<vector::distance::Cosine>(&cache, &checkpoint).await,
        0
    );

    cache
        .after_commit(
            first.operation_id(),
            CommittedOperationStep::Progressed,
            marked(&checkpoint),
        )
        .await;
    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &other_operation).await,
        0
    );
    assert_eq!(retained_checkpoints(), vec![checkpoint.clone()]);
    cache
        .after_commit(
            second.operation_id(),
            CommittedOperationStep::Progressed,
            None,
        )
        .await;
    assert_eq!(retained_checkpoints(), vec![checkpoint.clone()]);

    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &advanced).await,
        0
    );
    assert!(retained_checkpoints().is_empty());

    cache
        .after_commit(
            first.operation_id(),
            CommittedOperationStep::Progressed,
            marked(&checkpoint),
        )
        .await;
    cache
        .after_commit(
            first.operation_id(),
            CommittedOperationStep::Progressed,
            None,
        )
        .await;
    assert!(retained_checkpoints().is_empty());

    cache
        .after_commit(
            first.operation_id(),
            CommittedOperationStep::Progressed,
            marked(&checkpoint),
        )
        .await;
    cache
        .after_commit(
            second.operation_id(),
            CommittedOperationStep::Progressed,
            marked(&other_operation),
        )
        .await;
    assert_eq!(
        retained_checkpoints(),
        vec![checkpoint.clone(), other_operation.clone()]
    );
    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &other_operation).await,
        MARKED
    );
    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &checkpoint).await,
        MARKED
    );

    // Planning errors cross the step unchanged, before any session is offered.
    let ValidatedDynamicIndexDefinition::Vector(definition) = first_record.definition() else {
        panic!("contract index is a vector index");
    };
    let transaction = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("contract step transaction opens");
    assert!(matches!(
        step_build::<Euclidean>(
            &db,
            &transaction,
            scope,
            &first,
            &first_record,
            definition,
            &VectorBuildStage::Scan(SourceScanProgress {
                inclusive_upper_bound: source_cursor(1),
                cursor: Some(source_cursor(2)),
                counters: OperationCounters::default(),
            }),
            SearchIndexBackfillLimits::default().batch(),
            IndexLifecycleScanTuning::default(),
            Arc::new(vector::SimHasherRegistry::default()),
            crate::batch_reads::BatchReads::Single,
            &cache,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.close().await.expect("contract database closes");
    // A scan that still has source rows to read reaches closed storage.
    assert!(step_build::<Euclidean>(
        &db,
        &transaction,
        scope,
        &first,
        &first_record,
        definition,
        &VectorBuildStage::Scan(SourceScanProgress {
            inclusive_upper_bound: source_cursor(1),
            cursor: None,
            counters: OperationCounters::default(),
        }),
        SearchIndexBackfillLimits::default().batch(),
        IndexLifecycleScanTuning::default(),
        Arc::new(vector::SimHasherRegistry::default()),
        crate::batch_reads::BatchReads::Single,
        &cache,
    )
    .await
    .is_err());
}
