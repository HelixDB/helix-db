//! Production contracts for the retained vector build planning cache.
//!
//! This feature-gated child of the vector lifecycle driver exercises the
//! driver-owned [`VectorBuildCache`] against canonical operation and index
//! records written by the production lifecycle entry points. The sessions it
//! retains hold only fixture SimHashes that mark reuse, so no vector row
//! family is written. The budget contracts then bound retained build and
//! publication sessions in count, trim them to their max-min fair share off
//! the executor, and shrink checked-out sessions to their rebound share.

use std::num::NonZeroU64;

use slatedb::object_store::memory::InMemory;

use super::*;
use crate::config::VectorIndexDefinition;
use crate::encoding::v2::keys::NodePropertyKey;
use crate::index_lifecycle::lifecycle::{create_index_operation, InitialBuildProgress};
use crate::index_lifecycle::{IndexDdlReceipt, IndexScopeGates};
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
            OfferedVectorBuild::for_tests(
                &cache,
                VectorPlanningCheckpoint::Build(checkpoint.clone()),
                Box::new(VectorBuildSession::<Euclidean>::with_test_simhashes(
                    NonZeroU64::new(BUDGET).expect("marker budget is positive"),
                    u64::try_from(MARKED).expect("marker count fits u64"),
                )),
            ),
        )))
    };
    let retained_checkpoints = || {
        cache
            .retained
            .try_lock()
            .expect("no commit is trimming the retained sessions")
            .iter()
            .filter_map(|retained| retained.build_checkpoint().cloned())
            .collect::<Vec<_>>()
    };

    let execution = VectorStepResult::ordinary(IndexOperationStepResult::Progressed(
        first.progress().clone(),
    ))
    .retaining(
        &first,
        &first_record,
        cache.checkout_fresh::<Euclidean>().await,
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
                cache.checkout_fresh::<Euclidean>().await,
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

    budget_contracts(&first, &first_record).await;
}

/// Bytes one retained session of `simhashes` fixture SimHashes charges: its
/// namespace plus one SimHash entry each.
const fn session_bytes(simhashes: usize) -> usize {
    4_096 + simhashes * (104 + core::mem::size_of::<u64>())
}

/// Offers a build session of `simhashes` fixture SimHashes, created under a
/// 1 MiB budget, as committed at `checkpoint`.
fn build_offer(
    cache: &VectorBuildCache,
    checkpoint: &VectorBuildCheckpoint,
    simhashes: u64,
) -> Option<CommittedStepState> {
    Some(CommittedStepState::VectorBuild(Box::new(
        OfferedVectorBuild::for_tests(
            cache,
            VectorPlanningCheckpoint::Build(checkpoint.clone()),
            Box::new(VectorBuildSession::<Euclidean>::with_test_simhashes(
                NonZeroU64::new(1 << 20).expect("session budget is positive"),
                simhashes,
            )),
        ),
    )))
}

/// Retains a publication session of `simhashes` fixture SimHashes for `target`.
async fn retain_publication(
    cache: &VectorBuildCache,
    gates: &IndexScopeGates,
    target: QueueTarget,
    revision: crate::index_lifecycle::IndexRevision,
    simhashes: u64,
    backlog: PublicationBacklog,
) {
    let permit = gates.publication_permit(target).await;
    let offered = OfferedVectorBuild::for_tests(
        cache,
        VectorPlanningCheckpoint::Publication(VectorPublicationCheckpoint {
            target,
            index_record_revision: revision,
            commit: NonZeroU64::MIN,
        }),
        Box::new(VectorBuildSession::<Euclidean>::with_test_simhashes(
            NonZeroU64::new(1 << 20).expect("session budget is positive"),
            simhashes,
        )),
    );
    cache.retain_publication(&permit, offered, backlog).await;
}

/// Returns the queue target of fixture index `index`.
fn publication_target(index: u64) -> QueueTarget {
    QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(index).expect("fixture index id is positive"),
        IndexGenerationId::initial(),
    )
}

/// Returns the bytes of every retained session, least recently committed first.
fn retained_sizes(cache: &VectorBuildCache) -> Vec<usize> {
    cache
        .retained
        .try_lock()
        .expect("no rebalance is trimming the retained sessions")
        .iter()
        .map(|retained| retained.session.retained_bytes())
        .collect()
}

/// Returns the targets of every retained publication session, none of which
/// reports a build checkpoint.
fn retained_publications(cache: &VectorBuildCache) -> Vec<QueueTarget> {
    cache
        .retained
        .try_lock()
        .expect("no rebalance is trimming the retained sessions")
        .iter()
        .filter_map(|retained| match &retained.checkpoint {
            VectorPlanningCheckpoint::Publication(checkpoint) => {
                assert!(retained.build_checkpoint().is_none());
                Some(checkpoint.target)
            }
            VectorPlanningCheckpoint::Build(_) => None,
        })
        .collect()
}

/// Proves the shared planning budget bounds, trims, and rebinds sessions.
///
/// Builds beyond the retained-build bound evict the least recently committed
/// build. Publication commits beyond their bound evict the oldest drained
/// session, or are dropped when every retained target still has work. A
/// commit that leaves sessions over their max-min fair share trims them on
/// the blocking pool, and a drained session left no spare budget is dropped.
/// A checkout shrinks a reused session to the class caps of its share, and a
/// checked-out session shrinks at its next entity boundary when its share
/// fell, keeping everything when it rose. A shrink that panics on the
/// blocking pool resumes the panic in the caller, and one that fails yields
/// no session.
async fn budget_contracts(operation: &IndexOperationRecord, record: &IndexRecordV2) {
    let first = VectorBuildCheckpoint::new(operation, record, operation.progress().clone());
    let another = || VectorBuildCheckpoint {
        operation_id: IndexOperationId::new_v4(),
        ..first.clone()
    };

    // The retained-build bound evicts the least recently committed build.
    let cache = VectorBuildCache::new(NonZeroU64::new(1 << 20).expect("budget is positive"));
    let checkpoints = core::iter::once(first.clone())
        .chain((0..MAX_RETAINED_VECTOR_BUILDS).map(|_| another()))
        .collect::<Vec<_>>();
    for checkpoint in &checkpoints {
        cache
            .after_commit(
                checkpoint.operation_id,
                CommittedOperationStep::Progressed,
                build_offer(&cache, checkpoint, 0),
            )
            .await;
    }
    assert_eq!(
        cache
            .retained
            .try_lock()
            .expect("no rebalance is trimming the retained sessions")
            .iter()
            .filter_map(|retained| retained.build_checkpoint().cloned())
            .collect::<Vec<_>>(),
        checkpoints[1..].to_vec()
    );

    // Past the publication bound the oldest drained session is evicted; with
    // every retained target pending, the newest session is dropped instead.
    let gates = IndexScopeGates::default();
    let cache = VectorBuildCache::new(NonZeroU64::new(1 << 20).expect("budget is positive"));
    let bound = u64::try_from(MAX_RETAINED_PUBLICATIONS).expect("bound fits u64");
    for index in 1..bound {
        retain_publication(
            &cache,
            &gates,
            publication_target(index),
            record.revision(),
            0,
            PublicationBacklog::Pending,
        )
        .await;
    }
    for (index, backlog) in [
        (bound, PublicationBacklog::Drained),
        (bound + 1, PublicationBacklog::Pending),
        (bound + 2, PublicationBacklog::Pending),
    ] {
        retain_publication(
            &cache,
            &gates,
            publication_target(index),
            record.revision(),
            0,
            backlog,
        )
        .await;
    }
    assert_eq!(
        retained_publications(&cache),
        (1..bound)
            .chain([bound + 1])
            .map(publication_target)
            .collect::<Vec<_>>()
    );

    // Two builds over half the budget each are trimmed to their fair share,
    // and a drained session no longer fits the budget they leave.
    const TRIMMED_BUDGET: usize = 64 * 1024;
    let cache = VectorBuildCache::new(
        NonZeroU64::new(u64::try_from(TRIMMED_BUDGET).expect("budget fits u64"))
            .expect("budget is positive"),
    );
    let second = another();
    for checkpoint in [&first, &second] {
        cache
            .after_commit(
                checkpoint.operation_id,
                CommittedOperationStep::Progressed,
                build_offer(&cache, checkpoint, 400),
            )
            .await;
    }
    let sizes = retained_sizes(&cache);
    assert_eq!(sizes.len(), 2);
    assert!(
        sizes
            .iter()
            .all(|size| *size <= TRIMMED_BUDGET / 2 && *size < session_bytes(400)),
        "{sizes:?}"
    );
    retain_publication(
        &cache,
        &gates,
        publication_target(1),
        record.revision(),
        100,
        PublicationBacklog::Drained,
    )
    .await;
    assert!(retained_sizes(&cache).iter().sum::<usize>() <= TRIMMED_BUDGET);
    assert!(
        retained_sizes(&cache)
            .get(2)
            .is_none_or(|drained| *drained < session_bytes(100)),
        "a drained session keeps only spare budget"
    );

    // A reused session is shrunk to the class caps of its share at checkout.
    const CHECKOUT_BUDGET: u64 = 128 * 1024;
    let cache = VectorBuildCache::new(NonZeroU64::new(CHECKOUT_BUDGET).expect("positive"));
    cache
        .after_commit(
            first.operation_id,
            CommittedOperationStep::Progressed,
            build_offer(&cache, &first, 400),
        )
        .await;
    assert_eq!(retained_sizes(&cache), vec![session_bytes(400)]);
    let reused = cache.checkout::<Euclidean>(&first).await;
    assert!(
        (1..400).contains(&reused.simhash_count()),
        "{}",
        reused.simhash_count()
    );
    drop(reused);

    // A checked-out session shrinks at its next entity when its share falls
    // and keeps every entry when it rises again.
    let mut planning = cache.checkout_fresh::<Euclidean>().await;
    planning.session = VectorBuildSession::with_test_simhashes(
        NonZeroU64::new(1 << 20).expect("session budget is positive"),
        100,
    );
    let joining = cache.checkout_fresh::<Euclidean>().await;
    planning.rebind().await;
    let halved = planning.simhash_count();
    assert!((1..100).contains(&halved), "{halved}");
    assert_eq!(planning.bound.get(), CHECKOUT_BUDGET / 2);
    drop(joining);
    retain_publication(
        &cache,
        &gates,
        publication_target(2),
        record.revision(),
        0,
        PublicationBacklog::Pending,
    )
    .await;
    planning.rebind().await;
    assert!(planning.bound.get() > CHECKOUT_BUDGET / 2);
    assert_eq!(planning.simhash_count(), halved);
    drop(planning);

    // Shrinks run on the blocking pool: a failure yields no session and a
    // panic resumes in the caller.
    assert!(
        shrink_off_executor(0_u8, |_| Err(corruption("contract shrink fails")))
            .await
            .is_none()
    );
    let panicked = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
        shrink_off_executor(0_u8, |_| panic!("contract shrink panics")),
    ))
    .await;
    assert!(panicked.is_err(), "a panicking shrink resumes its panic");
}
