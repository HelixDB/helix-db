//! Production contracts for the retained vector build planning cache.
//!
//! This feature-gated child of the vector lifecycle driver exercises the
//! driver-owned [`VectorBuildCache`] slot against canonical operation and
//! index records written by the production lifecycle entry points. The
//! sessions it retains hold no rows, so no vector row family is written.

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

/// Proves the retained slot reuses only the exact committed checkpoint.
///
/// A session is released to the next step only for the operation, index
/// revision, and progress it was committed at, and only for its own metric.
/// Another operation's step leaves it in place, a stale checkpoint or a
/// committed step without state forgets it, and the newest committed session
/// replaces the slot. The committed state crosses the outbox boundary with a
/// diagnostic that names it without exposing its rows.
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
    let mut advanced = checkpoint.clone();
    advanced.progress = IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
        VectorBuildStage::CatchUp(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        }),
    ));

    const FRESH: u64 = 1 << 20;
    const MARKED: u64 = 4_099;
    let cache = VectorBuildCache::new(NonZeroU64::new(FRESH).expect("budget is positive"));
    let marked = |checkpoint: &VectorBuildCheckpoint| {
        Some(CommittedStepState::VectorBuild(Box::new(
            RetainedVectorBuild {
                checkpoint: checkpoint.clone(),
                session: Box::new(VectorBuildSession::<Euclidean>::new(
                    NonZeroU64::new(MARKED).expect("marker budget is positive"),
                )),
            },
        )))
    };
    let checkout_budget = |checkpoint: &VectorBuildCheckpoint| {
        u64::try_from(cache.checkout::<Euclidean>(checkpoint).max_payload_bytes())
            .expect("session budget fits u64")
    };
    let retained_checkpoint = || {
        cache
            .retained
            .lock()
            .as_ref()
            .map(|retained| retained.checkpoint.clone())
    };

    let execution = VectorStepResult::ordinary(IndexOperationStepResult::Progressed(
        first.progress().clone(),
    ))
    .retaining(
        &first,
        &first_record,
        VectorBuildSession::<Euclidean>::new(NonZeroU64::new(MARKED).expect("budget is positive")),
    )
    .into_execution();
    assert!(format!("{execution:?}").contains("CommittedStepState::VectorBuild"));

    assert_eq!(checkout_budget(&checkpoint), FRESH);
    for committed in [
        CommittedOperationStep::Blocked,
        CommittedOperationStep::Completed,
        CommittedOperationStep::TransientFailure,
    ] {
        cache.after_commit(&first, committed, marked(&checkpoint));
        assert!(retained_checkpoint().is_none(), "{committed:?}");
    }

    cache.after_commit(
        &first,
        CommittedOperationStep::Progressed,
        marked(&checkpoint),
    );
    assert_eq!(checkout_budget(&checkpoint), MARKED);
    assert!(retained_checkpoint().is_none());

    cache.after_commit(
        &first,
        CommittedOperationStep::Progressed,
        marked(&checkpoint),
    );
    assert_eq!(
        u64::try_from(
            cache
                .checkout::<vector::distance::Cosine>(&checkpoint)
                .max_payload_bytes()
        )
        .expect("session budget fits u64"),
        FRESH
    );

    cache.after_commit(
        &first,
        CommittedOperationStep::Progressed,
        marked(&checkpoint),
    );
    assert_eq!(checkout_budget(&other_operation), FRESH);
    assert_eq!(retained_checkpoint(), Some(checkpoint.clone()));
    cache.after_commit(&second, CommittedOperationStep::Progressed, None);
    assert_eq!(retained_checkpoint(), Some(checkpoint.clone()));

    assert_eq!(checkout_budget(&advanced), FRESH);
    assert!(retained_checkpoint().is_none());

    cache.after_commit(
        &first,
        CommittedOperationStep::Progressed,
        marked(&checkpoint),
    );
    cache.after_commit(&first, CommittedOperationStep::Progressed, None);
    assert!(retained_checkpoint().is_none());

    cache.after_commit(
        &first,
        CommittedOperationStep::Progressed,
        marked(&checkpoint),
    );
    cache.after_commit(
        &second,
        CommittedOperationStep::Progressed,
        marked(&other_operation),
    );
    assert_eq!(retained_checkpoint(), Some(other_operation));
    db.close().await.expect("contract database closes");
}
