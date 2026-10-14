//! Production contracts for queued asynchronous vector/text index work.
//!
//! Requires `production-coverage` and `index-lifecycle-testing`. Public
//! contracts open writers with explicit lifecycle scheduling, so queued
//! operations stay pending until a contract publishes them: admission limits,
//! publication counters, lag, restart discovery, retired-generation discard,
//! and hidden-build deferral are observed through the public API alone. The
//! ledger, reconciliation, codec, and fail-closed open contracts run through
//! `db::production_coverage`.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use db::config::{
    DbConfig, IndexOperationQueueTuning, IndexOperationQueueTuningError, QueueLayout,
    SearchIndexBackfillLimits, SearchIndexBatchLimits, TextIndexDefinition, VectorIndexDefinition,
};
use db::encoding::v2::keys::scope::DataScope;
use db::error::{HelixDbError, IndexBackpressureResource, IndexOperationBatchResource};
use db::index_lifecycle::{
    IndexDdlReceipt, IndexOperationBlockerCode, IndexOperationId, IndexOperationStatus,
    ValidatedDynamicIndexDefinition,
};
use db::index_lifecycle_testing::{
    LifecycleTestController, LifecycleTestScheduling, LifecycleWorkTarget,
};
use db::query_service::{HelixQueryService, QueryFailureClass};
use db::search::vector::VectorDistanceMetric;
use db::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken, PublicationLagHistogram};
use helix_ast::error_code::QueryErrorCode;
use helix_ast::graph::NodeRef;
use helix_ast::query::{QueryRequest, SearchConsistency};
use helix_ast::value::PropertyInput;
use helix_ast::{batch, traversal};
use helix_metrics::query::QueryErrorType;
use helix_planner::{context, ir};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

const LABEL: &str = "Doc";
const BODY: &str = "body";
const EMBEDDING: &str = "embedding";
const OPERATION_TIMEOUT: Duration = Duration::from_secs(120);

/// Proves the ledger's admission and outcome lifecycle, including orderings
/// a writer cannot schedule on demand.
#[test]
fn ledger_admits_atomically_and_settles_every_outcome_once() {
    db::production_coverage::index_operation_queue_ledger_contracts();
}

/// Proves the writer's publisher settles every uncertain enqueue and
/// acknowledgement against a flushed read of the queue.
#[tokio::test]
async fn publication_reconciles_every_uncertain_commit_outcome() {
    db::production_coverage::index_operation_queue_reconciliation_contracts().await;
}

/// Proves stored queue values and merge operands fail closed on every
/// noncanonical shape and compose exactly through SlateDB.
#[tokio::test]
async fn queue_codec_and_merge_algebra_fail_closed() {
    db::production_coverage::index_operation_queue_codec_contracts().await;
}

/// Proves tenant scopes with identical definitions queue, publish, recover,
/// and retire independently.
#[tokio::test]
async fn tenant_scopes_queue_publish_and_recover_independently() {
    Box::pin(db::production_coverage::index_operation_queue_tenant_scope_contracts()).await;
}

/// Proves a reopened writer discovers queues across the whole tenant
/// keyspace, from the first tenant ID to the last.
#[tokio::test]
async fn writer_open_discovers_queues_across_the_tenant_keyspace() {
    Box::pin(db::production_coverage::index_operation_queue_scope_walk_contracts()).await;
}

/// Proves a writer refuses queues its catalog cannot own or read.
#[tokio::test]
async fn writer_open_fails_closed_on_unowned_queues() {
    db::production_coverage::index_operation_queue_recovery_corruption_contracts().await;
}

/// Tuning validates before open and round-trips through the config; the lag
/// histogram and process-wide merge counters are readable public values.
#[test]
fn queue_tuning_lag_histogram_and_merge_counters_are_public_contracts() {
    let defaults = IndexOperationQueueTuning::default();
    assert_eq!(
        (
            defaults.max_retained_bytes().get(),
            defaults.max_members().get(),
            defaults.max_operand_bytes().get(),
            defaults.recovery_sweep_interval(),
            defaults.layout(),
        ),
        (
            1_000_000_000,
            250_000,
            8 * 1024 * 1024,
            Duration::from_secs(1),
            QueueLayout::Map
        )
    );
    let tuning = defaults
        .with_max_retained_bytes(nonzero(4_096))
        .expect("a small retained-byte ceiling is valid")
        .with_max_members(nonzero(7))
        .with_max_operand_bytes(nonzero(512))
        .with_recovery_sweep_interval(Duration::from_millis(250))
        .expect("a nonzero sweep interval is valid");
    assert_eq!(
        (
            tuning.max_retained_bytes().get(),
            tuning.max_members().get(),
            tuning.max_operand_bytes().get(),
            tuning.recovery_sweep_interval(),
        ),
        (4_096, 7, 512, Duration::from_millis(250))
    );
    let error = tuning
        .with_recovery_sweep_interval(Duration::ZERO)
        .expect_err("the sweep must run");
    assert_eq!(
        error,
        IndexOperationQueueTuningError::ZeroRecoverySweepInterval
    );
    assert_eq!(
        error.to_string(),
        "index operation queue recovery sweep interval must be nonzero"
    );
    let largest = IndexOperationQueueTuning::MAX_RETAINED_BYTES;
    assert_eq!(
        tuning
            .with_max_retained_bytes(nonzero(largest))
            .expect("the largest ceiling is valid")
            .max_retained_bytes()
            .get(),
        largest
    );
    let error = tuning
        .with_max_retained_bytes(nonzero(largest + 1))
        .expect_err("one byte more could outgrow a storage value");
    assert_eq!(
        error,
        IndexOperationQueueTuningError::RetainedBytesAboveQueueValueLimit {
            requested: largest + 1
        }
    );
    assert_eq!(
        error.to_string(),
        format!(
            "index operation queue max retained bytes {} exceed the queue value limit {largest}",
            largest + 1
        )
    );
    // Every operation is charged at least the smallest one's charge, so a
    // lower ceiling could admit nothing.
    let smallest = IndexOperationQueueTuning::MIN_RETAINED_BYTES;
    assert_eq!(
        smallest,
        IndexOperationQueueTuning::OPERATION_OVERHEAD_BYTES + 21
    );
    assert_eq!(
        tuning
            .with_max_retained_bytes(nonzero(smallest))
            .expect("the smallest ceiling is valid")
            .max_retained_bytes()
            .get(),
        smallest
    );
    for requested in [1, smallest - 1] {
        let error = tuning
            .with_max_retained_bytes(nonzero(requested))
            .expect_err("a ceiling below one operation's charge admits nothing");
        assert_eq!(
            error,
            IndexOperationQueueTuningError::RetainedBytesBelowOneOperation { requested }
        );
        assert_eq!(
            error.to_string(),
            format!(
                "index operation queue max retained bytes {requested} are below the charge of \
                 the smallest operation {smallest}"
            )
        );
    }
    assert_eq!(
        DbConfig::new()
            .with_index_operation_queue_tuning(tuning)
            .index_operation_queue(),
        tuning
    );

    let mut lag = PublicationLagHistogram::default();
    assert_eq!(lag.quantile_lower_bound(0.5), None);
    for micros in [3, 100, 101, 5_000] {
        lag.record(micros);
    }
    assert_eq!(
        (lag.count(), lag.sum_micros(), lag.max_micros()),
        (4, 5_204, 5_000)
    );
    assert_eq!(
        lag.buckets().collect::<Vec<_>>(),
        [(3, 1), (96, 2), (4_608, 1)]
    );
    assert_eq!(
        [0.0, 0.5, 1.0].map(|q| lag.quantile_lower_bound(q)),
        [Some(3), Some(96), Some(4_608)]
    );
    lag.record(u64::MAX);
    assert_eq!(lag.sum_micros(), u64::MAX, "the sum saturates");

    let merges = db::operation_queue_merge_stats();
    assert!(merges.partial.max_operands <= merges.partial.operands);
    assert!(merges.resolved.max_operands <= merges.resolved.operands);
}

/// A write whose members would exceed the limit is rejected whole and
/// retryable; one that exceeds it on its own is a hard batch error. Both
/// admit again once publication drains, and the drain is timed exactly.
#[tokio::test]
async fn member_backpressure_rejects_whole_writes_until_publication_drains() {
    let store = fixture("queue-members", vec![text_definition()]).await;
    let tuning = IndexOperationQueueTuning::default().with_max_members(nonzero(2));
    let db = Arc::new(
        open(
            "queue-members",
            &store,
            tuning,
            LifecycleTestScheduling::Explicit,
        )
        .await,
    );
    let merges = db::operation_queue_merge_stats();
    db.query(text_write(&["one"])).await.expect("first member");
    db.query(text_write(&["two"]))
        .await
        .expect("the limit itself is admitted");
    let error = db
        .query(text_write(&["three"]))
        .await
        .expect_err("one member too many");
    assert!(
        matches!(
            error,
            HelixDbError::IndexBackpressure {
                resource: IndexBackpressureResource::PendingMembers,
                requested: 3,
                limit: 2,
                ..
            }
        ),
        "{error}"
    );
    assert!(error.is_index_backpressure());
    assert_eq!(error.error_code(), QueryErrorCode::IndexBackpressure);
    assert!(error.to_string().contains("pending_members"), "{error}");

    // Transports classify backpressure as a retryable conflict.
    let error = HelixQueryService::new(Arc::clone(&db))
        .execute_query(text_write(&["three"]))
        .await
        .expect_err("still saturated");
    assert_eq!(error.classify(), QueryFailureClass::Backpressure);
    assert_eq!(
        QueryErrorType::from(error.classify()),
        QueryErrorType::Conflict
    );
    assert_eq!(error.error_code(), QueryErrorCode::IndexBackpressure);

    let error = db
        .query(text_write(&["a", "b", "c"]))
        .await
        .expect_err("three members never fit a limit of two");
    assert!(
        matches!(
            error,
            HelixDbError::IndexOperationBatchTooLarge {
                resource: IndexOperationBatchResource::PendingMembers,
                observed: 3,
                limit: 2,
                ..
            }
        ),
        "{error}"
    );
    assert!(error.is_invalid_input() && !error.is_index_backpressure());
    assert_eq!(
        error.error_code(),
        QueryErrorCode::IndexOperationBatchTooLarge
    );
    assert!(error.to_string().contains("pending_members"), "{error}");
    assert_eq!(document_count(&db).await, 2, "rejected writes rolled back");

    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.pending_operations,
            stats.pending_members,
            stats.committed_operations,
            stats.uncertain_operations,
        ),
        (2, 2, 2, 0)
    );
    assert!(stats.retained_bytes > 0);
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("publication drains"),
        2
    );
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.pending_operations,
            stats.retained_bytes,
            stats.published_operations,
            stats.acknowledged_operations,
            stats.censored_acknowledgements,
        ),
        (0, 0, 2, 2, 0)
    );
    assert!(stats.committed_batches >= 1 && stats.queue_reads >= 1);
    assert!(stats.publication_attempts >= stats.committed_batches);
    let lag = db.index_operation_publication_lag();
    assert_eq!(
        lag.count(),
        stats.acknowledged_operations - stats.censored_acknowledgements
    );
    assert_eq!(lag.buckets().map(|(_, count)| count).sum::<u64>(), 2);
    assert!(lag.sum_micros() >= lag.max_micros());
    assert!(lag
        .quantile_lower_bound(1.0)
        .is_some_and(|floor| floor <= lag.max_micros()));
    let after = db::operation_queue_merge_stats();
    assert!(after.resolved.merges > merges.resolved.merges);
    assert!(after.resolved.input_bytes > merges.resolved.input_bytes);

    db.query(text_write(&["three"]))
        .await
        .expect("publication freed the capacity");
    db.close().await.expect("fixture closes");
}

/// Retained bytes throttle like members; a write whose own operations or
/// operand exceed a ceiling is rejected before commit without leaking
/// capacity, and a reopened writer charges what is still durable. Every
/// operation counts its encoded size plus a fixed overhead.
#[tokio::test]
async fn byte_and_operand_limits_reject_writes_before_commit() {
    let store = fixture("queue-bytes", vec![text_definition()]).await;
    // Two one-letter documents (about 24 encoded bytes each) fit; a third
    // does not.
    let limit = 60 + 2 * IndexOperationQueueTuning::OPERATION_OVERHEAD_BYTES;
    let tuning = IndexOperationQueueTuning::default()
        .with_max_retained_bytes(nonzero(limit))
        .expect("a small retained-byte ceiling is valid");
    let db = open(
        "queue-bytes",
        &store,
        tuning,
        LifecycleTestScheduling::Explicit,
    )
    .await;
    db.query(text_write(&["a"])).await.expect("first document");
    db.query(text_write(&["b"])).await.expect("second document");
    let error = db
        .query(text_write(&["c"]))
        .await
        .expect_err("above the byte limit");
    assert!(
        matches!(
            error,
            HelixDbError::IndexBackpressure {
                resource: IndexBackpressureResource::RetainedBytes,
                requested,
                limit: refused_at,
                ..
            } if requested > limit && refused_at == limit
        ),
        "{error}"
    );
    assert!(error.to_string().contains("retained_bytes"), "{error}");
    let oversized = "x".repeat(usize::try_from(limit).expect("a small limit"));
    let error = db
        .query(text_write(&[&oversized]))
        .await
        .expect_err("one document above the byte limit");
    assert!(
        matches!(
            error,
            HelixDbError::IndexOperationBatchTooLarge {
                resource: IndexOperationBatchResource::RetainedBytes,
                observed,
                limit: refused_at,
                ..
            } if observed > limit && refused_at == limit
        ),
        "{error}"
    );
    assert!(error.to_string().contains("retained_bytes"), "{error}");
    assert_eq!(document_count(&db).await, 2);
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("publication drains"),
        2
    );
    db.query(text_write(&["c"]))
        .await
        .expect("publication freed the bytes");
    db.close().await.expect("fixture closes");

    let tuning = IndexOperationQueueTuning::default().with_max_operand_bytes(nonzero(64));
    let db = open(
        "queue-bytes",
        &store,
        tuning,
        LifecycleTestScheduling::Explicit,
    )
    .await;
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.pending_operations,
            stats.discovered_operations,
            stats.committed_operations,
        ),
        (1, 1, 0),
        "reopen charges the unpublished write"
    );
    let error = db
        .query(text_write(&[&"x".repeat(128)]))
        .await
        .expect_err("operand above its ceiling");
    assert!(
        matches!(
            error,
            HelixDbError::IndexOperationBatchTooLarge {
                resource: IndexOperationBatchResource::OperandBytes,
                observed,
                limit: 64,
                ..
            } if observed > 64
        ),
        "{error}"
    );
    assert!(error.to_string().contains("operand_bytes"), "{error}");
    assert_eq!(document_count(&db).await, 3);
    assert_eq!(db.index_operation_queue_stats().pending_operations, 1);
    db.query(text_write(&["small"]))
        .await
        .expect("small operands still commit");
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("publication drains"),
        2
    );
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.acknowledged_operations,
            stats.censored_acknowledgements
        ),
        (2, 1),
        "only the operation committed through this handle is timed"
    );
    assert_eq!(db.index_operation_publication_lag().count(), 1);
    db.close().await.expect("fixture closes");
}

/// A reopened writer charges every durable queue, including one whose
/// generation was dropped and replaced while its operations were pending,
/// and the publisher discards the retired queue instead of publishing it.
#[tokio::test]
async fn reopening_rediscovers_live_and_retired_queues() {
    let store = fixture(
        "queue-rediscovery",
        vec![vector_definition(), text_definition()],
    )
    .await;
    let tuning = IndexOperationQueueTuning::default();
    let db = open(
        "queue-rediscovery",
        &store,
        tuning,
        LifecycleTestScheduling::Explicit,
    )
    .await;
    for index in 0..3_u8 {
        db.query(QueryRequest::write(batch::write_batch().var_as(
            "created",
            traversal::g().add_n(
                LABEL,
                vec![
                    (EMBEDDING, PropertyInput::from(vec![f32::from(index), 1.0])),
                    (BODY, PropertyInput::from(format!("document {index}"))),
                ],
            ),
        )))
        .await
        .expect("document write commits");
    }
    assert_eq!(db.index_operation_queue_stats().pending_operations, 6);
    // Dropping and recreating the vector index moves its record to a new
    // generation, so no canonical record names the queued one any more.
    let controller = LifecycleTestController::new();
    let receipt = controller
        .drop_index(&db, DataScope::LegacyUnscoped, &vector_definition())
        .await
        .expect("drop is accepted");
    let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
        panic!("dropping an Active index starts an operation: {receipt:?}");
    };
    assert!(
        matches!(
            drive(&db, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ),
        "the drop completes"
    );
    let receipt = controller
        .create_index(
            &db,
            DataScope::LegacyUnscoped,
            vector_definition(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("recreate is accepted");
    let IndexDdlReceipt::Accepted {
        operation_id,
        generation,
        ..
    } = receipt
    else {
        panic!("recreating a dropped index starts a build: {receipt:?}");
    };
    assert!(generation.get() > 1, "the rebuild owns a new generation");
    assert!(
        matches!(
            drive(&db, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ),
        "the rebuild activates"
    );
    assert_eq!(
        db.index_operation_queue_stats().pending_operations,
        6,
        "retired work stays charged until discarded"
    );
    db.close().await.expect("fixture closes");

    let db = open(
        "queue-rediscovery",
        &store,
        tuning,
        LifecycleTestScheduling::Explicit,
    )
    .await;
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.pending_operations,
            stats.pending_members,
            stats.discovered_operations,
            stats.committed_operations,
            stats.oldest_pending_micros,
        ),
        (6, 6, 6, 0, 0),
        "discovered work has no observed commit instant"
    );
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("publication drains"),
        6
    );
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.discarded_operations,
            stats.published_operations,
            stats.acknowledged_operations,
            stats.censored_acknowledgements,
            stats.pending_operations,
        ),
        (3, 3, 6, 6, 0)
    );
    assert_eq!(db.index_operation_publication_lag().count(), 0);
    db.close().await.expect("fixture closes");
}

/// Writes routed to a hidden build, including the repair of a row the build
/// could not have indexed, wait in its queue: publication defers until the
/// build activates, then publishes them.
#[tokio::test]
async fn writes_to_a_hidden_build_wait_for_activation() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "queue-hidden-build",
        &store,
        IndexOperationQueueTuning::default(),
        LifecycleTestScheduling::Explicit,
    )
    .await;
    let invalid = created_id(
        db.query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(LABEL, vec![(BODY, PropertyInput::from(7_i64))]),
                )
                .returning(["created"]),
        ))
        .await
        .expect("a non-text body commits before the index exists"),
    );
    let receipt = LifecycleTestController::new()
        .create_index(
            &db,
            DataScope::LegacyUnscoped,
            text_definition(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("create is accepted");
    let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
        panic!("a new definition starts a build: {receipt:?}");
    };
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("the hidden build is routed");
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "repaired",
            traversal::g()
                .n(NodeRef::from(invalid))
                .set_property(BODY, "repaired words".to_string()),
        ),
    ))
    .await
    .expect("repairing a row the build never indexed commits");
    db.query(text_write(&["late words"]))
        .await
        .expect("a late write commits");
    assert_eq!(db.index_operation_queue_stats().pending_operations, 2);

    let error = db
        .publish_index_queues_for_lifecycle_testing()
        .await
        .expect_err("a hidden build defers publication");
    assert!(
        matches!(&error, HelixDbError::InvariantViolation(message)
            if message.contains("stalled") && message.contains("Deferred")),
        "{error}"
    );
    assert_eq!(db.index_operation_queue_stats().deferred_attempts, 1);
    assert!(
        matches!(
            drive(&db, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ),
        "the build activates"
    );
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("an Active generation publishes"),
        2
    );
    assert_eq!(db.index_operation_queue_stats().pending_operations, 0);
    db.close().await.expect("fixture closes");
}

/// Nothing publishes a hidden build's queue before activation. While the
/// build can still run, its saturation is retryable backpressure; once it
/// blocks, a write that would saturate it fails with the non-retryable
/// `index_build_blocked` error naming the operation, except a lone repair of
/// the blocker's entity: its first queued write, or its removal from the
/// index. Writes to pending entities add no member, so they stay admitted
/// above the member limit, before and after the retry. After a retry
/// activates the build, publication drains the queue.
#[tokio::test]
async fn a_blocked_build_admits_only_its_repair_beyond_the_limits() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "queue-blocked-build",
        &store,
        IndexOperationQueueTuning::default().with_max_members(nonzero(2)),
        LifecycleTestScheduling::Explicit,
    )
    .await;
    let invalid = created_id(
        db.query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(LABEL, vec![(BODY, PropertyInput::from(7_i64))]),
                )
                .returning(["created"]),
        ))
        .await
        .expect("a non-text body commits before the index exists"),
    );
    let receipt = LifecycleTestController::new()
        .create_index(
            &db,
            DataScope::LegacyUnscoped,
            text_definition(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("create is accepted");
    let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
        panic!("a new definition starts a build: {receipt:?}");
    };
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("the hidden build is routed");
    let resident = created_id(
        db.query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g()
                        .add_n(LABEL, vec![(BODY, PropertyInput::from("one".to_string()))]),
                )
                .returning(["created"]),
        ))
        .await
        .expect("first member"),
    );
    db.query(text_write(&["two"]))
        .await
        .expect("the limit itself is admitted");
    let error = db
        .query(text_write(&["three"]))
        .await
        .expect_err("a build that can still run is saturated");
    assert!(error.is_index_backpressure(), "{error}");
    assert_eq!(error.error_code(), QueryErrorCode::IndexBackpressure);

    assert!(
        matches!(
            drive(&db, operation_id).await,
            IndexOperationStatus::Blocked { .. }
        ),
        "the invalid row blocks the build"
    );
    let refused = |result: Result<serde_json::Value, HelixDbError>, write: &str| {
        let error = result.expect_err(write);
        assert!(
            matches!(
                error,
                HelixDbError::IndexBuildBlocked {
                    resource: IndexBackpressureResource::PendingMembers,
                    limit: 2,
                    ..
                }
            ),
            "{write}: {error}"
        );
        assert!(!error.is_index_backpressure(), "{write}: {error}");
        assert_eq!(error.error_code(), QueryErrorCode::IndexBuildBlocked);
        assert_eq!(error.index_error_code(), Some("index_build_blocked"));
        assert!(
            error
                .to_string()
                .contains(&operation_id.as_uuid().to_string()),
            "{write}: {error}"
        );
    };
    let set_body = |id: u64, body: &str| {
        QueryRequest::write(
            batch::write_batch().var_as(
                "updated",
                traversal::g()
                    .n(NodeRef::from(id))
                    .set_property(BODY, body.to_string()),
            ),
        )
    };
    refused(
        db.query(text_write(&["three"])).await,
        "an unrelated insert",
    );
    db.query(set_body(invalid, "repaired words"))
        .await
        .expect("the blocker's first repair is admitted beyond the limit");
    for (id, body, write) in [
        (invalid, "again", "a second write of the repaired entity"),
        (resident, "one updated", "a resident's update"),
    ] {
        db.query(set_body(id, body))
            .await
            .unwrap_or_else(|error| panic!("{write} adds no member: {error}"));
    }
    db.query(QueryRequest::write(batch::write_batch().var_as(
        "dropped",
        traversal::g().n(NodeRef::from(invalid)).drop(),
    )))
    .await
    .expect("removing the repaired entity adds no member");
    refused(db.query(text_write(&["three"])).await, "a later insert");
    let stats = db.index_operation_queue_stats();
    assert_eq!((stats.pending_members, stats.pending_operations), (3, 6));

    db.retry_index_operation(DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("the blocked build retries");
    assert!(
        matches!(
            drive(&db, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ),
        "the build activates without the deleted row"
    );
    let error = db
        .query(text_write(&["three"]))
        .await
        .expect_err("a new member is above the limit until publication drains");
    assert!(
        matches!(
            error,
            HelixDbError::IndexBackpressure {
                resource: IndexBackpressureResource::PendingMembers,
                requested: 4,
                limit: 2,
                ..
            }
        ),
        "a runnable build's saturation is retryable: {error}"
    );
    db.query(set_body(resident, "one again"))
        .await
        .expect("after the retry a resident's update still adds no member");
    let stats = db.index_operation_queue_stats();
    assert_eq!((stats.pending_members, stats.pending_operations), (3, 7));
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("an Active generation publishes"),
        7
    );
    db.query(text_write(&["three"]))
        .await
        .expect("publication freed the capacity");
    db.close().await.expect("fixture closes");
}

/// Only the build measures an entity's vector output, so an oversized
/// blocker's first write may leave it oversized. Once that write has taken
/// the queue past its byte limit, a second write of it is refused, as is a
/// resident's update, but deleting it is still admitted, and the retried
/// build activates without it.
#[tokio::test]
async fn an_oversized_blocker_admits_its_deletion_after_its_first_write() {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    // Every entity's own vector output exceeds one byte.
    let one_byte_vectors = SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            batch.max_entities(),
            batch.max_input_bytes(),
            batch.max_output_operations(),
            batch.max_output_bytes(),
            nonzero(1),
        )
        .expect("batch limits validate"),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .expect("a one-byte vector output budget validates");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let open_tight = |tuning: IndexOperationQueueTuning| {
        HelixDB::open_with_object_store_for_index_lifecycle_testing(
            "queue-blocked-oversized",
            Arc::clone(&store),
            DbConfig::new()
                .with_index_operation_queue_tuning(tuning)
                .with_search_index_backfill_limits(one_byte_vectors),
            LifecycleTestScheduling::Explicit,
        )
    };
    let db = open_tight(IndexOperationQueueTuning::default())
        .await
        .expect("queue fixture opens");
    let embed = |embedding: [f32; 2]| {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        LABEL,
                        vec![(EMBEDDING, PropertyInput::from(embedding.to_vec()))],
                    ),
                )
                .returning(["created"]),
        )
    };
    let oversized = created_id(db.query(embed([1.0, 1.0])).await.expect("seed commits"));
    let receipt = LifecycleTestController::new()
        .create_index(
            &db,
            DataScope::LegacyUnscoped,
            vector_definition(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("create is accepted");
    let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
        panic!("a new definition starts a build: {receipt:?}");
    };
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("the hidden build is routed");
    let blocked = |status: IndexOperationStatus| {
        assert!(
            matches!(
                status,
                IndexOperationStatus::Blocked {
                    blocker_code: IndexOperationBlockerCode::OversizedEntity,
                    ..
                }
            ),
            "{status:?}"
        );
    };
    blocked(drive(&db, operation_id).await);
    let resident = created_id(
        db.query(embed([2.0, 2.0]))
            .await
            .expect("a resident queues"),
    );
    // Room for the resident's operation and half of another, so any further
    // write is above the byte limit while no single one is too large.
    let resident_bytes = db.index_operation_queue_stats().retained_bytes;
    db.close().await.expect("fixture closes");
    let db = open_tight(
        IndexOperationQueueTuning::default()
            .with_max_retained_bytes(nonzero(resident_bytes * 3 / 2))
            .expect("a small retained-byte ceiling is valid"),
    )
    .await
    .expect("queue fixture reopens");
    let move_to = |id: u64, embedding: [f32; 2]| {
        QueryRequest::write(
            batch::write_batch().var_as(
                "moved",
                traversal::g()
                    .n(NodeRef::from(id))
                    .set_property(EMBEDDING, embedding.to_vec()),
            ),
        )
    };
    db.query(move_to(oversized, [1.5, 1.5]))
        .await
        .expect("the blocker's first write is admitted beyond the byte limit");
    db.retry_index_operation(DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("the blocked build retries");
    blocked(drive(&db, operation_id).await);
    for (write, request) in [
        (
            "a second write of the oversized entity",
            move_to(oversized, [1.25, 1.25]),
        ),
        ("a resident's update", move_to(resident, [2.5, 2.5])),
        ("an unrelated insert", embed([3.0, 3.0])),
    ] {
        let error = db.query(request).await.expect_err(write);
        assert!(
            matches!(
                error,
                HelixDbError::IndexBuildBlocked {
                    resource: IndexBackpressureResource::RetainedBytes,
                    ..
                }
            ),
            "{write}: {error}"
        );
    }
    db.query(QueryRequest::write(batch::write_batch().var_as(
        "dropped",
        traversal::g().n(NodeRef::from(oversized)).drop(),
    )))
    .await
    .expect("deleting the oversized entity is admitted beyond the byte limit");
    let stats = db.index_operation_queue_stats();
    assert_eq!((stats.pending_members, stats.pending_operations), (2, 3));
    db.close().await.expect("fixture closes");

    let db = open(
        "queue-blocked-oversized",
        &store,
        IndexOperationQueueTuning::default(),
        LifecycleTestScheduling::Explicit,
    )
    .await;
    db.retry_index_operation(DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("the blocked build retries");
    assert!(
        matches!(
            drive(&db, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ),
        "the default budget fits every remaining entity"
    );
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("an Active generation publishes"),
        3
    );
    db.close().await.expect("fixture closes");
}

/// A blocker that names no entity has no repair, so once its build's queue
/// is saturated every write that would exceed the limits fails with
/// `index_build_blocked`.
#[tokio::test]
async fn a_build_blocked_on_no_entity_admits_nothing_beyond_the_limits() {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    // An input budget below one manifest root blocks the empty index's
    // partition scan on its manifest rather than on an entity.
    let one_byte = SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            batch.max_entities(),
            nonzero(1),
            batch.max_output_operations(),
            batch.max_output_bytes(),
            batch.max_single_vector_output_bytes(),
        )
        .expect("batch limits validate"),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .expect("a one-byte input budget validates");
    let db = HelixDB::open_with_object_store_for_index_lifecycle_testing(
        "queue-blocked-manifest",
        Arc::new(InMemory::new()),
        DbConfig::new()
            .with_index_operation_queue_tuning(
                IndexOperationQueueTuning::default().with_max_members(nonzero(1)),
            )
            .with_search_index_backfill_limits(one_byte),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("queue fixture opens");
    let receipt = LifecycleTestController::new()
        .create_index(
            &db,
            DataScope::LegacyUnscoped,
            text_definition(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("create is accepted");
    let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
        panic!("a new definition starts a build: {receipt:?}");
    };
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("the hidden build is routed");
    let status = drive(&db, operation_id).await;
    assert!(
        matches!(
            status,
            IndexOperationStatus::Blocked {
                blocker_code: IndexOperationBlockerCode::ManifestLimit,
                ..
            }
        ),
        "{status:?}"
    );
    db.query(text_write(&["one"]))
        .await
        .expect("the limit itself is admitted");
    let error = db
        .query(text_write(&["two"]))
        .await
        .expect_err("a second member saturates the blocked build");
    assert!(
        matches!(
            error,
            HelixDbError::IndexBuildBlocked {
                resource: IndexBackpressureResource::PendingMembers,
                requested: 2,
                limit: 1,
                ..
            }
        ),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains(&operation_id.as_uuid().to_string()),
        "{error}"
    );
    assert_eq!(db.index_operation_queue_stats().pending_operations, 1);
    db.close().await.expect("fixture closes");
}

/// A reader owns neither the ledger nor a publisher.
#[tokio::test]
async fn reader_handles_own_no_queue_state() {
    let token = ProcessLocalDatabaseToken::new("queue-reader").expect("token");
    let source = || HelixDbSource::InMemoryToken {
        token: token.clone(),
    };
    HelixDB::open_for_server(source(), DbConfig::new())
        .await
        .expect("writer initializes")
        .close()
        .await
        .expect("writer closes");
    let reader = HelixDB::open_reader_for_server(source(), DbConfig::new())
        .await
        .expect("reader opens");
    assert_eq!(reader.index_operation_queue_stats(), Default::default());
    assert_eq!(
        reader.index_operation_publication_lag(),
        PublicationLagHistogram::default()
    );
    let error = reader
        .publish_index_queues_for_lifecycle_testing()
        .await
        .expect_err("readers do not publish");
    assert!(
        matches!(error, HelixDbError::WriterModeRequired { .. }),
        "{error}"
    );
    reader.close().await.expect("reader closes");
}

/// One read request ranking documents near the origin `searches` times.
fn vector_searches(searches: usize, consistency: SearchConsistency) -> QueryRequest {
    let names = (0..searches)
        .map(|index| format!("hits_{index}"))
        .collect::<Vec<_>>();
    let read = names.iter().fold(batch::read_batch(), |read, name| {
        read.var_as(
            name,
            traversal::g().vector_search_nodes(LABEL, EMBEDDING, vec![0.0, 0.0], 10, None),
        )
    });
    QueryRequest::read(read.returning(names))
        .with_search_consistency(consistency)
        .expect("read requests accept any consistency")
}

/// One write inserting a document at each of `positions`, then ranking
/// documents.
fn insert_and_search(positions: &[f32]) -> QueryRequest {
    let write =
        positions
            .iter()
            .enumerate()
            .fold(batch::write_batch(), |write, (index, position)| {
                write.var_as(
                    &format!("created_{index}"),
                    traversal::g().add_n(
                        LABEL,
                        vec![(EMBEDDING, PropertyInput::from(vec![*position, 0.0]))],
                    ),
                )
            });
    QueryRequest::write(
        write
            .var_as(
                "hits",
                traversal::g().vector_search_nodes(LABEL, EMBEDDING, vec![0.0, 0.0], 10, None),
            )
            .returning(["hits"]),
    )
}

fn hit_count(result: &serde_json::Value, name: &str) -> usize {
    result[name].as_array().map_or(0, Vec::len)
}

/// Strong vector searches, in read and write requests, fail with retryable
/// `pending_vector_bytes` backpressure while committed pending work exceeds
/// the configured bound, and transports classify it as backpressure.
/// Eventual searches keep answering, a write's own changes never count, and
/// publication clears the condition.
#[tokio::test]
async fn strong_vector_searches_past_their_bound_fail_until_publication() {
    let name = "queue-strong-vector-bound";
    let store = fixture(name, vec![vector_definition()]).await;
    let db = open(
        name,
        &store,
        IndexOperationQueueTuning::default(),
        LifecycleTestScheduling::Explicit,
    )
    .await;
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "first",
                traversal::g().add_n(
                    LABEL,
                    vec![(EMBEDDING, PropertyInput::from(vec![1.0_f32, 0.0]))],
                ),
            )
            .var_as(
                "second",
                traversal::g().add_n(
                    LABEL,
                    vec![(EMBEDDING, PropertyInput::from(vec![2.0_f32, 0.0]))],
                ),
            ),
    ))
    .await
    .expect("documents queue");
    // Two inserts and nothing they supersede: every retained byte is one
    // entity's latest operation. Admission charges each its encoded size
    // plus a fixed overhead; the strong bound counts only the encoded size.
    let pending = db.index_operation_queue_stats().retained_bytes
        - 2 * IndexOperationQueueTuning::OPERATION_OVERHEAD_BYTES;
    db.close().await.expect("fixture closes");

    let tuning = IndexOperationQueueTuning::default()
        .with_strong_vector_search_max_pending_bytes(nonzero(pending - 1));
    assert_eq!(
        tuning.strong_vector_search_max_pending_bytes().get(),
        pending - 1
    );
    assert_eq!(
        IndexOperationQueueTuning::default().strong_vector_search_max_pending_bytes(),
        IndexOperationQueueTuning::default().max_retained_bytes(),
        "by default the bound is the retained-byte ceiling"
    );
    let db = Arc::new(open(name, &store, tuning, LifecycleTestScheduling::Explicit).await);
    let error = db
        .query(vector_searches(3, SearchConsistency::Strong))
        .await
        .expect_err("committed work past the bound");
    assert!(
        matches!(
            error,
            HelixDbError::IndexBackpressure {
                resource: IndexBackpressureResource::PendingVectorBytes,
                requested,
                limit,
                ..
            } if requested == pending && limit == pending - 1
        ),
        "{error}"
    );
    assert!(error.is_index_backpressure());
    assert_eq!(error.error_code(), QueryErrorCode::IndexBackpressure);
    assert!(
        error.to_string().contains("pending_vector_bytes"),
        "{error}"
    );
    let error = HelixQueryService::new(Arc::clone(&db))
        .execute_query(vector_searches(1, SearchConsistency::Strong))
        .await
        .expect_err("still past the bound");
    assert_eq!(error.classify(), QueryFailureClass::Backpressure);

    let eventual = db
        .query(vector_searches(2, SearchConsistency::Eventual))
        .await
        .expect("eventual searches keep their own budget");
    assert_eq!(
        (
            hit_count(&eventual, "hits_0"),
            hit_count(&eventual, "hits_1")
        ),
        (2, 2)
    );
    let error = db
        .query(insert_and_search(&[3.0]))
        .await
        .expect_err("a write's search sees the same committed work");
    assert!(error.is_index_backpressure(), "{error}");
    assert_eq!(
        document_count(&db).await,
        2,
        "the rejected write rolled back"
    );

    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("publication drains"),
        2
    );
    let strong = db
        .query(vector_searches(3, SearchConsistency::Strong))
        .await
        .expect("nothing is pending");
    assert_eq!(hit_count(&strong, "hits_2"), 2);
    // Its own three inserts alone exceed the bound, and never count.
    let written = db
        .query(insert_and_search(&[3.0, 4.0, 5.0]))
        .await
        .expect("a write's own changes never count");
    assert_eq!(hit_count(&written, "hits"), 5);
    db.close().await.expect("fixture closes");
}

/// Creates `definitions` through an automatically scheduled writer, waits
/// for each to activate, and closes it.
async fn fixture(
    name: &str,
    definitions: Vec<ValidatedDynamicIndexDefinition>,
) -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        name,
        &store,
        IndexOperationQueueTuning::default(),
        LifecycleTestScheduling::Automatic,
    )
    .await;
    for definition in definitions {
        let receipt = LifecycleTestController::new()
            .create_index(
                &db,
                DataScope::LegacyUnscoped,
                definition,
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("create is accepted");
        let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("a new definition starts a build: {receipt:?}");
        };
        let started = Instant::now();
        loop {
            let status = operation_status(&db, operation_id).await;
            if matches!(status, IndexOperationStatus::Succeeded { .. }) {
                break;
            }
            assert!(
                started.elapsed() < OPERATION_TIMEOUT && !is_terminal(&status),
                "fixture build did not activate: {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    db.close().await.expect("fixture writer closes");
    store
}

async fn open(
    name: &str,
    store: &Arc<dyn ObjectStore>,
    tuning: IndexOperationQueueTuning,
    scheduling: LifecycleTestScheduling,
) -> HelixDB {
    HelixDB::open_with_object_store_for_index_lifecycle_testing(
        name,
        Arc::clone(store),
        DbConfig::new().with_index_operation_queue_tuning(tuning),
        scheduling,
    )
    .await
    .expect("queue fixture opens")
}

/// Steps one operation through its production driver until it terminates,
/// advancing a logical clock past any persisted retry deadline.
async fn drive(db: &HelixDB, operation_id: IndexOperationId) -> IndexOperationStatus {
    let controller = LifecycleTestController::new();
    let target = LifecycleWorkTarget::Operation {
        scope: DataScope::LegacyUnscoped,
        operation_id,
    };
    let start = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after the epoch")
            .as_millis(),
    )
    .expect("milliseconds fit u64");
    for turn in 0..4_096_u64 {
        let status = operation_status(db, operation_id).await;
        if is_terminal(&status) {
            return status;
        }
        controller
            .advance_at_unix_millis(db, target, start + turn * 60_000)
            .await
            .expect("lifecycle step runs");
    }
    panic!("operation did not terminate");
}

async fn operation_status(db: &HelixDB, operation_id: IndexOperationId) -> IndexOperationStatus {
    db.get_index_operation(DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("operation status reads")
}

const fn is_terminal(status: &IndexOperationStatus) -> bool {
    matches!(
        status,
        IndexOperationStatus::Succeeded { .. }
            | IndexOperationStatus::Blocked { .. }
            | IndexOperationStatus::Aborted { .. }
    )
}

/// One write creating a text document per body.
fn text_write(bodies: &[&str]) -> QueryRequest {
    QueryRequest::write(bodies.iter().enumerate().fold(
        batch::write_batch(),
        |write, (index, body)| {
            write.var_as(
                &format!("created_{index}"),
                traversal::g().add_n(LABEL, vec![(BODY, PropertyInput::from(body.to_string()))]),
            )
        },
    ))
}

async fn document_count(db: &HelixDB) -> u64 {
    db.query(QueryRequest::read(
        batch::read_batch()
            .var_as("count", traversal::g().n(NodeRef::all()).count())
            .returning(["count"]),
    ))
    .await
    .expect("count reads")["count"]
        .as_u64()
        .expect("count is an integer")
}

fn created_id(created: serde_json::Value) -> u64 {
    created["created"][0]["$id"]
        .as_u64()
        .expect("created node ID")
}

fn text_definition() -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(
        TextIndexDefinition::new_node(LABEL, BODY).expect("text definition validates"),
    )
    .expect("text definition converts")
}

fn vector_definition() -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(
        VectorIndexDefinition::new_node(LABEL, EMBEDDING, 2, VectorDistanceMetric::Euclidean)
            .expect("vector definition validates"),
    )
    .expect("vector definition converts")
}

fn nonzero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("nonzero limit")
}
