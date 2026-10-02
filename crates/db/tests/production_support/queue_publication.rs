//! Production contracts for bounded queue publication.
//!
//! This feature-gated child of the publication module drives the writer's own
//! publisher, and publishers over the same storage and planning budget with
//! narrower limits, against explicitly scheduled writers, so no background
//! attempt races a contract. An automatically scheduled writer builds each
//! fixture's indexes and is then reopened explicitly over the same store.
//! Graph writes enqueue every operation through the production producer.
//! Failure contracts stage one unobserved acknowledgement, corrupt one queue
//! value, rewrite one namespace's metadata through the current codecs, hold
//! the planning budget while a catalog change commits, fail WAL uploads, or
//! fence the writer with a newer one; none introduces a row family or
//! encoding.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use helix_ast::{
    batch,
    graph::NodeRef,
    query::{QueryRequest, SearchConsistency},
    traversal,
    value::{PropertyInput, PropertyValue},
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};

use super::*;
use crate::config::{
    DbConfig, SearchIndexBackfillLimits, TextBackfillCompactionLimits, TextIndexDefinition,
    VectorIndexDefinition,
};
use crate::encoding::v2::keys::indexes::vector::{VectorIndexMetadataKey, VectorKey};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::index_lifecycle::{
    IndexDdlReceipt, IndexGenerationId, IndexId, IndexOperationId, IndexOperationStatus,
    ValidatedDynamicIndexDefinition, VectorPhysicalLayout,
};
use crate::index_lifecycle_testing::{
    LifecycleTestController, LifecycleTestScheduling, LifecycleWorkTarget,
};
use crate::search::vector::VectorDistanceMetric;
use crate::HelixDB;

/// Scope of every fixture index.
const SCOPE: DataScope = DataScope::LegacyUnscoped;
/// Default transaction byte ceilings of the fixture batch limits.
const EIGHT_MIB: u64 = 8 * 1024 * 1024;

/// Runs every queue publication contract in isolated databases.
pub(crate) async fn run() {
    vector_budgets_trim_then_block().await;
    selection_and_collapse_boundaries().await;
    text_budgets_trim_then_block().await;
    uncertain_acknowledgements_reconcile().await;
    retired_generations_discard_and_retry().await;
    refused_corrupt_and_closed_attempts().await;
    inconsistent_namespace_metadata_fails_closed().await;
    concurrent_catalog_change_conflicts_publication().await;
    uncertain_commits_keep_their_charges().await;
    fenced_commits_end_publication().await;
}

fn vector_definition() -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(
        VectorIndexDefinition::new_node("Doc", "embedding", 2, VectorDistanceMetric::Euclidean)
            .expect("fixture vector definition validates"),
    )
    .expect("fixture vector definition converts to V2")
}

fn text_definition() -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(
        TextIndexDefinition::new_node("Doc", "body").expect("fixture text definition validates"),
    )
    .expect("fixture text definition converts to V2")
}

/// Opens an explicitly scheduled writer over `store` after an automatically
/// scheduled writer built every one of `definitions`.
async fn open_explicit(
    name: &str,
    store: Arc<dyn ObjectStore>,
    definitions: &[ValidatedDynamicIndexDefinition],
) -> HelixDB {
    let builder = HelixDB::open_with_object_store_for_index_lifecycle_testing(
        name,
        Arc::clone(&store),
        DbConfig::new(),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("building writer opens");
    for definition in definitions {
        builder
            .install_index_for_tests(definition.clone())
            .await
            .expect("fixture index builds");
    }
    builder.close().await.expect("building writer closes");
    HelixDB::open_with_object_store_for_index_lifecycle_testing(
        name,
        store,
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("explicit writer opens")
}

/// Returns the writer's own publisher.
fn writer_publisher(db: &HelixDB) -> &Arc<QueuePublisher> {
    db.inner
        .index_queue_publisher
        .as_ref()
        .expect("a writer owns a publisher")
}

/// Returns a publisher over `db`'s storage, ledger, gates, and planning
/// budget with its own batch and text limits.
fn publisher_with_limits(
    db: &HelixDB,
    limits: SearchIndexBatchLimits,
    text: ActiveTextMutationLimits,
) -> Arc<QueuePublisher> {
    let writer = writer_publisher(db);
    QueuePublisher::new(
        Arc::clone(&writer.db),
        Arc::clone(&writer.backlog),
        Arc::clone(&writer.store),
        Arc::clone(&writer.scope_gates),
        writer.vector.clone(),
        limits,
        TextPublicationResources {
            limits: text,
            ..writer.text.clone()
        },
    )
}

/// Batch limits with `max_output_operations` and a single-vector ceiling;
/// every other limit is generous.
fn batch_limits(
    max_output_operations: u64,
    max_single_vector_output_bytes: u64,
) -> SearchIndexBatchLimits {
    SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(512).expect("entity limit is positive"),
        NonZeroU64::new(EIGHT_MIB).expect("input limit is positive"),
        NonZeroU64::new(max_output_operations).expect("operation limit is positive"),
        NonZeroU64::new(EIGHT_MIB).expect("output limit is positive"),
        NonZeroU64::new(max_single_vector_output_bytes).expect("vector limit is positive"),
    )
    .expect("fixture batch limits validate")
}

/// Text limits of the configured policy.
fn default_text_limits() -> ActiveTextMutationLimits {
    DbConfig::new()
        .search_index_backfill()
        .active_text_mutation()
}

async fn add(db: &HelixDB, properties: Vec<(&'static str, PropertyInput)>) -> u64 {
    let result = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as("created", traversal::g().add_n("Doc", properties))
                .returning(["created"]),
        ))
        .await
        .expect("document write commits");
    result["created"][0]["$id"]
        .as_u64()
        .expect("created node id")
}

fn embedding(vector: [f32; 2]) -> Vec<(&'static str, PropertyInput)> {
    vec![("embedding", PropertyInput::from(vector.to_vec()))]
}

fn document(vector: [f32; 2], body: &str) -> Vec<(&'static str, PropertyInput)> {
    vec![
        ("embedding", PropertyInput::from(vector.to_vec())),
        ("body", PropertyInput::from(body.to_string())),
    ]
}

async fn set_embedding(db: &HelixDB, id: u64, vector: [f32; 2]) {
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "updated",
            traversal::g()
                .n(NodeRef::from(id))
                .set_property("embedding", vector.to_vec()),
        ),
    ))
    .await
    .expect("embedding update commits");
}

async fn remove(db: &HelixDB, id: u64) {
    db.query(QueryRequest::write(
        batch::write_batch().var_as("dropped", traversal::g().n(NodeRef::from(id)).drop()),
    ))
    .await
    .expect("document delete commits");
}

/// Returns the pending generation whose queue holds `family` operations.
async fn target(db: &HelixDB, family: QueueFamily) -> QueueTarget {
    let store = &writer_publisher(db).store;
    for target in db.index_operation_backlog().outstanding_targets() {
        if store
            .read(db.inner_db().as_ref(), target)
            .await
            .expect("queue reads")
            .is_some_and(|stored| stored.queue().family() == family)
        {
            return target;
        }
    }
    panic!("a {family:?} queue is pending");
}

/// Returns `target`'s queued operations in enqueue order.
async fn queued(db: &HelixDB, target: QueueTarget) -> Vec<QueuedOperation> {
    writer_publisher(db)
        .store
        .read(db.inner_db().as_ref(), target)
        .await
        .expect("queue reads")
        .map_or_else(Vec::new, |stored| stored.queue().operations().to_vec())
}

async fn all_keys(db: &HelixDB) -> BTreeSet<Bytes> {
    let storage = db.inner_db();
    let mut rows = storage
        .scan::<std::ops::RangeFull>(..)
        .await
        .expect("database scans");
    let mut keys = BTreeSet::new();
    while let Some(row) = rows.next().await.expect("row reads") {
        keys.insert(row.key);
    }
    keys
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

/// Advances one lifecycle operation until it succeeds.
async fn drive(db: &HelixDB, operation_id: IndexOperationId) {
    let controller = LifecycleTestController;
    for _ in 0..256 {
        match db
            .get_index_operation(SCOPE, operation_id)
            .await
            .expect("operation status reads")
        {
            IndexOperationStatus::Succeeded { .. } => return,
            IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. } => {
                controller
                    .advance(
                        db,
                        LifecycleWorkTarget::Operation {
                            scope: SCOPE,
                            operation_id,
                        },
                    )
                    .await
                    .expect("operation step runs");
            }
            status @ (IndexOperationStatus::Blocked { .. }
            | IndexOperationStatus::Aborted { .. }) => {
                panic!("fixture operation did not succeed: {status:?}")
            }
        }
    }
    panic!("fixture operation exceeded its step bound");
}

/// Drops `definition`, returning its cleanup operation.
async fn drop_index(
    db: &HelixDB,
    definition: &ValidatedDynamicIndexDefinition,
) -> IndexOperationId {
    let IndexDdlReceipt::Accepted { operation_id, .. } = LifecycleTestController
        .drop_index(db, SCOPE, definition)
        .await
        .expect("drop enqueues")
    else {
        panic!("dropping an Active index enqueues cleanup");
    };
    operation_id
}

/// Proves an effect that cannot fit beside its acknowledgement halves the
/// selection, then blocks its lone operation, without writing anything.
///
/// A one-operation transaction leaves no room beside the acknowledgement, and
/// a one-byte single-vector ceiling rejects every insertion; either way the
/// first attempt trims and the second blocks. The configured limits then
/// publish both entities.
async fn vector_budgets_trim_then_block() {
    let db = open_explicit(
        "queue-publication-vector-budgets",
        Arc::new(InMemory::new()),
        &[vector_definition()],
    )
    .await;
    add(&db, embedding([0.0, 0.0])).await;
    add(&db, embedding([1.0, 1.0])).await;
    let target = target(&db, QueueFamily::Vector).await;
    let before = all_keys(&db).await;
    for limits in [batch_limits(1, EIGHT_MIB), batch_limits(32_768, 1)] {
        let narrow = publisher_with_limits(&db, limits, default_text_limits());
        assert_eq!(
            narrow
                .publish_once(target)
                .await
                .expect("a trimmed attempt is not an error"),
            PublicationOutcome::Trimmed
        );
        assert_eq!(
            narrow
                .publish_once(target)
                .await
                .expect("a blocked attempt is not an error"),
            PublicationOutcome::Blocked
        );
        assert_eq!(load(&narrow.metrics().output_retries), 1);
        assert_eq!(load(&narrow.metrics().blocked_attempts), 1);
    }
    assert_eq!(
        all_keys(&db).await,
        before,
        "no trimmed or blocked attempt writes"
    );
    assert_eq!(queued(&db, target).await.len(), 2);
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("configured limits publish"),
        2
    );
    assert!(queued(&db, target).await.is_empty());
    db.close().await.expect("vector budget writer closes");
}

/// Proves batch selection takes whole ordered prefixes within its limits and
/// that effects collapse only operations of their own family.
async fn selection_and_collapse_boundaries() {
    let db = open_explicit(
        "queue-publication-selection",
        Arc::new(InMemory::new()),
        &[vector_definition(), text_definition()],
    )
    .await;
    let first = add(&db, document([0.0, 0.0], "alpha")).await;
    set_embedding(&db, first, [1.0, 1.0]).await;
    let second = add(&db, document([2.0, 2.0], "beta")).await;
    let vector_target = target(&db, QueueFamily::Vector).await;
    let vector = queued(&db, vector_target).await;
    let text = queued(&db, target(&db, QueueFamily::Text).await).await;
    let shape = |selected: &[SelectedEntity<'_>]| {
        selected
            .iter()
            .map(|selected| (selected.entity.id.get(), selected.operations.len()))
            .collect::<Vec<_>>()
    };

    // An operation ceiling ends the batch inside the first entity's prefix.
    assert_eq!(
        shape(&select_batch(
            &vector,
            None,
            512,
            NonZeroUsize::MIN,
            u64::MAX
        )),
        [(first, 1)]
    );
    // An input ceiling ends it before the next operation, but never before
    // the batch's first.
    assert_eq!(
        shape(&select_batch(
            &vector,
            None,
            512,
            NonZeroUsize::MAX,
            vector[0].retained_bytes()
        )),
        [(first, 1)]
    );
    // A batch after the first entity rotates to the second, then wraps.
    assert_eq!(
        shape(&select_batch(
            &vector,
            Some(vector[0].entity()),
            512,
            NonZeroUsize::MAX,
            u64::MAX
        )),
        [(second, 1), (first, 2)]
    );

    assert!(matches!(
        collapse_vector(&SelectedEntity {
            entity: vector[0].entity(),
            operations: vec![&text[0]],
        }),
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    assert!(matches!(
        collapse_text(&SelectedEntity {
            entity: vector[0].entity(),
            operations: vec![&vector[0]],
        }),
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    assert!(matches!(
        collapse_vector(&SelectedEntity {
            entity: vector[0].entity(),
            operations: Vec::new(),
        }),
        Err(HelixDbError::InvariantViolation(_))
    ));
    assert!(matches!(
        collapse_text(&SelectedEntity {
            entity: vector[0].entity(),
            operations: Vec::new(),
        }),
        Err(HelixDbError::InvariantViolation(_))
    ));
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("queued work publishes"),
        5
    );
    db.close().await.expect("selection writer closes");
}

/// Returns `count` distinct lowercase terms starting at `first`.
fn terms(first: usize, count: usize) -> String {
    (first..first + count)
        .map(|term| format!("w{term}q"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Text limits whose 64 KiB analysis budget admits one 150-term document
/// but not two, nor one of 500 terms.
fn narrow_text_limits() -> ActiveTextMutationLimits {
    let defaults = SearchIndexBackfillLimits::default();
    let compaction = defaults.text_compaction();
    ActiveTextMutationLimits::from_backfill(
        SearchIndexBackfillLimits::try_new(
            defaults.batch(),
            NonZeroUsize::MIN,
            defaults.text_artifacts(),
            TextBackfillCompactionLimits::new(
                compaction.max_fan_in(),
                NonZeroU64::new(64 * 1024).expect("analysis budget is positive"),
                compaction.max_temporary_disk_bytes(),
                compaction.max_output_blob_bytes(),
                NonZeroU64::new(4 * 1024).expect("page budget is positive"),
            ),
        )
        .expect("narrow text limits validate"),
    )
}

/// Proves text limits that shrank after admission halve the entity batch
/// until it fits, and block a lone document that can never fit.
async fn text_budgets_trim_then_block() {
    let db = open_explicit(
        "queue-publication-text-budgets",
        Arc::new(InMemory::new()),
        &[text_definition()],
    )
    .await;
    for first in [0, 150] {
        add(&db, vec![("body", PropertyInput::from(terms(first, 150)))]).await;
    }
    let target = target(&db, QueueFamily::Text).await;
    let narrow = publisher_with_limits(
        &db,
        SearchIndexBackfillLimits::default().batch(),
        narrow_text_limits(),
    );
    let mut observed = Vec::new();
    for _ in 0..3 {
        observed.push(
            narrow
                .publish_once(target)
                .await
                .expect("text attempts are not errors"),
        );
    }
    let published = PublicationOutcome::Published {
        operations: 1,
        entities: 1,
    };
    assert_eq!(
        observed,
        [PublicationOutcome::Trimmed, published, published]
    );
    add(&db, vec![("body", PropertyInput::from(terms(300, 500)))]).await;
    assert_eq!(
        narrow
            .publish_once(target)
            .await
            .expect("a blocked attempt is not an error"),
        PublicationOutcome::Blocked
    );
    assert_eq!(load(&narrow.metrics().output_retries), 1);
    assert_eq!(load(&narrow.metrics().blocked_attempts), 1);
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("configured limits publish"),
        1
    );
    db.close().await.expect("text budget writer closes");
}

/// Proves uncertain acknowledgements reconcile against a flushed read before
/// the next attempt publishes: an acknowledgement that committed releases its
/// charge, and one that did not, even when none did, is published once.
async fn uncertain_acknowledgements_reconcile() {
    let db = open_explicit(
        "queue-publication-reconcile",
        Arc::new(InMemory::new()),
        &[vector_definition()],
    )
    .await;
    let pending = || {
        let stats = db.index_operation_queue_stats();
        (stats.pending_operations, stats.uncertain_operations)
    };
    add(&db, embedding([0.0, 0.0])).await;
    let target = target(&db, QueueFamily::Vector).await;
    let publisher = writer_publisher(&db);
    // An uncertain acknowledgement of an operation still queued never
    // committed: reconciliation releases nothing and the attempt publishes it.
    db.index_operation_backlog()
        .mark_acknowledgement_uncertain(queued(&db, target).await.iter().map(QueuedOperation::id));
    assert_eq!(pending(), (1, 1));
    assert_eq!(
        publisher
            .publish_once(target)
            .await
            .expect("reconciliation then publication succeeds"),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(pending(), (0, 0));

    add(&db, embedding([1.0, 1.0])).await;
    add(&db, embedding([2.0, 2.0])).await;
    let stored = publisher
        .store
        .read(db.inner_db().as_ref(), target)
        .await
        .expect("queue reads")
        .expect("queue is stored");
    let operations = stored.queue().operations().to_vec();
    // The first acknowledgement commits but its outcome is never observed.
    let transaction = db
        .inner_db()
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .expect("acknowledgement transaction opens");
    publisher
        .store
        .stage_acknowledge(&transaction, target, &stored, &[operations[0].id()])
        .expect("acknowledgement stages");
    transaction.commit().await.expect("acknowledgement commits");
    db.index_operation_backlog()
        .mark_acknowledgement_uncertain(operations.iter().map(QueuedOperation::id));
    assert_eq!(pending(), (2, 2));
    assert_eq!(
        publisher
            .publish_once(target)
            .await
            .expect("reconciliation then publication succeeds"),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(pending(), (0, 0));
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.committed_operations,
            stats.acknowledged_operations,
            stats.uncertain_operations,
            stats.queue_reads,
        ),
        (3, 3, 0, 4)
    );
    assert_eq!(db.index_operation_publication_lag().count(), 3);
    db.close().await.expect("reconciliation writer closes");
}

/// Proves generations no record serves are discarded or empty, and that a
/// retirement the publication transaction observes after classification
/// retries.
async fn retired_generations_discard_and_retry() {
    let db = open_explicit(
        "queue-publication-retired",
        Arc::new(InMemory::new()),
        &[vector_definition(), text_definition()],
    )
    .await;
    add(&db, document([0.0, 0.0], "alpha")).await;
    add(&db, document([1.0, 1.0], "beta")).await;
    let vector = target(&db, QueueFamily::Vector).await;
    let text = target(&db, QueueFamily::Text).await;
    let publisher = writer_publisher(&db);

    // No record names these generations, so they are retired and empty.
    for foreign in [
        QueueTarget::new(
            SCOPE,
            IndexId::new(u64::from(u32::MAX)).expect("fixture index ID is positive"),
            vector.generation,
        ),
        QueueTarget::new(
            SCOPE,
            vector.index_id,
            IndexGenerationId::new(vector.generation.get() + 1)
                .expect("fixture generation is positive"),
        ),
    ] {
        assert_eq!(
            publisher
                .publish_once(foreign)
                .await
                .expect("a retired foreign generation is not an error"),
            PublicationOutcome::Empty
        );
    }

    let drops = [
        drop_index(&db, &vector_definition()).await,
        drop_index(&db, &text_definition()).await,
    ];
    // The attempts classified Active generations, but their publication
    // transactions read the retirement and retry.
    let stored = publisher
        .read_queue(vector)
        .await
        .expect("vector queue reads")
        .expect("vector queue remains");
    let permit = publisher.scope_gates.publication_permit(vector).await;
    assert_eq!(
        publisher
            .publish_vector(&permit, &stored)
            .await
            .expect("a retired vector generation retries"),
        PublicationOutcome::Retry
    );
    drop(permit);
    let stored = publisher
        .read_queue(text)
        .await
        .expect("text queue reads")
        .expect("text queue remains");
    assert_eq!(
        publisher
            .publish_text(text, &stored)
            .await
            .expect("a retired text generation retries"),
        PublicationOutcome::Retry
    );

    // Cleanup leaves Dropped records, whose generations' queues are discarded.
    for operation_id in drops {
        drive(&db, operation_id).await;
    }
    for target in [vector, text] {
        assert_eq!(
            publisher
                .publish_once(target)
                .await
                .expect("a dropped generation discards"),
            PublicationOutcome::Discarded { operations: 2 }
        );
        assert_eq!(
            publisher
                .publish_once(target)
                .await
                .expect("a discarded queue is empty"),
            PublicationOutcome::Empty
        );
    }
    assert_eq!(load(&publisher.metrics().discarded_operations), 4);
    db.close().await.expect("retired writer closes");
}

/// Proves a concurrent attempt is refused without touching its target, a
/// queue that no longer decodes retries, and closed storage ends publication.
async fn refused_corrupt_and_closed_attempts() {
    let db = open_explicit(
        "queue-publication-refused",
        Arc::new(InMemory::new()),
        &[vector_definition()],
    )
    .await;
    add(&db, embedding([0.0, 0.0])).await;
    let target = target(&db, QueueFamily::Vector).await;
    let publisher = Arc::clone(writer_publisher(&db));
    assert!(format!("{publisher:?}").contains("QueuePublisher"));

    assert!(publisher.attempts.lock().insert(target));
    assert_eq!(
        publisher
            .publish_once(target)
            .await
            .expect("a refused attempt is not an error"),
        PublicationOutcome::Retry
    );
    assert!(publisher.attempts.lock().remove(&target));
    assert_eq!(
        load(&publisher.metrics().attempts),
        0,
        "a refused call is no attempt"
    );

    let storage = db.inner_db();
    let queue = storage
        .get(target.key())
        .await
        .expect("queue reads")
        .expect("queue is stored");
    storage
        .put(target.key(), b"not an operation queue")
        .await
        .expect("corrupt queue writes");
    assert_eq!(
        publisher
            .publish_once(target)
            .await
            .expect("a corrupt queue retries"),
        PublicationOutcome::Retry
    );
    assert_eq!(load(&publisher.metrics().error_retries), 1);
    storage
        .put(target.key(), &queue)
        .await
        .expect("queue restores");
    assert_eq!(
        publisher
            .publish_once(target)
            .await
            .expect("the restored queue publishes"),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );

    db.close().await.expect("refused writer closes");
    assert!(matches!(
        publisher.publish_once(target).await,
        Err(HelixDbError::Storage(error)) if matches!(error.kind(), slatedb::ErrorKind::Closed(_))
    ));
}

/// One queued change to a published vector index.
enum Change {
    /// Deletes a published document, removing it from its namespace.
    Remove(u64),
    /// Inserts a document with this embedding.
    Insert([f32; 2]),
}

/// Proves publication fails closed, writing nothing, when a namespace's
/// metadata disagrees with its definition or is missing, for both removals
/// and upserts, and publishes once the metadata is restored.
async fn inconsistent_namespace_metadata_fails_closed() {
    let db = open_explicit(
        "queue-publication-metadata",
        Arc::new(InMemory::new()),
        &[vector_definition()],
    )
    .await;
    let removed = add(&db, embedding([0.0, 0.0])).await;
    let kept = add(&db, embedding([1.0, 1.0])).await;
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(
        db.publish_index_queues_for_lifecycle_testing()
            .await
            .expect("fixture documents publish"),
        2
    );
    let Some(ActiveIndexHandle::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        ..
    }) = db
        .active_index_handles_loaded(SCOPE)
        .into_iter()
        .find(|handle| matches!(handle, ActiveIndexHandle::Vector { .. }))
    else {
        panic!("one Active unpartitioned vector generation");
    };
    let key = DataKey::Data {
        scope: SCOPE,
        kind: DataKeyKind::Vector(
            crate::encoding::v2::keys::indexes::vector::VectorKey::IndexMetadata(
                VectorIndexMetadataKey::new(physical_index_id.get()),
            ),
        ),
    }
    .to_bytes();
    let storage = db.inner_db();
    let original = storage
        .get(&key)
        .await
        .expect("metadata reads")
        .expect("namespace has metadata");
    let mut contradicting =
        crate::search::vector::decode_metadata(&original).expect("metadata decodes");
    contradicting.config.property_name = "contradicting_embedding".to_string();
    let contradicting =
        Bytes::copy_from_slice(crate::search::vector::encode_metadata(&contradicting).as_slice());
    let publisher = writer_publisher(&db);
    for (errors, (metadata, change)) in [
        (Some(contradicting.clone()), Change::Remove(removed)),
        (Some(contradicting), Change::Insert([2.0, 2.0])),
        (None, Change::Remove(kept)),
        (None, Change::Insert([3.0, 3.0])),
    ]
    .into_iter()
    .enumerate()
    {
        match &metadata {
            Some(metadata) => {
                storage
                    .put(&key, metadata)
                    .await
                    .expect("metadata rewrites");
            }
            None => {
                storage.delete(&key).await.expect("metadata deletes");
            }
        }
        match change {
            Change::Remove(id) => remove(&db, id).await,
            Change::Insert(vector) => {
                add(&db, embedding(vector)).await;
            }
        }
        let keys = all_keys(&db).await;
        assert_eq!(
            publisher
                .publish_once(target)
                .await
                .expect("inconsistent metadata retries"),
            PublicationOutcome::Retry
        );
        assert_eq!(
            load(&publisher.metrics().error_retries),
            u64::try_from(errors + 1).expect("phase count fits u64"),
            "inconsistent metadata is an error, not a conflict"
        );
        assert_eq!(all_keys(&db).await, keys, "a failed attempt writes nothing");
        storage
            .put(&key, &original)
            .await
            .expect("metadata restores");
        assert!(matches!(
            publisher
                .publish_once(target)
                .await
                .expect("restored metadata publishes"),
            PublicationOutcome::Published { .. }
        ));
    }
    db.close().await.expect("metadata writer closes");
}

/// Proves an index created in the scope while a publication plans conflicts
/// its commit, which retries without an error and then publishes.
async fn concurrent_catalog_change_conflicts_publication() {
    let db = open_explicit(
        "queue-publication-conflict",
        Arc::new(InMemory::new()),
        &[vector_definition()],
    )
    .await;
    add(&db, embedding([0.0, 0.0])).await;
    let target = target(&db, QueueFamily::Vector).await;
    let publisher = Arc::clone(writer_publisher(&db));
    let planning = Arc::clone(&publisher.vector.planning_cache);
    let held = crate::index_lifecycle::vector::hold_planning_sessions(&planning).await;
    let attempt = tokio::spawn({
        let publisher = Arc::clone(&publisher);
        async move { publisher.publish_once(target).await }
    });
    // The attempt has read its generation's record and waits to plan.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while crate::index_lifecycle::vector::planning_session_lock_holders(&planning) < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the attempt waits for its planning session");
    LifecycleTestController
        .create_index(
            &db,
            SCOPE,
            text_definition(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("a concurrent index creation commits");
    drop(held);
    assert_eq!(
        attempt
            .await
            .expect("the attempt task joins")
            .expect("a conflict is not an error"),
        PublicationOutcome::Retry
    );
    assert_eq!(load(&publisher.metrics().commit_conflicts), 1);
    assert_eq!(load(&publisher.metrics().error_retries), 0);
    assert_eq!(
        publisher
            .publish_once(target)
            .await
            .expect("the retry publishes"),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    db.close().await.expect("conflict writer closes");
}

/// In-memory object store whose WAL uploads fail once `failing` is set, as an
/// unavailable object store's would.
#[derive(Debug, Default)]
struct FailingWalStore {
    inner: InMemory,
    failing: AtomicBool,
}

impl std::fmt::Display for FailingWalStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("failing-wal-memory")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailingWalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if location.as_ref().contains("/wal/") && self.failing.load(Ordering::SeqCst) {
            return Err(slatedb::object_store::Error::NotImplemented {
                operation: "put".to_string(),
                implementer: self.to_string(),
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

/// Proves a publication or discard commit whose WAL upload fails keeps its
/// acknowledged operations charged as uncertain and retries.
///
/// The failed upload closes the writer, so each case uses its own store and
/// leaves its writer unclosed.
async fn uncertain_commits_keep_their_charges() {
    for (name, family, retired) in [
        ("vector", QueueFamily::Vector, false),
        ("text", QueueFamily::Text, false),
        ("discard", QueueFamily::Vector, true),
    ] {
        let store = Arc::new(FailingWalStore::default());
        let db = open_explicit(
            &format!("queue-publication-uncertain-{name}"),
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &[vector_definition(), text_definition()],
        )
        .await;
        add(&db, document([0.0, 0.0], "alpha")).await;
        let target = target(&db, family).await;
        if retired {
            drop_index(&db, &vector_definition()).await;
        }
        store.failing.store(true, Ordering::SeqCst);
        let publisher = writer_publisher(&db);
        assert_eq!(
            publisher
                .publish_once(target)
                .await
                .expect("an uncertain commit retries"),
            PublicationOutcome::Retry,
            "{name}"
        );
        assert_eq!(load(&publisher.metrics().uncertain_commits), 1, "{name}");
        // One operation of each index stays charged; the attempted
        // acknowledgement may have committed.
        let stats = db.index_operation_queue_stats();
        assert_eq!(
            (stats.pending_operations, stats.uncertain_operations),
            (2, 1),
            "{name}"
        );
    }
}

/// Proves a writer that a newer writer fenced stops publishing: a
/// publication, text, or discard attempt returns the fatal fencing error
/// instead of retrying.
async fn fenced_commits_end_publication() {
    for (name, family, retired) in [
        ("vector", QueueFamily::Vector, false),
        ("text", QueueFamily::Text, false),
        ("discard", QueueFamily::Vector, true),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = format!("queue-publication-fenced-{name}");
        let db = open_explicit(
            &database,
            Arc::clone(&store),
            &[vector_definition(), text_definition()],
        )
        .await;
        add(&db, document([0.0, 0.0], "alpha")).await;
        let target = target(&db, family).await;
        if retired {
            drop_index(&db, &vector_definition()).await;
        }
        let newer = HelixDB::open_with_object_store_for_index_lifecycle_testing(
            database.as_str(),
            store,
            DbConfig::new(),
            LifecycleTestScheduling::Explicit,
        )
        .await
        .expect("a newer writer opens and fences the old one");
        // The fence surfaces at the attempt's commit, or at its first read if
        // the old writer already observed the newer epoch; either is fatal.
        let fenced = writer_publisher(&db).publish_once(target).await;
        assert!(
            matches!(&fenced, Err(HelixDbError::WriterFencedCommitOutcomeUnknown))
                || matches!(
                    &fenced,
                    Err(HelixDbError::Storage(error))
                        if error.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
                ),
            "{name}: {fenced:?}"
        );
        newer.close().await.expect("the newer writer closes");
    }
}

/// Vector definition partitioned by tenant, so a tenant move changes the
/// physical namespace that places a node.
fn tenant_vector_definition() -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(
        VectorIndexDefinition::new_node("Doc", "embedding", 2, VectorDistanceMetric::Euclidean)
            .expect("fixture vector definition validates")
            .with_tenant_property("tenant")
            .expect("fixture vector tenant property validates"),
    )
    .expect("fixture vector definition converts to V2")
}

/// Text definition partitioned by tenant.
fn tenant_text_definition() -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(
        TextIndexDefinition::new_node("Doc", "body")
            .expect("fixture text definition validates")
            .with_tenant_property("tenant")
            .expect("fixture text tenant property validates"),
    )
    .expect("fixture text definition converts to V2")
}

/// Batch limits admitting one entity per publication; every other limit is
/// the default.
fn one_entity_limits() -> SearchIndexBatchLimits {
    let batch = SearchIndexBackfillLimits::default().batch();
    SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        batch.max_input_bytes(),
        batch.max_output_operations(),
        batch.max_output_bytes(),
        batch.max_single_vector_output_bytes(),
    )
    .expect("one-entity batch limits validate")
}

/// Text limits whose epochs publish one entity.
fn one_entity_text_limits() -> ActiveTextMutationLimits {
    let defaults = SearchIndexBackfillLimits::default();
    ActiveTextMutationLimits::from_backfill(
        SearchIndexBackfillLimits::try_new(
            one_entity_limits(),
            NonZeroUsize::MIN,
            defaults.text_artifacts(),
            defaults.text_compaction(),
        )
        .expect("one-entity text limits validate"),
    )
}

/// A live document's tenant, embedding, and body.
type LiveDocuments = BTreeMap<u64, (&'static str, [f32; 2], String)>;

/// Commits an insert, an update, a tenant move, and a delete to both
/// families, returning the live documents.
async fn wal_workload(db: &HelixDB) -> LiveDocuments {
    let mut live = LiveDocuments::new();
    let mut ids = Vec::new();
    for (tenant, embedding, body) in [
        ("a", [0.0, 0.0], "alpha one"),
        ("a", [1.0, 1.0], "beta two"),
        ("b", [2.0, 2.0], "gamma three"),
        ("b", [3.0, 3.0], "delta four"),
    ] {
        let mut properties = document(embedding, body);
        properties.push(("tenant", PropertyInput::from(tenant.to_string())));
        let id = add(db, properties).await;
        ids.push(id);
        live.insert(id, (tenant, embedding, body.to_string()));
    }
    Box::pin(
        db.query(QueryRequest::write(
            batch::write_batch().var_as(
                "updated",
                traversal::g()
                    .n(NodeRef::from(ids[0]))
                    .set_property("embedding", vec![0.5_f32, 4.0])
                    .set_property("body", "alpha revised".to_string()),
            ),
        )),
    )
    .await
    .expect("document update commits");
    live.insert(ids[0], ("a", [0.5, 4.0], "alpha revised".to_string()));
    Box::pin(
        db.query(QueryRequest::write(
            batch::write_batch().var_as(
                "moved",
                traversal::g()
                    .n(NodeRef::from(ids[1]))
                    .set_property("tenant", "b".to_string()),
            ),
        )),
    )
    .await
    .expect("tenant move commits");
    live.get_mut(&ids[1]).expect("moved document is live").0 = "b";
    remove(db, ids[2]).await;
    live.remove(&ids[2]);
    live
}

/// Hit IDs of one search request in rank order.
async fn search_ids(
    db: &HelixDB,
    request: QueryRequest,
    consistency: SearchConsistency,
) -> Vec<u64> {
    let result = Box::pin(
        db.query(
            request
                .with_search_consistency(consistency)
                .expect("read requests accept a search consistency"),
        ),
    )
    .await
    .expect("search runs");
    if result["hits"].is_null() {
        return Vec::new();
    }
    result["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("search returned {result}"))
        .iter()
        .map(|hit| hit["$id"].as_u64().expect("hit ID"))
        .collect()
}

/// Asserts tenant-restricted searches equal exact search over `live`: vector
/// rank order when `vector` is set, and text hit sets.
async fn assert_searches_exact(
    db: &HelixDB,
    live: &LiveDocuments,
    consistency: SearchConsistency,
    vector: bool,
) {
    for tenant in ["a", "b"] {
        let owned = live
            .iter()
            .filter(|(_, (owner, _, _))| *owner == tenant)
            .collect::<Vec<_>>();
        if vector {
            for query in [[0.13_f32, 0.29], [2.71, 3.17], [0.5, 3.9]] {
                let mut exact = owned
                    .iter()
                    .map(|(id, (_, embedding, _))| {
                        (
                            (embedding[0] - query[0]).powi(2) + (embedding[1] - query[1]).powi(2),
                            **id,
                        )
                    })
                    .collect::<Vec<_>>();
                exact.sort_by(|left, right| left.partial_cmp(right).expect("finite distance"));
                let request = QueryRequest::read(
                    batch::read_batch()
                        .var_as(
                            "hits",
                            traversal::g().vector_search_nodes(
                                "Doc",
                                "embedding",
                                query.to_vec(),
                                10,
                                Some(PropertyValue::from(tenant)),
                            ),
                        )
                        .returning(["hits"]),
                );
                assert_eq!(
                    search_ids(db, request, consistency).await,
                    exact.into_iter().map(|(_, id)| id).collect::<Vec<_>>(),
                    "{consistency:?} vector search of tenant {tenant} at {query:?}"
                );
            }
        }
        for term in [
            "alpha", "revised", "one", "beta", "two", "gamma", "three", "delta", "four",
        ] {
            let exact = owned
                .iter()
                .filter(|(_, (_, _, body))| body.split(' ').any(|word| word == term))
                .map(|(id, _)| **id)
                .collect::<BTreeSet<_>>();
            let request = QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "hits",
                        traversal::g().text_search_nodes(
                            "Doc",
                            "body",
                            term,
                            10,
                            Some(PropertyValue::from(tenant)),
                        ),
                    )
                    .returning(["hits"]),
            );
            assert_eq!(
                search_ids(db, request, consistency)
                    .await
                    .into_iter()
                    .collect::<BTreeSet<_>>(),
                exact,
                "{consistency:?} text search of tenant {tenant} for {term:?}"
            );
        }
    }
}

/// Physical vector namespaces holding rows that place each node: its
/// vector, SimHash, layers, neighbor lists, or entry candidacy.
async fn vector_placements(db: &HelixDB) -> BTreeMap<u64, BTreeSet<u64>> {
    let storage = db.inner_db();
    let mut rows = storage
        .scan::<std::ops::RangeFull>(..)
        .await
        .expect("database scans");
    let mut placements = BTreeMap::<u64, BTreeSet<u64>>::new();
    while let Some(row) = rows.next().await.expect("row reads") {
        let Ok(DataKey::Data {
            kind: DataKeyKind::Vector(key),
            ..
        }) = DataKey::parse_from_slice(SCOPE, &row.key)
        else {
            continue;
        };
        let node = match key {
            VectorKey::Vector(placed) => placed.node_id(),
            VectorKey::SimHash(placed) => placed.node_id(),
            VectorKey::SimHashDirectory(placed) => placed.node_id(),
            VectorKey::UpperVector(placed) => placed.node_id(),
            VectorKey::UpperNeighbors(placed) => placed.node_id(),
            VectorKey::EntryCandidateSorted(placed) => placed.node_id(),
            // Both keys end in the node ID, which only test builds expose.
            VectorKey::Layer0Neighbors(_) | VectorKey::EntryCandidateNode(_) => u64::from_be_bytes(
                row.key[row.key.len() - 8..]
                    .try_into()
                    .expect("vector node keys end in an eight-byte node ID"),
            ),
            VectorKey::IndexMetadata(_)
            | VectorKey::IndexPrefix(_)
            | VectorKey::TxnGuard(_)
            | VectorKey::VectorPrefix(_)
            | VectorKey::SimHashDirectoryPrefix(_)
            | VectorKey::EntryCandidatePrefix(_)
            | VectorKey::MemoryPrefix(_)
            | VectorKey::L0Prefix(_)
            | VectorKey::ReverseEdgePrefix(_)
            | VectorKey::ReverseEdge(_) => continue,
        };
        placements.entry(node).or_default().insert(key.index_id());
    }
    placements
}

/// Entities holding a text statistics marker.
async fn text_marker_entities(db: &HelixDB) -> BTreeSet<u64> {
    let prefix = ManagedIndexKey::data_prefix(
        SCOPE,
        ScopedKey::logical_prefix(RecordKind::TextStatisticsEntity),
    );
    let storage = db.inner_db();
    let mut rows = storage
        .scan_prefix(&prefix, ..)
        .await
        .expect("statistics markers scan");
    let mut entities = BTreeSet::new();
    while let Some(row) = rows.next().await.expect("marker reads") {
        let ManagedIndexKey::Data {
            kind: ScopedKey::TextStatisticsEntity(marker),
            ..
        } = ManagedIndexKey::parse_from_slice(SCOPE, &row.key).expect("marker key parses")
        else {
            panic!("the marker prefix holds another key");
        };
        entities.insert(marker.entity.id.get());
    }
    entities
}

/// Paths of the SSTs `store` holds outside the WAL: memtable flushes and
/// compaction output.
async fn flushed_tables(store: &Arc<dyn ObjectStore>) -> BTreeSet<String> {
    use futures::TryStreamExt;
    store
        .list(None)
        .try_filter(|meta| futures::future::ready(meta.location.as_ref().contains("/compacted/")))
        .map_ok(|meta| meta.location.to_string())
        .try_collect()
        .await
        .expect("object store lists")
}

/// How a writer holding WAL-only queue work loses its store.
#[derive(Debug, Clone, Copy)]
enum WriterLoss {
    /// A newer writer fences it.
    Fenced,
    /// A publication or discard WAL upload fails; a new writer opens once the
    /// store heals.
    FailedWal,
}

/// Proves queue work that only the WAL holds survives a writer that loses
/// its store mid-publication.
///
/// Runs apart from [`run`]: its overlaid searches over a replayed writer
/// need a larger stack than a default debug-build test thread.
///
/// For each family, and for a retired generation's discard, a writer
/// commits an insert, an update, a tenant move, and a delete to
/// tenant-partitioned vector and text indexes, publishes one entity under
/// one-entity limits, and loses its store during the next attempt. Nothing
/// flushes the memtable, so the next writer recovers every queue by WAL
/// replay: it charges exactly the decoded queues and strong search is exact
/// before publication. Once drained, every queue is gone, eventual search is
/// exact, each live node's vector rows sit in exactly one partition, and only
/// live documents keep text statistics markers.
pub(crate) async fn wal_only_queue_work_survives_fencing_and_failed_commits() {
    for loss in [WriterLoss::Fenced, WriterLoss::FailedWal] {
        for (name, family, retired) in [
            ("vector", QueueFamily::Vector, false),
            ("text", QueueFamily::Text, false),
            ("discard", QueueFamily::Vector, true),
        ] {
            let case = format!("{loss:?}/{name}");
            let failing = Arc::new(FailingWalStore::default());
            let store: Arc<dyn ObjectStore> = Arc::clone(&failing) as Arc<dyn ObjectStore>;
            let database = format!("queue-publication-wal-only-{loss:?}-{name}");
            let db = Box::pin(open_explicit(
                &database,
                Arc::clone(&store),
                &[tenant_vector_definition(), tenant_text_definition()],
            ))
            .await;
            let flushed = flushed_tables(&store).await;
            assert!(!flushed.is_empty(), "{case}: the building writer flushed");
            let live = Box::pin(wal_workload(&db)).await;
            let targets = [
                target(&db, QueueFamily::Vector).await,
                target(&db, QueueFamily::Text).await,
            ];
            let target = target(&db, family).await;
            let before = queued(&db, target).await.len();
            assert!(
                matches!(
                    Box::pin(
                        publisher_with_limits(&db, one_entity_limits(), one_entity_text_limits())
                            .publish_once(target)
                    )
                    .await
                    .expect("the partial attempt publishes"),
                    PublicationOutcome::Published { entities: 1, .. }
                ),
                "{case}"
            );
            let remaining = queued(&db, target).await.len();
            assert!(
                remaining > 0 && remaining < before,
                "{case}: the partial attempt published part of the queue"
            );
            let dropped = if retired {
                Some(drop_index(&db, &tenant_vector_definition()).await)
            } else {
                None
            };
            let mut charged = 0;
            for pending in db.index_operation_backlog().outstanding_targets() {
                charged += queued(&db, pending).await.len() as u64;
            }
            assert_eq!(
                db.index_operation_queue_stats().pending_operations,
                charged,
                "{case}"
            );
            assert_eq!(
                flushed_tables(&store).await,
                flushed,
                "{case}: nothing flushed the memtable, so only the WAL holds the queue work"
            );

            let next = match loss {
                WriterLoss::Fenced => {
                    let newer =
                        Box::pin(HelixDB::open_with_object_store_for_index_lifecycle_testing(
                            database.as_str(),
                            Arc::clone(&store),
                            DbConfig::new(),
                            LifecycleTestScheduling::Explicit,
                        ))
                        .await
                        .expect("a newer writer opens and fences the old one");
                    let fenced = Box::pin(writer_publisher(&db).publish_once(target)).await;
                    assert!(
                        matches!(&fenced, Err(HelixDbError::WriterFencedCommitOutcomeUnknown))
                            || matches!(
                                &fenced,
                                Err(HelixDbError::Storage(error)) if error.kind()
                                    == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
                            ),
                        "{case}: {fenced:?}"
                    );
                    newer
                }
                WriterLoss::FailedWal => {
                    failing.failing.store(true, Ordering::SeqCst);
                    assert_eq!(
                        Box::pin(writer_publisher(&db).publish_once(target))
                            .await
                            .expect("an uncertain commit retries"),
                        PublicationOutcome::Retry,
                        "{case}"
                    );
                    assert_eq!(
                        load(&writer_publisher(&db).metrics().uncertain_commits),
                        1,
                        "{case}"
                    );
                    failing.failing.store(false, Ordering::SeqCst);
                    Box::pin(HelixDB::open_with_object_store_for_index_lifecycle_testing(
                        database.as_str(),
                        Arc::clone(&store),
                        DbConfig::new(),
                        LifecycleTestScheduling::Explicit,
                    ))
                    .await
                    .expect("a writer opens on the healed store")
                }
            };
            drop(db);
            let db = next;

            let mut decoded = 0;
            for pending in db.index_operation_backlog().outstanding_targets() {
                decoded += queued(&db, pending).await.len() as u64;
            }
            let stats = db.index_operation_queue_stats();
            assert_eq!(
                (
                    stats.pending_operations,
                    stats.discovered_operations,
                    stats.uncertain_operations
                ),
                (decoded, decoded, 0),
                "{case}: the new writer charges exactly the decoded queues"
            );
            assert_eq!(
                decoded, charged,
                "{case}: WAL replay restored the queue the lost attempt saw"
            );
            Box::pin(assert_searches_exact(
                &db,
                &live,
                SearchConsistency::Strong,
                !retired,
            ))
            .await;

            assert_eq!(
                Box::pin(db.publish_index_queues_for_lifecycle_testing())
                    .await
                    .expect("the recovered queues drain"),
                decoded,
                "{case}"
            );
            for pending in targets {
                assert!(
                    writer_publisher(&db)
                        .store
                        .read(db.inner_db().as_ref(), pending)
                        .await
                        .expect("queue reads")
                        .is_none(),
                    "{case}: the drained queue is gone"
                );
            }
            assert!(
                db.index_operation_backlog()
                    .outstanding_targets()
                    .is_empty(),
                "{case}"
            );
            Box::pin(assert_searches_exact(
                &db,
                &live,
                SearchConsistency::Eventual,
                !retired,
            ))
            .await;
            if let Some(dropped) = dropped {
                Box::pin(drive(&db, dropped)).await;
            }
            let placements = vector_placements(&db).await;
            if retired {
                assert!(
                    placements.is_empty(),
                    "{case}: cleanup reclaimed the dropped generation: {placements:?}"
                );
            } else {
                assert_eq!(
                    placements.keys().copied().collect::<Vec<_>>(),
                    live.keys().copied().collect::<Vec<_>>(),
                    "{case}: exactly the live nodes are placed"
                );
                assert!(
                    placements.values().all(|partitions| partitions.len() == 1),
                    "{case}: a node is placed in two partitions: {placements:?}"
                );
            }
            assert_eq!(
                text_marker_entities(&db).await,
                live.keys().copied().collect::<BTreeSet<_>>(),
                "{case}: only live documents keep text markers"
            );
            db.close().await.expect("the recovered writer closes");
        }
    }
}
