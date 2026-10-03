//! Production contracts for the immutable vector/text index-operation queue.
//!
//! These feature-gated contracts run the compiled queue against real storage:
//!
//! - the admission ledger's closed charge lifecycle, including every
//!   transition a cancelled or unanswered commit can take;
//! - reconciliation of those outcomes by the writer's own publisher, against
//!   operations committed through the production queue store exactly as a
//!   foreground write stages them;
//! - fail-closed decoding of stored queue values, merge-operand composition
//!   in SlateDB, and operand construction;
//! - writer opens that must refuse queues their catalog cannot own.
//!
//! Writers that publish open with explicit lifecycle scheduling, so no
//! background worker publishes or reconciles between a contract's steps.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use helix_ast::graph::NodeRef;
use helix_ast::query::{QueryRequest, SearchConsistency};
use helix_ast::value::PropertyInput;
use helix_ast::{batch, traversal};
use helix_planner::ir;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::IsolationLevel;

use crate::config::{
    DbConfig, SecondaryIndexDefinition, TextIndexDefinition, VectorIndexDefinition,
};
use crate::encoding::v2::keys::scope::{DataScope, TenantId, TENANT_KEY_PREFIX};
use crate::encoding::v2::keys::{
    IndexEntity, IndexOperationRowKey, ManagedIndexKey, RecordKind, ScopedKey,
};
use crate::encoding::v2::values::decode_index_record;
use crate::encoding::v2::values::indexes::operation_queue as codec;
use crate::error::HelixDbError;
use crate::index_lifecycle::queue::backlog::{
    BacklogLimits, BacklogReservation, IndexOperationBacklog, OperationCharge,
};
use crate::index_lifecycle::queue::publication::PublicationOutcome;
use crate::index_lifecycle::queue::QueueTarget;
use crate::index_lifecycle::{
    IndexDdlReceipt, IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexOperationId,
    IndexOperationStatus, TextPartition, ValidatedDynamicIndexDefinition,
};
use crate::index_lifecycle_testing::{
    LifecycleTestController, LifecycleTestScheduling, LifecycleWorkTarget,
};
use crate::search::vector::VectorDistanceMetric;
use crate::HelixDB;

const LABEL: &str = "Doc";
const EMBEDDING: &str = "embedding";
const BODY: &str = "body";
const VECTOR: u8 = 0x01;
const TEXT: u8 = 0x02;
const IF_ABSENT: u8 = 0x01;
const SET: u8 = 0x02;

/// Proves the retained-operation ledger's admission and outcome lifecycle.
///
/// A detached ledger covers the orderings a writer cannot schedule on
/// demand: duplicate identities, a transaction spanning two generations, a
/// reconciliation that must ignore uncertainty marked after its ticket, and
/// an acknowledgement that proves an uncertain enqueue durable.
pub fn index_operation_queue_ledger_contracts() {
    let target = |generation| {
        QueueTarget::new(
            DataScope::LegacyUnscoped,
            IndexId::new(1).expect("index ID is nonzero"),
            IndexGenerationId::new(generation).expect("generation is nonzero"),
        )
    };
    let charge = |target, entity, bytes| OperationCharge {
        target,
        entity: node(entity),
        id: codec::QueuedOperationId::generate(),
        bytes,
    };
    let invariant = |result: crate::error::Result<BacklogReservation>, expected: &str| {
        let Err(HelixDbError::InvariantViolation(message)) = &result else {
            panic!("reservation must fail with {expected:?}: {result:?}");
        };
        assert!(message.contains(expected), "{message}");
    };
    let backlog = IndexOperationBacklog::new(
        BacklogLimits {
            max_retained_bytes: 1_000,
            max_members: 10,
        },
        crate::index_lifecycle::worker::IndexWorkerWakeHandle::default(),
    );

    // One identity is reserved at most once, within or across transactions.
    let first = charge(target(1), 1, 10);
    invariant(backlog.reserve(&[first, first], &[]), "reserved twice");
    let staged = backlog.reserve(&[first], &[]).expect("a fresh identity");
    invariant(backlog.reserve(&[first], &[]), "reserved twice");
    // One catalog snapshot routes each logical index to one generation.
    invariant(
        backlog.reserve(&[charge(target(1), 2, 10), charge(target(2), 3, 10)], &[]),
        "two generations of one index",
    );
    assert!(backlog.has_charges(target(1)));
    // Dropping a reservation before commit submission is a definite abort,
    // and so is an explicit abort after it.
    drop(staged);
    let mut aborted = backlog.reserve(&[first], &[]).expect("the abort freed it");
    aborted.begin_commit();
    aborted.aborted();
    assert!(!backlog.has_charges(target(1)));
    assert!(backlog.outstanding_targets().is_empty());

    // Acknowledging an unknown identity changes nothing.
    backlog.acknowledge([codec::QueuedOperationId::generate()]);
    assert_eq!(backlog.totals().outcomes.acknowledged, 0);

    // Startup discovery charges each durable identity once.
    let durable = charge(target(1), 4, 25);
    let loaded = [(durable.id, durable.entity, durable.bytes)];
    backlog.load_durable(durable.target, loaded);
    backlog.load_durable(durable.target, loaded);
    assert_eq!(backlog.totals().usage.operations, 1);
    assert_eq!(backlog.totals().outcomes.discovered, 1);

    // An acknowledgement of an uncertain enqueue proves it durable.
    let proven = charge(target(1), 5, 10);
    let mut reservation = backlog.reserve(&[proven], &[]).expect("capacity");
    reservation.begin_commit();
    reservation.uncertain();
    assert!(backlog.has_uncertain(target(1)));
    backlog.acknowledge([proven.id]);
    assert!(!backlog.has_uncertain(target(1)));
    let outcomes = backlog.totals().outcomes;
    assert_eq!(
        (
            outcomes.discovered,
            outcomes.acknowledged,
            outcomes.acknowledged_censored
        ),
        (2, 1, 1)
    );

    // Uncertainty marked after a ticket survives that ticket's absent read;
    // presence proves an enqueue durable whenever it was marked.
    let ticket = backlog.begin_reconciliation();
    let late = charge(target(1), 6, 10);
    let mut reservation = backlog.reserve(&[late], &[]).expect("capacity");
    reservation.begin_commit();
    drop(reservation);
    assert_eq!(backlog.finish_reconciliation(ticket, target(1), []), 0);
    assert!(backlog.has_uncertain(target(1)));
    assert_eq!(
        backlog.finish_reconciliation(ticket, target(1), [(late.id, late.entity, late.bytes)]),
        0
    );
    assert!(!backlog.has_uncertain(target(1)));
    assert_eq!(backlog.finish_reconciliation(ticket, target(1), []), 0);
    assert_eq!(backlog.totals().outcomes.discovered, 3);

    // An uncertain acknowledgement of a locally committed operation keeps
    // its commit instant: it bounds the oldest pending age and times the
    // acknowledgement a later absent read proves.
    let timed = charge(target(1), 7, 10);
    backlog
        .reserve(&[timed], &[])
        .expect("capacity")
        .committed();
    backlog.mark_acknowledgement_uncertain([timed.id]);
    // A repeated uncertain acknowledgement keeps the observed commit.
    backlog.mark_acknowledgement_uncertain([timed.id]);
    std::thread::sleep(Duration::from_millis(2));
    let totals = backlog.totals();
    assert!(
        totals.oldest_committed_pending_micros >= 1_000,
        "{totals:?}"
    );
    assert_eq!(totals.usage.uncertain_operations, 1);
    let ticket = backlog.begin_reconciliation();
    assert_eq!(backlog.finish_reconciliation(ticket, target(1), []), 1);
    assert_eq!(backlog.lag().count(), 1);
    backlog.acknowledge([durable.id, late.id]);
    let totals = backlog.totals();
    assert_eq!(totals.usage, Default::default());
    assert_eq!(
        totals.outcomes.acknowledged - totals.outcomes.acknowledged_censored,
        backlog.lag().count()
    );
}

/// Proves that the writer's publisher settles every uncertain charge.
///
/// Each scenario commits real operations through the production queue store
/// and reports the commit outcome to the writer's ledger the way a
/// foreground transaction or a publication attempt would, including the
/// races where publication acknowledges before the producer's commit
/// returns. The final counters must account for every durable operation
/// exactly once.
pub async fn index_operation_queue_reconciliation_contracts() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_explicit_vector_fixture("queue-reconciliation", &store).await;
    let target = queue_target(&db, |definition| {
        matches!(definition, ValidatedDynamicIndexDefinition::Vector(_))
    })
    .await;
    let document = add_document(&db, [1.0, 0.0]).await;
    assert_eq!(publish(&db, target).await, published(1));

    // A cancelled commit that never committed is released; an operation
    // the ledger never charged is discovered by the same flushed read.
    set_embedding(&db, document, [1.0, 0.5]).await;
    let mut lost = db
        .index_operation_backlog()
        .reserve(
            &[OperationCharge {
                target,
                entity: node(document),
                id: codec::QueuedOperationId::generate(),
                bytes: 64,
            }],
            &[],
        )
        .expect("capacity");
    lost.begin_commit();
    drop(lost);
    commit_operation(&db, target, &fresh(&latest(&db, target).await)).await;
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (stats.pending_operations, stats.uncertain_operations),
        (2, 1)
    );
    assert_eq!(publish(&db, target).await, published(2));
    assert_eq!(db.index_operation_queue_stats().uncertain_operations, 0);

    // An uncertain enqueue that did commit is proven durable and published.
    set_embedding(&db, document, [0.0, 1.0]).await;
    let copy = fresh(&latest(&db, target).await);
    commit_reserved(&db, target, &copy).await.uncertain();
    assert_eq!(db.index_operation_queue_stats().uncertain_operations, 1);
    assert_eq!(publish(&db, target).await, published(2));

    // Publication acknowledges an operation before its producer observes
    // the commit return; the late success changes no charge.
    set_embedding(&db, document, [1.0, 1.0]).await;
    let copy = fresh(&latest(&db, target).await);
    let racing = commit_reserved(&db, target, &copy).await;
    assert_eq!(publish(&db, target).await, published(2));
    racing.committed();
    assert_eq!(db.index_operation_queue_stats().pending_operations, 0);

    // Uncertain acknowledgements whose flushed read still holds the
    // operations make them durable again, keeping how each was learned,
    // including one whose producer then reports success.
    set_embedding(&db, document, [2.0, 0.0]).await;
    let committed = latest(&db, target).await;
    let copy = fresh(&committed);
    let racing = commit_reserved(&db, target, &copy).await;
    db.index_operation_backlog()
        .mark_acknowledgement_uncertain([committed.id(), copy.id()]);
    racing.committed();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let stats = db.index_operation_queue_stats();
    assert_eq!(stats.uncertain_operations, 2);
    assert!(stats.oldest_pending_micros >= 1_000, "{stats:?}");
    assert_eq!(publish(&db, target).await, published(2));

    // An uncertain acknowledgement that did commit is counted as one and
    // times the lag from its observed commit.
    set_embedding(&db, document, [0.0, 2.0]).await;
    let settled = latest(&db, target).await;
    assert_eq!(publish(&db, target).await, published(1));
    let copy = fresh(&settled);
    commit_reserved(&db, target, &copy).await.committed();
    acknowledge_in_storage(&db, target, copy.id()).await;
    db.index_operation_backlog()
        .mark_acknowledgement_uncertain([copy.id()]);
    assert_eq!(publish(&db, target).await, PublicationOutcome::Empty);

    // An uncertain producer outcome after an uncertain acknowledgement, and
    // an uncertain acknowledgement of an uncertain enqueue, each prove one
    // discovery.
    let acknowledged_first = fresh(&settled);
    let reservation = commit_reserved(&db, target, &acknowledged_first).await;
    db.index_operation_backlog()
        .mark_acknowledgement_uncertain([acknowledged_first.id()]);
    reservation.uncertain();
    let enqueued_first = fresh(&settled);
    commit_reserved(&db, target, &enqueued_first)
        .await
        .uncertain();
    db.index_operation_backlog()
        .mark_acknowledgement_uncertain([enqueued_first.id()]);
    assert_eq!(db.index_operation_queue_stats().uncertain_operations, 2);
    assert_eq!(publish(&db, target).await, published(2));

    // Every durable operation is counted once as committed or discovered
    // and acknowledged once, timed or censored.
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.committed_operations,
            stats.discovered_operations,
            stats.acknowledged_operations,
            stats.censored_acknowledgements,
            stats.published_operations,
        ),
        (9, 4, 13, 6, 12),
        "{stats:?}"
    );
    assert_eq!(db.index_operation_publication_lag().count(), 7);
    assert_eq!(
        (
            stats.pending_operations,
            stats.uncertain_operations,
            stats.retained_bytes,
            stats.uncertain_commits,
        ),
        (0, 0, 0, 0)
    );
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    assert!(queued(&db, target).await.is_empty());
    db.close().await.expect("reconciliation fixture closes");
}

/// Proves that stored queue values, merge operands, and operand
/// construction fail closed on every noncanonical shape.
///
/// Stored values are read through the production queue store, which is the
/// read publication, recovery, and search overlays share. Merge operands go
/// through SlateDB and the Helix merge operator.
pub async fn index_operation_queue_codec_contracts() {
    let db = HelixDB::open_with_object_store_and_config(
        "queue-codec",
        Arc::new(InMemory::new()),
        DbConfig::new(),
    )
    .await
    .expect("codec fixture opens");
    let storage = db.inner_db();
    let body = vector_body([1.0, 2.0]);
    let header_then = |tail: &[u8]| [&[0x01, 0x14, VECTOR][..], tail].concat();
    let mut short_body = [0x01, 0x14, VECTOR, 0x00, 0x01, IF_ABSENT].to_vec();
    short_body.extend_from_slice(&1_u128.to_be_bytes());
    short_body.extend_from_slice(&[0x0A, 1, 2, 3]);
    let long_varint = |last| header_then(&[[0xFF; 9].as_slice(), &[last]].concat());
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (vec![0x01], "Buffer too short"),
        (
            vec![0x02, 0x14, VECTOR, 0, 0],
            "unsupported operation queue value version",
        ),
        (vec![0x01, 0x13, VECTOR, 0, 0], "Unexpected V2 value kind"),
        (
            vec![0x01, 0x14, 0x03, 0, 0],
            "unknown queued operation family",
        ),
        (
            raw_value(VECTOR, &[], &[(0x03, 1, &body)]),
            "unknown queued insert mode",
        ),
        (
            raw_value(VECTOR, &[], &[(IF_ABSENT, 9, &body), (IF_ABSENT, 9, &body)]),
            "inserts one operation ID twice",
        ),
        (
            raw_value(VECTOR, &[], &[]),
            "resolved operation queue is empty instead of absent",
        ),
        (header_then(&[0x00, 0x05]), "Buffer too short"),
        (
            raw_value(VECTOR, &[], &[(IF_ABSENT, (1 << 127) | 1, &body)]),
            "reserved entity-token bit",
        ),
        (
            raw_value(VECTOR, &[], &[(IF_ABSENT, 1, &[0x03, 0x05, 0x00, 0x00])]),
            "unknown queued entity kind",
        ),
        (
            raw_value(
                VECTOR,
                &[],
                &[(IF_ABSENT, 1, &[0x01, 0x05, 0x00, 0x01, 0x01, 0x00])],
            ),
            "must not be empty",
        ),
        (
            raw_value(
                VECTOR,
                &[],
                &[(IF_ABSENT, 1, &[&body[..5], &[0x04], &body[6..]].concat())],
            ),
            "Buffer too short",
        ),
        (
            raw_value(
                VECTOR,
                &[],
                &[(IF_ABSENT, 1, &vector_body([f32::NAN, 1.0]))],
            ),
            "not finite",
        ),
        (
            raw_value(VECTOR, &[], &[(IF_ABSENT, 1, &[0x01, 0x05, 0x00, 0x02])]),
            "noncanonical queued option tag",
        ),
        (
            raw_value(TEXT, &[], &[(IF_ABSENT, 1, &[0x01, 0x05, 0x02])]),
            "noncanonical queued option tag",
        ),
        (
            raw_value(
                VECTOR,
                &[],
                &[(IF_ABSENT, 1, &[0x01, 0x05, 0x02, 0x00, 0x00])],
            ),
            "tenant partition value must not be empty",
        ),
        (
            raw_value(VECTOR, &[], &[(IF_ABSENT, 1, &[0x01, 0x05, 0x03, 0x00])]),
            "unknown queued partition tag",
        ),
        (
            raw_value(
                VECTOR,
                &[],
                &[(IF_ABSENT, 1, &[body.as_slice(), &[0xAA]].concat())],
            ),
            "queued operation body has 1 trailing bytes",
        ),
        (
            [raw_value(VECTOR, &[], &[(IF_ABSENT, 1, &body)]), vec![0xAA]].concat(),
            "operation queue value has 1 trailing bytes",
        ),
        (long_varint(0x02), "queued varint overflows u64"),
        (header_then(&[0x80, 0x00]), "not minimally encoded"),
        (long_varint(0x81), "queued varint is too long"),
        (short_body, "Buffer too short"),
        (
            raw_value(VECTOR, &[3], &[]),
            "resolved operation queue retains acknowledgements",
        ),
        (
            raw_value(VECTOR, &[], &[(SET, 4, &body)]),
            "resolved operation queue retains an unconditional set",
        ),
        (
            raw_value(
                TEXT,
                &[],
                &[(IF_ABSENT, 1, &[0x01, 0x05, 0x01, 0x01, 0x01, 0xFF])],
            ),
            "Invalid UTF-8",
        ),
    ];
    for (index, (value, expected)) in (1_u64..).zip(cases) {
        let target = raw_target(index);
        storage
            .put(target.key(), &value)
            .await
            .expect("raw queue value writes");
        let Err(error) = db.index_queue_store().read(storage.as_ref(), target).await else {
            panic!("case {index} decoded a noncanonical queue value {value:02x?}");
        };
        assert!(
            error.to_string().contains(expected),
            "case {index}: {error} does not mention {expected:?}"
        );
    }

    // Merge composition over real SlateDB operands: a repeated insert keeps
    // the first body, a removal then insert re-inserts, and an unconditional
    // set replaces a live insert.
    let first = vector_operation(1, [1.0, 1.0]);
    let repeated = codec::QueuedOperation::new(
        first.id(),
        first.entity(),
        vector_operation(1, [9.0, 9.0]).payload().clone(),
    );
    let kept = raw_target(100);
    commit_operation(&db, kept, &first).await;
    commit_operation(&db, kept, &repeated).await;
    assert_eq!(queued(&db, kept).await, vec![first.clone()]);

    let reinserted = raw_target(101);
    commit_operation(&db, reinserted, &first).await;
    acknowledge_in_storage(&db, reinserted, first.id()).await;
    commit_operation(&db, reinserted, &repeated).await;
    assert_eq!(queued(&db, reinserted).await, vec![repeated.clone()]);

    let replaced = raw_target(102);
    commit_operation(&db, replaced, &first).await;
    let transaction = storage
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("transaction begins");
    transaction
        .merge_disjoint_tokens(
            replaced.key(),
            [first.id().token()],
            raw_value(
                VECTOR,
                &[],
                &[(SET, first.id().get(), &vector_body([4.0, 4.0]))],
            ),
        )
        .expect("set operand stages");
    transaction.commit().await.expect("set operand commits");
    let [set] = queued(&db, replaced)
        .await
        .try_into()
        .expect("one operation");
    assert_eq!(set.id(), first.id());
    let codec::QueuedPayload::Vector(payload) = set.payload() else {
        panic!("a vector queue decodes vector payloads");
    };
    assert_eq!(
        payload
            .replacement
            .as_ref()
            .map(codec::QueuedVectorReplacement::vector),
        Some(&[4.0_f32, 4.0][..])
    );

    // A checked merge validates against the resolved base: another family's
    // operand and a corrupt or noncanonical operand are refused before they
    // are staged.
    let text = text_operation(2);
    let text_operand =
        codec::QueueOperand::enqueue(std::slice::from_ref(&text)).expect("text operand encodes");
    for (operand, expected) in [
        (text_operand.bytes().clone(), "mixes index families"),
        (bytes::Bytes::from_static(&[0x01]), "Buffer too short"),
        (
            raw_value(VECTOR, &[2, 1], &[]).into(),
            "not strictly ascending",
        ),
        (
            raw_value(VECTOR, &[7], &[(IF_ABSENT, 7, &body)]).into(),
            "both removes and inserts",
        ),
        (
            raw_value(VECTOR, &[], &[(IF_ABSENT, 9, &body), (SET, 9, &body)]).into(),
            "inserts one operation ID twice",
        ),
        (
            raw_value(VECTOR, &[(1 << 127) | 1], &[]).into(),
            "reserved entity-token bit",
        ),
        (
            raw_value(
                VECTOR,
                &[],
                &[(IF_ABSENT, 1, &[0x01, 0x05, 0x02, 0x00, 0x00])],
            )
            .into(),
            "outside 1..=",
        ),
    ] {
        let transaction = storage
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("transaction begins");
        let Err(error) = transaction
            .merge_disjoint_tokens_checked(kept.key(), [text.id().token()], operand)
            .await
        else {
            panic!("an operand that cannot compose was staged");
        };
        assert!(format!("{error:?}").contains(expected), "{error:?}");
        transaction.rollback();
    }
    let second = vector_operation(2, [2.0, 2.0]);
    let (bytes, tokens) = codec::QueueOperand::enqueue(std::slice::from_ref(&second))
        .expect("vector operand encodes")
        .into_parts();
    let transaction = storage
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("transaction begins");
    transaction
        .merge_disjoint_tokens_checked(kept.key(), tokens, bytes)
        .await
        .expect("a canonical operand passes validation");
    transaction.commit().await.expect("checked merge commits");
    assert_eq!(queued(&db, kept).await, vec![first.clone(), second.clone()]);
    // A value without records is what a partial merge stores once each
    // acknowledgement it composed cancelled its own enqueue: it composes as
    // the identity.
    let transaction = storage
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("transaction begins");
    transaction
        .merge_disjoint_tokens_checked(
            kept.key(),
            [codec::QueuedOperationId::generate().token()],
            raw_value(VECTOR, &[], &[]),
        )
        .await
        .expect("the identity passes validation");
    transaction.commit().await.expect("identity merge commits");
    assert_eq!(queued(&db, kept).await, vec![first.clone(), second]);

    // Operand construction rejects batches no producer may stage.
    let operand_error = |operations: &[codec::QueuedOperation]| {
        codec::QueueOperand::enqueue(operations)
            .expect_err("invalid enqueue batch")
            .to_string()
    };
    assert!(operand_error(&[]).contains("at least one operation"));
    assert!(operand_error(&[first.clone(), text]).contains("mixes index families"));
    assert!(operand_error(&[first.clone(), first.clone()]).contains("repeats an operation ID"));
    assert!(operand_error(&[first.clone(), fresh(&first)]).contains("repeats an entity"));
    let acknowledgement_error = |ids: Vec<codec::QueuedOperationId>| {
        codec::QueueOperand::acknowledge(codec::QueueFamily::Vector, ids)
            .expect_err("invalid acknowledgement")
            .to_string()
    };
    assert!(acknowledgement_error(vec![first.id(), first.id()]).contains("repeats"));
    assert!(acknowledgement_error(Vec::new()).contains("at least one operation"));
    for (vector, expected) in [
        (Vec::new(), "must not be empty"),
        (vec![1.0, f32::INFINITY], "component 1 is not finite"),
    ] {
        let error = codec::QueuedVectorReplacement::try_new(
            TextPartition::Unpartitioned,
            Arc::from(vector),
        )
        .expect_err("invalid replacement");
        assert!(error.to_string().contains(expected), "{error}");
    }
    assert!(codec::QueuedOperationId::try_from_u128(1 << 127).is_err());
    db.close().await.expect("codec fixture closes");
}

/// Proves that a writer refuses to open over queues it cannot own or read.
///
/// Admission starts from exact durable usage, so a queue the writer cannot
/// attribute to its canonical definition, or cannot read in its own layout,
/// fails the open instead of silently dropping or misrouting work.
pub async fn index_operation_queue_recovery_corruption_contracts() {
    // A secondary index never owns a queue.
    let secondary = ValidatedDynamicIndexDefinition::try_from(
        SecondaryIndexDefinition::node_equality(LABEL, "status")
            .expect("secondary definition validates"),
    )
    .expect("secondary definition converts");
    let error = reopen_after("queue-owner-secondary", vec![secondary], |db| async move {
        let target = queue_target(&db, |definition| {
            matches!(definition, ValidatedDynamicIndexDefinition::Secondary(_))
        })
        .await;
        commit_operation(&db, target, &vector_operation(1, [1.0, 1.0])).await;
        db
    })
    .await;
    assert_corruption(&error, "owns an operation queue");

    // A queue must match its owner's family and element kind.
    for (name, operation) in [
        ("queue-owner-family", text_operation(1)),
        (
            "queue-owner-element",
            codec::QueuedOperation::new(
                codec::QueuedOperationId::generate(),
                IndexEntity {
                    kind: IndexElementKind::Edge,
                    id: IndexEntityId::new(1),
                },
                vector_operation(1, [1.0, 1.0]).payload().clone(),
            ),
        ),
    ] {
        let error = reopen_after(name, vec![vector_definition()], |db| async move {
            let target = queue_target(&db, |definition| {
                matches!(definition, ValidatedDynamicIndexDefinition::Vector(_))
            })
            .await;
            commit_operation(&db, target, &operation).await;
            db
        })
        .await;
        assert_corruption(&error, "does not match its canonical definition");
    }

    // Raw rows no product writer produces fail the open closed.
    let queue_prefix = ManagedIndexKey::data_prefix(
        DataScope::LegacyUnscoped,
        ScopedKey::logical_prefix(RecordKind::IndexOperationQueue),
    );
    let row_key = ManagedIndexKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: ScopedKey::IndexOperationRow(IndexOperationRowKey {
            index_id: IndexId::new(1).expect("index ID is nonzero"),
            generation: IndexGenerationId::initial(),
            sequence: 0,
        }),
    }
    .to_bytes();
    for (name, key, value, expected) in [
        (
            "queue-foreign-key",
            [queue_prefix.as_ref(), &[0x00]].concat(),
            vec![0x01],
            "operation queue prefix holds another key",
        ),
        (
            "queue-row-layout",
            row_key.to_vec(),
            vec![0x01],
            "written with a layout other than Map",
        ),
        (
            "queue-tenant-envelope",
            vec![TENANT_KEY_PREFIX, 0x01],
            vec![0x01],
            "tenant discovery encountered an invalid envelope",
        ),
        (
            "queue-undecodable",
            raw_target(1).key().to_vec(),
            vec![0x01],
            "Buffer too short",
        ),
    ] {
        let error = reopen_after(name, Vec::new(), |db| async move {
            db.inner_db()
                .put(&key, &value)
                .await
                .expect("raw row writes");
            db
        })
        .await;
        assert!(error.to_string().contains(expected), "{name}: {error}");
    }
}

/// One tenant scope's live documents: embedding and body by node ID.
type ScopeDocuments = BTreeMap<u64, ([f32; 2], String)>;

/// Proves tenant scopes queue, publish, recover, and retire independently.
///
/// Two tenant scopes hold identical vector and text definitions. Inserts,
/// updates, and deletes interleave across both scopes; strong search in each
/// sees only its own pending documents. Publishing one scope leaves the
/// other's queues pending, a reopened writer rediscovers exactly what is
/// left, and dropping one scope's vector index discards and releases only
/// that scope's queued work.
pub async fn index_operation_queue_tenant_scope_contracts() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let name = "queue-tenant-scopes";
    let db = Box::pin(open_explicit(name, &store)).await;
    let scopes = [1_u128, 2].map(|tenant| DataScope::Tenant(TenantId::from_u128(tenant)));
    let mut targets = Vec::new();
    for scope in scopes {
        for definition in [vector_definition(), text_definition()] {
            let receipt = LifecycleTestController
                .create_index(&db, scope, definition, ir::IndexCreateMode::ErrorIfExists)
                .await
                .expect("create is accepted");
            let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
                panic!("a new definition starts a build: {receipt:?}");
            };
            Box::pin(drive_scoped(&db, scope, operation_id)).await;
        }
        targets.push(scoped_targets(&db, scope).await);
    }
    let [first, second] = <[[QueueTarget; 2]; 2]>::try_from(targets).expect("two scopes");
    // Index IDs are allocated database-wide, so only the scope tells the
    // queues of identical definitions apart from each other's charges.
    assert!(first.iter().all(|target| !second.contains(target)));

    let mut documents = [ScopeDocuments::new(), ScopeDocuments::new()];
    for round in 0..3_u8 {
        for (ordinal, scope) in scopes.into_iter().enumerate() {
            let embedding = [
                f32::from(round) * 2.0 + ordinal as f32 * 0.5,
                ordinal as f32 + f32::from(round) * 0.25,
            ];
            let body = format!("shared round{round} scope{ordinal}");
            let id = Box::pin(scoped_insert(&db, scope, embedding, &body)).await;
            documents[ordinal].insert(id, (embedding, body));
        }
    }
    for (ordinal, scope) in scopes.into_iter().enumerate() {
        let ids = documents[ordinal].keys().copied().collect::<Vec<_>>();
        let (updated, deleted) = (ids[ordinal], ids[1 - ordinal]);
        let embedding = [7.25 + ordinal as f32, 0.75];
        let body = format!("shared revised scope{ordinal}");
        Box::pin(scoped_update(&db, scope, updated, embedding, &body)).await;
        documents[ordinal].insert(updated, (embedding, body));
        Box::pin(scoped_delete(&db, scope, deleted)).await;
        documents[ordinal].remove(&deleted);
    }
    for (ordinal, scope) in scopes.into_iter().enumerate() {
        Box::pin(assert_scope_searches_exact(
            &db,
            scope,
            &documents[ordinal],
            SearchConsistency::Strong,
        ))
        .await;
    }

    // Publishing the first scope leaves the second's queues and charges.
    let pending_second = queued_per_target(&db, second).await;
    assert!(pending_second.iter().all(|operations| *operations > 0));
    let charged = |db: &HelixDB, targets: [QueueTarget; 2]| {
        targets.map(|target| db.index_operation_backlog().has_charges(target))
    };
    for target in first {
        loop {
            match Box::pin(publish(&db, target)).await {
                PublicationOutcome::Published { .. } => {}
                PublicationOutcome::Empty => break,
                outcome @ (PublicationOutcome::Discarded { .. }
                | PublicationOutcome::Deferred
                | PublicationOutcome::Retry
                | PublicationOutcome::Trimmed
                | PublicationOutcome::Blocked
                | PublicationOutcome::Stalled) => {
                    panic!("the first scope did not publish: {outcome:?}")
                }
            }
        }
    }
    assert_eq!(queued_per_target(&db, first).await, [0, 0]);
    assert_eq!(queued_per_target(&db, second).await, pending_second);
    assert_eq!(
        (charged(&db, first), charged(&db, second)),
        ([false, false], [true, true])
    );
    assert_eq!(
        db.index_operation_queue_stats().pending_operations,
        pending_second.iter().sum::<u64>(),
        "only the first scope's charges were released"
    );
    for (ordinal, scope) in scopes.into_iter().enumerate() {
        Box::pin(assert_scope_searches_exact(
            &db,
            scope,
            &documents[ordinal],
            SearchConsistency::Strong,
        ))
        .await;
    }
    Box::pin(assert_scope_searches_exact(
        &db,
        scopes[0],
        &documents[0],
        SearchConsistency::Eventual,
    ))
    .await;
    db.close().await.expect("tenant scope writer closes");

    // A reopened writer rediscovers exactly the second scope's queues.
    let db = Box::pin(open_explicit(name, &store)).await;
    let remaining = pending_second.iter().sum::<u64>();
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (stats.pending_operations, stats.discovered_operations),
        (remaining, remaining)
    );
    assert_eq!(
        db.index_operation_backlog()
            .outstanding_targets()
            .into_iter()
            .collect::<BTreeSet<_>>(),
        second.into_iter().collect::<BTreeSet<_>>()
    );
    assert_eq!(
        (charged(&db, first), charged(&db, second)),
        ([false, false], [true, true])
    );
    assert_eq!(
        Box::pin(db.publish_index_queues_for_lifecycle_testing())
            .await
            .expect("the second scope publishes"),
        remaining
    );
    for (ordinal, scope) in scopes.into_iter().enumerate() {
        for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
            Box::pin(assert_scope_searches_exact(
                &db,
                scope,
                &documents[ordinal],
                consistency,
            ))
            .await;
        }
    }

    // Dropping the first scope's vector index discards only its queue.
    for (ordinal, scope) in scopes.into_iter().enumerate() {
        let embedding = [11.5 + ordinal as f32, 2.25];
        let body = format!("shared late scope{ordinal}");
        let id = Box::pin(scoped_insert(&db, scope, embedding, &body)).await;
        documents[ordinal].insert(id, (embedding, body));
        let updated = *documents[ordinal].keys().next().expect("a live document");
        let embedding = [0.5, 9.5 + ordinal as f32];
        let body = format!("shared final scope{ordinal}");
        Box::pin(scoped_update(&db, scope, updated, embedding, &body)).await;
        documents[ordinal].insert(updated, (embedding, body));
    }
    let pending_first = queued_per_target(&db, first).await;
    let pending_second = queued_per_target(&db, second).await;
    assert!(pending_first
        .iter()
        .chain(&pending_second)
        .all(|operations| *operations > 0));
    let receipt = LifecycleTestController
        .drop_index(&db, scopes[0], &vector_definition())
        .await
        .expect("drop is accepted");
    let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
        panic!("dropping an Active index starts cleanup: {receipt:?}");
    };
    Box::pin(drive_scoped(&db, scopes[0], operation_id)).await;
    assert_eq!(
        Box::pin(publish(&db, first[0])).await,
        PublicationOutcome::Discarded {
            operations: pending_first[0]
        }
    );
    assert_eq!(
        Box::pin(publish(&db, first[0])).await,
        PublicationOutcome::Empty
    );
    assert_eq!(
        (charged(&db, first), charged(&db, second)),
        ([false, true], [true, true])
    );
    assert_eq!(queued_per_target(&db, first).await, [0, pending_first[1]]);
    assert_eq!(queued_per_target(&db, second).await, pending_second);
    assert_eq!(
        db.index_operation_queue_stats().pending_operations,
        pending_first[1] + pending_second.iter().sum::<u64>(),
        "only the first scope's vector charges were released"
    );
    assert_eq!(
        db.index_operation_queue_stats().discarded_operations,
        pending_first[0]
    );
    assert_eq!(
        Box::pin(db.publish_index_queues_for_lifecycle_testing())
            .await
            .expect("the remaining queues publish"),
        pending_first[1] + pending_second.iter().sum::<u64>()
    );
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        Box::pin(assert_scope_searches_exact(
            &db,
            scopes[1],
            &documents[1],
            consistency,
        ))
        .await;
        Box::pin(assert_scope_text_exact(
            &db,
            scopes[0],
            &documents[0],
            consistency,
        ))
        .await;
    }
    db.close().await.expect("tenant scope writer closes");
}

/// Opens an explicitly scheduled writer, so only the contract publishes.
async fn open_explicit(name: &str, store: &Arc<dyn ObjectStore>) -> HelixDB {
    HelixDB::open_with_object_store_for_index_lifecycle_testing(
        name,
        Arc::clone(store),
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("explicit writer opens")
}

/// Steps one scoped operation until it succeeds.
async fn drive_scoped(db: &HelixDB, scope: DataScope, operation_id: IndexOperationId) {
    for _ in 0..256 {
        match db
            .get_index_operation(scope, operation_id)
            .await
            .expect("operation status reads")
        {
            IndexOperationStatus::Succeeded { .. } => return,
            IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. } => {
                LifecycleTestController
                    .advance(
                        db,
                        LifecycleWorkTarget::Operation {
                            scope,
                            operation_id,
                        },
                    )
                    .await
                    .expect("operation step runs");
            }
            status @ (IndexOperationStatus::Blocked { .. }
            | IndexOperationStatus::Aborted { .. }) => {
                panic!("scoped operation did not succeed: {status:?}")
            }
        }
    }
    panic!("scoped operation exceeded its step bound");
}

/// Returns the vector and text queue targets of `scope`'s canonical records.
async fn scoped_targets(db: &HelixDB, scope: DataScope) -> [QueueTarget; 2] {
    let prefix =
        ManagedIndexKey::data_prefix(scope, ScopedKey::logical_prefix(RecordKind::IndexRecord));
    let storage = db.inner_db();
    let mut rows = storage
        .scan_prefix(&prefix, ..)
        .await
        .expect("index records scan");
    let (mut vector, mut text) = (None, None);
    while let Some(row) = rows.next().await.expect("index record reads") {
        let record = decode_index_record(&row.value).expect("index record decodes");
        let target = QueueTarget::new(scope, record.index_id(), record.state().generation());
        match record.definition() {
            ValidatedDynamicIndexDefinition::Vector(_) => vector = Some(target),
            ValidatedDynamicIndexDefinition::Text(_) => text = Some(target),
            ValidatedDynamicIndexDefinition::Secondary(_) => {}
        }
    }
    [
        vector.expect("the scope has a vector index"),
        text.expect("the scope has a text index"),
    ]
}

async fn queued_per_target(db: &HelixDB, targets: [QueueTarget; 2]) -> [u64; 2] {
    let mut lengths = [0; 2];
    for (length, target) in lengths.iter_mut().zip(targets) {
        *length = queued(db, target).await.len() as u64;
    }
    lengths
}

async fn scoped_insert(db: &HelixDB, scope: DataScope, embedding: [f32; 2], body: &str) -> u64 {
    let created = Box::pin(
        db.query_scoped(
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "created",
                        traversal::g().add_n(
                            LABEL,
                            vec![
                                (EMBEDDING, PropertyInput::from(embedding.to_vec())),
                                (BODY, PropertyInput::from(body.to_string())),
                            ],
                        ),
                    )
                    .returning(["created"]),
            ),
            scope,
        ),
    )
    .await
    .expect("scoped insert commits");
    created["created"][0]["$id"]
        .as_u64()
        .expect("created node ID")
}

async fn scoped_update(db: &HelixDB, scope: DataScope, id: u64, embedding: [f32; 2], body: &str) {
    Box::pin(
        db.query_scoped(
            QueryRequest::write(
                batch::write_batch().var_as(
                    "updated",
                    traversal::g()
                        .n(NodeRef::from(id))
                        .set_property(EMBEDDING, embedding.to_vec())
                        .set_property(BODY, body.to_string()),
                ),
            ),
            scope,
        ),
    )
    .await
    .expect("scoped update commits");
}

async fn scoped_delete(db: &HelixDB, scope: DataScope, id: u64) {
    Box::pin(db.query_scoped(
        QueryRequest::write(
            batch::write_batch().var_as("dropped", traversal::g().n(NodeRef::from(id)).drop()),
        ),
        scope,
    ))
    .await
    .expect("scoped delete commits");
}

/// Hit IDs of one scoped search in rank order.
async fn scoped_hits(
    db: &HelixDB,
    scope: DataScope,
    request: QueryRequest,
    consistency: SearchConsistency,
) -> Vec<u64> {
    let result = Box::pin(
        db.query_scoped(
            request
                .with_search_consistency(consistency)
                .expect("read requests accept a search consistency"),
            scope,
        ),
    )
    .await
    .expect("scoped search runs");
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

/// Asserts `scope`'s vector search ranks exactly its own documents and its
/// text search matches exactly its own documents.
async fn assert_scope_searches_exact(
    db: &HelixDB,
    scope: DataScope,
    documents: &ScopeDocuments,
    consistency: SearchConsistency,
) {
    for query in [[0.37_f32, 0.11], [4.91, 1.29], [7.93, 0.61], [0.41, 9.83]] {
        let mut exact = documents
            .iter()
            .map(|(id, (embedding, _))| {
                (
                    (embedding[0] - query[0]).powi(2) + (embedding[1] - query[1]).powi(2),
                    *id,
                )
            })
            .collect::<Vec<_>>();
        exact.sort_by(|left, right| left.partial_cmp(right).expect("finite distance"));
        let request = QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g().vector_search_nodes(LABEL, EMBEDDING, query.to_vec(), 20, None),
                )
                .returning(["hits"]),
        );
        assert_eq!(
            scoped_hits(db, scope, request, consistency).await,
            exact.into_iter().map(|(_, id)| id).collect::<Vec<_>>(),
            "{scope:?} {consistency:?} vector search at {query:?}"
        );
    }
    assert_scope_text_exact(db, scope, documents, consistency).await;
}

/// Asserts `scope`'s text search matches exactly its own documents.
async fn assert_scope_text_exact(
    db: &HelixDB,
    scope: DataScope,
    documents: &ScopeDocuments,
    consistency: SearchConsistency,
) {
    let terms = documents
        .values()
        .flat_map(|(_, body)| body.split(' ').map(str::to_string))
        .chain(["scope0", "scope1", "round0", "revised"].map(str::to_string))
        .collect::<BTreeSet<_>>();
    for term in terms {
        let exact = documents
            .iter()
            .filter(|(_, (_, body))| body.split(' ').any(|word| word == term))
            .map(|(id, _)| *id)
            .collect::<BTreeSet<_>>();
        let request = QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g().text_search_nodes(LABEL, BODY, term.as_str(), 20, None),
                )
                .returning(["hits"]),
        );
        assert_eq!(
            scoped_hits(db, scope, request, consistency)
                .await
                .into_iter()
                .collect::<BTreeSet<_>>(),
            exact,
            "{scope:?} {consistency:?} text search for {term:?}"
        );
    }
}

fn text_definition() -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(
        TextIndexDefinition::new_node(LABEL, BODY).expect("text definition validates"),
    )
    .expect("text definition converts")
}

/// Opens a writer, installs `definitions`, runs `corrupt`, closes, and
/// returns the error the next writer open fails with.
async fn reopen_after<F, Fut>(
    name: &str,
    definitions: Vec<ValidatedDynamicIndexDefinition>,
    corrupt: F,
) -> HelixDbError
where
    F: FnOnce(HelixDB) -> Fut,
    Fut: std::future::Future<Output = HelixDB>,
{
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = HelixDB::open_with_object_store_and_config(name, Arc::clone(&store), DbConfig::new())
        .await
        .expect("corruption fixture opens");
    for definition in definitions {
        db.install_index_for_tests(definition)
            .await
            .expect("fixture index activates");
    }
    let db = corrupt(db).await;
    db.close().await.expect("corruption fixture closes");
    let Err(error) = HelixDB::open_with_object_store_and_config(name, store, DbConfig::new()).await
    else {
        panic!("{name}: the writer opened over a queue it cannot own");
    };
    error
}

fn assert_corruption(error: &HelixDbError, expected: &str) {
    assert!(
        matches!(error, HelixDbError::IndexCatalogCorruption(message) if message.contains(expected)),
        "{error}"
    );
}

/// Installs an Active vector index, then reopens with explicit scheduling so
/// only the contract publishes.
async fn open_explicit_vector_fixture(name: &str, store: &Arc<dyn ObjectStore>) -> HelixDB {
    let db = HelixDB::open_with_object_store_and_config(name, Arc::clone(store), DbConfig::new())
        .await
        .expect("vector fixture opens");
    db.install_index_for_tests(vector_definition())
        .await
        .expect("vector index activates");
    db.close().await.expect("vector fixture closes");
    HelixDB::open_with_object_store_for_index_lifecycle_testing(
        name,
        Arc::clone(store),
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("explicit vector fixture opens")
}

fn vector_definition() -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(
        VectorIndexDefinition::new_node(LABEL, EMBEDDING, 2, VectorDistanceMetric::Euclidean)
            .expect("vector definition validates"),
    )
    .expect("vector definition converts")
}

/// Returns the queue of the only canonical record whose definition matches.
async fn queue_target(
    db: &HelixDB,
    matches: impl Fn(&ValidatedDynamicIndexDefinition) -> bool,
) -> QueueTarget {
    let prefix = ManagedIndexKey::data_prefix(
        DataScope::LegacyUnscoped,
        ScopedKey::logical_prefix(RecordKind::IndexRecord),
    );
    let storage = db.inner_db();
    let mut rows = storage
        .scan_prefix(&prefix, ..)
        .await
        .expect("index records scan");
    while let Some(row) = rows.next().await.expect("index record reads") {
        let record = decode_index_record(&row.value).expect("index record decodes");
        if matches(record.definition()) {
            return QueueTarget::new(
                DataScope::LegacyUnscoped,
                record.index_id(),
                record.state().generation(),
            );
        }
    }
    panic!("the fixture index is installed");
}

/// A queue key that no canonical record owns.
fn raw_target(index: u64) -> QueueTarget {
    QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(1_000 + index).expect("index ID is nonzero"),
        IndexGenerationId::initial(),
    )
}

async fn publish(db: &HelixDB, target: QueueTarget) -> PublicationOutcome {
    db.inner
        .index_queue_publisher
        .as_ref()
        .expect("writers own a publisher")
        .publish_once(target)
        .await
        .expect("publication attempt completes")
}

const fn published(operations: u64) -> PublicationOutcome {
    PublicationOutcome::Published {
        operations,
        entities: 1,
    }
}

async fn queued(db: &HelixDB, target: QueueTarget) -> Vec<codec::QueuedOperation> {
    db.index_queue_store()
        .read(db.inner_db().as_ref(), target)
        .await
        .expect("queue reads")
        .map_or_else(Vec::new, |stored| stored.queue().operations().to_vec())
}

/// Returns the most recently committed operation of `target`.
async fn latest(db: &HelixDB, target: QueueTarget) -> codec::QueuedOperation {
    queued(db, target)
        .await
        .pop()
        .expect("the write queued an operation")
}

/// Copies an operation's effect under a fresh identity, so publishing it
/// changes nothing a real write did not already change.
fn fresh(operation: &codec::QueuedOperation) -> codec::QueuedOperation {
    codec::QueuedOperation::new(
        codec::QueuedOperationId::generate(),
        operation.entity(),
        operation.payload().clone(),
    )
}

/// Reserves, stages, and commits `operation` in the order a foreground
/// write does, returning the reservation before its outcome is reported.
async fn commit_reserved(
    db: &HelixDB,
    target: QueueTarget,
    operation: &codec::QueuedOperation,
) -> BacklogReservation {
    let mut reservation = db
        .index_operation_backlog()
        .reserve(
            &[OperationCharge {
                target,
                entity: operation.entity(),
                id: operation.id(),
                bytes: operation.retained_bytes(),
            }],
            &[],
        )
        .expect("capacity is available");
    reservation.begin_commit();
    commit_operation(db, target, operation).await;
    reservation
}

/// Stages and commits one blind enqueue operand through the queue store.
async fn commit_operation(db: &HelixDB, target: QueueTarget, operation: &codec::QueuedOperation) {
    let transaction = db
        .inner_db()
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("transaction begins");
    db.index_queue_store()
        .stage_enqueue(
            &transaction,
            target,
            codec::QueueOperand::enqueue(std::slice::from_ref(operation)).expect("operand encodes"),
            std::slice::from_ref(operation),
        )
        .expect("enqueue stages");
    transaction.commit().await.expect("enqueue commits");
}

/// Commits the exact acknowledgement publication stages for `id`.
async fn acknowledge_in_storage(db: &HelixDB, target: QueueTarget, id: codec::QueuedOperationId) {
    let storage = db.inner_db();
    let stored = db
        .index_queue_store()
        .read(storage.as_ref(), target)
        .await
        .expect("queue reads")
        .expect("the operation is queued");
    let transaction = storage
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("transaction begins");
    db.index_queue_store()
        .stage_acknowledge(&transaction, target, &stored, &[id])
        .expect("acknowledgement stages");
    transaction.commit().await.expect("acknowledgement commits");
}

async fn add_document(db: &HelixDB, embedding: [f32; 2]) -> u64 {
    let created = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        LABEL,
                        vec![(EMBEDDING, PropertyInput::from(embedding.to_vec()))],
                    ),
                )
                .returning(["created"]),
        ))
        .await
        .expect("document write commits");
    created["created"][0]["$id"]
        .as_u64()
        .expect("created node ID")
}

async fn set_embedding(db: &HelixDB, id: u64, embedding: [f32; 2]) {
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "updated",
            traversal::g()
                .n(NodeRef::from(id))
                .set_property(EMBEDDING, embedding.to_vec()),
        ),
    ))
    .await
    .expect("embedding update commits");
}

const fn node(id: u64) -> IndexEntity {
    IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(id),
    }
}

fn vector_operation(entity: u64, components: [f32; 2]) -> codec::QueuedOperation {
    codec::QueuedOperation::new(
        codec::QueuedOperationId::generate(),
        node(entity),
        codec::QueuedPayload::Vector(codec::QueuedVectorPayload {
            previous: None,
            replacement: Some(
                codec::QueuedVectorReplacement::try_new(
                    TextPartition::Unpartitioned,
                    Arc::from(components),
                )
                .expect("finite replacement"),
            ),
        }),
    )
}

fn text_operation(entity: u64) -> codec::QueuedOperation {
    codec::QueuedOperation::new(
        codec::QueuedOperationId::generate(),
        node(entity),
        codec::QueuedPayload::Text(codec::QueuedTextPayload {
            replacement: Some(codec::QueuedTextReplacement::new(
                TextPartition::Unpartitioned,
                Arc::from("queued words"),
            )),
        }),
    )
}

/// Encodes one raw queue value; every count and length stays below 128, so
/// each varint is one byte.
fn raw_value(family: u8, removes: &[u128], inserts: &[(u8, u128, &[u8])]) -> Vec<u8> {
    let mut bytes = vec![
        0x01,
        0x14,
        family,
        u8::try_from(removes.len()).expect("small count"),
    ];
    removes
        .iter()
        .for_each(|id| bytes.extend_from_slice(&id.to_be_bytes()));
    bytes.push(u8::try_from(inserts.len()).expect("small count"));
    for (mode, id, body) in inserts {
        bytes.push(*mode);
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.push(u8::try_from(body.len()).expect("small body"));
        bytes.extend_from_slice(body);
    }
    bytes
}

/// Node 5 replaced by an unpartitioned two-dimensional vector.
fn vector_body(components: [f32; 2]) -> Vec<u8> {
    let mut body = vec![0x01, 0x05, 0x00, 0x01, 0x01, 0x02];
    components
        .iter()
        .for_each(|component| body.extend_from_slice(&component.to_bits().to_be_bytes()));
    body
}
