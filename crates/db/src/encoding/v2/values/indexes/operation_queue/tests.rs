//! Codec, reference-model, and real-storage contracts for the operation queue.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::{BufMut, Bytes};
use proptest::prelude::*;

use super::algebra::InsertMode;
use super::*;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{
    IndexOperationQueueKey, IndexOperationRowKey, ManagedIndexKey, ScopedKey,
};
use crate::index_lifecycle::{IndexGenerationId, IndexId};

fn node(id: u64) -> IndexEntity {
    IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(id),
    }
}

fn id(value: u128) -> QueuedOperationId {
    QueuedOperationId::try_from_u128(value).expect("test operation IDs keep bit 127 clear")
}

fn tenant(value: &[u8]) -> TextPartition {
    TextPartition::try_tenant_value(Bytes::copy_from_slice(value)).expect("non-empty tenant")
}

fn text_operation(operation: u128, entity: u64, text: Option<&str>) -> QueuedOperation {
    QueuedOperation::new(
        id(operation),
        node(entity),
        QueuedPayload::Text(QueuedTextPayload {
            replacement: text.map(|text| {
                QueuedTextReplacement::new(TextPartition::Unpartitioned, Arc::from(text))
            }),
        }),
    )
}

fn vector_operation(
    operation: u128,
    entity: u64,
    previous: Option<TextPartition>,
    replacement: Option<(TextPartition, Vec<f32>)>,
) -> QueuedOperation {
    QueuedOperation::new(
        id(operation),
        node(entity),
        QueuedPayload::Vector(QueuedVectorPayload {
            previous,
            replacement: replacement.map(|(partition, vector)| {
                QueuedVectorReplacement::try_new(partition, Arc::from(vector))
                    .expect("finite test vector")
            }),
        }),
    )
}

fn resolve(operands: &[Bytes]) -> Option<OperationQueue> {
    match merge_with_base(None, operands).expect("valid operands resolve") {
        QueueMergeResult::Value(value) => {
            Some(OperationQueue::decode(&value).expect("resolved values decode"))
        }
        QueueMergeResult::Empty => None,
    }
}

fn ids_of(queue: Option<&OperationQueue>) -> Vec<u128> {
    queue.map_or_else(Vec::new, |queue| {
        queue
            .operations()
            .iter()
            .map(|operation| operation.id().get())
            .collect()
    })
}

#[test]
fn every_payload_shape_round_trips_with_exact_retained_bytes() {
    let operations = vec![
        vector_operation(1, 0, None, None),
        vector_operation(2, 1, Some(TextPartition::Unpartitioned), None),
        vector_operation(
            3,
            2,
            Some(tenant(b"old-tenant")),
            Some((tenant(b"new-tenant"), vec![0.0, -1.5, f32::MAX])),
        ),
        vector_operation(
            4,
            u64::MAX,
            None,
            Some((TextPartition::Unpartitioned, vec![-0.0])),
        ),
    ];
    let operand = QueueOperand::enqueue(&operations).expect("vector operand encodes");
    let queue = resolve(std::slice::from_ref(operand.bytes())).expect("queue is non-empty");
    assert_eq!(queue.family(), QueueFamily::Vector);
    assert_eq!(queue.operations(), operations.as_slice());
    let header = HEADER_LEN + 1 + varint_len(operations.len() as u64);
    assert_eq!(
        operand.bytes().len(),
        header
            + operations
                .iter()
                .map(|operation| usize::try_from(operation.retained_bytes()).unwrap())
                .sum::<usize>()
    );
    // Negative zero keeps its exact bit pattern.
    let QueuedPayload::Vector(payload) = queue.operations()[3].payload() else {
        panic!("vector family decodes vector payloads");
    };
    assert_eq!(
        payload.replacement.as_ref().unwrap().vector()[0].to_bits(),
        (-0.0_f32).to_bits()
    );

    let text = vec![
        text_operation(10, 0, None),
        text_operation(11, 1, Some("")),
        text_operation(12, 300, Some("héllo wörld")),
        QueuedOperation::new(
            id(13),
            IndexEntity {
                kind: IndexElementKind::Edge,
                id: IndexEntityId::new(7),
            },
            QueuedPayload::Text(QueuedTextPayload {
                replacement: Some(QueuedTextReplacement::new(
                    tenant(&[0xFF, 0x00]),
                    Arc::from("edge text"),
                )),
            }),
        ),
    ];
    let operand = QueueOperand::enqueue(&text).expect("text operand encodes");
    let queue = resolve(std::slice::from_ref(operand.bytes())).expect("queue is non-empty");
    assert_eq!(queue.family(), QueueFamily::Text);
    assert_eq!(queue.operations(), text.as_slice());
}

#[test]
fn enqueue_and_acknowledge_tokens_use_disjoint_namespaces() {
    let operation = text_operation(5, 9, Some("a"));
    let enqueue = QueueOperand::enqueue(std::slice::from_ref(&operation)).unwrap();
    let entity_token = entity_enqueue_token(node(9));
    assert_eq!(enqueue.tokens(), &[5, entity_token]);
    assert_ne!(entity_token & (1 << 127), 0);
    let acknowledge = QueueOperand::acknowledge(QueueFamily::Text, [id(5)]).unwrap();
    assert_eq!(acknowledge.tokens(), &[5]);
    // A newer enqueue for the same entity shares no token with the older ACK.
    let newer = QueueOperand::enqueue(&[text_operation(6, 9, Some("b"))]).unwrap();
    assert!(newer
        .tokens()
        .iter()
        .all(|token| !acknowledge.tokens().contains(token)));
    // Edge and node entities with one numeric ID never share a token.
    assert_ne!(
        entity_enqueue_token(node(9)),
        entity_enqueue_token(IndexEntity {
            kind: IndexElementKind::Edge,
            id: IndexEntityId::new(9),
        })
    );
    // Generated IDs always keep the entity namespace bit clear.
    for _ in 0..256 {
        assert_eq!(QueuedOperationId::generate().get() & (1 << 127), 0);
    }
}

#[test]
fn acknowledgement_capacity_fits_its_operand_bound() {
    // 5 fixed bytes plus 16 per ID: 63 IDs take 1013 bytes, 64 take 1029.
    assert_eq!(QueueOperand::acknowledgement_capacity(1024), 63);
    assert_eq!(QueueOperand::acknowledgement_capacity(20), 0);
    assert_eq!(QueueOperand::acknowledgement_capacity(21), 1);
    for max_bytes in (0..4_096).chain([8 * 1024 * 1024]) {
        let capacity = QueueOperand::acknowledgement_capacity(max_bytes);
        if capacity == 0 {
            assert!(max_bytes < 21, "one ID fits {max_bytes} bytes");
            continue;
        }
        let ids = |count: u64| (1..=u128::from(count)).map(id);
        let fitting = QueueOperand::acknowledge(QueueFamily::Text, ids(capacity)).unwrap();
        assert!(fitting.bytes().len() as u64 <= max_bytes, "{max_bytes}");
        let beyond = QueueOperand::acknowledge(QueueFamily::Text, ids(capacity + 2)).unwrap();
        assert!(beyond.bytes().len() as u64 > max_bytes, "{max_bytes}");
    }
}

#[test]
fn producer_operands_reject_invalid_shapes() {
    assert!(QueueOperand::enqueue(&[]).is_err());
    assert!(QueueOperand::enqueue(&[
        text_operation(1, 1, Some("a")),
        text_operation(1, 2, Some("b")),
    ])
    .is_err());
    assert!(QueueOperand::enqueue(&[
        text_operation(1, 1, Some("a")),
        text_operation(2, 1, Some("b")),
    ])
    .is_err());
    assert!(QueueOperand::enqueue(&[
        text_operation(1, 1, Some("a")),
        vector_operation(2, 2, None, None),
    ])
    .is_err());
    assert!(QueueOperand::acknowledge(QueueFamily::Text, []).is_err());
    assert!(QueueOperand::acknowledge(QueueFamily::Text, [id(1), id(1)]).is_err());
    assert!(QueuedVectorReplacement::try_new(TextPartition::Unpartitioned, Arc::from([])).is_err());
    assert!(QueuedVectorReplacement::try_new(
        TextPartition::Unpartitioned,
        Arc::from([1.0, f32::NAN])
    )
    .is_err());
    assert!(QueuedVectorReplacement::try_new(
        TextPartition::Unpartitioned,
        Arc::from([f32::INFINITY])
    )
    .is_err());
    assert!(QueuedOperationId::try_from_u128(1 << 127).is_err());
}

/// Builds one arbitrary canonical value for algebra tests.
fn raw_value(
    family: QueueFamily,
    removes: &[u128],
    inserts: &[(InsertMode, u128, Vec<u8>)],
) -> Bytes {
    let mut removes = removes.to_vec();
    removes.sort_unstable();
    let mut bytes = Vec::new();
    put_header(&mut bytes, family);
    put_varint(&mut bytes, removes.len() as u64);
    for id in removes {
        bytes.put_slice(&id.to_be_bytes());
    }
    put_varint(&mut bytes, inserts.len() as u64);
    for (mode, id, body) in inserts {
        bytes.put_u8(*mode as u8);
        bytes.put_slice(&id.to_be_bytes());
        put_varint(&mut bytes, body.len() as u64);
        bytes.put_slice(body);
    }
    Bytes::from(bytes)
}

/// Encodes one valid text body.
fn text_body(entity: u64, text: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_body(&mut bytes, &text_operation(0, entity, Some(text)));
    bytes
}

#[test]
fn malformed_values_are_errors_not_empty_queues() {
    let body = text_body(1, "a");
    let valid = raw_value(
        QueueFamily::Text,
        &[],
        &[(InsertMode::IfAbsent, 1, body.clone())],
    );
    assert!(validate_operand(&valid).is_ok());

    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    cases.push(("empty", Vec::new()));
    cases.push(("truncated header", valid[0..2].to_vec()));
    let mut version = valid.to_vec();
    version[0] = 0x02;
    cases.push(("unknown version", version));
    for reference_kind in [0x16, 0x17, 0x18, 0x03] {
        let mut kind = valid.to_vec();
        kind[1] = reference_kind;
        cases.push(("foreign value kind", kind));
    }
    let mut family = valid.to_vec();
    family[2] = 0x03;
    cases.push(("unknown family", family));
    let mut trailing = valid.to_vec();
    trailing.push(0);
    cases.push(("trailing byte", trailing));
    let mut mode = valid.to_vec();
    mode[HEADER_LEN + 2] = 0x03;
    cases.push(("unknown mode", mode));
    let reserved_id = {
        let mut bytes = Vec::new();
        put_header(&mut bytes, QueueFamily::Text);
        put_varint(&mut bytes, 1);
        bytes.put_slice(&(1_u128 << 127).to_be_bytes());
        put_varint(&mut bytes, 0);
        bytes
    };
    cases.push(("reserved token bit", reserved_id));
    cases.push((
        "duplicate insert",
        raw_value(
            QueueFamily::Text,
            &[],
            &[
                (InsertMode::IfAbsent, 1, body.clone()),
                (InsertMode::Set, 1, body.clone()),
            ],
        )
        .to_vec(),
    ));
    cases.push((
        "remove and insert one ID",
        raw_value(
            QueueFamily::Text,
            &[1],
            &[(InsertMode::IfAbsent, 1, body.clone())],
        )
        .to_vec(),
    ));
    let unsorted = {
        let mut bytes = Vec::new();
        put_header(&mut bytes, QueueFamily::Text);
        put_varint(&mut bytes, 2);
        bytes.put_slice(&2_u128.to_be_bytes());
        bytes.put_slice(&1_u128.to_be_bytes());
        put_varint(&mut bytes, 0);
        bytes
    };
    cases.push(("unsorted removes", unsorted));
    let overlong = {
        let mut bytes = Vec::new();
        put_header(&mut bytes, QueueFamily::Text);
        bytes.put_slice(&[0x80, 0x00]);
        put_varint(&mut bytes, 0);
        bytes
    };
    cases.push(("overlong varint", overlong));
    cases.push(("huge count", {
        let mut bytes = Vec::new();
        put_header(&mut bytes, QueueFamily::Text);
        put_varint(&mut bytes, u64::MAX);
        bytes
    }));
    let mut bad_bodies: Vec<(&str, Vec<u8>)> = vec![
        ("unknown entity kind", vec![0x03, 0x01, 0x00]),
        ("truncated entity", vec![0x01]),
        ("text option tag", vec![0x01, 0x01, 0x02]),
        ("partition tag", vec![0x01, 0x01, 0x01, 0x03]),
        ("empty tenant", vec![0x01, 0x01, 0x01, 0x02, 0x00, 0x00]),
        (
            "invalid utf8",
            vec![0x01, 0x01, 0x01, 0x01, 0x02, 0xC3, 0x28],
        ),
        ("text trailing", {
            let mut body = text_body(1, "a");
            body.push(0);
            body
        }),
        ("text truncated", {
            let body = text_body(1, "abc");
            body[0..body.len() - 1].to_vec()
        }),
    ];
    for (name, body) in bad_bodies.drain(..) {
        cases.push((
            name,
            raw_value(QueueFamily::Text, &[], &[(InsertMode::IfAbsent, 1, body)]).to_vec(),
        ));
    }
    let vector_bodies: Vec<(&str, Vec<u8>)> = vec![
        ("zero dimension", vec![0x01, 0x01, 0x00, 0x01, 0x01, 0x00]),
        ("nan component", {
            let mut body = vec![0x01, 0x01, 0x00, 0x01, 0x01, 0x01];
            body.put_u32(f32::NAN.to_bits());
            body
        }),
        ("infinite component", {
            let mut body = vec![0x01, 0x01, 0x00, 0x01, 0x01, 0x01];
            body.put_u32(f32::NEG_INFINITY.to_bits());
            body
        }),
        (
            "short vector",
            vec![0x01, 0x01, 0x00, 0x01, 0x01, 0x02, 0, 0, 0, 0],
        ),
        ("previous tag", vec![0x01, 0x01, 0x05, 0x00]),
        ("replacement tag", vec![0x01, 0x01, 0x00, 0x02]),
    ];
    for (name, body) in vector_bodies {
        cases.push((
            name,
            raw_value(QueueFamily::Vector, &[], &[(InsertMode::IfAbsent, 1, body)]).to_vec(),
        ));
    }
    // A text body is invalid inside a vector queue.
    cases.push((
        "family body mismatch",
        raw_value(
            QueueFamily::Vector,
            &[],
            &[(InsertMode::IfAbsent, 1, body.clone())],
        )
        .to_vec(),
    ));

    for (name, bytes) in cases {
        assert!(validate_operand(&bytes).is_err(), "{name} must be rejected");
        assert!(
            merge_with_base(None, &[Bytes::from(bytes.clone())]).is_err(),
            "{name} must not resolve to an empty queue"
        );
        assert!(
            merge_with_base(Some(&bytes), std::slice::from_ref(&valid)).is_err(),
            "{name} must fail as a base"
        );
        assert!(
            merge_partial(Some(&valid), &[Bytes::from(bytes.clone())]).is_err(),
            "{name} must fail as a partial operand"
        );
        assert!(
            OperationQueue::decode(&bytes).is_err(),
            "{name} must not decode"
        );
    }

    // Resolved readers reject unresolved transformations.
    assert!(OperationQueue::decode(&raw_value(QueueFamily::Text, &[1], &[])).is_err());
    assert!(OperationQueue::decode(&raw_value(
        QueueFamily::Text,
        &[],
        &[(InsertMode::Set, 1, body.clone())]
    ))
    .is_err());
    // Mixed families never merge.
    let vector = raw_value(
        QueueFamily::Vector,
        &[],
        &[(InsertMode::IfAbsent, 2, vec![0x01, 0x01, 0x00, 0x00])],
    );
    assert!(merge_partial(Some(&valid), std::slice::from_ref(&vector)).is_err());
    assert!(merge_with_base(Some(&valid), &[vector]).is_err());
}

#[test]
fn acknowledgement_and_newer_enqueue_for_one_entity_commute() {
    let old = text_operation(1, 7, Some("old"));
    let new = text_operation(2, 7, Some("new"));
    let base = QueueOperand::enqueue(std::slice::from_ref(&old)).unwrap();
    let base = match merge_with_base(None, std::slice::from_ref(base.bytes())).unwrap() {
        QueueMergeResult::Value(value) => value,
        QueueMergeResult::Empty => unreachable!("one enqueue is outstanding"),
    };
    let ack = QueueOperand::acknowledge(QueueFamily::Text, [old.id()]).unwrap();
    let enqueue = QueueOperand::enqueue(std::slice::from_ref(&new)).unwrap();
    let first =
        merge_with_base(Some(&base), &[ack.bytes().clone(), enqueue.bytes().clone()]).unwrap();
    let second =
        merge_with_base(Some(&base), &[enqueue.bytes().clone(), ack.bytes().clone()]).unwrap();
    assert_eq!(first, second);
    let QueueMergeResult::Value(value) = first else {
        panic!("the newer operation remains outstanding");
    };
    assert_eq!(
        OperationQueue::decode(&value).unwrap().operations(),
        std::slice::from_ref(&new)
    );
}

#[test]
fn acknowledgements_remove_only_named_ids_and_never_resurrect() {
    let operations = (1..=4)
        .map(|operation| text_operation(operation, 1, Some("body")))
        .collect::<Vec<_>>();
    let enqueues = operations
        .iter()
        .map(|operation| {
            QueueOperand::enqueue(std::slice::from_ref(operation))
                .unwrap()
                .bytes()
                .clone()
        })
        .collect::<Vec<_>>();
    let ack_two = QueueOperand::acknowledge(QueueFamily::Text, [id(2)])
        .unwrap()
        .bytes()
        .clone();
    let mut sequence = enqueues.clone();
    sequence.push(ack_two.clone());
    assert_eq!(ids_of(resolve(&sequence).as_ref()), vec![1, 3, 4]);

    // An acknowledgement composed before its older base still removes it.
    let partial = merge_partial(None, std::slice::from_ref(&ack_two)).unwrap();
    let base = merge_partial(None, &enqueues).unwrap();
    assert_eq!(
        ids_of(resolve(&[base.clone(), partial]).as_ref()),
        vec![1, 3, 4]
    );
    // Acknowledging every ID empties the queue into a tombstone.
    let ack_all = QueueOperand::acknowledge(QueueFamily::Text, (1..=4).map(id))
        .unwrap()
        .bytes()
        .clone();
    assert_eq!(
        merge_with_base(None, &[base.clone(), ack_all.clone()]).unwrap(),
        QueueMergeResult::Empty
    );
    // An acknowledgement for an unseen ID leaves others intact.
    let ack_unknown = QueueOperand::acknowledge(QueueFamily::Text, [id(99)])
        .unwrap()
        .bytes()
        .clone();
    assert_eq!(
        ids_of(resolve(&[base, ack_unknown]).as_ref()),
        vec![1, 2, 3, 4]
    );
}

#[test]
fn acknowledge_then_reenqueue_resets_even_above_an_unresolved_base() {
    let original = text_operation(1, 3, Some("original"));
    let other = text_operation(2, 4, Some("other"));
    let reset = text_operation(1, 3, Some("reset"));
    let base = merge_partial(
        None,
        &[QueueOperand::enqueue(&[original, other.clone()])
            .unwrap()
            .bytes()
            .clone()],
    )
    .unwrap();
    // Compose ACK(1); ENQUEUE(1) without the base, as compaction would.
    let upper = merge_partial(
        None,
        &[
            QueueOperand::acknowledge(QueueFamily::Text, [id(1)])
                .unwrap()
                .bytes()
                .clone(),
            QueueOperand::enqueue(std::slice::from_ref(&reset))
                .unwrap()
                .bytes()
                .clone(),
        ],
    )
    .unwrap();
    let queue = resolve(&[base, upper]).expect("two operations remain");
    assert_eq!(queue.operations(), &[other, reset]);
}

#[test]
fn reused_ids_keep_the_first_retained_bytes_in_every_grouping() {
    let first = QueueOperand::enqueue(&[text_operation(1, 1, Some("a"))])
        .unwrap()
        .bytes()
        .clone();
    let reused = QueueOperand::enqueue(&[text_operation(1, 1, Some("b"))])
        .unwrap()
        .bytes()
        .clone();
    let expected = resolve(std::slice::from_ref(&first));
    assert_eq!(resolve(&[first.clone(), reused.clone()]), expected);
    let partial = merge_partial(None, &[first.clone(), reused.clone()]).unwrap();
    assert_eq!(resolve(&[partial]), expected);
    let base = merge_partial(None, std::slice::from_ref(&first)).unwrap();
    assert_eq!(resolve(&[base, reused]), expected);
    // An identical duplicate is idempotent.
    assert_eq!(ids_of(resolve(&[first.clone(), first]).as_ref()), vec![1]);
}

#[test]
fn an_acknowledgement_cancels_its_own_enqueue_without_a_base() {
    let enqueue = |operation: u128| {
        QueueOperand::enqueue(&[text_operation(operation, 1, Some("x"))])
            .unwrap()
            .bytes()
            .clone()
    };
    let ack = |ids: &[u128]| {
        QueueOperand::acknowledge(QueueFamily::Text, ids.iter().copied().map(id))
            .unwrap()
            .bytes()
            .clone()
    };
    // Rounds of enqueues and their acknowledgements composed with no base,
    // as upper compactions and read batches fold them: nothing accumulates.
    let rounds = (0..50_u128)
        .flat_map(|round| [enqueue(round + 10), ack(&[round + 10])])
        .collect::<Vec<_>>();
    let partial = merge_partial(None, &rounds).unwrap();
    assert_eq!(partial, raw_value(QueueFamily::Text, &[], &[]));
    // An acknowledgement whose enqueue lies below keeps its removal, and a
    // cancelled pair beside it adds nothing.
    let below = merge_partial(None, &[enqueue(1), enqueue(2)]).unwrap();
    let upper = merge_partial(None, &[ack(&[1]), enqueue(3), ack(&[3])]).unwrap();
    assert_eq!(upper, raw_value(QueueFamily::Text, &[1], &[]));
    assert_eq!(
        ids_of(resolve(&[below.clone(), upper.clone()]).as_ref()),
        vec![2]
    );
    assert_eq!(
        ids_of(
            resolve(&[merge_partial(Some(&below), std::slice::from_ref(&upper)).unwrap()]).as_ref()
        ),
        vec![2]
    );
    // A set also removed whatever it replaced below, so acknowledging it
    // keeps that removal.
    let reset = merge_partial(None, &[ack(&[1]), enqueue(1), ack(&[1])]).unwrap();
    assert_eq!(reset, raw_value(QueueFamily::Text, &[1], &[]));
    assert_eq!(ids_of(resolve(&[below, reset]).as_ref()), vec![2]);
}

#[test]
fn the_retained_byte_ceiling_keeps_every_queue_value_within_a_u32_length() {
    // A text deletion is the smallest operation; every other shape is larger.
    assert_eq!(
        text_operation(1, 0, None).retained_bytes(),
        MIN_RETAINED_RECORD_LEN as u64
    );
    assert!(vector_operation(1, 0, None, None).retained_bytes() > MIN_RETAINED_RECORD_LEN as u64);
    // The largest value a ceiling admits: a removal for each smallest
    // operation of one full backlog, plus a second full backlog of records.
    let largest_value = |ceiling: u64| {
        MAX_VALUE_FRAMING_LEN as u64
            + OPERATION_ID_LEN as u64 * (ceiling / MIN_RETAINED_RECORD_LEN as u64)
            + ceiling
    };
    assert!(largest_value(MAX_RETAINED_BYTES) <= u64::from(u32::MAX));
    // Exact up to one smallest operation.
    assert!(
        largest_value(MAX_RETAINED_BYTES + MIN_RETAINED_RECORD_LEN as u64) > u64::from(u32::MAX)
    );
}

#[test]
fn a_value_without_records_is_the_identity_of_composition() {
    let empty = raw_value(QueueFamily::Text, &[], &[]);
    let one = QueueOperand::enqueue(&[text_operation(1, 1, Some("a"))])
        .unwrap()
        .bytes()
        .clone();
    assert!(validate_operand(&empty).is_ok());
    assert_eq!(
        merge_with_base(None, std::slice::from_ref(&empty)).unwrap(),
        QueueMergeResult::Empty
    );
    assert_eq!(
        merge_partial(None, std::slice::from_ref(&empty)).unwrap(),
        empty
    );
    for operands in [
        vec![empty.clone(), one.clone()],
        vec![one.clone(), empty.clone()],
    ] {
        assert_eq!(
            merge_partial(None, &operands).unwrap(),
            merge_partial(None, std::slice::from_ref(&one)).unwrap()
        );
        assert_eq!(ids_of(resolve(&operands).as_ref()), vec![1]);
    }
    let QueueMergeResult::Value(resolved) =
        merge_with_base(Some(&one), std::slice::from_ref(&empty)).unwrap()
    else {
        panic!("the base operation remains");
    };
    assert_eq!(
        ids_of(Some(&OperationQueue::decode(&resolved).unwrap())),
        vec![1]
    );
    // A resolved empty queue is a tombstone, never a stored value.
    assert!(OperationQueue::decode(&empty).is_err());
    // Families still never mix through an empty value.
    let vector = raw_value(QueueFamily::Vector, &[], &[]);
    assert!(merge_partial(Some(&vector), std::slice::from_ref(&one)).is_err());
}

#[test]
fn latest_decodes_select_each_entity_at_its_latest_operation_within_the_budget() {
    // Entity 1 changes twice; its latest operation is the largest.
    let operations = [
        text_operation(1, 1, Some("first")),
        text_operation(3, 3, None),
        text_operation(2, 1, Some("a much longer latest state")),
    ];
    let [_, deleted, latest] = operations.clone();
    let (deleted_bytes, latest_bytes) = (deleted.retained_bytes(), latest.retained_bytes());
    assert!(operations[0].retained_bytes() < latest_bytes);
    let QueueMergeResult::Value(value) = merge_with_base(
        None,
        std::slice::from_ref(QueueOperand::enqueue(&operations[..2]).unwrap().bytes()),
    )
    .unwrap() else {
        panic!("two operations are outstanding");
    };
    let QueueMergeResult::Value(value) = merge_with_base(
        Some(&value),
        std::slice::from_ref(QueueOperand::enqueue(&operations[2..]).unwrap().bytes()),
    )
    .unwrap() else {
        panic!("three operations are outstanding");
    };
    let selected = |value: &[u8], budget| {
        let selection = LatestOperations::decode(value, budget).unwrap();
        let refused = selection.refused();
        (selection.into_operations(), refused)
    };
    // Entity 1 is selected at its latest state or not at all, never at the
    // first operation that alone would fit; entity 3 follows in first-seen
    // order and is never selected ahead of it. A selection the budget ends
    // reports what the first entity it left out would have reached.
    assert_eq!(
        selected(&value, latest_bytes - 1),
        (Vec::new(), Some(latest_bytes))
    );
    let both_bytes = latest_bytes + deleted_bytes;
    assert_eq!(
        selected(&value, latest_bytes),
        (vec![latest.clone()], Some(both_bytes))
    );
    assert_eq!(
        selected(&value, both_bytes - 1),
        (vec![latest.clone()], Some(both_bytes))
    );
    let both = vec![latest.clone(), deleted.clone()];
    assert_eq!(selected(&value, both_bytes), (both.clone(), None));
    assert_eq!(selected(&value, u64::MAX), (both, None));
    assert_eq!(
        OperationQueue::decode(&value).unwrap().operations(),
        operations.as_slice()
    );

    // Neither a superseded payload nor one past the budget is decoded; a
    // budget that selects a corrupt payload, and a full decode, fail closed.
    let corrupt_payload = |entity: u8| vec![0x01, entity, 0x7F, 0x7F];
    let corrupt = raw_value(
        QueueFamily::Text,
        &[],
        &[
            (InsertMode::IfAbsent, 1, corrupt_payload(1)),
            (InsertMode::IfAbsent, 3, text_body(3, "kept")),
            (
                InsertMode::IfAbsent,
                2,
                text_body(1, "a much longer latest state"),
            ),
            (
                InsertMode::IfAbsent,
                4,
                [corrupt_payload(4), vec![0x7F; 96]].concat(),
            ),
        ],
    );
    let small = text_operation(3, 3, Some("kept")).retained_bytes();
    assert_eq!(
        selected(&corrupt, latest_bytes + small),
        (
            vec![latest.clone(), text_operation(3, 3, Some("kept"))],
            Some(latest_bytes + small + retained_len(4 + 96))
        )
    );
    assert!(LatestOperations::decode(&corrupt, u64::MAX).is_err());
    assert!(OperationQueue::decode(&corrupt).is_err());

    // Entities are compared by their raw bytes and validated only once
    // selected: an entity kind no body may name, a non-minimal ID, an ID
    // that never terminates, and an empty body each fail only a budget that
    // reaches them.
    let body = text_body(1, "first");
    let first = text_operation(1, 1, Some("first"));
    let mut unknown_entity = body.clone();
    unknown_entity[0] = 0x7F;
    let non_minimal_id = [&[0x01_u8, 0x82, 0x00][..], &text_body(2, "second")[2..]].concat();
    for (name, corrupt) in [
        ("unknown entity", unknown_entity),
        ("non-minimal ID", non_minimal_id),
        ("unterminated ID", [vec![0x01], vec![0xFF; 12]].concat()),
        ("empty body", Vec::new()),
    ] {
        let bytes = raw_value(
            QueueFamily::Text,
            &[],
            &[
                (InsertMode::IfAbsent, 1, body.clone()),
                (InsertMode::IfAbsent, 2, corrupt),
            ],
        );
        // A zero budget tracks no entity and charges the first the
        // smallest record any operation retains.
        assert_eq!(
            selected(&bytes, 0),
            (Vec::new(), Some(MIN_RETAINED_RECORD_LEN as u64)),
            "{name}"
        );
        let (operations, refused) = selected(&bytes, first.retained_bytes());
        assert_eq!(operations, vec![first.clone()], "{name}");
        assert!(
            refused.is_some_and(|reached| reached > first.retained_bytes()),
            "{name}: {refused:?}"
        );
        assert!(
            LatestOperations::decode(&bytes, u64::MAX).is_err(),
            "{name}"
        );
        assert!(OperationQueue::decode(&bytes).is_err(), "{name}");
    }

    // Every record's framing is walked whatever the budget: trailing bytes
    // and a set fail even when the budget selects nothing.
    let mut trailing = value.to_vec();
    trailing.push(0);
    for (name, bytes) in [
        ("trailing", Bytes::from(trailing)),
        (
            "late set",
            raw_value(
                QueueFamily::Text,
                &[],
                &[
                    (InsertMode::IfAbsent, 1, body.clone()),
                    (InsertMode::Set, 2, text_body(2, "set")),
                ],
            ),
        ),
        ("removal", raw_value(QueueFamily::Text, &[9], &[])),
        ("no records", raw_value(QueueFamily::Text, &[], &[])),
        ("truncated", value.slice(..HEADER_LEN)),
    ] {
        for budget in [0, u64::MAX] {
            assert!(
                LatestOperations::decode(&bytes, budget).is_err(),
                "{name} must not decode within {budget}"
            );
        }
    }
    // A full decode also rejects a repeated ID, which the walk does not track.
    let duplicate = raw_value(
        QueueFamily::Text,
        &[],
        &[
            (InsertMode::IfAbsent, 1, body.clone()),
            (InsertMode::IfAbsent, 1, body.clone()),
        ],
    );
    assert!(OperationQueue::decode(&duplicate).is_err());
}

#[test]
fn latest_decodes_select_as_many_minimal_entities_as_the_budget_covers() {
    // Each distinct entity's smallest valid operation is a text deletion.
    let operations = (0..10)
        .map(|entity| text_operation(u128::from(entity) + 1, entity, None))
        .collect::<Vec<_>>();
    assert!(operations
        .iter()
        .all(|operation| operation.retained_bytes() == MIN_RETAINED_RECORD_LEN as u64));
    let QueueMergeResult::Value(value) = merge_with_base(
        None,
        std::slice::from_ref(QueueOperand::enqueue(&operations).unwrap().bytes()),
    )
    .unwrap() else {
        panic!("ten operations are outstanding");
    };
    for count in 0..=10 {
        let budget = MIN_RETAINED_RECORD_LEN as u64 * count;
        let selection = LatestOperations::decode(&value, budget).unwrap();
        // Entities past the tracked ones still refuse the selection, each
        // charged at least the smallest record.
        assert_eq!(
            selection.refused(),
            (count < 10).then_some(budget + MIN_RETAINED_RECORD_LEN as u64),
            "within {budget}"
        );
        assert_eq!(
            selection.into_operations(),
            operations[..usize::try_from(count).unwrap()],
            "within {budget}"
        );
    }
    // An untracked entity's later operation is still walked, never selected.
    let mut late = operations.clone();
    late.push(text_operation(99, 9, Some("late")));
    let mut bytes = Vec::new();
    put_header(&mut bytes, QueueFamily::Text);
    put_varint(&mut bytes, 0);
    put_varint(&mut bytes, late.len() as u64);
    for operation in &late {
        put_insert(&mut bytes, InsertMode::IfAbsent, operation);
    }
    let selection = LatestOperations::decode(&bytes, MIN_RETAINED_RECORD_LEN as u64 * 2).unwrap();
    assert_eq!(
        selection.refused(),
        Some(MIN_RETAINED_RECORD_LEN as u64 * 3)
    );
    assert_eq!(selection.into_operations(), operations[..2].to_vec());
}

#[test]
fn latest_row_decodes_match_the_map_layout() {
    let operations = [
        text_operation(1, 1, Some("first")),
        text_operation(3, 3, None),
        text_operation(2, 1, Some("a much longer latest state")),
    ];
    let rows = operations
        .iter()
        .map(|operation| QueueRow::encode(QueueFamily::Text, operation))
        .collect::<Vec<_>>();
    let QueueMergeResult::Value(value) = merge_with_base(
        None,
        std::slice::from_ref(QueueOperand::enqueue(&operations[..2]).unwrap().bytes()),
    )
    .unwrap() else {
        panic!("two operations are outstanding");
    };
    let QueueMergeResult::Value(value) = merge_with_base(
        Some(&value),
        std::slice::from_ref(QueueOperand::enqueue(&operations[2..]).unwrap().bytes()),
    )
    .unwrap() else {
        panic!("three operations are outstanding");
    };
    let budgets = [
        0,
        operations[2].retained_bytes(),
        operations[2].retained_bytes() + operations[1].retained_bytes(),
        u64::MAX,
    ];
    for budget in budgets {
        assert_eq!(
            LatestOperations::decode_rows(rows.iter().map(Bytes::as_ref), budget).unwrap(),
            Some(LatestOperations::decode(&value, budget).unwrap()),
            "within {budget}"
        );
    }
    assert_eq!(
        LatestOperations::decode_rows(std::iter::empty(), u64::MAX).unwrap(),
        None
    );

    // A corrupt payload or entity is decoded only once selected; rows of two
    // families and a corrupt row header fail whatever the budget.
    let mut truncated = rows[2].to_vec();
    truncated.truncate(truncated.len() - 1);
    // Version, kind, family, and operation ID precede the body.
    let mut unknown_entity = rows[1].to_vec();
    unknown_entity[3 + 16] = 0x7F;
    for corrupt in [
        [rows[0].to_vec(), rows[1].to_vec(), truncated],
        [rows[0].to_vec(), unknown_entity, rows[2].to_vec()],
    ] {
        assert_eq!(
            LatestOperations::decode_rows(corrupt.iter().map(Vec::as_slice), 0)
                .unwrap()
                .map(LatestOperations::into_operations),
            Some(Vec::new())
        );
        assert!(
            LatestOperations::decode_rows(corrupt.iter().map(Vec::as_slice), u64::MAX).is_err()
        );
    }
    let vector = QueueRow::encode(QueueFamily::Vector, &vector_operation(4, 4, None, None));
    let mut header = rows[0].to_vec();
    header[0] = 0x02;
    for (name, bad) in [("mixed families", vector.to_vec()), ("version", header)] {
        let rows = [rows[0].to_vec(), bad];
        assert!(
            LatestOperations::decode_rows(rows.iter().map(Vec::as_slice), 0).is_err(),
            "{name}"
        );
    }
}

/// Sequential reference model: an ordered list of retained operations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ModelQueue {
    entries: Vec<(u128, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ModelRecord {
    Remove(u128),
    Insert(InsertMode, u128, Vec<u8>),
}

impl ModelQueue {
    fn position(&self, id: u128) -> Option<usize> {
        self.entries.iter().position(|(entry, _)| *entry == id)
    }

    fn apply(&mut self, record: &ModelRecord) -> Result<(), ()> {
        match record {
            ModelRecord::Remove(id) => {
                if let Some(position) = self.position(*id) {
                    self.entries.remove(position);
                }
            }
            ModelRecord::Insert(InsertMode::IfAbsent, id, body) => {
                if self.position(*id).is_none() {
                    self.entries.push((*id, body.clone()));
                }
            }
            ModelRecord::Insert(InsertMode::Set, id, body) => {
                if let Some(position) = self.position(*id) {
                    self.entries.remove(position);
                }
                self.entries.push((*id, body.clone()));
            }
        }
        Ok(())
    }

    fn encode(&self) -> QueueMergeResult {
        if self.entries.is_empty() {
            return QueueMergeResult::Empty;
        }
        let inserts = self
            .entries
            .iter()
            .map(|(id, body)| (InsertMode::IfAbsent, *id, body.clone()))
            .collect::<Vec<_>>();
        QueueMergeResult::Value(raw_value(QueueFamily::Text, &[], &inserts))
    }
}

/// One canonical operand: removals are applied before inserts, IDs unique.
#[derive(Debug, Clone)]
struct ModelOperand {
    records: Vec<ModelRecord>,
}

impl ModelOperand {
    fn encode(&self) -> Bytes {
        let removes = self
            .records
            .iter()
            .filter_map(|record| match record {
                ModelRecord::Remove(id) => Some(*id),
                ModelRecord::Insert(..) => None,
            })
            .collect::<Vec<_>>();
        let inserts = self
            .records
            .iter()
            .filter_map(|record| match record {
                ModelRecord::Insert(mode, id, body) => Some((*mode, *id, body.clone())),
                ModelRecord::Remove(_) => None,
            })
            .collect::<Vec<_>>();
        raw_value(QueueFamily::Text, &removes, &inserts)
    }

    fn apply(&self, model: &mut ModelQueue) -> Result<(), ()> {
        self.records
            .iter()
            .filter(|record| matches!(record, ModelRecord::Remove(_)))
            .chain(
                self.records
                    .iter()
                    .filter(|record| matches!(record, ModelRecord::Insert(..))),
            )
            .try_for_each(|record| model.apply(record))
    }
}

fn operand_strategy() -> impl Strategy<Value = ModelOperand> {
    // A small ID space forces repeats, removals of unseen IDs, and resets.
    proptest::collection::btree_map(0_u128..8, (0_u8..6, 0_u8..2), 1..5).prop_map(|records| {
        ModelOperand {
            records: records
                .into_iter()
                .map(|(id, (action, variant))| {
                    let body =
                        text_body(u64::try_from(id % 3).unwrap(), &format!("{id}-{variant}"));
                    match action {
                        0 | 1 => ModelRecord::Remove(id),
                        2 => ModelRecord::Insert(InsertMode::Set, id, body),
                        _ => ModelRecord::Insert(InsertMode::IfAbsent, id, body),
                    }
                })
                .collect(),
        }
    })
}

/// Histories storage can present: an operand enqueues IDs that are absent
/// and acknowledges IDs that are present, so each ID alternates between one
/// insert and its removal (re-enqueueing after a removal is a reset).
fn history_strategy() -> impl Strategy<Value = Vec<ModelOperand>> {
    proptest::collection::vec(
        proptest::collection::btree_map(0_u128..8, any::<bool>(), 1..5),
        1..10,
    )
    .prop_map(|steps| {
        let mut present = std::collections::BTreeSet::new();
        let mut inserts = 0_u32;
        steps
            .into_iter()
            .filter_map(|step| {
                let records = step
                    .into_iter()
                    .filter_map(|(id, acknowledge)| {
                        if present.contains(&id) {
                            return acknowledge.then(|| {
                                present.remove(&id);
                                ModelRecord::Remove(id)
                            });
                        }
                        present.insert(id);
                        inserts += 1;
                        Some(ModelRecord::Insert(
                            InsertMode::IfAbsent,
                            id,
                            text_body(u64::try_from(id % 3).unwrap(), &format!("{id}-{inserts}")),
                        ))
                    })
                    .collect::<Vec<_>>();
                (!records.is_empty()).then_some(ModelOperand { records })
            })
            .collect()
    })
}

/// Merges `operands` through one arbitrary nested grouping.
fn grouped_partial(operands: &[Bytes], splits: &[usize]) -> Result<Vec<Bytes>, EncodingError> {
    let mut groups = Vec::new();
    let mut start = 0;
    for split in splits
        .iter()
        .map(|split| split % (operands.len() + 1))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .chain(std::iter::once(operands.len()))
    {
        if split > start {
            groups.push(merge_partial(None, &operands[start..split])?);
            start = split;
        }
    }
    Ok(groups)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn every_partial_grouping_of_a_stored_history_matches_the_sequential_model(
        operands in history_strategy(),
        base_len in 0_usize..10,
        splits in proptest::collection::vec(0_usize..20, 0..6),
        nested_split in 0_usize..10,
    ) {
        prop_assume!(!operands.is_empty());
        let encoded = operands.iter().map(ModelOperand::encode).collect::<Vec<_>>();
        let mut model = ModelQueue::default();
        operands
            .iter()
            .try_for_each(|operand| operand.apply(&mut model))
            .expect("the model applies every record");
        let expected = model.encode();

        // One flat batch.
        prop_assert_eq!(merge_with_base(None, &encoded).unwrap(), expected.clone());

        // Arbitrary contiguous partial groups, then resolution.
        let groups = grouped_partial(&encoded, &splits).unwrap();
        prop_assert_eq!(merge_with_base(None, &groups).unwrap(), expected.clone());

        // A resolved prefix acts as the base for an unresolved suffix, whole
        // or folded first.
        let base_len = base_len.min(encoded.len());
        let (prefix, suffix) = encoded.split_at(base_len);
        let base = match merge_with_base(None, prefix).unwrap() {
            QueueMergeResult::Value(base) => Some(base),
            QueueMergeResult::Empty => None,
        };
        let with_base = if suffix.is_empty() {
            base.map_or(QueueMergeResult::Empty, QueueMergeResult::Value)
        } else {
            let folded = grouped_partial(suffix, &splits).unwrap();
            prop_assert_eq!(
                merge_with_base(base.as_deref(), &folded).unwrap(),
                merge_with_base(base.as_deref(), suffix).unwrap()
            );
            merge_with_base(base.as_deref(), suffix).unwrap()
        };
        prop_assert_eq!(with_base, expected.clone());

        // Nested partial composition: (A)∘B is byte-for-byte A∘B, and A∘(B)
        // resolves identically.
        if encoded.len() >= 2 {
            let middle = 1 + nested_split % (encoded.len() - 1);
            let (prefix, suffix) = encoded.split_at(middle);
            let whole = merge_partial(None, &encoded).unwrap();
            let left = merge_partial(Some(&merge_partial(None, prefix).unwrap()), suffix).unwrap();
            prop_assert_eq!(&left, &whole);
            let right = merge_partial(
                None,
                &prefix
                    .iter()
                    .cloned()
                    .chain(std::iter::once(merge_partial(None, suffix).unwrap()))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            prop_assert_eq!(
                merge_with_base(None, std::slice::from_ref(&right)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn a_partial_value_holds_at_most_the_backlogs_around_it(operands in history_strategy()) {
        // `MAX_RETAINED_BYTES` relies on this bound: a partial value over
        // operands `start..end` keeps at most one removal per operation
        // outstanding before `start` and records only of operations still
        // outstanding after `end`.
        let encoded = operands.iter().map(ModelOperand::encode).collect::<Vec<_>>();
        let mut backlogs = vec![ModelQueue::default()];
        for operand in &operands {
            let mut next = backlogs.last().expect("one backlog per prefix").clone();
            operand.apply(&mut next).expect("the model applies every record");
            backlogs.push(next);
        }
        for start in 0..encoded.len() {
            for end in start + 1..=encoded.len() {
                let partial = merge_partial(None, &encoded[start..end]).unwrap();
                let removals = OPERATION_ID_LEN * backlogs[start].entries.len();
                let records = backlogs[end]
                    .entries
                    .iter()
                    .map(|(_, body)| MODE_LEN + OPERATION_ID_LEN + varint_len(body.len() as u64) + body.len())
                    .sum::<usize>();
                prop_assert!(
                    partial.len() <= MAX_VALUE_FRAMING_LEN + removals + records,
                    "operands {}..{}: {} > {} + {} + {}",
                    start,
                    end,
                    partial.len(),
                    MAX_VALUE_FRAMING_LEN,
                    removals,
                    records
                );
            }
        }
    }

    #[test]
    fn arbitrary_records_compose_totally_and_fold_left_exactly(
        operands in proptest::collection::vec(operand_strategy(), 1..8),
        splits in proptest::collection::vec(0_usize..16, 0..6),
        nested_split in 0_usize..8,
    ) {
        // Duplicated inserts and removals of unseen IDs lie outside stored
        // histories: every grouping still composes and resolves without
        // error, and folding left is exact.
        let encoded = operands.iter().map(ModelOperand::encode).collect::<Vec<_>>();
        prop_assert!(merge_with_base(None, &encoded).is_ok());
        let groups = grouped_partial(&encoded, &splits).unwrap();
        prop_assert!(merge_with_base(None, &groups).is_ok());
        if encoded.len() >= 2 {
            let middle = 1 + nested_split % (encoded.len() - 1);
            let (prefix, suffix) = encoded.split_at(middle);
            let whole = merge_partial(None, &encoded).unwrap();
            let left = merge_partial(Some(&merge_partial(None, prefix).unwrap()), suffix).unwrap();
            prop_assert_eq!(left, whole);
        }
    }

    #[test]
    fn live_producer_sequences_never_error_and_preserve_entity_order(
        steps in proptest::collection::vec((0_u64..4, 0_u8..3), 1..40),
    ) {
        // Live producers enqueue unique IDs and acknowledge ordered prefixes.
        let mut next_id = 1_u128;
        let mut operands = Vec::new();
        let mut model: BTreeMap<u64, Vec<u128>> = BTreeMap::new();
        for (entity, action) in steps {
            if action == 0 {
                let Some(prefix) = model.get_mut(&entity).filter(|ids| !ids.is_empty()) else {
                    continue;
                };
                let head = prefix.remove(0);
                operands.push(QueueOperand::acknowledge(QueueFamily::Text, [id(head)]).unwrap().bytes().clone());
            } else {
                let operation = text_operation(next_id, entity, Some(&format!("v{next_id}")));
                model.entry(entity).or_default().push(next_id);
                next_id += 1;
                operands.push(QueueOperand::enqueue(&[operation]).unwrap().bytes().clone());
            }
        }
        let resolved = resolve(&operands);
        let mut actual: BTreeMap<u64, Vec<u128>> = BTreeMap::new();
        for operation in resolved.as_ref().map_or(&[][..], OperationQueue::operations) {
            actual.entry(operation.entity().id.get()).or_default().push(operation.id().get());
        }
        model.retain(|_, ids| !ids.is_empty());
        prop_assert_eq!(actual, model);
    }

    #[test]
    fn independent_entity_enqueues_commute_per_entity(
        left in proptest::collection::vec(0_u64..3, 1..6),
        right in proptest::collection::vec(3_u64..6, 1..6),
        interleave in proptest::collection::vec(any::<bool>(), 12),
    ) {
        // Two disjoint entity sets may commit in any interleaving; each
        // entity's projection is identical regardless of the interleaving.
        let make = |entities: &[u64], offset: u128| entities
            .iter()
            .enumerate()
            .map(|(index, entity)| QueueOperand::enqueue(&[text_operation(offset + index as u128, *entity, Some("x"))]).unwrap().bytes().clone())
            .collect::<Vec<_>>();
        let left = make(&left, 100);
        let right = make(&right, 200);
        let mut mixed = Vec::new();
        let (mut l, mut r) = (left.iter(), right.iter());
        for take_left in interleave {
            let next = if take_left { l.next().or_else(|| r.next()) } else { r.next().or_else(|| l.next()) };
            mixed.extend(next.cloned());
        }
        mixed.extend(l.cloned());
        mixed.extend(r.cloned());
        let sequential = [left, right].concat();
        let project = |queue: Option<OperationQueue>| {
            let mut entities: HashMap<u64, Vec<u128>> = HashMap::new();
            for operation in queue.as_ref().map_or(&[][..], OperationQueue::operations) {
                entities.entry(operation.entity().id.get()).or_default().push(operation.id().get());
            }
            entities
        };
        prop_assert_eq!(project(resolve(&mixed)), project(resolve(&sequential)));
    }
}

#[test]
fn queue_keys_round_trip_for_every_scope() {
    for scope in [
        DataScope::LegacyUnscoped,
        DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(0x42)),
    ] {
        let key = ManagedIndexKey::Data {
            scope,
            kind: ScopedKey::IndexOperationQueue(IndexOperationQueueKey {
                index_id: IndexId::new(7).unwrap(),
                generation: IndexGenerationId::new(3).unwrap(),
            }),
        };
        let bytes = key.to_bytes();
        assert_eq!(ManagedIndexKey::parse_data_from_slice(&bytes).unwrap(), key);
        assert_eq!(
            bytes.len(),
            match scope {
                DataScope::LegacyUnscoped => 0,
                DataScope::Tenant(_) => 17,
            } + 2
                + 16
        );
    }
}

#[test]
fn row_values_round_trip_every_payload_shape() {
    let operations = [
        (QueueFamily::Vector, vector_operation(1, 0, None, None)),
        (
            QueueFamily::Vector,
            vector_operation(
                2,
                u64::MAX,
                Some(tenant(b"old")),
                Some((tenant(b"new"), vec![-0.0, f32::MIN])),
            ),
        ),
        (QueueFamily::Text, text_operation(3, 300, None)),
        (QueueFamily::Text, text_operation(4, 1, Some("héllo"))),
    ];
    for (family, operation) in operations {
        let row = QueueRow::encode(family, &operation);
        assert_eq!(
            row.len(),
            HEADER_LEN + OPERATION_ID_LEN + body_encoded_len(&operation)
        );
        assert_eq!(
            retained_len(row.len() - HEADER_LEN - OPERATION_ID_LEN),
            operation.retained_bytes()
        );
        assert_eq!(QueueRow::decode(&row).unwrap(), (family, operation));
    }
}

#[test]
fn malformed_rows_are_errors() {
    let valid = QueueRow::encode(QueueFamily::Text, &text_operation(9, 5, Some("row")));
    let with = |offset: usize, byte: u8| {
        let mut row = valid.to_vec();
        row[offset] = byte;
        row
    };
    let mut trailing = valid.to_vec();
    trailing.push(0x00);
    let high_bit_id = {
        let mut row = valid.to_vec();
        row[HEADER_LEN] |= 0x80;
        row
    };
    for (case, bytes) in [
        ("empty", Vec::new()),
        ("version", with(0, 0x02)),
        ("map value kind", with(1, QUEUE_VALUE_KIND)),
        ("family", with(2, 0x09)),
        ("operation ID bit 127", high_bit_id),
        ("truncated ID", valid[..HEADER_LEN + 4].to_vec()),
        ("truncated body", valid[..valid.len() - 1].to_vec()),
        ("trailing bytes", trailing),
        (
            "family does not match body",
            with(2, QueueFamily::Vector as u8),
        ),
    ] {
        assert!(QueueRow::decode(&bytes).is_err(), "{case} must not decode");
    }
    // A merge-layout value is never accepted as a row.
    let map_value = QueueOperand::enqueue(&[text_operation(9, 5, Some("row"))]).unwrap();
    assert!(QueueRow::decode(map_value.bytes()).is_err());
}

#[test]
fn row_keys_round_trip_and_sort_by_generation_then_sequence() {
    let key = |scope, generation: u64, sequence| ManagedIndexKey::Data {
        scope,
        kind: ScopedKey::IndexOperationRow(IndexOperationRowKey {
            index_id: IndexId::new(7).unwrap(),
            generation: IndexGenerationId::new(generation).unwrap(),
            sequence,
        }),
    };
    for scope in [
        DataScope::LegacyUnscoped,
        DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(0x42)),
    ] {
        let first = key(scope, 3, u64::MAX - 1);
        let bytes = first.to_bytes();
        assert_eq!(
            ManagedIndexKey::parse_data_from_slice(&bytes).unwrap(),
            first
        );
        assert_eq!(
            bytes.len(),
            match scope {
                DataScope::LegacyUnscoped => 0,
                DataScope::Tenant(_) => 17,
            } + 2
                + 24
        );
        assert!(bytes < key(scope, 3, u64::MAX).to_bytes());
        assert!(key(scope, 3, u64::MAX).to_bytes() < key(scope, 4, 0).to_bytes());
        assert!(ManagedIndexKey::parse_data_from_slice(&bytes[..bytes.len() - 1]).is_err());
    }
}

/// Diagnostic, not a benchmark: the cost of materializing one map-layout
/// queue value grows linearly with retained operations, because every
/// publication attempt and strong search decodes the whole generation queue.
/// Run with `cargo test --release -p db --lib map_materialization_cost -- --ignored --nocapture`.
#[test]
#[ignore = "release-mode diagnostic that prints a cost table"]
fn map_materialization_cost_grows_with_retained_operations() {
    use std::time::Instant;
    println!(
        "operations,family,value_bytes,bytes_per_op,merge_micros,decode_micros,decode_ns_per_op"
    );
    for count in [1_000_u128, 10_000, 100_000, 250_000] {
        for family in [QueueFamily::Vector, QueueFamily::Text] {
            // One operand per foreground transaction of 100 entities.
            let operands = (0..count)
                .collect::<Vec<_>>()
                .chunks(100)
                .map(|chunk| {
                    let operations = chunk
                        .iter()
                        .map(|op| match family {
                            QueueFamily::Vector => vector_operation(
                                op + 1,
                                u64::try_from(*op).unwrap(),
                                None,
                                Some((TextPartition::Unpartitioned, vec![0.5; 128])),
                            ),
                            QueueFamily::Text => text_operation(
                                op + 1,
                                u64::try_from(*op).unwrap(),
                                Some("a representative hundred byte document body for the text index queue diagnostic ok"),
                            ),
                        })
                        .collect::<Vec<_>>();
                    QueueOperand::enqueue(&operations).unwrap().bytes().clone()
                })
                .collect::<Vec<_>>();
            let started = Instant::now();
            let QueueMergeResult::Value(value) = merge_with_base(None, &operands).unwrap() else {
                panic!("non-empty queue");
            };
            let merge_micros = started.elapsed().as_micros();
            let started = Instant::now();
            let queue = OperationQueue::decode(&value).unwrap();
            let decode = started.elapsed();
            assert_eq!(queue.operations().len() as u128, count);
            println!(
                "{count},{family:?},{},{},{merge_micros},{},{}",
                value.len(),
                value.len() as u128 / count,
                decode.as_micros(),
                decode.as_nanos() / count
            );
        }
    }
}
