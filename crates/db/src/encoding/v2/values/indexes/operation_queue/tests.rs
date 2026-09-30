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
    cases.push((
        "no records",
        raw_value(QueueFamily::Text, &[], &[]).to_vec(),
    ));
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

fn operand_strategy(allow_conflicts: bool) -> impl Strategy<Value = ModelOperand> {
    // A small ID space forces repeats, removals of unseen IDs, and resets.
    proptest::collection::btree_map(0_u128..8, (0_u8..6, 0_u8..2), 1..5).prop_map(move |records| {
        ModelOperand {
            records: records
                .into_iter()
                .map(|(id, (action, variant))| {
                    let variant = if allow_conflicts { variant } else { 0 };
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
    fn every_partial_grouping_matches_the_sequential_model(
        operands in proptest::collection::vec(operand_strategy(true), 1..8),
        base_len in 0_usize..8,
        splits in proptest::collection::vec(0_usize..16, 0..6),
        nested_split in 0_usize..8,
    ) {
        let encoded = operands.iter().map(ModelOperand::encode).collect::<Vec<_>>();
        let mut model = ModelQueue::default();
        let expected = operands
            .iter()
            .try_for_each(|operand| operand.apply(&mut model))
            .map(|()| model.encode());

        // One flat batch.
        let flat = merge_with_base(None, &encoded);
        prop_assert_eq!(flat.as_ref().ok(), expected.as_ref().ok());

        // Arbitrary contiguous partial groups, then resolution.
        let grouped = grouped_partial(&encoded, &splits)
            .and_then(|groups| merge_with_base(None, &groups));
        prop_assert_eq!(grouped.as_ref().ok(), expected.as_ref().ok());

        // A resolved prefix acts as the base for an unresolved suffix.
        let base_len = base_len.min(encoded.len());
        let with_base = merge_with_base(None, &encoded[0..base_len]).and_then(|base| {
            let suffix = &encoded[base_len..];
            match base {
                QueueMergeResult::Value(base) => merge_with_base(Some(&base), suffix),
                QueueMergeResult::Empty if suffix.is_empty() => Ok(QueueMergeResult::Empty),
                QueueMergeResult::Empty => merge_with_base(None, suffix),
            }
        });
        prop_assert_eq!(with_base.as_ref().ok(), expected.as_ref().ok());

        // Nested partial composition: (A)∘B == A∘(B) byte-for-byte.
        if encoded.len() >= 2 {
            let middle = 1 + nested_split % (encoded.len() - 1);
            let (prefix, suffix) = encoded.split_at(middle);
            let whole = merge_partial(None, &encoded);
            let left = merge_partial(None, prefix)
                .and_then(|left| merge_partial(Some(&left), suffix));
            let right = merge_partial(None, suffix).and_then(|right| {
                merge_partial(
                    None,
                    &prefix.iter().cloned().chain(std::iter::once(right)).collect::<Vec<_>>(),
                )
            });
            prop_assert_eq!(left.as_ref().ok(), whole.as_ref().ok());
            prop_assert_eq!(right.as_ref().ok(), whole.as_ref().ok());
            prop_assert_eq!(left.is_err(), whole.is_err());
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
