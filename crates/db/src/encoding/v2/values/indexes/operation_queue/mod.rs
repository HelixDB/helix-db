//! Immutable vector/text index-operation queue values.
//!
//! One SlateDB key per `(scope, logical index, generation)` holds every
//! committed-but-unpublished operation for that generation. Producers append
//! with blind [`slatedb::DbTransaction::merge_disjoint_tokens`] operands and the
//! publication worker removes exact operation IDs with acknowledgement
//! operands. Neither side reads the row to stage its operand.
//!
//! # Persisted format
//!
//! Every operand, partial merge result, and resolved value shares one
//! canonical layout:
//!
//! ```text
//! value      := version:u8(0x01) kind:u8(0x14) family:u8 removes inserts
//! family     := 0x01 vector | 0x02 text
//! removes    := count:varint operation_id{count}        (strictly ascending)
//! inserts    := count:varint insert{count}              (storage commit order)
//! insert     := mode:u8 operation_id body_len:varint body
//! mode       := 0x01 insert-if-absent | 0x02 unconditional set
//! operation_id := 16 bytes, big-endian u128 with bit 127 clear
//! body       := entity_kind:u8 entity_id:varint payload
//! payload    := vector_payload | text_payload           (selected by family)
//! vector_payload := previous:optional_partition replacement:vector_replacement
//! vector_replacement := 0x00 | 0x01 partition dimension:varint f32_be{dimension}
//! text_payload := 0x00 | 0x01 partition length:varint utf8{length}
//! optional_partition := 0x00 | partition
//! partition  := 0x01 unpartitioned | 0x02 length:varint tenant_bytes{length}
//! ```
//!
//! Varints are minimal unsigned LEB128. Each operation ID appears at most once
//! per value, and an operation ID never appears both as a removal and an
//! insert. A value resolved against a known base contains only
//! insert-if-absent records; an empty resolved queue is a SlateDB tombstone,
//! so an absent key is the empty queue.
//!
//! Relative insert order is storage commit order. Producers serialize enqueue
//! operations for one entity with a per-entity conflict token, so one
//! entity's operations appear in exactly the order their transactions
//! committed. Different entities have no global order.
//!
//! # Row layout (benchmark baseline)
//!
//! The row-per-operation layout stores each operation under its own
//! sequence-ordered key and acknowledges it by deleting that key:
//!
//! ```text
//! row := version:u8(0x01) kind:u8(0x15) family:u8 operation_id body
//! ```
//!
//! Both layouts decode into the same [`OperationQueue`], so workers and
//! overlays are identical; only storage and acknowledgement differ.

mod algebra;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;
use crate::encoding::v2::keys::IndexEntity;
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::{IndexElementKind, IndexEntityId};

#[cfg(test)]
pub(crate) use algebra::validate_operand;
pub(crate) use algebra::{merge_partial, merge_with_base, QueueMergeResult};

/// Frozen framing version for queue values.
const QUEUE_VALUE_VERSION: u8 = 0x01;
/// Value kind mirrors the key's `RecordKind::IndexOperationQueue` byte.
pub(crate) const QUEUE_VALUE_KIND: u8 = 0x14;
/// Row value kind mirrors the key's `RecordKind::IndexOperationRow` byte.
const ROW_VALUE_KIND: u8 = 0x15;
const VERSION_LEN: usize = core::mem::size_of::<u8>();
const KIND_LEN: usize = core::mem::size_of::<u8>();
const FAMILY_LEN: usize = core::mem::size_of::<u8>();
const MODE_LEN: usize = core::mem::size_of::<u8>();
/// Encoded operation-ID width.
pub(crate) const OPERATION_ID_LEN: usize = core::mem::size_of::<u128>();
const HEADER_LEN: usize = VERSION_LEN + KIND_LEN + FAMILY_LEN;
const F32_LEN: usize = core::mem::size_of::<f32>();
const MAX_VARINT_LEN: usize = 10;
/// Largest tenant partition accepted by canonical partitions.
const MAX_PARTITION_LEN: usize = 16 * 1024 * 1024;
const OPERATION_TOKEN_BIT: u128 = 1 << 127;

/// Unique immutable identity of one queued operation.
///
/// Bit 127 is always clear so operation tokens and entity enqueue-order tokens
/// occupy disjoint halves of SlateDB's `u128` token space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct QueuedOperationId(u128);

impl QueuedOperationId {
    /// Generates a fresh random identity that is never reused by a producer.
    pub(crate) fn generate() -> Self {
        Self(uuid::Uuid::new_v4().as_u128() & !OPERATION_TOKEN_BIT)
    }

    /// Validates a decoded identity.
    pub(crate) fn try_from_u128(value: u128) -> Result<Self, EncodingError> {
        if value & OPERATION_TOKEN_BIT != 0 {
            return Err(EncodingError::Custom(
                "queued operation ID uses the reserved entity-token bit".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the raw identity.
    pub(crate) const fn get(self) -> u128 {
        self.0
    }

    /// Returns the disjoint-merge token owned by exactly this operation.
    pub(crate) const fn token(self) -> u128 {
        self.0
    }

    fn to_be_bytes(self) -> [u8; OPERATION_ID_LEN] {
        self.0.to_be_bytes()
    }
}

/// Returns the stable per-entity enqueue-order token.
///
/// Every enqueue for the same entity carries this token, so competing
/// enqueues conflict and retry instead of committing in an ambiguous order.
/// Acknowledgements never carry it.
pub(crate) const fn entity_enqueue_token(entity: IndexEntity) -> u128 {
    OPERATION_TOKEN_BIT | ((entity.kind as u128) << 64) | entity.id.get() as u128
}

/// Index family whose payloads a queue retains.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum QueueFamily {
    /// Vector operations.
    Vector = 0x01,
    /// Text operations.
    Text = 0x02,
}

impl QueueFamily {
    fn try_from_u8(value: u8) -> Result<Self, EncodingError> {
        match value {
            0x01 => Ok(Self::Vector),
            0x02 => Ok(Self::Text),
            unknown => Err(EncodingError::Custom(format!(
                "unknown queued operation family {unknown:#04x}"
            ))),
        }
    }
}

/// Destination state of one vector operation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueuedVectorReplacement {
    partition: TextPartition,
    vector: Arc<[f32]>,
}

impl QueuedVectorReplacement {
    /// Accepts a non-empty finite vector already validated by its index metric.
    pub(crate) fn try_new(
        partition: TextPartition,
        vector: Arc<[f32]>,
    ) -> Result<Self, EncodingError> {
        if vector.is_empty() {
            return Err(EncodingError::Custom(
                "queued vector replacement must not be empty".to_string(),
            ));
        }
        if let Some(index) = vector.iter().position(|value| !value.is_finite()) {
            return Err(EncodingError::Custom(format!(
                "queued vector component {index} is not finite"
            )));
        }
        Ok(Self { partition, vector })
    }

    /// Returns the destination partition.
    pub(crate) const fn partition(&self) -> &TextPartition {
        &self.partition
    }

    /// Returns the exact queued vector components.
    pub(crate) fn vector(&self) -> &[f32] {
        &self.vector
    }

    /// Shares the exact queued vector components without copying them.
    pub(crate) fn shared_vector(&self) -> Arc<[f32]> {
        Arc::clone(&self.vector)
    }
}

/// Complete vector operation: previous routing plus optional replacement.
///
/// `previous` is the partition the committed graph state was indexed under
/// immediately before this operation. `None` replacement is a deletion.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueuedVectorPayload {
    pub(crate) previous: Option<TextPartition>,
    pub(crate) replacement: Option<QueuedVectorReplacement>,
}

/// Destination state of one text operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueuedTextReplacement {
    partition: TextPartition,
    text: Arc<str>,
}

impl QueuedTextReplacement {
    /// Accepts the exact normalized text projected by the index definition.
    pub(crate) const fn new(partition: TextPartition, text: Arc<str>) -> Self {
        Self { partition, text }
    }

    /// Returns the destination partition.
    pub(crate) const fn partition(&self) -> &TextPartition {
        &self.partition
    }

    /// Returns the exact queued text.
    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    /// Shares the exact queued text without copying it.
    pub(crate) fn shared_text(&self) -> Arc<str> {
        Arc::clone(&self.text)
    }
}

/// Complete text operation. `None` replacement is a deletion; the previous
/// indexed representation is recovered from physical entity records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueuedTextPayload {
    pub(crate) replacement: Option<QueuedTextReplacement>,
}

/// Family-specific immutable payload.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QueuedPayload {
    /// Vector payload.
    Vector(QueuedVectorPayload),
    /// Text payload.
    Text(QueuedTextPayload),
}

impl QueuedPayload {
    /// Returns the queue family this payload belongs to.
    pub(crate) const fn family(&self) -> QueueFamily {
        match self {
            Self::Vector(_) => QueueFamily::Vector,
            Self::Text(_) => QueueFamily::Text,
        }
    }
}

/// One complete immutable queued operation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueuedOperation {
    id: QueuedOperationId,
    entity: IndexEntity,
    payload: QueuedPayload,
}

impl QueuedOperation {
    /// Binds a fresh identity to one entity transition.
    pub(crate) const fn new(
        id: QueuedOperationId,
        entity: IndexEntity,
        payload: QueuedPayload,
    ) -> Self {
        Self {
            id,
            entity,
            payload,
        }
    }

    /// Returns the exact operation identity.
    pub(crate) const fn id(&self) -> QueuedOperationId {
        self.id
    }

    /// Returns the target graph entity.
    pub(crate) const fn entity(&self) -> IndexEntity {
        self.entity
    }

    /// Returns the immutable payload.
    pub(crate) const fn payload(&self) -> &QueuedPayload {
        &self.payload
    }

    /// Returns the exact bytes this operation retains in a resolved queue.
    ///
    /// Accounting charges this size: mode, identity, body length, entity, and
    /// the complete payload, including deletions.
    pub(crate) fn retained_bytes(&self) -> u64 {
        let body_len = body_encoded_len(self);
        u64::try_from(MODE_LEN + OPERATION_ID_LEN + varint_len(body_len as u64) + body_len)
            .unwrap_or(u64::MAX)
    }
}

/// Blind merge operand plus the SlateDB conflict tokens it must carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueOperand {
    bytes: Bytes,
    tokens: Vec<u128>,
}

impl QueueOperand {
    /// Encodes enqueue records for one generation's queue.
    ///
    /// Every operation contributes its own token plus its entity's
    /// enqueue-order token. Operations must share one family, carry unique
    /// IDs, and name each entity at most once: a transaction contributes at
    /// most one final operation per entity and generation.
    pub(crate) fn enqueue(operations: &[QueuedOperation]) -> Result<Self, EncodingError> {
        let Some(first) = operations.first() else {
            return Err(EncodingError::Custom(
                "queue enqueue operand requires at least one operation".to_string(),
            ));
        };
        let family = first.payload.family();
        let mut ids = std::collections::HashSet::with_capacity(operations.len());
        let mut entities = std::collections::HashSet::with_capacity(operations.len());
        let mut tokens = Vec::with_capacity(operations.len() * 2);
        let body_len = operations
            .iter()
            .map(QueuedOperation::retained_bytes)
            .sum::<u64>();
        let mut bytes = Vec::with_capacity(
            HEADER_LEN + 1 + MAX_VARINT_LEN + usize::try_from(body_len).unwrap_or(0),
        );
        put_header(&mut bytes, family);
        put_varint(&mut bytes, 0);
        put_varint(&mut bytes, operations.len() as u64);
        for operation in operations {
            if operation.payload.family() != family {
                return Err(EncodingError::Custom(
                    "queue enqueue operand mixes index families".to_string(),
                ));
            }
            if !ids.insert(operation.id) {
                return Err(EncodingError::Custom(
                    "queue enqueue operand repeats an operation ID".to_string(),
                ));
            }
            if !entities.insert(operation.entity) {
                return Err(EncodingError::Custom(
                    "queue enqueue operand repeats an entity".to_string(),
                ));
            }
            put_insert(&mut bytes, algebra::InsertMode::IfAbsent, operation);
            tokens.push(operation.id.token());
            tokens.push(entity_enqueue_token(operation.entity));
        }
        Ok(Self {
            bytes: Bytes::from(bytes),
            tokens,
        })
    }

    /// Encodes the removal of exactly the named operation IDs.
    ///
    /// Acknowledgements carry only operation tokens, so they stay disjoint
    /// from newer enqueues for the same entity.
    pub(crate) fn acknowledge(
        family: QueueFamily,
        ids: impl IntoIterator<Item = QueuedOperationId>,
    ) -> Result<Self, EncodingError> {
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(EncodingError::Custom(
                "queue acknowledgement repeats an operation ID".to_string(),
            ));
        }
        if ids.is_empty() {
            return Err(EncodingError::Custom(
                "queue acknowledgement requires at least one operation".to_string(),
            ));
        }
        let mut bytes = Vec::with_capacity(HEADER_LEN + MAX_VARINT_LEN * 2 + ids.len() * 16);
        put_header(&mut bytes, family);
        put_varint(&mut bytes, ids.len() as u64);
        for id in &ids {
            bytes.put_slice(&id.to_be_bytes());
        }
        put_varint(&mut bytes, 0);
        Ok(Self {
            bytes: Bytes::from(bytes),
            tokens: ids.into_iter().map(QueuedOperationId::token).collect(),
        })
    }

    /// Returns the most IDs one [`Self::acknowledge`] operand can name while
    /// staying within `max_bytes`.
    ///
    /// The operand is a header, the ID count, 16 bytes per ID, and an empty
    /// insert count. The count is sized for the largest candidate, so the
    /// result may be one below the exact maximum at a varint boundary but
    /// never above it.
    pub(crate) const fn acknowledgement_capacity(max_bytes: u64) -> u64 {
        const FIXED: u64 = (HEADER_LEN + 1) as u64;
        const ID: u64 = OPERATION_ID_LEN as u64;
        let largest = max_bytes.saturating_sub(FIXED) / ID;
        max_bytes.saturating_sub(FIXED + varint_len(largest) as u64) / ID
    }

    /// Returns the exact operand bytes.
    pub(crate) const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the operand's disjoint-merge tokens.
    #[cfg(test)]
    pub(crate) fn tokens(&self) -> &[u128] {
        &self.tokens
    }

    /// Splits the operand into its bytes and tokens.
    pub(crate) fn into_parts(self) -> (Bytes, Vec<u128>) {
        (self.bytes, self.tokens)
    }
}

/// Fully decoded resolved queue in storage commit order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OperationQueue {
    family: QueueFamily,
    operations: Vec<QueuedOperation>,
}

impl OperationQueue {
    /// Decodes one resolved queue value read from storage.
    ///
    /// Only fully resolved values are accepted: removals or unconditional
    /// sets mean the reader observed an unresolved merge operand, which is
    /// corruption at this boundary. Corrupt values are errors, never empty
    /// queues.
    pub(crate) fn decode(value: &[u8]) -> Result<Self, EncodingError> {
        let raw = algebra::RawValue::parse(value)?;
        if !raw.removes.is_empty() {
            return Err(EncodingError::Custom(
                "resolved operation queue retains acknowledgements".to_string(),
            ));
        }
        let operations = raw
            .inserts
            .iter()
            .map(|insert| {
                if insert.mode != algebra::InsertMode::IfAbsent {
                    return Err(EncodingError::Custom(
                        "resolved operation queue retains an unconditional set".to_string(),
                    ));
                }
                decode_body(raw.family, insert.id, insert.body)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if operations.is_empty() {
            return Err(EncodingError::Custom(
                "resolved operation queue is empty instead of absent".to_string(),
            ));
        }
        Ok(Self {
            family: raw.family,
            operations,
        })
    }

    /// Assembles a queue from row-layout operations in sequence order,
    /// returning `None` for the empty queue.
    pub(crate) fn from_rows(family: QueueFamily, operations: Vec<QueuedOperation>) -> Option<Self> {
        (!operations.is_empty()).then_some(Self { family, operations })
    }

    /// Returns the retained family.
    pub(crate) const fn family(&self) -> QueueFamily {
        self.family
    }

    /// Returns every outstanding operation in storage commit order.
    pub(crate) fn operations(&self) -> &[QueuedOperation] {
        &self.operations
    }

    /// Consumes the queue into its ordered operations.
    #[cfg(test)]
    pub(crate) fn into_operations(self) -> Vec<QueuedOperation> {
        self.operations
    }
}

/// One operation persisted as its own row in the row-layout baseline.
pub(crate) struct QueueRow;

impl QueueRow {
    /// Encodes one operation as a complete row value.
    pub(crate) fn encode(family: QueueFamily, operation: &QueuedOperation) -> Bytes {
        let mut bytes =
            Vec::with_capacity(HEADER_LEN + OPERATION_ID_LEN + body_encoded_len(operation));
        bytes.put_u8(QUEUE_VALUE_VERSION);
        bytes.put_u8(ROW_VALUE_KIND);
        bytes.put_u8(family as u8);
        bytes.put_slice(&operation.id.to_be_bytes());
        put_body(&mut bytes, operation);
        Bytes::from(bytes)
    }

    /// Fully validates and decodes one row value.
    pub(crate) fn decode(value: &[u8]) -> Result<(QueueFamily, QueuedOperation), EncodingError> {
        let mut cursor = Cursor::new(value);
        if cursor.take_u8()? != QUEUE_VALUE_VERSION {
            return Err(EncodingError::Custom(
                "unsupported queued operation row version".to_string(),
            ));
        }
        if cursor.take_u8()? != ROW_VALUE_KIND {
            return Err(EncodingError::Custom(
                "queued operation row has another value kind".to_string(),
            ));
        }
        let family = QueueFamily::try_from_u8(cursor.take_u8()?)?;
        let id = QueuedOperationId::try_from_u128(u128::from_be_bytes(
            cursor
                .take_raw(OPERATION_ID_LEN)?
                .try_into()
                .expect("operation ID slice is sixteen bytes"),
        ))?;
        let body = cursor.take_raw(cursor.remaining_len())?;
        Ok((family, decode_body(family, id, body)?))
    }
}

fn put_header(bytes: &mut Vec<u8>, family: QueueFamily) {
    bytes.put_u8(QUEUE_VALUE_VERSION);
    bytes.put_u8(QUEUE_VALUE_KIND);
    bytes.put_u8(family as u8);
}

fn put_insert(bytes: &mut Vec<u8>, mode: algebra::InsertMode, operation: &QueuedOperation) {
    bytes.put_u8(mode as u8);
    bytes.put_slice(&operation.id.to_be_bytes());
    put_varint(bytes, body_encoded_len(operation) as u64);
    put_body(bytes, operation);
}

fn body_encoded_len(operation: &QueuedOperation) -> usize {
    let entity = KIND_LEN + varint_len(operation.entity.id.get());
    let payload = match &operation.payload {
        QueuedPayload::Vector(payload) => {
            payload
                .previous
                .as_ref()
                .map_or(KIND_LEN, partition_encoded_len)
                + payload
                    .replacement
                    .as_ref()
                    .map_or(KIND_LEN, |replacement| {
                        KIND_LEN
                            + partition_encoded_len(&replacement.partition)
                            + varint_len(replacement.vector.len() as u64)
                            + replacement.vector.len() * F32_LEN
                    })
        }
        QueuedPayload::Text(payload) => {
            payload
                .replacement
                .as_ref()
                .map_or(KIND_LEN, |replacement| {
                    KIND_LEN
                        + partition_encoded_len(&replacement.partition)
                        + varint_len(replacement.text.len() as u64)
                        + replacement.text.len()
                })
        }
    };
    entity + payload
}

fn put_body(bytes: &mut Vec<u8>, operation: &QueuedOperation) {
    bytes.put_u8(operation.entity.kind as u8);
    put_varint(bytes, operation.entity.id.get());
    match &operation.payload {
        QueuedPayload::Vector(payload) => {
            match &payload.previous {
                Some(partition) => put_partition(bytes, partition),
                None => bytes.put_u8(0x00),
            }
            match &payload.replacement {
                Some(replacement) => {
                    bytes.put_u8(0x01);
                    put_partition(bytes, &replacement.partition);
                    put_varint(bytes, replacement.vector.len() as u64);
                    for component in replacement.vector.iter() {
                        bytes.put_u32(component.to_bits());
                    }
                }
                None => bytes.put_u8(0x00),
            }
        }
        QueuedPayload::Text(payload) => match &payload.replacement {
            Some(replacement) => {
                bytes.put_u8(0x01);
                put_partition(bytes, &replacement.partition);
                put_varint(bytes, replacement.text.len() as u64);
                bytes.put_slice(replacement.text.as_bytes());
            }
            None => bytes.put_u8(0x00),
        },
    }
}

fn partition_encoded_len(partition: &TextPartition) -> usize {
    match partition {
        TextPartition::Unpartitioned => KIND_LEN,
        TextPartition::TenantValue(value) => {
            KIND_LEN + varint_len(value.len() as u64) + value.len()
        }
    }
}

fn put_partition(bytes: &mut Vec<u8>, partition: &TextPartition) {
    match partition {
        TextPartition::Unpartitioned => bytes.put_u8(0x01),
        TextPartition::TenantValue(value) => {
            bytes.put_u8(0x02);
            put_varint(bytes, value.len() as u64);
            bytes.put_slice(value);
        }
    }
}

/// Fully validates one insert body and decodes it into a typed operation.
fn decode_body(
    family: QueueFamily,
    id: QueuedOperationId,
    body: &[u8],
) -> Result<QueuedOperation, EncodingError> {
    let mut cursor = Cursor::new(body);
    let entity = IndexEntity {
        kind: match cursor.take_u8()? {
            0x01 => IndexElementKind::Node,
            0x02 => IndexElementKind::Edge,
            unknown => {
                return Err(EncodingError::Custom(format!(
                    "unknown queued entity kind {unknown:#04x}"
                )));
            }
        },
        id: IndexEntityId::new(cursor.take_varint()?),
    };
    let payload = match family {
        QueueFamily::Vector => {
            let previous = match cursor.take_u8()? {
                0x00 => None,
                tag => Some(cursor.take_partition_body(tag)?),
            };
            let replacement = match cursor.take_u8()? {
                0x00 => None,
                0x01 => {
                    let tag = cursor.take_u8()?;
                    let partition = cursor.take_partition_body(tag)?;
                    let dimension = usize::try_from(cursor.take_varint()?).map_err(|_| {
                        EncodingError::Custom("queued vector dimension overflows".to_string())
                    })?;
                    if dimension > cursor.remaining_len() / F32_LEN {
                        return Err(EncodingError::BufferTooShort {
                            expected: dimension.saturating_mul(F32_LEN),
                            actual: cursor.remaining_len(),
                        });
                    }
                    let components = cursor.take_raw(dimension * F32_LEN)?;
                    let vector = components
                        .chunks_exact(F32_LEN)
                        .map(|chunk| {
                            f32::from_bits(u32::from_be_bytes(
                                chunk.try_into().expect("exact f32 chunk"),
                            ))
                        })
                        .collect::<Arc<[f32]>>();
                    Some(QueuedVectorReplacement::try_new(partition, vector)?)
                }
                unknown => return Err(noncanonical_option(unknown)),
            };
            QueuedPayload::Vector(QueuedVectorPayload {
                previous,
                replacement,
            })
        }
        QueueFamily::Text => {
            let replacement = match cursor.take_u8()? {
                0x00 => None,
                0x01 => {
                    let tag = cursor.take_u8()?;
                    let partition = cursor.take_partition_body(tag)?;
                    let len = usize::try_from(cursor.take_varint()?).map_err(|_| {
                        EncodingError::Custom("queued text length overflows".to_string())
                    })?;
                    let text = std::str::from_utf8(cursor.take_raw(len)?)?;
                    Some(QueuedTextReplacement::new(partition, Arc::from(text)))
                }
                unknown => return Err(noncanonical_option(unknown)),
            };
            QueuedPayload::Text(QueuedTextPayload { replacement })
        }
    };
    cursor.finish("queued operation body")?;
    Ok(QueuedOperation {
        id,
        entity,
        payload,
    })
}

fn noncanonical_option(tag: u8) -> EncodingError {
    EncodingError::Custom(format!("noncanonical queued option tag {tag:#04x}"))
}

pub(super) const fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

pub(super) fn put_varint(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        bytes.put_u8((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
    bytes.put_u8(value as u8);
}

/// Bounded forward reader over one queue value.
pub(super) struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    pub(super) const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    pub(super) const fn remaining_len(&self) -> usize {
        self.remaining.len()
    }

    pub(super) fn take_raw(&mut self, len: usize) -> Result<&'a [u8], EncodingError> {
        const FIELD_OFFSET: usize = 0;
        if self.remaining.len() < len {
            return Err(EncodingError::BufferTooShort {
                expected: len,
                actual: self.remaining.len(),
            });
        }
        let value = &self.remaining[FIELD_OFFSET..FIELD_OFFSET + len];
        self.remaining = &self.remaining[FIELD_OFFSET + len..FIELD_OFFSET + self.remaining.len()];
        Ok(value)
    }

    pub(super) fn take_u8(&mut self) -> Result<u8, EncodingError> {
        const BYTE_OFFSET: usize = 0;
        Ok(self.take_raw(core::mem::size_of::<u8>())?[BYTE_OFFSET])
    }

    pub(super) fn take_operation_id(&mut self) -> Result<QueuedOperationId, EncodingError> {
        let bytes: [u8; OPERATION_ID_LEN] = self
            .take_raw(OPERATION_ID_LEN)?
            .try_into()
            .expect("operation ID slice has exactly sixteen bytes");
        QueuedOperationId::try_from_u128(u128::from_be_bytes(bytes))
    }

    /// Reads one minimal unsigned LEB128 value.
    pub(super) fn take_varint(&mut self) -> Result<u64, EncodingError> {
        let mut value = 0_u64;
        for index in 0..MAX_VARINT_LEN {
            let byte = self.take_u8()?;
            let payload = u64::from(byte & 0x7F);
            if index == MAX_VARINT_LEN - 1 && payload > 1 {
                return Err(EncodingError::Custom(
                    "queued varint overflows u64".to_string(),
                ));
            }
            value |= payload << (7 * index);
            if byte & 0x80 == 0 {
                if index > 0 && byte == 0 {
                    return Err(EncodingError::Custom(
                        "queued varint is not minimally encoded".to_string(),
                    ));
                }
                return Ok(value);
            }
        }
        Err(EncodingError::Custom(
            "queued varint is too long".to_string(),
        ))
    }

    fn take_partition_body(&mut self, tag: u8) -> Result<TextPartition, EncodingError> {
        match tag {
            0x01 => Ok(TextPartition::Unpartitioned),
            0x02 => {
                let len = usize::try_from(self.take_varint()?).map_err(|_| {
                    EncodingError::Custom("queued partition length overflows".to_string())
                })?;
                if len > MAX_PARTITION_LEN {
                    return Err(EncodingError::Custom(format!(
                        "queued tenant partition is {len} bytes; maximum is {MAX_PARTITION_LEN}"
                    )));
                }
                TextPartition::try_tenant_value(Bytes::copy_from_slice(self.take_raw(len)?))
                    .map_err(|error| EncodingError::Custom(error.to_string()))
            }
            unknown => Err(EncodingError::Custom(format!(
                "unknown queued partition tag {unknown:#04x}"
            ))),
        }
    }

    pub(super) fn finish(self, context: &'static str) -> Result<(), EncodingError> {
        if !self.remaining.is_empty() {
            return Err(EncodingError::Custom(format!(
                "{context} has {} trailing bytes",
                self.remaining.len()
            )));
        }
        Ok(())
    }
}
