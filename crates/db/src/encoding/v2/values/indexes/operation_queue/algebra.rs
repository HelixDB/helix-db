//! Associative merge algebra over raw queue values.
//!
//! Each operation ID moves through a closed per-ID state machine:
//!
//! | earlier state        | later record          | composed state          |
//! |----------------------|-----------------------|-------------------------|
//! | unseen               | remove                | removed                 |
//! | unseen               | insert (mode `m`)     | insert `m`, new position|
//! | removed              | remove                | removed                 |
//! | removed              | insert (any mode)     | set, new position       |
//! | insert               | remove                | removed                 |
//! | insert               | insert-if-absent      | unchanged               |
//! | insert               | set                   | set, new position       |
//!
//! Every record is a total function on one ID's state (absent, or present with
//! bytes and a position), and the composed representation is closed under
//! function composition, so the algebra is associative for every grouping.
//! Producers never reuse IDs; if bytes were ever reused, the first retained
//! bytes win deterministically rather than failing only for some groupings.
//!
//! A removal only ever names one ID, so acknowledging an older operation can
//! never erase a newer operation. Remove-then-insert composes to an
//! unconditional set, so an unresolved older base cannot defeat the reset.
//! Resolving against a known base drops removals and turns surviving sets
//! into ordinary retained entries.

use std::collections::{BTreeSet, HashMap};

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;

use super::{
    put_header, put_varint, Cursor, QueueFamily, QueuedOperationId, F32_LEN, HEADER_LEN,
    MAX_PARTITION_LEN, MODE_LEN, OPERATION_ID_LEN, QUEUE_VALUE_KIND, QUEUE_VALUE_VERSION,
};

/// Whether an insert record is conditional on its ID being absent.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertMode {
    /// Insert only when the ID is absent; a present ID keeps its bytes.
    IfAbsent = 0x01,
    /// Remove any existing entry for the ID and append this one.
    Set = 0x02,
}

/// One insert record borrowed from an encoded value.
#[derive(Debug, Clone, Copy)]
pub(super) struct RawInsert<'a> {
    pub(super) mode: InsertMode,
    pub(super) id: QueuedOperationId,
    pub(super) body: &'a [u8],
}

/// Structurally validated view over one encoded queue value.
#[derive(Debug)]
pub(super) struct RawValue<'a> {
    pub(super) family: QueueFamily,
    pub(super) removes: Vec<QueuedOperationId>,
    pub(super) inserts: Vec<RawInsert<'a>>,
}

impl<'a> RawValue<'a> {
    /// Parses and validates every record without resolving any other value.
    pub(super) fn parse(value: &'a [u8]) -> Result<Self, EncodingError> {
        const VERSION_OFFSET: usize = 0;
        const KIND_OFFSET: usize = VERSION_OFFSET + core::mem::size_of::<u8>();
        const FAMILY_OFFSET: usize = KIND_OFFSET + core::mem::size_of::<u8>();
        if value.len() < HEADER_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: HEADER_LEN,
                actual: value.len(),
            });
        }
        if value[VERSION_OFFSET] != QUEUE_VALUE_VERSION {
            return Err(EncodingError::Custom(format!(
                "unsupported operation queue value version {:#04x}",
                value[VERSION_OFFSET]
            )));
        }
        if value[KIND_OFFSET] != QUEUE_VALUE_KIND {
            return Err(EncodingError::UnexpectedValueKind {
                expected: QUEUE_VALUE_KIND,
                actual: value[KIND_OFFSET],
            });
        }
        let family = QueueFamily::try_from_u8(value[FAMILY_OFFSET])?;
        let mut cursor = Cursor::new(&value[HEADER_LEN..HEADER_LEN + value.len() - HEADER_LEN]);

        let remove_count = bounded_count(&mut cursor, OPERATION_ID_LEN)?;
        let mut removes = Vec::with_capacity(remove_count);
        for _ in 0..remove_count {
            let id = cursor.take_operation_id()?;
            if removes.last().is_some_and(|previous| *previous >= id) {
                return Err(EncodingError::Custom(
                    "queued removals are not strictly ascending".to_string(),
                ));
            }
            removes.push(id);
        }

        let insert_count = bounded_count(&mut cursor, MODE_LEN + OPERATION_ID_LEN + 1)?;
        let mut inserts = Vec::with_capacity(insert_count);
        let mut insert_ids = std::collections::HashSet::with_capacity(insert_count);
        for _ in 0..insert_count {
            let mode = match cursor.take_u8()? {
                0x01 => InsertMode::IfAbsent,
                0x02 => InsertMode::Set,
                unknown => {
                    return Err(EncodingError::Custom(format!(
                        "unknown queued insert mode {unknown:#04x}"
                    )));
                }
            };
            let id = cursor.take_operation_id()?;
            if removes.binary_search(&id).is_ok() {
                return Err(EncodingError::Custom(
                    "queued value both removes and inserts one operation ID".to_string(),
                ));
            }
            if !insert_ids.insert(id) {
                return Err(EncodingError::Custom(
                    "queued value inserts one operation ID twice".to_string(),
                ));
            }
            let body_len = usize::try_from(cursor.take_varint()?)
                .map_err(|_| EncodingError::Custom("queued body length overflows".to_string()))?;
            let body = cursor.take_raw(body_len)?;
            validate_body(family, body)?;
            inserts.push(RawInsert { mode, id, body });
        }
        cursor.finish("operation queue value")?;
        if removes.is_empty() && inserts.is_empty() {
            return Err(EncodingError::Custom(
                "operation queue value contains no records".to_string(),
            ));
        }
        Ok(Self {
            family,
            removes,
            inserts,
        })
    }
}

/// Reads a count and rejects one that cannot fit in the remaining bytes.
fn bounded_count(
    cursor: &mut Cursor<'_>,
    minimum_record_len: usize,
) -> Result<usize, EncodingError> {
    let count = usize::try_from(cursor.take_varint()?)
        .map_err(|_| EncodingError::Custom("queued record count overflows".to_string()))?;
    if count > cursor.remaining_len() / minimum_record_len {
        return Err(EncodingError::BufferTooShort {
            expected: count.saturating_mul(minimum_record_len),
            actual: cursor.remaining_len(),
        });
    }
    Ok(count)
}

/// Validates one insert body without allocating its decoded payload.
fn validate_body(family: QueueFamily, body: &[u8]) -> Result<(), EncodingError> {
    let mut cursor = Cursor::new(body);
    match cursor.take_u8()? {
        0x01 | 0x02 => {}
        unknown => {
            return Err(EncodingError::Custom(format!(
                "unknown queued entity kind {unknown:#04x}"
            )));
        }
    }
    cursor.take_varint()?;
    match family {
        QueueFamily::Vector => {
            match cursor.take_u8()? {
                0x00 => {}
                tag => skip_partition_body(&mut cursor, tag)?,
            }
            match cursor.take_u8()? {
                0x00 => {}
                0x01 => {
                    let tag = cursor.take_u8()?;
                    skip_partition_body(&mut cursor, tag)?;
                    let dimension = usize::try_from(cursor.take_varint()?).map_err(|_| {
                        EncodingError::Custom("queued vector dimension overflows".to_string())
                    })?;
                    if dimension == 0 {
                        return Err(EncodingError::Custom(
                            "queued vector replacement must not be empty".to_string(),
                        ));
                    }
                    if dimension > cursor.remaining_len() / F32_LEN {
                        return Err(EncodingError::BufferTooShort {
                            expected: dimension.saturating_mul(F32_LEN),
                            actual: cursor.remaining_len(),
                        });
                    }
                    let components = cursor.take_raw(dimension * F32_LEN)?;
                    if components.chunks_exact(F32_LEN).any(|chunk| {
                        !f32::from_bits(u32::from_be_bytes(
                            chunk.try_into().expect("exact f32 chunk"),
                        ))
                        .is_finite()
                    }) {
                        return Err(EncodingError::Custom(
                            "queued vector component is not finite".to_string(),
                        ));
                    }
                }
                unknown => return Err(super::noncanonical_option(unknown)),
            }
        }
        QueueFamily::Text => match cursor.take_u8()? {
            0x00 => {}
            0x01 => {
                let tag = cursor.take_u8()?;
                skip_partition_body(&mut cursor, tag)?;
                let len = usize::try_from(cursor.take_varint()?).map_err(|_| {
                    EncodingError::Custom("queued text length overflows".to_string())
                })?;
                std::str::from_utf8(cursor.take_raw(len)?)?;
            }
            unknown => return Err(super::noncanonical_option(unknown)),
        },
    }
    cursor.finish("queued operation body")
}

fn skip_partition_body(cursor: &mut Cursor<'_>, tag: u8) -> Result<(), EncodingError> {
    match tag {
        0x01 => Ok(()),
        0x02 => {
            let len = usize::try_from(cursor.take_varint()?).map_err(|_| {
                EncodingError::Custom("queued partition length overflows".to_string())
            })?;
            if len == 0 || len > MAX_PARTITION_LEN {
                return Err(EncodingError::Custom(format!(
                    "queued tenant partition length {len} is outside 1..={MAX_PARTITION_LEN}"
                )));
            }
            cursor.take_raw(len).map(|_| ())
        }
        unknown => Err(EncodingError::Custom(format!(
            "unknown queued partition tag {unknown:#04x}"
        ))),
    }
}

/// Per-ID composed insert, retained in first-effective-position order.
#[derive(Debug, Clone, Copy)]
struct Slot<'a> {
    mode: InsertMode,
    id: QueuedOperationId,
    body: &'a [u8],
    live: bool,
}

/// Composition of an ordered sequence of values.
struct Composition<'a> {
    family: QueueFamily,
    removes: BTreeSet<QueuedOperationId>,
    slots: Vec<Slot<'a>>,
    live: HashMap<QueuedOperationId, usize>,
    input_bytes: usize,
}

impl<'a> Composition<'a> {
    fn new(family: QueueFamily) -> Self {
        Self {
            family,
            removes: BTreeSet::new(),
            slots: Vec::new(),
            live: HashMap::new(),
            input_bytes: 0,
        }
    }

    /// Applies one later value's records to the composed state.
    fn follow(&mut self, value: RawValue<'a>, encoded_len: usize) -> Result<(), EncodingError> {
        // Only structural family mismatches fail; record composition is total.
        if value.family != self.family {
            return Err(EncodingError::Custom(
                "operation queue merge mixes index families".to_string(),
            ));
        }
        self.input_bytes = self.input_bytes.saturating_add(encoded_len);
        for id in value.removes {
            if let Some(slot) = self.live.remove(&id) {
                self.slots[slot].live = false;
            }
            self.removes.insert(id);
        }
        for insert in value.inserts {
            if self.removes.remove(&insert.id) {
                self.push(InsertMode::Set, insert);
                continue;
            }
            let Some(&existing) = self.live.get(&insert.id) else {
                self.push(insert.mode, insert);
                continue;
            };
            match insert.mode {
                InsertMode::IfAbsent => {}
                InsertMode::Set => {
                    self.slots[existing].live = false;
                    self.push(InsertMode::Set, insert);
                }
            }
        }
        Ok(())
    }

    fn push(&mut self, mode: InsertMode, insert: RawInsert<'a>) {
        self.live.insert(insert.id, self.slots.len());
        self.slots.push(Slot {
            mode,
            id: insert.id,
            body: insert.body,
            live: true,
        });
    }

    /// Encodes the canonical unresolved composition.
    fn encode_partial(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(self.input_bytes);
        put_header(&mut bytes, self.family);
        put_varint(&mut bytes, self.removes.len() as u64);
        for id in &self.removes {
            bytes.put_slice(&id.get().to_be_bytes());
        }
        put_varint(&mut bytes, self.live.len() as u64);
        for slot in self.slots.iter().filter(|slot| slot.live) {
            put_raw_insert(&mut bytes, slot.mode, slot.id, slot.body);
        }
        Bytes::from(bytes)
    }

    /// Encodes the resolved value, or `None` when the queue is empty.
    fn encode_resolved(&self) -> Option<Bytes> {
        if self.live.is_empty() {
            return None;
        }
        let mut bytes = Vec::with_capacity(self.input_bytes);
        put_header(&mut bytes, self.family);
        put_varint(&mut bytes, 0);
        put_varint(&mut bytes, self.live.len() as u64);
        for slot in self.slots.iter().filter(|slot| slot.live) {
            put_raw_insert(&mut bytes, InsertMode::IfAbsent, slot.id, slot.body);
        }
        Some(Bytes::from(bytes))
    }
}

fn put_raw_insert(bytes: &mut Vec<u8>, mode: InsertMode, id: QueuedOperationId, body: &[u8]) {
    bytes.put_u8(mode as u8);
    bytes.put_slice(&id.get().to_be_bytes());
    put_varint(bytes, body.len() as u64);
    bytes.put_slice(body);
}

fn compose<'a>(
    values: impl IntoIterator<Item = &'a [u8]>,
) -> Result<Option<Composition<'a>>, EncodingError> {
    let mut composition: Option<Composition<'a>> = None;
    for value in values {
        let raw = RawValue::parse(value)?;
        let composition = composition.get_or_insert_with(|| Composition::new(raw.family));
        composition.follow(raw, value.len())?;
    }
    Ok(composition)
}

/// Resolved merge outcome: a retained queue or the empty queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueueMergeResult {
    /// At least one operation remains outstanding.
    Value(Bytes),
    /// No operation remains; storage keeps a tombstone.
    Empty,
}

/// Composes values whose older base may still be unresolved.
///
/// `existing` is itself a composed value (or an earlier resolved value) that
/// precedes `operands`. Removals and sets are preserved so a later merge with
/// the eventual base applies them exactly once.
pub(crate) fn merge_partial(
    existing: Option<&[u8]>,
    operands: &[Bytes],
) -> Result<Bytes, EncodingError> {
    compose(
        existing
            .into_iter()
            .chain(operands.iter().map(Bytes::as_ref)),
    )?
    .map(|composition| composition.encode_partial())
    .ok_or_else(|| EncodingError::Custom("operation queue merge has no operands".to_string()))
}

/// Resolves operands against an authoritative base (absent means empty).
pub(crate) fn merge_with_base(
    base: Option<&[u8]>,
    operands: &[Bytes],
) -> Result<QueueMergeResult, EncodingError> {
    let composition = compose(base.into_iter().chain(operands.iter().map(Bytes::as_ref)))?;
    Ok(composition
        .and_then(|composition| composition.encode_resolved())
        .map_or(QueueMergeResult::Empty, QueueMergeResult::Value))
}

/// Validates one operand's complete structure without reading any base.
#[cfg(test)]
pub(crate) fn validate_operand(operand: &[u8]) -> Result<(), EncodingError> {
    RawValue::parse(operand).map(|_| ())
}
