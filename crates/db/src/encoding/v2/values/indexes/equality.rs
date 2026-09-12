//! Stored values for lifecycle-managed equality indexes.

mod admission;

use std::io::Cursor;

use bytes::{BufMut, Bytes};
use roaring::RoaringTreemap;

use crate::encoding::error::EncodingError;
use crate::encoding::v2::keys::SecondaryEntryLane;
use crate::index_lifecycle::work::SecondaryEntryValue;
use crate::index_lifecycle::IndexEntityId;

use super::super::{
    put_generation, put_index_id, put_secondary_lane, take_generation, take_index_id,
    take_secondary_lane, ValueDecoder, ValueEncoder,
};

const SECONDARY_ENTRY_KIND: u8 = 0x05;
const BITMAP_MEMBERSHIP_DELTA_MAGIC: &[u8; 8] = b"HLXRBM2\0";
const BITMAP_MEMBERSHIP_DELTA_LEN_PREFIX_LEN: usize = core::mem::size_of::<u32>();

/// Associative last-write-wins membership changes for one physical bitmap row.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct BitmapMembershipDelta {
    additions: RoaringTreemap,
    removals: RoaringTreemap,
}

impl BitmapMembershipDelta {
    pub(crate) fn prepare_if_delta(
        bytes: &[u8],
    ) -> Result<Option<PreparedBitmap<'_>>, EncodingError> {
        bytes
            .starts_with(BITMAP_MEMBERSHIP_DELTA_MAGIC)
            .then(|| SecondaryEqualityBitmapValue::prepare(bytes))
            .transpose()
    }

    pub(crate) fn from_additions(additions: RoaringTreemap) -> Self {
        Self {
            additions,
            removals: RoaringTreemap::new(),
        }
    }

    pub(crate) fn add(&mut self, id: u64) {
        self.removals.remove(id);
        self.additions.insert(id);
    }

    pub(crate) fn remove(&mut self, id: u64) {
        self.additions.remove(id);
        self.removals.insert(id);
    }

    /// Applies a newer delta after this delta.
    pub(crate) fn compose(&mut self, newer: &Self) {
        self.removals -= &newer.additions;
        self.additions |= &newer.additions;
        self.additions -= &newer.removals;
        self.removals |= &newer.removals;
    }

    pub(crate) fn apply_to(&self, ids: &mut RoaringTreemap) {
        *ids -= &self.removals;
        *ids |= &self.additions;
    }

    pub(crate) fn members(&self) -> impl Iterator<Item = u64> + '_ {
        self.additions.iter().chain(self.removals.iter())
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut additions = Vec::new();
        self.additions
            .serialize_into(&mut additions)
            .expect("serializing membership additions into memory is infallible");
        let mut removals = Vec::new();
        self.removals
            .serialize_into(&mut removals)
            .expect("serializing membership removals into memory is infallible");
        let mut bytes = Vec::with_capacity(
            BITMAP_MEMBERSHIP_DELTA_MAGIC.len()
                + BITMAP_MEMBERSHIP_DELTA_LEN_PREFIX_LEN
                + additions.len()
                + BITMAP_MEMBERSHIP_DELTA_LEN_PREFIX_LEN
                + removals.len(),
        );
        bytes.extend_from_slice(BITMAP_MEMBERSHIP_DELTA_MAGIC);
        bytes.put_u32(u32::try_from(additions.len()).expect("roaring additions fit u32"));
        bytes.extend_from_slice(&additions);
        bytes.put_u32(u32::try_from(removals.len()).expect("roaring removals fit u32"));
        bytes.extend_from_slice(&removals);
        Bytes::from(bytes)
    }

    pub(crate) fn decode_if_delta(bytes: &[u8]) -> Result<Option<Self>, EncodingError> {
        if !bytes.starts_with(BITMAP_MEMBERSHIP_DELTA_MAGIC) {
            return Ok(None);
        }
        let mut offset = BITMAP_MEMBERSHIP_DELTA_MAGIC.len();
        let additions = take_delta_bitmap(bytes, &mut offset, "additions")?;
        let removals = take_delta_bitmap(bytes, &mut offset, "removals")?;
        if offset != bytes.len() {
            return Err(EncodingError::Custom(format!(
                "bitmap membership delta has {} trailing bytes",
                bytes.len() - offset
            )));
        }
        if !additions.is_disjoint(&removals) {
            return Err(EncodingError::Custom(
                "bitmap membership delta additions and removals overlap".to_string(),
            ));
        }
        Ok(Some(Self {
            additions,
            removals,
        }))
    }
}

fn take_delta_bitmap(
    bytes: &[u8],
    offset: &mut usize,
    name: &str,
) -> Result<RoaringTreemap, EncodingError> {
    let length_end = offset
        .checked_add(BITMAP_MEMBERSHIP_DELTA_LEN_PREFIX_LEN)
        .ok_or_else(|| EncodingError::Custom("bitmap delta length overflow".to_string()))?;
    if length_end > bytes.len() {
        return Err(EncodingError::BufferTooShort {
            expected: length_end,
            actual: bytes.len(),
        });
    }
    let length = u32::from_be_bytes(
        bytes[*offset..*offset + BITMAP_MEMBERSHIP_DELTA_LEN_PREFIX_LEN]
            .try_into()
            .expect("validated bitmap delta length slice is four bytes"),
    ) as usize;
    *offset = length_end;
    let value_end = offset
        .checked_add(length)
        .ok_or_else(|| EncodingError::Custom("bitmap delta value overflow".to_string()))?;
    if value_end > bytes.len() {
        return Err(EncodingError::BufferTooShort {
            expected: value_end,
            actual: bytes.len(),
        });
    }
    let mut cursor = Cursor::new(&bytes[*offset..*offset + length]);
    let bitmap = RoaringTreemap::deserialize_from(&mut cursor).map_err(|error| {
        EncodingError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("failed to decode bitmap membership delta {name}: {error}"),
        ))
    })?;
    if cursor.position() != length as u64 {
        return Err(EncodingError::Custom(format!(
            "bitmap membership delta {name} has {} trailing bytes",
            length as u64 - cursor.position()
        )));
    }
    *offset = value_end;
    Ok(bitmap)
}

/// Portable V4 value for one non-unique equality key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SecondaryEqualityBitmapValue(RoaringTreemap);

/// Structurally bounded encoded bitmap. Construction does not allocate decoded
/// containers. A query can reserve `allocation_bound` before consuming this
/// value through the ordinary validating decoder.
pub(crate) struct PreparedBitmap<'a> {
    bytes: &'a [u8],
    allocation_bound: usize,
}

impl SecondaryEqualityBitmapValue {
    pub(crate) fn new(ids: RoaringTreemap) -> Self {
        Self(ids)
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut bytes = Vec::new();
        self.0
            .serialize_into(&mut bytes)
            .expect("serializing a RoaringTreemap into memory is infallible");
        Bytes::from(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, EncodingError> {
        Self::prepare(bytes)?.decode()
    }

    pub(crate) fn prepare(bytes: &[u8]) -> Result<PreparedBitmap<'_>, EncodingError> {
        let allocation_bound = admission::allocation_bound(bytes)?;
        Ok(PreparedBitmap {
            bytes,
            allocation_bound,
        })
    }

    pub(crate) fn ids(&self) -> &RoaringTreemap {
        &self.0
    }

    pub(crate) fn into_ids(self) -> RoaringTreemap {
        self.0
    }
}

impl<'a> PreparedBitmap<'a> {
    /// Portable-only preflight, retaining the decoder's historical suffix
    /// leniency. Adjacency fields cannot contain membership-delta wrappers.
    pub(crate) fn portable(bytes: &'a [u8]) -> Result<Self, EncodingError> {
        let (overhead, consumed) = admission::portable_prefix(bytes)?;
        Ok(Self {
            bytes: &bytes[..consumed],
            allocation_bound: size_of::<RoaringTreemap>()
                .saturating_add(overhead)
                .saturating_add(consumed.saturating_mul(2)),
        })
    }

    pub(crate) fn allocation_bound(&self) -> usize {
        self.allocation_bound
    }

    pub(crate) fn decode(self) -> Result<SecondaryEqualityBitmapValue, EncodingError> {
        let ids = if let Some(delta) = BitmapMembershipDelta::decode_if_delta(self.bytes)? {
            delta.additions
        } else {
            let mut cursor = Cursor::new(self.bytes);
            let ids = RoaringTreemap::deserialize_from(&mut cursor).map_err(|error| {
                EncodingError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to decode secondary equality bitmap: {error}"),
                ))
            })?;
            // Structural preflight has already consumed exactly this payload.
            assert_eq!(cursor.position(), self.bytes.len() as u64);
            ids
        };
        let allocated = retained_allocation_estimate(&ids);
        assert!(allocated <= self.allocation_bound(),
            "bitmap decoder footprint {allocated} exceeded its preflight allocation bound {} for {} encoded bytes",
            self.allocation_bound(), self.bytes.len());
        Ok(SecondaryEqualityBitmapValue(ids))
    }
}

pub(crate) fn encode_entry(value: &SecondaryEntryValue) -> Result<Bytes, EncodingError> {
    if !value.lane.is_equality() {
        return Err(EncodingError::Custom(
            "range lane cannot use the equality value codec".to_string(),
        ));
    }
    let mut encoder = ValueEncoder::with_header(SECONDARY_ENTRY_KIND);
    put_index_id(&mut encoder, value.index_id);
    put_generation(&mut encoder, value.generation);
    put_secondary_lane(&mut encoder, value.lane);
    encoder.put_u64(value.entity_id.get());
    Ok(encoder.finish())
}

pub(crate) fn decode_entry(
    expected_lane: SecondaryEntryLane,
    value: &[u8],
) -> Result<SecondaryEntryValue, EncodingError> {
    if !expected_lane.is_equality() {
        return Err(EncodingError::Custom(
            "range lane cannot use the equality value codec".to_string(),
        ));
    }
    let mut decoder = ValueDecoder::new(value)?;
    if decoder.kind() != SECONDARY_ENTRY_KIND {
        return Err(EncodingError::UnexpectedValueKind {
            expected: SECONDARY_ENTRY_KIND,
            actual: decoder.kind(),
        });
    }
    let decoded = SecondaryEntryValue {
        index_id: take_index_id(&mut decoder)?,
        generation: take_generation(&mut decoder)?,
        lane: take_secondary_lane(&mut decoder)?,
        entity_id: IndexEntityId::new(decoder.take_u64()?),
    };
    decoder.finish()?;
    if decoded.lane != expected_lane {
        return Err(EncodingError::Custom(
            "secondary equality key/value lane mismatch".to_string(),
        ));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::new(), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
    }

    #[test]
    fn bitmap_value_round_trips_portable_roaring_bytes() {
        let ids = RoaringTreemap::from_iter([1, 9, u64::from(u32::MAX) + 1]);
        let encoded = SecondaryEqualityBitmapValue::new(ids.clone()).encode();
        let decoded = SecondaryEqualityBitmapValue::decode(&encoded).unwrap();

        assert_eq!(decoded.ids(), &ids);
        assert!(SecondaryEqualityBitmapValue::decode(b"not roaring").is_err());
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(SecondaryEqualityBitmapValue::decode(&trailing).is_err());
    }

    #[test]
    fn bitmap_membership_delta_round_trips_and_composes_last_write_wins() {
        let mut older = BitmapMembershipDelta::default();
        older.add(1);
        older.add(2);
        older.remove(3);
        let mut newer = BitmapMembershipDelta::default();
        newer.remove(1);
        newer.add(3);
        newer.remove(4);
        older.compose(&newer);

        let decoded = BitmapMembershipDelta::decode_if_delta(&older.encode())
            .unwrap()
            .expect("membership delta marker is recognized");
        let mut base = RoaringTreemap::from_iter([1, 4, 5]);
        decoded.apply_to(&mut base);

        assert_eq!(base.iter().collect::<Vec<_>>(), vec![2, 3, 5]);
        assert!(decoded.additions.contains(2));
        assert!(decoded.additions.contains(3));
        assert!(decoded.removals.contains(1));
        assert!(decoded.removals.contains(4));
    }

    #[test]
    fn bitmap_membership_delta_decoder_rejects_every_invalid_boundary() {
        let mut delta = BitmapMembershipDelta::default();
        delta.add(1);
        let encoded = delta.encode();
        assert_eq!(
            BitmapMembershipDelta::decode_if_delta(b"portable-roaring").unwrap(),
            None
        );
        for length in BITMAP_MEMBERSHIP_DELTA_MAGIC.len()..encoded.len() {
            assert!(BitmapMembershipDelta::decode_if_delta(&encoded[0..length]).is_err());
        }

        let mut trailing = encoded.to_vec();
        trailing.push(0xFF);
        assert!(BitmapMembershipDelta::decode_if_delta(&trailing).is_err());

        let overlapping = BitmapMembershipDelta {
            additions: RoaringTreemap::from_iter([7]),
            removals: RoaringTreemap::from_iter([7]),
        };
        assert!(BitmapMembershipDelta::decode_if_delta(&overlapping.encode()).is_err());
    }

    #[test]
    fn equality_value_decoders_project_delta_against_an_empty_base() {
        let mut delta = BitmapMembershipDelta::default();
        delta.add(3);
        delta.remove(4);
        let encoded = delta.encode();

        assert_eq!(
            SecondaryEqualityBitmapValue::decode(&encoded)
                .unwrap()
                .into_ids()
                .iter()
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(
            SecondaryEqualityValue::decode(&encoded)
                .unwrap()
                .into_ids()
                .iter()
                .collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[test]
    fn equality_entry_value_is_lane_bound_and_byte_frozen() {
        let value = SecondaryEntryValue {
            index_id: crate::index_lifecycle::IndexId::new(1).unwrap(),
            generation: crate::index_lifecycle::IndexGenerationId::new(2).unwrap(),
            lane: SecondaryEntryLane::NodeEquality,
            entity_id: IndexEntityId::new(3),
        };
        let encoded = encode_entry(&value).unwrap();

        assert_eq!(
            decode_entry(SecondaryEntryLane::NodeEquality, &encoded).unwrap(),
            value
        );
        assert!(decode_entry(SecondaryEntryLane::EdgeEquality, &encoded).is_err());
        assert!(decode_entry(SecondaryEntryLane::NodeRangeAscending, &encoded).is_err());
        insta::assert_snapshot!(
            hex(&encoded),
            @"010500000000000000010000000000000002010000000000000003"
        );
    }
}

/// Decoded current equality-row value containing node or edge identifiers.
///
/// The key family determines whether the identifiers are nodes or edges. The
/// value deliberately retains the deployed untyped integer bitmap so existing
/// bytes remain unchanged; callers receive it only after this codec validates
/// the portable Roaring representation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SecondaryEqualityValue(RoaringTreemap);

impl SecondaryEqualityValue {
    /// Preserve the built-in codec's deployed portable-prefix leniency while
    /// retaining strict framing for membership deltas.
    pub(crate) fn prepare(bytes: &[u8]) -> Result<PreparedBitmap<'_>, EncodingError> {
        if bytes.starts_with(BITMAP_MEMBERSHIP_DELTA_MAGIC) {
            return SecondaryEqualityBitmapValue::prepare(bytes);
        }
        PreparedBitmap::portable(bytes)
    }

    /// Encodes identifiers with the exact current portable Roaring format.
    pub(crate) fn encode_ids(ids: &RoaringTreemap) -> Bytes {
        let mut bytes = Vec::new();
        ids.serialize_into(&mut bytes)
            .expect("serializing a RoaringTreemap into memory is infallible");
        Bytes::from(bytes)
    }

    /// Decodes the exact current portable Roaring equality-row value.
    pub(crate) fn decode(data: &[u8]) -> Result<Self, EncodingError> {
        if let Some(delta) = BitmapMembershipDelta::decode_if_delta(data)? {
            return Ok(Self(delta.additions));
        }
        let ids = RoaringTreemap::deserialize_from(Cursor::new(data)).map_err(|error| {
            EncodingError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to decode RoaringTreemap: {error}"),
            ))
        })?;
        Ok(Self(ids))
    }

    /// Returns whether this physical equality row contains an exact entity ID.
    #[cfg(test)]
    pub(crate) fn contains(&self, id: u64) -> bool {
        self.0.contains(id)
    }

    /// Returns the number of entity IDs represented by this physical row.
    #[cfg(test)]
    pub(crate) fn len(&self) -> u64 {
        self.0.len()
    }

    /// Releases the validated identifier set to existing search callers.
    pub(crate) fn into_ids(self) -> RoaringTreemap {
        self.0
    }
}

#[cfg(test)]
mod deployed_row_tests {
    use super::*;

    #[test]
    fn equality_value_preserves_ids_and_rejects_malformed_bytes() {
        let ids = RoaringTreemap::from_iter([7, 9, u64::from(u32::MAX) + 1]);
        let encoded = SecondaryEqualityValue::encode_ids(&ids);
        let decoded = SecondaryEqualityValue::decode(&encoded).unwrap();

        assert_eq!(decoded.len(), 3);
        assert!(decoded.contains(7));
        assert!(decoded.contains(u64::from(u32::MAX) + 1));
        assert!(!decoded.contains(8));
        assert_eq!(decoded.into_ids(), ids);
        assert!(SecondaryEqualityValue::decode(b"not a bitmap").is_err());
    }
}

/// Estimated retained allocation of a compressed ID bitmap. Roaring exposes
/// array capacity but not spare run/container-vector capacity. Fresh decoded
/// query bitmaps retain their structural preflight bound instead of shrinking
/// their reservation to this estimate.
pub(crate) fn retained_allocation_estimate(bitmap: &roaring::RoaringTreemap) -> usize {
    bitmap.bitmaps().fold(
        size_of::<roaring::RoaringTreemap>(),
        |bytes, (_, bitmap)| {
            let statistics = bitmap.statistics();
            // roaring 0.11.3 reports BitmapStore::capacity() (bits) in
            // n_bytes_bitset_containers. A portable bitset owns 65,536 bits,
            // so derive its byte footprint from the container count instead.
            let payload = statistics
                .n_bytes_array_containers
                .saturating_add(
                    u64::from(statistics.n_bitset_containers)
                        .saturating_mul(65536 / u64::from(u8::BITS)),
                )
                .saturating_add(statistics.n_bytes_run_containers);
            bytes
                .saturating_add(usize::try_from(payload).unwrap_or(usize::MAX))
                .saturating_add((statistics.n_containers as usize).saturating_mul(256))
                .saturating_add(512)
        },
    )
}
