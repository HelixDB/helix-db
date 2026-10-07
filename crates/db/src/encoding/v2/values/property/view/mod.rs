//! In-place reads of stored property rows.
//!
//! Every read here validates a row with exactly the check
//! [`super::decode_properties`] performs: the same archived type, validator
//! and error mapping. A row that function rejects is rejected here with the
//! same [`EncodingError::Rkyv`] message (which, with debug assertions, prints
//! the validated buffer's addresses). Only the requested properties are then
//! deserialized; the others are never materialized.
//!
//! Validation needs the archive at an address aligned to
//! [`PROPERTY_ALIGNMENT`], the largest alignment of any archived property
//! type. A row already at such an address is read where it is; any other row
//! is copied once into an aligned buffer its caller reuses across rows.

use bytes::Bytes;
use rkyv::{rancor, util::AlignedVec};

use super::{
    property::{ArchivedProperty, Property},
    property_value::PropertyValue,
    row::PROPERTY_ALIGNMENT,
};
use crate::encoding::error::EncodingError;

#[cfg(test)]
pub(crate) mod tests;

/// Reusable aligned copy target for rows stored at unaligned addresses.
///
/// It keeps the capacity of the largest row copied into it, so an owner
/// that lives for one batch or step retains at most one such row.
pub(crate) type Scratch = AlignedVec<PROPERTY_ALIGNMENT>;

/// Validates `data` as a property row and returns its archived properties.
///
/// `data` is read in place when aligned; otherwise it is copied into
/// `scratch`, replacing what it held. Empty data is the empty row and is not
/// validated, as in [`super::decode_properties`].
pub(crate) fn access<'a>(
    data: &'a [u8],
    scratch: &'a mut Scratch,
) -> Result<&'a [ArchivedProperty], EncodingError> {
    if data.is_empty() {
        return Ok(&[]);
    }
    let aligned = if data.as_ptr().align_offset(PROPERTY_ALIGNMENT) == 0 {
        data
    } else {
        scratch.clear();
        scratch.extend_from_slice(data);
        scratch.as_slice()
    };
    rkyv::access::<rkyv::Archived<Vec<Property>>, rancor::Error>(aligned)
        .map(|properties| properties.as_slice())
        .map_err(|error| EncodingError::Rkyv(error.to_string()))
}

/// Decodes only the properties whose names `keep` selects, in stored order
/// and with duplicates, from a row validated as in [`access`].
///
/// The result equals the full decode with every unselected entry removed, so
/// first-match and any-match lookups of selected names answer exactly as
/// they would over the full row.
pub(crate) fn decode_selected(
    data: &[u8],
    scratch: &mut Scratch,
    keep: impl Fn(&str) -> bool,
) -> Result<Vec<Property>, EncodingError> {
    access(data, scratch)?
        .iter()
        .filter(|property| keep(property.name.as_str()))
        .map(deserialize_property)
        .collect()
}

/// Aligned copies released by dropped [`Row`]s, kept for later rows.
///
/// Its owner holds at most as many copies as it had rows alive at once,
/// each with the capacity of the largest row copied into it, until the
/// owner is dropped. Owners live for one predicate scan or one record batch.
#[derive(Default)]
pub(crate) struct Buffers(Vec<Scratch>);

#[cfg(test)]
impl Buffers {
    /// The number of released copies held for reuse.
    pub(crate) fn retained(&self) -> usize {
        self.0.len()
    }
}

/// A stored property row validated once and then read in place.
///
/// Construction is the only validation; every later read relies on it, so
/// the bytes behind a `Row` are never mutated.
///
/// An aligned row keeps the storage `Bytes` it was read as, which may be a
/// slice of a larger memtable or block buffer. Unlike the request read
/// cache, which copies values it keeps so a small value cannot pin a large
/// buffer (see `Budget::copy_read`), a `Row` lives only as long as one
/// predicate evaluation or one record batch of at most
/// `RECORD_BATCH_ROWS` rows, and its row bytes stay charged to the request
/// budget while it is held.
pub(crate) struct Row(Storage);

enum Storage {
    /// The empty row, which has no archive.
    Empty,
    /// Storage bytes already at an aligned address, shared without a copy.
    Shared(Bytes),
    /// An aligned copy of storage bytes at an unaligned address.
    Copied(Scratch),
}

impl Row {
    /// Validates `data`, copying it into a buffer taken from `buffers` only
    /// when it is not aligned. Fails exactly when
    /// [`super::decode_properties`] fails on the same bytes, dropping the
    /// copy it validated.
    pub(crate) fn new(data: Bytes, buffers: &mut Buffers) -> Result<Self, EncodingError> {
        if data.is_empty() {
            return Ok(Self(Storage::Empty));
        }
        let shared = data.as_ptr().align_offset(PROPERTY_ALIGNMENT) == 0;
        // An aligned row never touches the copy, which then never allocates.
        let mut copy = if shared {
            Scratch::new()
        } else {
            buffers.0.pop().unwrap_or_default()
        };
        access(&data, &mut copy)?;
        Ok(Self(if shared {
            Storage::Shared(data)
        } else {
            Storage::Copied(copy)
        }))
    }

    /// The validated archived properties, in stored order.
    pub(crate) fn properties(&self) -> &[ArchivedProperty] {
        let bytes = match &self.0 {
            Storage::Empty => return &[],
            Storage::Shared(bytes) => bytes.as_ref(),
            Storage::Copied(copy) => copy.as_slice(),
        };
        // SAFETY: `Row::new` validated exactly these bytes, at this address,
        // as `Archived<Vec<Property>>`. Neither `Bytes` nor the private copy
        // is ever mutated, and their heap address does not change when the
        // `Row` moves.
        unsafe { rkyv::access_unchecked::<rkyv::Archived<Vec<Property>>>(bytes) }.as_slice()
    }

    /// The value of the first property named `name`, deserialized alone.
    pub(crate) fn value(&self, name: &str) -> Result<Option<PropertyValue>, EncodingError> {
        self.properties()
            .iter()
            .find(|property| property.name.as_str() == name)
            .map(|property| {
                rkyv::deserialize::<PropertyValue, rancor::Error>(&property.value)
                    .map_err(|error| EncodingError::Rkyv(error.to_string()))
            })
            .transpose()
    }

    /// Every property, exactly as [`super::decode_properties`] returns them.
    pub(crate) fn decode(&self) -> Result<Vec<Property>, EncodingError> {
        self.properties().iter().map(deserialize_property).collect()
    }

    /// Returns this row's aligned copy, if it made one, to `buffers`.
    pub(crate) fn recycle(self, buffers: &mut Buffers) {
        let Storage::Copied(copy) = self.0 else {
            return;
        };
        buffers.0.push(copy);
    }
}

fn deserialize_property(property: &ArchivedProperty) -> Result<Property, EncodingError> {
    rkyv::deserialize::<Property, rancor::Error>(property)
        .map_err(|error| EncodingError::Rkyv(error.to_string()))
}
