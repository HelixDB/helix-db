//! Byte-compatible construction and structural splitting of vector item rows.
//!
//! The deployed row is a metric-specific header followed directly by codec
//! payload bytes. This module owns that physical assembly without interpreting
//! either part. The vector runtime must subsequently bind the split bytes to a
//! validated generation, dimension, codec, and metric before constructing an
//! item usable by distance kernels.

use bytes::Bytes;

use crate::encoding::error::EncodingError;

/// Structurally separated slices from one current-format vector item row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorItemParts<'a> {
    header: &'a [u8],
    payload: &'a [u8],
}

impl<'a> VectorItemParts<'a> {
    /// Returns the uninterpreted metric-specific header bytes.
    pub(crate) const fn header(self) -> &'a [u8] {
        self.header
    }

    /// Returns the uninterpreted codec payload bytes.
    pub(crate) const fn payload(self) -> &'a [u8] {
        self.payload
    }
}

/// Encodes header and payload bytes in the deployed contiguous row format.
#[must_use]
pub(crate) fn encode_item_parts(header: &[u8], payload: &[u8]) -> Bytes {
    let capacity = header
        .len()
        .checked_add(payload.len())
        .expect("vector item row length exceeds usize");
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(header);
    bytes.extend_from_slice(payload);
    Bytes::from(bytes)
}

/// Splits a current-format row at the bound metric header size.
///
/// This proves only that the complete header is present. The caller must decode
/// and validate the payload under the generation handle before using either part.
pub(crate) fn split_item_parts(
    data: &[u8],
    header_len: usize,
) -> Result<VectorItemParts<'_>, EncodingError> {
    if data.len() < header_len {
        return Err(EncodingError::BufferTooShort {
            expected: header_len,
            actual: data.len(),
        });
    }

    Ok(VectorItemParts {
        header: &data[0..header_len],
        payload: &data[header_len..],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_parts_preserve_the_deployed_concatenation() {
        let encoded = encode_item_parts(&[1, 2], &[3, 4, 5]);
        assert_eq!(encoded.as_ref(), &[1, 2, 3, 4, 5]);

        let parts = split_item_parts(&encoded, 2).unwrap();
        assert_eq!(parts.header(), &[1, 2]);
        assert_eq!(parts.payload(), &[3, 4, 5]);
    }

    #[test]
    fn item_parts_reject_a_truncated_header() {
        assert!(matches!(
            split_item_parts(&[1], 2),
            Err(EncodingError::BufferTooShort {
                expected: 2,
                actual: 1
            })
        ));
    }
}
