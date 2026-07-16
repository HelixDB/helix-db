//! Current-format vector entry-candidate values.
//!
//! Entry-candidate ordering lives in the key, while the node lookup row stores
//! its selected HNSW layer as exactly two big-endian bytes. Keeping that codec
//! here prevents lifecycle and search code from independently assembling the
//! persisted value. This module preserves the deployed bytes.

use bytes::Bytes;

use crate::encoding::error::EncodingError;

const ENTRY_CANDIDATE_LAYER_LEN: usize = core::mem::size_of::<u16>();

/// Encodes an entry-candidate node layer in the deployed `[layer:2 BE]` format.
///
/// ```
/// use db::encoding::v1::values::vectors::entry::{
///     decode_entry_candidate_layer, encode_entry_candidate_layer,
/// };
///
/// let bytes = encode_entry_candidate_layer(0x1234);
/// assert_eq!(bytes.as_ref(), &[0x12, 0x34]);
/// assert_eq!(decode_entry_candidate_layer(&bytes).unwrap(), 0x1234);
/// ```
#[must_use]
pub fn encode_entry_candidate_layer(layer: u16) -> Bytes {
    Bytes::copy_from_slice(&layer.to_be_bytes())
}

/// Decodes an entry-candidate node layer and rejects non-exact values.
pub fn decode_entry_candidate_layer(data: &[u8]) -> Result<u16, EncodingError> {
    if data.len() != ENTRY_CANDIDATE_LAYER_LEN {
        return Err(EncodingError::BufferTooShort {
            expected: ENTRY_CANDIDATE_LAYER_LEN,
            actual: data.len(),
        });
    }

    Ok(u16::from_be_bytes(
        data[0..ENTRY_CANDIDATE_LAYER_LEN]
            .try_into()
            .expect("entry-candidate layer slice is 2 bytes"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_candidate_layer_bytes_are_frozen() {
        let encoded = encode_entry_candidate_layer(0x1234);
        assert_eq!(encoded.as_ref(), &[0x12, 0x34]);
        assert_eq!(decode_entry_candidate_layer(&encoded).unwrap(), 0x1234);
    }

    #[test]
    fn entry_candidate_layer_rejects_truncation_and_trailing_bytes() {
        for malformed in [&[][..], &[0][..], &[0, 1, 2][..]] {
            assert!(matches!(
                decode_entry_candidate_layer(malformed),
                Err(EncodingError::BufferTooShort { .. })
            ));
        }
    }
}
