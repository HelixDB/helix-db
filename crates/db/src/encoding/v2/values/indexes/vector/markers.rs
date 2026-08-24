//! Current-format marker and sentinel values for vector rows.
//!
//! Empty marker values remain represented by an empty byte string.

use crate::encoding::error::EncodingError;
use bytes::Bytes;

const SIMHASH_DIRECTORY_MARKER_V1: u8 = 1;
const SIMHASH_DIRECTORY_MARKER_LEN: usize = core::mem::size_of::<u8>();

/// Encodes a presence-only vector marker as the deployed empty value.
#[must_use]
pub(crate) const fn encode_empty_marker() -> Bytes {
    Bytes::new()
}

/// Validates a presence-only vector marker as exactly empty.
pub(crate) fn decode_empty_marker(data: &[u8]) -> Result<(), EncodingError> {
    if !data.is_empty() {
        return Err(EncodingError::Custom(format!(
            "vector presence marker must be empty, got {} bytes",
            data.len()
        )));
    }
    Ok(())
}

/// Encodes the versioned marker for one complete SimHash directory row.
#[must_use]
pub(crate) fn encode_simhash_directory_marker_v1() -> Bytes {
    Bytes::copy_from_slice(&[SIMHASH_DIRECTORY_MARKER_V1])
}

/// Validates the exact deployed SimHash directory marker.
pub(crate) fn decode_simhash_directory_marker_v1(data: &[u8]) -> Result<(), EncodingError> {
    if data.len() != SIMHASH_DIRECTORY_MARKER_LEN {
        return Err(EncodingError::BufferTooShort {
            expected: SIMHASH_DIRECTORY_MARKER_LEN,
            actual: data.len(),
        });
    }
    if data[0] != SIMHASH_DIRECTORY_MARKER_V1 {
        return Err(EncodingError::Custom(format!(
            "invalid SimHash directory marker {:#04x}",
            data[0]
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_bytes_are_frozen() {
        assert!(encode_empty_marker().is_empty());
        assert!(decode_empty_marker(&[]).is_ok());
        assert!(decode_empty_marker(&[0]).is_err());
        assert_eq!(encode_simhash_directory_marker_v1().as_ref(), &[1]);
        assert!(decode_simhash_directory_marker_v1(&[1]).is_ok());
        assert!(decode_simhash_directory_marker_v1(&[]).is_err());
        assert!(decode_simhash_directory_marker_v1(&[2]).is_err());
    }
}
