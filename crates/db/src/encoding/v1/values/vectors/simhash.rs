//! Current-format persisted SimHash values.
//!
//! Dedicated vector-hot SimHash rows contain exactly one little-endian `u64`.
//! The algorithm seed/version belongs in the additive generation descriptor,
//! not in this deployed value, so this codec deliberately preserves eight bytes.

use crate::encoding::error::EncodingError;

const SIMHASH_LEN: usize = core::mem::size_of::<u64>();

/// Encodes SimHash bits in the deployed eight-byte little-endian format.
#[must_use]
pub(crate) const fn encode_simhash(bits: u64) -> [u8; SIMHASH_LEN] {
    bits.to_le_bytes()
}

/// Decodes an exact current-format persisted SimHash value.
pub(crate) fn decode_simhash(data: &[u8]) -> Result<u64, EncodingError> {
    if data.len() != SIMHASH_LEN {
        return Err(EncodingError::BufferTooShort {
            expected: SIMHASH_LEN,
            actual: data.len(),
        });
    }

    Ok(u64::from_le_bytes(
        data[0..SIMHASH_LEN]
            .try_into()
            .expect("SimHash value slice is 8 bytes"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simhash_bytes_are_frozen() {
        let encoded = encode_simhash(0x0102_0304_0506_0708);
        assert_eq!(encoded, [8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(decode_simhash(&encoded).unwrap(), 0x0102_0304_0506_0708);
    }

    #[test]
    fn simhash_rejects_truncation_and_trailing_bytes() {
        assert!(decode_simhash(&[0; SIMHASH_LEN - 1]).is_err());
        assert!(decode_simhash(&[0; SIMHASH_LEN + 1]).is_err());
    }
}
