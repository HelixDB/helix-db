//! Current-format marker and sentinel values for vector rows.
//!
//! Empty marker values remain represented by an empty byte string. Historical
//! vector transaction guards used the deployed single byte `[1]`; current
//! mutations no longer write that generation-wide hot key, but the exact codec
//! remains part of the unchanged physical vector-row format and migration
//! source inventory.

use bytes::Bytes;

#[cfg(any(test, feature = "fuzzing"))]
use crate::encoding::error::EncodingError;

#[cfg(any(test, feature = "fuzzing"))]
const ACTIVE_TXN_GUARD: u8 = 1;
#[cfg(any(test, feature = "fuzzing"))]
const TXN_GUARD_LEN: usize = core::mem::size_of::<u8>();

/// Encodes a presence-only vector marker as the deployed empty value.
#[must_use]
pub(crate) const fn encode_empty_marker() -> Bytes {
    Bytes::new()
}

/// Encodes the legacy deployed vector transaction-guard value `[1]`.
#[must_use]
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn encode_active_txn_guard() -> Bytes {
    Bytes::copy_from_slice(&[ACTIVE_TXN_GUARD])
}

/// Validates a deployed vector transaction-guard value for physical cleanup.
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn decode_active_txn_guard(data: &[u8]) -> Result<(), EncodingError> {
    if data.len() != TXN_GUARD_LEN {
        return Err(EncodingError::BufferTooShort {
            expected: TXN_GUARD_LEN,
            actual: data.len(),
        });
    }
    if data[0] != ACTIVE_TXN_GUARD {
        return Err(EncodingError::Custom(format!(
            "invalid vector transaction guard marker {:#04x}",
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
        assert_eq!(encode_active_txn_guard().as_ref(), &[1]);
        assert!(decode_active_txn_guard(&[1]).is_ok());
    }

    #[test]
    fn transaction_guard_rejects_wrong_length_and_discriminant() {
        assert!(decode_active_txn_guard(&[]).is_err());
        assert!(decode_active_txn_guard(&[1, 0]).is_err());
        assert!(decode_active_txn_guard(&[2]).is_err());
    }
}
