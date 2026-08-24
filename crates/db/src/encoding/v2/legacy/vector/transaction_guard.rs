//! Retired vector transaction-guard key and value.

use bytes::BufMut;
#[cfg(any(test, feature = "fuzzing", feature = "production-coverage"))]
use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::encoding::v2::keys::indexes::vector::{
    DEFAULT_INDEX_ID_OFFSET, DEFAULT_INDEX_TYPE_OFFSET, DEFAULT_KEY_LEN, DEFAULT_KIND_OFFSET,
    INDEX_ID_LEN, INDEX_TYPE_VECTOR, KEY_KIND_TXN_GUARD, KEY_SPACE_INDEX,
};

const ACTIVE_TXN_GUARD: u8 = 1;
const TXN_GUARD_LEN: usize = core::mem::size_of::<u8>();

/// Retired `[0x03][0x03][index_id:8][kind=txn_guard]` key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LegacyVectorTxnGuardKey {
    index_id: u64,
}

impl LegacyVectorTxnGuardKey {
    pub(crate) const fn new(index_id: u64) -> Self {
        Self { index_id }
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        match slice.len().cmp(&DEFAULT_KEY_LEN) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected: DEFAULT_KEY_LEN,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {DEFAULT_KEY_LEN} bytes, got {}",
                    slice.len()
                )));
            }
        }
        if slice[0] != KEY_SPACE_INDEX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }
        if slice[DEFAULT_INDEX_TYPE_OFFSET] != INDEX_TYPE_VECTOR {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector index type ({INDEX_TYPE_VECTOR:#04x}), got {:#04x}",
                slice[DEFAULT_INDEX_TYPE_OFFSET]
            )));
        }
        if slice[DEFAULT_KIND_OFFSET] != KEY_KIND_TXN_GUARD {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector txn guard key kind ({KEY_KIND_TXN_GUARD:#04x}), got {:#04x}",
                slice[DEFAULT_KIND_OFFSET]
            )));
        }
        Ok(Self::new(u64::from_be_bytes(
            slice[DEFAULT_INDEX_ID_OFFSET..DEFAULT_INDEX_ID_OFFSET + INDEX_ID_LEN]
                .try_into()
                .expect("index id slice is 8 bytes"),
        )))
    }

    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        DEFAULT_KEY_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KEY_SPACE_INDEX);
        buf.put_u8(INDEX_TYPE_VECTOR);
        buf.put_u64(self.index_id);
        buf.put_u8(KEY_KIND_TXN_GUARD);
    }
}

/// Encodes the retired deployed transaction-guard value `[1]` for fixtures.
#[must_use]
#[cfg(any(test, feature = "fuzzing", feature = "production-coverage"))]
#[allow(dead_code)]
pub(crate) fn encode_active_txn_guard() -> Bytes {
    Bytes::copy_from_slice(&[ACTIVE_TXN_GUARD])
}

/// Validates a retired transaction-guard value for physical cleanup.
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
    fn transaction_guard_bytes_and_errors_are_frozen() {
        let key = LegacyVectorTxnGuardKey::new(7);
        let mut encoded = Vec::new();
        key.encode_into(&mut encoded);
        assert_eq!(encoded, [3, 3, 0, 0, 0, 0, 0, 0, 0, 7, 9]);
        assert_eq!(
            LegacyVectorTxnGuardKey::parse_from_slice(&encoded).unwrap(),
            key
        );
        assert!(LegacyVectorTxnGuardKey::parse_from_slice(&encoded[..10]).is_err());
        assert!(
            LegacyVectorTxnGuardKey::parse_from_slice(&[encoded.as_slice(), &[0]].concat())
                .is_err()
        );
        assert_eq!(encode_active_txn_guard().as_ref(), &[1]);
        assert!(decode_active_txn_guard(&[1]).is_ok());
        assert!(decode_active_txn_guard(&[]).is_err());
        assert!(decode_active_txn_guard(&[1, 0]).is_err());
        assert!(decode_active_txn_guard(&[2]).is_err());
    }
}
