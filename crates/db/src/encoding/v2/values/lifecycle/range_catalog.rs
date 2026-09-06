//! Completion evidence for the direction-aware range catalog migration.

use crate::encoding::error::EncodingError;
use bytes::Bytes;

/// Presence proves that every range catalog record uses its physical direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RangeCatalogReady;

impl RangeCatalogReady {
    pub(crate) const fn encode(self) -> Bytes {
        Bytes::from_static(b"1")
    }

    pub(crate) fn decode(value: &[u8]) -> Result<Self, EncodingError> {
        if value == b"1" {
            Ok(Self)
        } else {
            Err(EncodingError::Custom(
                "range direction readiness marker is malformed".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_is_byte_frozen_and_rejects_partial_or_unknown_values() {
        assert_eq!(RangeCatalogReady.encode().as_ref(), b"1");
        assert_eq!(RangeCatalogReady::decode(b"1").unwrap(), RangeCatalogReady);
        for malformed in [b"".as_slice(), b"0", b"11", b"\x01"] {
            assert!(RangeCatalogReady::decode(malformed).is_err());
        }
    }
}
