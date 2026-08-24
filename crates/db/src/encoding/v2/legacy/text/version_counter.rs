//! Retired text version-counter JSON number.

use std::num::NonZeroU64;

#[cfg(any(test, feature = "production-coverage"))]
use bytes::Bytes;

#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum LegacyTextVersionCounterError {
    #[error("legacy text version-counter JSON failed: {0}")]
    Json(serde_json::Error),
    #[error("legacy text version counter must be positive")]
    Zero,
}

#[cfg(any(test, feature = "production-coverage"))]
pub(crate) fn encode_for_contract(
    version: NonZeroU64,
) -> Result<Bytes, LegacyTextVersionCounterError> {
    serde_json::to_vec(&version.get())
        .map(Bytes::from)
        .map_err(LegacyTextVersionCounterError::Json)
}

#[allow(dead_code)]
pub(crate) fn decode(data: &[u8]) -> Result<NonZeroU64, LegacyTextVersionCounterError> {
    let version =
        serde_json::from_slice::<u64>(data).map_err(LegacyTextVersionCounterError::Json)?;
    NonZeroU64::new(version).ok_or(LegacyTextVersionCounterError::Zero)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_json_counter_is_frozen() {
        let version = NonZeroU64::new(7).unwrap();
        let encoded = encode_for_contract(version).unwrap();
        assert_eq!(encoded.as_ref(), b"7");
        assert_eq!(decode(&encoded).unwrap(), version);
        assert!(matches!(
            decode(b"0"),
            Err(LegacyTextVersionCounterError::Zero)
        ));
        assert!(decode(b"7x").is_err());
    }
}
