//! Byte-compatible codecs for current text manifest and version-counter values.
//!
//! These functions centralize the deployed JSON representations without
//! adding an envelope, version byte, lifecycle field, or migration. The V2
//! lifecycle may reference these rows but never changes their representation.

use std::num::NonZeroU64;

use bytes::Bytes;

use crate::search::text::{TextIndexGenerationManifest, TextIndexLiveState};

/// Encodes the unchanged current text-manifest JSON representation.
pub(crate) fn encode_manifest(
    manifest: &TextIndexGenerationManifest,
) -> Result<Bytes, TextIndexValueError> {
    serde_json::to_vec(manifest)
        .map(Bytes::from)
        .map_err(TextIndexValueError::Manifest)
}

/// Decodes the unchanged current text-manifest JSON representation.
pub(crate) fn decode_manifest(
    data: &[u8],
) -> Result<TextIndexGenerationManifest, TextIndexValueError> {
    serde_json::from_slice(data).map_err(TextIndexValueError::Manifest)
}

/// Encodes the unchanged positive text version-counter JSON number.
pub(crate) fn encode_version_counter(version: NonZeroU64) -> Result<Bytes, TextIndexValueError> {
    serde_json::to_vec(&version.get())
        .map(Bytes::from)
        .map_err(TextIndexValueError::VersionCounter)
}

/// Decodes a positive current text version-counter JSON number.
pub(crate) fn decode_version_counter(data: &[u8]) -> Result<NonZeroU64, TextIndexValueError> {
    let version =
        serde_json::from_slice::<u64>(data).map_err(TextIndexValueError::VersionCounter)?;
    NonZeroU64::new(version).ok_or(TextIndexValueError::ZeroVersionCounter)
}

/// Encodes the unchanged current per-entity live-state representation.
pub(crate) fn encode_live_state(state: &TextIndexLiveState) -> Result<Bytes, TextIndexValueError> {
    crate::search::text::encode_live_state_bytes(state)
        .map(Bytes::from)
        .map_err(TextIndexValueError::LiveState)
}

/// Decodes the unchanged current per-entity live-state representation.
pub(crate) fn decode_live_state(data: &[u8]) -> Result<TextIndexLiveState, TextIndexValueError> {
    crate::search::text::decode_live_state_bytes(data).map_err(TextIndexValueError::LiveState)
}

/// Current text value JSON or semantic validation failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TextIndexValueError {
    /// Current manifest JSON could not be encoded or decoded.
    #[error("current text manifest JSON failed: {0}")]
    Manifest(serde_json::Error),
    /// Current version-counter JSON could not be encoded or decoded.
    #[error("current text version counter JSON failed: {0}")]
    VersionCounter(serde_json::Error),
    /// The current counter format cannot represent zero as an active version.
    #[error("current text version counter must be positive")]
    ZeroVersionCounter,
    /// Current live-state bytes could not be encoded or decoded.
    #[error("current text live state failed: {0}")]
    LiveState(crate::error::HelixDbError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TextAnalyzerKind;
    use crate::search::text::{TextBlobRef, TextSplitRef};

    fn manifest() -> TextIndexGenerationManifest {
        TextIndexGenerationManifest::new_split(
            "fts:n:Post:body",
            "generation",
            TextAnalyzerKind::Standard,
            false,
            TextSplitRef {
                blob: TextBlobRef {
                    sha256: [7; 32],
                    size_bytes: 52,
                },
                footer_offset: 10,
                footer_len: 5,
                hotcache_len: 9,
                total_size_bytes: 52,
            },
        )
    }

    #[test]
    fn codecs_match_the_existing_unwrapped_json_bytes() {
        let manifest = manifest();
        let encoded = encode_manifest(&manifest).unwrap();
        assert_eq!(encoded.as_ref(), serde_json::to_vec(&manifest).unwrap());
        assert_eq!(decode_manifest(&encoded).unwrap(), manifest);

        let version = NonZeroU64::new(7).unwrap();
        let encoded = encode_version_counter(version).unwrap();
        assert_eq!(encoded.as_ref(), serde_json::to_vec(&7_u64).unwrap());
        assert_eq!(decode_version_counter(&encoded).unwrap(), version);
        assert!(matches!(
            decode_version_counter(b"0"),
            Err(TextIndexValueError::ZeroVersionCounter)
        ));

        let live_state = TextIndexLiveState::dead(9);
        let encoded = encode_live_state(&live_state).unwrap();
        assert_eq!(
            encoded.as_ref(),
            crate::search::text::encode_live_state_bytes(&live_state).unwrap()
        );
        assert_eq!(decode_live_state(&encoded).unwrap(), live_state);
    }
}
