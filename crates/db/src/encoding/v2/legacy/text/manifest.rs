//! Retired text generation-manifest JSON format.

#[cfg(any(test, feature = "production-coverage"))]
use bytes::Bytes;

use crate::search::text::TextIndexGenerationManifest;

const LEGACY_TEXT_MANIFEST_FORMAT_VERSION: u32 = 2;

#[derive(Debug, thiserror::Error)]
pub(crate) enum LegacyTextManifestError {
    #[error("failed to decode text manifest: {0}")]
    Json(serde_json::Error),
    #[error("unsupported text manifest format version {0}")]
    UnsupportedVersion(u32),
    #[error("text manifest must contain its primary split as the first split")]
    InvalidPrimarySplit,
}

#[cfg(any(test, feature = "production-coverage"))]
pub(crate) fn encode_for_contract(
    manifest: &TextIndexGenerationManifest,
) -> Result<Bytes, LegacyTextManifestError> {
    serde_json::to_vec(manifest)
        .map(Bytes::from)
        .map_err(LegacyTextManifestError::Json)
}

pub(crate) fn decode(data: &[u8]) -> Result<TextIndexGenerationManifest, LegacyTextManifestError> {
    let manifest = serde_json::from_slice::<TextIndexGenerationManifest>(data)
        .map_err(LegacyTextManifestError::Json)?;
    if manifest.format_version != LEGACY_TEXT_MANIFEST_FORMAT_VERSION {
        return Err(LegacyTextManifestError::UnsupportedVersion(
            manifest.format_version,
        ));
    }
    if manifest.splits.is_empty() || manifest.splits.first() != Some(&manifest.split) {
        return Err(LegacyTextManifestError::InvalidPrimarySplit);
    }
    Ok(manifest)
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
    fn manifest_json_and_semantic_validation_are_frozen() {
        let manifest = manifest();
        let encoded = encode_for_contract(&manifest).unwrap();
        assert_eq!(decode(&encoded).unwrap(), manifest);
        assert!(decode(&encoded[..encoded.len() - 1]).is_err());
        assert!(decode(&[encoded.as_ref(), b"x"].concat()).is_err());
        let mut wrong_version = manifest.clone();
        wrong_version.format_version += 1;
        assert!(matches!(
            decode(&serde_json::to_vec(&wrong_version).unwrap()),
            Err(LegacyTextManifestError::UnsupportedVersion(_))
        ));
        let mut missing_primary = manifest;
        missing_primary.splits.clear();
        assert!(matches!(
            decode(&serde_json::to_vec(&missing_primary).unwrap()),
            Err(LegacyTextManifestError::InvalidPrimarySplit)
        ));
    }
}
