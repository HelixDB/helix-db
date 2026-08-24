//! Byte-compatible codec for the deployed vector metadata DTO.
//!
//! The DTO's rkyv field order and archived shape are an existing persistence
//! boundary, so this module owns serialization without changing the type or its
//! bytes. Callers must validate the decoded DTO before it enters the vector core;
//! canonical V2 index records add semantic binding beside it rather than adding
//! fields here.

use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::encoding::error::EncodingError;
use crate::encoding::NodeId;

/// Deployed vector index configuration wire DTO.
///
/// Field order, primitive types, and derives are frozen for rkyv compatibility.
/// Search code validates this DTO into stronger contracts instead of changing
/// the persisted structure.
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct VectorIndexConfig {
    /// Name of the physical index.
    pub(crate) index_name: String,
    /// Property name indexed by this physical index.
    pub(crate) property_name: String,
    /// Logical vector dimension.
    pub(crate) dimension: usize,
    /// Upper-layer HNSW connection limit.
    pub(crate) m: usize,
    /// Layer-0 HNSW connection limit.
    pub(crate) m0: usize,
    /// HNSW construction beam width.
    pub(crate) ef_construction: usize,
    /// Maximum-layer multiplier.
    pub(crate) ml: f32,
    /// SimHash collision threshold.
    pub(crate) simhash_threshold: usize,
    /// Layer-0 sampling ratio.
    pub(crate) sampling_ratio: f32,
    /// Whether adaptive layer-0 traversal is enabled.
    pub(crate) adaptive_enabled: bool,
    /// Failure probability used by adaptive thresholding.
    pub(crate) adaptive_failure_prob: f32,
}

/// Deployed vector index metadata wire DTO.
///
/// `count` remains advisory and the entry-point fields are validated after
/// decoding. New generation semantics are stored in canonical V2 records
/// rather than appended here.
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct VectorIndexMetadata {
    /// Persisted physical configuration.
    pub(crate) config: VectorIndexConfig,
    /// Entry point used for online HNSW traversal.
    pub(crate) entry_point: Option<NodeId>,
    /// Maximum populated HNSW layer.
    pub(crate) max_layer: u16,
    /// Advisory vector count; not a correctness boundary.
    pub(crate) count: u64,
}

/// Encodes the current vector metadata DTO using its deployed rkyv layout.
pub(crate) fn encode_metadata(metadata: &VectorIndexMetadata) -> AlignedVec<16> {
    rkyv::to_bytes::<rkyv::rancor::Error>(metadata)
        .expect("validated vector metadata must serialize")
}

/// Decodes one current vector metadata value without altering its wire shape.
///
/// Structural decoding is followed by `VectorIndexConfig::validate` at the
/// storage boundary; this function deliberately does not authorize row access.
pub(crate) fn decode_metadata(data: &[u8]) -> Result<VectorIndexMetadata, EncodingError> {
    if data.is_empty() {
        return Err(EncodingError::Custom("Empty metadata data".to_string()));
    }

    const ALIGNMENT: usize = core::mem::align_of::<rkyv::Archived<VectorIndexMetadata>>();
    let mut aligned = AlignedVec::<ALIGNMENT>::new();
    aligned.extend_from_slice(data);
    let archived = rkyv::access::<rkyv::Archived<VectorIndexMetadata>, rkyv::rancor::Error>(
        aligned.as_slice(),
    )
    .map_err(|error| {
        EncodingError::Custom(format!("Failed to access archived metadata: {error}"))
    })?;
    rkyv::deserialize::<VectorIndexMetadata, rkyv::rancor::Error>(archived)
        .map_err(|error| EncodingError::Custom(format!("Failed to deserialize metadata: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata() -> VectorIndexMetadata {
        VectorIndexMetadata {
            config: VectorIndexConfig {
                index_name: "metadata-codec".to_string(),
                property_name: "embedding".to_string(),
                dimension: 3,
                m: 16,
                m0: 32,
                ef_construction: 200,
                ml: 0.5,
                simhash_threshold: 43,
                sampling_ratio: 0.8,
                adaptive_enabled: true,
                adaptive_failure_prob: 0.1,
            },
            entry_point: None,
            max_layer: 0,
            count: 0,
        }
    }

    #[test]
    fn metadata_codec_preserves_the_current_archived_shape() {
        let metadata = metadata();
        let encoded = encode_metadata(&metadata);
        let decoded = decode_metadata(&encoded).unwrap();

        assert_eq!(decoded.config.index_name, "metadata-codec");
        assert_eq!(decoded.config.property_name, "embedding");
        assert_eq!(decoded.config.dimension, 3);
        assert_eq!(encode_metadata(&decoded).as_slice(), encoded.as_slice());
    }

    #[test]
    fn metadata_codec_rejects_empty_and_malformed_values() {
        assert!(decode_metadata(&[]).is_err());
        assert!(decode_metadata(b"malformed").is_err());
    }
}
