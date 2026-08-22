//! Frozen pre-V2 vector metadata archive.

use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::encoding::error::EncodingError;
use crate::encoding::v2::values::indexes::vector::metadata::{
    VectorIndexConfig, VectorIndexMetadata,
};
use crate::encoding::NodeId;

#[allow(dead_code)]
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct LegacyVectorIndexConfig {
    index_name: String,
    property_name: String,
    dimension: usize,
    m: usize,
    m0: usize,
    ef_construction: usize,
    ml: f32,
    simhash_threshold: usize,
    sampling_ratio: f32,
    adaptive_enabled: bool,
    adaptive_failure_prob: f32,
    reorder_enabled: bool,
    reorder_min_interval_secs: u64,
    reorder_min_queries: u64,
    reorder_min_heat_edges: usize,
    reorder_heat_capacity: usize,
    reorder_prefetch_window: usize,
    reorder_max_nodes_per_pass: usize,
    reorder_max_bytes_per_pass: usize,
    reorder_max_pass_secs: u64,
    reorder_gate_window_secs: u64,
    reorder_max_read_ops_per_sec: f64,
    reorder_max_write_ops_per_sec: f64,
    reorder_max_total_mem_bytes: u64,
    reorder_max_wal_buffer_bytes: u64,
    reorder_max_vector_l0_ssts: u64,
    reorder_abort_on_pressure: bool,
    reorder_cooldown_secs: u64,
    reorder_lease_enabled: bool,
    reorder_lease_ttl_secs: u64,
    reorder_kill_switch: bool,
    reorder_drain_mode: bool,
    reorder_lease_margin_secs: u64,
    reorder_tick_enabled: bool,
    reorder_tick_interval_secs: u64,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct LegacyVectorIndexMetadata {
    config: LegacyVectorIndexConfig,
    entry_point: Option<NodeId>,
    max_layer: u16,
    count: u64,
}

/// Decodes pre-V2 production metadata and drops only retired reorder controls.
pub(crate) fn decode_legacy_metadata(data: &[u8]) -> Result<VectorIndexMetadata, EncodingError> {
    if data.is_empty() {
        return Err(EncodingError::Custom("Empty metadata data".to_string()));
    }
    const ALIGNMENT: usize = core::mem::align_of::<rkyv::Archived<LegacyVectorIndexMetadata>>();
    let mut aligned = AlignedVec::<ALIGNMENT>::new();
    aligned.extend_from_slice(data);
    let archived = rkyv::access::<rkyv::Archived<LegacyVectorIndexMetadata>, rkyv::rancor::Error>(
        aligned.as_slice(),
    )
    .map_err(|error| {
        EncodingError::Custom(format!(
            "Failed to access archived legacy metadata: {error}"
        ))
    })?;
    let legacy = rkyv::deserialize::<LegacyVectorIndexMetadata, rkyv::rancor::Error>(archived)
        .map_err(|error| {
            EncodingError::Custom(format!(
                "Failed to deserialize legacy vector metadata: {error}"
            ))
        })?;
    Ok(VectorIndexMetadata {
        config: VectorIndexConfig {
            index_name: legacy.config.index_name,
            property_name: legacy.config.property_name,
            dimension: legacy.config.dimension,
            m: legacy.config.m,
            m0: legacy.config.m0,
            ef_construction: legacy.config.ef_construction,
            ml: legacy.config.ml,
            simhash_threshold: legacy.config.simhash_threshold,
            sampling_ratio: legacy.config.sampling_ratio,
            adaptive_enabled: legacy.config.adaptive_enabled,
            adaptive_failure_prob: legacy.config.adaptive_failure_prob,
        },
        entry_point: legacy.entry_point,
        max_layer: legacy.max_layer,
        count: legacy.count,
    })
}

/// Encodes the frozen pre-V2 metadata shape for migration fixtures.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) fn encode_legacy_metadata_for_contract(
    metadata: &VectorIndexMetadata,
) -> AlignedVec<16> {
    let config = &metadata.config;
    rkyv::to_bytes::<rkyv::rancor::Error>(&LegacyVectorIndexMetadata {
        config: LegacyVectorIndexConfig {
            index_name: config.index_name.clone(),
            property_name: config.property_name.clone(),
            dimension: config.dimension,
            m: config.m,
            m0: config.m0,
            ef_construction: config.ef_construction,
            ml: config.ml,
            simhash_threshold: config.simhash_threshold,
            sampling_ratio: config.sampling_ratio,
            adaptive_enabled: config.adaptive_enabled,
            adaptive_failure_prob: config.adaptive_failure_prob,
            reorder_enabled: false,
            reorder_min_interval_secs: 0,
            reorder_min_queries: 0,
            reorder_min_heat_edges: 0,
            reorder_heat_capacity: 0,
            reorder_prefetch_window: 0,
            reorder_max_nodes_per_pass: 0,
            reorder_max_bytes_per_pass: 0,
            reorder_max_pass_secs: 0,
            reorder_gate_window_secs: 0,
            reorder_max_read_ops_per_sec: 0.0,
            reorder_max_write_ops_per_sec: 0.0,
            reorder_max_total_mem_bytes: 0,
            reorder_max_wal_buffer_bytes: 0,
            reorder_max_vector_l0_ssts: 0,
            reorder_abort_on_pressure: false,
            reorder_cooldown_secs: 0,
            reorder_lease_enabled: false,
            reorder_lease_ttl_secs: 0,
            reorder_kill_switch: false,
            reorder_drain_mode: false,
            reorder_lease_margin_secs: 0,
            reorder_tick_enabled: false,
            reorder_tick_interval_secs: 0,
        },
        entry_point: metadata.entry_point,
        max_layer: metadata.max_layer,
        count: metadata.count,
    })
    .expect("production legacy metadata fixture has a bounded archived shape")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_metadata_projects_only_the_live_physical_contract() {
        let current = VectorIndexMetadata {
            config: VectorIndexConfig {
                index_name: "legacy-metadata".to_string(),
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
            entry_point: Some(42),
            max_layer: 2,
            count: 9,
        };
        let encoded = encode_legacy_metadata_for_contract(&current);
        let decoded = decode_legacy_metadata(&encoded).unwrap();
        assert_eq!(decoded.config.index_name, "legacy-metadata");
        assert_eq!(decoded.config.m0, 32);
        assert_eq!(decoded.entry_point, Some(42));
        assert_eq!(decoded.count, 9);
        assert!(decode_legacy_metadata(&[]).is_err());
        assert!(decode_legacy_metadata(b"malformed").is_err());
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(decode_legacy_metadata(&trailing).is_err());
    }
}
