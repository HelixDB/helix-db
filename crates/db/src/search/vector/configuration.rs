//! Validation boundary for byte-compatible persisted vector configuration and metadata DTOs.
//!
//! The existing rkyv DTOs remain unchanged for on-disk compatibility. Creation
//! and reopen paths call [`VectorIndexConfig::validate`] and
//! [`VectorIndexMetadata::validated_state`] before primitive or parallel fields
//! enter the vector core. The resulting algebraic types prevent contradictory
//! runtime states without changing serialized bytes.

use std::num::NonZeroUsize;

use super::{
    CollisionThreshold, Connections, ConstructionBeamWidth, FailureProbability, Layer0Connections,
    LayerMultiplier, UnitInterval, VectorDimension, VectorDimensionError, VectorIndexConfig,
    VectorIndexMetadata, VectorParameterError,
};
use crate::search::vector::SIMHASH_BITS;

impl VectorIndexConfig {
    /// Projects a validated V2 definition without legacy runtime clamping.
    ///
    /// Every numeric conversion is infallible because the canonical definition
    /// has already checked the frozen integer domains and semantic ranges.
    /// This constructs the unchanged deployed metadata DTO; no V2 field is
    /// appended to its persisted bytes.
    pub(crate) fn from_v2_definition(
        definition: &crate::index_v2::ValidatedVectorIndexDefinition,
        index_name: impl Into<String>,
    ) -> Self {
        Self {
            index_name: index_name.into(),
            property_name: definition.property().as_str().to_string(),
            dimension: definition.dimension() as usize,
            m: definition.m() as usize,
            m0: definition.m0() as usize,
            ef_construction: definition.ef_construction() as usize,
            ml: definition.ml(),
            simhash_threshold: definition.simhash_threshold() as usize,
            sampling_ratio: definition.sampling_ratio(),
            adaptive_enabled: definition.adaptive_enabled(),
            adaptive_failure_prob: definition.adaptive_failure_probability(),
        }
    }

    /// Compares every persisted field in the unchanged physical metadata contract.
    ///
    /// Lifecycle activation and serving both use this comparison so a physical
    /// namespace cannot be opened under a definition that merely shares its
    /// dimension or display name.
    pub(crate) fn has_same_physical_contract(&self, expected: &Self) -> bool {
        self.index_name == expected.index_name
            && self.property_name == expected.property_name
            && self.dimension == expected.dimension
            && self.m == expected.m
            && self.m0 == expected.m0
            && self.ef_construction == expected.ef_construction
            && self.ml.to_bits() == expected.ml.to_bits()
            && self.simhash_threshold == expected.simhash_threshold
            && self.sampling_ratio.to_bits() == expected.sampling_ratio.to_bits()
            && self.adaptive_enabled == expected.adaptive_enabled
            && self.adaptive_failure_prob.to_bits() == expected.adaptive_failure_prob.to_bits()
    }

    /// Validate every field before this byte-compatible DTO crosses into the vector core.
    ///
    /// This does not alter the persisted representation.
    ///
    /// Unit contracts in this module exercise valid and malformed deployed DTOs;
    /// the raw type is deliberately unavailable outside the crate.
    pub(crate) fn validate(&self) -> Result<(), VectorConfigError> {
        if self.index_name.trim().is_empty() {
            return Err(VectorConfigError::EmptyIndexName);
        }
        if self.property_name.trim().is_empty() {
            return Err(VectorConfigError::EmptyPropertyName);
        }

        VectorDimension::try_new(self.dimension)?;
        let connections = Connections::try_new(self.m)?;
        connections.checked_double()?;
        Layer0Connections::try_new(self.m0, connections)?;
        ConstructionBeamWidth::try_new(self.ef_construction, connections)?;
        LayerMultiplier::try_new(self.ml)?;
        CollisionThreshold::try_new(
            self.simhash_threshold,
            NonZeroUsize::new(SIMHASH_BITS).expect("SimHash bit width is nonzero"),
        )?;
        UnitInterval::try_new(self.sampling_ratio)?;
        FailureProbability::try_new(self.adaptive_failure_prob)?;
        Ok(())
    }
}

/// Validated runtime projection of the unchanged persisted metadata fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorIndexState {
    /// No entry point exists and no upper layer is populated.
    Empty,
    /// Searchable graph state with an entry point and maximum populated layer.
    Populated {
        /// Entry point used to begin HNSW traversal.
        entry_point: u64,
        /// Maximum populated HNSW layer.
        max_layer: u16,
    },
}

impl VectorIndexMetadata {
    /// Validates the wire DTO and returns its unambiguous runtime state.
    ///
    /// The rkyv fields and bytes remain unchanged. Call this immediately after
    /// decoding and before using `entry_point` or `max_layer`.
    pub(crate) fn validated_state(&self) -> Result<VectorIndexState, VectorConfigError> {
        self.config.validate()?;
        match (self.entry_point, self.max_layer) {
            (None, 0) => Ok(VectorIndexState::Empty),
            (Some(entry_point), max_layer) => Ok(VectorIndexState::Populated {
                entry_point,
                max_layer,
            }),
            (None, max_layer) => {
                Err(VectorConfigError::MissingEntryPointForPopulatedLayer { max_layer })
            }
        }
    }
}

/// Invalid vector index configuration decoded from or destined for the current DTO.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum VectorConfigError {
    /// Index identity is empty or whitespace-only.
    #[error("vector index name must not be empty")]
    EmptyIndexName,
    /// Indexed property identity is empty or whitespace-only.
    #[error("vector property name must not be empty")]
    EmptyPropertyName,
    /// Dimension validation failed.
    #[error(transparent)]
    Dimension(#[from] VectorDimensionError),
    /// Numeric parameter validation failed.
    #[error(transparent)]
    Parameter(#[from] VectorParameterError),
    /// Metadata claims populated upper layers without an entry point.
    #[error("vector metadata has max layer {max_layer} but no entry point")]
    MissingEntryPointForPopulatedLayer {
        /// Contradictory maximum layer decoded from metadata.
        max_layer: u16,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> VectorIndexConfig {
        VectorIndexConfig::new("items", "embedding", 128)
    }

    #[test]
    fn v2_projection_preserves_validated_fields_without_runtime_defaults() {
        let runtime = crate::config::VectorIndexDefinition::new_edge(
            "Passage",
            "semantic_vector",
            96,
            super::super::VectorDistanceMetric::Manhattan,
        )
        .unwrap()
        .with_m(11)
        .unwrap()
        .with_m0(23)
        .unwrap()
        .with_ef_construction(77)
        .unwrap()
        .with_ml(0.33)
        .unwrap()
        .with_simhash_threshold(47)
        .unwrap()
        .with_sampling_ratio(0.55)
        .unwrap()
        .with_adaptive_enabled(false)
        .with_adaptive_failure_prob(0.25)
        .unwrap();
        let definition =
            crate::index_v2::ValidatedVectorIndexDefinition::try_from_runtime(&runtime).unwrap();
        let config = VectorIndexConfig::from_v2_definition(&definition, "v2-physical-17");

        assert_eq!(config.index_name, "v2-physical-17");
        assert_eq!(config.property_name, definition.property().as_str());
        assert_eq!(config.dimension, definition.dimension() as usize);
        assert_eq!(config.m, definition.m() as usize);
        assert_eq!(config.m0, definition.m0() as usize);
        assert_eq!(
            config.ef_construction,
            definition.ef_construction() as usize
        );
        assert_eq!(config.ml.to_bits(), definition.ml().to_bits());
        assert_eq!(
            config.simhash_threshold,
            definition.simhash_threshold() as usize
        );
        assert_eq!(
            config.sampling_ratio.to_bits(),
            definition.sampling_ratio().to_bits()
        );
        assert_eq!(config.adaptive_enabled, definition.adaptive_enabled());
        assert_eq!(
            config.adaptive_failure_prob.to_bits(),
            definition.adaptive_failure_probability().to_bits()
        );
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn valid_current_defaults_cross_the_boundary_unchanged() {
        assert_eq!(valid_config().validate(), Ok(()));
        assert_eq!(
            VectorIndexMetadata::new(valid_config()).validated_state(),
            Ok(VectorIndexState::Empty)
        );
    }

    #[test]
    fn metadata_state_rejects_parallel_field_contradictions() {
        let mut metadata = VectorIndexMetadata::new(valid_config());
        metadata.max_layer = 2;
        assert_eq!(
            metadata.validated_state(),
            Err(VectorConfigError::MissingEntryPointForPopulatedLayer { max_layer: 2 })
        );

        metadata.entry_point = Some(7);
        assert_eq!(
            metadata.validated_state(),
            Ok(VectorIndexState::Populated {
                entry_point: 7,
                max_layer: 2,
            })
        );
    }

    #[test]
    fn identity_and_dimension_errors_are_typed() {
        let mut config = valid_config();
        config.index_name = "  ".to_string();
        assert_eq!(config.validate(), Err(VectorConfigError::EmptyIndexName));

        config.index_name = "items".to_string();
        config.property_name.clear();
        assert_eq!(config.validate(), Err(VectorConfigError::EmptyPropertyName));

        config.property_name = "embedding".to_string();
        config.dimension = 0;
        assert_eq!(
            config.validate(),
            Err(VectorConfigError::Dimension(
                VectorDimensionError::ZeroDimension
            ))
        );
    }

    #[test]
    fn dependent_hnsw_constraints_are_rejected_instead_of_clamped() {
        let mut config = valid_config();
        config.m = usize::MAX;
        config.m0 = usize::MAX;
        config.ef_construction = usize::MAX;
        assert!(matches!(
            config.validate(),
            Err(VectorConfigError::Parameter(
                VectorParameterError::ArithmeticOverflow {
                    parameter: "layer-0 connections"
                }
            ))
        ));

        config.m = 0;
        config.m0 = 32;
        config.ef_construction = 200;
        assert!(matches!(
            config.validate(),
            Err(VectorConfigError::Parameter(VectorParameterError::Zero {
                parameter: "connections"
            }))
        ));

        config.m = 16;
        config.m0 = 15;
        assert!(matches!(
            config.validate(),
            Err(VectorConfigError::Parameter(
                VectorParameterError::BelowMinimum {
                    parameter: "layer-0 connections",
                    minimum: 16,
                    actual: 15
                }
            ))
        ));

        config.m0 = 32;
        config.ef_construction = 15;
        assert!(matches!(
            config.validate(),
            Err(VectorConfigError::Parameter(
                VectorParameterError::BelowMinimum {
                    parameter: "construction beam width",
                    minimum: 16,
                    actual: 15
                }
            ))
        ));
    }

    #[test]
    fn floating_and_threshold_constraints_reject_invalid_values() {
        let mut config = valid_config();
        config.ml = f32::NAN;
        assert!(config.validate().is_err());

        config.ml = 0.5;
        config.simhash_threshold = SIMHASH_BITS + 1;
        assert!(config.validate().is_err());

        config.simhash_threshold = SIMHASH_BITS;
        config.sampling_ratio = f32::INFINITY;
        assert!(config.validate().is_err());

        config.sampling_ratio = 0.5;
        config.adaptive_failure_prob = 1.0;
        assert!(config.validate().is_err());
    }
}
