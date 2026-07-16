//! Stable vector-generation semantic identities.
//!
//! These IDs are reserved for additive generation descriptors. Defining an ID
//! does not define a physical row codec and does not make that codec active.
//! Descriptor decoding uses the broad stable enums so future IDs remain
//! recognizable; runtime activation converts them into the narrower `Active*`
//! enums and rejects capabilities that are only reserved today.

/// Stable entity kind bound by a vector generation descriptor.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum VectorEntityKind {
    /// Node-backed vectors.
    Node = 1,
    /// Edge-backed vectors.
    Edge = 2,
}

impl VectorEntityKind {
    /// Return the stable numeric V2-record identity.
    #[cfg(test)]
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for VectorEntityKind {
    type Error = VectorSemanticIdentityError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Node),
            2 => Ok(Self::Edge),
            unknown => Err(VectorSemanticIdentityError::UnknownEntityKind(unknown)),
        }
    }
}

/// Stable distance-metric identity bound by a vector generation descriptor.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum MetricKind {
    /// Current cosine metric.
    Cosine = 1,
    /// Current Euclidean metric, whose score semantic is separately identified.
    Euclidean = 2,
    /// Current Manhattan metric.
    Manhattan = 3,
    /// Reserved future Hamming metric for binary vectors.
    Hamming = 4,
}

impl MetricKind {
    /// Return the stable numeric V2-record identity.
    #[cfg(test)]
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for MetricKind {
    type Error = VectorSemanticIdentityError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Cosine),
            2 => Ok(Self::Euclidean),
            3 => Ok(Self::Manhattan),
            4 => Ok(Self::Hamming),
            unknown => Err(VectorSemanticIdentityError::UnknownMetricKind(unknown)),
        }
    }
}

/// Metric proven available for active descriptor-bound reads and writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ActiveMetricKind {
    /// Current cosine metric.
    Cosine,
    /// Current squared-Euclidean ranking metric.
    Euclidean,
    /// Current Manhattan metric.
    Manhattan,
}

impl ActiveMetricKind {
    /// Return the stable metric identity bound by this active capability.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) const fn kind(self) -> MetricKind {
        match self {
            Self::Cosine => MetricKind::Cosine,
            Self::Euclidean => MetricKind::Euclidean,
            Self::Manhattan => MetricKind::Manhattan,
        }
    }
}

impl TryFrom<MetricKind> for ActiveMetricKind {
    type Error = VectorSemanticIdentityError;

    fn try_from(metric: MetricKind) -> Result<Self, Self::Error> {
        match metric {
            MetricKind::Cosine => Ok(Self::Cosine),
            MetricKind::Euclidean => Ok(Self::Euclidean),
            MetricKind::Manhattan => Ok(Self::Manhattan),
            MetricKind::Hamming => Err(VectorSemanticIdentityError::UnsupportedMetric(metric)),
        }
    }
}

/// Stable identity of a vector payload codec.
///
/// Existing vector rows do not contain this ID. Canonical V2 generation
/// records bind current rows to `F32V1` without rewriting them.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum VectorCodecKind {
    /// Current byte-compatible `f32` payload codec.
    F32V1 = 1,
    /// Reserved future `f16` payload codec; no row format exists yet.
    F16V1 = 2,
    /// Reserved future bit-packed binary codec.
    BinaryV1 = 3,
    /// Reserved future sign-quantized binary codec.
    BinaryQuantizedV1 = 4,
}

impl VectorCodecKind {
    /// Return the stable numeric V2-record identity.
    #[cfg(test)]
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for VectorCodecKind {
    type Error = VectorSemanticIdentityError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::F32V1),
            2 => Ok(Self::F16V1),
            3 => Ok(Self::BinaryV1),
            4 => Ok(Self::BinaryQuantizedV1),
            unknown => Err(VectorSemanticIdentityError::UnknownVectorCodecKind(unknown)),
        }
    }
}

/// Codec proven safe for active descriptor-bound reads and writes.
///
/// Reserved codec IDs cannot be converted into this type. Adding an arm is the
/// explicit promotion point after that codec's format and safety review.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ActiveVectorCodec {
    /// Current byte-compatible `f32` payload codec.
    F32V1,
}

impl ActiveVectorCodec {
    /// Return the stable codec ID bound by this active capability.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) const fn kind(self) -> VectorCodecKind {
        match self {
            Self::F32V1 => VectorCodecKind::F32V1,
        }
    }
}

impl TryFrom<VectorCodecKind> for ActiveVectorCodec {
    type Error = VectorSemanticIdentityError;

    fn try_from(codec: VectorCodecKind) -> Result<Self, Self::Error> {
        match codec {
            VectorCodecKind::F32V1 => Ok(Self::F32V1),
            VectorCodecKind::F16V1
            | VectorCodecKind::BinaryV1
            | VectorCodecKind::BinaryQuantizedV1 => {
                Err(VectorSemanticIdentityError::UnsupportedVectorCodec(codec))
            }
        }
    }
}

/// Stable identity of the exact score accumulated and ordered by a generation.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ScoreSemanticId {
    /// Current `(1 - cosine) / 2` f32 score.
    CosineHalfF32V1 = 1,
    /// Current squared-Euclidean f32 score; no square root is applied.
    SquaredEuclideanF32V1 = 2,
    /// Current Manhattan f32 score.
    ManhattanF32V1 = 3,
    /// Reserved normalized Hamming score for the future binary codec.
    HammingNormalizedV1 = 4,
    /// Reserved binary-quantized half-cosine score.
    BinaryQuantizedCosineHalfV1 = 5,
    /// Reserved binary-quantized squared-Euclidean score.
    BinaryQuantizedSquaredEuclideanV1 = 6,
    /// Reserved binary-quantized Manhattan score.
    BinaryQuantizedManhattanV1 = 7,
}

impl ScoreSemanticId {
    /// Return the stable numeric V2-record identity.
    #[cfg(test)]
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for ScoreSemanticId {
    type Error = VectorSemanticIdentityError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::CosineHalfF32V1),
            2 => Ok(Self::SquaredEuclideanF32V1),
            3 => Ok(Self::ManhattanF32V1),
            4 => Ok(Self::HammingNormalizedV1),
            5 => Ok(Self::BinaryQuantizedCosineHalfV1),
            6 => Ok(Self::BinaryQuantizedSquaredEuclideanV1),
            7 => Ok(Self::BinaryQuantizedManhattanV1),
            unknown => Err(VectorSemanticIdentityError::UnknownScoreSemanticId(unknown)),
        }
    }
}

/// Score semantic proven available for current descriptor-bound execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ActiveScoreSemantic {
    /// Current `(1 - cosine) / 2` f32 score.
    CosineHalfF32V1,
    /// Current squared-Euclidean f32 score.
    SquaredEuclideanF32V1,
    /// Current Manhattan f32 score.
    ManhattanF32V1,
}

impl ActiveScoreSemantic {
    /// Returns the stable V2-record identity bound by this active capability.
    #[cfg(test)]
    pub(crate) const fn kind(self) -> ScoreSemanticId {
        match self {
            Self::CosineHalfF32V1 => ScoreSemanticId::CosineHalfF32V1,
            Self::SquaredEuclideanF32V1 => ScoreSemanticId::SquaredEuclideanF32V1,
            Self::ManhattanF32V1 => ScoreSemanticId::ManhattanF32V1,
        }
    }
}

impl TryFrom<ScoreSemanticId> for ActiveScoreSemantic {
    type Error = VectorSemanticIdentityError;

    fn try_from(score: ScoreSemanticId) -> Result<Self, Self::Error> {
        match score {
            ScoreSemanticId::CosineHalfF32V1 => Ok(Self::CosineHalfF32V1),
            ScoreSemanticId::SquaredEuclideanF32V1 => Ok(Self::SquaredEuclideanF32V1),
            ScoreSemanticId::ManhattanF32V1 => Ok(Self::ManhattanF32V1),
            ScoreSemanticId::HammingNormalizedV1
            | ScoreSemanticId::BinaryQuantizedCosineHalfV1
            | ScoreSemanticId::BinaryQuantizedSquaredEuclideanV1
            | ScoreSemanticId::BinaryQuantizedManhattanV1 => {
                Err(VectorSemanticIdentityError::UnsupportedScoreSemantic(score))
            }
        }
    }
}

/// Stable identity of the cosine norm and near-zero policy.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CosineNormPolicyId {
    /// Reject zero norms and use overflow/underflow-safe scaled L2 norms.
    RejectZeroScaledL2V1 = 1,
}

impl CosineNormPolicyId {
    /// Return the stable numeric V2-record identity.
    #[cfg(test)]
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for CosineNormPolicyId {
    type Error = VectorSemanticIdentityError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::RejectZeroScaledL2V1),
            unknown => Err(VectorSemanticIdentityError::UnknownCosineNormPolicyId(
                unknown,
            )),
        }
    }
}

/// Failure to decode or activate a vector semantic identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum VectorSemanticIdentityError {
    /// Numeric entity-kind ID is not assigned by this version.
    #[error("unknown vector entity kind id {0}")]
    UnknownEntityKind(u8),
    /// Numeric metric ID is not assigned by this version.
    #[error("unknown vector metric kind id {0}")]
    UnknownMetricKind(u8),
    /// Metric is known but not available for active generations.
    #[error("vector metric {0:?} is reserved but unsupported for active generations")]
    UnsupportedMetric(MetricKind),
    /// Numeric codec ID is not assigned by this version.
    #[error("unknown vector codec kind id {0}")]
    UnknownVectorCodecKind(u8),
    /// ID is reserved but cannot yet back an active generation.
    #[error("vector codec {0:?} is reserved but unsupported for active generations")]
    UnsupportedVectorCodec(VectorCodecKind),
    /// Numeric score-semantic ID is not assigned by this version.
    #[error("unknown vector score semantic id {0}")]
    UnknownScoreSemanticId(u8),
    /// Score semantic is reserved but cannot back current execution.
    #[error("vector score semantic {0:?} is reserved but unsupported for active generations")]
    UnsupportedScoreSemantic(ScoreSemanticId),
    /// Numeric cosine-norm-policy ID is not assigned by this version.
    #[error("unknown cosine norm policy id {0}")]
    UnknownCosineNormPolicyId(u8),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_stable_identity_roundtrips_exhaustively() {
        for entity_kind in [VectorEntityKind::Node, VectorEntityKind::Edge] {
            assert_eq!(
                VectorEntityKind::try_from(entity_kind.as_u8()).unwrap(),
                entity_kind
            );
        }
        for metric in [
            MetricKind::Cosine,
            MetricKind::Euclidean,
            MetricKind::Manhattan,
            MetricKind::Hamming,
        ] {
            assert_eq!(MetricKind::try_from(metric.as_u8()).unwrap(), metric);
        }
        for codec in [
            VectorCodecKind::F32V1,
            VectorCodecKind::F16V1,
            VectorCodecKind::BinaryV1,
            VectorCodecKind::BinaryQuantizedV1,
        ] {
            assert_eq!(VectorCodecKind::try_from(codec.as_u8()).unwrap(), codec);
        }
        for score in [
            ScoreSemanticId::CosineHalfF32V1,
            ScoreSemanticId::SquaredEuclideanF32V1,
            ScoreSemanticId::ManhattanF32V1,
            ScoreSemanticId::HammingNormalizedV1,
            ScoreSemanticId::BinaryQuantizedCosineHalfV1,
            ScoreSemanticId::BinaryQuantizedSquaredEuclideanV1,
            ScoreSemanticId::BinaryQuantizedManhattanV1,
        ] {
            assert_eq!(ScoreSemanticId::try_from(score.as_u8()).unwrap(), score);
        }
        let norm = CosineNormPolicyId::RejectZeroScaledL2V1;
        assert_eq!(CosineNormPolicyId::try_from(norm.as_u8()).unwrap(), norm);
    }

    #[test]
    fn unassigned_codec_ids_fail_closed() {
        for unknown in [0, 5, u8::MAX] {
            assert_eq!(
                VectorCodecKind::try_from(unknown),
                Err(VectorSemanticIdentityError::UnknownVectorCodecKind(unknown))
            );
        }
    }

    #[test]
    fn every_unassigned_semantic_lane_fails_closed() {
        assert_eq!(
            VectorEntityKind::try_from(0),
            Err(VectorSemanticIdentityError::UnknownEntityKind(0))
        );
        assert_eq!(
            MetricKind::try_from(0),
            Err(VectorSemanticIdentityError::UnknownMetricKind(0))
        );
        assert_eq!(
            ScoreSemanticId::try_from(0),
            Err(VectorSemanticIdentityError::UnknownScoreSemanticId(0))
        );
        assert_eq!(
            CosineNormPolicyId::try_from(0),
            Err(VectorSemanticIdentityError::UnknownCosineNormPolicyId(0))
        );
    }

    #[test]
    fn only_f32_converts_into_an_active_codec() {
        let active = ActiveVectorCodec::try_from(VectorCodecKind::F32V1).unwrap();
        assert_eq!(active.kind(), VectorCodecKind::F32V1);

        for reserved in [
            VectorCodecKind::F16V1,
            VectorCodecKind::BinaryV1,
            VectorCodecKind::BinaryQuantizedV1,
        ] {
            assert_eq!(
                ActiveVectorCodec::try_from(reserved),
                Err(VectorSemanticIdentityError::UnsupportedVectorCodec(
                    reserved
                ))
            );
        }
    }

    #[test]
    fn only_current_metrics_convert_into_active_metrics() {
        for (stable, active) in [
            (MetricKind::Cosine, ActiveMetricKind::Cosine),
            (MetricKind::Euclidean, ActiveMetricKind::Euclidean),
            (MetricKind::Manhattan, ActiveMetricKind::Manhattan),
        ] {
            assert_eq!(ActiveMetricKind::try_from(stable).unwrap(), active);
            assert_eq!(active.kind(), stable);
        }
        assert_eq!(
            ActiveMetricKind::try_from(MetricKind::Hamming),
            Err(VectorSemanticIdentityError::UnsupportedMetric(
                MetricKind::Hamming
            ))
        );
    }

    #[test]
    fn only_current_scores_convert_into_active_semantics() {
        for (stable, active) in [
            (
                ScoreSemanticId::CosineHalfF32V1,
                ActiveScoreSemantic::CosineHalfF32V1,
            ),
            (
                ScoreSemanticId::SquaredEuclideanF32V1,
                ActiveScoreSemantic::SquaredEuclideanF32V1,
            ),
            (
                ScoreSemanticId::ManhattanF32V1,
                ActiveScoreSemantic::ManhattanF32V1,
            ),
        ] {
            assert_eq!(ActiveScoreSemantic::try_from(stable).unwrap(), active);
            assert_eq!(active.kind(), stable);
        }

        for reserved in [
            ScoreSemanticId::HammingNormalizedV1,
            ScoreSemanticId::BinaryQuantizedCosineHalfV1,
            ScoreSemanticId::BinaryQuantizedSquaredEuclideanV1,
            ScoreSemanticId::BinaryQuantizedManhattanV1,
        ] {
            assert_eq!(
                ActiveScoreSemantic::try_from(reserved),
                Err(VectorSemanticIdentityError::UnsupportedScoreSemantic(
                    reserved
                ))
            );
        }
    }
}
