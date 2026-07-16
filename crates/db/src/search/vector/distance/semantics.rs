//! Closed descriptor-bindable semantics for current vector distance kernels.
//!
//! A Rust `Distance` type alone is not a stable persisted identity. This module
//! maps only reviewed current kernels to durable metric, codec, score, and norm
//! semantics. Custom kernels remain usable in memory but fail closed when code
//! attempts to create or open a durable generation descriptor.

use std::any::TypeId;

use crate::encoding::v1::values::vector_generation::{
    ActiveMetricKind, ActiveScoreSemantic, ActiveVectorCodec, CosineNormPolicyId,
};
use crate::search::vector::distance::{Cosine, Distance, Euclidean, Manhattan};

/// Complete semantic identity for a current descriptor-bindable distance type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActiveVectorSemantics {
    metric: ActiveMetricKind,
    codec: ActiveVectorCodec,
    score: ActiveScoreSemantic,
    cosine_norm: Option<CosineNormPolicyId>,
}

impl ActiveVectorSemantics {
    /// Return the closed semantic identity for a distance implementation.
    ///
    /// Custom public `Distance` implementations remain usable as in-memory types,
    /// but cannot be bound to durable generation state without a reviewed stable identity.
    pub(crate) fn for_distance<D: Distance>() -> Option<Self> {
        if TypeId::of::<D>() == TypeId::of::<Cosine>() {
            return Some(Self {
                metric: ActiveMetricKind::Cosine,
                codec: ActiveVectorCodec::F32V1,
                score: ActiveScoreSemantic::CosineHalfF32V1,
                cosine_norm: Some(CosineNormPolicyId::RejectZeroScaledL2V1),
            });
        }
        if TypeId::of::<D>() == TypeId::of::<Euclidean>() {
            return Some(Self {
                metric: ActiveMetricKind::Euclidean,
                codec: ActiveVectorCodec::F32V1,
                score: ActiveScoreSemantic::SquaredEuclideanF32V1,
                cosine_norm: None,
            });
        }
        if TypeId::of::<D>() == TypeId::of::<Manhattan>() {
            return Some(Self {
                metric: ActiveMetricKind::Manhattan,
                codec: ActiveVectorCodec::F32V1,
                score: ActiveScoreSemantic::ManhattanF32V1,
                cosine_norm: None,
            });
        }
        None
    }

    /// Return the active metric identity.
    pub(crate) const fn metric(self) -> ActiveMetricKind {
        self.metric
    }

    /// Return the active payload codec identity.
    pub(crate) const fn codec(self) -> ActiveVectorCodec {
        self.codec
    }

    /// Return the exact score semantic.
    pub(crate) const fn score(self) -> ActiveScoreSemantic {
        self.score
    }

    /// Return the metric-specific cosine norm policy, when applicable.
    pub(crate) const fn cosine_norm(self) -> Option<CosineNormPolicyId> {
        self.cosine_norm
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::{Pod, Zeroable};

    use super::*;
    use crate::encoding::v1::values::vector_generation::{
        ActiveScoreSemantic, MetricKind, VectorCodecKind,
    };
    use crate::search::vector::{item::Item, unaligned_vector::UnalignedVector};

    #[derive(Debug, Clone)]
    enum CustomDistance {}

    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, Pod, Zeroable)]
    struct CustomHeader(f32);

    impl Distance for CustomDistance {
        type Header = CustomHeader;
        type VectorCodec = f32;

        fn name() -> &'static str {
            "custom"
        }

        fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {
            CustomHeader(0.0)
        }

        fn distance(_p: &Item<Self>, _q: &Item<Self>) -> f32 {
            0.0
        }

        fn norm_no_header(_v: &UnalignedVector<Self::VectorCodec>) -> f32 {
            0.0
        }
    }

    impl crate::search::vector::distance::sealed::Sealed for CustomDistance {}

    #[test]
    fn current_f32_distances_have_closed_exact_semantics() {
        let cosine = ActiveVectorSemantics::for_distance::<Cosine>().unwrap();
        assert_eq!(cosine.metric().kind(), MetricKind::Cosine);
        assert_eq!(cosine.codec().kind(), VectorCodecKind::F32V1);
        assert_eq!(cosine.score(), ActiveScoreSemantic::CosineHalfF32V1);
        assert_eq!(
            cosine.cosine_norm(),
            Some(CosineNormPolicyId::RejectZeroScaledL2V1)
        );

        let euclidean = ActiveVectorSemantics::for_distance::<Euclidean>().unwrap();
        assert_eq!(euclidean.metric().kind(), MetricKind::Euclidean);
        assert_eq!(
            euclidean.score(),
            ActiveScoreSemantic::SquaredEuclideanF32V1
        );
        assert_eq!(euclidean.cosine_norm(), None);

        let manhattan = ActiveVectorSemantics::for_distance::<Manhattan>().unwrap();
        assert_eq!(manhattan.metric().kind(), MetricKind::Manhattan);
        assert_eq!(manhattan.score(), ActiveScoreSemantic::ManhattanF32V1);
        assert_eq!(manhattan.cosine_norm(), None);
    }

    #[test]
    fn custom_distance_has_no_durable_semantic_binding() {
        assert_eq!(
            ActiveVectorSemantics::for_distance::<CustomDistance>(),
            None
        );
    }
}
