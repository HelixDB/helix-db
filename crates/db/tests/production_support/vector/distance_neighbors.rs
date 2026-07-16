//! Production contracts for active distance kernels and canonical neighbors.
//!
//! This feature-gated child module verifies closed f32 metric identity,
//! scalar/architecture-dispatched arithmetic equivalence, dimension rejection,
//! and every constructible bounded-neighbor transition. It never enables a
//! reserved codec and proves canonical runtime state still enters the existing
//! neighbor value encoders unchanged.

use bytemuck::{Pod, Zeroable};

use super::*;
use crate::encoding::v1::values::vector_generation::{
    ActiveScoreSemantic, CosineNormPolicyId, MetricKind, VectorCodecKind,
};

/// Unsupported in-memory kernel used to prove durable semantics fail closed.
#[derive(Debug, Clone)]
enum UnsupportedDistance {}

/// Trivial header required by the unsupported in-memory distance kernel.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
struct UnsupportedHeader(f32);

impl distance::Distance for UnsupportedDistance {
    type Header = UnsupportedHeader;
    type VectorCodec = f32;

    fn name() -> &'static str {
        "unsupported"
    }

    fn new_header(_vector: &unaligned_vector::UnalignedVector<Self::VectorCodec>) -> Self::Header {
        UnsupportedHeader(0.0)
    }

    fn distance(_left: &item::Item<Self>, _right: &item::Item<Self>) -> f32 {
        0.0
    }

    fn norm_no_header(_vector: &unaligned_vector::UnalignedVector<Self::VectorCodec>) -> f32 {
        0.0
    }
}

impl distance::sealed::Sealed for UnsupportedDistance {}

/// Exercises active f32 semantics and every valid bounded-neighbor transition.
pub(crate) fn run() {
    #[cfg(feature = "force-vector-scalar-kernel")]
    assert!(spaces::simple::is_forced_scalar_kernel());

    let cosine = distance::ActiveVectorSemantics::for_distance::<distance::Cosine>().unwrap();
    assert_eq!(cosine.metric().kind(), MetricKind::Cosine);
    assert_eq!(cosine.codec().kind(), VectorCodecKind::F32V1);
    assert_eq!(cosine.score(), ActiveScoreSemantic::CosineHalfF32V1);
    assert_eq!(
        cosine.cosine_norm(),
        Some(CosineNormPolicyId::RejectZeroScaledL2V1)
    );
    let euclidean = distance::ActiveVectorSemantics::for_distance::<distance::Euclidean>().unwrap();
    assert_eq!(euclidean.metric().kind(), MetricKind::Euclidean);
    assert_eq!(euclidean.codec().kind(), VectorCodecKind::F32V1);
    assert_eq!(
        euclidean.score(),
        ActiveScoreSemantic::SquaredEuclideanF32V1
    );
    assert_eq!(euclidean.cosine_norm(), None);
    let manhattan = distance::ActiveVectorSemantics::for_distance::<distance::Manhattan>().unwrap();
    assert_eq!(manhattan.metric().kind(), MetricKind::Manhattan);
    assert_eq!(manhattan.codec().kind(), VectorCodecKind::F32V1);
    assert_eq!(manhattan.score(), ActiveScoreSemantic::ManhattanF32V1);
    assert_eq!(manhattan.cosine_norm(), None);
    assert_eq!(
        distance::ActiveVectorSemantics::for_distance::<UnsupportedDistance>(),
        None
    );

    let cosine_left = item::Item::<distance::Cosine>::new(vec![1.0, 0.0]);
    let cosine_right = item::Item::<distance::Cosine>::new(vec![0.0, 1.0]);
    let cosine_zero = item::Item::<distance::Cosine>::new(vec![0.0, 0.0]);
    assert_eq!(distance::Cosine::name(), "cosine");
    assert_eq!(distance::Cosine::distance(&cosine_left, &cosine_right), 0.5);
    assert!(distance::Cosine::distance(&cosine_left, &cosine_zero).is_nan());
    assert_eq!(distance::Cosine::norm(&cosine_left), 1.0);

    let euclidean_left = item::Item::<distance::Euclidean>::new(vec![1.0, 2.0]);
    let euclidean_right = item::Item::<distance::Euclidean>::new(vec![4.0, 6.0]);
    assert_eq!(distance::Euclidean::name(), "euclidean");
    assert_eq!(
        distance::Euclidean::distance(&euclidean_left, &euclidean_right),
        25.0
    );
    assert_eq!(distance::Euclidean::norm(&euclidean_right), 52.0_f32.sqrt());

    let manhattan_left = item::Item::<distance::Manhattan>::new(vec![1.0, -2.0, 3.0]);
    let manhattan_right = item::Item::<distance::Manhattan>::new(vec![-1.0, 2.0, 1.0]);
    assert_eq!(distance::Manhattan::name(), "manhattan");
    assert_eq!(
        distance::Manhattan::distance(&manhattan_left, &manhattan_right),
        8.0
    );
    assert_eq!(distance::Manhattan::norm(&manhattan_left), 14.0_f32.sqrt());

    let short_left = unaligned_vector::UnalignedVector::<f32>::from_slice(&[1.0, 2.0, 3.0]);
    let short_right = unaligned_vector::UnalignedVector::<f32>::from_slice(&[3.0, 2.0, 1.0]);
    assert_eq!(spaces::simple::dot_product(&short_left, &short_right), 10.0);
    assert_eq!(
        spaces::simple::dot_product_non_optimized(&short_left, &short_right),
        10.0
    );
    assert_eq!(
        spaces::simple::euclidean_distance(&short_left, &short_right),
        8.0
    );
    assert_eq!(
        spaces::simple::euclidean_distance_non_optimized(&short_left, &short_right),
        8.0
    );
    assert_eq!(
        spaces::simple::manhattan_distance(&short_left, &short_right),
        4.0
    );

    let long_left = unaligned_vector::UnalignedVector::<f32>::from_vec(
        (0..64).map(|value| value as f32).collect(),
    );
    let long_right = unaligned_vector::UnalignedVector::<f32>::from_vec(
        (0..64).rev().map(|value| value as f32).collect(),
    );
    let optimized_dot = spaces::simple::dot_product(&long_left, &long_right);
    let scalar_dot = spaces::simple::dot_product_non_optimized(&long_left, &long_right);
    assert!((optimized_dot - scalar_dot).abs() <= scalar_dot.abs() * 1e-5);
    let optimized_euclidean = spaces::simple::euclidean_distance(&long_left, &long_right);
    let scalar_euclidean =
        spaces::simple::euclidean_distance_non_optimized(&long_left, &long_right);
    assert!((optimized_euclidean - scalar_euclidean).abs() <= scalar_euclidean.abs() * 1e-5);

    let long_pair = dimension::SameDimensionPair::try_new(&long_left, &long_right).unwrap();
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        // SAFETY: SSE is part of the x86_64 baseline and is checked at runtime
        // on x86; `long_pair` proves both input ranges have equal length.
        if std::arch::is_x86_feature_detected!("sse") {
            let sse_dot = unsafe { spaces::simple_sse::dot_similarity_sse(long_pair) };
            let sse_euclidean = unsafe { spaces::simple_sse::euclid_similarity_sse(long_pair) };
            assert!((sse_dot - scalar_dot).abs() <= scalar_dot.abs() * 1e-5);
            assert!((sse_euclidean - scalar_euclidean).abs() <= scalar_euclidean.abs() * 1e-5);
        }
    }
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: Each direct kernel call is guarded by its required CPU
        // feature, and `long_pair` proves both input ranges have equal length.
        if std::arch::is_x86_feature_detected!("avx") {
            let avx_dot = unsafe { spaces::simple_avx::dot_similarity_avx(long_pair) };
            let avx_euclidean = unsafe { spaces::simple_avx::euclid_similarity_avx(long_pair) };
            assert!((avx_dot - scalar_dot).abs() <= scalar_dot.abs() * 1e-5);
            assert!((avx_euclidean - scalar_euclidean).abs() <= scalar_euclidean.abs() * 1e-5);
        }
        if std::arch::is_x86_feature_detected!("avx") && std::arch::is_x86_feature_detected!("fma")
        {
            let fma_dot = unsafe { spaces::simple_avx::dot_similarity_avx_fma(long_pair) };
            let fma_euclidean = unsafe { spaces::simple_avx::euclid_similarity_avx_fma(long_pair) };
            assert!((fma_dot - scalar_dot).abs() <= scalar_dot.abs() * 1e-5);
            assert!((fma_euclidean - scalar_euclidean).abs() <= scalar_euclidean.abs() * 1e-5);
        }
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        // SAFETY: AArch64 NEON availability is checked at runtime and
        // `long_pair` proves both input ranges have equal length.
        if std::arch::is_aarch64_feature_detected!("neon") {
            let neon_dot = unsafe { spaces::simple_neon::dot_similarity_neon(long_pair) };
            let neon_euclidean = unsafe { spaces::simple_neon::euclid_similarity_neon(long_pair) };
            assert!((neon_dot - scalar_dot).abs() <= scalar_dot.abs() * 1e-5);
            assert!((neon_euclidean - scalar_euclidean).abs() <= scalar_euclidean.abs() * 1e-5);
        }
    }

    let mismatched = unaligned_vector::UnalignedVector::<f32>::from_slice(&[1.0, 2.0]);
    for rejected in [
        std::panic::catch_unwind(|| spaces::simple::dot_product(&short_left, &mismatched)),
        std::panic::catch_unwind(|| {
            spaces::simple::dot_product_non_optimized(&short_left, &mismatched)
        }),
        std::panic::catch_unwind(|| spaces::simple::euclidean_distance(&short_left, &mismatched)),
        std::panic::catch_unwind(|| {
            spaces::simple::euclidean_distance_non_optimized(&short_left, &mismatched)
        }),
        std::panic::catch_unwind(|| spaces::simple::manhattan_distance(&short_left, &mismatched)),
    ] {
        assert!(rejected.is_err());
    }

    assert_eq!(
        neighbor_set::NeighborDegreeLimit::try_new(0),
        Err(neighbor_set::NeighborSetError::ZeroDegreeLimit)
    );
    assert_eq!(
        neighbor_set::NeighborDegreeLimits::try_new(3, 0),
        Err(neighbor_set::NeighborSetError::ZeroDegreeLimit)
    );
    let limits = neighbor_set::NeighborDegreeLimits::try_new(4, 2).unwrap();
    assert_eq!(limits.for_layer(0).get(), 4);
    assert_eq!(limits.for_layer(1).get(), 2);
    let empty = neighbor_set::NeighborSet::empty(9, limits.for_layer(0));
    assert!(empty.as_slice().is_empty());
    assert!(!empty.contains(1));
    assert!(empty.difference(&empty).unwrap().is_empty());

    assert_eq!(
        neighbor_set::NeighborSet::try_from_canonical(9, limits.for_layer(0), vec![1, 2, 3, 4, 5]),
        Err(neighbor_set::NeighborSetError::DegreeExceeded {
            limit: 4,
            actual: 5
        })
    );
    assert_eq!(
        neighbor_set::NeighborSet::try_from_canonical(9, limits.for_layer(0), vec![1, 9]),
        Err(neighbor_set::NeighborSetError::ContainsOwner(9))
    );
    assert_eq!(
        neighbor_set::NeighborSet::try_from_canonical(9, limits.for_layer(0), vec![1, 1]),
        Err(neighbor_set::NeighborSetError::Duplicate(1))
    );
    assert_eq!(
        neighbor_set::NeighborSet::try_from_canonical(9, limits.for_layer(0), vec![2, 1]),
        Err(neighbor_set::NeighborSetError::Unsorted)
    );

    let old = neighbor_set::NeighborSet::try_from_deployed(9, limits.for_layer(0), vec![5, 3, 1])
        .unwrap();
    assert_eq!(old.as_slice(), [1, 3, 5]);
    assert!(old.contains(3));
    assert_eq!(old.to_vec(), [1, 3, 5]);
    let next = neighbor_set::NeighborSet::try_from_canonical(9, limits.for_layer(0), vec![2, 3, 4])
        .unwrap();
    let (removed, added) = old.difference(&next).unwrap().into_parts();
    assert_eq!(removed, [1, 5]);
    assert_eq!(added, [2, 4]);
    let other_owner = neighbor_set::NeighborSet::empty(10, limits.for_layer(0));
    assert!(matches!(
        old.difference(&other_owner),
        Err(neighbor_set::NeighborSetError::OwnerMismatch {
            expected: 9,
            actual: 10
        })
    ));
    let other_limit =
        neighbor_set::NeighborSet::empty(9, neighbor_set::NeighborDegreeLimit::try_new(3).unwrap());
    assert_eq!(
        old.difference(&other_limit),
        Err(neighbor_set::NeighborSetError::DegreeLimitMismatch)
    );

    let deployed_upper =
        crate::encoding::v1::values::vectors::neighbors::encode_upper_neighbors(next.as_slice())
            .unwrap();
    assert_eq!(
        deployed_upper,
        crate::encoding::v1::values::vectors::neighbors::encode_upper_neighbors(&[2, 3, 4])
            .unwrap()
    );
    let deployed_layer0 =
        crate::encoding::v1::values::vectors::encode_layer0_neighbors(next.as_slice());
    assert_eq!(
        deployed_layer0,
        crate::encoding::v1::values::vectors::encode_layer0_neighbors(&[2, 3, 4])
    );
}
