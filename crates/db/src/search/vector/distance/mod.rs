//! Distance-kernel contracts for current and reserved vector codecs.
//!
//! The active durable descriptor surface binds only the reviewed `f32` cosine,
//! Euclidean, and Manhattan implementations. Additional codec-specific kernels
//! remain feature-gated until their persistence and semantic contracts are
//! introduced deliberately.

use std::fmt;

use bytemuck::{Pod, Zeroable};

use crate::search::vector::{
    item::Item,
    unaligned_vector::{UnalignedVector, UnalignedVectorCodec},
};

#[cfg(any(test, feature = "future-vector-codecs"))]
mod binary_quantized_cosine;
#[cfg(any(test, feature = "future-vector-codecs"))]
mod binary_quantized_euclidean;
#[cfg(any(test, feature = "future-vector-codecs"))]
mod binary_quantized_manhattan;
pub mod cosine;
pub mod euclidean;
#[cfg(any(test, feature = "future-vector-codecs"))]
mod hamming;
pub mod manhattan;
mod semantics;

// Re-export main types
#[cfg(feature = "future-vector-codecs")]
pub use binary_quantized_cosine::BinaryQuantizedCosine;
#[cfg(feature = "future-vector-codecs")]
pub use binary_quantized_euclidean::BinaryQuantizedEuclidean;
#[cfg(feature = "future-vector-codecs")]
pub use binary_quantized_manhattan::BinaryQuantizedManhattan;
pub use cosine::Cosine;
pub use euclidean::Euclidean;
#[cfg(feature = "future-vector-codecs")]
pub use hamming::Hamming;
pub use manhattan::Manhattan;
pub(crate) use semantics::ActiveVectorSemantics;

/// Prevents external distance implementations from bypassing the durable
/// metric, score, and numeric-safety contracts.
pub(crate) mod sealed {
    /// Marker implemented only for kernels reviewed by this crate.
    pub trait Sealed {}
}

impl sealed::Sealed for Cosine {}
impl sealed::Sealed for Euclidean {}
impl sealed::Sealed for Manhattan {}
#[cfg(any(test, feature = "future-vector-codecs"))]
impl sealed::Sealed for binary_quantized_cosine::BinaryQuantizedCosine {}
#[cfg(any(test, feature = "future-vector-codecs"))]
impl sealed::Sealed for binary_quantized_euclidean::BinaryQuantizedEuclidean {}
#[cfg(any(test, feature = "future-vector-codecs"))]
impl sealed::Sealed for binary_quantized_manhattan::BinaryQuantizedManhattan {}
#[cfg(any(test, feature = "future-vector-codecs"))]
impl sealed::Sealed for hamming::Hamming {}

/// A reviewed vector distance kernel with stable durable semantics.
///
/// This trait is sealed: callers may select a supported kernel but cannot add
/// one without also defining and reviewing its persisted metric, score, codec,
/// and numeric-safety contracts inside this crate.
#[allow(private_bounds)]
pub trait Distance: sealed::Sealed + Send + Sync + Sized + Clone + fmt::Debug + 'static {
    /// A header structure with informations related to the
    type Header: Pod + Zeroable + fmt::Debug + Send + Sync;
    type VectorCodec: UnalignedVectorCodec;

    /// The name of the distance.
    ///
    /// Note that the name is used to identify the distance and will help some performance improvements.
    /// For example, the "cosine" distance is matched against the "binary quantized cosine" to avoid
    /// recomputing links when moving from the former to the latter distance.
    fn name() -> &'static str;

    fn new_header(vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header;

    /// Returns a non-normalized distance.
    fn distance(p: &Item<Self>, q: &Item<Self>) -> f32;

    fn norm(item: &Item<Self>) -> f32 {
        Self::norm_no_header(&item.vector)
    }

    fn norm_no_header(v: &UnalignedVector<Self::VectorCodec>) -> f32;
}

#[cfg(test)]
mod tests {
    use super::{
        binary_quantized_cosine::BinaryQuantizedCosine,
        binary_quantized_euclidean::BinaryQuantizedEuclidean,
        binary_quantized_manhattan::BinaryQuantizedManhattan,
        hamming::{hamming_bitwise_fast, Hamming},
        *,
    };
    use crate::search::vector::item::Item;

    fn assert_f32_eq(actual: f32, expected: f32) {
        assert!(
            (actual - expected).abs() <= f32::EPSILON,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn float_distances_cover_headers_names_norms_and_invalid_zero_cosine() {
        assert_eq!(Cosine::name(), "cosine");
        let cosine_left = Item::<Cosine>::new(vec![1.0, 0.0]);
        let cosine_right = Item::<Cosine>::new(vec![0.0, 1.0]);
        let cosine_zero = Item::<Cosine>::new(vec![0.0, 0.0]);
        assert!(format!("{:?}", cosine_left.header).contains("norm"));
        assert_f32_eq(Cosine::distance(&cosine_left, &cosine_right), 0.5);
        assert!(Cosine::distance(&cosine_left, &cosine_zero).is_nan());
        assert_f32_eq(Cosine::norm(&cosine_left), 1.0);

        assert_eq!(Euclidean::name(), "euclidean");
        let euclidean_left = Item::<Euclidean>::new(vec![1.0, 2.0]);
        let euclidean_right = Item::<Euclidean>::new(vec![4.0, 6.0]);
        let euclidean_norm = Item::<Euclidean>::new(vec![3.0, 4.0]);
        assert!(format!("{:?}", euclidean_left.header).contains("bias"));
        assert_f32_eq(Euclidean::distance(&euclidean_left, &euclidean_right), 25.0);
        assert_f32_eq(Euclidean::norm(&euclidean_norm), 5.0);

        assert_eq!(Manhattan::name(), "manhattan");
        let manhattan_left = Item::<Manhattan>::new(vec![1.0, -2.0, 3.0]);
        let manhattan_right = Item::<Manhattan>::new(vec![-1.0, 2.0, 1.0]);
        let manhattan_norm = Item::<Manhattan>::new(vec![3.0, 4.0]);
        assert!(format!("{:?}", manhattan_left.header).contains("bias"));
        assert_f32_eq(Manhattan::distance(&manhattan_left, &manhattan_right), 8.0);
        assert_f32_eq(Manhattan::norm(&manhattan_norm), 5.0);
    }

    #[test]
    fn direct_float_items_reject_unequal_dimensions_before_dispatch() {
        let cosine_left = Item::<Cosine>::new(vec![1.0; 16]);
        let cosine_right = Item::<Cosine>::new(vec![1.0; 15]);
        assert!(
            std::panic::catch_unwind(|| Cosine::distance(&cosine_left, &cosine_right)).is_err()
        );

        let euclidean_left = Item::<Euclidean>::new(vec![1.0; 32]);
        let euclidean_right = Item::<Euclidean>::new(vec![1.0; 33]);
        assert!(std::panic::catch_unwind(|| {
            Euclidean::distance(&euclidean_left, &euclidean_right)
        })
        .is_err());

        let manhattan_left = Item::<Manhattan>::new(Vec::new());
        let manhattan_right = Item::<Manhattan>::new(vec![1.0]);
        assert!(std::panic::catch_unwind(|| {
            Manhattan::distance(&manhattan_left, &manhattan_right)
        })
        .is_err());
    }

    #[test]
    fn hamming_distance_covers_word_and_remainder_paths() {
        assert_eq!(Hamming::name(), "hamming");
        assert_f32_eq(hamming_bitwise_fast(&[0xff; 8], &[0x00; 8]), 64.0);
        assert_f32_eq(hamming_bitwise_fast(&[0xff; 10], &[0x00; 10]), 80.0);

        let left = Item::<Hamming>::new(vec![1.0; 64]);
        let mut right_values = vec![1.0; 64];
        right_values[0] = -1.0;
        right_values[63] = -1.0;
        let right = Item::<Hamming>::new(right_values);
        assert!(format!("{:?}", left.header).contains("idx"));
        assert_f32_eq(Hamming::distance(&left, &right), 2.0 / 64.0);
        assert_f32_eq(Hamming::norm(&left), 64.0);
    }

    #[test]
    fn binary_quantized_distances_cover_headers_names_norms_and_zero_cosine() {
        let positive_values = vec![1.0; 64];
        let mut left_values = vec![1.0; 64];
        let mut right_values = vec![1.0; 64];
        left_values
            .iter_mut()
            .enumerate()
            .filter(|(idx, _)| idx.is_multiple_of(2))
            .for_each(|(_, value)| *value = -1.0);
        right_values.clone_from(&left_values);
        right_values[1] = -1.0;
        right_values[2] = 1.0;

        assert_eq!(BinaryQuantizedCosine::name(), "binary quantized cosine");
        let cosine_left = Item::<BinaryQuantizedCosine>::new(left_values.clone());
        let cosine_right = Item::<BinaryQuantizedCosine>::new(right_values.clone());
        let cosine_zero = Item::<BinaryQuantizedCosine>::new(vec![0.0; 64]);
        assert!(format!("{:?}", cosine_left.header).contains("norm"));
        assert_f32_eq(
            BinaryQuantizedCosine::distance(&cosine_left, &cosine_right),
            2.0 / 64.0,
        );
        assert_f32_eq(
            BinaryQuantizedCosine::distance(&cosine_zero, &cosine_zero),
            0.0,
        );
        assert_f32_eq(BinaryQuantizedCosine::norm(&cosine_left), 8.0);

        assert_eq!(
            BinaryQuantizedEuclidean::name(),
            "binary quantized euclidean"
        );
        let euclidean_left = Item::<BinaryQuantizedEuclidean>::new(left_values.clone());
        let euclidean_right = Item::<BinaryQuantizedEuclidean>::new(right_values.clone());
        assert!(format!("{:?}", euclidean_left.header).contains("bias"));
        assert_f32_eq(
            BinaryQuantizedEuclidean::distance(&euclidean_left, &euclidean_right),
            8.0,
        );
        assert_f32_eq(BinaryQuantizedEuclidean::norm(&euclidean_left), 8.0);

        assert_eq!(
            BinaryQuantizedManhattan::name(),
            "binary quantized manhattan"
        );
        let manhattan_left = Item::<BinaryQuantizedManhattan>::new(left_values);
        let manhattan_right = Item::<BinaryQuantizedManhattan>::new(right_values);
        let manhattan_norm = Item::<BinaryQuantizedManhattan>::new(positive_values);
        assert!(format!("{:?}", manhattan_left.header).contains("bias"));
        assert_f32_eq(
            BinaryQuantizedManhattan::distance(&manhattan_left, &manhattan_right),
            4.0,
        );
        assert_f32_eq(BinaryQuantizedManhattan::norm(&manhattan_norm), 8.0);
    }
}
