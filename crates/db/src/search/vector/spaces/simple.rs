//! Checked active-f32 scalar and architecture-dispatched arithmetic.
//!
//! Every entrypoint first proves equal non-zero dimensions, then selects the
//! best available kernel for the current architecture. Scalar reference paths
//! remain available for equivalence tests. The `force-vector-scalar-kernel`
//! feature selects that reference path without disabling mandatory CPU features
//! for unrelated dependencies. Reserved binary-quantized arithmetic stays
//! behind its explicit future-codec feature.

#[cfg(any(test, feature = "future-vector-codecs"))]
use crate::search::vector::unaligned_vector::BinaryQuantized;
use crate::search::vector::{dimension::SameDimensionPair, unaligned_vector::UnalignedVector};
#[cfg(any(test, feature = "future-vector-codecs"))]
use std::ptr::read_unaligned;
use std::sync::OnceLock;

#[cfg(target_arch = "x86_64")]
use super::simple_avx::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use super::simple_neon::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use super::simple_sse::*;

#[cfg(target_arch = "x86_64")]
const MIN_DIM_SIZE_AVX: usize = 32;

#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon")
))]
const MIN_DIM_SIZE_SIMD: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FloatSimd {
    Scalar,
    #[cfg(target_arch = "x86_64")]
    Avx,
    #[cfg(target_arch = "x86_64")]
    AvxFma,
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    Sse,
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    Neon,
}

fn detect_float_simd() -> FloatSimd {
    #[cfg(feature = "force-vector-scalar-kernel")]
    {
        FloatSimd::Scalar
    }

    #[cfg(not(feature = "force-vector-scalar-kernel"))]
    {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx") {
                if is_x86_feature_detected!("fma") {
                    return FloatSimd::AvxFma;
                }
                return FloatSimd::Avx;
            }

            if is_x86_feature_detected!("sse") {
                return FloatSimd::Sse;
            }
        }

        #[cfg(target_arch = "x86")]
        {
            if is_x86_feature_detected!("sse") {
                return FloatSimd::Sse;
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                return FloatSimd::Neon;
            }
        }

        FloatSimd::Scalar
    }
}

#[inline(always)]
fn float_simd() -> FloatSimd {
    static SIMD: OnceLock<FloatSimd> = OnceLock::new();
    *SIMD.get_or_init(detect_float_simd)
}

/// Return whether production coverage explicitly selected the scalar kernel.
#[cfg(all(
    feature = "production-coverage",
    feature = "force-vector-scalar-kernel"
))]
pub(crate) fn is_forced_scalar_kernel() -> bool {
    float_simd() == FloatSimd::Scalar
}

#[inline(always)]
pub fn euclidean_distance(u: &UnalignedVector<f32>, v: &UnalignedVector<f32>) -> f32 {
    let pair = SameDimensionPair::try_new(u, v)
        .unwrap_or_else(|error| panic!("euclidean distance input is invalid: {error}"));
    let len = pair.dimension().get();
    match float_simd() {
        #[cfg(target_arch = "x86_64")]
        FloatSimd::AvxFma if len >= MIN_DIM_SIZE_AVX => unsafe { euclid_similarity_avx_fma(pair) },
        #[cfg(target_arch = "x86_64")]
        FloatSimd::Avx if len >= MIN_DIM_SIZE_AVX => unsafe { euclid_similarity_avx(pair) },
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        FloatSimd::Sse if len >= MIN_DIM_SIZE_SIMD => unsafe { euclid_similarity_sse(pair) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        FloatSimd::Neon if len >= MIN_DIM_SIZE_SIMD => unsafe { euclid_similarity_neon(pair) },
        _ => euclidean_distance_scalar(pair),
    }
}

// Don't use dot-product: avoid catastrophic cancellation in
// https://github.com/spotify/annoy/issues/314.
pub fn euclidean_distance_non_optimized(u: &UnalignedVector<f32>, v: &UnalignedVector<f32>) -> f32 {
    let pair = SameDimensionPair::try_new(u, v)
        .unwrap_or_else(|error| panic!("euclidean distance input is invalid: {error}"));
    euclidean_distance_scalar(pair)
}

#[inline(always)]
pub fn dot_product(u: &UnalignedVector<f32>, v: &UnalignedVector<f32>) -> f32 {
    let pair = SameDimensionPair::try_new(u, v)
        .unwrap_or_else(|error| panic!("dot product input is invalid: {error}"));
    let len = pair.dimension().get();
    match float_simd() {
        #[cfg(target_arch = "x86_64")]
        FloatSimd::AvxFma if len >= MIN_DIM_SIZE_AVX => unsafe { dot_similarity_avx_fma(pair) },
        #[cfg(target_arch = "x86_64")]
        FloatSimd::Avx if len >= MIN_DIM_SIZE_AVX => unsafe { dot_similarity_avx(pair) },
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        FloatSimd::Sse if len >= MIN_DIM_SIZE_SIMD => unsafe { dot_similarity_sse(pair) },
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        FloatSimd::Neon if len >= MIN_DIM_SIZE_SIMD => unsafe { dot_similarity_neon(pair) },
        _ => dot_product_scalar(pair),
    }
}
#[inline(always)]
pub fn dot_product_non_optimized(u: &UnalignedVector<f32>, v: &UnalignedVector<f32>) -> f32 {
    let pair = SameDimensionPair::try_new(u, v)
        .unwrap_or_else(|error| panic!("dot product input is invalid: {error}"));
    dot_product_scalar(pair)
}

/// Return the Manhattan distance after proving both inputs have one non-zero dimension.
pub(crate) fn manhattan_distance(u: &UnalignedVector<f32>, v: &UnalignedVector<f32>) -> f32 {
    let pair = SameDimensionPair::try_new(u, v)
        .unwrap_or_else(|error| panic!("Manhattan distance input is invalid: {error}"));
    let mut left = pair.left().values().iter();
    let mut right = pair.right().values().iter();
    let mut distance = 0.0;
    for _ in 0..pair.dimension().get() {
        let left = left
            .next()
            .expect("checked left vector has exact dimension");
        let right = right
            .next()
            .expect("checked right vector has exact dimension");
        distance += (left - right).abs();
    }
    distance
}

fn euclidean_distance_scalar(pair: SameDimensionPair<'_>) -> f32 {
    let mut left = pair.left().values().iter();
    let mut right = pair.right().values().iter();
    let mut distance = 0.0;
    for _ in 0..pair.dimension().get() {
        let left = left
            .next()
            .expect("checked left vector has exact dimension");
        let right = right
            .next()
            .expect("checked right vector has exact dimension");
        distance += (left - right) * (left - right);
    }
    distance
}

fn dot_product_scalar(pair: SameDimensionPair<'_>) -> f32 {
    let mut left = pair.left().values().iter();
    let mut right = pair.right().values().iter();
    let mut product = 0.0;
    for _ in 0..pair.dimension().get() {
        let left = left
            .next()
            .expect("checked left vector has exact dimension");
        let right = right
            .next()
            .expect("checked right vector has exact dimension");
        product += left * right;
    }
    product
}

/// For the binary quantized dot product:
/// 1. We need to multiply two scalars, in our case the only allowed values are -1 and 1:
/// ```text
/// -1 * -1 =  1
/// -1 *  1 = -1
///  1 * -1 = -1
///  1 *  1 =  1
/// ```
///
/// This looks like a negative xor already, if we replace the -1 by the binary quantized 0, and the 1 stays 1s:
/// ```text
/// ! 0 ^ 0 = 1
/// ! 0 ^ 1 = 0
/// ! 1 ^ 0 = 0
/// ! 1 ^ 1 = 1
/// ```
/// Is equivalent to `!(a ^ b)`.
///
/// 2. Then we need to do the sum of the results:
///    2.1 First we must do the sum of the operation on the `Word`s
///    /!\ We must be careful here because `1 - 0` actually translates to `1 - 1 = 0`.
///    `word.count_ones() - word.count_zeroes()` should do it:
/// ```text
///  00 => -2
///  01 => 0
///  10 => 0
///  11 => 2
/// ```
///  /!\ We must also take care to use signed integer to be able to go into negatives
///
/// 2.2 Finally we must sum the result of all the words
///     - By taking care of not overflowing: The biggest vectors contains like 5000 dimensions, a i16 could be enough. A i32 should be perfect.
///     - We can do the sum straight away without any more tricks
///     - We can cast the result to an f32 as expected
#[inline(always)]
#[cfg(any(test, feature = "future-vector-codecs"))]
pub fn dot_product_binary_quantized(
    u: &UnalignedVector<BinaryQuantized>,
    v: &UnalignedVector<BinaryQuantized>,
) -> f32 {
    const WORD_BYTES: usize = std::mem::size_of::<u64>();
    const WORD_BITS: i32 = u64::BITS as i32;

    let u_bytes = u.as_bytes();
    let v_bytes = v.as_bytes();
    assert_eq!(
        u_bytes.len(),
        v_bytes.len(),
        "binary dot product requires equal vector dimensions"
    );

    let mut sum = 0i32;
    let mut i = 0;
    let word_count = u_bytes.len() / WORD_BYTES;
    unsafe {
        let u_ptr = u_bytes.as_ptr() as *const u64;
        let v_ptr = v_bytes.as_ptr() as *const u64;
        while i < word_count {
            let equal_bits = !(read_unaligned(u_ptr.add(i)) ^ read_unaligned(v_ptr.add(i)));
            sum += ((equal_bits.count_ones() as i32) << 1) - WORD_BITS;
            i += 1;
        }
    }

    for (&u, &v) in u_bytes[word_count * WORD_BYTES..]
        .iter()
        .zip(&v_bytes[word_count * WORD_BYTES..])
    {
        let equal_bits = !(u ^ v);
        sum += equal_bits.count_ones() as i32 - equal_bits.count_zeros() as i32;
    }

    sum as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dot_product_binary_quantized_matches_byte_reference() {
        let u_bytes = vec![
            0b_0000_0000,
            0b_1111_0000,
            0b_1010_1010,
            0b_0101_0101,
            0b_0000_1111,
            0b_1100_1100,
            0b_0011_0011,
            0b_1111_1111,
        ];
        let v_bytes = vec![
            0b_1111_1111,
            0b_1111_0000,
            0b_0101_0101,
            0b_0101_0101,
            0b_1111_0000,
            0b_0011_0011,
            0b_1100_1100,
            0b_0000_0000,
        ];

        let u = UnalignedVector::<BinaryQuantized>::from_bytes(&u_bytes).unwrap();
        let v = UnalignedVector::<BinaryQuantized>::from_bytes(&v_bytes).unwrap();

        let expected = u_bytes
            .iter()
            .zip(&v_bytes)
            .map(|(u, v)| {
                let equal_bits = !(u ^ v);
                equal_bits.count_ones() as i32 - equal_bits.count_zeros() as i32
            })
            .sum::<i32>() as f32;

        assert_eq!(dot_product_binary_quantized(&u, &v), expected);
    }

    #[test]
    fn dot_product_binary_quantized_handles_mismatched_lengths() {
        let mismatched_u = UnalignedVector::<BinaryQuantized>::from_bytes(&[0x00; 8]).unwrap();
        let mismatched_v = UnalignedVector::<BinaryQuantized>::from_bytes(&[0x00; 16]).unwrap();
        let mismatch =
            std::panic::catch_unwind(|| dot_product_binary_quantized(&mismatched_u, &mismatched_v));
        assert!(mismatch.is_err());
    }

    #[test]
    fn float_distance_dispatch_rejects_mismatches_around_simd_widths() {
        for left_len in [0, 1, 15, 16, 17, 31, 32, 33] {
            for right_len in [0, 1, 15, 16, 17, 31, 32, 33] {
                if left_len == right_len {
                    continue;
                }

                let left = UnalignedVector::<f32>::from_vec(vec![1.0; left_len]);
                let right = UnalignedVector::<f32>::from_vec(vec![1.0; right_len]);
                assert!(std::panic::catch_unwind(|| euclidean_distance(&left, &right)).is_err());
                assert!(std::panic::catch_unwind(|| dot_product(&left, &right)).is_err());
                assert!(std::panic::catch_unwind(|| {
                    euclidean_distance_non_optimized(&left, &right)
                })
                .is_err());
                assert!(
                    std::panic::catch_unwind(|| dot_product_non_optimized(&left, &right)).is_err()
                );
                assert!(std::panic::catch_unwind(|| manhattan_distance(&left, &right)).is_err());
            }
        }
    }
}
