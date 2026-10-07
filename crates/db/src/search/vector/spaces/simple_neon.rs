#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use crate::search::vector::dimension::SameDimensionPair;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use crate::search::vector::unaligned_vector;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use std::arch::aarch64::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use std::ptr::read_unaligned;

#[inline(always)]
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub(crate) unsafe fn euclid_similarity_neon(pair: SameDimensionPair<'_>) -> f32 {
    // SAFETY: The pair proves both pointers cover the same non-zero number of f32 values.
    // The caller guarantees NEON support. AArch64 permits unaligned loads, and loop bounds
    // keep pointer arithmetic in range.
    unsafe {
        let n = pair.dimension().get();
        let m = n - (n % 16);
        let mut ptr1 = pair.left().values().as_ptr() as *const f32;
        let mut ptr2 = pair.right().values().as_ptr() as *const f32;
        let mut sum1 = vdupq_n_f32(0.);
        let mut sum2 = vdupq_n_f32(0.);
        let mut sum3 = vdupq_n_f32(0.);
        let mut sum4 = vdupq_n_f32(0.);

        let mut i: usize = 0;
        while i < m {
            let sub1 = vsubq_f32(vld1q_f32(ptr1), vld1q_f32(ptr2));
            sum1 = vfmaq_f32(sum1, sub1, sub1);

            let sub2 = vsubq_f32(vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
            sum2 = vfmaq_f32(sum2, sub2, sub2);

            let sub3 = vsubq_f32(vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
            sum3 = vfmaq_f32(sum3, sub3, sub3);

            let sub4 = vsubq_f32(vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
            sum4 = vfmaq_f32(sum4, sub4, sub4);

            ptr1 = ptr1.add(16);
            ptr2 = ptr2.add(16);
            i += 16;
        }
        let sum = vaddq_f32(vaddq_f32(sum1, sum2), vaddq_f32(sum3, sum4));
        let mut result = vaddvq_f32(sum);
        for i in 0..n - m {
            let a: f32 = read_unaligned(ptr1.add(i));
            let b: f32 = read_unaligned(ptr2.add(i));
            let d = a - b;
            result += d * d;
        }
        result
    }
}

#[inline(always)]
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub(crate) unsafe fn dot_similarity_neon(pair: SameDimensionPair<'_>) -> f32 {
    // SAFETY: The pair proves both pointers cover the same non-zero number of f32 values.
    // The caller guarantees NEON support. AArch64 permits unaligned loads, and loop bounds
    // keep pointer arithmetic in range.
    unsafe {
        let n = pair.dimension().get();
        let m = n - (n % 16);
        let mut ptr1 = pair.left().values().as_ptr() as *const f32;
        let mut ptr2 = pair.right().values().as_ptr() as *const f32;
        let mut sum1 = vdupq_n_f32(0.);
        let mut sum2 = vdupq_n_f32(0.);
        let mut sum3 = vdupq_n_f32(0.);
        let mut sum4 = vdupq_n_f32(0.);

        let mut i: usize = 0;
        while i < m {
            sum1 = vfmaq_f32(sum1, vld1q_f32(ptr1), vld1q_f32(ptr2));
            sum2 = vfmaq_f32(sum2, vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
            sum3 = vfmaq_f32(sum3, vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
            sum4 = vfmaq_f32(sum4, vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
            ptr1 = ptr1.add(16);
            ptr2 = ptr2.add(16);
            i += 16;
        }
        let sum = vaddq_f32(vaddq_f32(sum1, sum2), vaddq_f32(sum3, sum4));
        let mut result = vaddvq_f32(sum);
        for i in 0..n - m {
            let a = read_unaligned(ptr1.add(i));
            let b = read_unaligned(ptr2.add(i));
            result += a * b;
        }
        result
    }
}

/// Sum of squares in f64: each f32 lane is widened before an exact square is
/// fused into one of eight f64x2 accumulators, sixteen components per step.
#[inline(always)]
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub(crate) unsafe fn squared_norm_neon(vector: &unaligned_vector::UnalignedVector<f32>) -> f64 {
    // SAFETY: The view holds exactly `n` f32 values. The caller guarantees NEON support.
    // AArch64 permits unaligned loads, and every offset read stays below `n`.
    unsafe {
        let n = vector.len();
        let m = n - (n % 16);
        let ptr = vector.as_ptr() as *const f32;
        let mut sums = [vdupq_n_f64(0.); 8];

        let mut i: usize = 0;
        while i < m {
            for quad in 0..4 {
                let values = vld1q_f32(ptr.add(i + 4 * quad));
                let low = vcvt_f64_f32(vget_low_f32(values));
                let high = vcvt_high_f64_f32(values);
                sums[2 * quad] = vfmaq_f64(sums[2 * quad], low, low);
                sums[2 * quad + 1] = vfmaq_f64(sums[2 * quad + 1], high, high);
            }
            i += 16;
        }
        let sum = vaddq_f64(
            vaddq_f64(vaddq_f64(sums[0], sums[1]), vaddq_f64(sums[2], sums[3])),
            vaddq_f64(vaddq_f64(sums[4], sums[5]), vaddq_f64(sums[6], sums[7])),
        );
        let mut result = vaddvq_f64(sum);
        for i in m..n {
            let value = f64::from(read_unaligned(ptr.add(i)));
            result += value * value;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    #[test]
    fn test_spaces_neon() {
        use super::*;
        use crate::search::vector::spaces::simple::{
            dot_product_non_optimized, euclidean_distance_non_optimized,
        };
        use crate::search::vector::unaligned_vector::UnalignedVector;

        if std::arch::is_aarch64_feature_detected!("neon") {
            let v1: Vec<f32> = vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ];
            let v2: Vec<f32> = vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                56., 57., 58., 59., 60., 61.,
            ];

            let v1 = UnalignedVector::from_slice(&v1[..]);
            let v2 = UnalignedVector::from_slice(&v2[..]);
            let pair = SameDimensionPair::try_new(&v1, &v2).unwrap();

            let euclid_simd = unsafe { euclid_similarity_neon(pair) };
            let euclid = euclidean_distance_non_optimized(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let dot_simd = unsafe { dot_similarity_neon(pair) };
            let dot = dot_product_non_optimized(&v1, &v2);
            assert_eq!(dot_simd, dot);

            // let cosine_simd = unsafe { cosine_preprocess_neon(v1.clone()) };
            // let cosine = cosine_preprocess(v1);
            // assert_eq!(cosine_simd, cosine);
        } else {
            println!("neon test skipped");
        }
    }

    /// The fixed vectors above are small integers, so every intermediate is
    /// exactly representable and the kernel cannot disagree with the scalar
    /// reference no matter how it rounds. This covers the inputs that can.
    #[test]
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    fn neon_kernels_agree_with_scalar_on_rounding_sensitive_input() {
        use super::*;
        use crate::search::vector::spaces::kernel_agreement::{
            assert_agrees, dot_scale, TestRng, AGREEMENT_DIMENSIONS,
        };
        use crate::search::vector::spaces::simple::{
            dot_product_non_optimized, euclidean_distance_non_optimized,
        };
        use crate::search::vector::unaligned_vector::UnalignedVector;

        if std::arch::is_aarch64_feature_detected!("neon") {
            let mut rng = TestRng(0x2026_0904);
            for dimension in AGREEMENT_DIMENSIONS {
                let left_values = rng.vector(dimension);
                let right_values = rng.vector(dimension);
                let left = UnalignedVector::from_slice(&left_values[..]);
                let right = UnalignedVector::from_slice(&right_values[..]);
                let pair = SameDimensionPair::try_new(&left, &right).unwrap();

                let euclid_kernel = unsafe { euclid_similarity_neon(pair) };
                let euclid_scalar = euclidean_distance_non_optimized(&left, &right);
                assert_agrees(
                    euclid_kernel,
                    euclid_scalar,
                    euclid_scalar,
                    "euclidean",
                    dimension,
                );

                let dot_kernel = unsafe { dot_similarity_neon(pair) };
                let dot_scalar = dot_product_non_optimized(&left, &right);
                assert_agrees(
                    dot_kernel,
                    dot_scalar,
                    dot_scale(&left_values, &right_values),
                    "dot",
                    dimension,
                );
            }
        }
    }

    #[test]
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    fn neon_squared_norm_agrees_with_scalar_reference() {
        use crate::search::vector::spaces::kernel_agreement::assert_squared_norm_kernel;

        if std::arch::is_aarch64_feature_detected!("neon") {
            assert_squared_norm_kernel(
                |vector| unsafe { super::squared_norm_neon(vector) },
                "neon squared norm",
            );
        }
    }
}
