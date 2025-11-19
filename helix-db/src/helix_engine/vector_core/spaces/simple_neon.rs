#[cfg(target_feature = "neon")]
use crate::unaligned_vector::UnalignedVector;
use std::arch::aarch64::*;
use std::ptr::read_unaligned;

#[cfg(target_feature = "neon")]
pub(crate) unsafe fn euclid_similarity_neon(
    v1: &UnalignedVector<f32>,
    v2: &UnalignedVector<f32>,
) -> f32 {
    // We use the unaligned_float32x4_t helper function to read f32x4 NEON SIMD types
    // from potentially unaligned memory locations safely.
    // https://github.com/meilisearch/arroy/pull/13

    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1 = v1.as_ptr() as *const f32;
    let mut ptr2 = v2.as_ptr() as *const f32;
    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        let sub1 = vsubq_f32(unaligned_float32x4_t(ptr1), unaligned_float32x4_t(ptr2));
        sum1 = vfmaq_f32(sum1, sub1, sub1);

        let sub2 = vsubq_f32(
            unaligned_float32x4_t(ptr1.add(4)),
            unaligned_float32x4_t(ptr2.add(4)),
        );
        sum2 = vfmaq_f32(sum2, sub2, sub2);

        let sub3 = vsubq_f32(
            unaligned_float32x4_t(ptr1.add(8)),
            unaligned_float32x4_t(ptr2.add(8)),
        );
        sum3 = vfmaq_f32(sum3, sub3, sub3);

        let sub4 = vsubq_f32(
            unaligned_float32x4_t(ptr1.add(12)),
            unaligned_float32x4_t(ptr2.add(12)),
        );
        sum4 = vfmaq_f32(sum4, sub4, sub4);

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }
    let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);
    for i in 0..n - m {
        let a = read_unaligned(ptr1.add(i));
        let b = read_unaligned(ptr2.add(i));
        result += (a - b).powi(2);
    }
    result
}

#[cfg(target_feature = "neon")]
pub(crate) unsafe fn dot_similarity_neon(
    v1: &UnalignedVector<f32>,
    v2: &UnalignedVector<f32>,
) -> f32 {
    // We use the unaligned_float32x4_t helper function to read f32x4 NEON SIMD types
    // from potentially unaligned memory locations safely.
    // https://github.com/meilisearch/arroy/pull/13

    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1 = v1.as_ptr() as *const f32;
    let mut ptr2 = v2.as_ptr() as *const f32;
    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        sum1 = vfmaq_f32(
            sum1,
            unaligned_float32x4_t(ptr1),
            unaligned_float32x4_t(ptr2),
        );
        sum2 = vfmaq_f32(
            sum2,
            unaligned_float32x4_t(ptr1.add(4)),
            unaligned_float32x4_t(ptr2.add(4)),
        );
        sum3 = vfmaq_f32(
            sum3,
            unaligned_float32x4_t(ptr1.add(8)),
            unaligned_float32x4_t(ptr2.add(8)),
        );
        sum4 = vfmaq_f32(
            sum4,
            unaligned_float32x4_t(ptr1.add(12)),
            unaligned_float32x4_t(ptr2.add(12)),
        );
        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }
    let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);
    for i in 0..n - m {
        let a = read_unaligned(ptr1.add(i));
        let b = read_unaligned(ptr2.add(i));
        result += a * b;
    }
    result
}

/// Reads 4xf32 in a stack-located array aligned on a f32 and reads a `float32x4_t` from it.
unsafe fn unaligned_float32x4_t(ptr: *const f32) -> float32x4_t {
    vld1q_f32(read_unaligned(ptr as *const [f32; 4]).as_ptr())
}

#[cfg(test)]
mod tests {
    #[cfg(target_feature = "neon")]
    #[test]
    fn test_spaces_neon() {
        use super::*;
        use crate::spaces::simple::*;

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

            let euclid_simd = unsafe { euclid_similarity_neon(&v1, &v2) };
            let euclid = euclidean_distance_non_optimized(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let dot_simd = unsafe { dot_similarity_neon(&v1, &v2) };
            let dot = dot_product_non_optimized(&v1, &v2);
            assert_eq!(dot_simd, dot);

            // let cosine_simd = unsafe { cosine_preprocess_neon(v1.clone()) };
            // let cosine = cosine_preprocess(v1);
            // assert_eq!(cosine_simd, cosine);
        } else {
            println!("neon test skipped");
        }
    }
}
