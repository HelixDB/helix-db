#[cfg(target_arch = "x86")]
use std::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use std::ptr::read_unaligned;

use crate::helix_engine::vector_core::unaligned_vector::UnalignedVector;

#[target_feature(enable = "sse")]
unsafe fn hsum128_ps_sse(x: __m128) -> f32 {
    let x64: __m128 = _mm_add_ps(x, _mm_movehl_ps(x, x));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}

#[target_feature(enable = "sse")]
pub(crate) unsafe fn euclid_similarity_sse(
    v1: &UnalignedVector<f32>,
    v2: &UnalignedVector<f32>,
) -> f32 {
    unsafe {
        // It is safe to load unaligned floats from a pointer.
        // <https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html#text=_mm_loadu_ps&ig_expand=4131>

        let n = v1.len();
        let m = n - (n % 16);
        let mut ptr1 = v1.as_ptr() as *const f32;
        let mut ptr2 = v2.as_ptr() as *const f32;
        let mut sum128_1: __m128 = _mm_setzero_ps();
        let mut sum128_2: __m128 = _mm_setzero_ps();
        let mut sum128_3: __m128 = _mm_setzero_ps();
        let mut sum128_4: __m128 = _mm_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            let sub128_1 = _mm_sub_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2));
            sum128_1 = _mm_add_ps(_mm_mul_ps(sub128_1, sub128_1), sum128_1);

            let sub128_2 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4)));
            sum128_2 = _mm_add_ps(_mm_mul_ps(sub128_2, sub128_2), sum128_2);

            let sub128_3 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8)));
            sum128_3 = _mm_add_ps(_mm_mul_ps(sub128_3, sub128_3), sum128_3);

            let sub128_4 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12)));
            sum128_4 = _mm_add_ps(_mm_mul_ps(sub128_4, sub128_4), sum128_4);

            ptr1 = ptr1.add(16);
            ptr2 = ptr2.add(16);
            i += 16;
        }

        let mut result = hsum128_ps_sse(sum128_1)
            + hsum128_ps_sse(sum128_2)
            + hsum128_ps_sse(sum128_3)
            + hsum128_ps_sse(sum128_4);
        for i in 0..n - m {
            let a = read_unaligned(ptr1.add(i));
            let b = read_unaligned(ptr2.add(i));
            result += (a - b).powi(2);
        }
        result
    }
}

#[target_feature(enable = "sse")]
pub(crate) unsafe fn dot_similarity_sse(
    v1: &UnalignedVector<f32>,
    v2: &UnalignedVector<f32>,
) -> f32 {
    unsafe {
        // It is safe to load unaligned floats from a pointer.
        // <https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html#text=_mm_loadu_ps&ig_expand=4131>

        let n = v1.len();
        let m = n - (n % 16);
        let mut ptr1 = v1.as_ptr() as *const f32;
        let mut ptr2 = v2.as_ptr() as *const f32;
        let mut sum128_1: __m128 = _mm_setzero_ps();
        let mut sum128_2: __m128 = _mm_setzero_ps();
        let mut sum128_3: __m128 = _mm_setzero_ps();
        let mut sum128_4: __m128 = _mm_setzero_ps();

        let mut i: usize = 0;
        while i < m {
            sum128_1 = _mm_add_ps(_mm_mul_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2)), sum128_1);

            sum128_2 = _mm_add_ps(
                _mm_mul_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4))),
                sum128_2,
            );

            sum128_3 = _mm_add_ps(
                _mm_mul_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8))),
                sum128_3,
            );

            sum128_4 = _mm_add_ps(
                _mm_mul_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12))),
                sum128_4,
            );

            ptr1 = ptr1.add(16);
            ptr2 = ptr2.add(16);
            i += 16;
        }

        let mut result = hsum128_ps_sse(sum128_1)
            + hsum128_ps_sse(sum128_2)
            + hsum128_ps_sse(sum128_3)
            + hsum128_ps_sse(sum128_4);
        for i in 0..n - m {
            let a = read_unaligned(ptr1.add(i));
            let b = read_unaligned(ptr2.add(i));
            result += a * b;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_spaces_sse() {
        use super::*;
        use crate::helix_engine::vector_core::spaces::simple::{
            dot_product_non_optimized, euclidean_distance_non_optimized,
        };

        if is_x86_feature_detected!("sse") {
            let v1: Vec<f32> = vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ];
            let v2: Vec<f32> = vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                56., 57., 58., 59., 60., 61.,
            ];

            let v1 = UnalignedVector::from_slice(&v1[..]);
            let v2 = UnalignedVector::from_slice(&v2[..]);

            let euclid_simd = unsafe { euclid_similarity_sse(&v1, &v2) };
            let euclid = euclidean_distance_non_optimized(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let dot_simd = unsafe { dot_similarity_sse(&v1, &v2) };
            let dot = dot_product_non_optimized(&v1, &v2);
            assert_eq!(dot_simd, dot);

            // let cosine_simd = unsafe { cosine_preprocess_sse(v1.clone()) };
            // let cosine = cosine_preprocess(v1);
            // assert_eq!(cosine_simd, cosine);
        } else {
            println!("sse test skipped");
        }
    }
}
