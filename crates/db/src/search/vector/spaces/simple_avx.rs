use std::arch::x86_64::*;
use std::ptr::read_unaligned;

use crate::search::vector::dimension::SameDimensionPair;

#[target_feature(enable = "avx")]
unsafe fn hsum256_ps_avx(x: __m256) -> f32 {
    let x128: __m128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
    let x64: __m128 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}

#[target_feature(enable = "avx")]
pub(crate) unsafe fn euclid_similarity_avx(pair: SameDimensionPair<'_>) -> f32 {
    // The pair proves that both pointers cover the same non-zero number of f32 values.
    // Loop bounds keep all pointer arithmetic in range, and the loads accept unaligned data.
    // <https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html#text=_mm256_loadu_ps&ig_expand=4134>

    // SAFETY: The pair proves both pointers cover the same non-zero number of f32 values.
    // The caller guarantees AVX support, the loads accept unaligned data, and the loop bounds
    // keep every pointer operation in range.
    unsafe {
        let n = pair.dimension().get();
        let m = n - (n % 32);
        let mut ptr1 = pair.left().values().as_ptr() as *const f32;
        let mut ptr2 = pair.right().values().as_ptr() as *const f32;
        let mut sum256_1: __m256 = _mm256_setzero_ps();
        let mut sum256_2: __m256 = _mm256_setzero_ps();
        let mut sum256_3: __m256 = _mm256_setzero_ps();
        let mut sum256_4: __m256 = _mm256_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            let sub256_1 = _mm256_sub_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2));
            sum256_1 = _mm256_add_ps(_mm256_mul_ps(sub256_1, sub256_1), sum256_1);

            let sub256_2 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(8)), _mm256_loadu_ps(ptr2.add(8)));
            sum256_2 = _mm256_add_ps(_mm256_mul_ps(sub256_2, sub256_2), sum256_2);

            let sub256_3 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(16)), _mm256_loadu_ps(ptr2.add(16)));
            sum256_3 = _mm256_add_ps(_mm256_mul_ps(sub256_3, sub256_3), sum256_3);

            let sub256_4 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(24)), _mm256_loadu_ps(ptr2.add(24)));
            sum256_4 = _mm256_add_ps(_mm256_mul_ps(sub256_4, sub256_4), sum256_4);

            ptr1 = ptr1.add(32);
            ptr2 = ptr2.add(32);
            i += 32;
        }

        let sum256 = _mm256_add_ps(
            _mm256_add_ps(sum256_1, sum256_2),
            _mm256_add_ps(sum256_3, sum256_4),
        );
        let mut result = hsum256_ps_avx(sum256);
        for i in 0..n - m {
            let a = read_unaligned(ptr1.add(i));
            let b = read_unaligned(ptr2.add(i));
            let d = a - b;
            result += d * d;
        }
        result
    }
}

#[target_feature(enable = "avx")]
pub(crate) unsafe fn dot_similarity_avx(pair: SameDimensionPair<'_>) -> f32 {
    // The pair proves that both pointers cover the same non-zero number of f32 values.
    // Loop bounds keep all pointer arithmetic in range, and the loads accept unaligned data.
    // <https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html#text=_mm256_loadu_ps&ig_expand=4134>

    // SAFETY: The pair proves both pointers cover the same non-zero number of f32 values.
    // The caller guarantees AVX support, the loads accept unaligned data, and the loop bounds
    // keep every pointer operation in range.
    unsafe {
        let n = pair.dimension().get();
        let m = n - (n % 32);
        let mut ptr1 = pair.left().values().as_ptr() as *const f32;
        let mut ptr2 = pair.right().values().as_ptr() as *const f32;
        let mut sum256_1: __m256 = _mm256_setzero_ps();
        let mut sum256_2: __m256 = _mm256_setzero_ps();
        let mut sum256_3: __m256 = _mm256_setzero_ps();
        let mut sum256_4: __m256 = _mm256_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            sum256_1 = _mm256_add_ps(
                _mm256_mul_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2)),
                sum256_1,
            );
            sum256_2 = _mm256_add_ps(
                _mm256_mul_ps(_mm256_loadu_ps(ptr1.add(8)), _mm256_loadu_ps(ptr2.add(8))),
                sum256_2,
            );
            sum256_3 = _mm256_add_ps(
                _mm256_mul_ps(_mm256_loadu_ps(ptr1.add(16)), _mm256_loadu_ps(ptr2.add(16))),
                sum256_3,
            );
            sum256_4 = _mm256_add_ps(
                _mm256_mul_ps(_mm256_loadu_ps(ptr1.add(24)), _mm256_loadu_ps(ptr2.add(24))),
                sum256_4,
            );

            ptr1 = ptr1.add(32);
            ptr2 = ptr2.add(32);
            i += 32;
        }

        let sum256 = _mm256_add_ps(
            _mm256_add_ps(sum256_1, sum256_2),
            _mm256_add_ps(sum256_3, sum256_4),
        );
        let mut result = hsum256_ps_avx(sum256);

        for i in 0..n - m {
            let a = read_unaligned(ptr1.add(i));
            let b = read_unaligned(ptr2.add(i));
            result += a * b;
        }
        result
    }
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub(crate) unsafe fn euclid_similarity_avx_fma(pair: SameDimensionPair<'_>) -> f32 {
    // The pair proves that both pointers cover the same non-zero number of f32 values.
    // Loop bounds keep all pointer arithmetic in range, and the loads accept unaligned data.
    // <https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html#text=_mm256_loadu_ps&ig_expand=4134>

    // SAFETY: The pair proves both pointers cover the same non-zero number of f32 values.
    // The caller guarantees AVX and FMA support, the loads accept unaligned data, and the loop
    // bounds keep every pointer operation in range.
    unsafe {
        let n = pair.dimension().get();
        let m = n - (n % 32);
        let mut ptr1 = pair.left().values().as_ptr() as *const f32;
        let mut ptr2 = pair.right().values().as_ptr() as *const f32;
        let mut sum256_1: __m256 = _mm256_setzero_ps();
        let mut sum256_2: __m256 = _mm256_setzero_ps();
        let mut sum256_3: __m256 = _mm256_setzero_ps();
        let mut sum256_4: __m256 = _mm256_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            let sub256_1 = _mm256_sub_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2));
            sum256_1 = _mm256_fmadd_ps(sub256_1, sub256_1, sum256_1);

            let sub256_2 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(8)), _mm256_loadu_ps(ptr2.add(8)));
            sum256_2 = _mm256_fmadd_ps(sub256_2, sub256_2, sum256_2);

            let sub256_3 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(16)), _mm256_loadu_ps(ptr2.add(16)));
            sum256_3 = _mm256_fmadd_ps(sub256_3, sub256_3, sum256_3);

            let sub256_4 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(24)), _mm256_loadu_ps(ptr2.add(24)));
            sum256_4 = _mm256_fmadd_ps(sub256_4, sub256_4, sum256_4);

            ptr1 = ptr1.add(32);
            ptr2 = ptr2.add(32);
            i += 32;
        }

        let sum256 = _mm256_add_ps(
            _mm256_add_ps(sum256_1, sum256_2),
            _mm256_add_ps(sum256_3, sum256_4),
        );
        let mut result = hsum256_ps_avx(sum256);
        for i in 0..n - m {
            let a = read_unaligned(ptr1.add(i));
            let b = read_unaligned(ptr2.add(i));
            let d = a - b;
            result += d * d;
        }
        result
    }
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub(crate) unsafe fn dot_similarity_avx_fma(pair: SameDimensionPair<'_>) -> f32 {
    // The pair proves that both pointers cover the same non-zero number of f32 values.
    // Loop bounds keep all pointer arithmetic in range, and the loads accept unaligned data.
    // <https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html#text=_mm256_loadu_ps&ig_expand=4134>

    // SAFETY: The pair proves both pointers cover the same non-zero number of f32 values.
    // The caller guarantees AVX and FMA support, the loads accept unaligned data, and the loop
    // bounds keep every pointer operation in range.
    unsafe {
        let n = pair.dimension().get();
        let m = n - (n % 32);
        let mut ptr1 = pair.left().values().as_ptr() as *const f32;
        let mut ptr2 = pair.right().values().as_ptr() as *const f32;
        let mut sum256_1: __m256 = _mm256_setzero_ps();
        let mut sum256_2: __m256 = _mm256_setzero_ps();
        let mut sum256_3: __m256 = _mm256_setzero_ps();
        let mut sum256_4: __m256 = _mm256_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            sum256_1 = _mm256_fmadd_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2), sum256_1);
            sum256_2 = _mm256_fmadd_ps(
                _mm256_loadu_ps(ptr1.add(8)),
                _mm256_loadu_ps(ptr2.add(8)),
                sum256_2,
            );
            sum256_3 = _mm256_fmadd_ps(
                _mm256_loadu_ps(ptr1.add(16)),
                _mm256_loadu_ps(ptr2.add(16)),
                sum256_3,
            );
            sum256_4 = _mm256_fmadd_ps(
                _mm256_loadu_ps(ptr1.add(24)),
                _mm256_loadu_ps(ptr2.add(24)),
                sum256_4,
            );

            ptr1 = ptr1.add(32);
            ptr2 = ptr2.add(32);
            i += 32;
        }

        let sum256 = _mm256_add_ps(
            _mm256_add_ps(sum256_1, sum256_2),
            _mm256_add_ps(sum256_3, sum256_4),
        );
        let mut result = hsum256_ps_avx(sum256);

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
    use crate::search::vector::spaces::simple::{
        dot_product_non_optimized, euclidean_distance_non_optimized,
    };
    use crate::search::vector::unaligned_vector::UnalignedVector;

    #[test]
    fn test_spaces_avx() {
        use super::*;

        if is_x86_feature_detected!("avx") {
            let v1: Vec<f32> = vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ];
            let v2: Vec<f32> = vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                56., 57., 58., 59., 60., 61.,
            ];

            let v1 = UnalignedVector::from_slice(&v1[..]);
            let v2 = UnalignedVector::from_slice(&v2[..]);
            let pair = SameDimensionPair::try_new(&v1, &v2).unwrap();

            let euclid_simd = if is_x86_feature_detected!("fma") {
                unsafe { euclid_similarity_avx_fma(pair) }
            } else {
                unsafe { euclid_similarity_avx(pair) }
            };
            let euclid = euclidean_distance_non_optimized(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let dot_simd = if is_x86_feature_detected!("fma") {
                unsafe { dot_similarity_avx_fma(pair) }
            } else {
                unsafe { dot_similarity_avx(pair) }
            };
            let dot = dot_product_non_optimized(&v1, &v2);
            assert_eq!(dot_simd, dot);

            // let cosine_simd = unsafe { cosine_preprocess_avx(v1.clone()) };
            // let cosine = cosine_preprocess(v1);
            // assert_eq!(cosine_simd, cosine);
        } else {
            println!("avx test skipped");
        }
    }

    /// The fixed vectors above are small integers, so every intermediate is
    /// exactly representable and no kernel here can disagree with the scalar
    /// reference no matter how it rounds. FMA is the sharpest case: the fused
    /// kernels round once per term where the reference rounds twice, so integer
    /// input is precisely where the two cannot be told apart.
    ///
    /// The non-FMA kernels also have no other coverage. The test above picks the
    /// FMA variant whenever the CPU reports FMA, which every x86 part since 2013
    /// does, so `euclid_similarity_avx` and `dot_similarity_avx` are otherwise
    /// never executed. Both pairs run here.
    ///
    /// Seeded identically to the NEON and SSE agreement tests, so a divergence
    /// on one architecture and not another is a real difference in the kernel
    /// rather than a different input.
    #[test]
    fn avx_kernels_agree_with_scalar_on_rounding_sensitive_input() {
        use super::*;
        use crate::search::vector::spaces::kernel_agreement::{
            assert_agrees, dot_scale, TestRng, AGREEMENT_DIMENSIONS,
        };

        if !is_x86_feature_detected!("avx") {
            return;
        }
        let has_fma = is_x86_feature_detected!("fma");

        let mut rng = TestRng(0x2026_0904);
        for dimension in AGREEMENT_DIMENSIONS {
            let left_values = rng.vector(dimension);
            let right_values = rng.vector(dimension);
            let left = UnalignedVector::from_slice(&left_values[..]);
            let right = UnalignedVector::from_slice(&right_values[..]);
            let pair = SameDimensionPair::try_new(&left, &right).unwrap();

            let euclid_scalar = euclidean_distance_non_optimized(&left, &right);
            let dot_scalar = dot_product_non_optimized(&left, &right);
            let scale = dot_scale(&left_values, &right_values);

            assert_agrees(
                unsafe { euclid_similarity_avx(pair) },
                euclid_scalar,
                euclid_scalar,
                "euclidean avx",
                dimension,
            );
            assert_agrees(
                unsafe { dot_similarity_avx(pair) },
                dot_scalar,
                scale,
                "dot avx",
                dimension,
            );

            if has_fma {
                assert_agrees(
                    unsafe { euclid_similarity_avx_fma(pair) },
                    euclid_scalar,
                    euclid_scalar,
                    "euclidean avx+fma",
                    dimension,
                );
                assert_agrees(
                    unsafe { dot_similarity_avx_fma(pair) },
                    dot_scalar,
                    scale,
                    "dot avx+fma",
                    dimension,
                );
            }
        }
    }
}
