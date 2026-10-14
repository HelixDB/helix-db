//! Architecture-dispatched vector arithmetic kernels.
//!
//! Public distance types enter through checked dimension proofs in [`simple`].
//! Architecture-specific implementations are private dispatch targets and do
//! not define persistence or descriptor identity.

pub mod simple;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub(super) mod simple_sse;

#[cfg(target_arch = "x86_64")]
pub(super) mod simple_avx;

#[cfg(target_arch = "aarch64")]
pub(super) mod simple_neon;

#[cfg(test)]
pub(super) mod kernel_agreement {
    //! Support for checking a SIMD kernel against the scalar reference.
    //!
    //! The SIMD kernels fuse the multiply into the accumulate and carry four
    //! partial sums that are combined at the end. The scalar reference
    //! multiplies separately and accumulates in order. Both are correct, and
    //! they round differently, so agreement between them is bounded rather
    //! than exact. Integer inputs hide this because every intermediate is
    //! exactly representable.

    use super::simple::squared_l2_norm_scalar;
    use crate::search::vector::unaligned_vector::UnalignedVector;

    /// Largest relative gap allowed between a kernel and the scalar reference.
    ///
    /// Measured worst case on aarch64 over 4000 random pairs was 2.0e-6 at
    /// 1536 dimensions. This leaves headroom for wider lanes and different
    /// summation groupings on the x86 kernels while staying far below the
    /// error a genuinely wrong kernel would produce.
    pub const RELATIVE_TOLERANCE: f32 = 1e-4;

    /// Deterministic generator so a failure reproduces exactly.
    pub struct TestRng(pub u64);

    impl TestRng {
        pub fn next_f32(&mut self) -> f32 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            ((self.0 >> 40) as f32 / 8_388_608.0) - 1.0
        }

        pub fn vector(&mut self, dimension: usize) -> Vec<f32> {
            (0..dimension).map(|_| self.next_f32()).collect()
        }
    }

    /// Dimensions covering the main loop and the trailing remainder of every
    /// kernel, including the 32 wide AVX stride.
    ///
    /// The 16 wide kernels see remainders of 1, 14, 15 and 0, and the 32 wide
    /// ones see 1, 15, 17, 30 and 0. 17 and 30 also leave the main loop with
    /// nothing to do on a 32 wide stride, so the remainder path runs alone.
    pub const AGREEMENT_DIMENSIONS: [usize; 7] = [17, 30, 33, 47, 64, 384, 1536];

    /// Assert a kernel matches the scalar reference to within the tolerance.
    ///
    /// `scale` is the magnitude the error is measured against. Euclidean
    /// distance sums squares and never cancels, so it scales against its own
    /// result. Dot products can cancel to near zero, which makes a relative
    /// bound on the result meaningless, so they scale against the summed
    /// magnitude of the products instead.
    pub fn assert_agrees(kernel: f32, scalar: f32, scale: f32, what: &str, dimension: usize) {
        let allowed = RELATIVE_TOLERANCE * scale.abs().max(f32::MIN_POSITIVE);
        let error = (kernel - scalar).abs();
        assert!(
            error <= allowed,
            "{what} at {dimension} dimensions: kernel {kernel}, scalar {scalar}, \
             error {error} exceeds allowed {allowed}"
        );
    }

    /// Check a sum-of-squares kernel against the sequential scalar reference.
    ///
    /// Lengths cover every remainder of the 16 and 32 wide strides, both sides
    /// of each dispatch threshold, and common embedding sizes. Integer
    /// components below 2^10 make every square and partial sum exact
    /// (`4096 * 2^20 < 2^53`), so any grouping must match bit for bit. Otherwise
    /// both sums are within a relative `(n - 1) * 2^-53` of the exact value, so
    /// they may differ by at most `n * f64::EPSILON` of the reference; inputs
    /// span unit embeddings and arbitrary finite binades.
    pub fn assert_squared_norm_kernel(kernel: impl Fn(&UnalignedVector<f32>) -> f64, what: &str) {
        let mut rng = TestRng(0x2026_1007);
        for dimension in (1..=67).chain([128, 255, 256, 257, 768, 1536, 4096]) {
            let integers = (0..dimension)
                .map(|_| (rng.next_f32() * 1024.0).trunc())
                .collect::<Vec<_>>();
            let integers = UnalignedVector::from_slice(&integers);
            assert_eq!(
                kernel(&integers).to_bits(),
                squared_l2_norm_scalar(&integers).to_bits(),
                "{what} integer sum at {dimension} dimensions"
            );

            let wide = (0..dimension)
                .map(|_| {
                    rng.next_f32();
                    let value = f32::from_bits((rng.0 >> 32) as u32);
                    if value.is_finite() {
                        value
                    } else {
                        rng.next_f32()
                    }
                })
                .collect::<Vec<_>>();
            for values in [rng.vector(dimension), wide] {
                let values = UnalignedVector::from_slice(&values);
                let scalar = squared_l2_norm_scalar(&values);
                let error = (kernel(&values) - scalar).abs();
                let allowed = dimension as f64 * f64::EPSILON * scalar;
                assert!(
                    error <= allowed,
                    "{what} at {dimension} dimensions: error {error} exceeds {allowed}"
                );
            }
        }
    }

    /// Conditioning scale for a dot product.
    pub fn dot_scale(left: &[f32], right: &[f32]) -> f32 {
        left.iter()
            .zip(right.iter())
            .map(|(l, r)| (l * r).abs())
            .sum()
    }
}
