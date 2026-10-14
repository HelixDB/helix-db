//! Numerically stable cosine distance implementation for validated vectors.

use std::fmt;

use bytemuck::{Pod, Zeroable};

use crate::search::vector::{
    distance::Distance, item::Item, spaces::simple, unaligned_vector::UnalignedVector,
};

/// Relative half-width per component of the interval [`fast_norm`] proves
/// contains the reference norm: `16 * 2^-53 = 2^-49`. Derived in [`fast_norm`].
const NORM_TOLERANCE_PER_COMPONENT: f64 = 8.0 * f64::EPSILON;

/// Computes an L2 norm without overflowing or underflowing the sum of squares.
///
/// This is the reference norm: its rounded value is stored in every cosine row
/// header and re-derived on decode, so [`Cosine::norm_no_header`] must return
/// exactly `scaled_l2_norm(v).min(f64::from(f32::MAX)) as f32` for every input.
fn scaled_l2_norm(vector: &UnalignedVector<f32>) -> f64 {
    let mut scale = 0.0f64;
    let mut scaled_sum = 1.0f64;

    for component in vector.iter() {
        let magnitude = f64::from(component.abs());
        if magnitude == 0.0 {
            continue;
        }
        if scale < magnitude {
            let ratio = scale / magnitude;
            scaled_sum = 1.0 + scaled_sum * ratio * ratio;
            scale = magnitude;
        } else {
            let ratio = magnitude / scale;
            scaled_sum += ratio * ratio;
        }
    }

    if scale == 0.0 {
        0.0
    } else {
        scale * scaled_sum.sqrt()
    }
}

/// Return the reference norm's f32 bits from the vectorised f64 sum of
/// squares, or `None` when the fast result cannot prove which f32 the
/// reference rounds to.
///
/// Let `u = 2^-53`, `n` the component count, `R` the exact norm, and
/// `f(x) = x.min(f32::MAX as f64) as f32`. `f` is monotone non-decreasing
/// (`min` and round-to-nearest both are), so if `f(low)` and `f(high)` have the
/// same bits and the reference value lies in `[low, high]`, the reference
/// rounds to those bits too. This also covers the `f32::MAX` clamp and the
/// subnormal f32 range without special cases.
///
/// Neither computation can overflow or underflow for finite input: f32 squares
/// lie in `[2^-298, 2^256]`, partial sums stay below `n * 2^256`, and the
/// reference's ratios stay at or above `2^-277`, all inside the normal f64
/// range. Every rounding is therefore a relative factor in `[1 - u, 1 + u]`,
/// and because every term is non-negative, per-term factors bound the total:
///
/// * Fast: squares are exact and each term passes through at most `n - 1`
///   additions in any grouping (so every SIMD kernel is covered); `sqrt` halves
///   that and rounds once: `fast = R * (1 + u)^±((n + 1) / 2)`.
/// * Reference: a term costs at most 4 roundings to form (`ratio` twice through
///   the square, `ratio * ratio`, the add) and at most 5 per later component (a
///   rescale's ratio twice, two multiplies and the `1.0 +`; a plain step's add),
///   so the scaled sum carries `(1 + u)^±(5n - 1)`; `sqrt` and the multiply by
///   the exact `scale` give `R * (1 + u)^±((5n + 3) / 2)`.
///
/// So `reference / fast` lies in `[((1 - u) / (1 + u))^k, ((1 + u) / (1 - u))^k]`
/// with `k = 3n + 2 <= 3(n + 1)`, i.e. within `[1 - 2ku, exp(2ku / (1 - u))]`.
/// While `y = 2ku / (1 - u) <= 1`, `exp(y) - 1 <= (e - 1) y`, so the reference
/// is within a relative `10.6 (n + 1) u` of `fast`. The tolerance
/// `T = 16 (n + 1) u` leaves more than `5 (n + 1) u` of slack for the roundings
/// in forming `fast * T` and `fast ± tolerance`. Once `y > 1`, `T > 2.6`,
/// `fast - tolerance` is negative and its f32 carries the sign bit while
/// `high` does not, so the guard rejects; the proof holds for every `n`.
/// In practice `T` is about `2.7e-12` at 1536 dimensions against an f32
/// relative spacing of at least `6e-8`, so rejection is rare.
///
/// Non-finite input yields a non-finite `fast` and is rejected rather than
/// reasoned about. A zero vector gives `fast = tolerance = +0.0`, matching the
/// reference's `+0.0`.
fn fast_norm(vector: &UnalignedVector<f32>) -> Option<f32> {
    let clamp_to_f32 = |norm: f64| norm.min(f64::from(f32::MAX)) as f32;
    let fast = simple::squared_l2_norm(vector).sqrt();
    let tolerance = fast * (vector.len() as f64 + 1.0) * NORM_TOLERANCE_PER_COMPONENT;
    let low = clamp_to_f32(fast - tolerance);
    let high = clamp_to_f32(fast + tolerance);
    (fast.is_finite() && low.to_bits() == high.to_bits()).then_some(low)
}

/// Computes the fallback cosine score in f64 when the f32 fast path is unsafe.
fn stable_half_cosine(p: &UnalignedVector<f32>, q: &UnalignedVector<f32>) -> f32 {
    assert_eq!(
        p.len(),
        q.len(),
        "cosine distance requires equal vector dimensions"
    );

    let p_norm = scaled_l2_norm(p);
    let q_norm = scaled_l2_norm(q);
    if p_norm == 0.0 || q_norm == 0.0 {
        return f32::NAN;
    }

    let dot = p
        .iter()
        .zip(q.iter())
        .map(|(left, right)| f64::from(left) * f64::from(right))
        .sum::<f64>();
    let cosine = (dot / (p_norm * q_norm)).clamp(-1.0, 1.0);
    ((1.0 - cosine) * 0.5) as f32
}

/// The Cosine similarity is a measure of similarity between two
/// non-zero vectors defined in an inner product space. Cosine similarity
/// is the cosine of the angle between the vectors.
#[derive(Debug, Clone)]
pub enum Cosine {}

/// The header of Cosine item nodes.
#[repr(C)]
#[derive(Pod, Zeroable, Clone, Copy)]
pub struct NodeHeaderCosine {
    norm: f32,
}
impl fmt::Debug for NodeHeaderCosine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHeaderCosine")
            .field("norm", &format!("{:.4}", self.norm))
            .finish()
    }
}

impl Distance for Cosine {
    type Header = NodeHeaderCosine;
    type VectorCodec = f32;

    fn name() -> &'static str {
        "cosine"
    }

    fn new_header(vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {
        NodeHeaderCosine {
            norm: Self::norm_no_header(vector),
        }
    }

    #[inline(always)]
    fn distance(p: &Item<Self>, q: &Item<Self>) -> f32 {
        let pn = p.header.norm;
        let qn = q.header.norm;
        let pq = simple::dot_product(&p.vector, &q.vector);
        let pnqn = pn * qn;
        if pn > 0.0
            && qn > 0.0
            && pn != f32::MAX
            && qn != f32::MAX
            && pnqn.is_normal()
            && pq.is_finite()
        {
            let cos = pq / pnqn;
            let cos = cos.clamp(-1.0, 1.0);
            // cos is [-1; 1]
            // cos =  0. -> 0.5
            // cos = -1. -> 1.0
            // cos =  1. -> 0.0
            (1.0 - cos) / 2.0
        } else {
            stable_half_cosine(&p.vector, &q.vector)
        }
    }

    /// Return the stored cosine norm, bit-identical to the reference
    /// `scaled_l2_norm(v).min(f64::from(f32::MAX)) as f32`.
    ///
    /// The vectorised [`fast_norm`] answers almost every vector; the few whose
    /// norm sits within its proven error bound of an f32 rounding boundary fall
    /// back to the reference loop.
    ///
    /// ```
    /// use db::search::vector::distance::{Cosine, Distance};
    /// use db::search::vector::unaligned_vector::UnalignedVector;
    ///
    /// let vector = UnalignedVector::from_slice(&[3.0_f32, 4.0]);
    /// assert_eq!(Cosine::norm_no_header(&vector), 5.0);
    /// let huge = UnalignedVector::from_slice(&[f32::MAX, f32::MAX]);
    /// assert_eq!(Cosine::norm_no_header(&huge), f32::MAX);
    /// ```
    fn norm_no_header(v: &UnalignedVector<Self::VectorCodec>) -> f32 {
        fast_norm(v).unwrap_or_else(|| scaled_l2_norm(v).min(f64::from(f32::MAX)) as f32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::vector::spaces::kernel_agreement::TestRng;
    use proptest::prelude::*;

    /// The pre-vectorisation norm, whose bits every stored header holds.
    fn reference_norm(vector: &UnalignedVector<f32>) -> f32 {
        scaled_l2_norm(vector).min(f64::from(f32::MAX)) as f32
    }

    /// Assert the production norm reproduces the reference bits and report
    /// whether the fast path answered.
    fn assert_reference_bits(values: &[f32]) -> Option<f32> {
        let vector = UnalignedVector::from_slice(values);
        let reference = reference_norm(&vector);
        assert_eq!(
            Cosine::norm_no_header(&vector).to_bits(),
            reference.to_bits(),
            "norm of {values:?}"
        );
        let fast = fast_norm(&vector)?;
        assert_eq!(
            fast.to_bits(),
            reference.to_bits(),
            "fast norm of {values:?}"
        );
        Some(fast)
    }

    /// Integer legs of a right triangle whose hypotenuse `2^24 + 1` is the
    /// midpoint between the adjacent f32 values `2^24` and `2^24 + 2`.
    const MIDPOINT_LEGS: [f32; 2] = [8192.0, 16_777_215.0];

    #[test]
    fn fast_norm_rejects_exact_f32_midpoints_at_every_scale() {
        assert_eq!(
            8192_u64.pow(2) + 16_777_215_u64.pow(2),
            16_777_217_u64.pow(2)
        );
        // Scaling by 2^k keeps the legs exact and the hypotenuse a midpoint from
        // k = -149 (subnormal short leg, hypotenuse in the second normal binade)
        // up to the largest binade (k = 103).
        for exponent in [-149, -140, -126, -100, -50, -24, -1, 0, 1, 24, 50, 100, 103] {
            let scale = 2.0_f64.powi(exponent);
            let legs = MIDPOINT_LEGS.map(|leg| (f64::from(leg) * scale) as f32);
            assert_eq!(
                legs.map(|leg| f64::from(leg) / scale),
                MIDPOINT_LEGS.map(f64::from),
                "legs stay exact at 2^{exponent}"
            );
            assert_eq!(
                assert_reference_bits(&legs),
                None,
                "midpoint at 2^{exponent}"
            );
        }
    }

    #[test]
    fn fast_norm_rejects_inside_its_bound_and_accepts_just_outside() {
        // A third component of c moves the norm about c^2 * 2^-49 relative above
        // the midpoint, against a tolerance of 4 * 2^-49 for three components:
        // c = 1 sits at a quarter of the bound, c = 3 at 2.25 times it.
        let inside = [MIDPOINT_LEGS[0], MIDPOINT_LEGS[1], 1.0];
        assert_eq!(assert_reference_bits(&inside), None);
        let outside = [MIDPOINT_LEGS[0], MIDPOINT_LEGS[1], 3.0];
        assert_eq!(assert_reference_bits(&outside), Some(16_777_218.0));

        // The bound grows with the component count: c = 16 is 64 times outside
        // the three-component bound, yet inside the 1537 * 2^-49 bound once
        // zero padding stretches the same norm over the 1536-wide SIMD path.
        let mut values = vec![0.0; 1536];
        values[..3].copy_from_slice(&[MIDPOINT_LEGS[0], MIDPOINT_LEGS[1], 16.0]);
        assert_eq!(assert_reference_bits(&values[..3]), Some(16_777_218.0));
        assert_eq!(assert_reference_bits(&values), None);
    }

    /// The guard is load-bearing: here the exact norm lies just above the
    /// midpoint `2^24 + 1`, the fast sum sees that and rounds up, yet the
    /// reference loop lands on the midpoint itself and ties to even below.
    /// Without the guard the stored norm would change. Scaling by `2^k` keeps
    /// every operation exact relative to the unscaled case, so the same split
    /// recurs in every normal binade, and zero padding keeps it on the SIMD path.
    #[test]
    fn fast_norm_falls_back_where_the_fast_sum_rounds_away_from_the_reference() {
        let values = [MIDPOINT_LEGS[1], 9.0 / 32.0, MIDPOINT_LEGS[0]];
        for exponent in [-100, -50, -1, 0, 1, 50, 103] {
            let scale = 2.0_f64.powi(exponent);
            let scaled = values.map(|value| (f64::from(value) * scale) as f32);
            let vector = UnalignedVector::from_slice(&scaled);
            let unguarded = simple::squared_l2_norm(&vector).sqrt() as f32;
            assert_eq!(f64::from(unguarded) / scale, 16_777_218.0, "2^{exponent}");
            assert_eq!(
                f64::from(reference_norm(&vector)) / scale,
                16_777_216.0,
                "2^{exponent}"
            );
            assert_eq!(assert_reference_bits(&scaled), None, "2^{exponent}");
        }
        for dimension in [16, 33, 1536] {
            let mut padded = vec![0.0; dimension];
            padded[..3].copy_from_slice(&values);
            assert_eq!(
                assert_reference_bits(&padded),
                None,
                "{dimension} dimensions"
            );
            assert_eq!(
                Cosine::norm_no_header(&UnalignedVector::from_slice(&padded)),
                16_777_216.0
            );
        }
    }

    /// Zero padding leaves both norms unchanged but sends the fast sum through
    /// every SIMD main loop and tail and the reference through its zero skip,
    /// so exact midpoints must still fall back whichever kernel runs.
    #[test]
    fn fast_norm_rejects_zero_padded_midpoints_on_every_kernel_path() {
        for exponent in [-149, 0, 103] {
            let scale = 2.0_f64.powi(exponent);
            let legs = MIDPOINT_LEGS.map(|leg| (f64::from(leg) * scale) as f32);
            for dimension in [16, 17, 31, 32, 33, 63, 64, 65, 768, 1536, 4096] {
                for first in [0, dimension / 2 - 1, dimension - 2] {
                    let mut values = (0..dimension)
                        .map(|index| if index % 2 == 0 { 0.0 } else { -0.0 })
                        .collect::<Vec<f32>>();
                    values[first..first + 2].copy_from_slice(&legs);
                    assert_eq!(
                        assert_reference_bits(&values),
                        None,
                        "midpoint at 2^{exponent}, {dimension} dimensions, legs at {first}"
                    );
                }
            }
        }
    }

    #[test]
    fn fast_norm_rejects_near_midpoints_at_the_subnormal_and_clamp_boundaries() {
        // 6476^2 + 8387714^2 = j^2 + j for j = 8387716, so the norm sits 1 / (8j)
        // subnormal units below the subnormal midpoint j + 1/2.
        let j = 8_387_716_u64;
        assert_eq!(6476_u64.pow(2) + 8_387_714_u64.pow(2), j * j + j);
        let subnormal = [f32::from_bits(6476), f32::from_bits(8_387_714)];
        assert!(subnormal.iter().all(|value| value.is_subnormal()));
        assert_eq!(assert_reference_bits(&subnormal), None);

        // The largest f32 below MAX plus two small components land about 7e16
        // below the midpoint between that value and MAX, well inside the bound.
        let below_max = [
            f32::from_bits(0x7f7f_fffe),
            f32::from_bits(0x797f_fffe),
            f32::from_bits(0x73bf_ffff),
        ];
        assert_eq!(assert_reference_bits(&below_max), None);
    }

    #[test]
    fn fast_norm_answers_clamped_zero_and_empty_vectors() {
        for values in [
            vec![f32::MAX],
            vec![-f32::MAX, f32::MAX],
            vec![f32::MAX; 1536],
        ] {
            assert_eq!(assert_reference_bits(&values), Some(f32::MAX));
        }
        for dimension in [0, 1, 15, 16, 17, 33, 768] {
            for zero in [0.0, -0.0] {
                let values = vec![zero; dimension];
                assert_eq!(
                    assert_reference_bits(&values).map(f32::to_bits),
                    Some(0),
                    "{dimension} components of {zero}"
                );
            }
        }
        assert_eq!(
            assert_reference_bits(&[f32::from_bits(1)]),
            Some(f32::from_bits(1))
        );
    }

    #[test]
    fn fast_norm_defers_non_finite_input_to_the_reference() {
        for values in [
            vec![f32::INFINITY],
            vec![f32::NEG_INFINITY, 1.0],
            vec![f32::NAN],
            vec![1.0, f32::NAN],
            vec![f32::NAN, f32::INFINITY],
            vec![f32::NAN; 40],
        ] {
            assert_eq!(assert_reference_bits(&values), None, "{values:?}");
        }
    }

    /// Seeded sweep over the shapes stored vectors take: embedding-like unit
    /// components, one shared random binade (subnormal through near MAX), fully
    /// arbitrary finite bits, unit vectors with a few wild components, and
    /// sparse unit vectors. Every dimension cycles through every shape.
    #[test]
    fn fast_norm_matches_reference_bits_on_seeded_sweep() {
        let vectors = if cfg!(debug_assertions) {
            4_000
        } else {
            400_000
        };
        let dimensions = (1..=67)
            .chain([128, 255, 256, 257, 768, 1536, 4096])
            .collect::<Vec<_>>();
        let mut rng = TestRng(0x2026_1007);
        let next_bits = |rng: &mut TestRng| {
            rng.next_f32();
            (rng.0 >> 32) as u32
        };
        let random_finite = |rng: &mut TestRng| {
            let value = f32::from_bits(next_bits(rng));
            if value.is_finite() {
                value
            } else {
                f32::from_bits(value.to_bits() ^ (1 << 30))
            }
        };
        let mut fallbacks = 0_usize;
        for index in 0..vectors {
            let dimension = dimensions[index % dimensions.len()];
            let values = match (index / dimensions.len()) % 5 {
                0 => rng.vector(dimension),
                1 => {
                    let binade = next_bits(&mut rng) % 255;
                    (0..dimension)
                        .map(|_| {
                            let bits = next_bits(&mut rng);
                            let exponent = binade.saturating_sub(bits % 4);
                            f32::from_bits((bits & 0x807f_ffff) | (exponent << 23))
                        })
                        .collect()
                }
                2 => (0..dimension).map(|_| random_finite(&mut rng)).collect(),
                3 => {
                    let mut values = rng.vector(dimension);
                    let slot = next_bits(&mut rng) as usize % dimension;
                    values[slot] = random_finite(&mut rng);
                    values
                }
                _ => (0..dimension)
                    .map(|_| match next_bits(&mut rng) % 4 {
                        0 => 0.0,
                        1 => -0.0,
                        _ => rng.next_f32(),
                    })
                    .collect(),
            };
            if assert_reference_bits(&values).is_none() {
                fallbacks += 1;
            }
        }
        assert!(
            fallbacks * 1000 < vectors,
            "{fallbacks} of {vectors} vectors fell back"
        );
    }

    /// Arbitrary finite components, with signed zeros frequent enough to
    /// exercise the reference's zero skip inside non-zero vectors.
    fn finite_component() -> impl Strategy<Value = f32> {
        prop_oneof![
            6 => any::<u32>()
                .prop_map(f32::from_bits)
                .prop_filter("stored components are finite", |value| value.is_finite()),
            1 => Just(0.0_f32),
            1 => Just(-0.0_f32),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]

        #[test]
        fn fast_norm_matches_reference_bits_for_arbitrary_components(
            values in proptest::collection::vec(finite_component(), 1..300)
        ) {
            assert_reference_bits(&values);
        }

        #[test]
        fn fast_norm_matches_reference_bits_within_one_binade(
            binade in 0_u32..=254,
            components in proptest::collection::vec(
                (any::<bool>(), 0_u32..4, 0_u32..(1 << 23)),
                1..300,
            ),
        ) {
            let values = components
                .into_iter()
                .map(|(negative, drop, mantissa)| {
                    let sign = u32::from(negative) << 31;
                    f32::from_bits(sign | (binade.saturating_sub(drop) << 23) | mantissa)
                })
                .collect::<Vec<_>>();
            assert_reference_bits(&values);
        }

        #[test]
        fn fast_norm_matches_reference_bits_near_constructed_midpoints(
            exponent in -149_i32..=103,
            extra in proptest::collection::vec(-64.0_f32..64.0, 0..40),
        ) {
            let scale = 2.0_f64.powi(exponent);
            let values = MIDPOINT_LEGS
                .iter()
                .chain(&extra)
                .map(|value| (f64::from(*value) * scale) as f32)
                .collect::<Vec<_>>();
            assert_reference_bits(&values);
        }
    }

    /// Release measurement: `cargo test --release -p db cosine_norm_throughput
    /// -- --ignored --nocapture`.
    #[test]
    #[ignore = "throughput measurement, run explicitly in release"]
    fn cosine_norm_throughput() {
        use std::{hint::black_box, time::Instant};

        let mut rng = TestRng(0x2026_1007);
        for dimension in [768, 1536] {
            let vectors = (0..1_000)
                .map(|_| UnalignedVector::from_vec(rng.vector(dimension)))
                .collect::<Vec<_>>();
            let rounds = 200;
            let time = |norm: &dyn Fn(&UnalignedVector<f32>) -> f32| {
                let started = Instant::now();
                for _ in 0..rounds {
                    for vector in &vectors {
                        black_box(norm(black_box(vector)));
                    }
                }
                started.elapsed().as_nanos() as f64 / (rounds * vectors.len()) as f64
            };
            let reference = time(&reference_norm);
            let production = time(&Cosine::norm_no_header);
            let sampled = 200_000;
            let fallbacks = (0..sampled)
                .filter(|_| fast_norm(&UnalignedVector::from_vec(rng.vector(dimension))).is_none())
                .count();
            println!(
                "dimension={dimension} reference_ns={reference:.1} production_ns={production:.1} \
                 speedup={:.1}x fallbacks={fallbacks} sampled={sampled}",
                reference / production
            );
        }
    }

    #[test]
    fn scaled_norm_and_distance_remain_finite_at_f32_extremes() {
        let huge = Item::<Cosine>::new(vec![f32::MAX, f32::MAX]);
        let huge_same = Item::<Cosine>::new(vec![f32::MAX, f32::MAX]);
        assert_eq!(Cosine::norm(&huge), f32::MAX);
        assert!(Cosine::distance(&huge, &huge_same) <= f32::EPSILON);

        let tiny = Item::<Cosine>::new(vec![f32::from_bits(1), f32::from_bits(1)]);
        let tiny_same = Item::<Cosine>::new(vec![f32::from_bits(1), f32::from_bits(1)]);
        assert!(Cosine::norm(&tiny) > 0.0);
        assert!(Cosine::distance(&tiny, &tiny_same) <= f32::EPSILON);
    }

    #[test]
    fn zero_norm_distance_is_invalid_instead_of_nearest() {
        let nonzero = Item::<Cosine>::new(vec![1.0, 0.0]);
        let zero = Item::<Cosine>::new(vec![0.0, 0.0]);
        assert!(Cosine::distance(&nonzero, &zero).is_nan());
        assert!(Cosine::distance(&zero, &zero).is_nan());
    }

    #[test]
    #[should_panic(expected = "cosine distance requires equal vector dimensions")]
    fn raw_kernel_panics_if_typed_dimension_validation_is_bypassed() {
        let short = UnalignedVector::from_slice(&[1.0, 2.0]);
        let long = UnalignedVector::from_slice(&[1.0, 2.0, 3.0]);
        let _ = stable_half_cosine(&short, &long);
    }
}
