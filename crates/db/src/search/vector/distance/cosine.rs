//! Numerically stable cosine distance implementation for validated vectors.

use std::fmt;

use bytemuck::{Pod, Zeroable};

use crate::search::vector::{
    distance::Distance, item::Item, spaces::simple::dot_product, unaligned_vector::UnalignedVector,
};

/// Computes an L2 norm without overflowing or underflowing the sum of squares.
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
        let pq = dot_product(&p.vector, &q.vector);
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

    fn norm_no_header(v: &UnalignedVector<Self::VectorCodec>) -> f32 {
        scaled_l2_norm(v).min(f64::from(f32::MAX)) as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
