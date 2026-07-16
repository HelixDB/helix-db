use std::fmt;

use bytemuck::{Pod, Zeroable};

use crate::search::vector::{
    distance::Distance,
    item::Item,
    spaces::simple::dot_product_binary_quantized,
    unaligned_vector::{BinaryQuantized, UnalignedVector},
};

/// The Cosine similarity is a measure of similarity between two
/// non-zero vectors defined in an inner product space. Cosine similarity
/// is the cosine of the angle between the vectors.
/// /!\ This distance function is binary quantized, which means it loses all its precision
///     and their scalar values are converted to `-1` or `1`.
#[derive(Debug, Clone)]
pub enum BinaryQuantizedCosine {}

/// The header of `BinaryQuantizedCosine` item nodes.
#[repr(C)]
#[derive(Pod, Zeroable, Clone, Copy)]
pub struct NodeHeaderBinaryQuantizedCosine {
    norm: f32,
}
impl fmt::Debug for NodeHeaderBinaryQuantizedCosine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHeaderBinaryQuantizedCosine")
            .field("norm", &format!("{:.4}", self.norm))
            .finish()
    }
}

impl Distance for BinaryQuantizedCosine {
    type Header = NodeHeaderBinaryQuantizedCosine;
    type VectorCodec = BinaryQuantized;

    fn name() -> &'static str {
        "binary quantized cosine"
    }

    fn new_header(vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {
        NodeHeaderBinaryQuantizedCosine {
            norm: Self::norm_no_header(vector),
        }
    }

    fn distance(p: &Item<Self>, q: &Item<Self>) -> f32 {
        let pn = p.header.norm;
        let qn = q.header.norm;
        let pq = dot_product_binary_quantized(&p.vector, &q.vector);
        let pnqn = pn * qn;
        if pnqn != 0.0 {
            let cos = pq / pnqn;
            // cos is [-1; 1]
            // cos =  0. -> 0.5
            // cos = -1. -> 1.0
            // cos =  1. -> 0.0
            (1.0 - cos) / 2.0
        } else {
            0.0
        }
    }

    fn norm_no_header(v: &UnalignedVector<Self::VectorCodec>) -> f32 {
        dot_product_binary_quantized(v, v).sqrt()
    }
}
