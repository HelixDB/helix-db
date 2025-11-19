use std::fmt;

use bytemuck::{Pod, Zeroable};
use serde::Serialize;

use crate::helix_engine::vector_core::{
    distance::Distance, node::Item, spaces::simple::dot_product, unaligned_vector::UnalignedVector,
};

/// The Cosine similarity is a measure of similarity between two
/// non-zero vectors defined in an inner product space. Cosine similarity
/// is the cosine of the angle between the vectors.
#[derive(Debug, Serialize, Clone)]
pub enum Cosine {}

/// The header of Cosine item nodes.
#[repr(C)]
#[derive(Pod, Serialize, Zeroable, Clone, Copy)]
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

    fn distance(p: &Item<Self>, q: &Item<Self>) -> f32 {
        let pn = p.header.norm;
        let qn = q.header.norm;
        let pq = dot_product(&p.vector, &q.vector);
        let pnqn = pn * qn;
        if pnqn > f32::EPSILON {
            let cos = pq / pnqn;
            let cos = cos.clamp(-1.0, 1.0);
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
        dot_product(v, v).sqrt()
    }
}
