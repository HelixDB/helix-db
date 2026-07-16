use std::fmt;

use bytemuck::{Pod, Zeroable};

use crate::search::vector::{
    distance::Distance,
    item::Item,
    spaces::simple::{dot_product, manhattan_distance},
    unaligned_vector::UnalignedVector,
};

/// A taxicab geometry or a Manhattan geometry is a geometry whose usual distance function
/// or metric of Euclidean geometry is replaced by a new metric in which the distance between
/// two points is the sum of the absolute differences of their Cartesian coordinates.
#[derive(Debug, Clone)]
pub enum Manhattan {}

/// The header of Manhattan item nodes.
#[repr(C)]
#[derive(Pod, Zeroable, Clone, Copy)]
pub struct NodeHeaderManhattan {
    /// An extra constant term to determine the offset of the plane
    bias: f32,
}
impl fmt::Debug for NodeHeaderManhattan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHeaderManhattan")
            .field("bias", &format!("{:.4}", self.bias))
            .finish()
    }
}

impl Distance for Manhattan {
    type Header = NodeHeaderManhattan;
    type VectorCodec = f32;

    fn name() -> &'static str {
        "manhattan"
    }

    fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {
        NodeHeaderManhattan { bias: 0.0 }
    }

    fn distance(p: &Item<Self>, q: &Item<Self>) -> f32 {
        manhattan_distance(&p.vector, &q.vector)
    }

    fn norm_no_header(v: &UnalignedVector<Self::VectorCodec>) -> f32 {
        dot_product(v, v).sqrt()
    }
}
