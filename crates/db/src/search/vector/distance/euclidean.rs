use std::fmt;

use bytemuck::{Pod, Zeroable};

use crate::search::vector::{
    distance::Distance,
    item::Item,
    spaces::simple::{dot_product, euclidean_distance},
    unaligned_vector::UnalignedVector,
};

/// The Euclidean distance between two points in Euclidean space
/// is the length of the line segment between them.
///
/// `d(p, q) = sqrt((p - q)²)`
#[derive(Debug, Clone)]
pub enum Euclidean {}

/// The header of Euclidean item nodes.
#[repr(C)]
#[derive(Pod, Zeroable, Clone, Copy)]
pub struct NodeHeaderEuclidean {
    /// An extra constant term to determine the offset of the plane
    bias: f32,
}
impl fmt::Debug for NodeHeaderEuclidean {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHeaderEuclidean")
            .field("bias", &format!("{:.4}", self.bias))
            .finish()
    }
}

impl Distance for Euclidean {
    type Header = NodeHeaderEuclidean;
    type VectorCodec = f32;

    fn name() -> &'static str {
        "euclidean"
    }

    fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {
        NodeHeaderEuclidean { bias: 0.0 }
    }

    fn distance(p: &Item<Self>, q: &Item<Self>) -> f32 {
        euclidean_distance(&p.vector, &q.vector)
    }

    fn norm_no_header(v: &UnalignedVector<Self::VectorCodec>) -> f32 {
        dot_product(v, v).sqrt()
    }
}
