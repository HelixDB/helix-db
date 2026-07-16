use std::fmt;

use bytemuck::{Pod, Zeroable};

use crate::search::vector::{
    distance::Distance,
    item::Item,
    unaligned_vector::{self, BinaryQuantized, UnalignedVector},
};

/// A taxicab geometry or a Manhattan geometry is a geometry whose usual distance function
/// or metric of Euclidean geometry is replaced by a new metric in which the distance between
/// two points is the sum of the absolute differences of their Cartesian coordinates.
/// /!\ This distance function is binary quantized, which means it loses all its precision
///     and their scalar values are converted to `-1` or `1`.
#[derive(Debug, Clone)]
pub enum BinaryQuantizedManhattan {}

/// The header of BinaryQuantizedEuclidean item nodes.
#[repr(C)]
#[derive(Pod, Zeroable, Clone, Copy)]
pub struct NodeHeaderBinaryQuantizedManhattan {
    /// An extra constant term to determine the offset of the plane
    bias: f32,
}
impl fmt::Debug for NodeHeaderBinaryQuantizedManhattan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHeaderBinaryQuantizedManhattan")
            .field("bias", &format!("{:.4}", self.bias))
            .finish()
    }
}

impl Distance for BinaryQuantizedManhattan {
    type Header = NodeHeaderBinaryQuantizedManhattan;
    type VectorCodec = unaligned_vector::BinaryQuantized;

    fn name() -> &'static str {
        "binary quantized manhattan"
    }

    fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {
        NodeHeaderBinaryQuantizedManhattan { bias: 0.0 }
    }

    fn distance(p: &Item<Self>, q: &Item<Self>) -> f32 {
        manhattan_distance_binary_quantized(&p.vector, &q.vector)
    }

    fn norm_no_header(v: &UnalignedVector<Self::VectorCodec>) -> f32 {
        let ones = v
            .as_bytes()
            .iter()
            .map(|b| b.count_ones() as i32 - b.count_zeros() as i32)
            .sum::<i32>() as f32;
        ones.sqrt()
    }
}

/// For the binary quantized manhattan distance:
/// ```text
/// p.vector.iter().zip(q.vector.iter()).map(|(p, q)| (p - q).abs()).sum()
/// ```
/// 1. We need to subtract two scalars and take the absolute value:
/// ```text
/// -1 - -1 =  0 | abs => 0
/// -1 -  1 = -2 | abs => 2
///  1 - -1 =  2 | abs => 2
///  1 -  1 =  0 | abs => 0
/// ```
///
/// It's very similar to the euclidean distance.
/// => It's a xor, we counts the `1`s and multiplicate the result by `2` at the end.
fn manhattan_distance_binary_quantized(
    u: &UnalignedVector<BinaryQuantized>,
    v: &UnalignedVector<BinaryQuantized>,
) -> f32 {
    assert_eq!(
        u.len(),
        v.len(),
        "binary Manhattan distance requires equal vector dimensions"
    );
    let ret = u
        .as_bytes()
        .iter()
        .zip(v.as_bytes())
        .map(|(u, v)| (u ^ v).count_ones())
        .sum::<u32>()
        * 2;
    ret as f32
}
