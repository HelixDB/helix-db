use std::fmt;

use bytemuck::{Pod, Zeroable};

use crate::search::vector::{
    distance::Distance,
    item::Item,
    spaces::simple::dot_product_binary_quantized,
    unaligned_vector::{self, BinaryQuantized, UnalignedVector},
};

/// The Euclidean distance between two points in Euclidean space
/// is the length of the line segment between them.
///
/// `d(p, q) = sqrt((p - q)²)`
/// /!\ This distance function is binary quantized, which means it loses all its precision
///     and their scalar values are converted to `-1` or `1`.
#[derive(Debug, Clone)]
pub enum BinaryQuantizedEuclidean {}

/// The header of `BinaryQuantizedEuclidean` item nodes.
#[repr(C)]
#[derive(Pod, Zeroable, Clone, Copy)]
pub struct NodeHeaderBinaryQuantizedEuclidean {
    /// An extra constant term to determine the offset of the plane
    bias: f32,
}
impl fmt::Debug for NodeHeaderBinaryQuantizedEuclidean {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHeaderBinaryQuantizedEuclidean")
            .field("bias", &format!("{:.4}", self.bias))
            .finish()
    }
}

impl Distance for BinaryQuantizedEuclidean {
    type Header = NodeHeaderBinaryQuantizedEuclidean;
    type VectorCodec = unaligned_vector::BinaryQuantized;

    fn name() -> &'static str {
        "binary quantized euclidean"
    }

    fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {
        NodeHeaderBinaryQuantizedEuclidean { bias: 0.0 }
    }

    fn distance(p: &Item<Self>, q: &Item<Self>) -> f32 {
        squared_euclidean_distance_binary_quantized(&p.vector, &q.vector)
    }
    fn norm_no_header(v: &UnalignedVector<Self::VectorCodec>) -> f32 {
        dot_product_binary_quantized(v, v).sqrt()
    }
}

/// For the binary quantized squared euclidean distance:
/// 1. We need to do the following operation: `(u - v)^2`, in our case the only allowed values are `-1` and `1`:
/// ```text
/// -1 - -1 =  0 | ^2 => 0
/// -1 -  1 = -2 | ^2 => 4
///  1 - -1 =  2 | ^2 => 4
///  1 -  1 =  0 | ^2 => 0
/// ```
///
/// If we replace the `-1` by the binary quantized `0`, and the `1` stays `1`s:
/// ```text
/// 0 ^ 0 = 0
/// 0 ^ 1 = 1
/// 1 ^ 0 = 1
/// 1 ^ 1 = 0
/// ```
///
/// The result must be multiplicated by `4`. But that can be done at the very end.
///
/// 2. Then we need to do the sum of the results:
///    Since we cannot go into the negative, it's safe to hold everything in a `u32` and simply counts the `1`s.
///    At the very end, before converting the value to a `f32` we can multiply everything by 4.
fn squared_euclidean_distance_binary_quantized(
    u: &UnalignedVector<BinaryQuantized>,
    v: &UnalignedVector<BinaryQuantized>,
) -> f32 {
    assert_eq!(
        u.len(),
        v.len(),
        "binary Euclidean distance requires equal vector dimensions"
    );
    let ret = u
        .as_bytes()
        .iter()
        .zip(v.as_bytes())
        .map(|(u, v)| (u ^ v).count_ones())
        .sum::<u32>()
        * 4;
    ret as f32
}
