use std::fmt;

use bytemuck::{Pod, Zeroable};

use crate::search::vector::{
    distance::Distance,
    item::Item,
    unaligned_vector::{Binary, UnalignedVector},
};

/// The Hamming distance between two vectors is the number of positions at
/// which the corresponding symbols are different.
///
/// `d(u,v) = ||u ^ v||₁`
///
/// /!\ This distance function is binary, which means it loses all its precision
///     and their scalar values are converted to `0` or `1` under the rule
///     `x > 0.0 => 1`, otherwise `0`
#[derive(Debug, Clone)]
pub enum Hamming {}

/// The header of BinaryEuclidean Item nodes.
#[repr(C)]
#[derive(Pod, Zeroable, Clone, Copy)]
pub struct NodeHeaderHamming {
    idx: usize,
}
impl fmt::Debug for NodeHeaderHamming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHeaderHamming ")
            .field("idx", &format!("{}", self.idx))
            .finish()
    }
}

impl Distance for Hamming {
    type Header = NodeHeaderHamming;
    type VectorCodec = Binary;

    fn name() -> &'static str {
        "hamming"
    }

    fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {
        NodeHeaderHamming { idx: 0 }
    }

    fn distance(p: &Item<Self>, q: &Item<Self>) -> f32 {
        let dist = hamming_bitwise_fast(p.vector.as_bytes(), q.vector.as_bytes());
        dist / (p.vector.len() as f32)
    }

    fn norm_no_header(v: &UnalignedVector<Self::VectorCodec>) -> f32 {
        v.as_bytes()
            .iter()
            .map(|b| b.count_ones() as i32)
            .sum::<i32>() as f32
    }
}

#[inline]
pub fn hamming_bitwise_fast(u: &[u8], v: &[u8]) -> f32 {
    // based on : https://github.com/emschwartz/hamming-bitwise-fast
    // Explicitly structuring the code as below lends itself to SIMD optimizations by
    // the compiler -> https://matklad.github.io/2023/04/09/can-you-trust-a-compiler-to-optimize-your-code.html
    assert_eq!(u.len(), v.len());

    type BitPackedWord = u64;
    const CHUNK_SIZE: usize = std::mem::size_of::<BitPackedWord>();

    let mut distance = u
        .chunks_exact(CHUNK_SIZE)
        .zip(v.chunks_exact(CHUNK_SIZE))
        .map(|(u_chunk, v_chunk)| {
            let u_val = BitPackedWord::from_ne_bytes(u_chunk.try_into().unwrap());
            let v_val = BitPackedWord::from_ne_bytes(v_chunk.try_into().unwrap());
            (u_val ^ v_val).count_ones()
        })
        .sum::<u32>();

    if !u.len().is_multiple_of(CHUNK_SIZE) {
        distance += u
            .chunks_exact(CHUNK_SIZE)
            .remainder()
            .iter()
            .zip(v.chunks_exact(CHUNK_SIZE).remainder())
            .map(|(u_byte, v_byte)| (u_byte ^ v_byte).count_ones())
            .sum::<u32>();
    }

    distance as f32
}
