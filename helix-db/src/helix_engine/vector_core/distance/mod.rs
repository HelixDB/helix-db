use core::fmt;

use bytemuck::{Pod, Zeroable};

use crate::helix_engine::vector_core::{
    node::Item,
    unaligned_vector::{UnalignedVector, VectorCodec},
};

pub use cosine::{Cosine, NodeHeaderCosine};

mod cosine;

pub type DistanceValue = f32;

pub const MAX_DISTANCE: f32 = 2.0;
pub const ORTHOGONAL: f32 = 1.0;
pub const MIN_DISTANCE: f32 = 0.0;

pub trait Distance: Send + Sync + Sized + Clone + fmt::Debug + 'static {
    /// A header structure with informations related to the
    type Header: Pod + Zeroable + fmt::Debug;
    type VectorCodec: VectorCodec;

    /// The name of the distance.
    ///
    /// Note that the name is used to identify the distance and will help some performance improvements.
    /// For example, the "cosine" distance is matched against the "binary quantized cosine" to avoid
    /// recomputing links when moving from the former to the latter distance.
    fn name() -> &'static str;

    fn new_header(vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header;

    /// Returns a non-normalized distance.
    fn distance(p: &Item<Self>, q: &Item<Self>) -> DistanceValue;

    fn norm(item: &Item<Self>) -> f32 {
        Self::norm_no_header(&item.vector)
    }

    fn norm_no_header(v: &UnalignedVector<Self::VectorCodec>) -> f32;
}
