//! Native-endian `f32` views over the deployed vector payload bytes.
//!
//! The codec validates byte length before constructing an unaligned view and
//! copies values only when an aligned allocation is explicitly requested. It
//! preserves the existing row bytes; descriptor and generation validation live
//! at higher boundaries.

use std::{
    borrow::Cow,
    mem::{size_of, transmute},
};

use bytemuck::cast_slice;
use byteorder::{ByteOrder, NativeEndian};

use super::{
    SimHash, SimHashError, SimHasher, SizeMismatch, UnalignedVector, UnalignedVectorCodec,
};

impl UnalignedVectorCodec for f32 {
    /// Creates an unaligned slice of f32 wrapper from a slice of bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Cow<'_, UnalignedVector<Self>>, SizeMismatch> {
        let rem = bytes.len() % size_of::<f32>();
        if rem == 0 {
            // safety: `UnalignedF32Slice` is transparent
            Ok(Cow::Borrowed(unsafe {
                transmute::<&[u8], &UnalignedVector<f32>>(bytes)
            }))
        } else {
            Err(SizeMismatch {
                vector_codec: "f32",
                rem,
            })
        }
    }

    /// Creates an unaligned slice of f32 wrapper from a slice of f32.
    /// The slice is already known to be of the right length.
    fn from_slice(slice: &[f32]) -> Cow<'_, UnalignedVector<Self>> {
        Self::from_bytes(cast_slice(slice)).unwrap()
    }

    /// Creates an unaligned slice of f32 wrapper from a slice of f32.
    /// The slice is already known to be of the right length.
    fn from_vec(vec: Vec<f32>) -> Cow<'static, UnalignedVector<Self>> {
        let bytes = vec.into_iter().flat_map(|f| f.to_ne_bytes()).collect();
        Cow::Owned(bytes)
    }

    fn to_vec(vec: &UnalignedVector<Self>) -> Vec<f32> {
        let iter = vec.iter();
        let mut ret = Vec::with_capacity(iter.len());
        ret.extend(iter);
        ret
    }

    /// Returns an iterator of f32 that are read from the slice.
    /// The f32 are copied in memory and are therefore, aligned.
    fn iter(vec: &UnalignedVector<Self>) -> impl ExactSizeIterator<Item = f32> + '_ {
        vec.vector
            .chunks_exact(size_of::<f32>())
            .map(NativeEndian::read_f32)
    }

    /// Return the number of f32 that fits into this slice.
    fn len(vec: &UnalignedVector<Self>) -> usize {
        vec.vector.len() / size_of::<f32>()
    }

    fn is_zero(vec: &UnalignedVector<Self>) -> bool {
        vec.iter().all(|v| v == 0.0)
    }

    fn compute_simhash(
        vec: &UnalignedVector<Self>,
        hasher: &SimHasher,
    ) -> Result<SimHash, SimHashError> {
        // For f32 vectors, we can efficiently iterate and hash
        // We collect to a temporary vector for cache efficiency
        let values: Vec<f32> = vec.iter().collect();
        hasher.hash_from_slice(&values)
    }
}
