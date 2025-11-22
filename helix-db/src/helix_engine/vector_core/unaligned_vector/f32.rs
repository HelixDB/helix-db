use std::{
    borrow::Cow,
    mem::{size_of, transmute},
};

use bytemuck::cast_slice;
use byteorder::{ByteOrder, NativeEndian};

use super::{SizeMismatch, UnalignedVector, VectorCodec};

impl VectorCodec for f32 {
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
    fn from_vec<'arena>(
        vec: bumpalo::collections::Vec<'arena, f32>,
    ) -> Cow<'static, UnalignedVector<Self>> {
        let bytes = vec.into_iter().flat_map(|f| f.to_ne_bytes()).collect();
        Cow::Owned(bytes)
    }

    // todo: add arena
    fn to_vec<'arena>(
        vec: &UnalignedVector<Self>,
        arena: &'arena bumpalo::Bump,
    ) -> bumpalo::collections::Vec<'arena, f32> {
        let iter = vec.iter();
        let mut ret = bumpalo::collections::Vec::with_capacity_in(iter.len(), arena);
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
}
