use std::borrow::Cow;
use std::mem::transmute;
use std::slice::ChunksExact;

use super::{
    SimHash, SimHashError, SimHasher, SizeMismatch, UnalignedVector, UnalignedVectorCodec,
};

/// The type of the words used to quantize a vector
type BitPackedWord = u64;
/// The size of the words used to quantize a vector
const PACKED_WORD_BITS: usize = BitPackedWord::BITS as usize;
/// The number of bytes composing a Word
const PACKED_WORD_BYTES: usize = std::mem::size_of::<BitPackedWord>();

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Binary {}

impl UnalignedVectorCodec for Binary {
    fn from_bytes(bytes: &[u8]) -> Result<Cow<'_, UnalignedVector<Self>>, SizeMismatch> {
        let rem = bytes.len() % PACKED_WORD_BYTES;
        if rem == 0 {
            // safety: `UnalignedVector` is transparent
            Ok(Cow::Borrowed(unsafe {
                transmute::<&[u8], &UnalignedVector<Self>>(bytes)
            }))
        } else {
            Err(SizeMismatch {
                vector_codec: "binary",
                rem,
            })
        }
    }

    fn from_slice(slice: &[f32]) -> Cow<'static, UnalignedVector<Self>> {
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                return Cow::Owned(unsafe { from_slice_neon(slice) });
            }
        }
        Cow::Owned(from_slice_non_optimized(slice))
    }

    fn from_vec(vec: Vec<f32>) -> Cow<'static, UnalignedVector<Self>> {
        Cow::Owned(Self::from_slice(&vec).into_owned())
    }

    fn to_vec(vec: &UnalignedVector<Self>) -> Vec<f32> {
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                return unsafe { to_vec_neon(vec) };
            }
        }
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("sse4.1") {
                return unsafe { to_vec_sse(vec) };
            }
        }
        to_vec_non_optimized(vec)
    }

    fn iter(vec: &UnalignedVector<Self>) -> impl ExactSizeIterator<Item = f32> + '_ {
        BinaryIterator {
            current_element: 0,
            // Force the pulling of the first word
            current_iteration: PACKED_WORD_BITS,
            iter: vec.vector.chunks_exact(PACKED_WORD_BYTES),
        }
    }

    fn len(vec: &UnalignedVector<Self>) -> usize {
        (vec.vector.len() / PACKED_WORD_BYTES) * PACKED_WORD_BITS
    }

    fn is_zero(vec: &UnalignedVector<Self>) -> bool {
        vec.as_bytes().iter().all(|b| *b == 0)
    }

    fn word_size() -> usize {
        PACKED_WORD_BITS
    }

    fn compute_simhash(
        vec: &UnalignedVector<Self>,
        hasher: &SimHasher,
    ) -> Result<SimHash, SimHashError> {
        // For binary vectors, we already have efficient iteration
        // Convert bits to f32 (0.0 or 1.0) and hash
        hasher.hash_from_iter(vec.iter())
    }
}

pub(super) fn from_slice_non_optimized(slice: &[f32]) -> Vec<u8> {
    let mut output = Vec::with_capacity(slice.len().div_ceil(PACKED_WORD_BITS));
    for chunk in slice.chunks(PACKED_WORD_BITS) {
        let mut word: BitPackedWord = 0;
        let mut bits: u32;
        for scalar in chunk.iter().rev() {
            word <<= 1;
            bits = scalar.to_bits();
            // scalar > 0.0 => 1, otherwise 0
            word += (bits < 0x8000_0000 && bits > 0x0000_0000) as BitPackedWord;
        }
        output.extend_from_slice(&word.to_ne_bytes());
    }
    output
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
unsafe fn from_slice_neon(slice: &[f32]) -> Vec<u8> {
    use core::arch::aarch64::*;

    let iterations = slice.len() / PACKED_WORD_BYTES;
    // The size of the returned vector must be a multiple of a word
    let remaining = slice.len() % PACKED_WORD_BYTES;
    let mut len = iterations;
    if !len.is_multiple_of(PACKED_WORD_BYTES) {
        len += PACKED_WORD_BYTES - len % PACKED_WORD_BYTES;
    } else if remaining != 0 {
        // if we generated a valid number of Word but we're missing a few bits
        // then we need to add a full Word at the end.
        len += PACKED_WORD_BYTES;
    }
    let mut ret = vec![0; len];
    let ptr = slice.as_ptr();

    let low: [u32; 4] = [
        0b_00000000_00000000_00000000_00000001,
        0b_00000000_00000000_00000000_00000010,
        0b_00000000_00000000_00000000_00000100,
        0b_00000000_00000000_00000000_00001000,
    ];
    let high: [u32; 4] = [
        0b_00000000_00000000_00000000_00010000,
        0b_00000000_00000000_00000000_00100000,
        0b_00000000_00000000_00000000_01000000,
        0b_00000000_00000000_00000000_10000000,
    ];

    for i in 0..iterations {
        unsafe {
            let mut byte = 0;
            for (idx, mask) in [low, high].iter().enumerate() {
                let lane = vld1q_f32(ptr.add(i * 8 + 4 * idx));
                let lane = vcgtzq_f32(lane);
                let mask = vld1q_u32(mask.as_ptr());
                let lane = vandq_u32(lane, mask);

                byte |= vaddvq_u32(lane) as u8;
            }
            *ret.get_unchecked_mut(i) = byte;
        }
    }

    // Since we're iterating on bytes two by two.
    // If we had a number of dimensions not dividible by 8 we may be
    // missing some bits in the last byte.
    if remaining != 0 {
        let mut rem: u8 = 0;
        let mut bits: u32;
        for r in slice[slice.len() - remaining..].iter().rev() {
            rem <<= 1;
            bits = r.to_bits();
            let r = bits < 0x8000_0000 && bits > 0x0000_0000;
            rem |= r as u8;
        }
        ret[iterations] = rem;
    }

    ret
}

pub(super) fn to_vec_non_optimized(vec: &UnalignedVector<Binary>) -> Vec<f32> {
    vec.iter().collect()
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
unsafe fn to_vec_neon(vec: &UnalignedVector<Binary>) -> Vec<f32> {
    use core::arch::aarch64::*;

    let mut output: Vec<f32> = vec![0.0; vec.len()];
    let output_ptr = output.as_mut_ptr();
    let bytes = vec.as_bytes();
    let low_mask = [0b_0000_0001, 0b_0000_0010, 0b_0000_0100, 0b_0000_1000];
    let high_mask = [0b_0001_0000, 0b_0010_0000, 0b_0100_0000, 0b_1000_0000];
    let ones = unsafe { vld1q_dup_f32(&1.0) };
    let zeros = unsafe { vld1q_dup_f32(&0.0) };

    for (current_byte, base) in bytes.iter().enumerate() {
        unsafe {
            let base = *base as u32;
            let base = vld1q_dup_u32(&base);
            for (i, mask) in [low_mask, high_mask].iter().enumerate() {
                let mask = vld1q_u32(mask.as_ptr());
                let mask = vandq_u32(base, mask);
                // 0xffffffff if equal to zero and 0x00000000 otherwise
                let mask = vcgtq_u32(mask, vdupq_n_u32(0));
                let lane = vbslq_f32(mask, ones, zeros);
                let offset = output_ptr.add(current_byte * 8 + i * 4);
                vst1q_f32(offset, lane);
            }
        }
    }

    output
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse4.1")]
unsafe fn to_vec_sse(vec: &UnalignedVector<Binary>) -> Vec<f32> {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::*;

    let mut output: Vec<f32> = vec![0.0; vec.len()];
    let output_ptr = output.as_mut_ptr();
    let bytes = vec.as_bytes();
    let low_mask = [0b_0000_0001, 0b_0000_0010, 0b_0000_0100, 0b_0000_1000];
    let high_mask = [0b_0001_0000, 0b_0010_0000, 0b_0100_0000, 0b_1000_0000];
    let ones = unsafe { _mm_set1_ps(1.0) };
    let zeros = unsafe { _mm_setzero_ps() };

    for (current_byte, base) in bytes.iter().enumerate() {
        unsafe {
            let base = _mm_set1_epi32(*base as i32);
            for (i, mask) in [low_mask, high_mask].iter().enumerate() {
                let mask = _mm_set_epi32(mask[3], mask[2], mask[1], mask[0]);
                let mask = _mm_and_si128(base, mask);
                // 0xffffffff if equal to zero and 0x00000000 otherwise
                let mask = _mm_cmpeq_epi32(mask, _mm_setzero_si128());
                let lane = _mm_blendv_ps(ones, zeros, _mm_castsi128_ps(mask));
                let offset = output_ptr.add(current_byte * 8 + i * 4);
                _mm_storeu_ps(offset, lane);
            }
        }
    }

    output
}

pub struct BinaryIterator<'a> {
    current_element: BitPackedWord,
    current_iteration: usize,
    iter: ChunksExact<'a, u8>,
}

impl Iterator for BinaryIterator<'_> {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_iteration >= PACKED_WORD_BITS {
            let bytes = self.iter.next()?;
            self.current_element = BitPackedWord::from_ne_bytes(bytes.try_into().unwrap());
            self.current_iteration = 0;
        }

        let bit = self.current_element & 1;
        self.current_element >>= 1;
        self.current_iteration += 1;

        Some(bit as f32)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (low, high) = self.iter.size_hint();
        let rem = PACKED_WORD_BITS - self.current_iteration;

        (
            low * PACKED_WORD_BITS + rem,
            high.map(|h| h * PACKED_WORD_BITS + rem),
        )
    }
}

impl ExactSizeIterator for BinaryIterator<'_> {
    fn len(&self) -> usize {
        let (lower, upper) = self.size_hint();
        debug_assert_eq!(upper, Some(lower));
        lower
    }
}
