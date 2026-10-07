//! Exact framing prepared before allocation. Borrowing the delta prevents its
//! payload sizes changing between preparation and serialization. The portable
//! roaring writer may still use scratch for optimized run-container headers.
use bytes::{BufMut, Bytes};

use super::{
    BitmapMembershipDelta, BITMAP_MEMBERSHIP_DELTA_LEN_PREFIX_LEN, BITMAP_MEMBERSHIP_DELTA_MAGIC,
};

pub(crate) struct Prepared<'a> {
    delta: &'a BitmapMembershipDelta,
    additions_len: u32,
    removals_len: u32,
    len: usize,
}

impl BitmapMembershipDelta {
    pub(crate) fn prepare_encoding(&self) -> Prepared<'_> {
        let additions_len =
            u32::try_from(self.additions.serialized_size()).expect("roaring additions fit u32");
        let removals_len =
            u32::try_from(self.removals.serialized_size()).expect("roaring removals fit u32");
        let len = (BITMAP_MEMBERSHIP_DELTA_MAGIC.len()
            + 2 * BITMAP_MEMBERSHIP_DELTA_LEN_PREFIX_LEN)
            .checked_add(additions_len as usize)
            .and_then(|len| len.checked_add(removals_len as usize))
            .expect("bitmap delta framing fits the address space");
        Prepared {
            delta: self,
            additions_len,
            removals_len,
            len,
        }
    }
}

impl Prepared<'_> {
    pub(crate) fn encoded_len(&self) -> usize {
        self.len
    }

    pub(crate) fn encode(self) -> Bytes {
        let mut bytes = Vec::with_capacity(self.len);
        self.write_into(&mut bytes);
        Bytes::from(bytes)
    }

    /// Append into an already sized outer frame. The capacity assertion keeps
    /// nested encoding from silently creating an additional growing buffer.
    pub(crate) fn write_into(self, bytes: &mut Vec<u8>) {
        assert!(
            bytes.capacity() - bytes.len() >= self.len,
            "bitmap delta needs its complete prepared capacity"
        );
        let expected_len = bytes.len() + self.len;
        bytes.extend_from_slice(BITMAP_MEMBERSHIP_DELTA_MAGIC);
        for (bitmap, len) in [
            (&self.delta.additions, self.additions_len),
            (&self.delta.removals, self.removals_len),
        ] {
            bytes.put_u32(len);
            bitmap
                .serialize_into(&mut *bytes)
                .expect("serializing membership changes into memory is infallible");
        }
        assert_eq!(bytes.len(), expected_len, "prepared bitmap framing length");
    }
}
