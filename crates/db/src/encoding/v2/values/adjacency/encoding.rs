//! Compose the existing adjacency frame directly from borrowed bitmap encoders.
//! Nested bitmap lengths retain their existing little-endian framing; each
//! bitmap's internal lengths remain big-endian. No stored representation changes.
use bytes::{BufMut, Bytes};

use super::{
    AdjacencyMembershipDelta, ADJACENCY_MEMBERSHIP_DELTA_MAGIC, ADJACENCY_RESET_OUT_LEN,
    BITMAP_LEN_PREFIX_LEN,
};
use crate::encoding::v2::values::indexes::equality::encoding as bitmap;

pub(crate) struct Prepared<'a> {
    outgoing: bitmap::Prepared<'a>,
    incoming: bitmap::Prepared<'a>,
    outgoing_len: u32,
    incoming_len: u32,
    reset_out: bool,
    len: usize,
}

impl AdjacencyMembershipDelta {
    pub(crate) fn prepare_encoding(&self) -> Prepared<'_> {
        let outgoing = self.outgoing.prepare_encoding();
        let incoming = self.incoming.prepare_encoding();
        let outgoing_len =
            u32::try_from(outgoing.encoded_len()).expect("outgoing delta length fits u32");
        let incoming_len =
            u32::try_from(incoming.encoded_len()).expect("incoming delta length fits u32");
        let len = (ADJACENCY_MEMBERSHIP_DELTA_MAGIC.len()
            + ADJACENCY_RESET_OUT_LEN
            + 2 * BITMAP_LEN_PREFIX_LEN)
            .checked_add(outgoing_len as usize)
            .and_then(|len| len.checked_add(incoming_len as usize))
            .expect("adjacency delta framing fits the address space");
        Prepared {
            outgoing,
            incoming,
            outgoing_len,
            incoming_len,
            reset_out: self.reset_out,
            len,
        }
    }
}

impl Prepared<'_> {
    pub(crate) fn encoded_len(&self) -> usize {
        self.len
    }

    pub(crate) fn encode(self) -> Bytes {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        bytes.extend_from_slice(ADJACENCY_MEMBERSHIP_DELTA_MAGIC);
        bytes.put_u8(u8::from(self.reset_out));
        bytes.put_u32_le(self.outgoing_len);
        self.outgoing.write_into(&mut bytes);
        bytes.put_u32_le(self.incoming_len);
        self.incoming.write_into(&mut bytes);
        assert_eq!(bytes.len(), self.len, "prepared adjacency framing length");
        Bytes::from(bytes)
    }
}
