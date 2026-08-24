//! Byte-compatible codec for current ID-allocation watermarks.
//!
//! Node and edge allocators persist the exclusive end of their leased ID range
//! as one big-endian `u64`. [`IdAllocationWatermarkValue`] owns that unchanged
//! value construction and parsing so allocator recovery and bounded secondary
//! source capture share one typed contract. This is an existing metadata value,
//! independent of lifecycle records, and no version byte is added.

use crate::encoding::error::EncodingError;

const WATERMARK_LEN: usize = core::mem::size_of::<u64>();
const WATERMARK_OFFSET: usize = 0;

/// Exclusive upper end of the current leased entity-ID range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IdAllocationWatermarkValue(u64);

impl IdAllocationWatermarkValue {
    /// Wraps an exclusive lease end; zero is the valid fresh-store sentinel.
    pub(crate) const fn new(exclusive_id: u64) -> Self {
        Self(exclusive_id)
    }

    /// Returns the exclusive entity-ID ceiling represented by this value.
    pub(crate) const fn exclusive_id(self) -> u64 {
        self.0
    }

    /// Encodes the exact deployed eight-byte big-endian value.
    pub(crate) const fn encode(self) -> [u8; WATERMARK_LEN] {
        self.0.to_be_bytes()
    }

    /// Decodes only the exact deployed eight-byte value.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, EncodingError> {
        if bytes.len() != WATERMARK_LEN {
            return Err(EncodingError::InvalidKey(format!(
                "ID allocation watermark must be {WATERMARK_LEN} bytes, got {}",
                bytes.len()
            )));
        }
        Ok(Self(u64::from_be_bytes(
            bytes[WATERMARK_OFFSET..WATERMARK_OFFSET + WATERMARK_LEN]
                .try_into()
                .expect("watermark slice is eight bytes"),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_current_bytes_round_trip_every_boundary() {
        for value in [0, 1, 1_000, u64::MAX] {
            let watermark = IdAllocationWatermarkValue::new(value);
            assert_eq!(watermark.encode(), value.to_be_bytes());
            assert_eq!(
                IdAllocationWatermarkValue::decode(&watermark.encode()).unwrap(),
                watermark
            );
            assert_eq!(watermark.exclusive_id(), value);
        }
        assert!(IdAllocationWatermarkValue::decode(&[]).is_err());
        assert!(IdAllocationWatermarkValue::decode(&[0; WATERMARK_LEN + 1]).is_err());
    }
}
