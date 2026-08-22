//! Shared edge direction for label and range index keys.

use crate::encoding::error::EncodingError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum EdgeDirection {
    Out = 0x00,
    In = 0x01,
}

impl EdgeDirection {
    pub(crate) fn from_u8(u: u8) -> Result<Self, EncodingError> {
        match u {
            0x00 => Ok(EdgeDirection::Out),
            0x01 => Ok(EdgeDirection::In),
            _ => Err(EncodingError::InvalidEdgeIndexDirection(u)),
        }
    }

    #[cfg(test)]
    pub(crate) fn as_u8(&self) -> u8 {
        match self {
            EdgeDirection::Out => 0x00,
            EdgeDirection::In => 0x01,
        }
    }
}
