//! Typed stored values owned by lifecycle-managed index families.

use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::encoding::v2::keys::SecondaryEntryLane;
use crate::index_lifecycle::work::SecondaryEntryValue;

use super::{equality, range};

pub(crate) fn encode_secondary_entry(value: &SecondaryEntryValue) -> Bytes {
    if value.lane.is_equality() {
        equality::encode_entry(value).expect("equality lane selects its typed value codec")
    } else {
        range::encode_entry(value).expect("range lane selects its typed value codec")
    }
}

pub(crate) fn decode_secondary_entry(
    expected_lane: SecondaryEntryLane,
    value: &[u8],
) -> Result<SecondaryEntryValue, EncodingError> {
    if expected_lane.is_equality() {
        equality::decode_entry(expected_lane, value)
    } else {
        range::decode_entry(expected_lane, value)
    }
}
