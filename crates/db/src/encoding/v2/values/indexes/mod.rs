//! Stored values for secondary, text, and vector indexes.

pub(crate) mod equality;
pub(crate) mod operation_queue;
pub(crate) mod range;
mod secondary_entry;
pub(crate) mod text;
pub mod vector;

pub(crate) use equality::{
    BitmapMembershipDelta, SecondaryEqualityBitmapValue, SecondaryEqualityValue,
};
pub(crate) use secondary_entry::{decode_secondary_entry, encode_secondary_entry};
