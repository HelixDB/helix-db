use std::{borrow::Cow, ffi::CStr};

use byteorder::{BigEndian, ByteOrder};
use heed3::BoxedError;
use roaring::RoaringBitmap;

use crate::helix_engine::vector_core::node::ItemIds;

#[derive(Debug)]
pub struct Metadata<'a> {
    pub dimensions: u32,
    pub items: RoaringBitmap,
    pub distance: &'a str,
    pub entry_points: ItemIds<'a>,
    pub max_level: u8,
}

pub enum MetadataCodec {}

impl<'a> heed3::BytesEncode<'a> for MetadataCodec {
    type EItem = Metadata<'a>;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let Metadata {
            dimensions,
            items,
            entry_points,
            distance,
            max_level,
        } = item;
        debug_assert!(!distance.as_bytes().iter().any(|&b| b == 0));

        let mut output = Vec::with_capacity(
            size_of::<u32>()
                + items.serialized_size()
                + entry_points.len() * size_of::<u32>()
                + distance.len()
                + 1,
        );
        output.extend_from_slice(distance.as_bytes());
        output.push(0);
        output.extend_from_slice(&dimensions.to_be_bytes());
        output.extend_from_slice(&(items.serialized_size() as u32).to_be_bytes());
        items.serialize_into(&mut output)?;
        output.extend_from_slice(entry_points.raw_bytes());
        output.push(*max_level);

        Ok(Cow::Owned(output))
    }
}

impl<'a> heed3::BytesDecode<'a> for MetadataCodec {
    type DItem = Metadata<'a>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        let distance = CStr::from_bytes_until_nul(bytes)?.to_str()?;
        let bytes = &bytes[distance.len() + 1..];
        let dimensions = BigEndian::read_u32(bytes);
        let bytes = &bytes[size_of::<u32>()..];
        let items_size = BigEndian::read_u32(bytes) as usize;
        let bytes = &bytes[size_of::<u32>()..];
        let items = RoaringBitmap::deserialize_from(&bytes[..items_size])?;
        let bytes = &bytes[items_size..];
        let entry_points = ItemIds::from_bytes(&bytes[..bytes.len() - 1]);
        let max_level = bytes[bytes.len() - 1];

        Ok(Metadata {
            dimensions,
            items,
            distance,
            entry_points,
            max_level,
        })
    }
}
