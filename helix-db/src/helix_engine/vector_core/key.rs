use std::borrow::Cow;
use std::mem::size_of;

use byteorder::{BigEndian, ByteOrder};
use heed3::BoxedError;

use crate::helix_engine::vector_core::node_id::{NodeId, NodeMode};

/// This whole structure must fit in an u64 so we can tell LMDB to optimize its storage.
/// The `index` is specified by the user and is used to differentiate between multiple indexes.
/// The `mode` indicates what we're looking at.
/// The `item` point to a specific node.
/// If the mode is:
///  - `Item`: we're looking at an `Item` node.
///  - `Links`: we're looking at the `Links` bitmap of neighbours for a node
///  - `Updated`: The list of items that has been updated since the last build of the database.
///  - `Metadata`: There is only one item at `0` that contains the header required to read the index.
#[derive(Debug, Copy, Clone)]
pub struct Key {
    /// The prefix specified by the user.
    pub index: u16,
    pub node: NodeId,
}

impl Key {
    pub const fn new(index: u16, node: NodeId) -> Self {
        Self { index, node }
    }

    pub const fn metadata(index: u16) -> Self {
        Self::new(index, NodeId::metadata())
    }

    pub const fn version(index: u16) -> Self {
        Self::new(index, NodeId::version())
    }

    pub const fn updated(index: u16, item: u32) -> Self {
        Self::new(index, NodeId::updated(item))
    }

    pub const fn item(index: u16, item: u32) -> Self {
        Self::new(index, NodeId::item(item))
    }

    pub const fn links(index: u16, item: u32, layer: u8) -> Self {
        Self::new(index, NodeId::links(item, layer))
    }
}

/// The heed codec used internally to encode/decoding the internal key type.
pub enum KeyCodec {}

impl<'a> heed3::BytesEncode<'a> for KeyCodec {
    type EItem = Key;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let mut output = Vec::with_capacity(size_of::<u64>());
        output.extend_from_slice(&item.index.to_be_bytes());
        output.extend_from_slice(&(item.node.mode as u8).to_be_bytes());
        output.extend_from_slice(&item.node.item.to_be_bytes());
        output.extend_from_slice(&(item.node.layer).to_be_bytes());

        Ok(Cow::Owned(output))
    }
}

impl heed3::BytesDecode<'_> for KeyCodec {
    type DItem = Key;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        let prefix = BigEndian::read_u16(bytes);
        let bytes = &bytes[size_of::<u16>()..];
        let mode = bytes[0].try_into()?;
        let bytes = &bytes[size_of::<u8>()..];
        let item = BigEndian::read_u32(bytes);
        let bytes = &bytes[size_of::<u32>()..];
        let layer = bytes[0];

        Ok(Key {
            index: prefix,
            node: NodeId { mode, item, layer },
        })
    }
}

/// This is used to query part of a key.
#[derive(Debug, Copy, Clone)]
pub struct Prefix {
    /// The index specified by the user.
    index: u16,
    // Indicate what the item represent.
    mode: Option<NodeMode>,
}

impl Prefix {
    pub const fn all(index: u16) -> Self {
        Self { index, mode: None }
    }

    pub const fn item(index: u16) -> Self {
        Self {
            index,
            mode: Some(NodeMode::Item),
        }
    }

    pub const fn links(index: u16) -> Self {
        Self {
            index,
            mode: Some(NodeMode::Links),
        }
    }

    pub const fn updated(index: u16) -> Self {
        Self {
            index,
            mode: Some(NodeMode::Updated),
        }
    }
}

pub enum PrefixCodec {}

impl<'a> heed3::BytesEncode<'a> for PrefixCodec {
    type EItem = Prefix;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let mode_used = item.mode.is_some() as usize;
        let mut output = Vec::with_capacity(size_of::<u16>() + mode_used);

        output.extend_from_slice(&item.index.to_be_bytes());
        if let Some(mode) = item.mode {
            output.extend_from_slice(&(mode as u8).to_be_bytes());
        }

        Ok(Cow::Owned(output))
    }
}

#[cfg(test)]
mod test {
    use heed3::{BytesDecode, BytesEncode};

    use super::*;

    #[test]
    fn check_size_of_types() {
        let key = Key::metadata(0);
        let encoded = KeyCodec::bytes_encode(&key).unwrap();
        assert_eq!(encoded.len(), size_of::<u64>());
    }

    // TODO: fuzz this
    #[test]
    fn test_links_key() {
        let key = Key::links(0, 1, 42);
        let bytes = KeyCodec::bytes_encode(&key).unwrap();
        let key2 = KeyCodec::bytes_decode(&bytes).unwrap();
        assert_eq!(key.node.item, key2.node.item);
        assert_eq!(key.node.layer, key2.node.layer);
        assert_eq!(key.node.mode, key2.node.mode);
    }

    #[test]
    fn test_item_key() {
        let key = Key::item(0, 42);
        let bytes = KeyCodec::bytes_encode(&key).unwrap();
        let key2 = KeyCodec::bytes_decode(&bytes).unwrap();
        assert_eq!(key.node.item, key2.node.item);
        assert_eq!(key.node.layer, key2.node.layer);
        assert_eq!(key.node.mode, key2.node.mode);
    }
}
