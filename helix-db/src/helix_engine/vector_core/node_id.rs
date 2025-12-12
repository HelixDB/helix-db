use core::fmt;

use byteorder::{BigEndian, ByteOrder};

use crate::helix_engine::vector_core::{ItemId, LayerId};

/// /!\ Changing the value of the enum can be DB-breaking /!\
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum NodeMode {
    /// Stores the metadata under the `ItemId` 0
    Metadata = 0,
    /// Stores the list of all the `ItemId` that have been updated.
    /// We only stores `Unit` values under the keys.
    Updated = 1,
    /// The graph edges re stored under this id
    Links = 2,
    /// The original vectors are stored under this id in `Item` structures.
    Item = 3,
}

impl TryFrom<u8> for NodeMode {
    type Error = String;

    fn try_from(v: u8) -> std::result::Result<Self, Self::Error> {
        match v {
            v if v == NodeMode::Item as u8 => Ok(NodeMode::Item),
            v if v == NodeMode::Links as u8 => Ok(NodeMode::Links),
            v if v == NodeMode::Updated as u8 => Ok(NodeMode::Updated),
            v if v == NodeMode::Metadata as u8 => Ok(NodeMode::Metadata),
            v => Err(format!("Could not convert {v} as a `NodeMode`.")),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId {
    /// Indicate what the item represent.
    pub mode: NodeMode,
    /// The item we want to get.
    pub item: ItemId,
    /// Store Hnsw layer ID after ItemId for co-locality of (vec, its_links) in lmdb (?)
    /// Safe to store in a u8 since impossible the graph will have >256 layers
    pub layer: LayerId,
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}({},{})", self.mode, self.item, self.layer)
    }
}

impl NodeId {
    pub const fn metadata() -> Self {
        Self {
            mode: NodeMode::Metadata,
            item: 0,
            layer: 0,
        }
    }

    pub const fn version() -> Self {
        Self {
            mode: NodeMode::Metadata,
            item: 1,
            layer: 0,
        }
    }

    pub const fn updated(item: u32) -> Self {
        Self {
            mode: NodeMode::Updated,
            item,
            layer: 0,
        }
    }

    pub const fn links(item: u32, layer: u8) -> Self {
        Self {
            mode: NodeMode::Links,
            item,
            layer,
        }
    }

    pub const fn item(item: u32) -> Self {
        Self {
            mode: NodeMode::Item,
            item,
            layer: 0,
        }
    }

    /// Return the underlying `ItemId` if it is an item.
    /// Panic otherwise.
    #[track_caller]
    pub fn unwrap_item(&self) -> ItemId {
        assert_eq!(self.mode, NodeMode::Item);
        self.item
    }

    /// Return the underlying `ItemId` if it is a links node.
    /// Panic otherwise.
    #[track_caller]
    pub fn unwrap_node(&self) -> (ItemId, LayerId) {
        assert_eq!(self.mode, NodeMode::Links);
        (self.item, self.layer)
    }

    pub fn to_bytes(self) -> [u8; 6] {
        let mut output = [0; 6];

        output[0] = self.mode as u8;
        output[1] = self.layer;
        let item_bytes = self.item.to_be_bytes();
        output[2..=5].copy_from_slice(&item_bytes);

        output
    }

    pub fn from_bytes(bytes: &[u8]) -> (Self, &[u8]) {
        let mode = NodeMode::try_from(bytes[0]).expect("Could not parse the node mode");
        let layer = bytes[1];
        let item = BigEndian::read_u32(&bytes[2..]);

        (
            Self { mode, item, layer },
            &bytes[size_of::<NodeMode>() + size_of::<ItemId>()..],
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn check_node_id_ordering() {
        // NOTE: `layer`s take precedence over item_ids
        assert!(NodeId::item(0) == NodeId::item(0));
        assert!(NodeId::item(1) > NodeId::item(0));
        assert!(NodeId::item(0) < NodeId::item(1));

        assert!(NodeId::links(0, 0) == NodeId::links(0, 0));
        assert!(NodeId::links(1, 0) > NodeId::links(0, 0));
        assert!(NodeId::links(0, 1) > NodeId::links(0, 0));
        assert!(NodeId::links(1, 0) > NodeId::links(0, 1));

        assert!(NodeId::updated(0) == NodeId::updated(0));
        assert!(NodeId::updated(1) > NodeId::updated(0));
        assert!(NodeId::updated(0) < NodeId::updated(1));

        assert!(NodeId::links(u32::MAX, 0) < NodeId::item(0));

        assert!(NodeId::metadata() == NodeId::metadata());
        assert!(NodeId::metadata() < NodeId::links(u32::MIN, 0));
        assert!(NodeId::metadata() < NodeId::updated(u32::MIN));
        assert!(NodeId::metadata() < NodeId::item(u32::MIN));
    }
}
