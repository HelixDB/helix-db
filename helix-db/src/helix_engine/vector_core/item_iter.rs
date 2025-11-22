use heed3::RoTxn;

use crate::helix_engine::vector_core::{
    CoreDatabase, LmdbResult,
    distance::Distance,
    key::{KeyCodec, Prefix, PrefixCodec},
    node::{Item, Node, NodeCodec},
    node_id::NodeId,
};

// used by the reader
pub struct ItemIter<'t, D: Distance> {
    pub inner: heed3::RoPrefix<'t, KeyCodec, NodeCodec<D>>,
    dimensions: usize,
}

impl<'t, D: Distance> ItemIter<'t, D> {
    pub fn new(
        database: CoreDatabase<D>,
        index: u16,
        dimensions: usize,
        rtxn: &'t RoTxn,
    ) -> heed3::Result<Self> {
        Ok(ItemIter {
            inner: database
                .remap_key_type::<PrefixCodec>()
                .prefix_iter(rtxn, &Prefix::item(index))?
                .remap_key_type::<KeyCodec>(),
            dimensions,
        })
    }

    pub fn next_id(&mut self) -> Option<LmdbResult<NodeId>> {
        match self.inner.next() {
            Some(Ok((key, node))) => match node {
                Node::Item(_) => Some(Ok(key.node)),
                Node::Links(_) => unreachable!("Node must not be a link"),
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

impl<'t, D: Distance> Iterator for ItemIter<'t, D> {
    type Item = LmdbResult<(NodeId, Item<'t, D>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok((key, node))) => match node {
                Node::Item(mut item) => {
                    if item.vector.len() != self.dimensions {
                        // quantized codecs pad to 8-bytes so we truncate to recover len
                        item.vector.to_mut().truncate(self.dimensions);
                    }
                    Some(Ok((key.node, item)))
                }
                Node::Links(_) => unreachable!("Node must not be a link"),
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}
