use heed3::RoTxn;

use crate::helix_engine::vector_core::{
    CoreDatabase, ItemId, LmdbResult,
    distance::Distance,
    key::{KeyCodec, Prefix, PrefixCodec},
    node::{Item, Node, NodeCodec},
};

// used by the reader
pub struct ItemIter<'t, D: Distance> {
    pub inner: heed3::RoPrefix<'t, KeyCodec, NodeCodec<D>>,
    dimensions: usize,
    arena: &'t bumpalo::Bump,
}

impl<'t, D: Distance> ItemIter<'t, D> {
    pub fn new(
        database: CoreDatabase<D>,
        index: u16,
        dimensions: usize,
        rtxn: &'t RoTxn,
        arena: &'t bumpalo::Bump,
    ) -> heed3::Result<Self> {
        Ok(ItemIter {
            inner: database
                .remap_key_type::<PrefixCodec>()
                .prefix_iter(rtxn, &Prefix::item(index))?
                .remap_key_type::<KeyCodec>(),
            dimensions,
            arena,
        })
    }
}

impl<'t, D: Distance> Iterator for ItemIter<'t, D> {
    type Item = LmdbResult<(ItemId, bumpalo::collections::Vec<'t, f32>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok((key, node))) => match node {
                Node::Item(Item { header: _, vector }) => {
                    let mut vector = vector.to_vec(&self.arena);
                    if vector.len() != self.dimensions {
                        // quantized codecs pad to 8-bytes so we truncate to recover len
                        vector.truncate(self.dimensions);
                    }
                    Some(Ok((key.node.item, vector)))
                }
                Node::Links(_) => unreachable!("Node must not be a link"),
            },
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }
}
