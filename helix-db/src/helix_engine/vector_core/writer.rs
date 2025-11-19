use std::path::PathBuf;

use heed3::{
    RoTxn, RwTxn,
    types::{DecodeIgnore, Unit},
};
use rand::{Rng, SeedableRng};
use roaring::RoaringBitmap;

use crate::helix_engine::vector_core::{
    CoreDatabase, ItemId, VectorCoreResult, VectorError,
    distance::Distance,
    hnsw::HnswBuilder,
    item_iter::ItemIter,
    key::{Key, KeyCodec, Prefix, PrefixCodec},
    metadata::{Metadata, MetadataCodec},
    node::{Item, ItemIds, Links, Node},
    parallel::{ImmutableItems, ImmutableLinks},
    unaligned_vector::UnalignedVector,
    version::{Version, VersionCodec},
};

pub struct VectorBuilder<'a, D: Distance, R: Rng + SeedableRng> {
    writer: &'a Writer<D>,
    rng: &'a mut R,
    inner: BuildOption,
}

pub(crate) struct BuildOption {
    pub(crate) ef_construction: usize,
    pub(crate) alpha: f32,
    pub(crate) available_memory: Option<usize>,
    pub(crate) m: usize,
    pub(crate) m_max_0: usize,
}

impl BuildOption {
    fn default() -> Self {
        Self {
            ef_construction: 100,
            alpha: 1.0,
            available_memory: None,
            m: 16,
            m_max_0: 32,
        }
    }
}

impl<'a, D: Distance, R: Rng + SeedableRng> VectorBuilder<'a, D, R> {
    /// Controls the search range when inserting a new item into the graph. This value must be
    /// greater than or equal to the `M` used in [`Self::build<M,M0>`]
    ///
    /// Typical values range from 50 to 500, with larger `ef_construction` producing higher
    /// quality hnsw graphs at the expense of longer builds. The default value used in hannoy is
    /// 100.
    pub fn ef_construction(&mut self, ef_construction: usize) -> &mut Self {
        self.inner.ef_construction = ef_construction;
        self
    }

    /// Tunable hyperparameter for the graph building process. Alpha decreases the tolerance for
    /// link creation during index time. Alpha = 1 is the normal HNSW build while alpha > 1 is
    /// more similar to DiskANN. Increasing alpha increases indexing times as more neighbours are
    /// considered per linking step, but results in higher recall.
    ///
    /// DiskANN authors suggest using alpha=1.1 or alpha=1.2. By default alpha=1.0.
    pub fn alpha(&mut self, alpha: f32) -> &mut Self {
        self.inner.alpha = alpha;
        self
    }

    /// Generates an HNSW graph with max `M` links per node in layers > 0 and max `M0` links in layer 0.
    ///
    /// A general rule of thumb is to take `M0`= 2*`M`, with `M` >=3.  Some common choices for
    /// `M` include : 8, 12, 16, 32. Note that increasing `M` produces a denser graph at the cost
    /// of longer build times.
    pub fn build(&mut self, wtxn: &mut RwTxn) -> VectorCoreResult<()> {
        self.writer.build::<R>(wtxn, self.rng, &self.inner)
    }
}

/// A writer to store new items, remove existing ones, and build the search
/// index to query the nearest neighbors to items or vectors.
#[derive(Debug)]
pub struct Writer<D: Distance> {
    database: CoreDatabase<D>,
    index: u16,
    dimensions: usize,
    /// The folder in which tempfile will write its temporary files.
    tmpdir: Option<PathBuf>,
}

impl<D: Distance> Writer<D> {
    /// Creates a new writer from a database, index and dimensions.
    pub fn new(database: CoreDatabase<D>, index: u16, dimensions: usize) -> Writer<D> {
        Writer {
            database,
            index,
            dimensions,
            tmpdir: None,
        }
    }

    /// Sets the path to the temporary directory where files are written.
    pub fn set_tmpdir(&mut self, path: impl Into<PathBuf>) {
        self.tmpdir = Some(path.into());
    }

    /// Returns `true` if the index is empty.
    pub fn is_empty(&self, rtxn: &RoTxn, arena: &bumpalo::Bump) -> VectorCoreResult<bool> {
        self.iter(rtxn, arena).map(|mut iter| iter.next().is_none())
    }

    /// Returns `true` if the index needs to be built before being able to read in it.
    pub fn need_build(&self, rtxn: &RoTxn) -> VectorCoreResult<bool> {
        Ok(self
            .database
            .remap_types::<PrefixCodec, DecodeIgnore>()
            .prefix_iter(rtxn, &Prefix::updated(self.index))?
            .remap_key_type::<KeyCodec>()
            .next()
            .is_some()
            || self
                .database
                .remap_data_type::<DecodeIgnore>()
                .get(rtxn, &Key::metadata(self.index))?
                .is_none())
    }

    /// Returns `true` if the database contains the given item.
    pub fn contains_item(&self, rtxn: &RoTxn, item: ItemId) -> VectorCoreResult<bool> {
        self.database
            .remap_data_type::<DecodeIgnore>()
            .get(rtxn, &Key::item(self.index, item))
            .map(|opt| opt.is_some())
            .map_err(Into::into)
    }

    /// Returns an iterator over the items vector.
    pub fn iter<'t>(
        &self,
        rtxn: &'t RoTxn,
        arena: &'t bumpalo::Bump,
    ) -> VectorCoreResult<ItemIter<'t, D>> {
        Ok(ItemIter::new(
            self.database,
            self.index,
            self.dimensions,
            rtxn,
            arena,
        )?)
    }

    /// Add an item associated to a vector in the database.
    pub fn add_item(&self, wtxn: &mut RwTxn, item: ItemId, vector: &[f32]) -> VectorCoreResult<()> {
        if vector.len() != self.dimensions {
            return Err(VectorError::InvalidVecDimension {
                expected: self.dimensions,
                received: vector.len(),
            });
        }

        let vector = UnalignedVector::from_slice(vector);
        let db_item = Item {
            header: D::new_header(&vector),
            vector,
        };
        self.database
            .put(wtxn, &Key::item(self.index, item), &Node::Item(db_item))?;
        self.database
            .remap_data_type::<Unit>()
            .put(wtxn, &Key::updated(self.index, item), &())?;

        Ok(())
    }

    /// Deletes an item stored in this database and returns `true` if it existed.
    pub fn del_item(&self, wtxn: &mut RwTxn, item: ItemId) -> VectorCoreResult<bool> {
        if self.database.delete(wtxn, &Key::item(self.index, item))? {
            self.database.remap_data_type::<Unit>().put(
                wtxn,
                &Key::updated(self.index, item),
                &(),
            )?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Removes everything in the database, user items and internal graph links.
    pub fn clear(&self, wtxn: &mut RwTxn) -> VectorCoreResult<()> {
        let mut cursor = self
            .database
            .remap_key_type::<PrefixCodec>()
            .prefix_iter_mut(wtxn, &Prefix::all(self.index))?
            .remap_types::<DecodeIgnore, DecodeIgnore>();

        while let Some((_id, _node)) = cursor.next().transpose()? {
            // SAFETY: Safe because we don't keep any references to the entry
            unsafe { cursor.del_current() }?;
        }

        Ok(())
    }

    pub fn builder<'a, R>(&'a self, rng: &'a mut R) -> VectorBuilder<'a, D, R>
    where
        R: Rng + SeedableRng,
    {
        VectorBuilder {
            writer: self,
            rng,
            inner: BuildOption::default(),
        }
    }

    fn build<R>(&self, wtxn: &mut RwTxn, rng: &mut R, options: &BuildOption) -> VectorCoreResult<()>
    where
        R: Rng + SeedableRng,
    {
        let item_indices = self.item_indices(wtxn)?;
        // updated items can be an update, an addition or a removed item
        let updated_items = self.reset_and_retrieve_updated_items(wtxn)?;

        let to_delete = updated_items.clone() - &item_indices;
        let to_insert = &item_indices & &updated_items;

        let metadata = self
            .database
            .remap_data_type::<MetadataCodec>()
            .get(wtxn, &Key::metadata(self.index))?;

        let (entry_points, max_level) = metadata.as_ref().map_or_else(
            || (Vec::new(), usize::MIN),
            |metadata| {
                (
                    metadata.entry_points.iter().collect(),
                    metadata.max_level as usize,
                )
            },
        );

        // we should not keep a reference to the metadata since they're going to be moved by LMDB
        drop(metadata);

        let mut hnsw = HnswBuilder::<D>::new(options)
            .with_entry_points(entry_points)
            .with_max_level(max_level);

        let _ = hnsw.build(to_insert, &to_delete, self.database, self.index, wtxn, rng)?;

        // Remove deleted links from lmdb AFTER build; in DiskANN we use a deleted item's
        // neighbours when filling in the "gaps" left in the graph from deletions. See
        // [`HnswBuilder::maybe_patch_old_links`] for more details.
        self.delete_links_from_db(to_delete, wtxn)?;

        let metadata = Metadata {
            dimensions: self.dimensions.try_into().unwrap(),
            items: item_indices,
            entry_points: ItemIds::from_slice(&hnsw.entry_points),
            max_level: hnsw.max_level as u8,
            distance: D::name(),
        };
        self.database.remap_data_type::<MetadataCodec>().put(
            wtxn,
            &Key::metadata(self.index),
            &metadata,
        )?;
        self.database.remap_data_type::<VersionCodec>().put(
            wtxn,
            &Key::version(self.index),
            &Version::current(),
        )?;

        Ok(())
    }

    fn reset_and_retrieve_updated_items(
        &self,
        wtxn: &mut RwTxn,
    ) -> VectorCoreResult<RoaringBitmap> {
        let mut updated_items = RoaringBitmap::new();
        let mut updated_iter = self
            .database
            .remap_types::<PrefixCodec, DecodeIgnore>()
            .prefix_iter_mut(wtxn, &Prefix::updated(self.index))?
            .remap_key_type::<KeyCodec>();

        while let Some((key, _)) = updated_iter.next().transpose()? {
            let inserted = updated_items.insert(key.node.item);
            debug_assert!(inserted, "The keys should be sorted by LMDB");
            // SAFETY: Safe because we don't hold any reference to the database currently
            unsafe { updated_iter.del_current()? };
        }
        Ok(updated_items)
    }

    // Fetches the item's ids, not the links.
    fn item_indices(&self, wtxn: &mut RwTxn) -> VectorCoreResult<RoaringBitmap> {
        let mut indices = RoaringBitmap::new();
        for (_, result) in self
            .database
            .remap_types::<PrefixCodec, DecodeIgnore>()
            .prefix_iter(wtxn, &Prefix::item(self.index))?
            .remap_key_type::<KeyCodec>()
            .enumerate()
        {
            let (i, _) = result?;
            indices.insert(i.node.unwrap_item());
        }

        Ok(indices)
    }

    // Iterates over links in lmdb and deletes those in `to_delete`. There can be several links
    // with the same NodeId.item, each differing by their layer
    fn delete_links_from_db(
        &self,
        to_delete: RoaringBitmap,
        wtxn: &mut RwTxn,
    ) -> VectorCoreResult<()> {
        let mut cursor = self
            .database
            .remap_key_type::<PrefixCodec>()
            .prefix_iter_mut(wtxn, &Prefix::links(self.index))?
            .remap_types::<KeyCodec, DecodeIgnore>();

        while let Some((key, _)) = cursor.next().transpose()? {
            if to_delete.contains(key.node.item) {
                // SAFETY: Safe because we don't keep any references to the entry
                unsafe { cursor.del_current() }?;
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct FrozenReader<'a, D: Distance> {
    pub index: u16,
    pub items: &'a ImmutableItems<'a, D>,
    pub links: &'a ImmutableLinks<'a, D>,
}

impl<'a, D: Distance> FrozenReader<'a, D> {
    pub fn get_item(&self, item_id: ItemId) -> VectorCoreResult<Item<'a, D>> {
        let key = Key::item(self.index, item_id);
        // key is a `Key::item` so returned result must be a Node::Item
        self.items
            .get(item_id)?
            .ok_or(VectorError::missing_key(key))
    }

    pub fn get_links(&self, item_id: ItemId, level: usize) -> VectorCoreResult<Links<'a>> {
        let key = Key::links(self.index, item_id, level as u8);
        // key is a `Key::item` so returned result must be a Node::Item
        self.links
            .get(item_id, level as u8)?
            .ok_or(VectorError::missing_key(key))
    }
}

/// Clears all the links. Starts from the last node and stops at the first item.
fn clear_links<D: Distance>(
    wtxn: &mut RwTxn,
    database: CoreDatabase<D>,
    index: u16,
) -> VectorCoreResult<()> {
    let mut cursor = database
        .remap_types::<PrefixCodec, DecodeIgnore>()
        .prefix_iter_mut(wtxn, &Prefix::links(index))?
        .remap_key_type::<DecodeIgnore>();

    while let Some((_id, _node)) = cursor.next().transpose()? {
        // SAFETY: Safe because we don't keep any references to the entry
        unsafe { cursor.del_current()? };
    }

    Ok(())
}
