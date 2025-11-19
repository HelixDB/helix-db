use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

use hashbrown::HashMap;
use heed3::{Result, RoTxn};

use crate::helix_engine::vector_core::CoreDatabase;
use crate::helix_engine::vector_core::distance::Distance;
use crate::helix_engine::vector_core::key::{KeyCodec, Prefix, PrefixCodec};
use crate::helix_engine::vector_core::node::{Links, Node};

// TODO: ignore the phantom
#[derive(Debug)]
pub(crate) struct BuildStats<D> {
    /// a counter to see how many times `HnswBuilder.add_link` is invoked
    pub n_links_added: AtomicUsize,
    /// a counter tracking how many times we hit lmdb
    pub lmdb_hits: AtomicUsize,
    /// average rank of a node, calculated at the end of build
    pub mean_degree: f32,
    /// number of elements per layer
    pub layer_dist: HashMap<usize, usize>,
    /// track some race condition violations
    pub link_misses: AtomicUsize,

    _phantom: PhantomData<D>,
}

impl<D: Distance> BuildStats<D> {
    pub fn new() -> BuildStats<D> {
        BuildStats {
            n_links_added: AtomicUsize::new(0),
            lmdb_hits: AtomicUsize::new(0),
            mean_degree: 0.0,
            layer_dist: HashMap::default(),
            link_misses: AtomicUsize::new(0),
            _phantom: PhantomData,
        }
    }

    pub fn incr_link_count(&self, val: usize) {
        self.n_links_added.fetch_add(val, Ordering::Relaxed);
    }

    pub fn incr_lmdb_hits(&self) {
        self.lmdb_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_link_misses(&self) {
        self.link_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// iterate over all links in db and average out node rank
    pub fn compute_mean_degree(
        &mut self,
        rtxn: &RoTxn,
        db: &CoreDatabase<D>,
        index: u16,
    ) -> Result<()> {
        let iter = db
            .remap_key_type::<PrefixCodec>()
            .prefix_iter(rtxn, &Prefix::links(index))?
            .remap_key_type::<KeyCodec>();

        let mut n_links = 0;
        let mut total_links = 0;

        for res in iter {
            let (_key, node) = res?;

            let links = match node {
                Node::Links(Links { links }) => links,
                Node::Item(_) => unreachable!("Node must not be an item"),
            };

            total_links += links.len();
            n_links += 1;
        }

        self.mean_degree = (total_links as f32) / (n_links as f32);

        Ok(())
    }
}
