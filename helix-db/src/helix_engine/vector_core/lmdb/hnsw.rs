use crate::helix_engine::vector_core::vector::HVector;
use crate::{helix_engine::types::VectorError, utils::properties::ImmutablePropertiesMap};

pub trait HNSW {
    /// Search for the k nearest neighbors of a query vector
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction to use
    /// * `query` - The query vector
    /// * `k` - The number of nearest neighbors to search for
    ///
    /// # Returns
    ///
    /// A vector of tuples containing the id and distance of the nearest neighbors
    fn search<'db, 'arena, 'txn, F>(
        &'db self,
        txn: &'txn heed3::RoTxn<'db>,
        query: &'arena [f64],
        k: usize,
        label: &'arena str,
        filter: Option<&'arena [F]>,
        should_trickle: bool,
        arena: &'arena bumpalo::Bump,
    ) -> Result<bumpalo::collections::Vec<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &RoTxn<'db>) -> bool,
        'db: 'arena,
        'arena: 'txn;

    /// Insert a new vector into the index
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction to use
    /// * `data` - The vector data
    ///
    /// # Returns
    ///
    /// An HVector of the data inserted
    fn insert<'db, 'arena, 'txn, F>(
        &'db self,
        txn: &'txn mut heed3::RwTxn<'db>,
        label: &'arena str,
        data: &'arena [f64],
        properties: Option<ImmutablePropertiesMap<'arena>>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<HVector<'arena>, VectorError>
    where
        F: Fn(&HVector<'arena>, &heed3::RoTxn<'db>) -> bool,
        'db: 'arena,
        'arena: 'txn;

    /// Delete a vector from the index
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction to use
    /// * `id` - The id of the vector
    fn delete(
        &self,
        txn: &mut heed3::RwTxn,
        id: u128,
        arena: &bumpalo::Bump,
    ) -> Result<(), VectorError>;
}
