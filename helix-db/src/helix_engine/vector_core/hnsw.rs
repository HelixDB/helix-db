use crate::{helix_engine::types::VectorError, protocol::value::Value};
use crate::helix_engine::vector_core::vector::HVector;
use heed3::{RoTxn, RwTxn};

pub trait HNSW
{
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
    fn search<F>(
        &self,
        txn: &RoTxn,
        query: &[f64],
        k: usize,
        label: &str,
        filter: Option<&[F]>,
        should_trickle: bool,
    ) -> Result<Vec<HVector>, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool;

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
    fn insert<F>(
        &self,
        txn: &mut RwTxn,
        data: &[f64],
        fields: Option<Vec<(String, Value)>>,
    ) -> Result<HVector, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool;

    /// Get all vectors from the index at a specific level
    ///
    /// # Arguments
    ///
    /// * `txn` - The read-only transaction to use for retrieving vectors
    /// * `level` - A usize for which level to get all vectors from
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec` of `HVector` if successful
    fn get_all_vectors(
        &self,
        txn: &RoTxn,
        level: Option<usize>,
    ) -> Result<Vec<HVector>, VectorError>;

    /// Delete a vector from the index
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction to use
    /// * `id` - The id of the vector
    fn delete(
        &self,
        txn: &mut RwTxn,
        id: u128,
    ) -> Result<(), VectorError>;

    /// Get specific vector based on id and level
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction to use
    /// * `id` - The id of the vector
    /// * `level` - Which level to get the vector from
    /// * `with_data` - Whether or not to fetch the vector with data
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec` of `HVector` if successful
    fn get_vector(
        &self,
        txn: &RoTxn,
        id: u128,
        level: usize,
        with_data: bool,
    ) -> Result<HVector, VectorError>;
}

