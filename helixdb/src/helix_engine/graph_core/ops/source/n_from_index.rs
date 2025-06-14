use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::{items::Node, value::Value},
};
use crate::helix_storage::heed3::{byteorder::BE, types::Bytes, RoTxn};
use serde::Serialize;
use std::{iter::Once, sync::Arc};

pub struct NFromIndex<'a> {
    iter:
        crate::helix_storage::heed3::RoPrefix<'a, crate::helix_storage::heed3::types::Bytes, crate::helix_storage::heed3::types::LazyDecode<crate::helix_storage::heed3::types::U128<BE>>>,
    txn: &'a RoTxn<'a>,
    storage: Arc<HelixGraphStorage>,
}

impl<'a> Iterator for NFromIndex<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(value) = self.iter.next() {
            let (key_, value) = value.unwrap();
            match value.decode() {
                Ok(value) => match self.storage.get_node(self.txn, &value) {
                    Ok(node) => return Some(Ok(TraversalVal::Node(node))),
                    Err(e) => {
                        println!("{} Error getting node: {:?}", line!(), e);
                        return Some(Err(GraphError::ConversionError(e.to_string())));
                    }
                },

                Err(e) => return Some(Err(GraphError::ConversionError(e.to_string()))),
            }
        }
        None
    }
}

pub trait NFromIndexAdapter<'a, K: Into<Value> + Serialize>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    type OutputIter: Iterator<Item = Result<TraversalVal, GraphError>>;

    /// Returns a new iterator that will return the node from the secondary index.
    ///
    /// # Arguments
    ///
    /// * `index` - The name of the secondary index.
    /// * `key` - The key to search for in the secondary index.
    ///
    /// Note that both the `index` and `key` must be provided.
    /// The index must be a valid and existing secondary index and the key should match the type of the index.
    fn n_from_index(self, index: &'a str, key: &'a K) -> Self::OutputIter
    where
        K: Into<Value> + Serialize + Clone;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>, K: Into<Value> + Serialize + 'a>
    NFromIndexAdapter<'a, K> for RoTraversalIterator<'a, I>
{
    type OutputIter = RoTraversalIterator<'a, NFromIndex<'a>>;

    #[inline]
    fn n_from_index(self, index: &'a str, key: &'a K) -> Self::OutputIter
    where
        K: Into<Value> + Serialize + Clone,
    {
        let db = self
            .storage
            .secondary_indices
            .get(index)
            .ok_or(GraphError::New(format!(
                "Secondary Index {} not found",
                index
            )))
            .unwrap();
        println!("DB: {:?}", self.storage.secondary_indices);
        let res = db
            .lazily_decode_data()
            .prefix_iter(self.txn, &bincode::serialize(&Value::from(key)).unwrap())
            .unwrap();

        let n_from_index = NFromIndex {
            iter: res,
            txn: self.txn,
            storage: Arc::clone(&self.storage),
        };

        RoTraversalIterator {
            inner: n_from_index,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
