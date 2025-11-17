use crate::{
    helix_engine::{
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    protocol::value::Value,
};
use serde::Serialize;

#[cfg(feature = "rocks")]
use crate::helix_engine::storage_core::storage_methods::StorageMethods;
#[cfg(feature = "lmdb")]
use crate::{helix_engine::traversal_core::LMDB_STRING_HEADER_LENGTH, utils::items::Node};

pub trait NFromIndexAdapter<'db, 'arena, 'txn, 's, K: Into<Value> + Serialize>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns a new iterator that will return the node from the secondary index.
    ///
    /// # Arguments
    ///
    /// * `index` - The name of the secondary index.
    /// * `key` - The key to search for in the secondary index.
    ///
    /// Note that both the `index` and `key` must be provided.
    /// The index must be a valid and existing secondary index and the key should match the type of the index.
    fn n_from_index(
        self,
        label: &'s str,
        index: &'s str,
        key: &'s K,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: Into<Value> + Serialize + Clone;
}

#[cfg(feature = "lmdb")]
impl<
    'db,
    'arena,
    'txn,
    's,
    K: Into<Value> + Serialize,
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
> NFromIndexAdapter<'db, 'arena, 'txn, 's, K> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn n_from_index(
        self,
        label: &'s str,
        index: &'s str,
        key: &K,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: Into<Value> + Serialize + Clone,
    {
        let db = self
            .storage
            .secondary_indices
            .get(index)
            .ok_or(GraphError::New(format!(
                "Secondary Index {index} not found"
            )))
            .unwrap();
        let label_as_bytes = label.as_bytes();
        let res = db
            .prefix_iter(self.txn, &bincode::serialize(&Value::from(key)).unwrap())
            .unwrap()
            .filter_map(move |item| {
                if let Ok((_, node_id)) = item &&
                 let Some(value) = self.storage.nodes_db.get(self.txn, &node_id).ok()? {
                    assert!(
                        value.len() >= LMDB_STRING_HEADER_LENGTH,
                        "value length does not contain header which means the `label` field was missing from the node on insertion"
                    );
                    let length_of_label_in_lmdb =
                        u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap()) as usize;

                    if length_of_label_in_lmdb != label.len() {
                        return None;
                    }

                    assert!(
                        value.len() >= length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH,
                        "value length is not at least the header length plus the label length meaning there has been a corruption on node insertion"
                    );
                    let label_in_lmdb = &value[LMDB_STRING_HEADER_LENGTH
                        ..LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb];

                    if label_in_lmdb == label_as_bytes {
                        match Node::<'arena>::from_bincode_bytes(node_id, value, self.arena) {
                            Ok(node) => {
                                return Some(Ok(TraversalValue::Node(node)));
                            }
                            Err(e) => {
                                println!("{} Error decoding node: {:?}", line!(), e);
                                return Some(Err(GraphError::ConversionError(e.to_string())));
                            }
                        }
                    } else {
                        return None;
                    }

                }
                None


            });

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: res,
        }
    }
}

#[cfg(feature = "rocks")]
impl<
    'db,
    'arena,
    'txn,
    's,
    K: Into<Value> + Serialize,
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
> NFromIndexAdapter<'db, 'arena, 'txn, 's, K> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn n_from_index(
        self,
        label: &'s str,
        index: &'s str,
        key: &K,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: Into<Value> + Serialize + Clone,
    {
        let cf = self.storage.get_secondary_index_cf_handle(index).unwrap();
        let search_key = bincode::serialize(&Value::from(key)).unwrap();

        let storage = self.storage;
        let arena = self.arena;
        let txn = self.txn;

        let res = txn
            .prefix_iterator_cf(&cf, &search_key)
            .filter_map(move |result| {
                match result {
                    Ok((key_bytes, _value)) => {
                        // Manual prefix check for RocksDB
                        if !key_bytes.starts_with(&search_key) {
                            return None;
                        }

                        // Extract node_id from the end of the composite key (last 16 bytes)
                        if key_bytes.len() < 16 {
                            return None;
                        }
                        let node_id = u128::from_be_bytes(
                            key_bytes[key_bytes.len() - 16..].try_into().unwrap(),
                        );

                        // Get the full node using get_node()
                        // TODO FOR DIRECT LABEL CHECKING
                        match storage.get_node(txn, node_id, arena) {
                            Ok(node) => {
                                // Filter by label using deserialized node
                                if node.label == label {
                                    Some(Ok(TraversalValue::Node(node)))
                                } else {
                                    None
                                }
                            }
                            Err(e) => Some(Err(e)),
                        }
                    }
                    Err(_e) => Some(Err(GraphError::New("RocksDB iterator error".to_string()))),
                }
            });

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: res,
        }
    }
}
